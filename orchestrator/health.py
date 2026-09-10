"""Audit-durable, redacted health evidence.

Component health probes (state DB connectivity, kanban source reachability,
action gateway transport status, ...) may incidentally carry secrets — an
auth header used to reach a probe endpoint, a token embedded in a connection
string. ``redact`` strips anything under a sensitive-looking key before
``record_health_evidence`` writes the snapshot to the durable, queryable
audit log, so the evidence trail is safe to read back without re-exposing
credentials.
"""

from __future__ import annotations

import re
import sqlite3
from typing import Any

from orchestrator.audit import AuditEntry, AuditLog

REDACTED_VALUE = "***REDACTED***"

# Substring match, case-insensitive, against each dict key — deliberately a
# denylist of *key* shapes (not values): health evidence is arbitrary
# probe-shaped data this module doesn't control the schema of, so the safe
# default is to redact by suspicious key name rather than try to recognize
# every possible secret value shape.
_SENSITIVE_KEY_SUBSTRINGS = ("token", "secret", "password", "credential", "authorization", "api_key", "apikey")

_SENSITIVE_QUERY_PARAMS = ("token", "secret", "password", "credential", "authorization", "api_key", "apikey", "key")

_URL_USERINFO_RE = re.compile(r"(?P<scheme>[A-Za-z][A-Za-z0-9+.-]*://)(?P<user>[^:@/\s]+):(?P<password>[^@/\s]+)@")
# Username-only userinfo (no ``:password``) -- e.g. ``https://ghp_xxx@host/``,
# a convention where the "username" position itself carries a token/secret.
# The excluded ``:`` in the character class means this never matches the
# ``user:password@`` form above (that stops at the colon and requires an
# immediate ``@``, which a password segment never provides), so both regexes
# can run unconditionally without stepping on each other.
_URL_USERNAME_ONLY_RE = re.compile(r"(?P<scheme>[A-Za-z][A-Za-z0-9+.-]*://)(?P<user>[^:@/\s]+)@")
# Substring match against the query param *name* (like _is_sensitive_key),
# not an exact match -- so e.g. ``access_token=`` is caught by the ``token``
# marker just as a dict key ``access_token`` already is.
_QUERY_PARAM_RE = re.compile(
    r"(?P<prefix>[?&][^=&\s]*(?:" + "|".join(_SENSITIVE_QUERY_PARAMS) + r")[^=&\s]*=)(?P<value>[^&\s]+)",
    re.IGNORECASE,
)
_BEARER_RE = re.compile(r"(?P<prefix>Bearer\s+)(?P<value>[^\s]+)", re.IGNORECASE)
# ``Authorization: <scheme> <value>`` for any scheme word (Bearer, token,
# Basic, ApiKey, ...) -- e.g. ``Authorization: token abc123`` -- not just the
# literal ``Bearer`` scheme _BEARER_RE already covers on its own (including
# outside an explicit ``Authorization:`` prefix, e.g. a bare header value).
_AUTHORIZATION_HEADER_RE = re.compile(r"(?P<prefix>Authorization:\s*\S+\s+)(?P<value>\S+)", re.IGNORECASE)


def _is_sensitive_key(key: str) -> bool:
    lowered = key.lower()
    return any(marker in lowered for marker in _SENSITIVE_KEY_SUBSTRINGS)


def _redact_string(text: str) -> str:
    text = _URL_USERINFO_RE.sub(lambda m: f"{m.group('scheme')}{m.group('user')}:{REDACTED_VALUE}@", text)
    text = _URL_USERNAME_ONLY_RE.sub(lambda m: f"{m.group('scheme')}{REDACTED_VALUE}@", text)
    text = _QUERY_PARAM_RE.sub(lambda m: f"{m.group('prefix')}{REDACTED_VALUE}", text)
    text = _AUTHORIZATION_HEADER_RE.sub(lambda m: f"{m.group('prefix')}{REDACTED_VALUE}", text)
    text = _BEARER_RE.sub(lambda m: f"{m.group('prefix')}{REDACTED_VALUE}", text)
    return text


def redact(value: Any) -> Any:
    """Recursively redact sensitive-looking dict keys without mutating ``value``."""
    if isinstance(value, dict):
        return {
            key: (REDACTED_VALUE if _is_sensitive_key(key) else redact(sub_value))
            for key, sub_value in value.items()
        }
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact(item) for item in value)
    if isinstance(value, str):
        return _redact_string(value)
    return value


def _overall_status(components: dict[str, Any]) -> str:
    statuses = {str(component.get("status", "unknown")).lower() for component in components.values()}
    return "ok" if statuses <= {"ok"} else "degraded"


def record_health_evidence(
    conn: sqlite3.Connection, *, components: dict[str, Any], now: str | None = None
) -> AuditEntry:
    """Record one durable, redacted health snapshot in the append-only audit log."""
    detail = {"components": redact(components), "overall_status": _overall_status(components)}
    return AuditLog(conn).record(actor="health", action="health.snapshot", detail=detail, now=now)
