"""Policy guard: the control plane's hard boundary on what an outbox action may do.

Neither ``runtime_orchestrator`` nor ``lead_orchestrator`` may execute a
merge or release (this package implements orchestration, not execution), gate
approval at High or Critical severity, or any other action flagged
irreversible. Critical gates have no approval route at all — there is no
severity value or flag that lets one through.

This guards with an allowlist, not a denylist: only action types and gate
severities named below may ever be enqueued. A denylist has to name every
dangerous variant in advance (an unnormalized spelling, an unknown new action
type an adapter starts emitting) or it silently lets it through; an allowlist
rejects anything not explicitly vetted, so an unrecognized or malformed value
fails closed instead of fails open. Every value is normalized (stripped,
lowercased) before being checked against the allowlist, so whitespace or case
variants of a blocked or unknown value can't slip past.

This is intentionally a single small function so every outbox write can call
it as a pre-write guard (``Outbox.enqueue`` does) and the whole policy stays
in one place to audit.
"""

from __future__ import annotations

from typing import Any

# The only action types this control plane will ever enqueue, regardless of
# consumer or payload. Anything not listed here — a typo, an unnormalized
# variant, a new action type nobody has vetted yet — is rejected fail-closed,
# rather than requiring every dangerous name to be enumerated in a denylist.
ALLOWED_ACTION_TYPES = frozenset(
    {
        "post_comment",
        "approve_gate",
    }
)

# Gate approval is a distinct action type: its severity is inspected rather
# than the action type alone. Only these normalized severities have an
# approval route; anything else — high, critical, an unknown value, a
# malformed payload — is rejected fail-closed by the same allowlist check.
GATE_ACTION_TYPES = frozenset({"approve_gate"})
ALLOWED_GATE_SEVERITIES = frozenset({"low", "medium"})


class PolicyViolation(Exception):
    """Raised when an outbox action would violate the control-plane policy."""


def _normalize_token(value: Any) -> str | None:
    """Strip/lowercase a string field for policy comparison, or None if malformed.

    Anything that isn't a non-blank string (wrong type, empty, or
    all-whitespace) is malformed input and normalizes to None, which every
    allowlist check below treats as "not allowed" — malformed input must
    fail closed, not fall through as some default value.
    """
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    return normalized or None


def _is_marked_irreversible(payload: dict[str, Any]) -> bool:
    """True if the payload itself declares the action irreversible.

    Independent of ``action_type`` on purpose: even an allowlisted action
    type's payload can be marked ``"irreversible": true``, and this guard
    rejects it the same way, so the boundary doesn't depend solely on the
    action-type allowlist catching every irreversible case in advance.

    Fails closed on presence: only the literal boolean ``False`` is accepted
    as "not irreversible". Any other present value -- ``True``, a string
    (``"on"``, ``"false"``, ``""``, or anything else), a number, ``None``, a
    dict, or a list -- is treated as irreversible. A denylist of "true-ish"
    spellings (the previous approach) always misses one -- e.g. ``"on"``, a
    common HTML-checkbox truthy value that isn't in ``{"true", "1", "yes"}``.
    Absence of the key is checked explicitly so it isn't conflated with a
    present-but-non-``False`` value.
    """
    if "irreversible" not in payload:
        return False
    return payload["irreversible"] is not False


def check_outbox_action(*, consumer: str, action_type: str, payload: dict[str, Any]) -> None:
    """Raise ``PolicyViolation`` if this action is not explicitly allowed; otherwise return None.

    Called by ``Outbox.enqueue`` before any row is written, so a rejected
    action never reaches durable storage — it is only ever visible as an
    audit-log rejection entry.
    """
    normalized_type = _normalize_token(action_type)
    if normalized_type is None or normalized_type not in ALLOWED_ACTION_TYPES:
        raise PolicyViolation(
            f"{consumer!r} may not enqueue {action_type!r}: not in the control plane's action allowlist"
        )

    if _is_marked_irreversible(payload):
        raise PolicyViolation(
            f"{consumer!r} may not enqueue {action_type!r}: payload is marked irreversible, "
            "regardless of action type"
        )

    if normalized_type in GATE_ACTION_TYPES:
        severity = _normalize_token(payload.get("severity"))
        if severity is None or severity not in ALLOWED_GATE_SEVERITIES:
            raise PolicyViolation(
                f"{consumer!r} may not approve a {payload.get('severity')!r} gate from the orchestration "
                "control plane: only low/medium severities have an approval route"
            )
