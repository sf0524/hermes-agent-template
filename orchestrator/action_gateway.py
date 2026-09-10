"""Target-scoped, default-deny action gateway for GitHub / non-release Railway actions.

This is the only place in the orchestrator that may ever call out to GitHub
or Railway. Two structural boundaries hold regardless of anything a caller
passes in:

- **Category A** — source mutation of the native Kanban task table, waiver
  grants, role overrides, and trading actions — has no dispatch route at
  all. It is checked first, unconditionally, before any grant lookup: a
  real-looking Human Owner grant or an otherwise well-formed call cannot talk
  a caller past this boundary. There is deliberately no code path in this
  module that can call ``transport`` for a Category A action type.
- **Category B** — the explicit, target-scoped GitHub/Railway action
  allowlist below — requires a durable Human Owner grant (``human_owner.py``)
  that exactly names the action's scope, subject, type, and payload (bound
  by its canonical digest) — a grant approving one comment body can't be
  replayed to authorize a different, never-approved one. ``_record_intent``
  validates that match and then inserts the ``action_intents`` row in one
  transaction; schema.py's v6 triggers derive the grant's consumption from
  that insert itself (not a second, separate UPDATE this module issues), so
  a crash between "grant consumed" and "intent recorded" is structurally
  impossible rather than merely a two-step convention. Every action type not
  explicitly listed — including any release/deploy/merge/force-push verb —
  is rejected fail-closed by the same default-deny discipline
  ``orchestrator/policy.py`` uses for the outbox.

The transport that actually sends a Category B action anywhere is injected;
the shipped default (``default_transport``) always raises
``TransportDisabledError`` — no real external call is wired into this
deployment. Tests inject a test-double transport to exercise the positive
path. Each idempotency key's transport call happens at most once: the
pending -> dispatching status transition is claimed atomically (an
``UPDATE ... WHERE status = 'pending'``, whose row-lock serializes
concurrent claimants), and a dispatched or terminally-failed intent is never
retried. ``reconcile_pending`` resumes any intent a crashed prior process
left at ``pending`` — durably consumed grant, transport never attempted —
without ever re-consuming its grant. A crash strictly between that claim's
commit and the transport call's own outcome being recorded leaves the row at
``dispatching``; this module does not claim to auto-recover that narrow
window, the same kind of documented boundary ``control_plane/kanban_tail.py``
draws around its own trust contract.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from orchestrator.audit import AuditLog
from orchestrator.db import transaction
from orchestrator.health import redact
from orchestrator.human_owner import (
    GrantAlreadyConsumedError,
    HumanOwnerGrants,
    canonical_payload_digest,
)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


# Source mutation, waivers, role overrides, trading: these action types
# never have a dispatch route, ever — not behind a grant, not behind a stub
# transport. Checked first and unconditionally in ``dispatch()``.
CATEGORY_A_ACTION_TYPES = frozenset(
    {
        "kanban_task_write",
        "grant_waiver",
        "override_role",
        "execute_trade",
    }
)

# The only action types this gateway will ever dispatch, each pinned to the
# scope prefix its target must be namespaced under. Anything not listed here
# — a typo, an unnormalized variant, a release/deploy/merge/force-push verb
# — is rejected fail-closed by the default-deny check below.
CATEGORY_B_ACTION_SCOPES: dict[str, str] = {
    "github_comment": "github",
    "github_add_label": "github",
    "railway_restart_service": "railway",  # explicitly not railway_deploy/_release/_promote
}


class CategoryAForbiddenError(PermissionError):
    """Raised for any Category A action type — always, unconditionally, no-call."""


class UnknownActionTypeError(PermissionError):
    """Raised for any action type not on the Category B allowlist — default-deny."""


class GrantMismatchError(PermissionError):
    """Raised when no durable Human Owner grant exactly matches this action's scope/subject/type."""


class IdempotencyKeyConflictError(PermissionError):
    """Raised when a replayed idempotency key's request fields don't exactly match the original.

    Reusing a key is only safe as a no-op replay of the *same* request. A
    replay that changes any immutable field -- consumer, action_type,
    target_scope, subject, payload, grant_id, or causal_event_id -- must
    fail closed rather than silently reuse the original intent or, worse,
    consume a different grant.
    """


class TransportDisabledError(RuntimeError):
    """Raised by the shipped default transport: no real external call is wired in."""


def default_transport(intent: "ActionIntentRecord") -> None:
    raise TransportDisabledError(
        f"real transport is disabled in this deployment; {intent.action_type!r} was not sent anywhere"
    )


def _normalize(action_type: Any) -> str | None:
    if not isinstance(action_type, str):
        return None
    normalized = action_type.strip().lower()
    return normalized or None


@dataclass(frozen=True)
class ActionIntentRecord:
    idempotency_key: str
    consumer: str
    action_type: str
    target_scope: str
    subject: str
    payload: dict[str, Any]
    grant_id: str
    status: str
    causal_event_id: str | None
    created_at: str
    updated_at: str
    dispatched_at: str | None

    @classmethod
    def _from_row(cls, row: sqlite3.Row) -> "ActionIntentRecord":
        return cls(
            idempotency_key=row["idempotency_key"],
            consumer=row["consumer"],
            action_type=row["action_type"],
            target_scope=row["target_scope"],
            subject=row["subject"],
            payload=json.loads(row["payload"]),
            grant_id=row["grant_id"],
            status=row["status"],
            causal_event_id=row["causal_event_id"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            dispatched_at=row["dispatched_at"],
        )


class ActionGateway:
    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        grants: HumanOwnerGrants | None = None,
        transport: Callable[[ActionIntentRecord], None] | None = None,
    ):
        self._conn = conn
        self._grants = grants or HumanOwnerGrants(conn)
        self._transport = transport or default_transport

    def dispatch(
        self,
        *,
        idempotency_key: str,
        consumer: str,
        action_type: Any,
        target_scope: str,
        subject: str,
        payload: dict[str, Any],
        grant_id: str,
        causal_event_id: str | None = None,
        now: str | None = None,
    ) -> ActionIntentRecord:
        moment = now or _utcnow()
        normalized_type = _normalize(action_type)

        if normalized_type is not None and normalized_type in CATEGORY_A_ACTION_TYPES:
            with transaction(self._conn) as conn:
                AuditLog(conn).record(
                    actor=consumer,
                    action="action.category_a_rejected",
                    subject_id=idempotency_key,
                    detail={"action_type": normalized_type},
                    now=moment,
                )
            raise CategoryAForbiddenError(
                f"{action_type!r} is a Category A action (source mutation/waiver/role override/trading); "
                "it has no dispatch route, ever"
            )

        if normalized_type is None or normalized_type not in CATEGORY_B_ACTION_SCOPES:
            with transaction(self._conn) as conn:
                AuditLog(conn).record(
                    actor=consumer,
                    action="action.rejected",
                    subject_id=idempotency_key,
                    detail=redact({"action_type": repr(action_type), "reason": "not on the Category B allowlist"}),
                    now=moment,
                )
            raise UnknownActionTypeError(f"{action_type!r} is not an allowed action type: default-deny")

        expected_scope_prefix = CATEGORY_B_ACTION_SCOPES[normalized_type]
        if target_scope.split(":", 1)[0] != expected_scope_prefix:
            with transaction(self._conn) as conn:
                AuditLog(conn).record(
                    actor=consumer,
                    action="action.rejected",
                    subject_id=idempotency_key,
                    detail={
                        "action_type": normalized_type,
                        "reason": "target_scope prefix mismatch",
                        "target_scope": target_scope,
                    },
                    now=moment,
                )
            raise GrantMismatchError(
                f"{normalized_type!r} must be scoped under {expected_scope_prefix!r}, got {target_scope!r}"
            )

        existing = self.get(idempotency_key)
        if existing is not None:
            if (
                existing.consumer != consumer
                or existing.action_type != normalized_type
                or existing.target_scope != target_scope
                or existing.subject != subject
                or canonical_payload_digest(existing.payload) != canonical_payload_digest(payload)
                or existing.grant_id != grant_id
                or existing.causal_event_id != causal_event_id
            ):
                with transaction(self._conn) as conn:
                    AuditLog(conn).record(
                        actor=consumer,
                        action="action.idempotency_conflict",
                        subject_id=idempotency_key,
                        detail={"reason": "replay does not exactly match the original request"},
                        now=moment,
                    )
                raise IdempotencyKeyConflictError(
                    f"idempotency key {idempotency_key!r} was already used for a different request"
                )
            return self._claim_and_dispatch(existing, now=moment)

        existing = self._record_intent(
            idempotency_key=idempotency_key,
            consumer=consumer,
            action_type=normalized_type,
            target_scope=target_scope,
            subject=subject,
            payload=payload,
            grant_id=grant_id,
            causal_event_id=causal_event_id,
            now=moment,
        )
        return self._claim_and_dispatch(existing, now=moment)

    def _record_intent(
        self,
        *,
        idempotency_key: str,
        consumer: str,
        action_type: str,
        target_scope: str,
        subject: str,
        payload: dict[str, Any],
        grant_id: str,
        causal_event_id: str | None,
        now: str,
    ) -> ActionIntentRecord:
        """Validate the grant, then insert action_intents — one transaction.

        schema.py's v6 ``action_intents_consumes_grant`` trigger derives the
        grant's ``consumed_at``/``consumed_by_idempotency_key`` from the
        insert below itself, in the same transaction, rather than this
        method issuing a second, separate UPDATE. A crash between "grant
        consumed" and "intent recorded" is therefore structurally
        impossible, not just a two-step convention this method has to get
        right every time.
        """
        with transaction(self._conn) as conn:
            # Re-checked inside the write lock: a concurrent caller may have
            # raced this same idempotency_key to the insert.
            row = conn.execute(
                "SELECT * FROM action_intents WHERE idempotency_key = ?", (idempotency_key,)
            ).fetchone()
            if row is not None:
                existing = ActionIntentRecord._from_row(row)
                if (
                    existing.consumer != consumer
                    or existing.action_type != action_type
                    or existing.target_scope != target_scope
                    or existing.subject != subject
                    or canonical_payload_digest(existing.payload) != canonical_payload_digest(payload)
                    or existing.grant_id != grant_id
                    or existing.causal_event_id != causal_event_id
                ):
                    raise IdempotencyKeyConflictError(
                        f"idempotency key {idempotency_key!r} was already used for a different request"
                    )
                return existing

            grant_row = conn.execute(
                "SELECT * FROM human_owner_grants WHERE grant_id = ?", (grant_id,)
            ).fetchone()
            if grant_row is None:
                raise GrantMismatchError(f"no human owner grant {grant_id!r} exists to authorize this action")
            if (
                grant_row["scope"] != target_scope
                or grant_row["subject"] != subject
                or grant_row["action_type"] != action_type
                or grant_row["payload_digest"] != canonical_payload_digest(payload)
            ):
                raise GrantMismatchError(
                    f"grant {grant_id!r} does not exactly match "
                    f"scope={target_scope!r} subject={subject!r} action_type={action_type!r} "
                    "and/or the exact payload it approved"
                )
            if grant_row["consumed_at"] is not None:
                # Reached only for a *different* idempotency key: a matching
                # key would already have returned via the existing-row check
                # above. schema.py's action_intents_requires_fresh_grant
                # trigger would reject the insert below anyway, but this
                # raises the same typed error consume_grant_in_transaction
                # used to, instead of a raw sqlite3.IntegrityError.
                raise GrantAlreadyConsumedError(
                    f"grant {grant_id!r} was already consumed by idempotency key "
                    f"{grant_row['consumed_by_idempotency_key']!r}"
                )

            conn.execute(
                """
                INSERT INTO action_intents
                    (idempotency_key, consumer, action_type, target_scope, subject, payload, grant_id,
                     status, causal_event_id, created_at, updated_at, dispatched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, NULL)
                """,
                (
                    idempotency_key,
                    consumer,
                    action_type,
                    target_scope,
                    subject,
                    json.dumps(payload),
                    grant_id,
                    causal_event_id,
                    now,
                    now,
                ),
            )
            # The insert above just derived the grant's consumption (schema.py
            # v6's action_intents_consumes_grant trigger) — record the audit
            # trail for both effects of that one insert, in the same
            # transaction.
            AuditLog(conn).record(
                actor="human_owner_grants",
                action="grant.consumed",
                subject_id=grant_id,
                detail={"idempotency_key": idempotency_key},
                now=now,
            )
            AuditLog(conn).record(
                actor=consumer,
                action="action.intent_recorded",
                subject_id=idempotency_key,
                detail={"action_type": action_type, "target_scope": target_scope, "grant_id": grant_id},
                now=now,
            )
            inserted = conn.execute(
                "SELECT * FROM action_intents WHERE idempotency_key = ?", (idempotency_key,)
            ).fetchone()
            return ActionIntentRecord._from_row(inserted)

    def _claim_and_dispatch(
        self,
        record: ActionIntentRecord,
        *,
        now: str | None = None,
        transport: Callable[[ActionIntentRecord], None] | None = None,
    ) -> ActionIntentRecord:
        if record.status in ("dispatched", "failed"):
            return record

        moment = now or _utcnow()
        with transaction(self._conn) as conn:
            cur = conn.execute(
                "UPDATE action_intents SET status = 'dispatching', updated_at = ? "
                "WHERE idempotency_key = ? AND status = 'pending'",
                (moment, record.idempotency_key),
            )
            claimed = cur.rowcount == 1

        if not claimed:
            return self.get(record.idempotency_key)

        active_transport = transport or self._transport
        claimed_record = self.get(record.idempotency_key)
        try:
            active_transport(claimed_record)
        except Exception as exc:
            self._finish(record.idempotency_key, "failed", detail=redact({"reason": str(exc)}))
            raise
        return self._finish(record.idempotency_key, "dispatched")

    def _finish(self, idempotency_key: str, status: str, *, detail: dict[str, Any] | None = None) -> ActionIntentRecord:
        moment = _utcnow()
        with transaction(self._conn) as conn:
            row = conn.execute(
                "SELECT * FROM action_intents WHERE idempotency_key = ?", (idempotency_key,)
            ).fetchone()
            conn.execute(
                "UPDATE action_intents SET status = ?, updated_at = ?, dispatched_at = ? WHERE idempotency_key = ?",
                (status, moment, moment if status == "dispatched" else row["dispatched_at"], idempotency_key),
            )
            AuditLog(conn).record(
                actor=row["consumer"],
                action=f"action.{status}",
                subject_id=idempotency_key,
                detail=detail,
                now=moment,
            )
            updated = conn.execute(
                "SELECT * FROM action_intents WHERE idempotency_key = ?", (idempotency_key,)
            ).fetchone()
            return ActionIntentRecord._from_row(updated)

    def get(self, idempotency_key: str) -> ActionIntentRecord | None:
        row = self._conn.execute(
            "SELECT * FROM action_intents WHERE idempotency_key = ?", (idempotency_key,)
        ).fetchone()
        return ActionIntentRecord._from_row(row) if row is not None else None

    def reconcile_pending(
        self, *, transport: Callable[[ActionIntentRecord], None] | None = None
    ) -> list[ActionIntentRecord]:
        """Resume every intent left at ``pending``, and surface every intent
        stuck at ``dispatching``, by a crashed prior process.

        A ``pending`` row never re-consumes a grant — that already happened
        durably, in the same transaction that recorded the intent, before
        this method could ever see it. It only (re-)attempts the transport
        call, via the same atomic claim ``_claim_and_dispatch`` uses for a
        fresh dispatch, so two reconcilers racing the same intent still call
        transport at most once between them.

        A ``dispatching`` row means a prior process crashed strictly between
        claiming it and recording the transport call's own outcome, so
        whether transport was ever attempted -- let alone whether it
        succeeded -- is unknown. Calling transport again here could
        double-send, so this method never does: it leaves the row exactly as
        found for explicit manual recovery and records a redacted audit
        event instead.
        """
        rows = self._conn.execute(
            "SELECT * FROM action_intents WHERE status IN ('pending', 'dispatching') ORDER BY created_at ASC"
        ).fetchall()
        results = []
        for row in rows:
            record = ActionIntentRecord._from_row(row)
            if record.status == "dispatching":
                self._record_recovery_required(record)
                results.append(record)
                continue
            results.append(self._claim_and_dispatch(record, transport=transport))
        return results

    def _record_recovery_required(self, record: ActionIntentRecord) -> None:
        with transaction(self._conn) as conn:
            AuditLog(conn).record(
                actor="action_gateway",
                action="action.recovery_required",
                subject_id=record.idempotency_key,
                detail=redact(
                    {
                        "reason": "ambiguous_dispatch_outcome",
                        "action_type": record.action_type,
                        "target_scope": record.target_scope,
                    }
                ),
            )


class GitHubActionInterface:
    """Target-scoped, default-deny action interface for GitHub."""

    SCOPE_PREFIX = "github"
    ALLOWED_ACTION_TYPES = frozenset({"github_comment", "github_add_label"})

    def __init__(self, gateway: ActionGateway):
        self._gateway = gateway

    def dispatch(
        self,
        *,
        idempotency_key: str,
        consumer: str,
        action_type: Any,
        repo: str,
        subject: str,
        payload: dict[str, Any],
        grant_id: str,
        causal_event_id: str | None = None,
        now: str | None = None,
    ) -> ActionIntentRecord:
        normalized = _normalize(action_type)
        if normalized is None or normalized not in self.ALLOWED_ACTION_TYPES:
            raise UnknownActionTypeError(f"{action_type!r} is not an allowed GitHub action type: default-deny")
        return self._gateway.dispatch(
            idempotency_key=idempotency_key,
            consumer=consumer,
            action_type=normalized,
            target_scope=f"{self.SCOPE_PREFIX}:{repo}",
            subject=subject,
            payload=payload,
            grant_id=grant_id,
            causal_event_id=causal_event_id,
            now=now,
        )


class RailwayActionInterface:
    """Target-scoped, default-deny action interface for non-release Railway actions.

    Deliberately offers no deploy/release/promote verb: those remain Human
    Owner actions taken outside this control plane, never an action type
    this gateway could dispatch.
    """

    SCOPE_PREFIX = "railway"
    ALLOWED_ACTION_TYPES = frozenset({"railway_restart_service"})

    def __init__(self, gateway: ActionGateway):
        self._gateway = gateway

    def dispatch(
        self,
        *,
        idempotency_key: str,
        consumer: str,
        action_type: Any,
        project: str,
        subject: str,
        payload: dict[str, Any],
        grant_id: str,
        causal_event_id: str | None = None,
        now: str | None = None,
    ) -> ActionIntentRecord:
        normalized = _normalize(action_type)
        if normalized is None or normalized not in self.ALLOWED_ACTION_TYPES:
            raise UnknownActionTypeError(f"{action_type!r} is not an allowed Railway action type: default-deny")
        return self._gateway.dispatch(
            idempotency_key=idempotency_key,
            consumer=consumer,
            action_type=normalized,
            target_scope=f"{self.SCOPE_PREFIX}:{project}",
            subject=subject,
            payload=payload,
            grant_id=grant_id,
            causal_event_id=causal_event_id,
            now=now,
        )
