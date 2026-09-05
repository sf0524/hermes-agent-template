# Requirements & Acceptance Criteria — Hermes/Railway Autonomous AI-Development Team

**Status:** READY_FOR_ARCHITECTURE

## Intent

Transform this Railway/Hermes deployment into a durable, continuously maintained autonomous AI-development team. The user-provided default workflow is design input: retain its goals (explicit roles, model/effort discipline, evidence, approvals, recovery, and controlled updates), then tailor them to verified Hermes/Railway capabilities rather than copying its runtime assumptions.

## Reference principles to retain and tailor

- Explicit role separation, staged work, evidence-based handoffs, and bounded autonomy.
- Model/effort selection by task risk, ambiguity, and verification need.
- Durable task state, retries, observability, rollback, and Human Owner approval boundaries.
- `lead_orchestrator` autonomously resolves cross-role semantic conflicts; `runtime_orchestrator` is deterministic and refuses semantic/policy decisions.
- Event-driven orchestration is normal behavior. Cron is restricted to bounded anti-entropy, health checks, reconciliation, and update windows.
- Merge/release, High-risk exceptions, and irreversible decisions remain Human Owner actions. Critical is never auto-accepted.

## Current verified baselines

| Component | Baseline / evidence |
|---|---|
| Hermes Agent | `0.20.1` Docker installation; Gateway live |
| Claude Code | updated `2.1.251 → 2.1.259`; OAuth first-party smoke passed; `fable` alias probe passed |
| Codex CLI | updated `0.151.0 → 0.153.0`; ChatGPT subscription login passed |
| Persistent state | `/data/.hermes` is the durable runtime home |
| Existing updater | weekly Claude/Codex updater exists, but is an insufficient blind updater |
| Orchestration foundation | GitHub PR #1 contains the durable ledger foundation; it is not a deployed capability until Human Owner merge + deployment evidence |
| Kanban integration | loopback 9119 returned `401`; authenticated contract is a discovery blocker, not successful integration |

## Requirements

### REQ-01 — Durable control plane
All coordination state, evidence, policy versions, update records, migration/rollback metadata, and task correlations persist under `/data`. Writes are idempotent, attributable, timestamped, and recovery-safe through container restart/replacement.

### REQ-02 — Hermes-native integration
Use verified native Hermes mechanisms—Kanban, Gateway, profiles, skills, sessions, cron, and webhooks—as their authenticated contracts are discovered. Do not use undocumented endpoints or generic polling as substitutes.

### REQ-03 — Roles and authority
`lead_orchestrator` makes autonomous cross-role semantic decisions within approved policy. `runtime_orchestrator` deterministically validates schema, permissions, dependencies, and transition guards; it escalates ambiguous or policy-changing decisions. Every state transition records actor, source event, policy version, correlation ID, and evidence links.

### REQ-04 — Risk and approval
Classify Low/Medium/High/Critical. No orchestrator can auto-accept/merge/release/complete Critical work. High-risk exceptions, irreversible actions, merge, and release require recorded Human Owner action.

### REQ-05 — Capability registry
The control plane maintains an expiry-bound, probe-backed registry of usable model aliases/full IDs, reasoning efforts, CLIs, skills, plugins, profiles, Gateway routes, and integration contracts. Roles can dispatch only to registry-approved capabilities.

### REQ-06 — Controlled continuous maintenance
Replace blind weekly updates with a durable lifecycle:

1. Discover inventory and release candidates.
2. Assess compatibility/auth/API/skill/plugin/state migration impact.
3. Probe in an isolated or non-production-safe context.
4. Create version-pinned plan with validation and rollback.
5. Apply one bounded component set at a time while preserving prior configuration/artifacts.
6. Validate CLI health, auth, Gateway, sessions, skills/plugins, role capability registry, event/webhook behavior, and persistence.
7. Promote only after gates pass; observe during a bounded period.
8. Roll back binaries/configuration and retain durable evidence on failure.

### REQ-07 — Event-driven recovery
Normal dispatch proceeds from authenticated events. Anti-entropy jobs are low-frequency, rate-limited, source-scoped reconciliation only; ordinary dispatch continues if those jobs are disabled.

### REQ-08 — Integration discovery gates
Before production control-plane use, discover and record:

- authenticated Kanban API/event/write contract, scopes, payloads, idempotency, retry behavior, and token rotation;
- Gateway webhook/event authentication, retries, ordering, replay/dead-letter behavior;
- `/data` backup/restore, ownership, capacity, encryption/secrets, and restart semantics;
- installed profile/skill/plugin/session/cron capabilities;
- PR #1 compatibility, migration, test, rollback, merge, and deployment evidence.

## Acceptance criteria

1. Runtime restart/replacement preserves durable ledger, task/update state, and evidence references; an automated recovery check proves it.
2. A verified event creates/updates exactly one correlated Kanban work item; duplicate delivery causes no duplicate work.
3. `lead_orchestrator` resolves a documented semantic conflict and persists rationale, policy version, and plan.
4. `runtime_orchestrator` rejects and escalates ambiguous, unsupported, unauthorized, or policy-changing input.
5. Kanban `401` is recorded as a discovery condition; integration is not called successful without its authenticated contract.
6. Disabling anti-entropy does not stop normal event-driven dispatch.
7. Every anti-entropy job has a bounded scope and proves reconciliation without becoming the normal dispatcher.
8. A pre-promotion update test validates versions, auth, Gateway, session persistence, enabled skills/plugins, and role model/effort capabilities.
9. A deliberately incompatible tool/plugin/profile update is blocked before production activation and produces rollback-ready evidence.
10. A failed update restores known-good binary/config state without losing durable task evidence.
11. High-risk exception, irreversible action, merge, and release cannot proceed without recorded Human Owner action.
12. Critical cannot be auto-accepted, auto-merged, auto-released, or marked complete.
13. PR #1 is not represented as deployed until merge and deployment evidence exist.
14. Every model alias/full ID/effort used by a role exists in the post-update capability registry.

## Required artifacts

- Architecture and trust-boundary specification.
- Role charter and policy version for lead/runtime/specialist/Human Owner boundaries.
- Capability inventory and compatibility registry with revalidation expiry.
- Kanban/Gateway contracts with authenticated examples and error semantics.
- Event schema, verification, idempotency, retry/dead-letter/replay procedures.
- Update/rollback runbooks and durable evidence ledger.
- Bounded anti-entropy schedule and reconciliation rules.
