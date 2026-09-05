# Implementation Plan — Phase 1 Shadow Control-Plane Recorder

**Source requirements:** `docs/specs/2026-09-03-autonomous-team-control-plane.md`
**Architecture status:** Independent review returned `REQUEST_CHANGES` for the full active-control-plane proposal. The reviewer-approved safe first slice below is deliberately read-only and has no dispatch capability.

## Goal

Establish durable, restart-safe, zero-side-effect observation over the verified native Kanban `task_events` SQLite contract. This proves persistence, deduplication/conflict handling, recovery, evidence recording, and explicit state-root isolation before any authenticated Kanban write, webhook ingress, runner, Human Owner decision, update application, or GitHub integration is built.

## Scope / non-goals

In scope:

- Persistent DB explicitly rooted at `/data/.hermes/orchestrator/control-plane.db`, independent of a worker-modified `HERMES_HOME`.
- Append-only observation ledger, durable source cursor, duplicate suppression, conflicting duplicate quarantine, correlation/policy/evidence fields.
- Read-only tail of exact Kanban schema: `task_events(id, task_id, run_id, kind, payload, created_at)`.
- Shadow observer mode under a supervised process/CLI boundary; no public ingress required for this slice.
- Recovery and backup/restore coordination tests using temporary DB fixtures.
- Inventory-only capability records for required Claude/Codex role triples, never dispatch approval.

Out of scope (hard disabled):

- Kanban/API writes, outbox dispatch, role runner execution, Git mutation, GitHub API writes, webhooks, update application, merge, release, deployment, or owner approval UI.
- Any assumed 9119 route, session-token flow, inbound webhook contract, or undocumented CLI flag.

## Tasks

### 1. Add isolated control-plane state module and schema

**Files:** `orchestrator/control_plane/{db,schema,ledger,models}.py`, tests

- Resolve state from `ORCH_STATE_DB`, otherwise `${HERMES_ROOT:-/data/.hermes}/orchestrator/control-plane.db`; reject a relative state path.
- Add idempotent additive migrations for observations, source cursors, duplicate-conflict quarantine, capability inventory/probe evidence, and recovery checkpoints.
- Enforce append-only observation records at the DB level.

**Verification:** fresh boot, repeat boot, relative path rejection, direct SQL update/delete rejection.

### 2. Implement the read-only Kanban tail adapter

**Files:** `orchestrator/control_plane/kanban_tail.py`, tests

- Open source DB in SQLite read-only URI mode.
- Supported source-path contract: every lookup component must be real (not a
  symlink); a non-sticky group/world-writable directory or a group/world-
  writable source file is rejected before SQLite opens it. Existing SQLite
  `-wal`, `-shm`, and `-journal` sidecars receive the same no-symlink,
  non-writable check. The source publisher/administrator that owns the path
  is the explicit trusted principal, so an untrusted filesystem principal
  cannot rename-exchange the source or substitute a symlink in the supported
  deployment contract.
- Do **not** treat `/proc/self/fd/N` as a descriptor-stable SQLite source.
  SQLite reopens it as a path and derives the wrong sidecar location for a
  live WAL. Open the checked normal URI so SQLite retains its standard
  read-only WAL behavior. A hostile trusted publisher or same-UID compromise
  remains outside Phase 1's source contract and must fail an integration gate
  rather than be represented as solved.
- Read only the exact verified columns in ordered, bounded batches.
- Persist cursor only after all corresponding observations commit.
- Deduplicate exact replay; quarantine same source key with conflicting canonical content.
- Never write to Kanban DB.

**Verification:** replay, duplicate, conflict, bounded batch, source DB remains byte-identical.

### 3. Implement shadow observer lifecycle and recovery check

**Files:** `orchestrator/control_plane/observer.py`, `orchestrator/control_plane/cli.py`, tests

- Only `shadow` mode exists; reject all active/dispatch modes.
- Resume from durable cursor after process interruption.
- Record policy version, actor/principal field, correlation ID, canonical evidence hash for every observation.
- Write/read recovery checkpoint with schema/integrity evidence.

**Verification:** kill/restart simulation; dropped-notification equivalent recovered by tailing; zero side effects.

### 4. Add inventory-only capability registry

**Files:** `orchestrator/control_plane/capabilities.py`, tests

- Record exact requested capability identities and probe result metadata.
- All entries are `inventory` or `unverified`; no `approved`/runner APIs exist in this slice.
- Seed documented target identities: `claude-fable-5-1/high`, `claude-sonnet-5/high`, `claude-opus-5/xhigh`, Codex Terra high/xhigh and Sol high counter-review.

**Verification:** no inventory entry can produce a command or side effect.

### 5. Verify and independently review

- Run targeted tests and `git diff --check`.
- Independent Codex review must verify no write-capable routes, runner paths, or update execution paths have entered the slice.

## Rollback

Disable/stop observer process or omit its future supervisor registration. The observer never mutates Kanban or external systems. Its DB is additive evidence under `/data/.hermes/orchestrator/`; retain it on rollback for audit.
