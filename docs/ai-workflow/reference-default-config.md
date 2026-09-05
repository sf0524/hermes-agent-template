# Reference: Default AI Workflow Configuration

Source: user-provided `default-config.md` in Discord on 2026-09-03.

This is **reference material, not a configuration to apply verbatim**. It informs a purpose-built autonomous-team design for this Hermes/Railway environment. Any executable project configuration, role model, control-plane code, or deployment wiring must be derived from the live Hermes capabilities, persistent-volume/runtime constraints, existing AI-team policy, and the user-approved requirements—not copied from this document.

Key reference decisions to evaluate and adapt:

- `lead_orchestrator` is an autonomous semantic/conflict-resolution role using Codex Sol high, escalating to Sol xhigh for defined cross-role/recovery/high-risk conflicts.
- `runtime_orchestrator` is deterministic and owns mechanical routing, artifact checks, config handling, and routine bookkeeping.
- Conversation-facing handling is deterministic and restricted to typed envelopes; execution control plane performs workspace inspection, guard evaluation, command construction, and role dispatch.
- Human approval gates remain for configuration creation, design/spec, spec/plan, plan/implementation, role substitution, unclear E2E, post-stop extra fixes, high-risk max effort, and Claude auth substitutions.
- Implementation is Claude Code-owned; reviews are independent Codex roles; external `@codex` PR review is advisory but must be triaged.
- Workflow state, specs, plans, and config live under `docs/ai-workflow/`, `docs/specs/`, and `docs/plans/`.

The full original document is retained in Hermes document cache at `/data/.hermes/cache/documents/doc_43785dea5d77_default-config.md` for traceability during this session.
