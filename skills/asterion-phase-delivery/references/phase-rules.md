# Phase Rules

Use this file for implementation decisions that affect architecture, contracts, persistence, or promotion to the next phase.

## Source-of-truth order

When there is ambiguity, resolve it in this order:

1. Current code and migrations
2. Tests that pin current behavior
3. Active implementation plan
4. Runbooks and closeout checklists
5. README / roadmap / overview docs

If these disagree, do not guess. Call out the drift and proceed based on current code plus the active plan.

## Contract rules

- Reuse existing contracts before adding a new one.
- Do not create a second “internal” order or execution interface when a canonical one already exists.
- Keep handoff seams explicit and reconstructable from persisted state.
- Prefer deterministic identifiers and payloads for replay and audit.

## Persistence rules

- `trading.*` is the canonical execution ledger.
- `runtime.*` is the runtime and audit layer.
- `capability.*` holds capability and execution-context style frozen references.
- Do not create a new schema or table if it duplicates an existing canonical semantic.
- If a new table is necessary, state why the existing ledgers cannot represent the required state.

## Phase-boundary rules

- Paper phases must not perform live trading side effects.
- Do not touch real signer flows, wallet side effects, chain broadcasts, or capital deployment unless the active phase explicitly authorizes them.
- Manual-first and human-in-the-loop constraints remain explicit until the active plan closes them.

## Delivery rules

- Each task should have a clear goal, landing area, tests, and exit criteria.
- Add targeted regression tests for each new seam or failure mode.
- Acceptance is phase-based, not just unit-based.
- Closeout requires both code reality and doc navigation to be consistent.

## Future-version rule

If `Asterion v2.0` introduces new modules or reorganizes phases, preserve these rules unless the new version explicitly replaces them in a canonical plan.
