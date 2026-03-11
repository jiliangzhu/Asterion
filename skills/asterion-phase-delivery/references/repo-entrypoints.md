# Repo Entrypoints

Use this file to discover where the active Asterion work is anchored before planning or implementing anything.

## Primary navigation

- `README.md`
  - Current project status
  - Current implementation entrypoint
  - High-level code layout
- `docs/00-overview/DEVELOPMENT_ROADMAP.md`
  - Phase sequence and current roadmap framing
- `docs/00-overview/Documentation_Index.md`
  - Overview doc navigation
- `docs/10-implementation/Implementation_Index.md`
  - Implementation doc navigation

## Delivery entrypoints

- `docs/10-implementation/phase-plans/`
  - Phase implementation plans
  - Prefer the currently active plan referenced by README and roadmap
- `docs/10-implementation/checklists/`
  - Closeout and acceptance checklists
- `docs/10-implementation/runbooks/`
  - Operator and runtime procedures

## Current code surfaces

- `asterion_core/`
  - Canonical contracts, runtime, execution, risk, journal, monitoring, UI lite DB, storage
- `dagster_asterion/`
  - Cold-path orchestration shell and handlers
- `domains/`
  - Domain logic, currently including weather flows
- `agents/`
  - Review and support agents, not canonical execution
- `sql/migrations/`
  - Schema truth
- `tests/`
  - Regression truth and current system expectations

## Discovery pattern

For any task:

1. Read README and the current implementation plan.
2. Read the most relevant checklist or runbook if acceptance, operations, or readiness is involved.
3. Inspect the matching code modules and tables.
4. Inspect the tests that already pin the behavior.

## If phases or versions evolve

If the repo later introduces `P4`, `P5`, or `Asterion v2.0`, keep using the same discovery pattern:

- find the active plan
- confirm the matching code and tests
- treat old plans as historical context, not current truth
