---
name: asterion-phase-delivery
description: Use this skill for Asterion phase delivery, milestone implementation, closeout, acceptance, documentation sync, or versioned roadmap work such as future P3/P4 tasks and Asterion v2.0. It tells the agent how to discover the active implementation entrypoint, plan against real code instead of memory, preserve canonical contracts and persistence, implement in bounded increments, run targeted regressions, perform phase acceptance, and keep docs synchronized with code reality.
---

# Asterion Phase Delivery

This skill is for Asterion delivery work only.

Use it when the user is asking for any of these:

- phase or milestone planning
- implementation of an approved phase task
- acceptance, readiness, or closeout
- documentation drift cleanup
- roadmap, README, or index sync
- later phase or version work such as `P3-04`, `P4-01`, or `Asterion v2.0`

Do not use it for unrelated generic coding.

## Required Operating Mode

Follow these rules every time:

1. Discover repo truth before proposing or changing anything.
2. Treat the active implementation plan as the delivery contract.
3. Reuse existing contracts, ledgers, and orchestration paths before adding anything new.
4. Implement only the bounded task in front of you. Do not pull future-phase work forward.
5. Run targeted tests and at least one regression surface relevant to the change.
6. Perform acceptance explicitly. Do not stop at “tests passed”.
7. If docs now drift from code, fix the drift or record it explicitly.

## Discovery Order

Always discover the active entrypoint in this order:

1. Read [references/repo-entrypoints.md](references/repo-entrypoints.md).
2. Identify the active plan from `README.md`, roadmap, implementation index, and the matching phase plan.
3. Read the matching code modules, migrations, and tests before making assumptions.

If docs and code disagree:

- do not silently reconcile them in your head
- state the drift clearly
- base implementation decisions on current code plus the active plan

## Hard Constraints

Read [references/phase-rules.md](references/phase-rules.md) before changing execution, persistence, or phase boundaries.

Non-negotiable rules:

- code and migrations define what is currently real
- tests define the pinned behavior that must not regress
- active phase plans define what the task is allowed to do
- `trading.*` remains the canonical execution ledger
- `runtime.*` remains runtime and audit state
- do not introduce parallel contracts, duplicate ledgers, or sidecar execution paths when a canonical path already exists
- keep paper, readiness, and live boundaries explicit
- agents are support tooling, not canonical execution paths

## Delivery Loop

Execute tasks in this order unless the user explicitly constrains the request:

1. Discover current code and doc reality.
2. Lock the task scope, non-goals, and exit criteria from the active plan.
3. If the task is not decision-complete, produce or refine the implementation plan first.
4. Implement in the existing module boundaries.
5. Add or update regression tests for the new seam or failure mode.
6. Run the smallest test set that proves the change plus the most relevant regression surface.
7. Perform acceptance against phase goals, operator behavior, persistence behavior, and doc state when applicable.
8. Commit only after acceptance is complete and the worktree state is understood.

## Output Rules

Use [references/acceptance-template.md](references/acceptance-template.md) for plan, implementation, and acceptance summaries.

When doc drift may be involved, run [references/doc-drift-checklist.md](references/doc-drift-checklist.md).

Prefer concise, decision-level summaries:

- what changed
- what was validated
- whether the phase task is accepted
- whether the next task or phase can proceed
- what residual risks remain

## Extensibility Rule

Do not hardcode this skill to today’s phase numbering.

The workflow must continue to work when:

- new task ranges appear, such as `P3-04` or `P4-01`
- a later roadmap reorders phases
- `Asterion v2.0` introduces new module boundaries

When that happens, update the references files and active entrypoints. Do not bloat this file with version-specific details.
