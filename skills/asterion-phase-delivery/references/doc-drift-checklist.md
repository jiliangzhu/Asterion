# Doc Drift Checklist

Use this checklist when the task touches project state, active phase status, roadmap meaning, or canonical module boundaries.

## Check for drift

Review the active navigation and implementation docs:

- `README.md`
- `docs/00-overview/DEVELOPMENT_ROADMAP.md`
- `docs/00-overview/Documentation_Index.md`
- `docs/10-implementation/Implementation_Index.md`
- the active phase plan
- the relevant closeout checklist or runbook

Compare those docs against:

- current code modules
- current migrations and tables
- current tests

## Typical drift patterns

- aspirational modules described as already implemented
- old phase numbering still presented as current
- closed phases still marked as active
- persistence docs that no longer match live tables
- implementation entrypoint links that still point to an old phase

## Repair rule

If drift is material to the task:

- fix it in the same change when feasible
- otherwise record it explicitly in the plan or acceptance result

Do not silently continue as if the docs were correct.

## Minimal validation

After doc changes:

- search for outdated phase labels or module names
- ensure indices still point to the active phase plan
- ensure README reflects the real current phase and repo boundaries
- if code changed, ensure docs do not claim more than what is now implemented
