# Acceptance Template

Use these output shapes when reporting planning, implementation, or acceptance work.

## Plan summary

Include:

- current state discovered from code and docs
- target behavior
- non-goals
- key implementation seams
- tests required
- acceptance criteria

## Implementation summary

Include:

- what changed
- where the canonical path now lives
- what stayed intentionally out of scope
- what tests were run
- any residual risk

## Acceptance summary

Prefer this structure:

- `修改摘要`
  - what changed, grouped by behavior not file dump
- `验收结论`
  - pass or fail, with concrete reason
- `验收依据`
  - code path, persistence path, orchestration path, or UI/operator path verified
- `测试`
  - exact commands or test modules run
- `是否可进入下一阶段`
  - yes or no, plus blockers if any
- `剩余风险`
  - only real unresolved items

## Commit / push summary

When code is accepted and committed, report:

- branch
- commit hash
- commit message
- whether push succeeded

Keep summaries concise. Prefer decision-level conclusions over file-by-file narration.
