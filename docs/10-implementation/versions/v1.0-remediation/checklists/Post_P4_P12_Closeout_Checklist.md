# Post-P4 Phase 12 Closeout Checklist

**阶段**: `Post-P4 Phase 12: Execution Economics and Ranking v2`
**状态**: accepted (`2026-03-17`)

---

## 1. Phase Objective

把当前 ranking 从 heuristic penalty-aware score 升级成 dollar-EV / capture-aware / risk-aware 主排序语义。

---

## 2. Delivery Lock

必须完成：

- `weather.weather_execution_priors`
- `ranking_score` v2 semantics
- `why-ranked` decomposition
- `ops_readiness_score` 降级为 gate / tie-breaker

不得顺带做：

- black-box ML ranking
- new UI-only score
- autonomous capital optimizer

---

## 3. Must-Run Tests

- `.venv/bin/python3 -m unittest tests.test_execution_priors_materialization -v`
- `.venv/bin/python3 -m unittest tests.test_ranking_score_v2 -v`
- `.venv/bin/python3 -m unittest tests.test_opportunity_service_ranking_v2 -v`
- `.venv/bin/python3 -m unittest tests.test_ui_why_ranked -v`
- `.venv/bin/python3 -m unittest tests.test_execution_foundation -v`

---

## 4. Required Docs Sync

- `Post_P4_Remediation_Implementation_Plan.md`
- `README.md`
- `Implementation_Index.md`
- `Documentation_Index.md`
- `Execution_Economics_Design.md`

---

## 5. Required Migration Review

必须审查：

- `weather.weather_execution_priors` schema
- materialization cadence / backfill strategy
- no duplication of `trading.*`

---

## 6. Explicit Non-Goals Not Violated

- 没有引入第二套 paper-only ranking
- 没有让 UI 自己算一套 why-ranked
- 没有把 ops readiness 重新当主得分

---

## 7. Acceptance Evidence To Record

- prior materialization sample rows
- before/after ranking examples
- why-ranked JSON example
- targeted test output

---

## 8. Ready To Mark Accepted

- runtime / UI / paper candidate 继续共用一个 `ranking_score`
- ranking 已带 dollar-EV / capture / risk-aware 语义
- execution priors 已进入 opportunity 主链
- 所有必跑测试通过
