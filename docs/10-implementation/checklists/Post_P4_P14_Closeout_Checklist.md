# Post-P4 Phase 14 Closeout Checklist

**阶段**: `Post-P4 Phase 14: Execution Feedback Loop and Cohort Priors`  
**状态**: accepted (`2026-03-17`)

---

## 1. Phase Objective

把 execution science 从 descriptive analytics 升级成可 materialize、可回灌 ranking 的 cohort priors。

---

## 2. Delivery Lock

必须完成：

- nightly cohort priors materialization
- execution science -> opportunity assessment feedback loop
- miss / distortion cohort suppression

不得顺带做：

- new live automation
- new execution ledger
- autonomous strategy mutation

---

## 3. Must-Run Tests

- `.venv/bin/python3 -m unittest tests.test_execution_feedback_loop -v`
- `.venv/bin/python3 -m unittest tests.test_cohort_prior_backfill -v`
- `.venv/bin/python3 -m unittest tests.test_execution_science_to_priors -v`
- `.venv/bin/python3 -m unittest tests.test_execution_science_summary -v`
- `.venv/bin/python3 -m unittest tests.test_post_trade_analytics -v`

---

## 4. Required Docs Sync

- `Post_P4_Remediation_Implementation_Plan.md`
- `Execution_Economics_Design.md`
- `Operator_Console_Truth_Source_Design.md`
- `README.md`

---

## 5. Required Migration Review

必须审查：

- `weather.weather_execution_priors` 扩列是否必要
- `runtime.execution_feedback_materializations` schema
- nightly materialization idempotency

---

## 6. Explicit Non-Goals Not Violated

- 没有让 descriptive analytics 直接越过 canonical ranking seam
- 没有把 UI explanation path 当作 runtime input 真相源
- 没有引入无人值守 live rollout

---

## 7. Acceptance Evidence To Record

- nightly materialization output
- cohort prior examples by market / strategy / wallet
- feedback loop before/after ranking examples
- targeted test output

---

## 8. Ready To Mark Accepted

- execution science 已能反馈进入 ranking
- cohort priors nightly materialization 稳定
- miss / distortion 不再只是 UI 解释层
- 所有必跑测试通过
