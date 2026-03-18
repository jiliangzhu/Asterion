# Post-P4 Phase 15 Closeout Checklist

**阶段**: `Post-P4 Phase 15: UI Read-Model and Truth-Source Refactor`  
**状态**: accepted (`2026-03-17`)

---

## 1. Phase Objective

把 `ui_lite_db.py` / `ui.data_access.py` 的维护热点拆分成 versioned builders + loaders + truth-source checks，同时保持一个 UI truth-source。

---

## 2. Delivery Lock

必须完成：

- split builders
- split loaders
- `ui.read_model_catalog`
- `ui.truth_source_checks`
- golden tests / truth-source checker

不得顺带做：

- front-end framework migration
- new UI backend service
- second UI data source

---

## 3. Must-Run Tests

- `.venv/bin/python3 -m unittest tests.test_ui_read_model_catalog -v`
- `.venv/bin/python3 -m unittest tests.test_truth_source_checks -v`
- `.venv/bin/python3 -m unittest tests.test_ui_loader_contracts -v`
- `.venv/bin/python3 -m unittest tests.test_ui_golden_surfaces -v`
- `.venv/bin/python3 -m unittest tests.test_ui_lite_builder_registry -v`

---

## 4. Required Docs Sync

- `Post_P4_Remediation_Implementation_Plan.md`
- `Implementation_Index.md`
- `Documentation_Index.md`
- `UI_Read_Model_Design.md`
- `Operator_Console_Truth_Source_Design.md`

---

## 5. Required Migration Review

- 默认不做 canonical DB migration
- 若新增 UI lite internal tables，确认只属于 `ui.*`
- 检查 meta / validation / refresh loop 不回退

---

## 6. Explicit Non-Goals Not Violated

- 没有引入第二套 loader truth-source
- 没有让 page 层重算 canonical ranking / boundary logic
- 没有拆坏现有 UI lite single-source 架构

---

## 7. Acceptance Evidence To Record

- builder registry dump
- read model catalog sample rows
- truth source check sample rows
- golden test output
- targeted test output

---

## 8. Ready To Mark Accepted

- `ui_lite_db.py` / `ui.data_access.py` 已按职责拆分
- read-model schema versioning 可见
- truth-source drift 可自动检查
- 所有必跑测试通过
