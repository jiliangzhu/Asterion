# Post-P4 Phase 11 Closeout Checklist

**阶段**: `Post-P4 Phase 11: Operator Truth-Source and Surface Hardening`  
**状态**: accepted (`2026-03-17`)

---

## 1. Phase Objective

把 operator console 的 truth-source 从“文案大体一致”升级成“sidebar / row badges / primary score / analysis doc banner”可被统一校验。

---

## 2. Delivery Lock

必须完成：

- dynamic sidebar truth-source
- row-level source badges
- `ranking_score` 唯一 primary score baseline
- Agents / System operator-first 收口
- analysis docs banner baseline

不得顺带做：

- 新排序模型
- 新页面
- execution behavior changes

---

## 3. Must-Run Tests

- `.venv/bin/python3 -m unittest tests.test_operator_truth_source -v`
- `.venv/bin/python3 -m unittest tests.test_ui_source_badges -v`
- `.venv/bin/python3 -m unittest tests.test_sidebar_boundary_summary -v`
- `.venv/bin/python3 -m unittest tests.test_analysis_doc_banner_policy -v`
- `.venv/bin/python3 -m unittest tests.test_phase9_wording -v`

---

## 4. Required Docs Sync

- `Post_P4_Remediation_Implementation_Plan.md`
- `README.md`
- `Asterion_Project_Plan.md`
- `DEVELOPMENT_ROADMAP.md`
- `Documentation_Index.md`
- `Implementation_Index.md`
- `Operator_Console_Truth_Source_Design.md`

---

## 5. Required Migration Review

- 默认不做 canonical migration
- 若扩 `ui.*` projection 列，确认仅限 UI lite / read model 范围

---

## 6. Explicit Non-Goals Not Violated

- 没有新增第二套排序 contract
- 没有改 execution gate
- 没有让 analysis docs 升格成 implementation truth-source

---

## 7. Acceptance Evidence To Record

- sidebar truth-source screenshot / dump
- row badge examples for canonical / fallback / stale / degraded
- analysis doc banner examples
- targeted test output

---

## 8. Ready To Mark Accepted

- sidebar 文案不再靠硬编码 optimistic copy
- row-level degraded/fallback truth-source 可见
- `ranking_score` 成为唯一 primary score baseline
- 所有必跑测试通过
