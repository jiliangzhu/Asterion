# P10 Closeout Checklist

**版本**: v1.0
**更新日期**: 2026-03-22
**阶段**: `v2.0 / Phase 10`
**状态**: accepted closeout checklist
**主题**: Deterministic ROI Repair and Execution Intelligence Foundation

---

> 本文件用于收口 `Phase 10` 的 closeout 条件。
> `Phase 10` 已 accepted；本文件保留为更早 accepted tranche 的 closeout checklist。
> umbrella active implementation contract 仍是 [V2_Implementation_Plan.md](../phase-plans/V2_Implementation_Plan.md)。

## 1. Closeout Items

1. guarded-connection `PRAGMA` regression landed
   - `tests.test_execution_foundation`
   - `tests.test_operator_surface_refresh_job`
2. paper / readiness / resolution refresh chain regression coverage landed
   - `tests.test_paper_execution_allocator_integration`
   - `tests.test_cold_path_orchestration`
3. `Home` 主 action queue 已排除 `blocked`
   - `tests.test_home_action_queue_excludes_blocked_items`
   - `tests.test_operator_workflow_acceptance`
4. deterministic execution-intelligence seam landed
   - `tests.test_execution_intelligence_summary`
   - `tests.test_microstructure_ranking_penalty`
5. microstructure summary 已进入 UI persisted surfaces
   - `tests.test_ui_data_access`
   - `tests.test_ui_golden_surfaces`
   - `tests.test_ui_read_model_catalog`
   - `tests.test_ui_lite_builder_registry`
6. execution priors serving grain / fallback hardening landed
   - `tests.test_execution_priors_feature_space`
   - `tests.test_execution_priors_materialization`
   - `tests.test_execution_feedback_loop`
   - `tests.test_ranking_score_v2`
   - `tests.test_ranking_retro_harness`
7. allocator scheduling uplift landed
   - `tests.test_allocator_v1`
   - `tests.test_allocator_v1_p10_scheduling`
   - `tests.test_capital_aware_ranking_acceptance`
   - `tests.test_ui_action_queue_summary`
8. `P10` / `V2` / entry docs accepted wording refreshed
9. `P10_Closeout_Checklist.md` 已进入导航

## 2. Minimum Acceptance

- `PermissionError: Reader connection rejects SQL statement type: PRAGMA` 不再出现
- `Home` 主 action queue 不再展示 `blocked`
- `why_ranked_json` 已出现 microstructure-derived terms
- `runtime.execution_intelligence_summaries` 已 deterministic materialized
- empirical-primary serving 更 sharp，heuristic fallback 占比下降
- allocator 在 scarce budget 下更像 capital scheduler，且 sizing 解释可审计
- `P11` request assembly 所需 inputs 已 persisted，不依赖页面拼装
- `V2` umbrella 与入口导航不再把 `P10` 写成 planned follow-on tranche
- `git diff --check` 通过
