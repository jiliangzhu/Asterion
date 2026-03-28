# Asterion P10 Implementation Plan

**版本**: v1.0
**更新日期**: 2026-03-22
**阶段**: `v2.0 / Phase 10`
**状态**: accepted closeout record
**主题**: Deterministic ROI Repair and Execution Intelligence Foundation

---

> 本文件保留为 `v2.0 / Phase 10` 的 accepted closeout record。
> umbrella active implementation contract 仍是 [V2_Implementation_Plan.md](./V2_Implementation_Plan.md)。
> 当前仓库阶段状态统一表达为：`P4 accepted; post-P4 remediation accepted; v2.0 implementation active`。
> `Phase 0` 到 `Phase 10` 已 accepted；后续 `Phase 11` 已 accepted closeout；当前还没有比 `Phase 11` 更新的 tranche-specific implementation plan 已打开。
> 本文件保留 assessment-driven 的 planning lineage，主要 planning input 仍来自 [00_0322_Asterion_Assessment.md](../../../../analysis/00_0322_Asterion_Assessment.md)。
> 当前针对 `P10` plan 的代码核对没有发现新的功能性疏漏；这轮收口的剩余工作是文档状态、导航与 closeout checklist 同步。

## 1. Summary

`Phase 10` 不再是 assessment-driven 的未来规划，而是已经落地并完成 closeout 的 pre-agent deterministic ROI repair tranche。
它吸收了两个当时真实存在的当前 HEAD regressions：

- `run_operator_surface_refresh` 的 guarded-connection `PRAGMA` regression
- `Home` action queue 的 `blocked` pollution

并把以下 5 条主线推进到 accepted baseline：

1. execution / operator foundation repair
2. home action queue cleanup
3. deterministic execution-intelligence v1
4. execution priors serving grain + fallback hardening
5. allocator scheduling uplift

固定边界保持不变：

- 不扩大 live boundary
- 不新增页面
- 不新开 `risk.*`
- 不引入新的 `agent.*` 主线能力
- 不改写 `trading.*` canonical execution ledger

当前代码 reality：

- guarded reader connection 已通过只读 `db_path` seam 避开 `PRAGMA database_list`
- `Home` 主 action queue 默认只展示 `ready_now / high_risk / review_required`，`blocked` 下沉到 secondary backlog
- `runtime.execution_intelligence_runs`、`runtime.execution_intelligence_summaries` 与 `ui.market_microstructure_summary` 已 materialized
- `why_ranked_json` / `pricing_context_json` / `ui.market_opportunity_summary` 已进入 microstructure-derived explanations
- execution priors 已按更完整的 feature grain 进行 materialization / lookup discrimination
- allocator 已在现有 rerank / scaling-aware discipline 之上补入 uncertainty / execution-intelligence / concentration sizing tightening

## 2. P9 Baseline vs P10 New Work

当前 HEAD 已有并作为 `P9` carry-forward baseline 保留的内容：

- `weather_operator_surface_refresh`
- `runtime.operator_surface_refresh_runs`
- `ui.surface_delivery_summary`
- `ui.system_runtime_summary`
- `ui_lite > runtime_db > smoke_report` fallback precedence
- delivery-aware operator bucket gating
- `Home / Markets / System` 的 persisted delivery surfaces

`P10` 新增并已 accepted 的内容：

- guarded connection `db_path` seam 与 refresh-path foundation repair
- `Home` 主 action queue 的 `blocked_backlog` 分流
- deterministic `ExecutionIntelligenceSummary`
- `runtime.execution_intelligence_runs`
- `runtime.execution_intelligence_summaries`
- `ui.market_microstructure_summary`
- microstructure-derived ranking explanation terms
- 更细粒度 execution priors serving / fallback discrimination
- uncertainty-aware / execution-intelligence-aware / concentration-aware sizing tightening

当前 closeout 结论：

- 对照 [P10_Implementation_Plan.md](/Users/jayzhu/web3/Asterion/docs/10-implementation/versions/v2.0/phase-plans/P10_Implementation_Plan.md) 与当前代码、migrations、tests 核对，未发现新的功能性漏项
- 当前剩余 drift 已收口为文档状态刷新、导航同步与 closeout checklist 补齐

## 3. Workstreams

### WS1. Execution Foundation Repair

目标：

- 修掉 execution / operator foundation regressions
- 让 surface refresh / replica / lite build / cold-path refresh 链恢复 deterministic

accepted closeout 已包含：

- `GuardedConnection` 增加只读 `db_path` 暴露
- `connect_duckdb()` 构造 guarded connection 时把真实 db path 一起挂载
- `_resolve_connection_db_path(con)` 固定按：
  1. `getattr(con, "db_path", None)`
  2. 仅 raw duckdb connection 才 fallback 到 `PRAGMA database_list`
- `run_operator_surface_refresh()`、`weather_live_prereq_readiness`、`weather_paper_execution`、`weather_resolution_review` 不再因 reader-guarded connection 的 `PRAGMA` 访问失败而被打断

固定 landing areas：

- `dagster_asterion/handlers.py`
- `asterion_core/storage/database.py`
- `asterion_core/ui/surface_refresh_runtime.py`

### WS2. Action Queue and Operator Throughput Cleanup

目标：

- 清理 `Home` action queue 中 `blocked` rows 混入主决策队列的问题

accepted closeout 已包含：

- `Home` 主 action queue 默认只显示：
  - `ready_now`
  - `high_risk`
  - `review_required`
- `blocked` 不再进入主队列
- `blocked_count` metrics 保留
- `blocked` rows 收进 secondary `blocked_backlog` panel
- 不改 `operator_bucket` 语义，不改 `ui.action_queue_summary` 的 persisted classification

固定 landing areas：

- `ui/loaders/home_loader.py`
- `ui/pages/home.py`

### WS3. Deterministic Execution-Intelligence v1

目标：

- 引入 deterministic `execution_intelligence` supporting seam，但不走 `agent.*`

accepted closeout 已包含：

- 新增 Python contract：
  - `ExecutionIntelligenceSummary`
- 新增 runtime materialization seam：
  - `runtime.execution_intelligence_runs`
  - `runtime.execution_intelligence_summaries`
- 新增 UI read model：
  - `ui.market_microstructure_summary`
- 固定最小输出字段：
  - `summary_id`
  - `run_id`
  - `market_id`
  - `side`
  - `quote_imbalance_score`
  - `top_of_book_stability`
  - `book_update_intensity`
  - `spread_regime`
  - `visible_size_shock_flag`
  - `book_pressure_side`
  - `expected_capture_regime`
  - `expected_slippage_regime`
  - `execution_intelligence_score`
  - `reason_codes_json`
  - `source_window_start`
  - `source_window_end`
  - `materialized_at`
- `why_ranked_json` 已出现 microstructure-derived terms
- `pricing_context_json` 已透传 execution-intelligence context
- `ui.market_opportunity_summary` / `Home` / `Markets` 已可消费 persisted execution-intelligence evidence

固定输入来源：

- submit attempts
- external order / fill observations
- fills / orders
- execution priors
- ranking retrospective
- 当前可用的 book / quote evidence

固定边界：

- 不新建 parallel score family
- 不让 microstructure score 变成独立 top-level ranking family
- 不通过 UI 拼接临时 facts 替代 persistence

### WS4. Execution Priors Serving Grain and Fallback Hardening

目标：

- 提升 execution priors 的 serving grain 与 fallback sharpness，而不是重造 priors family

accepted closeout 已包含：

- `execution_prior_key_id(...)` 与 materializer grouping 已真实纳入：
  - `market_age_bucket`
  - `hours_to_close_bucket`
  - `calibration_quality_bucket`
  - `source_freshness_bucket`
- fallback 顺序固定为：
  1. exact market cohort
  2. exact strategy / wallet cohort
  3. station+metric fallback
  4. heuristic fallback
- `prior_quality_status` / `prior_lookup_mode` 继续作为 canonical serving flags
- `service.py` 中 `empirical_primary / blended / heuristic_fallback` 路径不改语义，只提高 empirical-primary 命中率

固定 landing areas：

- `domains/weather/opportunity/execution_priors.py`
- `domains/weather/opportunity/service.py`

### WS5. Allocator Scheduling Uplift

目标：

- 把 allocator 从“已可部署”提升成更像 capital scheduler 的 sizing seam

accepted closeout 已包含：

- 在现有 rerank / scaling-aware discipline 之上补入：
  - `uncertainty_sizing_penalty`
  - `execution_intelligence_penalty`
  - 更强的 concentration-aware sizing tightening
- 新调节项继续通过：
  - `budget_impact`
  - `AllocationDecision`
  - `ui.action_queue_summary`
  来解释，不允许黑箱化
- `budget_impact["sizing"]` 已持久化：
  - `uncertainty_sizing_penalty`
  - `execution_intelligence_penalty`
  - `sizing_reason_codes`
- `capital_scaling_reason_codes` 已补入：
  - `uncertainty_sizing_tighten`
  - `execution_intelligence_tighten`
  - `concentration_sizing_tighten`

固定 landing areas：

- `asterion_core/risk/allocator_v1.py`
- `ui.action_queue_summary`
- `ui.market_opportunity_summary`

## 4. Public Interfaces and Persistence

`P10` accepted closeout 新增：

- `ExecutionIntelligenceSummary`
- `runtime.execution_intelligence_runs`
- `runtime.execution_intelligence_summaries`
- `ui.market_microstructure_summary`

`P10` accepted closeout 扩展：

- `why_ranked_json`
- `pricing_context_json`
- `ui.market_opportunity_summary`
- `ui.action_queue_summary`
- `OpportunityAssessment`
- `AllocationDecision`
- `budget_impact`

固定不变：

- `trading.*` 仍是 canonical execution ledger
- `runtime.*` 只承接 runtime / audit / serving state
- `ranking_score` 仍是唯一主排序字段
- `agent.*` 不进入 `P10`

## 5. Tests and Acceptance

当前 accepted closeout 已锁住的核心测试面：

- `tests.test_execution_foundation`
- `tests.test_operator_surface_refresh_job`
- `tests.test_paper_execution_allocator_integration`
- `tests.test_cold_path_orchestration`
- `tests.test_home_action_queue_excludes_blocked_items`
- `tests.test_operator_workflow_acceptance`
- `tests.test_ui_pages`
- `tests.test_execution_intelligence_summary`
- `tests.test_microstructure_ranking_penalty`
- `tests.test_ui_data_access`
- `tests.test_ui_golden_surfaces`
- `tests.test_ui_read_model_catalog`
- `tests.test_ui_lite_builder_registry`
- `tests.test_migrations`
- `tests.test_execution_priors_feature_space`
- `tests.test_execution_priors_materialization`
- `tests.test_execution_feedback_loop`
- `tests.test_ranking_score_v2`
- `tests.test_ranking_retro_harness`
- `tests.test_allocator_v1`
- `tests.test_allocator_v1_p10_scheduling`
- `tests.test_capital_aware_ranking_acceptance`
- `tests.test_ui_action_queue_summary`

固定 acceptance：

- `PRAGMA` regression 被修掉，paper / readiness / resolution refresh 链恢复稳定
- `Home` 主队列不再展示 `blocked`
- `why_ranked_json` 可解释 microstructure-derived terms
- execution-intelligence summary 已 deterministic materialized
- empirical-primary serving 命中率提升，fallback 更清晰
- allocator 在 scarce budget 下的 sizing / ordering 更像 capital scheduler
- 当前系统在没有 agent 的情况下，operator surface、ranking economics、allocation throughput 都显著更稳
- `P11` 所需 execution-intelligence inputs 已 deterministic materialized
- `git diff --check` 通过

## 6. Explicit P11 Deferrals

`P10` 固定不做：

- `Opportunity Triage / Execution Intelligence Agent`
- `agent.*` advisory request / output / evaluation path
- replay / evaluation 驱动的 triage overlay UI
- 任何 live side-effect shortcut

这些保留给 [P11_Implementation_Plan.md](./P11_Implementation_Plan.md)。
