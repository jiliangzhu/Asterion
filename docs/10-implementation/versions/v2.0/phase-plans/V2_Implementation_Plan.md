# Asterion V2.0 Implementation Plan

**版本**: v1.0
**更新日期**: 2026-03-21
**阶段**: `v2.0`
**状态**: active implementation contract
**定位**: Weather-first 盈利强化

---

> 当前仓库阶段状态统一表达为：`P4 accepted; post-P4 remediation accepted; v2.0 implementation active`。
> 本文件现在是当前唯一 active implementation entry。
> `P4` 与 [Post_P4_Remediation_Implementation_Plan.md](../../v1.0-remediation/phase-plans/Post_P4_Remediation_Implementation_Plan.md) 继续保留为 historical accepted records。
> 最近 accepted tranche record 是 [P8_Implementation_Plan.md](./P8_Implementation_Plan.md)：`Phase 8 — Calibration Hard Gates and Scaling-Aware Capital Discipline`。
> 当前 tranche-specific implementation plan 是 [P9_Implementation_Plan.md](./P9_Implementation_Plan.md)：`Phase 9 — Operator Surface Delivery and Throughput Scaling`。

## 1. Summary

`v2.0` 不是新的 live boundary 扩张 phase，也不是重新发明 execution spine 的版本。
它的主任务是把当前已经 accepted 的 constrained execution foundation，推进成更接近“可稳定、规模化、高置信赚钱”的 Weather-first 系统。

本计划固定采用以下基线：

- **主定位**：`Weather-first 盈利强化`
- **主目标**：提升“可稳定、规模化、高置信赚钱”的能力
- **主输入**：以 [Asterion_Deep_Audit_and_v2_Planning_Blueprint_Revised.md](../../../../analysis/Asterion_Deep_Audit_and_v2_Planning_Blueprint_Revised.md) 为主；[Asterion_Deep_Audit_and_v2_Planning_Blueprint.md](../../../../analysis/Asterion_Deep_Audit_and_v2_Planning_Blueprint.md) 只保留仍成立的高价值判断
- **范围边界**：不把 live boundary 扩张当成 `v2.0` 主线，不引入新的 `risk.*` schema 作为默认答案
- **最近 accepted tranche**：`Phase 8 — Calibration Hard Gates and Scaling-Aware Capital Discipline`
- **当前 tranche 状态**：`Phase 9 — Operator Surface Delivery and Throughput Scaling` in progress，且 `core implemented / closeout pending`

## 2. 当前代码现实与 Why Now

当前仓库已经具备以下 accepted 基线：

- constrained real submit boundary 已经收口到 attestation v2 / single-use / auditable boundary proof
- `ranking_score` 已成为唯一主排序字段，`why_ranked_json` 已进入主链
- calibration v2、threshold probability quality、execution feedback loop 已接进 weather 机会主链
- UI truth-source、source badge、read-model catalog、truth-source checks 已具备 contract 基线
- `P4` 与 post-P4 remediation 已经作为 historical accepted records 收口

`v2.0` 当前真正要解决的，不再是“有没有基础设施”，而是：

- truth-source / read-model contract 仍有 delivery drift 风险
- execution economics 虽已进入 deployable / feedback-backed 主链，但 empirical-primary 与 retrospective validation 仍需继续收口
- allocator / sizing / capital discipline baseline 已落地，但 allocator v2 explanation / acceptance 仍需继续收口
- calibration freshness / materialization ops 的基础运营闭环已落地，`P8` 已把 hard gate 与 scaling-aware capital discipline 推进到主链并完成 closeout
- operator throughput 还不够高，但 `P9` 主干已进入代码；当前剩余工作固定收口为 delivery contract closeout、acceptance hardening 与 final phase closeout sync

当前 `Phase 0` 到 `Phase 6` 已 accepted，`v2.0` 当前真正要解决的 next-step bottlenecks 固定为：

- true allocation-aware rerank 已进入主链，`P7` closeout 已 accepted
- calibration hard gate 与 scaling-aware capital discipline 已进入主链，`P8` 已 accepted，并保留为最近 accepted tranche record
- empirical-primary economics 已进入主链，retrospective validation seam 已落地并继续靠 acceptance 守住
- startup / UI / docs / supporting design 已完成 first-wave cleanup，仍需靠 tests 与 checklist 继续守住

## 3. Active Inputs and Frozen References

`v2.0` 的 source-of-truth 优先级固定为：

1. 当前代码、migrations、tests
2. 本文件
3. frozen supporting designs
4. historical accepted phase plans / closeout / runbooks
5. overview / roadmap / README

本阶段直接依赖的 active / frozen references：

- active implementation contract：
  - [V2_Implementation_Plan.md](./V2_Implementation_Plan.md)
- current tranche-specific implementation plan：
  - [P9_Implementation_Plan.md](./P9_Implementation_Plan.md)
- most recent accepted tranche record：
  - [P8_Implementation_Plan.md](./P8_Implementation_Plan.md)
- earlier accepted tranche records：
  - [P7_Implementation_Plan.md](./P7_Implementation_Plan.md)
  - [P6_Implementation_Plan.md](./P6_Implementation_Plan.md)
- primary analysis input：
  - [Asterion_Deep_Audit_and_v2_Planning_Blueprint_Revised.md](../../../../analysis/Asterion_Deep_Audit_and_v2_Planning_Blueprint_Revised.md)
- retained high-value historical analysis：
  - [Asterion_Deep_Audit_and_v2_Planning_Blueprint.md](../../../../analysis/Asterion_Deep_Audit_and_v2_Planning_Blueprint.md)
- frozen supporting designs：
  - [Controlled_Live_Boundary_Design.md](../../../../30-trading/Controlled_Live_Boundary_Design.md)
  - [Execution_Economics_Design.md](../../../../30-trading/Execution_Economics_Design.md)
  - [Forecast_Calibration_v2_Design.md](../../../../40-weather/Forecast_Calibration_v2_Design.md)
  - [Operator_Console_Truth_Source_Design.md](../../../../50-operations/Operator_Console_Truth_Source_Design.md)
  - [UI_Read_Model_Design.md](../../../../20-architecture/UI_Read_Model_Design.md)
- historical accepted records：
  - [P4_Implementation_Plan.md](../../v1.0/phase-plans/P4_Implementation_Plan.md)
  - [P4_Closeout_Checklist.md](../../v1.0/checklists/P4_Closeout_Checklist.md)
  - [Post_P4_Remediation_Implementation_Plan.md](../../v1.0-remediation/phase-plans/Post_P4_Remediation_Implementation_Plan.md)

## 4. V2.0 Principles

`v2.0` 固定遵守以下原则：

1. **Weather-first**
   当前版本只围绕 Weather domain 提升盈利与运营质量，不同时展开 Tech / Crypto 新 domain。
2. **Profit Engine over Boundary Expansion**
   `v2.0` 的主线是赚钱能力增强，而不是扩大 live scope。
3. **No Parallel Contracts**
   继续复用 `ranking_score`、`why_ranked_json`、`OpportunityAssessment`、`ExecutionContext`、`CanonicalOrderContract` 等现有 seams。
4. **Persistence Discipline Stays Intact**
   `trading.*` 仍是 canonical execution ledger，`runtime.*` 仍是 runtime / audit 层；不默认新开 `risk.*` schema。
5. **Allocator Follows Ranking, Not Replaces It**
   allocator seam 固定接在 `ranking_score` 之后、execution gate 之前，不重写 ranking 主链。
6. **Calibration Must Become Ops**
   calibration v2 不再只是模型能力，而要成为可调度、可观测、可解释的运营能力。
7. **Operator Surface Must Increase Throughput**
   页面和 read models 的目标是提升 actionability，而不是增加工程噪音。
8. **Delivery Contracts Must Be Testable**
   truth-source、read-model、loader、score label、source badge 继续保持可测试 contract。

## 5. Fixed Boundaries and Persistence Baseline

### 5.1 保持不变

- `ranking_score` 继续作为唯一主排序字段
- `why_ranked_json` 继续作为排序解释主 contract
- 当前 constrained live boundary 继续保持 `manual-only / default-off / constrained`
- 不把 boundary 扩张当成 active `v2.0` 主 workstream
- 不引入 unattended live / unrestricted live

### 5.2 计划新增或扩展的 interfaces / types

`v2.0` 默认预留以下扩展：

- `OpportunityAssessment`
  - `recommended_size`
  - `allocation_status`
  - `budget_impact`
- `why_ranked_json`
  - sizing / capital discipline explanation
  - pre-allocation vs post-allocation score context
- allocator seam
  - 新模块，接在 `ranking_score` 之后、execution gate 之前

### 5.3 计划新增或扩展的持久化方向

默认先使用现有 persistence discipline，不直接新开 `risk.*`：

- `runtime.capital_allocation_runs`
- `runtime.allocation_decisions`
- `runtime.position_limit_checks`
- `runtime.calibration_profile_materializations`
- `trading.allocation_policies`
- `trading.position_limit_policies`

当前 `v2.0 Phase 2` 已经明确把 policy truth 固定落入 `trading.*`，而 runtime decision / audit artifacts 固定落入 `runtime.*`。
本文件当前**不**预设新的 `risk.*` schema。

## 6. Active Workstreams

### WS0. Truth-Source and Delivery Baseline

目标：

- 收口当前 HEAD 的 truth-source / read-model / startup drift
- 修掉会破坏 v2.0 基线的已知 failing surfaces
- 把 docs、UI、startup 和 tests 锁到同一阶段表达

核心 landing areas：

- `asterion_core/ui/surface_truth_shared.py`
- `ui/app.py`
- `ui/pages/home.py`
- `start_asterion.sh`
- `asterion_core/ui/ui_lite_db.py`
- `asterion_core/ui/read_model_registry.py`
- truth-source doc tests

明确不做：

- 不做 UI 大改版
- 不做 read-model service 化
- 不扩 live boundary

### WS1. Execution Economics and Ranking Refinement

目标：

- 把当前 ranking v2 从“已经合理”推进到“更接近真钱资本部署价值”
- 扩 execution priors / feedback 特征空间
- 强化 replay / retrospective acceptance

核心 landing areas：

- `domains/weather/opportunity/service.py`
- `domains/weather/opportunity/execution_feedback.py`
- `domains/weather/opportunity/execution_priors.py`
- `domains/weather/pricing/engine.py`
- `asterion_core/runtime/strategy_engine_v3.py`

明确不做：

- 不重写 ranking 主链
- 不并行引入第二套 score contract

### WS2. Allocation, Sizing, and Capital Discipline

目标：

- 把“机会排序”推进成“资本部署建议”
- 把 budget / concentration / inventory constraints 接入 operator decision
- 让 actionable opportunity 具备明确 `recommended_size`

核心 landing areas：

- `asterion_core/risk/portfolio_v3.py`
- 新 allocator module
- `asterion_core/runtime/strategy_engine_v3.py`
- `asterion_core/execution/execution_gate_v1.py`
- `ui/pages/home.py`
- `ui/pages/markets.py`

明确不做：

- 不引入平行 `risk.*` ledger
- 不做复杂多资产风险系统

### WS3. Calibration Ops and Forecast Quality Operations

目标：

- 把 calibration v2 从“模型能力”推进成“日常运营能力”
- 让 stale / sparse / degraded calibration 对 operator 可见
- 让 calibration refresh 不再是黑箱人工流程

核心 landing areas：

- `domains/weather/forecast/calibration.py`
- `domains/weather/forecast/adapters.py`
- `domains/weather/forecast/service.py`
- `dagster_asterion/job_map.py`
- `dagster_asterion/handlers.py`
- `ui/pages/markets.py`
- `ui/pages/system.py`

明确不做：

- 不抢做更重的新模型体系
- 不把 calibration workstream 变成纯研究项目

### WS4. Operator Workflow and Read-Model Hardening

目标：

- 提高 operator throughput
- 让 action queue、cohort history、feedback / calibration / source truth 真正合流
- 继续降低 UI-lite / loader / builder 的 delivery risk

核心 landing areas：

- `ui/pages/home.py`
- `ui/pages/markets.py`
- `ui/pages/execution.py`
- `ui/data_access.py`
- `asterion_core/ui/ui_lite_db.py`
- `asterion_core/ui/read_model_registry.py`

明确不做：

- 不增加更多页面数量
- 不重写前端框架
- 不过早引入新的 domain complexity

## 7. Phase Breakdown

### Phase 0. Stabilize Current HEAD

**状态**

- accepted

**目标**

- 修复当前 HEAD 的显式 drift 和 failing surfaces
- 建立 `v2.0` 的 delivery baseline

**主要交付物**

- 修 `ui.daily_review_input.item_id`
- 收口 UI / startup / shared constants 的 phase split-brain
- 把 truth-source tests 从“自洽”升级成“与 docs 对齐”
- full UI-lite build acceptance baseline

**当前代码现实**

- `ui.daily_review_input.item_id` 已补入 `ui_lite_db` projection
- `tests.test_execution_foundation` 当前已回到 green baseline
- 当前 `v2.0` 实施入口与 truth-source / read-model acceptance 面已经重新对齐

**明确不做**

- 不改 live boundary contract
- 不做 allocator
- 不扩 domain

**planned interfaces / tables / jobs**

- 无新的 canonical tables
- 继续复用 `ui.read_model_catalog`、`ui.truth_source_checks`
- 继续复用现有 UI/startup surfaces，只修 contract drift

**测试与验收**

- `tests.test_execution_foundation`
  - first-wave acceptance 必须覆盖当前两处 `ui.daily_review_input` 失败面
- `tests.test_operator_truth_source`
- `tests.test_ui_read_model_catalog`
- `tests.test_truth_source_checks`
- `tests.test_phase9_wording`
- `tests.test_p4_plan_docs`

**exit criteria**

- 当前代表性测试集全绿
- repo 不再存在入口 docs 与 UI/startup phase split-brain
- UI-lite build 不再因 registry-builder drift 失败

### Phase 1. Strengthen Profit Engine

**状态**

- accepted

**目标**

- 提升 ranking economics 的真实度
- 强化 replay / retrospective 比较能力
- 继续把 feedback-backed ranking 做得更真钱化

**主要交付物**

- 扩 execution priors 特征空间
- refine ranking economics
- replay-based comparison harness
- 稳定 `why_ranked_json` contract

**当前代码现实**

- `weather.weather_execution_priors` 已扩到 Phase 1 feature space：
  - `station_id`
  - `metric`
  - `market_age_bucket`
  - `hours_to_close_bucket`
  - `calibration_quality_bucket`
  - `source_freshness_bucket`
  - `submit_latency_ms_p50/p90`
  - `fill_latency_ms_p50/p90`
  - `realized_edge_retention_bps_p50/p90`
- `ExecutionPriorSummary` 与 `ExecutionPriorKey` 已同步扩展，`load_execution_prior_summary(...)` 已改成：
  - exact market prior
  - station/metric fallback
  - heuristic fallback
- ranking v2 仍保持单主链 contract，但已新增：
  - `latency_penalty`
  - `tail_slippage_penalty`
  - `edge_retention_penalty`
  - `quality_confidence_multiplier`
- `why_ranked_json` 现已稳定写出：
  - `prior_lookup_mode`
  - `prior_feature_scope`
  - `retrospective_baseline_version`
  - 上述新的 penalty / multiplier 字段
- retrospective harness 已落地为：
  - `domains/weather/opportunity/ranking_retrospective.py`
  - `runtime.ranking_retrospective_runs`
  - `runtime.ranking_retrospective_rows`
  - manual job `weather_ranking_retrospective_refresh`
- 本阶段 migrations：
  - `0023_weather_execution_priors_phase1.sql`
  - `0024_runtime_ranking_retrospective.sql`

**明确不做**

- 不重写 ranking 主链
- 不引入 parallel economics engine

**planned interfaces / tables / jobs**

- 优先扩现有 `ExecutionPriorSummary` / `why_ranked_json`
- 如需新增 serving features，优先扩 `weather.weather_execution_priors`
- retrospective artifacts 优先落 `runtime.*`

**已落地的 interfaces / tables / jobs**

- contracts
  - `ExecutionPriorKey`
  - `ExecutionPriorSummary`
  - `RankingRetrospectiveRun`
  - `RankingRetrospectiveRow`
  - `RankingRetrospectiveSummary`
- tables
  - `weather.weather_execution_priors` 扩列
  - `runtime.ranking_retrospective_runs`
  - `runtime.ranking_retrospective_rows`
- jobs
  - `weather_ranking_retrospective_refresh`

**测试与验收**

- ranking replay regression tests
- feedback prior refinement tests
- ranking explanation contract tests
- retrospective windows correlation acceptance

**当前验收面**

- `tests.test_execution_foundation`
- `tests.test_execution_priors_feature_space`
- `tests.test_ranking_score_v2`
- `tests.test_execution_feedback_loop`
- `tests.test_opportunity_service_ranking_v2`
- `tests.test_forecast_replay`
- `tests.test_ranking_retro_harness`
- `tests.test_why_ranked_contract_phase1`
- `tests.test_execution_priors_materialization`
- `tests.test_ui_data_access`
- `tests.test_cold_path_orchestration`
- `tests.test_migrations`

**exit criteria**

- retrospective windows 上，top-ranked opportunity 的 capture / realized outcome 相关性优于当前 baseline
- `ops_tie_breaker` 不再覆盖显著更优的经济机会
- `why_ranked_json` 稳定可被 UI / tests 直接消费

### Phase 2. Add Capital Discipline

**状态**

- accepted

**目标**

- 把 ranking 结果变成资金部署建议
- 让 portfolio constraints 真正进入 operator decision

**主要交付物**

- allocator v1
- `recommended_size`
- budget / concentration / inventory-aware decision path
- UI action queue 初版

**当前代码现实**

- allocator seam 已落地为：
  - `asterion_core/risk/allocator_v1.py`
- policy truth 已固定落在：
  - `trading.allocation_policies`
  - `trading.position_limit_policies`
- runtime audit artifacts 已固定落在：
  - `runtime.capital_allocation_runs`
  - `runtime.allocation_decisions`
  - `runtime.position_limit_checks`
- paper execution path 现在会在 `run_strategy_engine(...)` 之后、`build_trade_ticket(...)` 之前调用 allocator
- paper path 已真实消费 `recommended_size`
  - approved / resized decisions 进入 order path
  - blocked / policy_missing decisions fail-closed，并留下 allocation audit artifacts
- constrained live 仍保持 display-only，不自动消费 allocator size
- `OpportunityAssessment` 已扩为：
  - `recommended_size`
  - `allocation_status`
  - `budget_impact`
- `why_ranked_json` 现已稳定写出：
  - `recommended_size`
  - `allocation_status`
  - `budget_impact`
  - `allocation_decision_id`
  - `policy_id`
  - `policy_version`
- `ui.market_opportunity_summary` 已透传 allocation overlay
- Home 已增加 Action Queue 初版，Markets 已显示 sizing / budget impact / allocation diagnostics
- orchestration 已新增 manual job：
  - `weather_allocation_preview_refresh`
- 本阶段 migrations：
  - `0025_trading_allocation_policies.sql`
  - `0026_runtime_allocation_artifacts.sql`

**明确不做**

- 不引入 `risk.*` schema
- 不做复杂多域 risk engine
- 不让 constrained live 自动消费 allocator size

**planned interfaces / tables / jobs**

- `OpportunityAssessment`
  - `recommended_size`
  - `allocation_status`
  - `budget_impact`
- `why_ranked_json`
  - sizing / capital discipline explanation
- new allocator seam
- `runtime.capital_allocation_runs`
- `runtime.allocation_decisions`
- `runtime.position_limit_checks`

**已落地的 interfaces / tables / jobs**

- contracts
  - `CapitalAllocationRun`
  - `AllocationDecision`
  - `PositionLimitCheck`
  - `OpportunityAssessment.recommended_size`
  - `OpportunityAssessment.allocation_status`
  - `OpportunityAssessment.budget_impact`
- tables
  - `trading.allocation_policies`
  - `trading.position_limit_policies`
  - `runtime.capital_allocation_runs`
  - `runtime.allocation_decisions`
  - `runtime.position_limit_checks`
- jobs
  - `weather_allocation_preview_refresh`

**测试与验收**

- budget / concentration / inventory-aware sizing tests
- allocator runtime persistence tests
- UI recommended size acceptance tests
- decision monotonicity tests

**当前验收面**

- `tests.test_allocator_v1`
- `tests.test_allocation_preview_persistence`
- `tests.test_paper_execution_allocator_integration`
- `tests.test_execution_foundation`
- `tests.test_ui_data_access`
- `tests.test_ui_pages`
- `tests.test_cold_path_orchestration`
- `tests.test_migrations`
- `tests.test_weather_pricing`
- `tests.test_live_prereq_read_model`

**exit criteria**

- 每个 actionable opportunity 都能得到 size-aware recommendation
- 超预算 / 超 concentration / inventory constrained 时会显式降级或 block
- Home / Markets 能展示 recommended size 和 budget impact

### Phase 3. Operationalize Calibration

**状态**

- accepted

**目标**

- 把 calibration v2 推进成稳定运营能力
- 把 freshness / materialization status 变成 operator 可消费信息

**主要交付物**

- calibration refresh 从 manual-only 推进到默认 scheduled
- calibration materialization status
- stale / sparse / degraded calibration surfaces

**当前代码现实**

- `weather_forecast_calibration_profiles_v2_refresh` 已升级为 scheduled cold-path job
- default schedule key 已固定为：
  - `weather_forecast_calibration_profiles_v2_nightly`
- `runtime.calibration_profile_materializations` 已落地为 runtime audit table
- `ForecastDistributionSummaryV2` / `pricing_context_json` / `assessment_context_json` / `why_ranked_json` 已带：
  - `calibration_freshness_status`
  - `profile_materialized_at`
  - `profile_window_end`
  - `profile_age_hours`
- `ui.calibration_health_summary` 已从 sample-only 摘要切到 profile-v2 + freshness 摘要
- Markets / System 已能显示 calibration freshness 与 latest materialization diagnostics

**明确不做**

- 不抢做更重的新模型体系
- 不把 calibration workstream 扩成新研究平台

**planned interfaces / tables / jobs**

- 现有 calibration v2 contracts 继续保留
- `runtime.calibration_profile_materializations`
- `weather_forecast_calibration_profiles_v2_refresh`
- `weather_forecast_calibration_profiles_v2_nightly`
- extend UI diagnostics around freshness / sample sufficiency / degradation

**测试与验收**

- `tests.test_calibration_materialization_status`
- `tests.test_calibration_freshness_penalty`
- `tests.test_calibration_ops_ui_surfaces`
- `tests.test_calibration_profile_v2`
- `tests.test_weather_pricing`
- `tests.test_opportunity_service_ranking_v2`
- `tests.test_cold_path_orchestration`
- `tests.test_ui_data_access`
- `tests.test_ui_pages`
- `tests.test_migrations`

**exit criteria**

- calibration refresh 不再是纯人工触发黑箱
- stale / missing / sparse profile 在 UI 上显式可见
- calibration freshness 进入日常运营闭环

### Phase 4. Increase Operator Throughput and Delivery Stability

**状态**

- accepted

**目标**

- 让 operator 能在更稳定的 read-model / UI surface 上处理更多机会
- 提高 actionability，降低 delivery drift

**主要交付物**

- richer action queue
- cohort history surfaces
- feedback / calibration / source badge 的更显式组合展示
- read-model / loader acceptance 收口

**明确不做**

- 不扩 unrestricted / unattended live
- 不增加新 domain complexity
- 不重写前端框架

**当前代码现实**

- `ui.action_queue_summary` 与 `ui.cohort_history_summary` 已作为 UI-only read models 落地，并已注册进 `ui.read_model_catalog` / `ui.truth_source_checks`
- `Home` 当前直接读取 persisted `ui.action_queue_summary`
  - 首页主队列只展示：
    - `ready_now`
    - `high_risk`
    - `review_required`
  - bucket metrics 已显式展示：
    - `ready_now_count`
    - `high_risk_count`
    - `review_required_count`
    - `blocked_count`
    - `research_only_count`
- `Markets` 已接入 persisted operator workflow overlay
  - 每个 market row 现在显式带出：
    - `operator_bucket`
    - `queue_reason_codes`
    - `cohort_history`
- `Execution` 已新增 `Cohort History` 视图，直接消费 Phase 1 retrospective artifacts 的 UI read-model 化结果
- `asterion_core/ui/builders/opportunity_builder.py`
  - 已真实承载 `ui.action_queue_summary`
- `asterion_core/ui/builders/execution_builder.py`
  - 已真实承载 `ui.cohort_history_summary`
- `ui/loaders/home_loader.py`
  - 已真实承载 `load_home_decision_snapshot()` / `build_ops_console_overview()`
- `ui/loaders/markets_loader.py`
  - 已真实承载 `load_market_chain_analysis_data()`
- `ui/loaders/execution_loader.py`
  - 已真实承载 `load_execution_console_data()`
- `ui/data_access.py`
  - 已降成 backward-compatible façade + shared helpers
- UI builder contract 还顺手修掉了一个真实 bug：
  - `src.runtime.*` 三段路径在 builder table discovery 下已不再被误判为缺表
- action queue / cohort history 排序语义现在固定为：
  - queue priority `1 -> 5` 升序
  - `ranking_decile` 较优的 cohort 先展示，而不是反序展示

**planned interfaces / tables / jobs**

- 新增 `ui.action_queue_summary`
- 新增 `ui.cohort_history_summary`
- 继续复用：
  - `ui.market_opportunity_summary`
  - `ui.execution_science_summary`
  - `runtime.ranking_retrospective_*`
- 不新增 canonical migration；新增表只在 UI lite build 内创建
- 继续复用现有 loader façade，不引入第二套 truth-source

**测试与验收**

- `tests.test_ui_action_queue_summary`
- `tests.test_ui_cohort_history_summary`
- `tests.test_operator_workflow_acceptance`
- `tests.test_ui_loader_contracts`
- `tests.test_ui_data_access`
- `tests.test_ui_pages`
- `tests.test_ui_phase4_console`
- `tests.test_ui_read_model_catalog`
- `tests.test_truth_source_checks`
- `tests.test_ui_golden_surfaces`
- `tests.test_ui_lite_builder_registry`
- `tests.test_ranking_retro_harness`

**exit criteria**

- operator 可以快速区分：
  - 高质量机会
  - 高风险机会
  - review-required 机会
  - research-only 机会
- UI / read-model delivery drift 明显下降
- 首页与 Markets 页更接近 action surface，而不只是 score table
- 当前 targeted acceptance 面已覆盖：
  - persisted action queue
  - persisted cohort history
  - registry / truth-source coverage
  - loader façade parity
  - Home / Markets / Execution page smoke

### Phase 5. Agent Rationalization and Resolution Review Closure

**状态**

- accepted

**目标**

- 删除低 ROI 的 `Rule2Spec` / `Data QA` LLM review chain
- 保留 deterministic validation 语义
- 保留 `Resolution Agent`，并把它升级成带 operator accept/reject/defer 闭环的 constrained decision seam

**主要交付物**

- `domains/weather/spec/rule2spec_validation.py`
- `domains/weather/forecast/replay_validation.py`
- `resolution.operator_review_decisions`
- resolution-only readiness gate
- `Agents` 页面 resolution queue + operator action entry
- `ui.proposal_resolution_summary.effective_redeem_status`

**明确不做**

- 不让任何 agent 进入 execution path
- 不让 LLM 输出直接触发 redeem / dispute side effects
- 不新增页面，不扩大 live boundary

**当前代码现实**

- `weather_rule2spec_review` 与 `weather_data_qa_review` 已从 job map / handler wiring 中移除
- readiness 已从 generic three-agent gate 收口为 `resolution_review_surface`
- deterministic validation 已固定为：
  - `Rule2SpecValidationResult`
  - `ReplayQualityValidationResult`
- `Markets` 继续展示 `rule2spec_*` / `data_qa_*` 字段，但当前语义已切换为 deterministic validation outputs
- `Agents` 页面当前只展示 `Resolution Review`
  - operator 可对 proposal 执行：
    - `Accept`
    - `Reject`
    - `Defer`
- `ui.proposal_resolution_summary` 已显式带出：
  - `latest_agent_invocation_id`
  - `latest_agent_verdict`
  - `latest_recommended_operator_action`
  - `latest_settlement_risk_score`
  - `latest_operator_review_status`
  - `latest_operator_action`
  - `effective_redeem_status`
- `effective_redeem_status` 当前固定规则为：
  - agent 建议 `hold_redeem` / `manual_review` / `consider_dispute` 且尚未被 operator accept/reject 覆盖时：
    - `pending_operator_review`
  - operator `accepted + ready_for_redeem_review`：
    - `ready_for_redeem_review`
  - operator `accepted + hold_redeem/manual_review/consider_dispute`：
    - `blocked_by_operator_review`
  - operator `rejected`：
    - 回退到 deterministic `redeem_readiness_suggestions` baseline
  - operator `deferred`：
    - `pending_operator_review`

**planned interfaces / tables / jobs**

- 新增 `resolution.operator_review_decisions`
- 继续保留：
  - `agent.invocations`
  - `agent.outputs`
  - `agent.reviews`
  - `agent.evaluations`
  - `weather_resolution_review`
- 删除：
  - `weather_rule2spec_review`
  - `weather_data_qa_review`
- 不新增 `agent.*` 记录给 deterministic validation

**测试与验收**

- `tests.test_weather_agents`
- `tests.test_rule2spec_validation`
- `tests.test_replay_quality_validation`
- `tests.test_resolution_operator_review_closure`
- `tests.test_resolution_review_ui_actions`
- `tests.test_cold_path_orchestration`
- `tests.test_ui_pages`
- `tests.test_ui_phase4_console`
- `tests.test_ui_read_model_catalog`
- `tests.test_truth_source_checks`

**exit criteria**

- `Rule2Spec` 与 `Data QA` 不再依赖 LLM client
- readiness 不再要求三份 agent jobs
- `Resolution Agent` 建议可被 operator 接纳 / 驳回 / 延后
- `effective_redeem_status` 会随 operator review state 变化
- active docs / UI 不再把系统写成 three-agent execution-adjacent stack

### Phase 6. Capital-Aware Ranking and Deployable Action Queue

**状态**

- accepted baseline

**目标**

- 把当前系统从“已有 ranking + allocator + action queue 初版”推进到“按 deployable value 排序并直接服务 operator 资本部署”

**主要交付物**

- allocator self-sorting / invariant hardening
- `runtime.allocation_decisions` 扩展 deployable metrics 与 `base_ranking_score`
- allocation / operator deployment surfaces 上的 deployable-value-first `ranking_score`
- `ui.action_queue_summary` / `ui.market_opportunity_summary` 的 deployable pnl / binding-limit explanation
- startup / docs / truth-source cleanup around the current tranche

**实施文档**

- [P6_Implementation_Plan.md](./P6_Implementation_Plan.md)

**明确不做**

- 不扩 live boundary
- 不优先拉入 `execution economics v2` 与 `calibration scaling discipline` 全量内容
- 不新开 `risk.*`

### Phase 7. Deployable Rerank, Allocator v2, and Execution Economics Closure

**状态**

- accepted closeout baseline

**主题**

- allocator pass-1 structural preview + rerank
- allocator v2 capital deployment closure
- richer empirical execution priors
- feedback closure uplift
- retrospective-backed economics validation

**实施文档**

- [P7_Implementation_Plan.md](./P7_Implementation_Plan.md)

**明确不做**

- 不把 calibration freshness hard gate 主体拉进当前 tranche
- 不新增 `trading.capital_budget_policies`
- 不新开 `risk.*`

### Phase 8. Calibration Hard Gates and Scaling-Aware Capital Discipline

**状态**

- accepted

**主题**

- calibration freshness / regime / profile quality deeper actionability gating
- scaling-aware capital discipline follow-on
- accepted closeout 已包含：
  - acceptance hardening
  - fallback / degraded source truth-source hardening
  - `P8` / umbrella wording refresh

### Phase 9. Operator Surface Delivery and Throughput Scaling

**状态**

- current tranche in progress
- core implemented / closeout pending

**主题**

- operator surface refresh orchestration
- persisted surface delivery contract
- fallback governance and delivery-aware operator gating
- throughput scaling inside existing pages

**实施文档**

- [P9_Implementation_Plan.md](./P9_Implementation_Plan.md)

## 8. Non-Goals

`v2.0` 当前明确不做：

- 不扩大 current constrained live boundary
- 不进入 unattended live / unrestricted live
- 不引入新的 `risk.*` schema 作为默认持久化答案
- 不重写 canonical execution spine
- 不并行发明第二套 ranking / allocation / truth-source contract
- 不同时展开 Tech / Crypto 新 domain
- 不以“更多 UI 页面数量”替代 operator throughput 提升

## 9. 与历史 P4 / Post-P4 Remediation 的关系

`v2.0` 与历史阶段的关系固定如下：

- `P4`
  - 保留为 historical accepted live-prereq closeout record
  - 不再承担 active implementation entry 身份
- post-P4 remediation
  - 保留为 `Phase 5` 到 `Phase 15` 的 historical accepted remediation record
  - 继续提供 frozen supporting designs 与 accepted seam baseline
- `v2.0`
  - 从上述历史 accepted records 之上继续开发
  - 不重做它们已经 accepted 的工作
  - 只把仍然影响“稳定、规模化、高置信赚钱”的缺口拉入新版本主线

## 10. Acceptance and Exit Rule

`v2.0` 的开发接受以下总验收规则：

1. 每个 phase 都必须同时满足：
   - code / migrations / tests reality
   - operator surface truthfulness
   - docs navigation consistency
2. 每个 phase 的 acceptance 不能只停在 unit tests；必须包含至少一个贴近 operator / retrospective / orchestration 的回归面。
3. `v2.0` 的任何 planned persistence 扩展，必须先满足：
   - 不破坏 `trading.*` / `runtime.*` 纪律
   - 不引入 parallel truth-source
4. `v2.0` 的任何赚钱能力增强，都必须优先落在：
   - ranking economics
   - sizing / capital discipline
   - calibration ops
   - operator throughput
   而不是 boundary scope expansion。

## 11. Default Next Action

当前默认状态：

- `Phase 0` 到 `Phase 8` 已 accepted
- `Phase 9` 当前已打开 tranche-specific implementation plan
- 当前默认下一步是继续按 [P9_Implementation_Plan.md](./P9_Implementation_Plan.md) 收口 delivery contract closeout / acceptance
