# Asterion Profitability Reality, Pre-Agent ROI Fixes, and Highest-ROI Agent Plan

## 1. Executive Summary

我重新基于当前仓库 HEAD 做了定向代码审查，并重点复核了与“赚钱能力”最相关的代码、migrations、调度定义与代表性测试。

这次结论可以先压缩成一句话：

**Asterion 当前已经明显跨过“没有基础设施”的阶段，但仍没有跨过“稳定、规模化、高置信赚钱”的门槛。当前最大的矛盾，不再是有没有 execution spine，而是 economics、allocation、operator throughput、feedback closure 这四条盈利主链还没有完全收口。**

对你给出的上一版五个主要矛盾，我的当前结论是：

1. **真钱排序还不够真钱**：**部分解决，但没有彻底解决**。`ranking_score` 已经是真正主排序字段，`ranking_v2` 也已经把 execution priors / feedback penalty / capital efficiency 接进主链；但很多核心输入仍然是粗粒度 heuristic，尤其是 fill/slippage/depth。
2. **执行经济学还不够经验化**：**明显改善，但仍是当前最主要的盈利瓶颈之一**。`weather_execution_priors` 已经真实 materialize 并回灌排序，但 serving 粒度和 fallback 仍然偏粗，微观结构层还没建起来。
3. **operator 注意力分配还不够高效**：**仍未解决，而且当前有明确回归**。`Home` action queue 还把 `blocked` 项混进来；这不仅是 UX bug，也是直接稀释 operator 注意力的盈利问题。
4. **execution feedback 还没变成足够强的盈利闭环**：**已经不再是“只有 dashboard 没有主链”了，但闭环仍不够强**。反馈 penalty 已进入 `ranking_score`，但 retrospective 仍偏 manual / nightly，离“快速修正赚钱错误”的闭环还差一步。
5. **capital deployment / sizing discipline 仍是更大的潜在 ROI 点**：**这条已经从“缺失”变成“已落地但仍值得继续投资”**。allocator v1、capital budget policy、position limits、deployable rerank 都已经是真实现；但 capital deployment 仍是序贯、线性、policy-heavy 的分配器，还不是更高质量的 capital scheduler。

在引入任何新 agent 之前，需要完成下面工作：

- 先修 **execution foundation regression**（当前 paper execution / allocator integration 有真实回归）
- 先修 **operator action queue 污染问题**
- 先做 **deterministic microstructure / execution intelligence layer**
- 先把 **execution priors serving 粒度与 cadence** 做强
- 先把 **allocator 从 ‘能分配’ 提升到 ‘更会分配’**

在这些做好之后，接入一个 ROI 最高、又不会破坏当前 constrained / auditable 体系的 agent：

## **Opportunity Triage / Execution Intelligence Agent**

但它必须是一个 **advisory-only、基于 canonical facts、结构化输出、可回放评估** 的 agent，而不是交易执行 agent。

---

## 2. 审查方法与复核范围

### 2.1 我实际复核了哪些入口

我优先复核了这些 active docs：

- `README.md`
- `AGENTS.md`
- `docs/00-overview/Documentation_Index.md`
- `docs/00-overview/Version_Index.md`
- `docs/00-overview/versions/v2.0/Asterion_Project_Plan.md`
- `docs/00-overview/versions/v2.0/DEVELOPMENT_ROADMAP.md`
- `docs/10-implementation/Implementation_Index.md`
- `docs/10-implementation/versions/v2.0/phase-plans/V2_Implementation_Plan.md`
- supporting designs：
  - `docs/30-trading/Controlled_Live_Boundary_Design.md`
  - `docs/30-trading/Execution_Economics_Design.md`
  - `docs/40-weather/Forecast_Calibration_v2_Design.md`
  - `docs/50-operations/Operator_Console_Truth_Source_Design.md`
  - `docs/20-architecture/UI_Read_Model_Design.md`

### 2.2 我重点复核了哪些代码

- `asterion_core/runtime/strategy_engine_v3.py`
- `asterion_core/execution/live_submitter_v1.py`
- `asterion_core/signer/signer_service_v1.py`
- `asterion_core/blockchain/chain_tx_v1.py`
- `asterion_core/risk/allocator_v1.py`
- `asterion_core/risk/portfolio_v3.py`
- `domains/weather/opportunity/service.py`
- `domains/weather/opportunity/execution_priors.py`
- `domains/weather/opportunity/execution_feedback.py`
- `domains/weather/opportunity/ranking_retrospective.py`
- `domains/weather/forecast/calibration.py`
- `dagster_asterion/handlers.py`
- `dagster_asterion/job_map.py`
- `dagster_asterion/schedules.py`
- `asterion_core/ui/surface_truth_shared.py`
- `ui/surface_truth.py`
- `ui/data_access.py`
- `ui/loaders/home_loader.py`
- `ui/loaders/markets_loader.py`
- `asterion_core/ui/read_model_registry.py`
- `asterion_core/ui/ui_lite_db.py`
- `scripts/run_real_weather_chain_smoke.py`

### 2.3 我实际跑过并用来锁定现状的测试

我跑过的重点测试里，以下是**通过**并可作为当前事实依据的：

- `tests.test_ranking_score_v2`
- `tests.test_execution_feedback_loop`
- `tests.test_allocator_v1`
- `tests.test_allocation_preview_persistence`
- `tests.test_calibration_profile_v2`
- `tests.test_calibration_materialization_status`
- `tests.test_calibration_freshness_penalty`
- `tests.test_operator_truth_source`
- `tests.test_ui_read_model_catalog`
- `tests.test_truth_source_checks`
- `tests.test_real_weather_chain_smoke`
- `tests.test_cold_path_orchestration`

我跑到的**当前真实失败/回归**包括：

- `tests.test_execution_foundation`：多处失败，根因是 `run_operator_surface_refresh()` 对 reader-guarded connection 执行 `PRAGMA database_list`
- `tests.test_paper_execution_allocator_integration`：同样被上述 `PRAGMA` 问题打断
- `tests.test_operator_workflow_acceptance`：`Home` action queue 多出一个 `blocked` bucket，和预期不一致
- `tests.test_weather_agents.ResolutionAgentTest.test_resolution_review_handler_stays_on_agent_pipeline`：`run_weather_resolution_review_job()` 无条件调用 UI refresh，导致 test double `object()` 也要被当成数据库连接使用

说明：我没有把所有测试都全量跑完；但对当前问题判断，我已经跑了足够多的代表性测试来锚定关键结论。

---

## 3. 当前代码现实：哪些已经真的成立了

先把“已经做对了”的部分说清楚，否则后面的判断会失真。

### 3.1 `ranking_score` 已经是主排序字段

**代码事实**：

- `strategy_engine_v3.py` 现在按 `pricing_context["ranking_score"]` 排序
- `surface_truth_shared.py` 把 `PRIMARY_SCORE_FIELD` 固定为 `ranking_score`
- `README.md` 和 `V2_Implementation_Plan.md` 也明确把 `ranking_score` 作为唯一主排序字段

**测试事实**：

- `tests.test_ranking_score_v2`
- `tests.test_operator_truth_source`

**结论**：

上一版“主排序还没有统一”的问题，已经不是当前现实。

---

### 3.2 execution feedback 已经进入主排序，不再只是 dashboard

**代码事实**：

- `execution_feedback.py` 真实计算 `feedback_penalty`
- `service.py` 里 `final_ranking_score = pre_feedback_ranking_score * (1 - feedback_penalty)`
- `why_ranked_json` 也会写回 `pre_feedback_ranking_score` 和 feedback 相关字段

**测试事实**：

- `tests.test_execution_feedback_loop`
- `tests.test_ranking_score_v2`

**结论**：

上一版“execution feedback 还只是旁路统计”的说法，已经过时。

---

### 3.3 capital allocation / sizing 已经不再是空白区

**代码事实**：

- `allocator_v1.py` 已经真实落地：
  - `trading.allocation_policies`
  - `trading.position_limit_policies`
  - `trading.capital_budget_policies`
  - `runtime.capital_allocation_runs`
  - `runtime.allocation_decisions`
  - `runtime.position_limit_checks`
- allocator 现在已经做：
  - per-ticket / per-run budget
  - open-markets cap
  - same-station cap
  - position limits
  - inventory constraints
  - calibration gate 对 sizing 的阻断
  - rerank by `pre_budget_deployable_expected_pnl`

**测试事实**：

- `tests.test_allocator_v1`
- `tests.test_allocation_preview_persistence`
- `tests.test_cold_path_orchestration`

**结论**：

上一版“capital deployment / sizing 还基本没有进主链”的说法，已经不成立。
它现在是**已落地但仍值得深化**，不是“尚未开始”。

---

### 3.4 calibration v2 不是 manual-only 了

**代码事实**：

- `job_map.py` 中 `weather_forecast_calibration_profiles_v2_refresh` 是 `scheduled`
- 默认 schedule `weather_forecast_calibration_profiles_v2_nightly` 是 `enabled_by_default=True`

**测试事实**：

- `tests.test_calibration_materialization_status`
- `tests.test_calibration_profile_v2`
- `tests.test_cold_path_orchestration`

**结论**：

“calibration refresh 仍是 manual job”已经不是当前现实。
但“calibration freshness / regime handling 是否已经足够支撑稳定赚钱”，答案仍然是否定的，后面会展开。

---

### 3.5 当前 agent reality：Resolution seam 仍是唯一真正 active 的 agent seam

**代码事实**：

- `job_map.py` 里只有 `weather_resolution_review` 是明确接入 cold-path job map 的 agent job
- `scripts/run_real_weather_chain_smoke.py` 当前主线使用 `parse_rule2spec_draft()` + `validate_rule2spec_draft()` 的 deterministic validation
- `domains/weather/spec/rule2spec_validation.py` 也明确输出 `deterministic rule2spec validation ...`
- `agents/weather/` 目录里虽然仍有 `rule2spec_agent.py`、`data_qa_agent.py` 文件，但当前 orchestrated active seam 不是它们

**测试事实**：

- `tests.test_real_weather_chain_smoke`
- `tests.test_weather_agents`

**结论**：

当前代码现实更接近：

- Rule2Spec / Data QA：**deterministic seam 优先**
- Resolution：**仍是 active agent seam**

这也直接影响我后面对新 agent 的建议：**不要再把 agent 放进 deterministic 主链里抢 canonical truth。**

---

## 4. 旧五大矛盾，现在到底解决了多少

| 旧矛盾 | 当前状态 | 修正后的判断 |
|---|---|---|
| 真钱排序还不够真钱 | **部分解决** | `ranking_score` 主链已统一，但核心 execution economics 输入仍偏粗 |
| 执行经济学还不够经验化 | **部分解决** | execution priors / feedback 已进入主链，但 serving 粒度与 fallback 仍偏 heuristic |
| operator 注意力分配还不够高效 | **未解决，且有回归** | Home queue 仍把 `blocked` 混入主队列，注意力分配仍不干净 |
| execution feedback 还没变成足够强的盈利闭环 | **明显改善但未闭环** | suppression 已入 ranking；retrospective / backlog / cadence 仍不够强 |
| capital deployment / sizing discipline 是更大 ROI 点 | **已落地，但仍是高 ROI 区域** | allocator v1 已经在代码里，但还不够“会分配” |

修正后的总体判断是：

- 第 1、2、4、5 条**都不是“完全没做”**
- 但也都**还没做到足以支撑稳定、规模化、高置信赚钱**
- 第 3 条，也就是 operator attention / action queue / throughput，当前仍然是很现实的盈利限制项

---

## 5. 详细问题清单（按对 ROI 的影响排序）

---

### 问题 1：`run_operator_surface_refresh()` 对 reader guard 执行 `PRAGMA`，打断 paper execution 主链

- **优先级**：P0
- **类型**：Bug / Ops / Delivery
- **受影响文件**：
  - `dagster_asterion/handlers.py`
  - `asterion_core/storage/database.py`
- **当前代码事实**：
  - `handlers.py:1528` 中 `run_operator_surface_refresh()` 调用 `_resolve_connection_db_path(con)`
  - `_resolve_connection_db_path()` 使用 `PRAGMA database_list`
  - `GuardedConnection` reader 模式禁止 `PRAGMA`
- **当前测试事实**：
  - `tests.test_execution_foundation` 当前多处失败
  - `tests.test_paper_execution_allocator_integration` 当前失败
  - 根因一致：`PermissionError: Reader connection rejects SQL statement type: PRAGMA`
- **当前文档事实**：
  - active docs 把 `v2.0` 描述为 profitability + operator throughput 强化阶段
  - 但当前 core paper path 被 surface refresh regression 打断，这和“closeout pending”现实一致
- **风险描述**：
  这是当前最实的 execution foundation regression。它不是 UI cosmetic，而是直接打断 `weather_paper_execution` 及 allocator integration 的基础路径。
- **对系统的直接影响**：
  - 破坏 execution foundation baseline
  - 拖慢 allocator / execution 相关回归验证
- **对稳定赚钱的影响**：
  在基础回放/纸面执行路径不稳时，任何盈利优化都会变得不可信。
- **具体修复方案**：
  1. 在 `GuardedConnection` 增加安全的 `db_path` 暴露（例如 `db_path` property）
  2. `connect_duckdb()` 在构造 `GuardedConnection` 时把 `cfg.db_path` 挂进去
  3. `_resolve_connection_db_path(con)` 先读 `getattr(con, "db_path", None)`；只有 raw duckdb connection 才 fallback 到 `PRAGMA database_list`
  4. 不要再对 reader-guarded connection 发 `PRAGMA`
- **需要改哪些模块**：
  - `asterion_core/storage/database.py`
  - `dagster_asterion/handlers.py`
- **需要补哪些测试**：
  - 新增 `test_resolve_connection_db_path_prefers_guarded_connection_property`
  - 让 `tests.test_execution_foundation` / `tests.test_paper_execution_allocator_integration` 回到 green
- **是否需要 migration**：不需要
- **修复优先级顺序**：**1**

---

### 问题 2：Home action queue 把 `blocked` 混进主操作队列，operator 注意力被稀释

- **优先级**：P1
- **类型**：UX / Trading / Delivery
- **受影响文件**：
  - `ui/loaders/home_loader.py`
  - `tests/test_operator_workflow_acceptance.py`
- **当前代码事实**：
  - `home_loader.py:53` 当前过滤条件包含 `blocked`
  - 结果 Home action queue 会展示 `ready_now / high_risk / review_required / blocked`
- **当前测试事实**：
  - `tests.test_operator_workflow_acceptance` 当前失败
  - 预期只应显示：`ready_now / high_risk / review_required`
- **当前文档事实**：
  - `P9` 的主题就是 `Operator Surface Delivery and Throughput Scaling`
  - 当前这个回归正好说明 throughput 主题还没有真正 closeout
- **风险描述**：
  `blocked` 项不应该占用主 action queue 的头部注意力。它会让 operator 的“下一步该看什么”变脏。
- **对系统的直接影响**：
  Home 视图会误导 operator，把不可行动的队列混入可行动队列。
- **对稳定赚钱的影响**：
  这不是小 UX。它直接降低 operator 在高价值机会上的 attention share。
- **具体修复方案**：
  1. `home_loader.py` 主 action queue 过滤改为只包含：
     - `ready_now`
     - `high_risk`
     - `review_required`
  2. `blocked` 保留在 summary metrics 与独立 secondary panel 中，不放进主 action queue
  3. 如果需要显示 blocked backlog，单独建 `blocked_backlog` 子视图
- **需要改哪些模块**：
  - `ui/loaders/home_loader.py`
  - `ui/pages/home.py`
- **需要补哪些测试**：
  - 修复 `tests.test_operator_workflow_acceptance`
  - 新增 `test_home_action_queue_excludes_blocked_items`
- **是否需要 migration**：不需要
- **修复优先级顺序**：**2**

---

### 问题 3：`ranking_v2` 已上线，但核心 execution economics 输入仍然偏 heuristic

- **优先级**：P1
- **类型**：Trading
- **受影响文件**：
  - `domains/weather/opportunity/service.py`
- **当前代码事实**：
  - `_slippage_bps()`：当前仍是 `40 / 80`
  - `_liquidity_penalty_bps()`：当前仍是 `25 / 60 / 999999`
  - `_fill_probability()`：当前主要根据 `agent_review_status` / `live_prereq_status` 做 0.25 / 0.50 / 0.75 / 0.60 heuristic
  - `_depth_proxy()`：当前仍是 `0.85 / 0.55 / 0.25`
- **当前测试事实**：
  - `tests.test_ranking_score_v2` 证明 ranking_v2 contract 成立
  - 但这些测试锁的是“机制存在”，不是“数值已经足够接近真钱执行经济”
- **当前文档事实**：
  - README / v2 plan 明确说 execution economics 已经进入 ranking_v2
  - 这个说法在“机制接线”层面成立
  - 但在“数值质量”层面仍然偏乐观
- **风险描述**：
  当前最影响赚钱的核心问题，不是没有 `ranking_score`，而是 `ranking_score` 里最重要的几项仍不够 empirical。
- **对系统的直接影响**：
  - 高 edge 机会可能被过度高估
  - book 很差的机会可能因为 heuristic 过于粗糙而被误提到前面
- **对稳定赚钱的影响**：
  这是当前最接近“真钱排序还不够真钱”的真实原因。
- **具体修复方案**：
  1. 用 empirical priors 替换 heuristic fallback 的主输入顺序：
     - `fill_probability`
     - `slippage_penalty`
     - `depth_proxy`
  2. 只在 priors 真缺失时才退回 heuristic
  3. 引入 deterministic microstructure features（见问题 7）后，把以下项接入 ranking：
     - `quote_imbalance_score`
     - `top_of_book_stability`
     - `book_update_intensity`
     - `spread_regime`
  4. `why_ranked_json` 中明确区分：
     - `heuristic_terms_used`
     - `empirical_terms_used`
- **需要改哪些模块**：
  - `domains/weather/opportunity/service.py`
  - `domains/weather/opportunity/execution_priors.py`
- **需要补哪些测试**：
  - `test_ranking_score_prefers_empirical_fill_probability_when_prior_ready`
  - `test_ranking_score_microstructure_penalty_suppresses_unstable_book`
  - `test_why_ranked_json_marks_heuristic_vs_empirical_inputs`
- **是否需要 migration**：视 microstructure table 是否引入而定
- **修复优先级顺序**：**3**

---

### 问题 4：execution priors 已落地，但 serving feature-space 仍然过粗

- **优先级**：P1
- **类型**：Trading / Data
- **受影响文件**：
  - `domains/weather/opportunity/execution_priors.py`
- **当前代码事实**：
  - `build_execution_prior_key()` 已经计算 richer 维度：
    - `market_age_bucket`
    - `hours_to_close_bucket`
    - `calibration_quality_bucket`
    - `source_freshness_bucket`
  - 但 `materialize_execution_priors()` 的实际 grouping key 目前仍然主要是：
    - `cohort_type`
    - `cohort_key`
    - `market_id`
    - `side`
    - `horizon_bucket`
    - `liquidity_bucket`
  - 也就是说 richer features 已经算出来了，但没有全部进入主 grouping grain
- **当前测试事实**：
  - `tests.test_execution_feedback_loop` / `tests.test_ranking_score_v2` 证明 priors 会进入 ranking
  - 但我没有看到能证明 richer feature buckets 已经完全成为主要 serving grain 的测试
- **当前文档事实**：
  - active docs 强调 execution priors 已 nightly materialize 并服务 ranking
  - 这在“有 serving table”层面成立
  - 在“粒度是否足够匹配真钱行为”层面还没有 closeout
- **风险描述**：
  过粗的 prior 会把不同 execution regime 混在一起，导致 empirical term 名义上存在、实际上不够 sharp。
- **对系统的直接影响**：
  - 同一 market/side 下，不同临近收盘 / calibration / freshness 状态的机会会被混估
- **对稳定赚钱的影响**：
  这会直接拖累 capture 和 realized edge retention。
- **具体修复方案**：
  1. 把 richer features 升级为 primary serving grain 的一部分，而不是只存储不强用
  2. lookup 顺序改成：
     - exact rich cohort
     - relaxed cohort
     - strategy/wallet aggregated fallback
     - station+metric fallback
     - heuristic
  3. 增加 `sample_weight` / `quality_weight` 机制，避免稀疏小样本硬顶替大样本
  4. 把 `prior_quality_status` 从简单的 `>=10 ready` 升级为综合：
     - sample_count
     - regime consistency
     - freshness consistency
     - calibration quality consistency
- **需要改哪些模块**：
  - `domains/weather/opportunity/execution_priors.py`
  - `domains/weather/opportunity/service.py`
- **需要补哪些测试**：
  - `test_execution_prior_lookup_prefers_rich_exact_cohort`
  - `test_sparse_rich_cohort_falls_back_to_station_metric_summary`
  - `test_prior_quality_status_considers_more_than_sample_count`
- **是否需要 migration**：可能不需要，如果复用现有列；如果新增权重列则需要
- **修复优先级顺序**：**4**

---

### 问题 5：feedback 已在主链，但 retrospective 仍没有形成“快速修正赚钱错误”的闭环

- **优先级**：P1
- **类型**：Trading / Ops
- **受影响文件**：
  - `domains/weather/opportunity/execution_feedback.py`
  - `domains/weather/opportunity/ranking_retrospective.py`
  - `dagster_asterion/job_map.py`
- **当前代码事实**：
  - `weather_execution_priors_refresh` 是 nightly scheduled
  - `weather_ranking_retrospective_refresh` 仍是 manual
  - retrospective 可以 materialize，但没有看到它被强接进 operator backlog / auto-remediation queue
- **当前测试事实**：
  - `tests.test_execution_feedback_loop` 证明 suppression 存在
  - `tests.test_cold_path_orchestration` 证明 retrospective job 现在仍是 manual
- **当前文档事实**：
  - v2 plan 承认 operator throughput 与 delivery closeout 仍在进行中
- **风险描述**：
  系统已经知道“哪些 cohort 经常 miss / distortion 高”，但修正节奏仍偏慢，更多是 nightly + research 风格，而不是连续减少赚钱错误。
- **对系统的直接影响**：
  - 同类错误可能跨多个周期重复发生
  - ranking/allocator/operator surface 的修正不够快
- **对稳定赚钱的影响**：
  这是“feedback 已在主链，但还没变成强盈利闭环”的主要原因。
- **具体修复方案**：
  1. 把 retrospective 从 manual-only 提升为默认 nightly 或日内 scheduled（至少 daily）
  2. 从 retrospective rows 自动 materialize 一个 `high_score_miss_backlog`
  3. backlog 按以下 bucket 分类：
     - `ranking_overstated`
     - `allocation_too_small`
     - `book_moved_before_fill`
     - `calibration_regime_shift`
     - `operator_not_reached`
  4. 在 Home / Execution 页显式展示 top miss cohorts
- **需要改哪些模块**：
  - `domains/weather/opportunity/ranking_retrospective.py`
  - `dagster_asterion/job_map.py`
  - `asterion_core/ui/ui_lite_db.py`
  - `ui/loaders/execution_loader.py`
- **需要补哪些测试**：
  - `test_ranking_retrospective_refresh_is_default_scheduled`（如果你决定启用默认调度）
  - `test_high_score_miss_backlog_materialization`
- **是否需要 migration**：如果新增 backlog 持久表则需要
- **修复优先级顺序**：**5**

---

### 问题 6：allocator v1 已经很有价值，但仍然是“能分配”，不是“更会分配”

- **优先级**：P1
- **类型**：Trading / Architecture
- **受影响文件**：
  - `asterion_core/risk/allocator_v1.py`
  - `asterion_core/risk/portfolio_v3.py`
- **当前代码事实**：
  - allocator 当前 rerank 依据是 `pre_budget_deployable_expected_pnl`，其次 `base_ranking_score`
  - `deployable_ranking_score` 当前本质上就是 `deployable_expected_pnl`
  - 这意味着 allocator 虽然做了预算、限制、cap、concentration、calibration gate，但最后的 deployable score 仍然相对线性
- **当前测试事实**：
  - `tests.test_allocator_v1` 证明 allocator 当前排序 / resize / blocking 行为是稳定的
- **当前文档事实**：
  - docs 已经把 scaling-aware capital discipline 归为 accepted tranche 基线
  - 这成立
  - 但“更高质量 capital scheduler”还没真正做出来
- **风险描述**：
  当前 allocator 更像一个受限条件下的顺序筛选器，而不是更高质量的 capital deployment engine。
- **对系统的直接影响**：
  - 对 budget scarcity 的反应较线性
  - 对 uncertainty / capture quality / concentration / regime 的 sizing 仍可更细
- **对稳定赚钱的影响**：
  这是当前最接近“资本部署质量还没有吃到头部 ROI”的地方。
- **具体修复方案**：
  1. 不新开 `risk.*` schema，继续沿用当前 discipline：
     - canonical policies 继续放 `trading.*`
     - runtime decisions 继续放 `runtime.*`
  2. 把 `deployable_ranking_score` 升级为至少包含：
     - `deployable_expected_pnl`
     - `capture_quality_multiplier`
     - `calibration_gate_multiplier`
     - `concentration_penalty`
     - `capital_scarcity_penalty`
  3. sizing 从 hard cut / linear resize，升级到：
     - uncertainty-aware sizing
     - regime-aware sizing
     - concentration-aware sizing
  4. 增加 `capital_policy_explanation_json`
- **需要改哪些模块**：
  - `asterion_core/risk/allocator_v1.py`
  - `domains/weather/opportunity/service.py`
  - `ui/data_access.py`
- **需要补哪些测试**：
  - `test_deployable_ranking_score_penalizes_low_capture_quality`
  - `test_allocator_reduces_size_under_degraded_calibration_gate`
  - `test_allocator_prefers_higher_capital_efficiency_under_budget_scarcity`
- **是否需要 migration**：大概率只需在 `runtime.allocation_decisions` 加少量 explanation 字段
- **修复优先级顺序**：**6**

---

### 问题 7：缺少一个真正 deterministic 的 microstructure / execution-intelligence 特征层

- **优先级**：P1
- **类型**：Trading
- **受影响文件**：
  - `asterion_core/ws/ws_agg_v3.py`
  - `domains/weather/opportunity/service.py`
  - `ui/loaders/markets_loader.py`
- **当前代码事实**：
  - 仓库里已经有 `ws_agg_v3.py`，能按分钟聚合 BBO / spread / coverage / quote delay
  - 但当前 ranking 主链里还没有看到系统性进入：
    - `quote_imbalance_score`
    - `top_of_book_stability`
    - `book_update_intensity`
    - `visible_size_shock`
    - `book_pressure_side`
- **当前测试事实**：
  - execution/economics 测试能证明 current heuristics 工作
  - 但没有看到“微观结构特征已进入 ranking / sizing / operator action”的锁定测试
- **当前文档事实**：
  - docs 讲了 execution economics，但当前代码实现更偏 execution priors，不是 microstructure-aware execution intelligence
- **风险描述**：
  没有 microstructure 层，就很难把“看起来有 edge”变成“更容易抓住 edge”。
- **对系统的直接影响**：
  - 决定什么时候进、挂什么价、等不等 book 的能力仍弱
- **对稳定赚钱的影响**：
  这是当前最影响 capture / realized edge 的高 ROI 缺口之一。
- **具体修复方案**：
  1. 新增 deterministic microstructure materialization：
     - 建议表：`runtime.market_microstructure_snapshots`
  2. 字段至少包含：
     - `market_id`
     - `observed_at`
     - `best_bid`
     - `best_ask`
     - `spread_bps`
     - `quote_imbalance_score`
     - `top_of_book_stability`
     - `book_update_intensity`
     - `visible_size_shock_flag`
     - `book_pressure_side`
  3. 在 `service.py` 中把这些特征接进：
     - `fill_probability`
     - `slippage_penalty`
     - `depth_proxy`
     - `execution_posture_hint`
  4. 在 UI 上增加 `execution_tactical_signal`
- **需要改哪些模块**：
  - 新增 microstructure builder / materializer
  - `asterion_core/ws/ws_agg_v3.py`
  - `domains/weather/opportunity/service.py`
  - `asterion_core/ui/ui_lite_db.py`
  - `ui/loaders/markets_loader.py`
- **需要补哪些测试**：
  - microstructure feature extraction tests
  - ranking suppression / tactical signal tests
  - UI rendering tests for tactical badge
- **是否需要 migration**：需要（若新增 runtime table）
- **修复优先级顺序**：**7**

---

### 问题 8：calibration v2 已 scheduled，但仍偏 nightly；对快速 regime shift 仍不够敏感

- **优先级**：P2
- **类型**：Trading / Ops
- **受影响文件**：
  - `domains/weather/forecast/calibration.py`
  - `dagster_asterion/job_map.py`
- **当前代码事实**：
  - calibration profiles v2 已 nightly materialize
  - freshness status 已存在：`fresh / stale / degraded_or_missing`
  - regime bucket 与 regime stability 也存在
- **当前测试事实**：
  - `tests.test_calibration_profile_v2`
  - `tests.test_calibration_materialization_status`
  - `tests.test_calibration_freshness_penalty`
- **当前文档事实**：
  - docs 把 calibration v2 视为 accepted capability，这一点成立
- **风险描述**：
  calibration v2 已经在代码里，但对于天气类高波动事件，nightly refresh 仍可能不足以应对日内 regime shift。
- **对系统的直接影响**：
  - 某些突发天气事件日内变化会让 calibration freshness 名义上“还行”，但经济意义上已经落后
- **对稳定赚钱的影响**：
  这更多影响高置信度，而不是基础能否运行。
- **具体修复方案**：
  1. 保持 nightly full rebuild
  2. 新增 intraday lightweight refresh / degraded override：
     - 只更新受影响 station/source/horizon buckets
  3. 把 recent replay drift / threshold probability drift 接入 freshness or regime downgrade
- **需要改哪些模块**：
  - `domains/weather/forecast/calibration.py`
  - `dagster_asterion/job_map.py`
- **需要补哪些测试**：
  - `test_intraday_calibration_refresh_marks_impacted_segments`
- **是否需要 migration**：可能需要新增 materialization metadata 字段
- **修复优先级顺序**：**8**

---

### 问题 9：truth-source 大体正确，但 boundary sidebar 仍保留 hardcoded fallback

- **优先级**：P2
- **类型**：Docs / UI
- **受影响文件**：
  - `ui/surface_truth.py`
- **当前代码事实**：
  - boundary sidebar 已优先从 readiness/evidence/runtime 动态生成
  - 但如果这些都没有值，会 fallback 到硬编码：
    - `manual-only`
    - `default-off`
    - `approve_usdc only`
    - `constrained real submit`
- **当前测试事实**：
  - `tests.test_operator_truth_source` 证明动态 truth-source path 是存在的
- **风险描述**：
  这是次要问题，但它说明 truth-source 还没有彻底闭环。
- **对稳定赚钱的影响**：
  影响不大，但会影响 operator 对系统当前 boundary 状态的信任。
- **具体修复方案**：
  1. fallback 改成 `unknown / unavailable`，不要再假装知道具体边界
  2. 在 UI 上显式标明“boundary truth unavailable”
- **修复优先级顺序**：**9**

---

### 问题 10：paper execution / resolution review handler 与 UI refresh 过度耦合，降低隔离性和测试韧性

- **优先级**：P2
- **类型**：Architecture / Ops / Testing
- **受影响文件**：
  - `dagster_asterion/handlers.py`
- **当前代码事实**：
  - `run_weather_paper_execution_job()` 无条件调用 `run_operator_surface_refresh()`
  - `run_weather_resolution_review_job()` 也无条件调用 `run_operator_surface_refresh()`
- **当前测试事实**：
  - `tests.test_weather_agents` 当前就被这个设计打穿：test double `object()` 也被要求具有数据库连接语义
- **风险描述**：
  handler 的核心语义是完成自己的 domain job，不该强耦合 UI rebuild。当前设计降低了 isolation，也放大了 surface refresh regressions 的爆炸半径。
- **对稳定赚钱的影响**：
  它不是直接决定赚钱，但会拖慢迭代和回归速度。
- **具体修复方案**：
  1. 给这些 handler 增加 `refresh_surface: bool = True` flag
  2. 在测试和 cold-path orchestration 中明确控制是否刷新 surface
  3. 长期更好的方式：由 orchestration layer 统一决定 job 后是否触发 operator surface refresh，而不是每个 handler 自己强耦合
- **修复优先级顺序**：**10**

---

## 6. 在引入新 agent 之前，哪些工作更能显著提高 ROI

如果只从 ROI 排序，而不是“为了做 agent 而做 agent”，我会把下列工作排在任何新 agent 之前。

### 6.1 第一优先：修 execution foundation regression

**为什么 ROI 最高**：

因为当前 regression 直接打断 paper execution / allocator integration / execution foundation tests。主链都不稳，agent 再聪明也没意义。

**执行任务**：

- 修 `_resolve_connection_db_path()` 不再对 guarded reader 发 `PRAGMA`
- 给 handler 的 surface refresh 增加更稳的 connection path resolution
- 修复 `test_execution_foundation` / `test_paper_execution_allocator_integration`

**验收标准**：

- `tests.test_execution_foundation` 全绿
- `tests.test_paper_execution_allocator_integration` 全绿

---

### 6.2 第二优先：先把 operator 主动作队列做干净

**为什么 ROI 高**：

当前系统已经不是“没有机会”，而是“operator 怎么更快抓住更值得做的机会”。
主 action queue 被 `blocked` 污染，是最典型的低成本高 ROI 修复。

**执行任务**：

- Home queue 移除 `blocked`
- 增加单独 blocked backlog
- 让 queue 排序更偏向：
  - `actionability_status`
  - `allocation_status`
  - `ranking_score`
  - `feedback_status`
  - `source_badge`

**验收标准**：

- `tests.test_operator_workflow_acceptance` 全绿
- Home 页面主队列不再展示 blocked rows

---

### 6.3 第三优先：先做 deterministic microstructure / execution intelligence v1

**为什么 ROI 高**：

这是把 paper alpha 变成 realized alpha 的关键层。

**执行任务**：

- 基于 `ws_agg_v3.py` 新增 microstructure summary
- 输出 deterministic features
- 把这些特征接入 ranking / queue / operator surface

**验收标准**：

- `why_ranked_json` 能显示 microstructure-derived terms
- execution tactics 能在 UI 上可见
- 相同 edge 下，book unstable 市场会被更明显降权

---

### 6.4 第四优先：把 execution priors 从“有”做成“更准”

**为什么 ROI 高**：

现在不是没有 priors，而是 priors 还不够 sharp。

**执行任务**：

- 提升 serving 粒度
- richer cohort lookup
- 更合理的 quality gating
- 更清晰的 fallback order

**验收标准**：

- empirical-primary 占比提升
- fallback-heuristic 占比下降
- retrospective 上 top-ranked capture 与 realized retention 提升

---

### 6.5 第五优先：把 allocator 从 v1 提升到 “v2.1 capital deployment”

**为什么 ROI 高**：

当前 allocator 已经很有价值，但资本仍可能没有打到最该打的地方。

**执行任务**：

- uncertainty-aware sizing
- capital scarcity aware deployable score
- regime-aware sizing
- concentration-aware sizing

**验收标准**：

- scarce budget 下，deployable ranking 与 realized pnl 的一致性提升
- 类似机会中，低 capture / 高 concentration 的单子尺寸被更合理压缩

---

## 7. 上述问题解决后，接入的 ROI 最高 agent

如果上述 deterministic 高 ROI 问题已经解决到位，接入下面agent：

# **Opportunity Triage / Execution Intelligence Agent**

### 7.1 为什么是它

因为它最直接服务于当前 Asterion 的真实瓶颈：

- 不是再造 forecast
- 不是替代 allocator
- 不是碰 live boundary
- 不是做“看起来很 AI”的问答系统

它做的唯一事情是：

**把当前系统已经知道、但 operator 不容易在几秒内综合判断的盈利信息，压缩成清晰的优先级和执行姿态建议。**

### 7.2 它能通过什么机制提升赚钱能力

1. **提高 operator 吞吐**
   - 更快知道先看哪个机会
2. **提高 capture rate**
   - 更快判断现在进还是等，激进还是被动
3. **减少误判**
   - 把 research-only / blocked / degraded source 机会从主行动流里压下去
4. **几乎不碰安全边界**
   - advisory only，不进入 submitter / signer / chain tx
---

## 8. Opportunity Triage / Execution Intelligence Agent 详细设计

### 8.1 设计原则

1. **只做 advisory，不做 hard execution**
2. **只吃 canonical / deterministic facts，不创造 canonical truth**
3. **输出必须结构化**，不能只是一段 narrative
4. **必须可回放评估**
5. **必须直接服务于盈利链条**

---

### 8.2 设计目标

这个 agent 的目标不是回答“这个市场是什么”，而是回答：

- 这个机会现在值不值得 operator 先看
- 这个机会如果要做，执行姿态应该偏 aggressive 还是 passive
- 哪些理由支持它值得做
- 哪些风险意味着它不该现在花太多时间

---

### 8.3 非目标

这个 agent **不应该**：

- 直接生成下单请求
- 直接修改 `ranking_score`
- 直接修改 allocator policy
- 直接修改 canonical spec / fair value / readiness decision
- 进入 signer / submitter / chain tx boundary

---

### 8.4 与 Asterion 现有持久化 discipline 的关系

我建议它**复用现有 `agent.*` pipeline**，不要新开平行 schema。

#### 保持不变

- `trading.*`：继续作为 canonical trading ledger
- `runtime.*`：继续作为 runtime / audit / execution artifacts
- `agent.*`：继续作为 agent invocation/output/review/evaluation 的统一持久层

#### 新 agent 不建议一上来就新增的东西

- 不建议先建 `risk.*`
- 不建议先建大而全的 `agent_runtime.*`

#### 最小化接入方案

- 扩展 `agents.common.runtime.AgentType`：新增 `OPPORTUNITY_TRIAGE = "opportunity_triage"`
- 复用：
  - `agent.invocations`
  - `agent.outputs`
  - `agent.reviews`
  - `agent.evaluations`
- 新增一个 UI read model：
  - `ui.opportunity_triage_summary`

这样做的好处是：

- 不破坏现有 persistence discipline
- 代码改动最小
- 可以直接复用当前 resolution agent 的通用 pipeline

---

### 8.5 输入数据设计

这个 agent 不直接吃杂乱 JSON，而是吃一个明确的 `OpportunityTriageAgentRequest`。

#### 主要输入来源

建议 primary source 来自 **UI lite / operator surface 已经整理过的 canonical facts**：

1. `ui.market_opportunity_summary`
   - `market_id`
   - `location_name`
   - `question`
   - `best_side`
   - `ranking_score`
   - `base_ranking_score`
   - `deployable_expected_pnl`
   - `recommended_size`
   - `allocation_status`
   - `calibration_gate_status`
   - `feedback_status`
   - `feedback_penalty`
   - `source_badge`
   - `source_truth_status`
   - `why_ranked_json`

2. `ui.action_queue_summary`
   - `operator_bucket`
   - `queue_priority`
   - `queue_reason_codes_json`
   - `binding_limit_scope`
   - `remaining_run_budget`

3. `ui.execution_science_summary`
   - cohort capture / feedback summary

4. `ui.cohort_history_summary`
   - 最近相似 cohort 的 capture / realized pnl / replay drift

5. `ui.calibration_health_summary`
   - calibration health / impacted market counts

6. 新增 deterministic microstructure read model：
   - `ui.market_microstructure_summary`
   - 字段建议：
     - `quote_imbalance_score`
     - `top_of_book_stability`
     - `book_update_intensity`
     - `spread_regime`
     - `visible_size_shock_flag`
     - `book_pressure_side`

#### Python dataclass 示例

```python
@dataclass(frozen=True)
class OpportunityTriageAgentRequest:
    market_id: str
    location_name: str | None
    question: str
    best_side: str
    ranking_score: float
    base_ranking_score: float | None
    deployable_expected_pnl: float | None
    recommended_size: float | None
    allocation_status: str | None
    operator_bucket: str | None
    queue_priority: int | None
    calibration_gate_status: str | None
    feedback_status: str | None
    feedback_penalty: float | None
    source_badge: str | None
    source_truth_status: str | None
    why_ranked_json: dict[str, Any]
    cohort_history_summary: dict[str, Any]
    execution_science_summary: dict[str, Any]
    microstructure_summary: dict[str, Any] | None
```

#### 输入 JSON 示例

```json
{
  "market_id": "mkt_nyc_90f_yes",
  "location_name": "New York",
  "question": "NYC high temp >= 90F on 2026-07-14",
  "best_side": "BUY",
  "ranking_score": 82.4,
  "base_ranking_score": 74.8,
  "deployable_expected_pnl": 1.68,
  "recommended_size": 30.0,
  "allocation_status": "resized",
  "operator_bucket": "ready_now",
  "queue_priority": 2,
  "calibration_gate_status": "clear",
  "feedback_status": "watch",
  "feedback_penalty": 0.12,
  "source_badge": "canonical",
  "source_truth_status": "canonical",
  "why_ranked_json": {
    "economics_path": "empirical_primary",
    "capture_probability": 0.43,
    "expected_dollar_pnl": 0.055,
    "risk_penalty": 0.014,
    "capital_efficiency": 2.07,
    "quality_confidence_multiplier": 0.91
  },
  "cohort_history_summary": {
    "fill_capture_ratio": 0.34,
    "resolution_capture_ratio": 0.28,
    "avg_realized_pnl": 0.021,
    "forecast_replay_change_rate": 0.07
  },
  "execution_science_summary": {
    "submission_capture_ratio": 0.61,
    "fill_capture_ratio": 0.34,
    "resolution_capture_ratio": 0.28,
    "feedback_status": "watch"
  },
  "microstructure_summary": {
    "quote_imbalance_score": 0.68,
    "top_of_book_stability": 0.77,
    "book_update_intensity": "medium",
    "spread_regime": "wide_but_stable",
    "visible_size_shock_flag": true,
    "book_pressure_side": "buy_pressure"
  }
}
```

---

### 8.6 输出数据设计

输出必须是结构化的，并且能直接服务 Home / Markets 两个页面。

#### 输出 dataclass

```python
@dataclass(frozen=True)
class OpportunityTriageAgentOutput:
    verdict: AgentVerdict
    confidence: float
    summary: str
    triage_bucket: str
    execution_posture: str
    priority_score: int
    recommended_review_within_minutes: int | None
    suggested_size_contracts: float | None
    profit_reasons: list[str]
    risk_reasons: list[str]
    operator_checks: list[str]
    why_now: str | None
    why_not_now: str | None
    human_review_required: bool
```

#### 字段语义建议

- `triage_bucket`
  - `act_now`
  - `high_priority_review`
  - `watch`
  - `research_only`
- `execution_posture`
  - `lean_aggressive`
  - `prefer_passive`
  - `wait_for_book`
  - `do_not_chase`
- `priority_score`
  - 0-100
- `profit_reasons`
  - 2-4 条最关键盈利理由
- `risk_reasons`
  - 2-4 条最关键风险理由
- `operator_checks`
  - 具体到行动层面的检查项

#### 输出 JSON 示例

```json
{
  "verdict": "review",
  "confidence": 0.86,
  "summary": "机会真实可做，但更适合小尺寸、被动进场，避免追价。",
  "triage_bucket": "high_priority_review",
  "execution_posture": "prefer_passive",
  "priority_score": 88,
  "recommended_review_within_minutes": 5,
  "suggested_size_contracts": 20.0,
  "profit_reasons": [
    "ranking_score 位于当前前列",
    "deployable_expected_pnl 为正且 allocator 已给出可执行尺寸",
    "calibration gate 为 clear",
    "source_badge 为 canonical"
  ],
  "risk_reasons": [
    "同 cohort 历史 fill_capture_ratio 偏低",
    "book pressure 正向同侧推进，追价风险上升",
    "feedback_status 为 watch"
  ],
  "operator_checks": [
    "优先挂被动价，不要直接 chase",
    "若盘口再向同侧移动 2c 以上，先重看 residual edge",
    "若 recommended_size 再次被 allocator 压缩，放弃主动进场"
  ],
  "why_now": "当前 edge 还在，但 book 正在重定价，窗口不宜拖太久。",
  "why_not_now": "不适合无脑主动吃单。",
  "human_review_required": true
}
```

---

### 8.7 与现有 agent infra 的接入方式

#### 需要新增/修改的代码模块

1. **扩展 agent type**
   - `agents/common/runtime.py`
   - 新增：`AgentType.OPPORTUNITY_TRIAGE`

2. **新增 agent 实现**
   - 新文件：`agents/weather/opportunity_triage_agent.py`
   - 参考 `resolution_agent.py` 的实现风格：
     - request loader
     - input payload builder
     - output parser
     - `run_opportunity_triage_review()`

3. **新增 handler**
   - `dagster_asterion/handlers.py`
   - 新增：`run_weather_opportunity_triage_review_job(...)`

4. **新增 job spec**
   - `dagster_asterion/job_map.py`
   - 新增 manual job：`weather_opportunity_triage_review`

5. **新增 UI read model builder**
   - `asterion_core/ui/ui_lite_db.py`
   - 新增表：`ui.opportunity_triage_summary`
   - 从 `agent.outputs` 中挑最新 `opportunity_triage` 输出，并 join 当前 `ui.market_opportunity_summary`

6. **新增 loader integration**
   - `ui/loaders/home_loader.py`
   - `ui/loaders/markets_loader.py`

#### 第一阶段建议的 orchestration

- **先做 manual-only job**，不要先 scheduled
- operator 或 researcher 在 `weather_operator_surface_refresh` 之后手动触发：
  - `weather_opportunity_triage_review`
- 等离线 replay 和 acceptance 通过后，再考虑 schedule

#### 为什么先 manual-only

因为你当前系统定位仍是：

- operator console
- constrained execution infra

这个 agent 的价值首先要在 operator loop 里被验证，而不是一上来就自动跑一切。

---

### 8.8 与当前 UI 的接入方式

#### Home 页

在当前 `action_queue` 上新增 triage 信号：

- `triage_bucket`
- `execution_posture`
- `triage_priority_score`
- `triage_summary`

建议排序逻辑改为：

1. `triage_bucket`（`act_now` > `high_priority_review` > `watch`）
2. `triage_priority_score`
3. `queue_priority`
4. `ranking_score`

#### Markets 页

每一行 market row 展开后新增一个 `Triage Card`：

- 当前 verdict
- 为什么值得看
- 为什么不能太激进
- 当前 execution posture
- operator checks

#### Execution 页

新增一个 `Miss vs Triage` 小节：

- triage 为 `act_now` 的机会，后续 capture / fill / realized 情况如何
- triage 是否真的提升了 capture rate

---

### 8.9 持久化与 migration 建议

#### MVP 版本

如果严格按 ROI 和克制原则来做：

- **不新增主库 runtime 新表**
- **不新增 `risk.*`**
- **复用现有 `agent.*`**
- **只新增 UI read model `ui.opportunity_triage_summary`**

这意味着：

- 主库 migration：通常不需要
- UI lite builder：需要代码改造，但不需要主库 schema 扩张

#### 如果后续需要更高吞吐/更强审计

再考虑新增：

- `runtime.opportunity_triage_runs`
- `runtime.opportunity_triage_items`

但这不应该是 MVP 第一版的默认答案。

---

### 8.10 评估这个 agent 到底有没有提高赚钱能力

这类 agent 不能只看“看起来好不好用”，必须做收益验证。

#### 我建议至少看这 5 个指标

1. **operator review latency**
   - 高价值机会从进入 queue 到被 operator 查看的时间是否下降

2. **top-decile capture uplift**
   - `triage_bucket in {act_now, high_priority_review}` 的机会，capture rate 是否高于未 triage baseline

3. **realized pnl concentration uplift**
   - 实际 realized pnl 是否更集中在 triage 高优先级 bucket

4. **false positive rate**
   - triage 高优先级但最终无 capture / 低 realized edge 的比例

5. **operator throughput uplift**
   - 单位时间内 operator 真正处理的高价值机会数是否提升

#### 对应 acceptance gate

建议第一版必须满足：

- 不降低 top-ranked actionable opportunities 的 capture rate
- `act_now + high_priority_review` bucket 的 realized pnl share 高于 baseline
- Home / Markets 页面不出现 source truth 语义漂移

---

### 8.11 需要新增的测试

#### 单元测试

- `tests.test_opportunity_triage_agent.py`
  - request builder contract
  - output parser contract
  - unsupported field rejection

#### handler 测试

- `tests.test_opportunity_triage_handler.py`
  - handler 只走 agent pipeline，不碰 execution boundary
  - FakeAgentClient 下可稳定产出 artifacts

#### UI read model / loader 测试

- `tests.test_ui_opportunity_triage_summary.py`
- `tests.test_home_loader_uses_triage_priority.py`
- `tests.test_markets_loader_triage_card.py`

#### acceptance / replay 测试

- `tests.test_opportunity_triage_replay_acceptance.py`
  - 比较 triage 前后 top-decile capture / realized pnl

---

## 9. 推荐实施顺序

### Phase A：先修 deterministic 高 ROI 问题

1. 修 `PRAGMA` regression
2. 修 Home queue blocked pollution
3. 做 microstructure v1
4. 做 priors serving v2
5. 做 allocator v2.1
6. 把 ranking retrospective 变成更强的日常闭环

### Phase B：再接入 triage agent MVP

1. 扩展 `AgentType`
2. 新增 `opportunity_triage_agent.py`
3. 新增 manual job
4. 新增 `ui.opportunity_triage_summary`
5. Home / Markets 接入
6. 用 replay acceptance 验证

---

## 10. Top 10 Highest-ROI 改动

1. 修 `run_operator_surface_refresh()` 的 `PRAGMA` regression
2. Home queue 移除 `blocked` 主队列污染
3. 新建 deterministic microstructure 特征层
4. 用 richer cohort 提升 execution priors serving 精度
5. allocator 升级到 capital-aware sizing v2.1
6. 让 ranking retrospective 不再 manual-only
7. 把 retrospective 结果 materialize 成 operator/research backlog
8. `why_ranked_json` 明确标记 empirical vs heuristic term
9. truth-source fallback 从“假定边界”改成“unknown/unavailable”
10. 以上做完后，再接入 `Opportunity Triage / Execution Intelligence Agent`

---

## 11. What Not To Prioritize Yet

1. 不要优先做“市场情报大模型”
2. 不要优先做“自动交易 agent”
3. 不要优先做“Capital Policy Agent”
4. 不要优先做“Coverage Expansion Scout Agent”
5. 不要优先新开 `risk.*` schema
6. 不要优先扩大 live boundary

这些方向不是永远不做，而是 **短期 ROI 不如把 deterministic 主链补强到足够好**。


---

## 12. Appendix: 重点复核文件

### Docs

- `README.md`
- `AGENTS.md`
- `docs/00-overview/Documentation_Index.md`
- `docs/00-overview/Version_Index.md`
- `docs/00-overview/versions/v2.0/Asterion_Project_Plan.md`
- `docs/00-overview/versions/v2.0/DEVELOPMENT_ROADMAP.md`
- `docs/10-implementation/Implementation_Index.md`
- `docs/10-implementation/versions/v2.0/phase-plans/V2_Implementation_Plan.md`
- `docs/30-trading/Controlled_Live_Boundary_Design.md`
- `docs/30-trading/Execution_Economics_Design.md`
- `docs/40-weather/Forecast_Calibration_v2_Design.md`
- `docs/50-operations/Operator_Console_Truth_Source_Design.md`
- `docs/20-architecture/UI_Read_Model_Design.md`

### Code

- `asterion_core/runtime/strategy_engine_v3.py`
- `asterion_core/execution/live_submitter_v1.py`
- `asterion_core/signer/signer_service_v1.py`
- `asterion_core/blockchain/chain_tx_v1.py`
- `asterion_core/risk/allocator_v1.py`
- `asterion_core/risk/portfolio_v3.py`
- `domains/weather/opportunity/service.py`
- `domains/weather/opportunity/execution_priors.py`
- `domains/weather/opportunity/execution_feedback.py`
- `domains/weather/opportunity/ranking_retrospective.py`
- `domains/weather/forecast/calibration.py`
- `dagster_asterion/handlers.py`
- `dagster_asterion/job_map.py`
- `dagster_asterion/schedules.py`
- `asterion_core/ui/surface_truth_shared.py`
- `ui/surface_truth.py`
- `ui/data_access.py`
- `ui/loaders/home_loader.py`
- `ui/loaders/markets_loader.py`
- `asterion_core/ui/read_model_registry.py`
- `asterion_core/ui/ui_lite_db.py`
- `scripts/run_real_weather_chain_smoke.py`

### Tests

- `tests.test_ranking_score_v2`
- `tests.test_execution_feedback_loop`
- `tests.test_allocator_v1`
- `tests.test_allocation_preview_persistence`
- `tests.test_calibration_profile_v2`
- `tests.test_calibration_materialization_status`
- `tests.test_calibration_freshness_penalty`
- `tests.test_operator_truth_source`
- `tests.test_ui_read_model_catalog`
- `tests.test_truth_source_checks`
- `tests.test_real_weather_chain_smoke`
- `tests.test_cold_path_orchestration`
- `tests.test_execution_foundation`（当前失败，已定位）
- `tests.test_paper_execution_allocator_integration`（当前失败，已定位）
- `tests.test_operator_workflow_acceptance`（当前失败，已定位）
- `tests.test_weather_agents`（当前失败，已定位）

