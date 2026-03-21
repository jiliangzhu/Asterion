# Asterion P7 Implementation Plan

**版本**: v1.0  
**更新日期**: 2026-03-21  
**阶段**: `v2.0 / Phase 7`  
**状态**: accepted closeout record  
**主题**: Deployable Rerank, Allocator v2, and Execution Economics Closure

---

> 本文件是 `v2.0` 最近 accepted tranche 的 closeout record。  
> umbrella active implementation contract 仍是 [V2_Implementation_Plan.md](./V2_Implementation_Plan.md)。  
> 当前仓库阶段状态统一表达为：`P4 accepted; post-P4 remediation accepted; v2.0 implementation active`。  
> `Phase 0` 到 `Phase 7` 已 accepted；当前 tranche 已切换到 `Phase 8`。

## 1. Summary

`Phase 7` 不再重复实现 `P6` 已落地的 deployable metrics、action queue v2 基线或 allocator invariant baseline。  
它只承接当前仍未完成、且直接影响“稳定、规模化、高置信赚钱”的高 ROI 缺口：

- true allocation-aware / deployable rerank
- allocator v2 算法强化
- execution economics / feedback closure v2
- startup / truth-source / stale supporting docs / acceptance cleanup

本阶段固定采用以下边界：

- 不扩 live boundary
- 不做 unattended / unrestricted live
- 不新开 `risk.*`
- 不新增页面
- calibration freshness hard gate 主体与 scaling discipline 固定留给 `P8`

## 2. Current Code Reality and Why P7

截至当前 HEAD，`P6` 已把以下基线收口为 accepted delivery baseline：

- allocator 按 `StrategyDecision.decision_rank` 做 canonical self-sort，并对 duplicate rank / duplicate decision fail fast
- `runtime.allocation_decisions` 已扩出：
  - `base_ranking_score`
  - `deployable_expected_pnl`
  - `deployable_notional`
  - `max_deployable_size`
  - `capital_scarcity_penalty`
  - `concentration_penalty`
- `ui.market_opportunity_summary` / `ui.action_queue_summary` / `Home` / `Markets` 已直接消费 deployable fields 与 binding-limit explanation
- allocation / operator deployment surfaces 上的 `ranking_score` 已具备 deployable-adjusted surface semantics

当前 tranche 要锁定并继续强化的高 ROI 缺口，不再是“有没有 deployable fields”，而是：

- allocation-aware rerank 已进入 allocator，但还需要用 acceptance 和 explanation contract 把它锁死
- allocator v2 级别资本部署解释仍需继续收口，尤其是 preview / final dominant constraint / rerank reason 的一致性
- execution economics 仍需从 heuristic-heavy 彻底推进到 empirical-primary，并用 retrospective uplift 验证
- startup / UI / supporting design / doc acceptance 仍需持续保持 truth-source 一致，避免再次 split-brain

### 2.1 P6 Accepted Baseline vs P7 New Work

当前 HEAD 已有、`P7` 不重做的基线：

- `recommended_size` / `allocation_status` / `budget_impact` 已进入 `OpportunityAssessment`
- `binding_limit_scope` / `binding_limit_key` 已进入 `runtime.allocation_decisions`
- `ui.market_opportunity_summary` 与 `ui.action_queue_summary` 已存在并被 operator surfaces 消费
- deployable fields 已进入 persisted read models、`why_ranked_json` 与 ticket provenance

`P7` 当前 tranche 已落地的主干：

- allocator pass-1 structural preview
- allocator pass-1 结果驱动的 allocation-aware rerank
- allocator pass-2 真正按 reranked order 消耗 run budget
- `runtime.allocation_decisions` 新增 pre-budget deployable preview 与 rerank explanation fields
- empirical execution priors 在样本足够时覆盖 heuristic fallback
- retrospective uplift comparison seam
- startup / UI / supporting docs / acceptance 的 truth-source cleanup

`P7` 当前待 closeout 的 residual gaps：

- preview dominant structural constraint / final dominant constraint / rerank reason 还需要用统一 explanation contract 锁死
- allocator final order、paper path order / size、`ui.action_queue_summary` order 还需要在同一 fixture 下锁成单一 acceptance
- retrospective uplift 还需要从 helper acceptance 升级成 materialized integration acceptance
- `P7` / `V2` / indices / checklist 还需要统一到 `in progress / closeout pending` 的当前口径

## 3. Fixed Goals and Non-Goals

### 3.1 固定目标

`P7` 只做以下 4 条线：

1. delivery hardening and truth-source closeout
2. allocation-aware rerank
3. allocator v2 capital deployment closure
4. execution economics and feedback closure v2

### 3.2 固定不做

- 不把 calibration freshness hard gate 主体塞进 `P7`
- 不做更深的 scaling-aware capital policy schema
- 不新增 `trading.capital_budget_policies`
- 不做 same-date / same-regime / cohort budget policy 扩张
- 不做 portfolio optimizer
- 不做跨 domain 资本联动
- 不新增 page count

## 4. Workstreams

### WS0. Delivery Hardening and Truth-Source Closeout

目标：

- 修掉 startup / UI / docs / supporting design 的 tranche drift
- 为后续盈利工作先收口 operator impression 与 truth-source split-brain

核心 landing areas：

- `start_asterion.sh`
- `ui/app.py`
- `asterion_core/ui/surface_truth_shared.py`
- active docs / supporting designs / doc tests

主要交付物：

- 启动脚本不再输出 stale `remediation in progress`
- sidebar 不再硬编码 `Asterion v1.2`
- supporting design 不再把 attestation caller-trusted / calibration manual-only 写成 current reality
- startup / header / sidebar / doc truth-source acceptance

### WS1. Allocation-Aware Rerank

目标：

- 让 allocator 不只消费 base candidate order，而是先基于 structural preview 做 deployable rerank
- 让预算稀缺时的资本消耗顺序真正对齐 deployable value

核心 landing areas：

- `asterion_core/risk/allocator_v1.py`
- `domains/weather/opportunity/service.py`
- `dagster_asterion/handlers.py`

固定实现语义：

1. `strategy_engine_v3` 继续只负责产出 base candidate order
2. allocator pass-1 在不消耗 run budget 的前提下，为每条机会计算：
   - `pre_budget_deployable_size`
   - `pre_budget_deployable_notional`
   - `pre_budget_deployable_expected_pnl`
   - `binding_limit_scope`
   - `binding_limit_key`
3. allocator 按 pass-1 结果做 rerank：
   - `pre_budget_deployable_expected_pnl DESC`
   - `base_ranking_score DESC`
   - `decision_rank ASC`
   - `decision_id ASC`
4. allocator pass-2 再按 reranked order 真正消耗 run budget，并生成最终 `AllocationDecision`

### WS2. Allocator v2 Capital Deployment Closure

目标：

- 把当前 allocator 从“preview + resize”推进到更强的资本部署模块
- 让 structural constraints、run-budget scarcity、concentration distortion 的语义更清晰可解释

核心 landing areas：

- `asterion_core/risk/allocator_v1.py`
- `asterion_core/risk/portfolio_v3.py`
- `asterion_core/execution/trade_ticket_v1.py`

固定增强项：

1. `capital_scarcity_penalty` 继续表示 run-budget scarcity 对 final size 的压缩比例
2. `concentration_penalty` 继续表示 structural concentration distortion
3. `why_ranked_json` / `budget_impact` / `AllocationDecision` 必须能解释：
   - 为什么被 rerank
   - 为什么推荐 size 变小
   - 当前主导约束是什么
4. `ranking_score` 在 allocation / operator surfaces 上继续是唯一主排序字段，但主语义升级为 reranked deployable score

固定实现约束：

- pass-1 rerank key 直接使用：
  - `pre_budget_deployable_expected_pnl`
  - `base_ranking_score`
  - `decision_rank`
  - `decision_id`
- `capital_scarcity_penalty` 不单独进入 pass-1 sort key
- `concentration_penalty` 通过 pass-1 structural preview size / pnl 间接进入 rerank
- final dominant constraint 继续由 pass-2 `binding_limit_scope` / `binding_limit_key` 表达

### WS3. Execution Economics and Feedback Closure v2

目标：

- 让 empirical priors 在样本足够时真正压过 heuristic
- 让 economics 变化可被 retrospective uplift 明确验证

核心 landing areas：

- `domains/weather/opportunity/service.py`
- `domains/weather/opportunity/execution_priors.py`
- `domains/weather/opportunity/execution_feedback.py`
- `domains/weather/opportunity/ranking_retrospective.py`

固定实现项：

1. 首轮继续复用现有 `weather.weather_execution_priors` 字段：
   - `submit_ack_rate`
   - `fill_rate`
   - `resolution_rate`
   - `cancel_rate`
   - `partial_fill_rate`
   - `adverse_fill_slippage_bps_p50/p90`
   - `submit_latency_ms_p50/p90`
   - `fill_latency_ms_p50/p90`
   - `realized_edge_retention_bps_p50/p90`
   - feedback-derived miss / distortion fields
2. `service.py`
   - `prior_quality_status == "ready"` 且 `sample_count >= 10` 时，empirical terms 成为 primary path
   - `sample_count >= 5` 或 `prior_quality_status in {"watch", "sparse"}` 时，走 blended path
   - 其余情况保持 heuristic fallback
3. `execution_feedback.py`
   - feedback penalty 以 realized miss / distortion / slippage 为主
4. `ranking_retrospective.py`
   - 增加 baseline vs candidate uplift comparison seam

## 5. Interfaces, Persistence, and Truth-Source Rules

### 5.1 固定扩展的 public shapes

`P7` 继续复用现有单主链 contract，不引入 parallel score family。

固定扩展：

- `pricing_context_json`
- `why_ranked_json`
- allocation overlay / `runtime.allocation_decisions`
- `ui.action_queue_summary`
- `ui.market_opportunity_summary`

固定新增或扩展字段：

- `pre_budget_deployable_size`
- `pre_budget_deployable_notional`
- `pre_budget_deployable_expected_pnl`
- `rerank_position`
- `rerank_reason_codes_json`
- `economics_path`
- `heuristic_capture_probability`
- `empirical_capture_probability`

### 5.2 固定行为约束

- `ranking_score` 仍是唯一主排序字段
- `base_ranking_score` 继续只作为兼容解释字段保留
- allocator 仍接在 ranking 之后、execution gate 之前
- paper path 继续 consume allocator
- constrained live 继续 display-only，不自动 consume allocator recommendation

### 5.3 固定持久化方向

- 优先扩已有 `runtime.allocation_decisions`
- 优先扩已有 `weather.weather_execution_priors`
- uplift comparison 优先复用并扩已有：
  - `runtime.ranking_retrospective_runs`
  - `runtime.ranking_retrospective_rows`
- policy truth 继续优先落 `trading.*`
- 默认不在 `P7` 承诺任何 `risk.*` 或新的 `trading.capital_budget_policies`

## 6. Implementation Order

固定实施顺序：

1. 先修 startup / UI / supporting design / doc truth-source drift
2. 再做 allocator pass-1 structural preview 与 rerank
3. 再做 allocator v2 capital deployment explanation closure
4. 最后做 execution economics v2 与 retrospective uplift validation

原因：

- startup / truth-source 不先收口，后续 tranche 切换会继续 split-brain
- rerank 不先落地，allocator v2 只能继续建立在 base-order 预算消耗上
- economics uplift 需要建立在更稳定的 deployable ordering 之上

## 7. Testing and Acceptance Contract

`P7` 必须直接以 acceptance contract 驱动实施，而不是只写方向。

### 7.1 必须新增的 acceptance 面

- `tests.test_allocator_v2_rerank`
  - pass-1 structural preview
  - reranked order 与 base order 分离
  - pass-2 按 reranked order 消耗预算
- `tests.test_deployable_rerank_acceptance`
  - base score 更高但 pre-budget deployable pnl 更低的机会，在最终 allocation 中落后
- `tests.test_allocator_rerank_surface_consistency`
  - 同一 fixture 下锁住 allocator final order、paper path order / size 与 `ui.action_queue_summary` order 一致
- `tests.test_execution_economics_v2_acceptance`
  - priors sufficient 时 empirical terms 覆盖 heuristic fallback
- `tests.test_retrospective_uplift_integration`
  - economics 变更对真实 materialized retrospective rows 输出 deterministic baseline-vs-candidate comparison
- `tests.test_startup_truth_source`
  - `start_asterion.sh` / `ui/app.py` / shared truth / active docs 一致
- `tests.test_supporting_design_current_state`
  - supporting design 不再保留已失效 current-state 说法
- `tests.test_p7_closeout_docs`
  - `P7` / `V2` / indices / checklist 同步为 `in progress / closeout pending` 的 current-state wording

### 7.2 必须显式引用的现有回归基线

- allocator / paper execution integration tests
- `ui.action_queue_summary` / `ui.market_opportunity_summary` tests
- ranking / retrospective tests
- operator truth-source tests
- docs / index hygiene tests

### 7.3 Exit Criteria

- 预算受限时，系统先消耗 deployable value 更高的机会
- allocator preview、paper path、UI action queue 三者排序一致
- `Home` / `Markets` 同时显示 base vs pre-budget vs final deployable value，以及 preview dominant structural constraint / final dominant constraint / rerank reason
- empirical execution terms 在 retrospective 上可验证
- retrospective comparison seam 能对真实 materialized rows 输出 deterministic uplift summary
- startup / UI / docs / supporting designs 不再 split-brain

## 8. Explicit P8 Deferrals

以下内容固定不在 `P7` 实施：

- calibration freshness `stale -> review_required` / `degraded_or_missing -> research_only` 的强 gate 主体
- calibration stale impacted market counts 的更深 operator gating
- scaling-aware capital policy schema
- same-date / regime / cohort budget policy 扩张

这些内容固定保留给：

- `Phase 8 — Calibration Ops and Scaling Discipline`

## 9. Assumptions and Defaults

- `P7` 已作为 accepted closeout record 保留
- `P6` 视为更早的 accepted baseline；`P7` 在其上完成 residual gaps closeout
- `P7` 不新增页面、不扩 live boundary、不新开 `risk.*`
- `P7` 不新增 `trading.capital_budget_policies`
- calibration hard gate 主体固定留给 `P8`
- `P7` 的核心主线固定为：
  - allocation-aware rerank
  - allocator v2 算法强化
  - execution economics / feedback closure v2
  - truth-source / supporting-doc / acceptance cleanup
