# Asterion P6 Implementation Plan

**版本**: v1.0  
**更新日期**: 2026-03-20  
**阶段**: `v2.0 / Phase 6`  
**状态**: accepted tranche baseline record  
**主题**: Capital-Aware Ranking and Deployable Action Queue

---

> 本文件保留为 `v2.0 Phase 6` 的 accepted tranche baseline record。  
> umbrella active implementation contract 仍是 [V2_Implementation_Plan.md](./V2_Implementation_Plan.md)。  
> 当前仓库阶段状态统一表达为：`P4 accepted; post-P4 remediation accepted; v2.0 implementation active`。  
> `Phase 0` 到 `Phase 6` 已 accepted；当前 tranche 固定为 `Phase 7`。

## 1. Summary

`Phase 6` 已完成 deployable metrics、action queue、allocator invariant baseline 的首轮收口。
它的定位是：

**把当前已经具备 ranking + allocator + action queue 初版的 Weather-first 系统，推进成按 deployable value 排序并直接服务 operator 资本部署的 action surface。**

本阶段固定采用以下基线：

- 主要输入：
  - [Asterion_Current-State_Code_Review_and_Next-Stage_Profitability_Upgrade_Blueprint.md](../../../../analysis/Asterion_Current-State_Code_Review_and_Next-Stage_Profitability_Upgrade_Blueprint.md)
  - [V2_Implementation_Plan.md](./V2_Implementation_Plan.md)
- 当前最大 ROI 固定集中在：
  - deployable-value-first ranking
  - allocator invariant hardening
  - action queue v2
  - truth-source / startup / docs drift cleanup
- `execution economics v2` 与 `calibration scaling discipline` 已明确移交给 `P7 / P8`

## 2. Current Code Reality and Why P6

截至当前 HEAD，`v2.0` 已具备以下 accepted baseline：

- `Phase 0` 已修复 truth-source / UI-lite / startup drift baseline
- `Phase 1` 已把 execution priors、ranking penalties、retrospective harness 接入同一条主排序链
- `Phase 2` 已落地 allocator v1、`recommended_size`、policy truth 与 runtime allocation artifacts
- `Phase 3` 已把 calibration freshness / materialization status 推进成 scheduled ops + persisted diagnostics
- `Phase 4` 已把 action queue / cohort history / loader-builder split 收口到 persisted operator surfaces
- `Phase 5` 已把低 ROI agent seam deterministic 化，并把 Resolution review closure 变成 operator-persisted review state

`P6` 完成后明确移交给后续 tranche 的 residual gaps 是：

- `ranking_score` 仍然更像 unit-opportunity 排序，而不是 deployable capital ordering
- allocator 对输入顺序与 invariant 的依赖仍然偏强，解释也还不够直观
- 当前 action queue 已经存在，但还没把 deployable pnl / binding limit / why-this-action 变成第一公民
- startup / UI / docs / acceptance 仍需要跟随这条 deployable 主线继续收口，避免再次 drift

### 2.1 Existing vs New in P6

当前 HEAD 已有、`P6` 不重做的基线：

- `recommended_size` / `allocation_status` / `budget_impact` 已进入 `OpportunityAssessment`
- `binding_limit_scope` / `binding_limit_key` 已进入 `runtime.allocation_decisions`
- `ui.market_opportunity_summary` 与 `ui.action_queue_summary` 已经存在，并被 `Home` / `Markets` 消费
- paper path 已经 consume allocator recommendation；constrained live 仍保持 display-only

`P6` 当前 tranche 新增并锁定的增量：

- allocator 按 `StrategyDecision.decision_rank` 做 canonical self-sort，并对 duplicate rank / duplicate decision fail fast
- `runtime.allocation_decisions` 新增 deployable metrics：
  - `base_ranking_score`
  - `deployable_expected_pnl`
  - `deployable_notional`
  - `max_deployable_size`
  - `capital_scarcity_penalty`
  - `concentration_penalty`
- allocation / operator deployment surfaces 上的 `ranking_score` 直接重定义为 deployable-adjusted score
- `ui.market_opportunity_summary` / `ui.action_queue_summary` / `why_ranked_json` 直接透传 deployable metrics 和 binding-limit explanation

## 3. Fixed Goals and Non-Goals

### 3.1 固定目标

`P6` 只做以下 4 条线：

1. allocator self-sorting / invariant hardening
2. ranking 从 unit-opportunity 进一步推进到 deployable-value-first
3. action queue v2 与 deployable operator surface
4. truth-source / startup / docs / acceptance cleanup，确保上述能力不会再 drift

### 3.2 固定不做

- 不扩 live boundary
- 不做 unattended / unrestricted live
- 不新开 `risk.*`
- 不优先扩新 domain
- 不把 execution priors v2 / calibration scaling 全量塞进 `P6`
- 不把更多 agent surface 当主线

## 4. Workstreams

### WS1. Allocator Invariants and Capital-Aware Ordering

目标：

- 让 allocator 在乱序输入下仍能稳定输出正确推荐
- 让资本稀缺条件下的排序更接近 deployable value，而不是原始 unit-opportunity EV

核心 landing areas：

- `asterion_core/risk/allocator_v1.py`
- `domains/weather/opportunity/service.py`
- `asterion_core/risk/portfolio_v3.py`
- `dagster_asterion/handlers.py`

主要交付物：

- allocator self-sort
- stronger ordering invariants
- `ranking_score` 主语义升级到 deployable-value-first
- allocation-aware rerank seam，仍挂在现有 ranking/allocation workflow 下

### WS2. Deployable Metrics and Action Queue v2

目标：

- 把 deployable pnl / recommended size / binding limit 变成 operator 首屏可消费信息
- 让首页和 Markets 页默认展示“最值得部署”的 action

核心 landing areas：

- `ui.action_queue_summary`
- `ui.market_opportunity_summary`
- `ui/pages/home.py`
- `ui/pages/markets.py`

主要交付物：

- action queue enriched columns
- deployable metrics surfaced in persisted read models
- 首页 / Markets 的 top-action 语义收口

### WS3. Operator Explanation and Binding-Limit Visibility

目标：

- 让 operator 不再靠脑补理解推荐 size 与 action queue 排序
- 让“为什么是这单、为什么不是另一单”成为 persisted explanation contract

核心 landing areas：

- `pricing_context_json`
- `why_ranked_json`
- `runtime.allocation_decisions`
- `ui.market_opportunity_summary`

主要交付物：

- binding-limit explanation
- deployable expected pnl explanation
- top action explanation contract

### WS4. Delivery Hardening for Truth-Source and Docs

目标：

- 防止 `P6` 开发时重新进入 startup / UI / docs split-brain
- 清理 supporting design 与 active plan 中已过时的 current-state 表述

核心 landing areas：

- `start_asterion.sh`
- `ui/app.py`
- `asterion_core/ui/surface_truth_shared.py`
- active docs / supporting designs / doc tests

主要交付物：

- startup truth-source cleanup
- header/sidebar copy consistency cleanup
- stale supporting design cleanup
- Phase 6 acceptance tests

## 5. Interfaces, Persistence, and Truth-Source Rules

### 5.1 固定扩展的 public shapes

`P6` 继续复用现有单主链 contract，不引入 parallel score family。

固定扩展：

- `pricing_context_json`
- `why_ranked_json`
- allocation overlay / `runtime.allocation_decisions`
- `ui.action_queue_summary`
- `ui.market_opportunity_summary`

固定新增或扩展字段：

- `deployable_expected_pnl`
- `deployable_notional`
- `max_deployable_size`
- `base_ranking_score`
- `binding_limit_scope`
- `binding_limit_key`
- `capital_scarcity_penalty`
- `concentration_penalty`

### 5.2 固定行为约束

- `ranking_score` 仍是唯一主排序字段
- 允许把其主语义升级成 deployable-value-first，但不改字段名
- `base_ranking_score` 只作为兼容解释字段保留，不形成第二套长期主排序 contract
- allocator 仍在 ranking 之后，不反向替代 ranking 主链
- paper path 继续 consume allocator
- constrained live 继续 display-only，不自动 consume allocator recommendation

### 5.3 固定持久化方向

- 优先扩已有 `runtime.allocation_decisions`
- policy truth 继续优先落 `trading.*`
- 仅当 budget policy 的表达确实超出现有 policy 表能力时，才在 `trading.*` 内新增 `trading.capital_budget_policies`
- 默认不在 `P6` 承诺任何 `risk.*`

## 6. Implementation Order

固定实施顺序：

1. 先修 allocator invariant / self-sort / ordering consistency
2. 再把 ranking 主语义推进到 deployable-value-first
3. 再升级 `ui.action_queue_summary` / Markets top action explanation
4. 最后收口 startup truth-source、supporting design、acceptance drift

原因：

- allocator 不变量不先锁住，后面的 deployable ranking 和 action queue 会建立在不稳定输出之上
- ranking 语义不先升级，UI 只能更好地展示“还不够 deployable”的排序
- explanation 和 docs cleanup 应建立在前两步的真实 contract 之上，而不是先写 aspirational 文案

## 7. Testing and Acceptance Contract

`P6` 必须直接以 acceptance contract 驱动实施，而不是只写方向。

### 7.1 必须新增的 acceptance 面

- capital-aware ranking acceptance
  - 预算受限时优先 deployable value 更高机会
- allocator invariant acceptance
  - 输入乱序时输出排序与推荐结果仍稳定正确
- deployable action queue acceptance
  - 首页和 Markets 首屏默认看到最值得部署的 action
  - 显示 deployable pnl / recommended size / binding limit / source truth
- truth-source safety acceptance
  - degraded / fallback rows 不得伪装成 canonical
  - startup / UI / docs / shared constants 不再 split-brain

### 7.2 必须显式引用的现有回归基线

- allocation / paper execution integration tests
- ranking / retrospective tests
- UI loader / read-model / truth-source checks
- docs / index hygiene tests

### 7.3 Exit Criteria

- `ranking_score` 主语义已经升级到 deployable-value-first，但仍保持单字段主排序 contract
- allocator 在乱序输入、预算稀缺、limit hit 场景下输出稳定且可解释
- `Home` / `Markets` 的默认 action surface 能直接说明：
  - 下哪边
  - 下多大
  - 预期 deployable pnl
  - 当前主要 binding limit 是什么
- startup / UI / docs / supporting design 与当前 tranche 表达一致

## 8. Follow-On Reservations

`P6` 之后只在 umbrella plan / roadmap 中预留 follow-on，不在本文件展开成当前实施合同：

- `P7` Execution Economics and Feedback Closure v2
- `P8` Calibration Ops and Scaling Discipline
- `P9` Delivery / Scaling follow-on

这些阶段必须建立在 `P6` accepted 之后再具体化，不提前把实现细节拉进当前 tranche。

## 9. Assumptions and Defaults

- 本文件保留为 `Phase 6` 的 accepted tranche baseline record，不再承担当前 tranche 身份
- [V2_Implementation_Plan.md](./V2_Implementation_Plan.md) 保持 umbrella active contract 身份，不降级
- `P6` 的 residual gaps 已明确移交给 `P7 / P8 / P9`
- `P6` 默认不同时引入新的 closeout checklist；closeout checklist 在实施接近完成时再补
- `P6` 的核心主线固定是 deployable-value-first ranking + allocator hardening + action queue v2，而不是更大范围的全面重写
