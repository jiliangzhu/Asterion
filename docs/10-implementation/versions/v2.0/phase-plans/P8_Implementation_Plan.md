# Asterion P8 Implementation Plan

**版本**: v1.0  
**更新日期**: 2026-03-21  
**阶段**: `v2.0 / Phase 8`  
**状态**: accepted closeout record  
**主题**: Calibration Hard Gates and Scaling-Aware Capital Discipline

---

> 本文件保留为 `v2.0 / Phase 8` 的 accepted closeout record。  
> umbrella active implementation contract 仍是 [V2_Implementation_Plan.md](./V2_Implementation_Plan.md)。  
> 当前仓库阶段状态统一表达为：`P4 accepted; post-P4 remediation accepted; v2.0 implementation active`。  
> `Phase 0` 到 `Phase 8` 已 accepted；当前未打开新的 tranche-specific implementation plan，`Phase 9` 继续保留为 umbrella / roadmap reservation。

## 1. Summary

`Phase 8` 不再重复做已经落地的 calibration materialization、freshness visibility、deployable rerank 或 empirical-primary execution economics。  
它只收 umbrella plan 中已经明确 defer 的两类能力：

- calibration freshness / quality 的硬 actionability gate
- scaling-aware capital discipline 的 policy truth + allocator follow-on

固定边界：

- 不扩 live boundary
- 不新增页面
- 不新开 `risk.*`
- 不重写 ranking / allocator / UI read-model 主链
- 不把 `P9` delivery / scaling follow-on 提前拉进来

当前代码 reality：

- `P8` 主干已经落地到 calibration hard gate、`trading.capital_budget_policies`、allocator scaling-aware lookup、`ui.market_opportunity_summary` / `ui.action_queue_summary` / `ui.calibration_health_summary`
- `P8` closeout residuals也已经收口到 acceptance hardening、fallback / degraded source truth-source hardening 与 `P8` / umbrella wording refresh
- 当前本文件只作为 accepted closeout record 保留，不再承担 current tranche 身份

## 2. P7 Accepted Baseline vs P8 New Work

当前 HEAD 已有并作为 `P7` accepted closeout baseline 保留的内容：

- calibration materialization 已 scheduled，并有 `runtime.calibration_profile_materializations`
- `calibration_freshness_status` / `profile_age_hours` 已进入 pricing / assessment / UI persisted context
- allocator 已具备 `pass-1 structural preview -> rerank -> pass-2`
- `runtime.allocation_decisions` 已持久化 deployable preview、rerank explanation 与 capital penalties
- Home / Markets / System 已能展示 deployable ladder 与 calibration freshness diagnostics

`P8` 当前 tranche 只新增：

- calibration gate status / reason codes / impacted-market semantics
- `trading.capital_budget_policies`
- allocator scaling-aware budget lookup 与 fail-closed gate consumption
- impacted market counts 与 scaling reasons 的 persisted operator surface

当前 accepted closeout 已包含：

- page-level baseline 已把 `calibration_gate_status / capital_policy_id / capital_scaling_reason_codes` 锁死
- fallback / degraded source 下的 gate 字段已有专项 acceptance
- `P8` / `V2` / entry docs 已完成 accepted wording refresh

## 3. Workstreams

### WS1. Calibration Hard Gates

目标：

- 把 calibration freshness / quality 从 soft penalty 推进成 actionability constraint

固定实现：

- `domains/weather/opportunity/service.py`
  - `_actionability_status(...)` 增加 calibration gate inputs
  - 输出固定：
    - `calibration_gate_status`
    - `calibration_gate_reason_codes`
    - `calibration_impacted_market`
- gate 语义固定为：
  - `fresh + healthy/watch` -> `clear`
  - `stale` -> `review_required`
  - `degraded_or_missing` -> `review_required`
  - `degraded_or_missing + sparse/lookup_missing` -> `research_only`
- UI / read-model 只消费 persisted fields，不在页面内重算

### WS2. Calibration Impacted Market Visibility

目标：

- 把 calibration gate 变成 operator 可运营 surface，而不是藏在 `why_ranked_json`

固定实现：

- `ui.calibration_health_summary`
  - 聚合：
    - `impacted_market_count`
    - `hard_gate_market_count`
    - `review_required_market_count`
    - `research_only_market_count`
- `ui.market_opportunity_summary`
  - 持久化：
    - `calibration_gate_status`
    - `calibration_gate_reason_codes`
    - `calibration_impacted_market`
- `ui.action_queue_summary`
  - stale / degraded calibration 不再只落 `high_risk`
  - 命中 hard gate 的机会进入 `review_required` 或 `research_only`
- `Home` / `Markets` / `System`
  - 直接展示 persisted gate status / impacted counts / gate reasons

### WS3. Scaling-Aware Capital Discipline

目标：

- 在现有 `trading.*` 纪律内新增 scaling-aware budget policy，而不引入 `risk.*`

固定实现：

- 新增 `trading.capital_budget_policies`
- lookup 优先级固定为：
  1. `wallet + strategy + regime_bucket + calibration_gate_status`
  2. `wallet + strategy + regime_bucket`
  3. `wallet + strategy`
  4. fallback 到 `trading.allocation_policies`
- scaling-aware policy 只影响：
  - run budget cap
  - per-ticket cap
  - concurrent market count caps

### WS4. Allocator P8 Follow-On

目标：

- 在 `P7` rerank 之后引入 scaling-aware budget context，但不引入第二套 allocator

固定实现：

- `asterion_core/risk/allocator_v1.py`
  - calibration gate 为 `review_required` / `research_only` / `blocked` 时 fail-closed
  - persisted explanation 增加：
    - `capital_policy_id`
    - `capital_policy_version`
    - `capital_scaling_reason_codes_json`
    - `regime_bucket`
    - `calibration_gate_status`
- `why_ranked_json` / `budget_impact`
  - 必须能解释：
    - sizing 是被 calibration gate 卡住，还是被 scaling policy 压缩
    - 当前 regime 下为何 budget 更紧或更松

## 4. Persistence and Interfaces

固定新增 canonical policy table：

- `trading.capital_budget_policies`

固定扩展 canonical runtime table：

- `runtime.allocation_decisions`
  - `capital_policy_id`
  - `capital_policy_version`
  - `regime_bucket`
  - `calibration_gate_status`
  - `capital_scaling_reason_codes_json`

固定扩展 persisted operator surfaces：

- `ui.market_opportunity_summary`
- `ui.action_queue_summary`
- `ui.calibration_health_summary`

## 5. Test Plan

必须新增：

- `tests.test_calibration_hard_gate_acceptance`
- `tests.test_calibration_impacted_market_summary`
- `tests.test_scaling_aware_capital_policy_lookup`
- `tests.test_allocator_scaling_discipline_acceptance`
- `tests.test_p8_operator_surface_acceptance`

必须扩展：

- `tests.test_weather_pricing`
- `tests.test_opportunity_service_ranking_v2`
- `tests.test_ui_action_queue_summary`
- `tests.test_ui_data_access`
- `tests.test_ui_pages`
- `tests.test_truth_source_checks`
- `tests.test_cold_path_orchestration`
- `tests.test_migrations`
- doc/index hygiene tests

closeout 专项 acceptance：

- `tests.test_calibration_gate_default_clear`
- `tests.test_calibration_gate_fallback_surfaces`
- `tests.test_market_chain_degraded_source_preserves_gate_fields`

固定 acceptance：

- stale calibration 不再只作为 soft penalty，必须改变 actionability / operator bucket
- degraded_or_missing calibration 的 non-actionable 语义必须稳定持久化到 UI surfaces
- scaling-aware capital policy lookup 与 allocator output deterministic
- `Markets` / `System` 不重算 gate，只消费 persisted state
- `git diff --check` 通过

## 6. Explicit P9 Deferrals

`P8` 固定不做：

- delivery / scaling service 化 follow-on
- 更泛化的 multi-domain capital routing
- 新页面或更重的 operator workflow shell

这些保留给 `Phase 9` 再具体化。
