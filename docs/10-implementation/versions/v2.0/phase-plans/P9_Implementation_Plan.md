# Asterion P9 Implementation Plan

**版本**: v1.0  
**更新日期**: 2026-03-21  
**阶段**: `v2.0 / Phase 9`  
**状态**: current tranche implementation plan; core implemented / closeout pending  
**主题**: Operator Surface Delivery and Throughput Scaling

---

> 本文件是当前 `v2.0 / Phase 9` tranche-specific implementation plan。  
> umbrella active implementation contract 仍是 [V2_Implementation_Plan.md](./V2_Implementation_Plan.md)。  
> 当前仓库阶段状态统一表达为：`P4 accepted; post-P4 remediation accepted; v2.0 implementation active`。  
> 最近 accepted tranche record 仍是 [P8_Implementation_Plan.md](./P8_Implementation_Plan.md)；`Phase 0` 到 `Phase 8` 已 accepted，`Phase 9` 当前处于 current tranche in-progress / closeout-pending 状态。

## 1. Summary

`P9` 不再继续扩 ranking / calibration / capital policy 主链。  
`P6` 到 `P8` 已经把 deployable rerank、empirical-primary economics、calibration hard gate、scaling-aware capital discipline推进到了 canonical 主链。

`P9` 只解决当前 `V2` umbrella 中仍明确存在的两类 delivery / operator 缺口：

- truth-source / read-model / fallback 仍有 delivery drift 风险
- `Home / Markets / Execution / System` 还没有成为高吞吐、可持续依赖的 operator surface

固定边界：

- 不扩 live boundary
- 不新增页面
- 不新开 `risk.*`
- 不重写 ranking / allocator / calibration 主链
- 不引入新的 parallel truth-source
- 不把 multi-domain capital routing 拉进当前 tranche

按当前工作区 reality，`P9` 主干已经落地；当前剩余工作固定收口为：

- delivery contract closeout
- acceptance hardening
- final phase closeout sync

## 2. Goals and Non-Goals

### 2.1 Goals

- 把 operator surface refresh 从 readiness 副作用升级成显式 delivery seam
- 把 degraded / fallback source 状态升级成 persisted、可测试、可运营的 delivery contract
- 让现有页面的 `ready_now / review_required / blocked` 同时受交易语义和 surface delivery 可靠性约束

### 2.2 Non-Goals

- 不扩 constrained live boundary
- 不新增新的 operator workflow page
- 不 service 化 UI refresh pipeline
- 不提前把 `P9` 做成更重的 backend/service refactor

## 3. Workstreams

### WS1. Explicit Operator Surface Refresh Orchestration

固定新增显式冷路径 job：

- `weather_operator_surface_refresh`

固定语义：

- refresh UI replica
- build UI lite
- validate `ui.read_model_catalog`
- validate `ui.truth_source_checks`
- materialize operator-surface delivery status

固定接线：

- shared helper：`run_operator_surface_refresh(...)`
- 以下路径统一复用：
  - `weather_live_prereq_readiness`
  - `weather_paper_execution`
  - `weather_resolution_review`
  - `scripts/refresh_operator_console_surfaces.py`

固定 schedule：

- key：`weather_operator_surface_refresh_hourly`
- cron：`58 * * * *`
- timezone：`UTC`
- `enabled_by_default = True`

### WS2. Persisted Surface Delivery Contract

固定新增 runtime 审计表：

- `runtime.operator_surface_refresh_runs`

固定新增 UI-only read models：

- `ui.surface_delivery_summary`
- `ui.system_runtime_summary`

`ui.surface_delivery_summary` 最小字段：

- `surface_id`
- `primary_table`
- `delivery_status`
- `primary_source`
- `fallback_origin`
- `truth_check_status`
- `truth_check_issue_count`
- `row_count`
- `last_refresh_ts`
- `degraded_reason_codes_json`
- `primary_score_label`

`ui.system_runtime_summary` 最小字段：

- `generated_at`
- `latest_surface_refresh_run_id`
- `latest_surface_refresh_status`
- `ui_replica_status`
- `ui_lite_status`
- `readiness_status`
- `weather_chain_status`
- `degraded_surface_count`
- `read_error_surface_count`
- `calibration_hard_gate_market_count`
- `pending_operator_review_count`

### WS3. Fallback Governance and Delivery-Aware Operator Gating

固定 source precedence：

1. `ui_lite`
2. `runtime_db`
3. `smoke_report`

固定扩展 persisted surfaces：

- `ui.market_opportunity_summary`
- `ui.action_queue_summary`

最小新增字段：

- `surface_delivery_status`
- `surface_fallback_origin`
- `surface_delivery_reason_codes_json`
- `surface_last_refresh_ts`

固定 operator bucket 升级：

- `ready_now`
  - 交易语义可执行 + `surface_delivery_status = ok`
- `review_required`
  - 交易语义可执行，但 surface delivery 为 `degraded_source` 或 `stale`
- `blocked`
  - `surface_delivery_status in {read_error, missing}`
- `research_only`
  - 保留原有交易 / 校准语义

### WS4. Throughput Scaling Inside Existing Pages

固定不新增页面，只增强现有页。

`Home`

- action queue 继续读 `ui.action_queue_summary`
- delivery-aware focus slices：
  - `ready_now`
  - `review_required`
  - `blocked`
- 行上固定显示：
  - `ranking_score`
  - `surface_delivery_status`
  - `surface_fallback_origin`
  - `surface_last_refresh_ts`

`Markets`

- selected market detail 增加 `Surface Delivery` 区块
- 只显示 persisted delivery fields，不在页面内重算

`System`

- 主叙事切成 `persisted runtime summary + surface delivery summary`
- file paths 下沉到 debug/details
- 顶部 summary 必须显示：
  - latest surface refresh status
  - degraded surface count
  - read error surface count
  - latest weather chain status
  - calibration hard gate market count

## 4. Public Interfaces

固定新增或扩展：

- `runtime.operator_surface_refresh_runs`
- `ui.surface_delivery_summary`
- `ui.system_runtime_summary`
- `ui.market_opportunity_summary`
  - `surface_delivery_status`
  - `surface_fallback_origin`
  - `surface_delivery_reason_codes_json`
  - `surface_last_refresh_ts`
- `ui.action_queue_summary`
  - 同名字段
- `load_home_decision_snapshot()`
  - `surface_delivery_summary`
- `load_market_chain_analysis_data()`
  - market rows 带 delivery fields
- `load_system_surface_contract()`
  - primary table 切到 `ui.system_runtime_summary`

固定不变：

- `ranking_score` 仍是唯一主排序字段
- `why_ranked_json` 继续只解释交易 / 部署语义
- `trading.*` 仍是 canonical execution ledger
- `runtime.*` 只承接 refresh audit / runtime delivery state

## 5. Tests and Acceptance

必须新增：

- `tests.test_operator_surface_refresh_job`
- `tests.test_surface_delivery_summary`
- `tests.test_system_runtime_summary`
- `tests.test_fallback_precedence_acceptance`
- `tests.test_delivery_gate_operator_bucket`
- `tests.test_refresh_operator_console_surfaces_script`

必须扩展：

- `tests.test_cold_path_orchestration`
- `tests.test_ui_loader_contracts`
- `tests.test_truth_source_checks`
- `tests.test_ui_data_access`
- `tests.test_ui_pages`
- `tests.test_ui_read_model_catalog`
- `tests.test_ui_golden_surfaces`
- `tests.test_ui_db_replica`
- `tests.test_real_weather_chain_smoke`
- doc/index hygiene tests

固定 acceptance：

- `weather_operator_surface_refresh` 可独立运行并 materialize persisted delivery surfaces
- `weather_live_prereq_readiness`、`weather_paper_execution`、`weather_resolution_review` 与 shared refresh helper 结果一致
- 同一 fixture 下，`Home / Markets / System` 看到的 delivery status / fallback origin 一致
- `runtime_db` 可用时，UI 不会错误退回 `smoke_report`
- degraded source rows 不会继续出现在 `ready_now`
- `git diff --check` 通过

## 6. Current Implementation Status

当前 tranche 已落地的主干：

- 显式 `weather_operator_surface_refresh` job 与 schedule
- `runtime.operator_surface_refresh_runs`
- `ui.surface_delivery_summary`
- `ui.system_runtime_summary`
- `ui_lite > runtime_db > smoke_report` fallback precedence
- delivery-aware operator bucket gating
- `Home / Markets / System` persisted delivery fields

当前 tranche 当前仍需持续守住的重点：

- `surface_delivery_reason_codes_json` 等 persisted delivery fields 必须继续由 catalog / truth checks / golden tests 锁死
- `ui_db_replica`、`real_weather_chain_smoke`、`Home / Markets / System` consistency、refresh metadata parity 必须继续由 acceptance 防漂移
- 当前阶段状态表达固定为：`Phase 9 in progress`，且 `core implemented / closeout pending`
- operator surface refresh 与 readiness / paper / resolution paths 的长期一致性
- 文档 / startup / sidebar 当前 tranche 口径同步

## 7. Assumptions and Defaults

- `P9` 是当前 tranche-specific implementation plan
- `P8` 保持 accepted closeout record，不回头重做 calibration / capital policy 主链
- `P9` 默认允许新增：
  - `runtime.operator_surface_refresh_runs`
  - `ui.surface_delivery_summary`
  - `ui.system_runtime_summary`
- `P9` 不新增页面，只增强现有 `Home / Markets / System`
- `P9` 不把 multi-domain capital routing、更重的 workflow shell、service backend 重构拉进当前 tranche
