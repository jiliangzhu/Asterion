# P2 Cold-Path Orchestration Job Map Runbook

**版本**: v1.0  
**更新日期**: 2026-03-10  
**阶段**: `P2`  
**状态**: 生效中  

---

## 1. 目标

本 runbook 定义 `P2-07` 到 `P2-09` 的 Weather cold-path orchestration canonical 入口。

source of truth 顺序：

1. `dagster_asterion/job_map.py`
2. 本 runbook
3. `docs/10-implementation/phase-plans/P2_Implementation_Plan.md`

说明：

- orchestration 只编排，不重写业务 contract
- `Dagster` 是可选依赖，不是业务逻辑定义者
- 未安装 `Dagster` 时，仍应能通过纯 Python handler 运行 cold-path job
- 3 个 Weather agent review job 保持 `manual`，不作为 deterministic handler 的 inline hook

---

## 2. Canonical Jobs

### `weather_spec_sync`

- 模式：`scheduled`
- handler：`run_weather_spec_sync`
- 输入表：
  - `weather.weather_markets`
  - `weather.weather_station_map`
- 输出表：
  - `weather.weather_market_specs`
- 作用：
  - 把 market discovery 结果解析成 station-first spec
  - 通过 `StationMapper` 闭合站点元数据

### `weather_forecast_refresh`

- 模式：`scheduled`
- handler：`run_weather_forecast_refresh`
- 输入表：
  - `weather.weather_market_specs`
- 输出表：
  - `weather.weather_forecast_runs`
- 作用：
  - 从 canonical spec 生成 forecast runs
  - 保持 forecast provenance 在 `weather.weather_forecast_runs`

### `weather_forecast_replay`

- 模式：`manual`
- handler：`run_weather_forecast_replay_job`
- 输入表：
  - `weather.weather_market_specs`
  - `weather.weather_forecast_runs`
  - `weather.weather_fair_values`
  - `weather.weather_watch_only_snapshots`
- 输出表：
  - `weather.weather_forecast_replays`
  - `weather.weather_forecast_replay_diffs`
- 作用：
  - 按 replay key 重放 forecast/pricing
  - 产出 diff 审计记录

### `weather_watcher_backfill`

- 模式：`scheduled`
- handler：`run_weather_watcher_backfill_job`
- 输入表：
  - `resolution.block_watermarks`
- 输出表：
  - `resolution.uma_proposals`
  - `resolution.proposal_state_transitions`
  - `resolution.processed_uma_events`
  - `resolution.block_watermarks`
  - `resolution.watcher_continuity_checks`
  - `resolution.watcher_continuity_gaps`
- 作用：
  - 以 finalized watermark 为起点回放 UMA events
  - 记录 multi-RPC fallback 与 continuity/gap 审计

### `weather_resolution_reconciliation`

- 模式：`scheduled`
- handler：`run_weather_resolution_reconciliation`
- 输入表：
  - `resolution.uma_proposals`
  - `resolution.settlement_verifications`
- 输出表：
  - `resolution.settlement_verifications`
  - `resolution.proposal_evidence_links`
  - `resolution.redeem_readiness_suggestions`
- 作用：
  - 持久化 verification / evidence link
  - 生成 redeem suggestion
- 边界：
  - orchestration 层不自动推导 `expected_outcome`
- 如需 verification，必须显式传入 verification input

### `weather_rule2spec_review`

- 模式：`manual`
- handler：`run_weather_rule2spec_review_job`
- 输入表：
  - `weather.weather_markets`
  - `weather.weather_station_map`
  - `weather.weather_market_specs`
- 输出表：
  - `agent.invocations`
  - `agent.outputs`
  - `agent.reviews`
  - `agent.evaluations`
- 作用：
  - 审阅 deterministic Rule2Spec 结果
  - 输出 station-first patch 建议与 risk flags

### `weather_data_qa_review`

- 模式：`manual`
- handler：`run_weather_data_qa_review_job`
- 输入表：
  - `weather.weather_market_specs`
  - `weather.weather_forecast_runs`
  - `weather.weather_forecast_replays`
  - `weather.weather_forecast_replay_diffs`
  - `weather.weather_fair_values`
  - `weather.weather_watch_only_snapshots`
- 输出表：
  - `agent.invocations`
  - `agent.outputs`
  - `agent.reviews`
  - `agent.evaluations`
- 作用：
  - 审阅 replay diff、source fallback 与 pricing provenance
  - 不改写 forecast / pricing canonical tables

### `weather_resolution_review`

- 模式：`manual`
- handler：`run_weather_resolution_review_job`
- 输入表：
  - `resolution.uma_proposals`
  - `resolution.settlement_verifications`
  - `resolution.proposal_evidence_links`
  - `resolution.redeem_readiness_suggestions`
  - `resolution.watcher_continuity_checks`
- 输出表：
  - `agent.invocations`
  - `agent.outputs`
  - `agent.reviews`
  - `agent.evaluations`
- 作用：
  - 审阅 settlement verification / evidence linkage / redeem readiness
  - `Resolution Sentinel` 只保留为 legacy alias

---

## 3. Job Dependencies

- `weather_spec_sync -> weather_forecast_refresh`
- `weather_forecast_refresh -> weather_forecast_replay`
- `weather_watcher_backfill -> weather_resolution_reconciliation`
- `weather_spec_sync -> weather_rule2spec_review`
- `weather_forecast_replay -> weather_data_qa_review`
- `weather_resolution_reconciliation -> weather_resolution_review`
- `weather_forecast_replay` 与 `weather_watcher_backfill` 互相独立

本阶段不引入：

- execution foundation jobs
- Dagster assets
- cross-domain orchestration

---

## 4. Default Schedules

- `weather_spec_sync_daily`
  - cron: `15 0 * * *`
  - 时区: `UTC`
  - 默认启用
- `weather_forecast_refresh_hourly`
  - cron: `10 * * * *`
  - 时区: `UTC`
  - 默认启用
- `weather_forecast_replay_manual`
  - 无自动调度语义
  - 默认关闭
- `weather_watcher_backfill_bihourly`
  - cron: `20 */2 * * *`
  - 时区: `UTC`
  - 默认启用
- `weather_resolution_reconciliation_bihourly`
  - cron: `35 */2 * * *`
  - 时区: `UTC`
  - 默认启用

---

## 5. 运行入口

纯 Python canonical 入口：

- `dagster_asterion/handlers.py`

可选 Dagster 壳：

- `dagster_asterion/resources.py`
- `dagster_asterion/schedules.py`
- `dagster_asterion/__init__.py`

说明：

- 未安装 `Dagster` 时，`dagster_asterion` 顶层导入不能失败
- 安装 `Dagster` 时，可构造 `Definitions` / schedule shell
- `job_map.py` 始终是唯一 source of truth

---

## 6. 失败与重跑规则

- `weather_spec_sync`
  - 解析失败直接 fail，不在 orchestration 层吞错误
- `weather_forecast_refresh`
  - 依赖 canonical spec；如 spec 不完整，应 fail 而不是隐式跳过
- `weather_forecast_replay`
  - 只接受显式 replay request
- `weather_watcher_backfill`
  - 失败不得推进 watermark
  - continuity/gap 结果必须可审计
- `weather_resolution_reconciliation`
  - 无 verification input 时可只生成 redeem suggestion
  - 不得在编排层自行推导 expected outcome
- 3 个 agent review jobs
  - 只写 `agent.*`
  - 不得改写 `weather.* / resolution.* / trading.*`
  - 失败不阻塞 deterministic 主链路

---

## 7. 验收检查

- `dagster_asterion/job_map.py` 能列出 8 个 canonical job，其中 3 个为 manual review jobs
- 未安装 `Dagster` 时，`dagster_asterion` 可导入
- 安装 `Dagster` 时，可构造 definitions / schedules
- handler smoke tests 能确认 8 个 job 都调用到对应业务 pipeline
- 现有 forecast replay / watcher backfill / settlement verification 回归测试保持通过
