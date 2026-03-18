# P1 Watch-Only Replay Cold-Path Runbook

**版本**: v1.0
**更新日期**: 2026-03-09
**目标**: 固定 `P1` 结束时 Weather MVP 的 `watch-only / replay / cold path` 运行入口、持久化落点、operator 只读面和 human-in-the-loop 边界，供 `P2` 开发直接接续。

---

## 1. 适用范围

本 runbook 只覆盖 `P1` 已落地的最小可运行链路：

- Weather 市场发现
- Rule2Spec / station-first onboarding
- forecast adapters / cache / persistence
- pricing / fair value / watch-only snapshots
- UMA watcher replay
- settlement verification / evidence linkage / redeem suggestion
- UI replica 与 operator 只读路径

本 runbook 不覆盖：

- 真实下单
- paper execution
- 自动 dispute
- 自动 redeem
- Dagster 编排产品化

---

## 2. Canonical 入口

### 2.1 Watch-only 主链路

市场发现入口：

- `domains/weather/scout/market_discovery.py`
- canonical function: `run_weather_market_discovery()`

规则与 spec 入口：

- `domains/weather/spec/rule2spec.py`
- canonical functions:
  - `parse_rule2spec_draft()`
  - `build_resolution_spec_via_station_mapper()`
  - `build_weather_market_spec_record_via_station_mapper()`

station mapping 入口：

- `domains/weather/spec/station_mapper.py`
- canonical APIs:
  - `StationMapper.resolve_from_spec_inputs()`
  - `StationMapper.get_station_metadata()`

forecast 入口：

- `domains/weather/forecast/service.py`
- `domains/weather/forecast/adapters.py`
- `domains/weather/forecast/persistence.py`
- canonical APIs:
  - `build_forecast_request()`
  - `ForecastService.get_forecast()`
  - `build_forecast_run_record()`

pricing 入口：

- `domains/weather/pricing/engine.py`
- `domains/weather/pricing/persistence.py`
- canonical APIs:
  - `build_binary_fair_values()`
  - `build_watch_only_snapshot()`

### 2.2 Replay / Resolution 主链路

watcher replay 入口：

- `domains/weather/resolution/watcher_replay.py`
- canonical APIs:
  - `replay_uma_events()`
  - `enqueue_uma_replay_writes()`

settlement verification 入口：

- `domains/weather/resolution/verification.py`
- `domains/weather/resolution/persistence.py`
- canonical APIs:
  - `build_settlement_verification()`
  - `build_evidence_package_link()`
  - `RedeemScheduler.decide()`
  - `build_redeem_readiness_record()`

### 2.3 Cold-path / Storage / Read Surface

storage 入口：

- `asterion_core/storage/db_migrate.py`
- `asterion_core/storage/write_queue.py`
- `asterion_core/storage/writerd.py`

replica / read surface 入口：

- `asterion_core/ui/ui_db_replica.py`
- `asterion_core/monitoring/health_monitor_v1.py`

说明：

- `P1` 结束时的 canonical operator 读路径不是产品化 UI 页面，而是 `DuckDB tables + UI replica + runbook`
- `P2` 再决定是否补更强的 UI / orchestration 壳

---

## 3. Canonical 持久化落点

### 3.1 Weather

- `weather.weather_markets`
- `weather.weather_station_map`
- `weather.weather_market_specs`
- `weather.weather_forecast_runs`
- `weather.weather_fair_values`
- `weather.weather_watch_only_snapshots`

### 3.2 Resolution

- `resolution.uma_proposals`
- `resolution.proposal_state_transitions`
- `resolution.processed_uma_events`
- `resolution.block_watermarks`
- `resolution.settlement_verifications`
- `resolution.proposal_evidence_links`
- `resolution.redeem_readiness_suggestions`

### 3.3 Ops

- `meta.schema_versions`
- `meta.watermarks`
- `write_queue_tasks`

---

## 4. 当前推荐运行顺序

1. 应用 schema migrations
2. 运行 Weather market discovery
3. 运行 Rule2Spec + StationMapper onboarding
4. 运行 forecast fetch / cache / forecast run persistence
   - forecast adapters 产出离散化温度概率分布，不再使用单点值
   - HTTP 调用默认应经过 retry wrapper；允许可选 circuit breaker
   - 内存 cache 语义为 TTL + LRU，而不是无限缓存
5. 运行 pricing / fair value / watch-only snapshot persistence
6. 运行 UMA watcher replay
7. 运行 settlement verification / evidence linkage / redeem suggestion
8. 刷新 UI replica，供 operator 只读查看

说明：

- 当前阶段的入口以模块函数和表写入为主，不要求统一的 CLI 或 Dagster job
- 统一编排属于 `P2`

---

## 5. Operator 只读面

`P1` 结束时，operator 至少应查看以下对象：

- 市场发现结果：`weather.weather_markets`
- station mapping 与 override：`weather.weather_station_map`
- 审核后的 spec：`weather.weather_market_specs`
- 预测 provenance：`weather.weather_forecast_runs`
- 定价输出：`weather.weather_fair_values`
- watch-only 决策：`weather.weather_watch_only_snapshots`
- proposal 当前状态：`resolution.uma_proposals`
- proposal 状态转移：`resolution.proposal_state_transitions`
- 结算校验记录：`resolution.settlement_verifications`
- evidence package 关联：`resolution.proposal_evidence_links`
- redeem 建议：`resolution.redeem_readiness_suggestions`

当前 operator 视图目标不是“点按钮执行”，而是：

- 看见当前状态
- 追溯 provenance
- 发现异常 spec / fallback / replay / settlement mismatch

---

## 6. Human-In-The-Loop 边界

以下环节在 `P1` 结束时仍保持人工介入：

- station mapping override
- spec 审核与发布
- authoritative source 异常或多源证据冲突
- `human_review_required=true` 的 redeem suggestion
- dispute 是否发起

---

## 7. Replay 与状态判断规则

- proposal 权威状态只来自链上事件、链上读取和 finalized watermark
- wall clock 只能用于调度建议，不能定义 proposal 最终状态
- replay 必须依赖 processed event idempotency
- state transition 必须记录 `old_status -> new_status`
- settled 后不再使用 challenge/liveness 时间反推最终状态

---

## 8. 最低验证动作

推荐命令：

```bash
source .venv/bin/activate
python3 -m unittest discover -s tests -v
```

重点关注的集成测试：

- `tests/test_weather_market_discovery.py`
- `tests/test_rule2spec.py`
- `tests/test_station_mapper.py`
- `tests/test_forecast_persistence.py`
- `tests/test_weather_pricing.py`
- `tests/test_uma_watcher_replay.py`
- `tests/test_settlement_verification.py`

---

## 9. P2 接续点

`P2` 从以下点继续，而不是重开 `P1` 的基础链路：

- forecast replay / deterministic recompute
- watcher multi-RPC fallback / backfill 强化
- Dagster / cold-path orchestration
- execution foundation 剩余模块迁移
- journal / readiness / ui_lite_db
- AlphaDesk Exit Gate 收口
