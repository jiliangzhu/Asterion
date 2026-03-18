# Asterion P1 Implementation Plan

**版本**: v1.1
**更新日期**: 2026-03-09
**阶段**: `P1`
**状态**: 已关闭
**目标**: 跑通 Weather MVP 的 `watch-only / replay / cold path` 主链路，并把 AlphaDesk 中仍会阻塞 P1 的剩余基础模块迁入 Asterion。

---

## 1. P1 定位

`P1` 是第一个真正进入业务主链路实现的阶段，但仍然不做真实交易。

本阶段只做六类事情：

1. 迁入 P1 所需的 AlphaDesk 剩余可复用基础模块
2. 建立 Weather market discovery 与 Rule2Spec -> ResolutionSpec 闭环
3. 建立 station-first onboarding 与 station metadata 落库
4. 建立 forecast adapters、cache、forecast run persistence
5. 建立 pricing / watch-only opportunity / replay 主链路
6. 建立 UMA watcher finalized watermark、event replay、evidence linkage 与最小 operator 读路径

`P1` 完成后，项目应达到：

- 不再需要依赖 AlphaDesk 才能继续开发 Weather watch-only 主链路
- Asterion 可以独立产出 `market -> spec -> forecast -> fair value -> watch-only snapshot`
- Asterion 可以独立运行 `UMA watcher -> replay -> evidence package -> redeem readiness suggestion`
- operator 可以在只读路径上查看 market/spec/forecast/fair value/watcher 状态

---

## 2. P1 Source Of Truth

本阶段实施以以下文档为准：

- [Asterion_Project_Plan.md](../../../../00-overview/Asterion_Project_Plan.md)
- [DEVELOPMENT_ROADMAP.md](../../../../00-overview/DEVELOPMENT_ROADMAP.md)
- [Documentation_Index.md](../../../../00-overview/Documentation_Index.md)
- [Implementation_Index.md](../../../Implementation_Index.md)
- [AlphaDesk_Migration_Ledger.md](../migration-ledger/AlphaDesk_Migration_Ledger.md)
- [AlphaDesk_Migration_Ledger.md](../migration-ledger/AlphaDesk_Migration_Ledger.md)
- [Forecast_Ensemble_Design.md](../../../../40-weather/Forecast_Ensemble_Design.md)
- [UMA_Watcher_Design.md](../../../../40-weather/UMA_Watcher_Design.md)
- [OMS_Design.md](../../../../30-trading/OMS_Design.md)
- [Market_Capability_Registry_Design.md](../../../../30-trading/Market_Capability_Registry_Design.md)
- [Hot_Cold_Path_Architecture.md](../../../../20-architecture/Hot_Cold_Path_Architecture.md)

如果实施中发现冲突，优先处理顺序：

1. 对应 subsystem 设计文档
2. `Asterion_Project_Plan.md`
3. `AlphaDesk_Migration_Ledger.md`
4. 本实施文档

---

## 3. P1 退出条件

`P1` 完成必须同时满足：

1. `ws_subscribe.py`、`ws_agg_v3.py`、`strategy_base.py`、`watch_only_gate_v3.py`、`health_monitor_v1.py`、`ui_db_replica.py` 已迁入或已有 Asterion 原生替代
2. Weather 市场能被发现并落库，且 Rule2Spec 可以产出闭合 `ResolutionSpec`
3. station-first onboarding 已闭合，`station_id + lat/lon + timezone` 能稳定落库
4. forecast adapters、cache key、forecast run persistence 已闭合
5. pricing / fair value / watch-only opportunities 已能稳定落库
6. UMA watcher finalized watermark、event replay、event idempotency、evidence linkage 已闭合
7. operator 只读面可以查看 P1 主链路的关键实体和异常
8. `P1` 路径的实现不再需要回头查 AlphaDesk 作为必须参考

---

## 4. P1 不做什么

`P1` 不做以下事情：

- 不做真实下单
- 不做 paper execution
- 不做 live signing
- 不做自动 dispute
- 不做自动 redeem
- 不做 execution gate 的经济公式收口
- 不做 portfolio / journal / signal_to_order 的完整执行链路
- 不做完整 operator UI 产品化页面

---

## 5. P1 关键代码落点

`P1` 结束时，代码树至少应落到下面这个层次：

```text
asterion_core/
  ws/
    ws_subscribe.py
    ws_agg_v3.py
  runtime/
    strategy_base.py
  execution/
    watch_only_gate_v3.py
  monitoring/
    health_monitor_v1.py
  ui/
    ui_db_replica.py

domains/weather/
  scout/
  spec/
  forecast/
  pricing/
  resolution/
```

说明：

- `P1` 的关键不是把所有目录写满，而是把 watch-only 主链路和相关可观测性先闭合
- `P2` 才继续迁 execution foundation / journal / risk / readiness

---

## 6. P1 六个工作流

### 6.1 工作流 A: AlphaDesk P1-blocker 剩余迁移

目标：

- 让 `watch-only / replay / cold path` 的基础运行壳不再依赖 AlphaDesk 源码

必须产出：

- `asterion_core/ws/ws_subscribe.py`
- `asterion_core/ws/ws_agg_v3.py`
- `asterion_core/runtime/strategy_base.py`
- `asterion_core/execution/watch_only_gate_v3.py`
- `asterion_core/monitoring/health_monitor_v1.py`
- `asterion_core/ui/ui_db_replica.py`
- 对应 module notes
- 更新后的迁移台账

迁移顺序建议：

1. `ws_subscribe.py`
2. `ws_agg_v3.py`
3. `strategy_base.py`
4. `watch_only_gate_v3.py`
5. `health_monitor_v1.py`
6. `ui_db_replica.py`

要求：

- 不保留 `ALPHADESK_*` env
- 所有 WS/health/ui replica 事件命名对齐 Asterion
- 迁入后同步更新 [AlphaDesk_Migration_Ledger.md](../migration-ledger/AlphaDesk_Migration_Ledger.md)

### 6.2 工作流 B: Market Discovery 与 Rule2Spec

目标：

- 建立 Weather 市场发现和规则结构化主链路

必须产出：

- Weather market discovery job
- `weather_markets` 或等价市场落库
- Rule2Spec parser
- 人工审核入口
- 闭合 `ResolutionSpec`

要求：

- 只保留 `station-first`
- 不允许 `city-first` 回流
- geocode 只用于 onboarding / spec 生成，不进入热路径

### 6.3 工作流 C: Station-first Onboarding

目标：

- 形成 `station_id + lat/lon + timezone` 闭合 spec

必须产出：

- `StationMapper.resolve_from_spec_inputs()`
- station metadata persistence
- station review / override 能力
- spec_version 管理

要求：

- `station_id` 变成 Forecast / Resolution 共用主键之一
- `YES/NO`、city name 只能做展示信息，不能代替内部实体主键

### 6.4 工作流 D: Forecast Pipeline

目标：

- 建立可 replay 的 forecast 数据链路

必须产出：

- Open-Meteo adapter
- NWS adapter
- adapter router / fallback
- forecast cache
- `weather_forecast_runs` 落库
- 统一 cache key

要求：

- cache key 至少包含 `market_id / station_id / spec_version / source / model_run / forecast_target_time`
- 所有 forecast runs 必须可重放
- source fallback 必须可观测

### 6.5 工作流 E: Pricing / Watch-Only / Read Surface

目标：

- 跑通 `forecast -> fair value -> watch-only snapshot -> operator read`

必须产出：

- Weather pricing engine
- fair value persistence
- watch-only opportunity snapshots
- 最小 operator read surface
- UI replica 最小数据同步

要求：

- watch-only output 不进入真实下单
- 必须保留足够的 provenance 供后续 replay / review

### 6.6 工作流 F: UMA Watcher Replay 与 Evidence Package

目标：

- 建立 watch-only 阶段可运行的 resolution 监控主链路

必须产出：

- finalized block watermark
- on-chain event ingest
- restart replay
- event idempotency
- settlement verification persistence
- evidence package linkage
- redeem readiness suggestion

要求：

- proposal 权威状态只来自链上事件/链上读取/finalized watermark
- wall clock 只用于调度建议
- settled 后不再用 challenge window 推导最终状态

---

## 7. P1 任务拆解

### P1-01 迁入 WS 订阅底座

输出：

- `asterion_core/ws/ws_subscribe.py`
- 对应 module note

完成条件：

- 最小订阅 API 可导入
- 可支撑后续 market / orderbook / resolution 事件流接入

### P1-02 迁入 WS 聚合底座

输出：

- `asterion_core/ws/ws_agg_v3.py`
- 对应 module note

完成条件：

- 事件聚合壳可独立运行
- replay / watch-only 可共享同一聚合入口

### P1-03 迁入 strategy_base

输出：

- `asterion_core/runtime/strategy_base.py`

完成条件：

- `StrategyContext` 或等价对象可被 weather watch-only runtime 使用

### P1-04 迁入 watch_only_gate_v3

输出：

- `asterion_core/execution/watch_only_gate_v3.py`

完成条件：

- watch-only 机会输出有统一 gate 壳

### P1-05 迁入 health_monitor_v1

输出：

- `asterion_core/monitoring/health_monitor_v1.py`

完成条件：

- queue / ws / degrade 健康指标可采集

### P1-06 迁入 ui_db_replica

输出：

- `asterion_core/ui/ui_db_replica.py`

完成条件：

- 只读副本复制与校验主链可运行

### P1-07 实现 Weather market discovery

输出：

- Weather 市场发现 job
- market persistence

完成条件：

- 新市场能被发现并结构化落库

### P1-08 实现 Rule2Spec

输出：

- Rule2Spec parser
- 人工审核入口
- `ResolutionSpec`

完成条件：

- spec 不再缺 `station_id`
- 规则结构化后可直接服务 forecast / settlement verifier

### P1-09 实现 station-first onboarding

输出：

- station mapper
- station metadata
- override 流程

完成条件：

- geocode 不进入热路径
- station metadata 与 spec_version 闭合

### P1-10 实现 forecast adapters 与 cache

输出：

- Open-Meteo adapter
- NWS adapter
- forecast cache

完成条件：

- cache key 稳定
- fallback 行为可观测

### P1-11 实现 forecast run persistence

输出：

- `weather_forecast_runs` 写入链路

完成条件：

- replay / cold path 可重建 forecast run

### P1-12 实现 pricing / watch-only snapshots

输出：

- pricing engine
- fair value persistence
- watch-only opportunities

完成条件：

- fair value 与 forecast provenance 可关联

### P1-13 实现 UMA watcher replay

输出：

- finalized watermark
- event replay
- processed events

完成条件：

- restart replay 不重复入账
- event idempotency 已闭合

### P1-14 实现 settlement verification 与 evidence package

输出：

- settlement verification persistence
- evidence package linkage
- redeem readiness suggestion

完成条件：

- 每个 settled proposal 都能追到 evidence package

### P1-15 实现 operator 只读面

输出：

- operator 最小只读面、等价视图或 runbook

完成条件：

- 可查看 market / spec / forecast / fair value / watcher 状态
- 当前阶段允许以 `DuckDB + UI replica + runbook` 作为 canonical 交付，不强制要求产品化页面

---

## 8. P1 测试与验证

截至 `2026-03-09`，`P1-01` 到 `P1-15` 均已完成。正式关闭依据见：

- [P1_Closeout_Checklist.md](../checklists/P1_Closeout_Checklist.md)
- [P1_Watch_Only_Replay_Cold_Path_Runbook.md](../runbooks/P1_Watch_Only_Replay_Cold_Path_Runbook.md)

必须补的测试类别：

- WS subscribe / aggregate smoke tests
- strategy_base import / construct tests
- weather market discovery parser tests
- Rule2Spec / station-first contract tests
- forecast cache key and persistence tests
- pricing / opportunity snapshot tests
- UMA watcher replay / watermark / idempotency tests
- UI replica smoke tests

最低验证命令：

```bash
source .venv/bin/activate
python3 -m unittest discover -s tests -v
```

如新增独立 test suites，也必须在本文件和 [Implementation_Index.md](../../../Implementation_Index.md) 同步登记。

---

## 9. P1 文档产物要求

`P1` 期间新增的实施文档，统一按下面规则放置：

- 阶段计划放 `docs/10-implementation/phase-plans/`
- 检查清单放 `docs/10-implementation/checklists/`
- runbook 放 `docs/10-implementation/runbooks/`
- 迁移台账放 `docs/10-implementation/versions/v1.0/migration-ledger/`
- module note 放 `docs/10-implementation/versions/v1.0/module-notes/`

`P1` 第一批建议补的 module notes：

- `AlphaDesk_ws_subscribe_Module_Note.md`
- `AlphaDesk_ws_agg_v3_Module_Note.md`
- `AlphaDesk_strategy_base_Module_Note.md`
- `AlphaDesk_watch_only_gate_v3_Module_Note.md`
- `AlphaDesk_health_monitor_v1_Module_Note.md`
- `AlphaDesk_ui_db_replica_Module_Note.md`

---

## 10. P1 完成后的下一步

`P1` 完成后，立刻进入：

1. `P2` execution foundation
2. runtime skeleton 剩余模块迁移
3. trade ticket / signal_to_order / execution_gate / portfolio / journal 迁移适配
4. readiness / ui_lite_db 收口
5. AlphaDesk Exit Gate 审查

也就是开始收口“独立脱离 AlphaDesk 参考仓库”的最后一批基础模块，而不是过早切独立 Git 仓库。

---

## 11. P1 Closeout 文档

`P1` 关闭后的 canonical 交接文档如下：

- [P1_Closeout_Checklist.md](../checklists/P1_Closeout_Checklist.md)
- [P1_Watch_Only_Replay_Cold_Path_Runbook.md](../runbooks/P1_Watch_Only_Replay_Cold_Path_Runbook.md)

后续如果 `P2` 发现入口、运行顺序或只读面发生变化，应优先更新 runbook，而不是在 issue、聊天记录或临时文档中重新定义。
