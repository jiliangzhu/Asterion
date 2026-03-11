# Asterion P3 Implementation Plan

**版本**: v1.1
**更新日期**: 2026-03-11
**阶段**: `P3`  
**状态**: 已关闭
**目标**: 在 `P2` 已关闭的 deterministic `watch-only / replay / cold path / execution foundation` 底座之上，打通 `paper execution` 主链、operator 读面、审计与回放闭环，并为后续 `P4 live prerequisites` 建立明确的进入条件。

---

## 1. Phase Summary

`P3` 的唯一主题是 `paper execution`。

本阶段唯一允许推进的执行能力是：

1. 继续复用当前 Asterion 仓库里已经冻结并落地的 execution contracts
2. 在不触发真实资金 side effects 的前提下，把 `strategy -> ticket -> gate -> order -> fill -> inventory -> journal -> UI` 跑通
3. 收口 operator、reconciliation、daily ops 与 review surface
4. 为下一阶段 `live prerequisites` 提供明确的 readiness / closeout 入口

本阶段明确约束：

- source of truth 是**当前 Asterion 仓库代码**与**已冻结设计文档 contract**
- 不做 `live submitter`
- 不做 `real signer RPC`
- 不做 `real wallet` / `real chain` side effects
- 不做真实链上广播
- 不引入与现有 `RouteAction / CanonicalOrderContract / ExecutionContext / Order / Fill / Reservation / ExposureSnapshot` 平行的新执行接口

---

## 2. P3 Source Of Truth

本阶段实施以以下内容为准：

- [README.md](../../../README.md)
- [Asterion_Project_Plan.md](../../00-overview/Asterion_Project_Plan.md)
- [DEVELOPMENT_ROADMAP.md](../../00-overview/DEVELOPMENT_ROADMAP.md)
- [Documentation_Index.md](../../00-overview/Documentation_Index.md)
- [Implementation_Index.md](../Implementation_Index.md)
- [P2_Implementation_Plan.md](./P2_Implementation_Plan.md)
- [P2_Closeout_Checklist.md](../checklists/P2_Closeout_Checklist.md)
- [P1_Watch_Only_Replay_Cold_Path_Runbook.md](../runbooks/P1_Watch_Only_Replay_Cold_Path_Runbook.md)
- [P2_Cold_Path_Orchestration_Job_Map_Runbook.md](../runbooks/P2_Cold_Path_Orchestration_Job_Map_Runbook.md)
- [CLOB_Order_Router_Design.md](../../30-trading/CLOB_Order_Router_Design.md)
- [OMS_Design.md](../../30-trading/OMS_Design.md)
- [Market_Capability_Registry_Design.md](../../30-trading/Market_Capability_Registry_Design.md)
- [Signer_Service_Design.md](../../30-trading/Signer_Service_Design.md)
- `asterion_core/`
- `domains/weather/`
- `dagster_asterion/`
- `agents/`
- `sql/migrations/`
- `tests/`

### 2.1 当前代码级验证结论

在当前仓库内，以下结论已经由代码与测试支撑：

- `P2` closeout 已真实落地，不再只是文档口径
- `.venv/bin/python -m unittest discover -s tests -v` 当前通过 `138` 个测试
- system Python 可能缺少 `duckdb`，但仓库内 `.venv` 是 canonical 验证环境
- `P2` 已有真实实现的关键落点包括：
  - replay / continuity: `domains/weather/forecast/replay.py`、`domains/weather/resolution/backfill.py`、`domains/weather/resolution/watcher_replay.py`
  - cold-path orchestration: `dagster_asterion/job_map.py`、`dagster_asterion/handlers.py`、`dagster_asterion/resources.py`、`dagster_asterion/schedules.py`
  - execution foundation: `asterion_core/runtime/strategy_engine_v3.py`、`asterion_core/execution/trade_ticket_v1.py`、`asterion_core/execution/signal_to_order_v1.py`、`asterion_core/execution/execution_gate_v1.py`、`asterion_core/risk/portfolio_v3.py`、`asterion_core/journal/journal_v3.py`
  - readiness / read model: `asterion_core/monitoring/readiness_checker_v1.py`、`asterion_core/ui/ui_lite_db.py`
  - weather agents: `agents/weather/rule2spec_agent.py`、`agents/weather/data_qa_agent.py`、`agents/weather/resolution_agent.py`

### 2.2 Drift Closure

截至 `2026-03-11`，`P3` 开工前显式登记的文档漂移已完成收口：

#### Drift A Closed: README 已按当前代码树收口

- `README.md` 已把“当前已落地模块”与“未来规划内容”分开
- `order_router_v1.py`、paper adapter、quote-based fill simulator、OMS state machine 已同步为已落地代码
- `daily_review_agent.py` 继续保留为后续 automation / agent 化范围

#### Drift B Closed: Project Plan 已拆分 current / planned

- [Asterion_Project_Plan.md](../../00-overview/Asterion_Project_Plan.md) 顶部状态已同步到当前 `P3 已关闭 / P4 planning 可开始`
- 项目结构已拆成“当前已落地代码骨架”与“未来规划模块（未落地）”
- 未来设计仍保留，但不再与当前代码事实混写

#### Drift C Closed: Roadmap 编号已统一

- [DEVELOPMENT_ROADMAP.md](../../00-overview/DEVELOPMENT_ROADMAP.md) 已统一使用当前 `P0-P4` 口径
- 旧的 execution foundation / paper execution / live prerequisites 编号已被收口为历史说明或当前正式 phase

#### Drift D Closed at Documentation Level: 已有 canonical 表的文档角色已同步

以下表的**文档口径**已统一：

- `capability.execution_contexts`
- `trading.order_state_transitions`
- `trading.reconciliation_results`

当前结论是：

- 文档已不再把这些表误写成“已经 fully operational”
- 代码闭环已在 `P3` 中完成，并已进入 closeout / runbook / readiness 口径
- `P3` 的重点仍然是把这些现有 canonical 表真正接入 paper execution 主链，而不是发明平行新表

### 2.3 冲突处理顺序

实施中若出现冲突，优先级如下：

1. 当前代码与 migration
2. subsystem 设计文档
3. `P2_Closeout_Checklist.md`
4. 本实施文档
5. 未来规划段与当前代码骨架并存的总览文档

---

## 3. Scope / Non-Goals

### 3.1 P3 要做什么

`P3` 必须完成以下能力：

1. `paper order lifecycle`
2. `paper router / paper adapter / quote-based paper fill simulator`
3. OMS 状态流闭环
4. `reservation / inventory / exposure` 在 paper fills 下闭环
5. operator read model / runbook / reconciliation
6. paper run journal / daily ops / review flow
7. readiness for next phase

### 3.2 P3 不做什么

`P3` 明确不做：

- 不真实下单
- 不接真实 signer RPC
- 不接 KMS / Vault / HSM
- 不做真实链上广播
- 不做真实资金部署
- 不做生产级告警体系全量建设
- 不把 `paper` 与 `live` 拆成两套 execution contracts
- 不要求引入真实 orderbook persistence 才能开始 paper fill

---

## 4. Canonical P3 Execution Model

### 4.1 Canonical Data Flow

`P3` 的主链路固定为：

```text
weather.weather_watch_only_snapshots
-> strategy_engine_v3
-> runtime.strategy_runs
-> trade_ticket_v1
-> runtime.trade_tickets
-> signal_to_order_v1
-> capability.execution_contexts
-> execution_gate_v1
-> runtime.gate_decisions
-> order_router_v1
-> paper adapter / quote-based fill simulator
-> trading.orders
-> trading.order_state_transitions
-> trading.reservations
-> trading.fills
-> trading.inventory_positions
-> trading.exposure_snapshots
-> trading.reconciliation_results
-> runtime.journal_events
-> ui.execution_ticket_summary / ui.phase_readiness_summary
```

补充规则：

- `weather.weather_watch_only_snapshots` 仍是 execution candidate 的上游输入
- `runtime.*` 继续保存 run / ticket / gate / journal 审计事实
- `trading.*` 继续保存 order / fill / reservation / inventory / reconciliation canonical ledger
- UI 不直接重写业务语义，只消费 canonical tables

### 4.2 Paper Fill Baseline

`P3` 的 paper fill baseline 固定为 `quote-based`：

- 基于现有 `reference_price / fair_value / edge_bps / route_action`
- 使用 deterministic fill rules
- 不要求先引入真实 orderbook snapshots
- 不要求先实现 live submitter
- 不要求真实 signer

### 4.3 Human-In-The-Loop 边界

`P3` 结束时仍保持人工介入的环节：

- wallet / signer / key material 相关动作
- live 资金启用
- 对账异常处置
- readiness 最终放行
- 日报 / review 输出的最终采纳

---

## 5. Persistence Model

### 5.1 Canonical Ledger 原则

- `trading.*` 仍是 canonical execution ledger
- `runtime.*` 仍是运行时 / 审计层
- `agent.*` 仍只保存 review / evaluation，不改写 canonical execution state
- 不新建 `paper.*` schema

### 5.2 P3 必须真正用起来的已有表

`P3` 明确要把以下表从“已建表”推进到“已闭环”：

- `capability.execution_contexts`
- `trading.order_state_transitions`
- `trading.reconciliation_results`

### 5.3 新增表的默认约束

如果 `P3` 确实需要新增表，必须满足：

1. 不能与 `trading.orders / fills / reservations / inventory_positions / exposure_snapshots / reconciliation_results` 重复表达同一语义
2. 不能与 `runtime.strategy_runs / trade_tickets / gate_decisions / journal_events` 重复表达同一审计语义
3. 只能作为当前 canonical tables 的补充，不得成为平行账本

默认情况下，`P3` 不应新增 canonical schema，只应优先复用现有表。

---

## 6. Canonical P3 Workstreams

### 6.1 Workstream A: Paper Execution Orchestration

目标：

- 在现有 `dagster_asterion` 编排壳之上，增加 paper execution 的 canonical 入口
- 保持 orchestration 只负责编排，不重写业务 contract

### 6.2 Workstream B: Paper Router / Adapter / Fill Simulator

目标：

- 在现有 execution contracts 上实现 canonical router
- 增加 paper adapter 与 `quote-based` fill simulator
- 保持 route decision 与 paper execution 使用同一份 order contract

### 6.3 Workstream C: OMS State Machine Completion

目标：

- 把 `created -> reserved -> posted -> partial_filled -> filled / cancelled / expired / rejected` 状态机真正闭合
- 把 `order_state_transitions` 接入主链

### 6.4 Workstream D: Reservation / Inventory / Exposure Reconciliation

目标：

- 在 paper fills 下闭合 BUY / SELL reservation conversion
- 让 inventory / exposure / reconciliation 与 order/fill 一致

### 6.5 Workstream E: Operator Read Model

目标：

- 扩展已有 `ui.execution_ticket_summary`
- 增加 paper execution operator 读路径与 runbook
- 不再造平行 operator surface

### 6.6 Workstream F: Paper Run Journal / Daily Ops / Review Flow

目标：

- 把 `runtime.journal_events` 从 foundation demo 推进到 paper execution run journal
- 增加 daily ops / daily review 的输入面
- 保持 agent 在执行路径之外

### 6.7 Workstream G: P3 Readiness / Closeout

目标：

- 给 `P4 live prerequisites` 提供可执行的进入门槛
- 明确哪些能力已通过、哪些仍需人工介入、哪些仍禁止 live

---

## 7. Task Breakdown

### P3-01 纸面执行编排入口收口

- **goal**: 在现有 `dagster_asterion` 边界内新增 paper execution canonical job / handler 入口
- **code landing area**: `dagster_asterion/job_map.py`、`dagster_asterion/handlers.py`、`dagster_asterion/jobs.py`、`dagster_asterion/schedules.py`
- **input tables**: `weather.weather_watch_only_snapshots`、`capability.market_capabilities`、`capability.account_trading_capabilities`
- **output tables**: `runtime.strategy_runs`、`runtime.trade_tickets`、`capability.execution_contexts`
- **contracts consumed**: `StrategyRun`、`TradeTicket`、`ExecutionContext`
- **tests required**: orchestration smoke tests、job map tests、manual-vs-scheduled behavior tests
- **exit criteria**: paper execution 有唯一 canonical 编排入口；不新增平行 CLI / sidecar contract

### P3-02 Canonical handoff 持久化闭环

- **goal**: 让 `strategy_engine -> trade_ticket -> signal_to_order` 的 handoff 不只存在内存对象，还落到 canonical persistence
- **code landing area**: `asterion_core/runtime/strategy_engine_v3.py`、`asterion_core/execution/trade_ticket_v1.py`、`asterion_core/execution/signal_to_order_v1.py`、`asterion_core/journal/journal_v3.py`
- **input tables**: `weather.weather_watch_only_snapshots`、`capability.market_capabilities`、`capability.account_trading_capabilities`
- **output tables**: `runtime.strategy_runs`、`runtime.trade_tickets`、`capability.execution_contexts`、`runtime.journal_events`
- **contracts consumed**: `StrategyDecision`、`TradeTicket`、`CanonicalOrderContract`、`ExecutionContext`
- **tests required**: unit tests、DuckDB integration tests、idempotent handoff tests
- **exit criteria**: execution context 与 request/order handoff 可追溯，且不需要再引入平行 intent 表

### P3-03 Canonical order router 落地

- **goal**: 在现有 `RouteAction` freeze 基础上实现 `order_router_v1`
- **code landing area**: `asterion_core/execution/order_router_v1.py`
- **input tables**: `capability.market_capabilities`、`weather.weather_watch_only_snapshots`
- **output tables**: `runtime.journal_events`，必要时更新 `capability.execution_contexts`
- **contracts consumed**: `RouteAction`、`TimeInForce`、`CanonicalOrderContract`、`MarketCapability`
- **tests required**: route mapping tests、time-in-force normalization tests、market capability validation tests
- **exit criteria**: Router 输出唯一 canonical `RouteAction`，不引入第二套 paper-only order type

### P3-04 Paper adapter 落地

- **goal**: 把 canonical order handoff 到 paper execution adapter，而不是 live submitter
- **code landing area**: `asterion_core/execution/` 下新增 paper adapter 模块
- **input tables**: `runtime.trade_tickets`、`capability.execution_contexts`、`runtime.gate_decisions`
- **output tables**: `trading.orders`、`trading.order_state_transitions`、`runtime.journal_events`
- **contracts consumed**: `CanonicalOrderContract`、`ExecutionContext`、`Order`
- **tests required**: adapter contract tests、order creation tests、journal persistence tests
- **exit criteria**: `paper` 与 `live` 不共用 submitter 实现，但仍共用同一份 canonical order contract

### P3-05 Quote-based paper fill simulator

- **goal**: 基于现有 `reference_price / fair_value / edge_bps / route_action` 实现 deterministic paper fills
- **code landing area**: `asterion_core/execution/` 下新增 paper fill simulator 模块
- **input tables**: `trading.orders`、`weather.weather_watch_only_snapshots`
- **output tables**: `trading.fills`、`trading.order_state_transitions`、`runtime.journal_events`
- **contracts consumed**: `Order`、`Fill`、`RouteAction`
- **tests required**: unit fill rule tests、determinism tests、replay/regression tests
- **exit criteria**: 同一输入可稳定生成同一 fills；不依赖真实 orderbook 或真实撮合

### P3-06 OMS state machine 闭环

- **goal**: 让 order lifecycle 真正闭合到 `order_state_transitions`
- **code landing area**: `asterion_core/execution/`、`asterion_core/contracts/inventory.py`、`asterion_core/journal/journal_v3.py`
- **input tables**: `trading.orders`、`trading.fills`
- **output tables**: `trading.orders`、`trading.order_state_transitions`、`runtime.journal_events`
- **contracts consumed**: `OrderStatus`、`Order`、`Fill`
- **tests required**: state transition unit tests、invalid transition rejection tests、DuckDB lifecycle tests
- **exit criteria**: order 可完整经历 `created -> reserved -> posted -> partial_filled -> filled / cancelled / expired / rejected`

### P3-07 Reservation / inventory / exposure under paper fills

- **goal**: 在 paper fills 下闭合 BUY / SELL reservation conversion 与 inventory movement
- **code landing area**: `asterion_core/risk/portfolio_v3.py`、`asterion_core/contracts/inventory.py`、`asterion_core/journal/journal_v3.py`
- **input tables**: `trading.orders`、`trading.reservations`、`trading.fills`、`trading.inventory_positions`
- **output tables**: `trading.reservations`、`trading.inventory_positions`、`trading.exposure_snapshots`、`runtime.journal_events`
- **contracts consumed**: `Reservation`、`InventoryPosition`、`ExposureSnapshot`、`Fill`
- **tests required**: unit tests、DuckDB integration tests、BUY/SELL asymmetry tests
- **exit criteria**: paper fills 能稳定更新 reservation、inventory、exposure，且不引入新的 inventory 主键语义

### P3-08 Reconciliation result 闭环

- **goal**: 把 `trading.reconciliation_results` 从预留表推进到 paper execution 的 operator 入口
- **code landing area**: `asterion_core/risk/`、`asterion_core/journal/`、`asterion_core/ui/ui_lite_db.py`
- **input tables**: `trading.orders`、`trading.fills`、`trading.inventory_positions`、`trading.exposure_snapshots`
- **output tables**: `trading.reconciliation_results`、`runtime.journal_events`
- **contracts consumed**: `ExposureSnapshot`、`Reservation`、`Fill`
- **tests required**: reconciliation classification tests、DuckDB integration tests、operator mismatch tests
- **exit criteria**: operator 能区分正常对账与异常对账；paper execution 不再停留在单次 demo

### P3-09 Operator read model 扩展

- **goal**: 在现有 `ui.execution_ticket_summary` 基础上扩展 paper execution operator 读面
- **code landing area**: `asterion_core/ui/ui_lite_db.py`、`asterion_core/ui/ui_db_replica.py`
- **input tables**: `runtime.trade_tickets`、`runtime.gate_decisions`、`trading.orders`、`trading.order_state_transitions`、`trading.reservations`、`trading.fills`、`trading.reconciliation_results`
- **output tables**: `ui.execution_ticket_summary`、新增或扩展的 `ui.*` 只读 summary tables、`ui.phase_readiness_summary`
- **contracts consumed**: runtime/trading canonical tables
- **tests required**: UI lite contract tests、DuckDB build tests、operator read model tests
- **exit criteria**: operator 无需回表到 raw ledger 即可看清 paper order 当前状态、最近 fill、reconciliation 状态

### P3-10 Paper run journal / daily ops / review flow

- **goal**: 把 paper execution journal、daily ops 和 review flow 接起来，并为 `Daily Review Agent` 提供明确落点
- **code landing area**: `asterion_core/journal/journal_v3.py`、`asterion_core/ui/ui_lite_db.py`、`agents/weather/` 下未来的 daily review 入口
- **input tables**: `runtime.journal_events`、`trading.orders`、`trading.fills`、`trading.reconciliation_results`、`ui.phase_readiness_summary`
- **output tables**: `runtime.journal_events`、`agent.*` review artifacts、runbook / ops summary 输入面
- **contracts consumed**: `JournalEvent`
- **tests required**: journal event tests、daily review input assembly tests、agent non-interference tests
- **exit criteria**: daily ops / review flow 有稳定输入；agent 仍保持在执行路径之外

### P3-11 P3 readiness gates

- **goal**: 把 readiness 从“P3 可开工”推进到“P3 可关闭并进入 P4”
- **code landing area**: `asterion_core/monitoring/readiness_checker_v1.py`、`asterion_core/ui/ui_lite_db.py`
- **input tables**: `runtime.*`、`trading.*`、`agent.*`、`ui.*`
- **output tables**: readiness JSON / markdown 报告、`ui.phase_readiness_summary`
- **contracts consumed**: readiness report contract
- **tests required**: gate pass/fail tests、missing-table tests、operator surface dependency tests
- **exit criteria**: readiness gates 能区分 `P3 complete but still not live` 与 `ready for P4 planning`

### P3-12 Closeout / regression / next-phase entry criteria

- **goal**: 固定 `P3` 关闭条件与 `P4` 开工边界
- **code landing area**: `tests/`、`docs/10-implementation/checklists/`、`docs/10-implementation/runbooks/`
- **input tables**: 全部 paper execution canonical tables
- **output tables**: closeout checklist 所依赖的测试与 read model 产物
- **contracts consumed**: 本文档中定义的 canonical data flow 与 readiness boundary
- **tests required**: full regression、end-to-end paper chain tests、operator read model tests、replay/regression tests
- **exit criteria**: `P3` closeout 可被单独审查；后续进入 `P4` 不再反向改写 `P3` 主链 contract

---

## 8. Test Plan

### 8.1 Unit Tests

必须覆盖：

- `RouteAction -> time_in_force / post_only` 映射
- paper router 决策
- quote-based fill rules
- order state transition validity
- reservation / inventory conversion
- reconciliation classification
- daily ops / review input assembly

### 8.2 DuckDB Integration Tests

必须覆盖：

- `capability.execution_contexts` persistence
- `trading.orders / order_state_transitions / reservations / fills / inventory_positions / exposure_snapshots / reconciliation_results`
- `runtime.journal_events` 与 order lifecycle 的一致性
- `ui.execution_ticket_summary` / `ui.phase_readiness_summary`

### 8.3 End-to-End Paper Execution Tests

必须覆盖：

- `watch_only_snapshot -> strategy_run -> trade_ticket -> execution_context -> gate -> order -> fill -> inventory -> journal -> ui`
- BUY 路径
- SELL 路径
- partial fill 路径
- cancel / reject / expire 路径

### 8.4 Replay / Regression Tests

必须覆盖：

- 同一输入下 paper fills 的 deterministic 输出
- replay 后 journal / reconciliation 不漂移
- 多次重复运行不产生不可解释的 duplicate state transitions

### 8.5 Operator Read Model Tests

必须覆盖：

- `ui.execution_ticket_summary` 正确展示 gate/order/fill/reconciliation 最新状态
- readiness summary 与 execution 状态联动
- read model build failure 不覆盖旧产物

---

## 9. Exit Criteria

`P3` 完成时，必须同时满足以下条件：

1. paper execution 主链已贯通，从 `weather.weather_watch_only_snapshots` 一直到 `trading.* / runtime.* / ui.*`
2. `capability.execution_contexts`、`trading.order_state_transitions`、`trading.reconciliation_results` 已成为运行中的一等表，而不是仅存在 migration
3. quote-based paper fills 已可 deterministic replay
4. OMS / reservation / inventory / exposure / reconciliation 已形成闭环
5. operator read model 已能稳定查看 paper execution 当前状态与异常
6. daily ops / review flow 已有稳定输入面
7. `P3` readiness report 已能明确输出 `GO / NO-GO`，但其 `GO` 只代表“可进入 P4 规划”，不代表可 live

### 9.1 仍保持 human-in-the-loop 的能力

- readiness 最终放行
- reconciliation exception 处置
- daily review 结论采纳
- signer / wallet / key material 相关审批

### 9.2 P3 完成时仍然不能进入 live 的能力

- 真实 signer 调用
- 真实 wallet side effects
- 真实链上广播
- KMS / HSM / Vault 集成
- 真实资金 deployment

---

## 10. Risks / Open Questions

### 10.1 风险：后续文档更新再次混淆 current / planned

当前文档已经完成 current / planned 分离。  
后续若继续把未来模块写回“当前已落地结构”，会再次制造实现漂移。

### 10.2 风险：已有 canonical 表尚未 fully operational

`execution_contexts / order_state_transitions / reconciliation_results` 已在 schema 中，但还未成为当前 execution foundation 的主链一部分。  
如果 `P3` 不优先收口这三处，paper execution 仍会停留在 demo 层。

### 10.3 风险：operator read model 目前只覆盖 foundation-level 状态

当前 `ui.execution_ticket_summary` 已能读取部分 runtime / trading / journal 信息，但仍缺少完整的 paper execution read surface。  
`P3` 应扩展现有 UI lite，而不是再造一套 operator 表。

### 10.4 风险：paper fill fidelity 与 deterministic baseline 的权衡

本阶段选择 `quote-based` baseline，是为了先确保 deterministic、可回放、可审计。  
更高保真度的 orderbook-like simulator 如需引入，应放在 `P3` 后段或 `P4` 前评估，而不是阻塞 paper execution 主链。

### 10.5 风险：环境口径不一致

当前 `.venv` 是 canonical 验证环境；system Python 可能缺少 `duckdb`。  
所有 `P3` 测试和 closeout 结论都应以 `.venv` 为准。

---

## 11. P3 Default Decisions

本阶段默认决策固定如下：

1. `paper fill baseline = quote-based deterministic`
2. `trading.*` 继续作为 canonical execution ledger
3. `runtime.*` 继续作为审计层
4. 不新建 `paper.*` schema
5. 不在 `P3` 引入 live signer / live submitter / real wallet side effects
6. 不在本轮重写 [Asterion_Project_Plan.md](../../00-overview/Asterion_Project_Plan.md) 的大段历史设计，只在本计划中显式登记漂移
