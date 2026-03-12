# Asterion（星枢）完整项目方案 

**中文代号：星枢**

> 一个建立在 AlphaDesk 底座之上的、面向 Polymarket 多领域事件市场的"研究、Agent、定价、执行、风控"统一平台

**文档版本**: v1.2
**创建日期**: 2026-03-07
**更新日期**: 2026-03-12
**状态**: 详细设计完成，P3 已关闭，P4 已关闭（`P4-01` / `P4-02` / `P4-03` / `P4-04` / `P4-05` / `P4-06` / `P4-07` / `P4-08` / `P4-09` / `P4-10` / `P4-11` / `P4-12` 已完成；ready for controlled live rollout decision）

---

## 文档说明

本文档是 Asterion 项目的完整技术方案，包含：

✅ **项目定位与架构理念** - 为什么做、怎么做
✅ **核心模块设计** - CLOB Order Router、UMA Watcher 等关键模块
✅ **Weather MVP 实施方案** - 第一个业务包的完整设计
✅ **数据模型设计** - 完整的数据库表设计
✅ **风险管理框架** - 仓位限制、熔断机制
✅ **开发路线图** - 4 个 Phase 的详细计划

**详细设计文档**:
- [Documentation Index](./Documentation_Index.md)
- [开发路线图](./DEVELOPMENT_ROADMAP.md)
- [Implementation Index](../10-implementation/Implementation_Index.md)
- [P4 实施文档](../10-implementation/phase-plans/P4_Implementation_Plan.md)
- [P4 关闭清单](../10-implementation/checklists/P4_Closeout_Checklist.md)
- [P4 Controlled Rollout Decision Runbook](../10-implementation/runbooks/P4_Controlled_Rollout_Decision_Runbook.md)
- [P3 实施文档](../10-implementation/phase-plans/P3_Implementation_Plan.md)
- [P2 实施文档](../10-implementation/phase-plans/P2_Implementation_Plan.md)
- [P1 实施文档](../10-implementation/phase-plans/P1_Implementation_Plan.md)
- [P0 实施文档](../10-implementation/phase-plans/P0_Implementation_Plan.md)
- [P1 关闭清单](../10-implementation/checklists/P1_Closeout_Checklist.md)
- [P1 运行交接 Runbook](../10-implementation/runbooks/P1_Watch_Only_Replay_Cold_Path_Runbook.md)
- [CLOB Order Router 详细设计](../30-trading/CLOB_Order_Router_Design.md)
- [UMA Watcher 详细设计](../40-weather/UMA_Watcher_Design.md)
- [OMS + Inventory + Reconciliation 详细设计](../30-trading/OMS_Design.md)
- [数据库架构设计](../20-architecture/Database_Architecture_Design.md)
- [Market Capability Registry 详细设计](../30-trading/Market_Capability_Registry_Design.md)
- [Signer Service 详细设计](../30-trading/Signer_Service_Design.md)
- [Event Sourcing 详细设计](../20-architecture/Event_Sourcing_Design.md)
- [热路径 vs 冷路径架构](../20-architecture/Hot_Cold_Path_Architecture.md)
- [Forecast Ensemble 详细设计](../40-weather/Forecast_Ensemble_Design.md)
- [Agent Monitor 详细设计](../50-operations/Agent_Monitor_Design.md)
- [Gas Manager 详细设计](../30-trading/Gas_Manager_Design.md)
---

## 目录

1. [项目定位与架构理念](#1-项目定位与架构理念)
2. [从 AlphaDesk 复用的内容](#2-从-alphadesk-复用的内容)
3. [Asterion 项目结构](#3-asterion-项目结构)
4. [核心模块设计概览](#4-核心模块设计概览)
5. [Weather MVP 实施方案](#5-weather-mvp-实施方案)
6. [数据模型设计](#6-数据模型设计)
7. [风险管理框架](#7-风险管理框架)
8. [开发路线图](#8-开发路线图)
9. [技术栈与工具](#9-技术栈与工具)
10. [附录](#10-附录)

---

## 1. 项目定位与架构理念

### 1.1 核心定位

**Asterion** 不是 AlphaDesk 的"天气分支"，而是一个**新的、面向多模块扩展的事件交易平台**。

**核心思路**:
> **复用 AlphaDesk 的平台底座，重建 domain layer，先落地 Weather MVP，再扩到 Tech，最后接 Crypto。**

### 1.2 为什么不继续沿用 AlphaDesk

AlphaDesk 的产品边界和数据假设明显是 **crypto-first 的 trading cockpit**。它的核心链路：

```
bronze.py → silver → gold → strategy_engine_v3.py → execution_gate_v1.py
→ portfolio_v3.py → realtime_loop_v1.py → trade_ticket_v1.py → signal_to_order_v1.py
```

这是一个很好的底座，但不适合把"天气、科技、加密"都往同一个 crypto 语义空间里塞。

**问题**:
- Universe 明确限制在 Crypto Top 500 / Top 100
- 特征层围绕钱包画像、smart money 设计
- 策略层有大量 crypto-specific 的逻辑

**解决方案**:
- **AlphaDesk** - 旧系统/底座来源
- **Asterion** - 新主项目
- **Weather** - Asterion 的第一个 domain pack
- **Tech / Crypto** - 后续接入的第二、第三个 domain pack

### 1.3 Asterion 的架构理念

#### 1.3.1 Domain Pack 架构

Asterion 从一开始就不是"天气项目"，而是 **domain-pack 平台**。

```
Asterion Core (平台底座)
    ↓
├── Weather Pack (天气市场)
│   ├── MarketSpec
│   ├── ForecastAdapter
│   └── PricingModel
│
├── Tech Pack (科技事件)
│   ├── MarketSpec
│   ├── EvidenceAdapter
│   └── ResolutionLogic
│
└── Crypto Pack (加密货币)
    ├── MarketSpec
    ├── ExternalMarketAdapter
    └── HedgePlanner
```

#### 1.3.2 Agent 在执行路径之外

**关键设计原则**: 所有 AI Agent 只做"编译、校验、监控、复盘"，永远不直接触发交易。

**原因**:
- 金融系统的安全第一原则
- Agent 输出需要人工审核或规则验证
- 避免 LLM 的不确定性直接影响资金

**Agent 职责**:
- ✅ 规则解析（Rule2Spec）
- ✅ 数据质量检查（Data QA）
- ✅ 结算审阅（Resolution Agent，legacy alias: Resolution Sentinel）
- ✅ 日报生成（Daily Review）
- ❌ 直接下单
- ❌ 直接撤单
- ❌ 直接调整仓位

#### 1.3.3 Event Sourcing 模式

所有状态变更都记录为不可变事件，可以"时光倒流"重现任何时刻的系统状态。

**好处**:
- 完整的审计轨迹
- 可以重放历史状态
- 方便 backtesting 和 what-if 分析

---

## 2. 从 AlphaDesk 复用的内容

### 2.1 第一类：直接复用（平台底座）

这些是平台底座，价值最大，几乎原样带走。

#### 2.1.1 接入与数据底座

```python
alphadesk/bronze.py              → asterion_core/ingest/bronze.py
alphadesk/clients/data_api.py    → asterion_core/clients/data_api.py
alphadesk/clients/gamma.py       → asterion_core/clients/gamma.py
alphadesk/ws_subscribe.py        → asterion_core/ws/ws_subscribe.py
alphadesk/ws_agg_v3.py           → asterion_core/ws/ws_agg_v3.py
alphadesk/database.py            → asterion_core/storage/database.py
alphadesk/db_migrate.py          → asterion_core/storage/db_migrate.py
```

**价值**: 这些正对应 AlphaDesk 文档里的 Data Pipeline + Infrastructure 两层：原始采集、Gamma/Data API、WS 订阅与聚合、数据库管理。它们天然适合继续作为 Asterion 的 Polymarket 接入层。

**代码级结论**:

- `bronze.py` 的 `BronzeJsonlRollingWriter` 可以原样迁入
- `clients/data_api.py` 的分页抓取壳可以原样迁入
- `clients/gamma.py` 的 `scan_gamma_markets()` / `infer_condition_id()` 适合直接作为 market discovery 底座
- `ws_subscribe.py` / `ws_agg_v3.py` 仍属于可复用接入层，但截至 `P0` 关闭时尚未迁入，已转入 `P1` 剩余迁移清单
- `database.py` 的 `GuardedConnection`、`connect_duckdb()`、`meta_* watermark/run-log` 很成熟，应保留
- 这批代码只需要做 env 前缀、schema allow-list、日志命名适配

#### 2.1.2 写入、审计、确定性

```python
alphadesk/write_queue.py         → asterion_core/storage/write_queue.py
alphadesk/writerd.py             → asterion_core/storage/writerd.py
alphadesk/write_guard_audit.py   → asterion_core/storage/write_guard_audit.py
alphadesk/os_queue.py            → asterion_core/storage/os_queue.py
alphadesk/determinism.py         → asterion_core/storage/determinism.py
```

**价值**: AlphaDesk 的单写者、写入队列、审计日志、稳定哈希，是非常值得保留的设计。文档明确写了 `ALPHADESK_STRICT_SINGLE_WRITER=1`、`WRITERD=1`、`ALPHADESK_DB_ROLE=writer` 的规则，以及"Producer → Write Queue(SQLite) → Writerd → Audit Log"的模式。

**代码级结论**:

- `write_queue.py` 已经具备 `claim/retry/stale-running/archive` 闭环
- `writerd.py` 的 allow-list、batch merge、fallback single-task 处理值得直接保留
- `os_queue.py` 的 producer API 非常适合作为 Asterion 的统一写入口
- `determinism.py` 的 canonical JSON + stable hash 应直接成为 Asterion 基础库

#### 2.1.3 策略与执行骨架

```python
alphadesk/strategies/base.py     → asterion_core/runtime/strategy_base.py
alphadesk/strategy_engine_v3.py  → asterion_core/runtime/strategy_engine_v3.py
alphadesk/trade_ticket_v1.py     → asterion_core/execution/trade_ticket_v1.py
alphadesk/signal_to_order_v1.py  → asterion_core/execution/signal_to_order_v1.py
alphadesk/execution_gate_v1.py   → asterion_core/execution/execution_gate_v1.py
alphadesk/portfolio_v3.py        → asterion_core/risk/portfolio_v3.py
alphadesk/journal_v3.py          → asterion_core/journal/journal_v3.py
alphadesk/watch_only_gate_v3.py  → asterion_core/execution/watch_only_gate_v3.py
```

**价值**: AlphaDesk 的 `StrategyV3` 协议、trade ticket、execution gate、portfolio gate、journal，这些都不是 crypto 专属，而是"事件交易系统"的通用运行时。Asterion 最应该继承的，是这套 runtime 的边界与调度方式，而不是旧字段。

**代码级结论**:

- `strategies/base.py` 的 `StrategyContext + StrategyV3.generate()` 可以直接迁
- `strategy_engine_v3.py` 只能保留快照读取、稳定排序、run_id 生成等外层壳；`opportunities_v1/v2/v3` 和 arb hardening 不能照搬
- `trade_ticket_v1.py` 适合复用 ticket hash / provenance 结构，但要把 `asset_id`、`planned_notional_usd`、`recommended_exec_template_id` 改成 Asterion canonical contract
- `signal_to_order_v1.py` 依赖 `exec_plan_v3` 和旧 exec template，不能原样迁，只能保留 dedup / ticket->plan 转换思路
- `execution_gate_v1.py` 适合保留 gate pipeline，不适合保留经济公式
- `portfolio_v3.py` / `journal_v3.py` 只能保留壳和写入模式，内部 schema 要改成 `Reservation / InventoryPosition / Fill / Order`

#### 2.1.4 运维与可观测性

```python
alphadesk/health_monitor_v1.py   → asterion_core/monitoring/health_monitor_v1.py
alphadesk/readiness_checker_v1.py → asterion_core/monitoring/readiness_checker_v1.py
alphadesk/ui_db_replica.py       → asterion_core/ui/ui_db_replica.py
alphadesk/ui_lite_db.py          → asterion_core/ui/ui_lite_db.py
supervisord.conf                 → supervisord.conf
Dagster 资源层与调度外壳          → dagster_asterion/
```

**价值**: AlphaDesk 文档把它的优势总结为：数据驱动、策略插件化、确定性、安全第一、可观测性和高可用。这些不是某个模块的 feature，而是项目级能力，应该整体带进 Asterion。

**代码级结论**:

- `health_monitor_v1.py` 的 queue / ws / degrade 健康采集可以直接迁
- `ui_db_replica.py` 的 replica copy + validate + meta 机制可以直接迁
- `readiness_checker_v1.py` 和 `ui_lite_db.py` 只适合保留框架，不能保留旧 milestone 和旧 UI contract
- Dagster 的 `resources.py` / `schedules.py` 是薄壳，可直接迁；assets 要重写

### 2.2 第二类：保留框架，内容重写

这些模块不能直接拿来跑 Weather，但值得保留接口。

```python
alphadesk/universe_v3.py         → domains/weather/universe_v1.py (重写)
alphadesk/cost_model_v3.py       → asterion_core/execution/fee_calculator.py (重写)
alphadesk/fill_ttf_v3.py         → asterion_core/execution/liquidity_estimator.py (重写)
alphadesk/dagster_alphadesk/assets/* → dagster_asterion/assets/* (重写)
alphadesk/pages/dashboard.py    → apps/operator_ui/pages/dashboard.py (重写)
```

**原因**: 这些模块现在的参数和业务语义，大概率是围绕 crypto universe、crypto liquidity 和 crypto fill behavior 调出来的。但它们的"壳"仍然有价值：比如 universe manager、成本模型接口、fillability 评估、仪表盘骨架、调度框架，都可以保留，只是换成 Weather 的 domain 逻辑。

### 2.3 第三类：不继承

这些会把新项目重新绑回 crypto-first 的历史包袱。

```
❌ alphadesk/wallet_features
❌ alphadesk/smart_money_alerts
❌ alphadesk/smart_accumulation_follow_v1.py
❌ alphadesk/consistency_arb_v1.py
❌ alphadesk/constraints_arb_v1.py
❌ 与 smart money、wallet、positions market daily 强耦合的脚本和页面
```

**原因**: AlphaDesk 的特征层和策略层里，已经有明显围绕钱包画像、smart money、constraint arbitrage 的设计；这些对 Asterion 的第一阶段没有帮助，反而会让 Weather MVP 的 schema、页面和告警变得很脏。Asterion 应该从 domain-neutral runtime 出发，而不是从旧 alpha stack 出发。

---

## 3. Asterion 项目结构

### 3.1 当前已落地代码骨架（截至 2026-03-11）

```
Asterion/
  asterion_core/                  # 平台核心
    clients/                      # Polymarket API 客户端
      data_api.py
      gamma.py
      shared.py
    contracts/                    # canonical contracts / IDs / shared objects
      execution.py
      ids.py
      inventory.py
      weather.py
    ingest/                       # 数据采集
      bronze.py
    storage/                      # 数据存储 / queue / determinism
      database.py
      db_migrate.py
      determinism.py
      os_queue.py
      write_queue.py
      writerd.py
      write_guard_audit.py
    ws/                           # WebSocket 管理
      ws_subscribe.py
      ws_agg_v3.py
    runtime/                      # 策略运行时
      strategy_base.py
      strategy_engine_v3.py
    execution/                    # 执行层
      trade_ticket_v1.py
      signal_to_order_v1.py
      execution_gate_v1.py        # 执行门禁
      watch_only_gate_v3.py       # Watch-only 模式
    risk/                         # 风控层
      portfolio_v3.py
    journal/                      # 交易日志
      journal_v3.py
    monitoring/                   # 监控
      health_monitor_v1.py
      readiness_checker_v1.py
    ui/                           # UI 只读面
      ui_db_replica.py
      ui_lite_db.py

  domains/                        # 领域模块
    markets/
      __init__.py
    trading/
      oms/
        __init__.py
    weather/                      # 天气市场
      scout/                      # 市场发现
        market_discovery.py
      spec/                       # 规则解析
        rule2spec.py
        station_mapper.py
      forecast/                   # 预测服务
        adapters.py
        cache.py
        persistence.py
        replay.py
        service.py
      pricing/                    # 定价引擎
        engine.py
        persistence.py
      resolution/                 # 结算监控
        backfill.py
        continuity.py
        persistence.py
        rpc_fallback.py
        verification.py
        watcher_replay.py

  agents/                         # AI Agent
    common/
      client.py
      persistence.py
      runtime.py
    weather/
      rule2spec_agent.py
      data_qa_agent.py
      resolution_agent.py

  dagster_asterion/               # Dagster 编排（冷路径）
    handlers.py
    job_map.py
    jobs.py
    resources.py
    schedules.py

  sql/                            # SQL 脚本
    migrations/
      0001_core_meta.sql
      0002_market_and_capability.sql
      0003_orders_inventory.sql
      0004_weather_specs_and_forecasts.sql
      0005_uma_watcher.sql
      0006_runtime_execution.sql
      0007_agent_runtime.sql

  tests/                          # 标准库 unittest 测试
    test_execution_foundation.py
    test_forecast_replay.py
    test_cold_path_orchestration.py
    test_weather_agents.py
    ...

  docs/                           # 文档
    00-overview/
      Documentation_Index.md
      Asterion_Project_Plan.md    # 本文件
      DEVELOPMENT_ROADMAP.md
    10-implementation/
      Implementation_Index.md
      phase-plans/
        P0_Implementation_Plan.md
        P1_Implementation_Plan.md
        P2_Implementation_Plan.md
        P3_Implementation_Plan.md
      checklists/
        P0_Closeout_Checklist.md
        P1_Closeout_Checklist.md
        P2_Closeout_Checklist.md
        P1_P2_AlphaDesk_Remaining_Migration_Checklist.md
      runbooks/
        P1_Watch_Only_Replay_Cold_Path_Runbook.md
        P2_Cold_Path_Orchestration_Job_Map_Runbook.md
      migration-ledger/
        AlphaDesk_Migration_Ledger.md
      module-notes/
        AlphaDesk_*.md
    20-architecture/
      Database_Architecture_Design.md
      Event_Sourcing_Design.md
      Hot_Cold_Path_Architecture.md
    30-trading/
      CLOB_Order_Router_Design.md
      OMS_Design.md
      Market_Capability_Registry_Design.md
      Signer_Service_Design.md
      Gas_Manager_Design.md
    40-weather/
      Forecast_Ensemble_Design.md
      UMA_Watcher_Design.md
    50-operations/
      Agent_Monitor_Design.md

  README.md                       # 项目导航（根目录唯一文档）
  pyproject.toml                  # Python 项目配置
```

说明：

- 上面这部分只描述**当前仓库中已经落地**的代码骨架
- `domains/markets/`、`domains/trading/oms/` 当前只存在占位包，不应被误读为功能已闭合
- paper execution 已闭合到 `order_router_v1 / paper_adapter_v1 / paper_fill_simulator_v1 / oms_state_machine_v1 / portfolio_v3 / journal_v3 / ui_lite_db`
- `daily_review_agent.py` 仍属于后续规划；当前只落地了 daily review input surface

### 3.2 未来规划模块（未落地）

以下内容仍保留为设计与规划，不代表当前仓库已存在可运行实现：

```
asterion_core/execution/
  liquidity_estimator.py
  fee_calculator.py
  slippage_model.py

asterion_core/signer/
  key_manager.py
  order_signer.py
  transaction_signer.py

domains/trading/
  inventory/
  reconciliation/
  ctf/

agents/weather/
  daily_review_agent.py

apps/operator_ui/
  ...
```

这些模块在当前阶段的定位：

- `daily_review_agent.py`：后续 review automation / agent 化范围
- signer shell / official-order-compatible signing：`P4-04` / `P4-05` 已落地；real signer backend / key management / transaction signing：后续 `P4`
- 更完整的 operator UI：后续 operator productization 范围

### 3.3 为什么这么拆

- **`asterion_core`** - 继承 AlphaDesk 底座，domain-neutral 的平台能力
- **`domains/*`** - 每个垂类一套自己的 MarketSpec、外部数据适配器和策略
- **`agents/*`** - 让 LLM/Agent 从第一天起就是"按领域拆"的，而不是一个通用大脑
- **`dagster_asterion` + `ui_lite_db`** - 当前 operator / orchestration 的最小可运行外壳
- **`apps/operator_ui`** - 更完整的运营台产品形态，当前仍属于未来规划

这样到后面接 Tech 或 Crypto 时，不会出现"整个项目都得翻修"的问题。

---

## 4. 核心模块设计概览

### 4.1 CLOB Order Router（订单路由）

**核心改进**: 动态费率模型 + canonical 订单动作冻结

**解决方案**:
- 从 Market Capability Registry 动态查询费率（不再硬编码）
- Router / OMS / CLOB adapter 统一使用唯一 `RouteAction`
- Weather MVP 默认按当前 market capability 解析 fee，不允许写死 maker/taker 全局常量

**核心组件**:

#### 4.1.1 Market Capability Integration
- 动态查询市场能力（feesEnabled, feeRateBps, tickSize）
- 支持 fee-enabled 和 fee-free 市场
- 自动适配市场配置变更

#### 4.1.2 Liquidity Estimator（流动性评估器）
- 实时评估订单簿深度（1档、5档）
- 计算流动性评分（0-100）
- 预估不同订单量的滑点

#### 4.1.3 Fee Calculator（费用计算器）
- 动态费率查询（从 Market Capability Registry）
- 所有费率均按 market/token 实时解析
- `fee_rate_bps` 写入 canonical order contract，供 OMS、Signer、审计统一使用

#### 4.1.4 Slippage Model（滑点模型）
- 遍历订单簿计算平均成交价
- 预估买单/卖单滑点
- 置信度评估

#### 4.1.5 Routing Engine（路由引擎）
- **Canonical `RouteAction`**:
  - `POST_ONLY_GTC`
  - `POST_ONLY_GTD`
  - `FAK`
  - `FOK`
- `Adaptive` 不再是订单类型，而是 routing policy / 决策器
- Router 输出 `RouteAction`
- OMS 接收 `RouteAction`
- CLOB adapter 负责把 `RouteAction` 映射为 `orderType` 和 `postOnly`
- `postOnly` 只允许和 `GTC/GTD` 组合，不能与 `FAK/FOK` 混用

#### 4.1.6 Canonical Order Contract

```python
class RouteAction(Enum):
    POST_ONLY_GTC = "post_only_gtc"
    POST_ONLY_GTD = "post_only_gtd"
    FAK = "fak"
    FOK = "fok"

@dataclass
class CanonicalOrderContract:
    market_id: str
    token_id: str
    outcome: str
    side: str
    price: Decimal
    size: Decimal
    route_action: RouteAction
    expiration: Optional[datetime]
    time_in_force: str
    fee_rate_bps: int
    signature_type: int
    funder: str
```

**决策逻辑**:
```
# 1. 查询市场能力
capability = await capability_registry.get_capability(market_id)

# 2. policy 决定 route_action
if policy == "urgent" and 可完整成交:
    route_action = RouteAction.FOK
elif policy == "urgent":
    route_action = RouteAction.FAK
elif policy == "passive" and 需要截止时间:
    route_action = RouteAction.POST_ONLY_GTD
else:
    route_action = RouteAction.POST_ONLY_GTC

# 3. adapter 负责落地到官方字段
order_type, post_only = adapter.map(route_action)
```

**详细文档**: [CLOB_Order_Router_Design.md](../30-trading/CLOB_Order_Router_Design.md)

---

### 4.2 UMA Watcher（结算监控）

**核心改进**: 链上状态机 + 动态参数读取 + Human-in-the-loop

**解决方案**:
- 从链上动态读取 liveness 配置（不再硬编码 2 小时）
- 实现 finalized_block_watermark 机制（防止事件漏采集）
- 监听完整状态转移（proposal → disputed → settled → redeemed）
- Dispute 决策改为 human-in-the-loop（最终决策由人工确认）

**核心组件**:

#### 4.2.1 UMA Monitor（UMA 监控器）
- 监听 Polygon 链上的完整状态转移事件
- 从链上读取动态参数（liveness, challenge_period, bonds）
- 实现 finalized_block_watermark 机制
- 支持 backfill 逻辑（补采集历史事件）
- proposal 权威状态只来自 on-chain events / on-chain reads / finalized block watermark
- wall clock 只用于调度建议，不定义 proposal 最终状态

#### 4.2.2 Settlement Verifier（结算验证器）
- 从多个天气数据源获取实际观测值（NWS、Weather.com、Open-Meteo）
- 使用 ResolutionSpec（升级版 MarketSpec）
- 包含字段：authoritative_source, station_id, observation_window_local, rounding_rule, inclusive_bounds
- 生成 Evidence Package（完整的验证证据）
- 分离 forecast source 和 settlement source

#### 4.2.3 Dispute Analyzer（Dispute 分析器）
- 评估 dispute 的预期收益
- 计算置信度（多数据源一致性）
- 生成 dispute 建议报告
- **最终决策由人工确认**（human-in-the-loop）

#### 4.2.4 Redeem Scheduler（赎回调度器）
- 输入固定为 `proposal_status / on_chain_settled_at / safe_redeem_after / human_review_required`
- 输出固定为 `WAIT / READY_FOR_REDEEM / BLOCKED_PENDING_REVIEW / NOT_REDEEMABLE`
- `safe_redeem_after` 是本地调度建议，不是链上权威状态

**UMA 机制**（动态参数）:
- **Proposal bond**: 从 OptimisticOracle 合约读取
- **Dispute bond**: 从 OptimisticOracle 合约读取
- **Liveness period**: 从链上配置读取（不同市场可能不同）
- **Challenge period**: 从链上配置读取（替代硬编码的 2 小时）

**详细文档**: [UMA_Watcher_Design.md](../40-weather/UMA_Watcher_Design.md)

---

### 4.3 OMS + Inventory + Reconciliation（订单与库存管理）

**核心职责**: 订单生命周期管理、库存追踪、CTF 操作、对账

**核心组件**:

#### 4.3.1 Order Management System (OMS)
- 订单状态机（PENDING → SUBMITTED → FILLED/CANCELLED）
- 订单生命周期管理
- 成交追踪和聚合
- OMS 外部接口只接收 canonical order contract，不再暴露独立 `postOnly` 开关

#### 4.3.2 Inventory Manager
- 实时追踪 `wallet_id + asset_type + token_id + market_id + outcome + balance_type` 维度的库存
- BUY 订单预留 `USDC.e`，SELL 订单预留对应 `token_id`
- reservation 按 `price * size` 建立，fill 后按真实成交更新
- cancel / expire / reject 后释放剩余 reservation

#### 4.3.3 CTF Operations Manager
- Split 操作（USDC → YES + NO）
- Merge 操作（YES + NO → USDC）
- Redeem 操作（结算后赎回）

#### 4.3.4 Reconciliation Engine
- 链上链下状态一致性验证
- 差异检测和告警
- 自动修复机制

**详细文档**: [OMS_Design.md](../30-trading/OMS_Design.md)

---

### 4.4 Market Capability Registry（市场能力注册表）

**核心职责**: 统一的执行前能力查询接口

**解决方案**:
- 拆分 `MarketCapability` 与 `AccountTradingCapability`
- 按 `token_id` 读取 fee / tick / negRisk
- 在最终下单前合并为 `ExecutionContext`

**核心组件**:

#### 4.4.1 Market Capability Provider
- 从 Gamma / CLOB public methods / chain read 获取 token 级市场能力
- 负责 `fee_rate_bps` / `tick_size` / `neg_risk` / `tradable`

#### 4.4.2 Account Capability Provider
- 从 local config / chain read / operator override 获取账户交易能力
- 负责 `wallet_type` / `signature_type` / `funder` / `allowance_targets`

#### 4.4.3 ExecutionContext Builder
- 合并 market 与 account 能力
- 输出最终提交前使用的 `ExecutionContext`
- 作为 Router/OMS/Signer 的统一交接层

**集成点**:
- CLOB Router / Pricing → 消费 `MarketCapability`
- OMS / Signer → 消费 `AccountTradingCapability`
- Submit Orchestrator → 构造 `ExecutionContext`

**详细文档**: [Market_Capability_Registry_Design.md](../30-trading/Market_Capability_Registry_Design.md)

---

### 4.5 Signer Service（签名服务）

**核心职责**: 安全的签名管理

**解决方案**:
- 独立的签名服务（与业务逻辑分离）
- 管理 L1 signer（EOA / Proxy / Safe）
- 派生和管理 L2 API credentials
- 负责链上交易签名
- 负责调用订单签名能力，但不自定义订单签名协议
- 完整的审计日志

**核心组件**:

#### 4.5.1 Key Manager
- 支持环境变量（开发/测试）
- MVP：独立 signer 进程 + 环境隔离 + 最小暴露面
- Production：KMS / Vault / HSM

#### 4.5.2 Order Signer
- Polymarket CLOB 订单签名必须优先依赖官方 `py-clob-client`
- 如不直接使用官方库，也必须兼容官方 EIP-712 订单结构
- 不允许自行拼接 JSON 后签名

#### 4.5.3 Transaction Signer
- 链上交易签名
- 自动填充交易参数（gas, nonce, chainId）

#### 4.5.4 Audit Logger
- 记录所有签名操作
- 只记录 payload hash（保护隐私）
- 支持审计追踪

**安全特性**:
- Agent 不能直接访问私钥
- UI 不能直接调用原始签名接口
- Signer Service 只暴露 `sign_order(request)` / `sign_transaction(request)` / `derive_api_credentials(wallet_ref)`
- 所有签名请求必须携带 `request_id` 并写入审计日志

**详细文档**: [Signer_Service_Design.md](../30-trading/Signer_Service_Design.md)

---

### 4.6 Event Sourcing（事件溯源）

**核心职责**: 完整的审计轨迹和状态重放

**解决方案**:
- 所有状态变更记录为不可变事件
- 支持因果关系追踪（causation_id, correlation_id）
- 支持幂等性保证（idempotency_key）

**核心组件**:

#### 4.6.1 Event Store
- 统一的 domain_events 表
- 支持按聚合根查询
- 支持按关联 ID 查询

#### 4.6.2 Event Bus
- 事件发布订阅机制
- 异步事件处理
- 解耦业务逻辑

#### 4.6.3 Event Handlers
- 订单事件处理
- 库存事件处理
- 对账事件处理

**事件类型**:
- 市场事件（discovered/activated/settled）
- 订单事件（created/submitted/filled/cancelled）
- 库存事件（reserved/released/updated）
- CTF 事件（split/merge/redeem）
- UMA 事件（proposal/verified/disputed）
- 对账事件（completed/discrepancy）

**详细文档**: [Event_Sourcing_Design.md](../20-architecture/Event_Sourcing_Design.md)

---

### 4.7 Hot/Cold Path Architecture（热路径/冷路径架构）

**核心职责**: 明确职责边界，优化性能

**热路径（Hot Path）**:
- 实时交易和决策
- 低延迟要求（< 100ms）
- 高可用性要求（99.9%+）
- 技术栈：Python + SQLite/Postgres + asyncio

**冷路径（Cold Path）**:
- 批处理和分析
- 延迟容忍（分钟级）
- 可用性要求较低（95%+）
- 技术栈：Dagster + DuckDB + Parquet

**数据流**:
```
热路径（实时） → SQLite/Postgres → 定期导出 → Parquet → DuckDB（分析）
```

**详细文档**: [Hot_Cold_Path_Architecture.md](../20-architecture/Hot_Cold_Path_Architecture.md)

---

### 4.8 Database Architecture（数据库架构）

**核心改进**: 明确数据库角色边界

**数据库角色**:

#### 4.8.1 DuckDB - 分析引擎（冷路径）
- 只用于分析、回测、报表
- 不用于在线状态存储
- 列式存储，OLAP 性能优秀

#### 4.8.2 SQLite (WAL) - 队列和 Outbox（热路径辅助）
- Write Queue（单写者模式）
- Outbox Pattern（事件发布）
- 轻量级，低延迟

#### 4.8.3 在线状态库 - SQLite 或 Postgres（热路径核心）
- 订单状态管理
- 库存管理
- 实时仓位追踪
- MVP 使用 SQLite，生产环境可升级到 Postgres

**详细文档**: [Database_Architecture_Design.md](../20-architecture/Database_Architecture_Design.md)

---

### 4.9 Gas Manager（Gas 管理）

**问题**: Polygon 交易需要优化 gas 使用，避免 nonce 冲突。

**解决方案**:

#### 4.9.1 Gas Oracle
- 监控 Polygon gas price
- 预测 gas price 趋势
- 动态调整 gas price

#### 4.9.2 TX Batcher
- 批量处理多个订单
- 减少交易数量
- 降低总 gas 成本

#### 4.9.3 Nonce Manager
- 维护 nonce 状态机
- 避免 nonce 冲突
- 处理交易失败重试

**MVP 简化**:
- 只保留 approve/split/merge/redeem + tx monitor
- 移除 batcher（后置到 Phase 2）
- Nonce Manager 改为单 signer 进程职责

**详细文档**: [Gas_Manager_Design.md](../30-trading/Gas_Manager_Design.md)

---

### 4.10 Forecast Ensemble（预测集成）

**核心改进**: station-first contract + 闭合的 Forecast/Resolution 接口

**解决方案**: 组合多个预测源，生成更稳健的概率分布。

**数据源**:
- **Open-Meteo Ensemble** - 主预测源，保留 ensemble 成员级概率
- **NWS API** - 美国官方气象局，权威交叉验证
- **Weather.com** - 如果市场规则指定
- **历史气候数据** - 作为 prior

**组合策略**:
- 保留 Open-Meteo ensemble 成员级概率
- cache key 固定包含 `market_id + station_id + spec_version + source + model_run + forecast_target_time`
- 失败降级到 watch-only（不用 climatology）
- 动态权重（站点/季节/提前期）
- geocode 只用于 onboarding / spec 生成，不在热路径调用

**Forecast-Resolution Contract**:
- `market_id`
- `condition_id`
- `station_id`
- `location_name`
- `latitude`
- `longitude`
- `timezone`
- `observation_window_local`
- `authoritative_source`
- `fallback_sources`
- `rounding_rule`
- `inclusive_bounds`
- `spec_version`

**详细文档**: [Forecast_Ensemble_Design.md](../40-weather/Forecast_Ensemble_Design.md)

---

### 4.11 Agent Monitor（Agent 监控）

**核心改进**: 增强评估维度 + 成本控制 + 模型分层

**解决方案**:

#### 4.11.1 Agent Evaluator
- 评估 Agent 输出质量
- 新增指标：schema_valid_rate, calibration, prompt_version_drift, cost_per_accepted_output
- 建立 golden set（100-300 条）
- Rule2Spec 准确率
- Data QA 召回率
- Resolution Agent 误报率（legacy alias: Resolution Sentinel）

#### 4.11.2 Agent Monitor
- 运行时监控（延迟、失败率）
- 性能追踪
- 异常告警

#### 4.11.3 Human Feedback
- 人工反馈收集
- 标注正确/错误输出
- 持续改进 Agent prompt

#### 4.11.4 成本控制
- 模型分层：Rule2Spec/Resolution 用 Sonnet, Daily Review 用 Haiku/Batch
- Prompt caching
- 成本追踪

**详细文档**: [Agent_Monitor_Design.md](../50-operations/Agent_Monitor_Design.md)

---

## 5. Weather MVP 实施方案

### 5.1 MVP 定位

**一句话**: Asterion Weather 是一个面向 Polymarket 天气市场的"规则结构化、概率定价、执行与结算监控"系统。

**目标市场**: 美国城市单日最高温区间市场

**不做**:
- ❌ 降水市场
- ❌ 风速市场
- ❌ 全球市场
- ❌ 长周期气候衍生题
- ❌ Tech/AI 发布类市场

**为什么选这个切口**:
- ✅ 规则模板化强
- ✅ 生命周期短（1-2天）
- ✅ 结果可数值化
- ✅ Agent 价值明确
- ✅ 执行难度中等
- ✅ 能快速做闭环

### 5.2 核心产品模块

#### 5.2.1 Market Scout（市场发现）

**职责**:
- 从 Gamma API 找出活跃 Weather 市场
- 识别模板化日温区间市场
- 建立 `weather_markets` 池

**实现**:
```python
# 1. 查询 active markets with tag "weather"
markets = gamma_client.get_markets(tags=["weather"], active=True)

# 2. 过滤模板化市场
for market in markets:
    if is_temperature_range_market(market):
        save_to_weather_markets(market)
```

#### 5.2.2 Rule2Spec Agent（规则解析）

**职责**:
- 审阅 deterministic `Rule2SpecDraft -> WeatherMarketSpecRecord`
- 只输出 station-first 的解析建议、风险标记和 review hook
- 不替代 deterministic parser，不直接写 `weather.weather_market_specs`

**输入**:
- `WeatherMarket`
- deterministic `Rule2SpecDraft`
- 当前 `WeatherMarketSpecRecord | None`
- `StationMetadata | None`
- `weather_station_map` override 摘要

**输出**:
```python
Rule2SpecAgentOutput(
    verdict="pass|review|block",
    confidence=0.95,
    summary="station-first parse looks valid",
    risk_flags=[],
    suggested_patch_json={
        "station_id": "KNYC",
        "authoritative_source": "weather.com",
        "bucket_min_value": 50,
        "bucket_max_value": 59,
    },
    findings=[],
)
```

**约束**:
- `station_id` 是必需语义
- 不允许回流 `city-first`
- `suggested_patch_json` 只能作用于已冻结的 station-first 字段

#### 5.2.3 Forecast Service（预测服务）

**职责**:
- 对 ResolutionSpec 中已闭合的站点元数据取 forecast
- 输出温度分布而不是点预测

**数据源**:
- **Open-Meteo Ensemble** - 主 forecast 分布
- **NWS API** - 权威交叉校验

**canonical 模式**:
- station-first
- `ResolutionSpec` 必须携带 `station_id + latitude + longitude + timezone`
- `ForecastRequest` 必须携带 `station_id`
- forecast adapter 使用 `latitude / longitude`
- `StationMapper` 只负责找站点元数据
- geocode 不进入热路径

**输出**:
```python
ForecastDistribution(
    market_id="0x123...",
    forecast_time=datetime.now(),
    temperature_distribution={
        45: 0.05,  # 5% 概率
        46: 0.08,
        ...
        55: 0.15,  # 15% 概率（峰值）
        ...
        65: 0.03,
    }
)
```

#### 5.2.4 Pricing Engine（定价引擎）

**职责**:
- 把 forecast 分布离散到整数温度
- 汇总到 outcome bins
- 生成每个 outcome 的 fair value

**实现**:
```python
# 1. 汇总到 outcome bins
prob_50_59 = sum(dist[t] for t in range(50, 60))  # 50-59°F

# 2. 生成 fair value
fair_value_yes = prob_50_59
fair_value_no = 1 - prob_50_59
```

#### 5.2.5 Opportunity Builder（机会识别）

**职责**:
- 结合 fair value 与 Polymarket 盘口
- 生成买入/卖出/不做的机会

**逻辑**:
```python
if market_price < fair_value - threshold:
    # 市场低估，买入
    opportunity = BuyOpportunity(...)
elif market_price > fair_value + threshold:
    # 市场高估，卖出
    opportunity = SellOpportunity(...)
else:
    # 价格合理，不做
    opportunity = None
```

**Threshold 考虑**:
- Maker/Taker 费率（2.2%）
- 滑点
- 预测不确定性

#### 5.2.6 Execution Runtime（执行运行时）

**职责**:
- watch-only / paper / live 三种模式
- 下单、撤单、重试、门禁、日志
- 只接受结构化的 `TradePlan`

**模式**:
- **Watch-only** - 只记录机会，不下单
- **Paper** - 模拟下单，不真实执行
- **Live** - 真实下单

#### 5.2.7 Resolution Agent（结算审阅，legacy alias: Resolution Sentinel）

**职责**:
- 审阅 `settlement verification / evidence linkage / redeem suggestion`
- 给 operator 输出结构化审阅建议
- 不直接发起 dispute，不直接发起 redeem

**流程**:
```
市场关闭 → UMA 提案 → 验证提案 → Dispute 决策 → Redeem 调度
```

### 5.3 Weather MVP 的 Agent 设计

Weather 模块只上 4 个 Agent，且全部在**执行路径之外**。

**canonical code path**:
- `agents/common/*`
- `agents/weather/*`

**orchestration**:
- 3 个 Weather agent 都以独立 `manual jobs` 运行
- 不作为 forecast / watcher / reconciliation handler 的 inline hook
- 失败不阻塞 deterministic 主链路

#### Agent A: Rule2Spec
- **输入**: `WeatherMarket`、`Rule2SpecDraft`、当前 `WeatherMarketSpecRecord`、`StationMetadata`
- **输出**: `Rule2SpecAgentOutput`
- **作用**: 审阅 deterministic parser 的 station-first 结果，输出 patch 建议和风险标记

#### Agent B: Data QA
- **输入**: `WeatherMarketSpecRecord`、`ForecastReplayRecord`、`ForecastReplayDiffRecord[]`、pricing provenance 摘要
- **输出**: `DataQaAgentOutput`
- **作用**: 审阅 replay、source fallback、pricing provenance，不直接改写 forecast / pricing 表

#### Agent C: Resolution Agent
- **输入**: `UMAProposal`、`SettlementVerificationRecord`、`EvidencePackageLinkRecord`、`RedeemReadinessRecord`
- **输出**: `ResolutionAgentOutput`
- **作用**: 审阅结算与 redeem 建议，输出 operator action suggestion；`Resolution Sentinel` 只保留为 legacy alias

#### Agent D: Daily Review
- **输入**: 机会、订单、成交、PnL、预测误差、parse 误差
- **输出**: 日报、错误归因、下轮参数建议
- **作用**: 持续改进系统

### 5.4 Weather MVP 的技术方案

#### 5.4.1 数据与接入

**Polymarket 路**:
- Gamma API - 市场发现
- Data API - 历史交易/持仓分析
- CLOB API - orderbook / prices / 下单
- WebSocket - market / user channel

**天气数据路**:
- Open-Meteo Ensemble - 主 forecast 分布
- NWS API - 权威交叉校验
- Wunderground / market-specified source watcher - 结算源监视

#### 5.4.2 执行与认证

- 读接口全部匿名
- 下单走 CLOB L1/L2 认证
- 私钥只在 server-side signer 使用
- 绝不让 Agent 接触私钥

#### 5.4.3 速率与调度

- 行情优先 WebSocket
- REST 用于补快照、补历史、补 metadata
- 大量轮询要做本地缓存与 backoff

#### 5.4.4 数据库

**数据库架构**（明确角色边界）：
- **SQLite (WAL) / Postgres** - 在线状态库（热路径核心）
  - 订单管理、库存管理、实时仓位追踪
  - MVP 使用 SQLite，生产环境可升级到 Postgres
- **SQLite (WAL)** - Write Queue 和 Outbox（热路径辅助）
  - 单写者模式，事件发布
- **DuckDB** - 分析引擎（冷路径）
  - 只用于分析、回测、报表
  - 不用于在线状态存储
- **Parquet** - 历史数据存储
  - 从在线库定期导出
- **Streamlit** - Operator UI
- **Dagster** - 编排（冷路径）

**数据流**:
```
热路径（实时） → SQLite/Postgres → 定期导出 → Parquet → DuckDB（分析）
```

---

## 6. 数据模型设计

### 6.0 数据库角色说明

**在线状态库（SQLite/Postgres）** - 热路径核心：
- 订单管理（orders, fills, order_state_transitions）
- 库存管理（inventory_positions, inventory_ledger）
- CTF 操作（ctf_operations）
- UMA 监控（uma_proposals, settlement_verifications, proposal_state_transitions, block_watermarks）
- 对账（reconciliation_results）
- 市场能力（market_capabilities）
- 领域事件（domain_events）

**Write Queue（SQLite）** - 热路径辅助：
- write_queue, outbox

**分析库（DuckDB）** - 冷路径：
- 历史数据（historical_trades, historical_orderbook）
- 回测（backtest_runs, backtest_trades）
- 报表（daily_pnl, position_snapshots）

---

### 6.1 Core / Meta（核心元数据）

```sql
-- 数据采集运行记录
CREATE TABLE ingest_runs (
    run_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status TEXT NOT NULL,
    records_processed INTEGER,
    errors TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 数据水位线
CREATE TABLE watermarks (
    source TEXT PRIMARY KEY,
    last_processed_id TEXT,
    last_processed_time TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 写入队列任务
CREATE TABLE queue_tasks (
    task_id TEXT PRIMARY KEY,
    task_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP
);

-- 写入审计日志
CREATE TABLE writer_audit (
    audit_id TEXT PRIMARY KEY,
    operation TEXT NOT NULL,
    table_name TEXT NOT NULL,
    record_id TEXT,
    payload_hash TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 6.2 Weather Silver（Weather 银层）

```sql
-- Weather 市场
CREATE TABLE weather_markets (
    market_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    rules TEXT,
    close_time TIMESTAMP NOT NULL,
    status TEXT NOT NULL,
    tags TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Weather outcomes
CREATE TABLE weather_outcomes (
    outcome_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    label TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (market_id) REFERENCES weather_markets(market_id)
);

-- Weather 市场规格
CREATE TABLE weather_market_specs (
    spec_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    spec_version TEXT NOT NULL,
    location_name TEXT NOT NULL,
    station_id TEXT NOT NULL,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    timezone TEXT NOT NULL,
    observation_date DATE NOT NULL,
    observation_window_local TEXT NOT NULL,
    metric TEXT NOT NULL,
    unit TEXT NOT NULL,
    min_value REAL,
    max_value REAL,
    authoritative_source TEXT NOT NULL,
    fallback_sources TEXT NOT NULL,
    rounding_rule TEXT NOT NULL,
    inclusive_bounds BOOLEAN NOT NULL,
    parse_confidence REAL,
    risk_flags TEXT,
    rules_hash TEXT,
    parsed_at TIMESTAMP NOT NULL,
    deprecated_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (market_id) REFERENCES weather_markets(market_id)
);

-- Weather 站点映射
CREATE TABLE weather_station_map (
    map_id TEXT PRIMARY KEY,
    location_name TEXT NOT NULL,
    station_id TEXT NOT NULL,
    station_name TEXT,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    timezone TEXT NOT NULL,
    source TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Weather 数据源监控
CREATE TABLE weather_source_watch (
    watch_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    source TEXT NOT NULL,
    last_check_time TIMESTAMP,
    status TEXT NOT NULL,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (market_id) REFERENCES weather_markets(market_id)
);
```

### 6.3 Weather Gold（Weather 金层）

```sql
-- Weather 预测运行
CREATE TABLE weather_forecast_runs (
    run_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    spec_id TEXT NOT NULL,
    station_id TEXT NOT NULL,
    source TEXT NOT NULL,
    model_run TEXT NOT NULL,
    forecast_target_time TIMESTAMP NOT NULL,
    spec_version TEXT NOT NULL,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    timezone_used TEXT NOT NULL,
    data_staleness_seconds INTEGER,
    temperature_distribution TEXT,
    confidence REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (market_id) REFERENCES weather_markets(market_id),
    FOREIGN KEY (spec_id) REFERENCES weather_market_specs(spec_id)
);

-- Weather 公允价值
CREATE TABLE weather_fair_values (
    fair_value_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    outcome_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    fair_value REAL NOT NULL,
    confidence REAL NOT NULL,
    calculated_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (market_id) REFERENCES weather_markets(market_id),
    FOREIGN KEY (outcome_id) REFERENCES weather_outcomes(outcome_id),
    FOREIGN KEY (run_id) REFERENCES weather_forecast_runs(run_id)
);

-- Weather 机会
CREATE TABLE weather_opportunities_v1 (
    opportunity_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    outcome_id TEXT NOT NULL,
    side TEXT NOT NULL,
    fair_value REAL NOT NULL,
    market_price REAL NOT NULL,
    edge_bps INTEGER NOT NULL,
    size_usd REAL NOT NULL,
    urgency TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (market_id) REFERENCES weather_markets(market_id),
    FOREIGN KEY (outcome_id) REFERENCES weather_outcomes(outcome_id)
);

-- Weather 交易票据
CREATE TABLE weather_trade_tickets_v1 (
    ticket_id TEXT PRIMARY KEY,
    opportunity_id TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    executed_at TIMESTAMP,
    FOREIGN KEY (opportunity_id) REFERENCES weather_opportunities_v1(opportunity_id)
);

-- Weather 执行计划
CREATE TABLE weather_exec_plans_v1 (
    plan_id TEXT PRIMARY KEY,
    ticket_id TEXT NOT NULL,
    routing_decision_id TEXT,
    status TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (ticket_id) REFERENCES weather_trade_tickets_v1(ticket_id)
);

-- Weather 结算监控
CREATE TABLE weather_resolution_watch_v1 (
    watch_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    uma_proposal_id TEXT,
    uma_proposal_tx TEXT,
    dispute_deadline TIMESTAMP,
    settlement_source_snapshot TEXT,
    redeem_status TEXT NOT NULL,
    settlement_risk_score REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (market_id) REFERENCES weather_markets(market_id)
);

-- Weather 模型评估
CREATE TABLE weather_model_review_v1 (
    review_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    predicted_outcome TEXT NOT NULL,
    actual_outcome TEXT NOT NULL,
    error_magnitude REAL,
    error_type TEXT,
    review_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (market_id) REFERENCES weather_markets(market_id)
);
```

### 6.4 Execution（执行层）

```sql
-- 订单路由决策
CREATE TABLE order_routing_decisions (
    decision_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    side TEXT NOT NULL,
    price REAL NOT NULL,
    size REAL NOT NULL,
    route_action TEXT NOT NULL,
    time_in_force TEXT NOT NULL,
    expiration TIMESTAMP,
    fee_rate_bps INTEGER NOT NULL,
    best_bid REAL,
    best_ask REAL,
    spread_bps INTEGER,
    liquidity_score REAL,
    limit_price REAL NOT NULL,
    expected_slippage_bps INTEGER,
    total_cost_bps INTEGER,
    confidence REAL,
    reason TEXT,
    actual_fill_price REAL,
    actual_fee_bps INTEGER,
    actual_slippage_bps INTEGER,
    fill_time_ms INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 流动性快照
CREATE TABLE liquidity_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    outcome_id TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    bid_depth_1cent REAL,
    ask_depth_1cent REAL,
    bid_depth_5cent REAL,
    ask_depth_5cent REAL,
    total_bid_liquidity REAL,
    total_ask_liquidity REAL,
    spread_bps INTEGER,
    mid_price REAL,
    liquidity_score REAL,
    is_liquid BOOLEAN,
    estimated_slippage_10usd INTEGER,
    estimated_slippage_50usd INTEGER,
    estimated_slippage_100usd INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 执行质量指标
CREATE TABLE execution_quality_metrics (
    metric_id TEXT PRIMARY KEY,
    decision_id TEXT NOT NULL,
    trade_id TEXT,
    expected_price REAL,
    actual_price REAL,
    price_improvement_bps INTEGER,
    expected_fee_bps INTEGER,
    actual_fee_bps INTEGER,
    expected_slippage_bps INTEGER,
    actual_slippage_bps INTEGER,
    decision_time TIMESTAMP,
    order_sent_time TIMESTAMP,
    fill_time TIMESTAMP,
    total_latency_ms INTEGER,
    fill_status TEXT,
    fill_ratio REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (decision_id) REFERENCES order_routing_decisions(decision_id)
);
```

### 6.5 OMS + Inventory + CTF（订单与库存管理）

```sql
-- 订单表
CREATE TABLE orders (
    order_id TEXT PRIMARY KEY,
    client_order_id TEXT UNIQUE NOT NULL,
    wallet_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    side TEXT NOT NULL,
    price DECIMAL NOT NULL,
    size DECIMAL NOT NULL,
    route_action TEXT NOT NULL,
    time_in_force TEXT NOT NULL,
    expiration TIMESTAMP,
    fee_rate_bps INTEGER NOT NULL,
    signature_type INTEGER NOT NULL,
    funder TEXT NOT NULL,
    status TEXT NOT NULL,
    filled_size DECIMAL NOT NULL DEFAULT 0,
    remaining_size DECIMAL NOT NULL,
    avg_fill_price DECIMAL,
    created_at TIMESTAMP NOT NULL,
    submitted_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL,
    exchange_order_id TEXT,
    fee_paid DECIMAL NOT NULL DEFAULT 0,
    inventory_reserved BOOLEAN NOT NULL DEFAULT FALSE,
    inventory_updated BOOLEAN NOT NULL DEFAULT FALSE
);

-- 成交表
CREATE TABLE fills (
    fill_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    wallet_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    side TEXT NOT NULL,
    price DECIMAL NOT NULL,
    size DECIMAL NOT NULL,
    fee DECIMAL NOT NULL,
    fee_rate_bps INTEGER NOT NULL,
    trade_id TEXT UNIQUE NOT NULL,
    exchange_order_id TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- 订单状态转移表
CREATE TABLE order_state_transitions (
    transition_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    from_status TEXT NOT NULL,
    to_status TEXT NOT NULL,
    reason TEXT,
    timestamp TIMESTAMP NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- 库存仓位表
CREATE TABLE inventory_positions (
    wallet_id TEXT NOT NULL,
    asset_type TEXT NOT NULL,
    token_id TEXT,
    market_id TEXT,
    outcome TEXT,
    balance_type TEXT NOT NULL,
    quantity DECIMAL NOT NULL,
    funder TEXT NOT NULL,
    signature_type INTEGER NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (wallet_id, asset_type, token_id, market_id, outcome, balance_type)
);

-- reservation 表
CREATE TABLE reservations (
    reservation_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    wallet_id TEXT NOT NULL,
    asset_type TEXT NOT NULL,
    token_id TEXT,
    market_id TEXT,
    outcome TEXT,
    funder TEXT NOT NULL,
    signature_type INTEGER NOT NULL,
    reserved_quantity DECIMAL NOT NULL,
    remaining_quantity DECIMAL NOT NULL,
    reserved_notional DECIMAL NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

-- 库存账本表
CREATE TABLE inventory_ledger (
    ledger_id TEXT PRIMARY KEY,
    wallet_id TEXT NOT NULL,
    asset_type TEXT NOT NULL,
    token_id TEXT,
    market_id TEXT,
    outcome TEXT,
    balance_type TEXT NOT NULL,
    operation TEXT NOT NULL,
    amount DECIMAL NOT NULL,
    balance_after DECIMAL NOT NULL,
    funder TEXT NOT NULL,
    signature_type INTEGER NOT NULL,
    reference_id TEXT,
    reference_type TEXT,
    timestamp TIMESTAMP NOT NULL
);

-- 暴露快照表
CREATE TABLE exposure_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    wallet_id TEXT NOT NULL,
    funder TEXT NOT NULL,
    signature_type INTEGER NOT NULL,
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    open_order_size DECIMAL NOT NULL,
    reserved_notional_usdc DECIMAL NOT NULL,
    filled_position_size DECIMAL NOT NULL,
    settled_position_size DECIMAL NOT NULL,
    redeemable_size DECIMAL NOT NULL,
    captured_at TIMESTAMP NOT NULL
);

-- CTF 操作表
CREATE TABLE ctf_operations (
    operation_id TEXT PRIMARY KEY,
    operation_type TEXT NOT NULL,
    market_id TEXT NOT NULL,
    amount DECIMAL NOT NULL,
    status TEXT NOT NULL,
    tx_hash TEXT,
    created_at TIMESTAMP NOT NULL,
    confirmed_at TIMESTAMP
);

-- 对账结果表
CREATE TABLE reconciliation_results (
    reconciliation_id TEXT PRIMARY KEY,
    wallet_id TEXT NOT NULL,
    funder TEXT NOT NULL,
    signature_type INTEGER NOT NULL,
    asset_type TEXT NOT NULL,
    token_id TEXT,
    market_id TEXT,
    balance_type TEXT NOT NULL,
    local_quantity DECIMAL NOT NULL,
    remote_quantity DECIMAL NOT NULL,
    discrepancy DECIMAL NOT NULL,
    status TEXT NOT NULL,
    resolution TEXT,
    created_at TIMESTAMP NOT NULL
);
```

### 6.6 Resolution（结算层）

```sql
-- UMA 提案表（升级版）
CREATE TABLE uma_proposals (
    proposal_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    proposer TEXT NOT NULL,
    proposed_outcome TEXT NOT NULL,
    proposal_bond REAL NOT NULL,
    dispute_bond REAL,
    proposal_tx_hash TEXT NOT NULL,
    proposal_block_number INTEGER NOT NULL,
    proposal_timestamp TIMESTAMP NOT NULL,
    status TEXT NOT NULL,
    on_chain_settled_at TIMESTAMP,
    safe_redeem_after TIMESTAMP,
    human_review_required BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 提案状态转移表
CREATE TABLE proposal_state_transitions (
    transition_id TEXT PRIMARY KEY,
    proposal_id TEXT NOT NULL,
    old_status TEXT NOT NULL,
    new_status TEXT NOT NULL,
    tx_hash TEXT NOT NULL,
    block_number INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    recorded_at TIMESTAMP NOT NULL,
    FOREIGN KEY (proposal_id) REFERENCES uma_proposals(proposal_id)
);

-- 区块水位线表
CREATE TABLE block_watermarks (
    chain_id INTEGER PRIMARY KEY,
    last_processed_block INTEGER NOT NULL,
    last_finalized_block INTEGER NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

-- 结算验证
CREATE TABLE settlement_verifications (
    verification_id TEXT PRIMARY KEY,
    proposal_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    proposed_outcome TEXT NOT NULL,
    expected_outcome TEXT NOT NULL,
    is_correct BOOLEAN NOT NULL,
    confidence REAL NOT NULL,
    discrepancy_details TEXT,
    sources_checked TEXT,
    evidence_package TEXT,  -- JSON: 完整的验证证据
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (proposal_id) REFERENCES uma_proposals(proposal_id)
);

CREATE TABLE processed_uma_events (
    event_id TEXT PRIMARY KEY,
    tx_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    block_number INTEGER NOT NULL,
    processed_at TIMESTAMP NOT NULL
);

CREATE TABLE proposal_evidence_links (
    proposal_id TEXT NOT NULL,
    verification_id TEXT NOT NULL,
    evidence_package_id TEXT NOT NULL,
    linked_at TIMESTAMP NOT NULL,
    PRIMARY KEY (proposal_id, verification_id)
);

-- Dispute 决策（human-in-the-loop）
CREATE TABLE dispute_decisions (
    decision_id TEXT PRIMARY KEY,
    proposal_id TEXT NOT NULL,
    verification_id TEXT NOT NULL,
    should_dispute BOOLEAN NOT NULL,
    confidence REAL NOT NULL,
    expected_profit REAL NOT NULL,
    risk_assessment TEXT NOT NULL,
    reasoning TEXT NOT NULL,
    position_value REAL NOT NULL,
    dispute_bond REAL NOT NULL,
    human_approved BOOLEAN DEFAULT FALSE,  -- 人工确认标志
    approved_by TEXT,
    approved_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (proposal_id) REFERENCES uma_proposals(proposal_id),
    FOREIGN KEY (verification_id) REFERENCES settlement_verifications(verification_id)
);

-- Redeem 计划
CREATE TABLE redeem_plans (
    plan_id TEXT PRIMARY KEY,
    proposal_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    position_value REAL NOT NULL,
    optimal_redeem_time TIMESTAMP NOT NULL,
    urgency TEXT NOT NULL,
    reasoning TEXT NOT NULL,
    executed BOOLEAN DEFAULT FALSE,
    executed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (proposal_id) REFERENCES uma_proposals(proposal_id)
);
```

### 6.7 Capability Registry（能力注册表）

```sql
-- MarketCapability 表
CREATE TABLE market_capabilities (
    market_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    token_id TEXT PRIMARY KEY,
    outcome TEXT NOT NULL,
    tick_size DECIMAL NOT NULL,
    fee_rate_bps INTEGER NOT NULL,
    neg_risk BOOLEAN NOT NULL,
    min_order_size DECIMAL NOT NULL,
    tradable BOOLEAN NOT NULL,
    fees_enabled BOOLEAN NOT NULL,
    data_sources TEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

-- AccountTradingCapability 表
CREATE TABLE account_trading_capabilities (
    wallet_id TEXT PRIMARY KEY,
    wallet_type TEXT NOT NULL,
    signature_type INTEGER NOT NULL,
    funder TEXT NOT NULL,
    allowance_targets TEXT NOT NULL,
    can_use_relayer BOOLEAN NOT NULL,
    can_trade BOOLEAN NOT NULL,
    restricted_reason TEXT,
    updated_at TIMESTAMP NOT NULL
);

-- operator override 表
CREATE TABLE capability_overrides (
    override_id TEXT PRIMARY KEY,
    scope TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    field_name TEXT NOT NULL,
    value TEXT NOT NULL,
    reason TEXT,
    created_at TIMESTAMP NOT NULL
);

-- ExecutionContext 快照表
CREATE TABLE execution_contexts (
    execution_context_id TEXT PRIMARY KEY,
    wallet_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    route_action TEXT NOT NULL,
    fee_rate_bps INTEGER NOT NULL,
    tick_size DECIMAL NOT NULL,
    signature_type INTEGER NOT NULL,
    funder TEXT NOT NULL,
    risk_gate_result TEXT NOT NULL,
    market_capability_ref TEXT NOT NULL,
    account_capability_ref TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);
```

### 6.8 Signer Service（签名服务）

```sql
-- 签名审计日志表
CREATE TABLE signature_audit_logs (
    log_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    signature_type TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    signature TEXT,
    status TEXT NOT NULL,
    requester TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    error TEXT
);

CREATE INDEX idx_audit_logs_requester ON signature_audit_logs(requester);
CREATE INDEX idx_audit_logs_timestamp ON signature_audit_logs(timestamp);
CREATE INDEX idx_audit_logs_status ON signature_audit_logs(status);
```

### 6.9 Event Sourcing（事件溯源）

```sql
-- 领域事件表
CREATE TABLE domain_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    payload TEXT NOT NULL,  -- JSON
    timestamp TIMESTAMP NOT NULL,

    -- 因果关系
    causation_id TEXT,
    correlation_id TEXT NOT NULL,

    -- 幂等性
    idempotency_key TEXT UNIQUE,

    -- 元数据
    metadata TEXT,  -- JSON

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_domain_events_aggregate ON domain_events(aggregate_id, aggregate_type);
CREATE INDEX idx_domain_events_correlation ON domain_events(correlation_id);
CREATE INDEX idx_domain_events_type ON domain_events(event_type);
CREATE INDEX idx_domain_events_timestamp ON domain_events(timestamp);
CREATE INDEX idx_domain_events_idempotency ON domain_events(idempotency_key);
```

### 6.10 Resolution Spec（结算规范）

```sql
-- 结算规范表（升级版 MarketSpec）
CREATE TABLE resolution_specs (
    spec_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    spec_version TEXT NOT NULL,

    -- 基础信息
    location_name TEXT NOT NULL,
    station_id TEXT NOT NULL,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    timezone TEXT NOT NULL,
    observation_date DATE NOT NULL,
    observation_window_local TEXT NOT NULL,
    metric TEXT NOT NULL,
    unit TEXT NOT NULL,
    min_value REAL,
    max_value REAL,

    -- 结算源配置
    authoritative_source TEXT NOT NULL,
    fallback_sources TEXT NOT NULL,
    rounding_rule TEXT NOT NULL,
    inclusive_bounds BOOLEAN NOT NULL,

    -- 元数据
    parse_confidence REAL,
    risk_flags TEXT,
    rules_hash TEXT,
    parsed_at TIMESTAMP NOT NULL,
    deprecated_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (market_id) REFERENCES weather_markets(market_id)
);
```

---

### 6.11 Agent（Agent 层）
    market_id TEXT NOT NULL,
    position_value REAL NOT NULL,
    optimal_redeem_time TIMESTAMP NOT NULL,
    urgency TEXT NOT NULL,
    reasoning TEXT NOT NULL,
    executed BOOLEAN DEFAULT FALSE,
    executed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (proposal_id) REFERENCES uma_proposals(proposal_id)
);
```

### 6.6 Agent（Agent 层）

```sql
-- Agent 性能
CREATE TABLE agent_performance_v1 (
    performance_id TEXT PRIMARY KEY,
    agent_name TEXT NOT NULL,
    task_id TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    output_hash TEXT NOT NULL,
    latency_ms INTEGER NOT NULL,
    confidence_score REAL,
    human_feedback TEXT,
    error_type TEXT,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 6.7 Shared Runtime（共享运行时）

```sql
-- 交易日志
CREATE TABLE journal_trades (
    trade_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    outcome_id TEXT NOT NULL,
    side TEXT NOT NULL,
    size REAL NOT NULL,
    price REAL NOT NULL,
    fee REAL NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 组合约束
CREATE TABLE portfolio_constraints_v1 (
    constraint_id TEXT PRIMARY KEY,
    max_position_per_market REAL NOT NULL,
    max_correlated_exposure REAL NOT NULL,
    min_liquidity_depth REAL NOT NULL,
    max_model_error_rate REAL NOT NULL,
    circuit_breaker_triggered BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 系统健康事件
CREATE TABLE system_health_events (
    event_id TEXT PRIMARY KEY,
    component TEXT NOT NULL,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    message TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## 7. 风险管理框架

### 7.1 仓位限制（Position Limiter）

**目标**: 控制单市场和总体敞口，避免过度集中。

**规则**:
```python
# 单市场最大敞口
MAX_POSITION_PER_MARKET = 500  # USD

# 单领域最大敞口
MAX_DOMAIN_EXPOSURE = 2000  # USD

# 总敞口
MAX_TOTAL_EXPOSURE = 5000  # USD
```

**实现**:
```python
def check_position_limit(market_id: str, new_position: float) -> bool:
    current_position = get_current_position(market_id)
    total_position = current_position + new_position
    
    if abs(total_position) > MAX_POSITION_PER_MARKET:
        return False
    
    domain_exposure = get_domain_exposure("weather")
    if domain_exposure + new_position > MAX_DOMAIN_EXPOSURE:
        return False
    
    total_exposure = get_total_exposure()
    if total_exposure + new_position > MAX_TOTAL_EXPOSURE:
        return False
    
    return True
```

### 7.2 相关性监控（Correlation Monitor）

**目标**: 监控相关市场的敞口，避免系统性风险。

**问题**: 同时做 10 个城市的温度市场，它们可能高度相关（同一天气系统影响）。

**解决方案**:
```python
# 1. 计算市场间相关性
correlation_matrix = calculate_correlation(markets)

# 2. 识别高相关市场组
correlated_groups = find_correlated_groups(correlation_matrix, threshold=0.7)

# 3. 限制相关市场总敞口
for group in correlated_groups:
    group_exposure = sum(get_position(m) for m in group)
    if group_exposure > MAX_CORRELATED_EXPOSURE:
        alert("Correlated exposure too high")
```

### 7.3 流动性保护（Liquidity Guard）

**目标**: 避免在流动性不足的市场交易。

**规则**:
```python
MIN_LIQUIDITY_DEPTH = 50  # USD
MAX_SLIPPAGE_BPS = 100    # 1%
MIN_LIQUIDITY_SCORE = 50  # 0-100
```

**实现**:
```python
def check_liquidity(snapshot: OrderBookSnapshot, size_usd: float) -> bool:
    liquidity = liquidity_estimator.estimate(snapshot)
    
    if not liquidity.is_liquid:
        return False
    
    if liquidity.liquidity_score < MIN_LIQUIDITY_SCORE:
        return False
    
    slippage = slippage_model.estimate_slippage(snapshot, size_usd)
    if slippage.slippage_bps > MAX_SLIPPAGE_BPS:
        return False
    
    return True
```

### 7.4 模型验证（Model Validator）

**目标**: 检测模型系统性偏差，及时熔断。

**指标**:
```python
# 预测误差率
MAX_MODEL_ERROR_RATE = 0.20  # 20%

# 连续错误次数
MAX_CONSECUTIVE_ERRORS = 3
```

**实现**:
```python
def validate_model_performance():
    recent_predictions = get_recent_predictions(days=7)
    
    error_rate = calculate_error_rate(recent_predictions)
    if error_rate > MAX_MODEL_ERROR_RATE:
        trigger_circuit_breaker("Model error rate too high")
    
    consecutive_errors = count_consecutive_errors(recent_predictions)
    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
        trigger_circuit_breaker("Too many consecutive errors")
```

### 7.5 熔断机制（Circuit Breaker）

**目标**: 在异常情况下自动停止交易。

**触发条件**:
- 模型错误率 > 20%
- 连续 3 次预测错误
- 单日亏损 > 10%
- 系统异常（数据源不可用、API 错误等）

**实现**:
```python
class CircuitBreaker:
    def __init__(self):
        self.is_open = False
        self.reason = None
    
    def trigger(self, reason: str):
        self.is_open = True
        self.reason = reason
        alert(f"Circuit breaker triggered: {reason}")
        stop_all_trading()
    
    def reset(self):
        self.is_open = False
        self.reason = None
        alert("Circuit breaker reset")
    
    def check(self) -> bool:
        if self.is_open:
            raise CircuitBreakerError(f"Trading halted: {self.reason}")
        return True
```

---

## 8. 开发路线图

详细执行顺序、阶段依赖、验收条件与首批任务列表，统一以 [DEVELOPMENT_ROADMAP.md](./DEVELOPMENT_ROADMAP.md) 为准。本节保留项目级 phase 视图，不再承载实施级拆解。

`P0` 的实施拆解、任务顺序、交付物和验收动作，统一以下面文档为准：

- [P0_Implementation_Plan.md](../10-implementation/phase-plans/P0_Implementation_Plan.md)

### Phase 0: 项目初始化 ✅

**目标**: 建立项目基础

**任务**:
- ✅ 新仓库：`asterion`
- ✅ 从 AlphaDesk 抽出 `asterion_core`
- ✅ 保留 AlphaDesk 作为只读参考
- ✅ 完成详细设计文档

**交付物**:
- ✅ 项目结构
- ✅ 详细设计文档
- ✅ CLOB Order Router 设计
- ✅ UMA Watcher 设计

---

### Phase 1: Weather MVP（已完成）

**目标**: 实现 watch-only 模式的完整闭环

**时间**: 4-6 周

**当前结论**:

- `market discovery -> Rule2Spec -> station-first onboarding -> forecast -> pricing -> watch-only snapshots` 已闭合
- `UMA watcher replay -> settlement verification -> evidence linkage -> redeem readiness suggestion` 已闭合
- operator 最小只读面已固定为 `DuckDB + UI replica + runbook`
- `P1` 关闭依据见 [P1_Closeout_Checklist.md](../10-implementation/checklists/P1_Closeout_Checklist.md)
- 当前 canonical 运行入口见 [P1_Watch_Only_Replay_Cold_Path_Runbook.md](../10-implementation/runbooks/P1_Watch_Only_Replay_Cold_Path_Runbook.md)

**交付物**:
- Watch-only 模式运行
- Forecast / fair value / watch-only snapshot provenance
- Replay + settlement verification 基础闭环
- Operator 只读面（DuckDB + UI replica + runbook）

---

### Phase 2: Replay / Cold Path / Monitor（已完成）

**目标**: 强化 replay / cold path，并完成 execution foundation、operator 读面和 AlphaDesk Exit Gate 收口，为 `P3 paper execution` 做准备

**实际完成**:

#### Step 1: Replay / Cold Path 强化
- [x] 实现 forecast replay / deterministic recompute
- [x] 实现 watcher multi-RPC fallback / backfill
- [x] 实现 cold-path orchestration

#### Step 2: Execution Foundation
- [x] 实现 strategy_engine_v3
- [x] 实现 trade_ticket_v1
- [x] 实现 signal_to_order_v1
- [x] 实现 execution_gate_v1
- [x] 实现 portfolio_v3
- [x] 实现 journal_v3

#### Step 3: Ops / Exit Gate
- [x] 实现 readiness_checker_v1
- [x] 实现 ui_lite_db
- [x] 完成 AlphaDesk Exit Gate 审查

**交付物**:
- replay / backfill / cold path 编排
- execution foundation 主干
- `UI replica + UI lite + readiness report` operator surface
- AlphaDesk Exit Gate `EXIT_READY`

---

### Phase 3: Paper execution（已完成）

**目标**: 在 deterministic / watch-only / replay 底座之上打通 `paper execution` 主链、operator 读面、daily ops / review input 与 readiness / closeout 闭环

**任务**:
- [x] Canonical handoff / order router / paper adapter / quote-based fill simulator
- [x] OMS state machine / reservation / inventory / exposure / reconciliation
- [x] operator read model / paper run journal / daily ops / review input
- [x] readiness gate / closeout checklist / paper execution runbook
- [ ] Daily Review Agent（后续 automation / agent 化范围）

**交付物**:
- paper execution 主链路
- operator / readiness / journal / closeout 联调闭环

---

### Phase 4: Live prerequisites（已完成）

**目标**: 在 paper execution 之后补齐真实数据 ingress、capability refresh、external observation、signer / submitter / chain-tx scaffold、external reconciliation、operator live-prereq read model、minimum ops hardening，以及 controlled live rollout decision 边界

**任务**:
- [x] real data ingress / capability refresh / wallet state observation
- [x] signer shell / official-order-compatible signing / submitter dry-run-shadow
- [x] approve-first chain tx scaffold / external execution reconciliation
- [x] operator live-prereq read model / minimum ops hardening
- [x] controlled live smoke boundary / closeout checklist / rollout decision runbook

**交付物**:
- canonical `P4` live-prereq chain
- `P4` closeout checklist
- controlled live smoke runbook
- controlled rollout decision runbook
- `ready for controlled live rollout decision`

---

## 9. 技术栈与工具

### 9.1 编程语言

- **Python 3.11+** - 主要开发语言

### 9.2 数据层

| 技术 | 用途 | 原因 |
|------|------|------|
| DuckDB | 主数据库 | 列式存储，OLAP 性能优秀，支持 Parquet |
| SQLite | Write queue | 轻量级，单写者模式 |
| Parquet | 原始数据存储 | 压缩率高，列式存储 |

### 9.3 区块链交互

| 技术 | 用途 |
|------|------|
| Web3.py | Polygon 交互 |
| eth-account | 私钥管理 |
| eth-abi | ABI 编解码 |

### 9.4 HTTP 与 WebSocket

| 技术 | 用途 |
|------|------|
| HTTPX | 异步 HTTP 客户端 |
| websockets | WebSocket 客户端 |
| aiohttp | 备用异步 HTTP 库 |

### 9.5 数据处理

| 技术 | 用途 |
|------|------|
| Pandas | 数据分析 |
| NumPy | 数值计算 |
| SciPy | 科学计算 |

### 9.6 调度与编排

| 技术 | 用途 |
|------|------|
| Dagster | 数据编排 |
| Supervisord | 进程管理 |
| APScheduler | 定时任务 |

### 9.7 UI 与可视化

| 技术 | 用途 |
|------|------|
| Streamlit | Operator UI |
| Plotly | 交互式图表 |
| Altair | 声明式可视化 |

### 9.8 AI 与 Agent

| 技术 | 用途 |
|------|------|
| Anthropic SDK | Claude API |
| LangChain | Agent 框架（可选） |

### 9.9 测试

| 技术 | 用途 |
|------|------|
| pytest | 单元测试 |
| pytest-asyncio | 异步测试 |
| pytest-mock | Mock 测试 |

### 9.10 开发工具

| 技术 | 用途 |
|------|------|
| Poetry | 依赖管理 |
| Black | 代码格式化 |
| Ruff | Linter |
| mypy | 类型检查 |

---

## 10. 附录

### 10.1 关键设计决策记录

#### 决策 1: 为什么不继续沿用 AlphaDesk

**背景**: AlphaDesk 已经有完整的交易系统。

**决策**: 新建 Asterion 项目，复用底座，重建 domain layer。

**原因**:
- AlphaDesk 的 universe 明确限制在 Crypto
- 特征层和策略层有大量 crypto-specific 逻辑
- 直接修改会导致代码混乱

**结果**: 清晰的架构边界，易于扩展到多领域。

---

#### 决策 2: Agent 在执行路径之外

**背景**: AI Agent 可以自动化很多任务。

**决策**: 所有 Agent 只做"编译、校验、监控、复盘"，永远不直接触发交易。

**原因**:
- 金融系统的安全第一原则
- LLM 的不确定性
- 需要人工审核或规则验证

**结果**: 系统安全性高，Agent 价值明确。

---

#### 决策 3: 自适应订单路由

**背景**: Polymarket 的 Maker/Taker 费率差异（2.2%）很大。

**决策**: 实现自适应订单路由，根据市场状态选择 Maker/Taker。

**原因**:
- Maker 返佣 0.2%，但不保证成交
- Taker 收费 2%，但成交确定
- 需要在两者之间权衡

**结果**: 执行成本优化，提高盈利能力。

---

#### 决策 4: 多数据源验证

**背景**: 单一数据源可能不准确。

**决策**: 结算时交叉验证多个天气数据源。

**原因**:
- 提高准确性
- 降低 dispute 风险
- 增加置信度

**结果**: 结算准确率高，dispute 决策更可靠。

---

#### 决策 5: Event Sourcing 模式

**背景**: 需要完整的审计轨迹。

**决策**: 所有状态变更都记录为不可变事件。

**原因**:
- 完整的审计轨迹
- 可以重放历史状态
- 方便 backtesting

**结果**: 系统可追溯性强，便于调试和优化。

---

### 10.2 术语表

| 术语 | 解释 |
|------|------|
| **Domain Pack** | 领域模块，如 Weather、Tech、Crypto |
| **MarketSpec** | 市场规格，结构化的市场规则 |
| **Fair Value** | 公允价值，基于预测的理论价格 |
| **Maker** | 挂单者，提供流动性 |
| **Taker** | 吃单者，消耗流动性 |
| **Slippage** | 滑点，预期价格与实际成交价的差异 |
| **UMA** | Universal Market Access，去中心化预言机 |
| **Optimistic Oracle** | 乐观预言机，UMA 的结算机制 |
| **Dispute** | 质疑，对 UMA 提案的挑战 |
| **Redeem** | 赎回，结算后提取资金 |
| **CLOB** | Central Limit Order Book，中央限价订单簿 |
| **Polygon** | 以太坊 Layer 2 扩容方案 |
| **Gamma API** | Polymarket 的市场数据 API |
| **CLOB API** | Polymarket 的订单簿 API |

---

### 10.3 参考资料

#### Polymarket 文档
- [Gamma API Documentation](https://docs.polymarket.com/)
- [CLOB API Documentation](https://docs.polymarket.com/clob)
- [Polymarket Resolution Process](https://docs.polymarket.com/resolution)

#### UMA 文档
- [UMA Optimistic Oracle](https://docs.uma.xyz/optimistic-oracle)
- [UMA Resolution Process](https://docs.uma.xyz/resolution)

#### 天气数据源
- [Open-Meteo API](https://open-meteo.com/en/docs)
- [NWS API](https://www.weather.gov/documentation/services-web-api)
- [Weather.com API](https://www.weather.com/swagger-docs/)

#### AlphaDesk 参考
- AlphaDesk 项目文档（内部）
- AlphaDesk 代码库（内部）

---

### 10.4 FAQ

#### Q1: 为什么选择 Weather 作为第一个 domain pack？

**A**: Weather 市场具有以下特点：
- 规则模板化强，易于解析
- 生命周期短（1-2天），快速验证
- 结果可数值化，易于验证
- Agent 价值明确（规则解析、数据验证）
- 执行难度中等，适合 MVP

#### Q2: Asterion 和 AlphaDesk 的关系是什么？

**A**: Asterion 复用 AlphaDesk 的平台底座（数据接入、写入队列、执行骨架、监控），但重建 domain layer。AlphaDesk 保留作为只读参考，不再主动开发。

#### Q3: Agent 为什么不直接触发交易？

**A**: 金融系统的安全第一原则。LLM 的不确定性不应该直接影响资金。Agent 只做"编译、校验、监控、复盘"，交易决策由规则引擎执行。

#### Q4: 如何处理 Polymarket 的 Maker/Taker 费率差异？

**A**: 实现自适应订单路由。根据市场流动性、价差、滑点动态选择 Maker（挂单）或 Taker（吃单）策略。在流动性好、价差小时选择 Taker；否则选择 Maker。

#### Q5: 如何保证结算的准确性？

**A**: 
1. 多数据源交叉验证（Open-Meteo、NWS、Weather.com）
2. 计算置信度（数据源一致性）
3. 只在高置信度（>95%）时发起 dispute
4. 人工审核高风险案例

#### Q6: 如何扩展到 Tech 和 Crypto？

**A**: 
- **Tech**: 复用 Market Scout、Rule2Spec、Execution Runtime，但把 ForecastAdapter 换成 EvidenceAdapter
- **Crypto**: 复用 Polymarket 接入、Runtime、Gate、Journal，但新建 crypto domain pack，增加外部市场锚定和对冲逻辑

#### Q7: 如何控制风险？

**A**: 
1. 仓位限制（单市场、单领域、总敞口）
2. 相关性监控（避免高相关市场过度敞口）
3. 流动性保护（最小流动性要求）
4. 模型验证（错误率监控）
5. 熔断机制（异常情况自动停止交易）

#### Q8: 开发周期是多久？

**A**: 
- Phase 1 (Weather MVP watch-only): 4-6 周
- Phase 2 (Replay / cold path / monitor): 2-3 个月
- Phase 3 (Paper execution): 4-8 周
- Phase 4 (Live prerequisites): 4-8 周

Tech / Crypto domain expansion 进入后续 backlog，不再作为当前阶段编号的一部分。

---

### 10.5 下一步行动

#### 立即行动（本周）
1. ✅ 完成项目方案文档
2. [ ] 设置开发环境
3. [ ] 创建 Git 仓库
4. [ ] 从 AlphaDesk 抽取核心代码

#### 短期目标（2 周内）
1. [ ] 实现 Gamma API 客户端
2. [ ] 实现 Market Scout
3. [ ] 实现 Rule2Spec Agent（第一版）
4. [ ] 建立数据库 schema

#### 中期目标（1 个月内）
1. [ ] 完成 Forecast Ensemble
2. [ ] 完成 Pricing Engine
3. [ ] 完成 Watch-only 模式
4. [ ] 部署第一版 Operator UI

#### 长期目标（3 个月内）
1. [ ] 完成 Paper Trade
2. [ ] 完成 UMA Watcher
3. [ ] 开始小资金 Live

---

### 10.6 联系方式

- **项目负责人**: Jay Zhu
- **创建日期**: 2026-03-07
- **文档版本**: v1.0
- **最后更新**: 2026-03-07

---

### 10.7 版本历史

| 版本   | 日期         | 变更说明                |
| ---- | ---------- | ------------------- |
| v1.0 | 2026-03-07 | 完整项目方案，包含所有核心模块详细设计 |


---

## 结语

Asterion 项目的核心价值在于：

1. **清晰的架构边界** - Domain Pack 架构支持多领域扩展
2. **安全的 Agent 设计** - Agent 在执行路径之外，保证系统安全
3. **优化的执行策略** - CLOB Order Router 优化 Maker/Taker 费率
4. **可靠的结算监控** - UMA Watcher 保证结算准确性
5. **完善的风险管理** - 多层次风险控制机制

通过复用 AlphaDesk 的平台底座，Asterion 可以快速启动；通过重建 domain layer，Asterion 可以灵活扩展到多个领域。

Weather MVP 是第一步，但不是终点。Asterion 的目标是成为一个通用的 Polymarket 事件交易平台，支持 Weather、Tech、Crypto 等多个领域。

**让我们开始构建吧！** 🚀

---

**文档结束**
