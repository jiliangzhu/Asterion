# Asterion（星枢）项目

**版本**: v1.2
**更新日期**: 2026-03-12
**状态**: P4 implementation in progress (`P4-01` / `P4-02` / `P4-03` / `P4-04` / `P4-05` / `P4-06` / `P4-07` / `P4-08` / `P4-09` / `P4-10` completed)

---

## 📖 项目简介

**Asterion（星枢）** 是一个建立在 AlphaDesk 底座之上的、面向 Polymarket 多领域事件市场的"研究、Agent、定价、执行、风控"统一平台。

**核心定位**: 不是 AlphaDesk 的"天气分支"，而是一个面向多模块扩展的事件交易平台。

---

## 📚 文档结构

```text
Asterion/
├── README.md                                  # 本文件 - 唯一保留在根目录的导航文档
└── docs/
    ├── 00-overview/
    │   ├── Documentation_Index.md            # 文档索引与分类规范
    │   ├── Asterion_Project_Plan.md          # 完整项目计划（主文档）
    │   └── DEVELOPMENT_ROADMAP.md            # 开发路线图（执行顺序）
    ├── 10-implementation/
    │   ├── Implementation_Index.md          # 实施文档总入口
    │   ├── phase-plans/
    │   │   ├── P0_Implementation_Plan.md
    │   │   ├── P1_Implementation_Plan.md
    │   │   ├── P2_Implementation_Plan.md
    │   │   ├── P3_Implementation_Plan.md
    │   │   └── P4_Implementation_Plan.md
    │   ├── checklists/
    │   │   ├── P0_Closeout_Checklist.md
    │   │   ├── P1_Closeout_Checklist.md
    │   │   ├── P2_Closeout_Checklist.md
    │   │   ├── P3_Closeout_Checklist.md
    │   │   └── P1_P2_AlphaDesk_Remaining_Migration_Checklist.md
    │   ├── runbooks/
    │   │   ├── P1_Watch_Only_Replay_Cold_Path_Runbook.md
    │   │   ├── P2_Cold_Path_Orchestration_Job_Map_Runbook.md
    │   │   └── P3_Paper_Execution_Runbook.md
    │   ├── migration-ledger/
    │   │   └── AlphaDesk_Migration_Ledger.md
    │   └── module-notes/
    │       └── AlphaDesk_*.md
    ├── 20-architecture/
    │   ├── Database_Architecture_Design.md
    │   ├── Event_Sourcing_Design.md
    │   └── Hot_Cold_Path_Architecture.md
    ├── 30-trading/
    │   ├── CLOB_Order_Router_Design.md
    │   ├── OMS_Design.md
    │   ├── Market_Capability_Registry_Design.md
    │   ├── Signer_Service_Design.md
    │   └── Gas_Manager_Design.md
    ├── 40-weather/
    │   ├── Forecast_Ensemble_Design.md
    │   └── UMA_Watcher_Design.md
    └── 50-operations/
        └── Agent_Monitor_Design.md
```

---

## 🎯 快速开始

### 1. 了解项目

**推荐阅读顺序**:
1. 阅读本 README（5 分钟）
2. 阅读 [Documentation_Index.md](./docs/00-overview/Documentation_Index.md)（10 分钟）
3. 阅读 [Asterion_Project_Plan.md](./docs/00-overview/Asterion_Project_Plan.md)（30 分钟）
4. 阅读 [DEVELOPMENT_ROADMAP.md](./docs/00-overview/DEVELOPMENT_ROADMAP.md)（15 分钟）
   - 重点看其中的 `AlphaDesk Migration Track`，这里已经按实际代码语义区分了“直接迁入 / 保留壳重写 / 禁止迁入”
5. 阅读 [Implementation_Index.md](./docs/10-implementation/Implementation_Index.md)
   - 这是所有实施文档的统一入口，后续阶段文档都从这里找
6. 阅读 [P4_Implementation_Plan.md](./docs/10-implementation/phase-plans/P4_Implementation_Plan.md)
   - 这是 `P4 live prerequisites` 当前唯一实施入口，后续开发以本文件为准
7. 阅读 [P3_Closeout_Checklist.md](./docs/10-implementation/checklists/P3_Closeout_Checklist.md)
   - 这是 `P3` 是否具备 closeout 条件、是否可进入 `P4 planning` 的 closeout 审查入口
8. 阅读 [P3_Paper_Execution_Runbook.md](./docs/10-implementation/runbooks/P3_Paper_Execution_Runbook.md)
   - 这是 `P3 paper execution` 当前 canonical operator / daily ops / readiness 运行入口
9. 如需进入 `P4` 之前的阶段边界，再阅读 [P3_Implementation_Plan.md](./docs/10-implementation/phase-plans/P3_Implementation_Plan.md)
10. 阅读 [P1_P2_AlphaDesk_Remaining_Migration_Checklist.md](./docs/10-implementation/checklists/P1_P2_AlphaDesk_Remaining_Migration_Checklist.md)
   - 如果目标是“彻底脱离 AlphaDesk 后再建独立 Git 仓库”，这份清单是当前唯一判断依据
11. 阅读 [P2_Closeout_Checklist.md](./docs/10-implementation/checklists/P2_Closeout_Checklist.md)
   - 这是 `P2` 是否已经关闭、`P3` 是否可以开工、AlphaDesk Exit Gate 是否通过的唯一关闭依据
12. 阅读 [P1_Watch_Only_Replay_Cold_Path_Runbook.md](./docs/10-implementation/runbooks/P1_Watch_Only_Replay_Cold_Path_Runbook.md)
   - 这是 `watch-only / replay / cold path` 当前 canonical 入口和 operator 读路径
13. 阅读 [P2_Cold_Path_Orchestration_Job_Map_Runbook.md](./docs/10-implementation/runbooks/P2_Cold_Path_Orchestration_Job_Map_Runbook.md)
   - 这是 `P2-07` 到 `P2-09` 的 canonical job map、schedule 和 handler 入口
14. 如需回看 `P2` 的实施顺序，再阅读 [P2_Implementation_Plan.md](./docs/10-implementation/phase-plans/P2_Implementation_Plan.md)
15. 如需回看 `P1` 阶段计划，再阅读 [P1_Implementation_Plan.md](./docs/10-implementation/phase-plans/P1_Implementation_Plan.md)
16. 如需回看底座建设，再阅读 [P0_Implementation_Plan.md](./docs/10-implementation/phase-plans/P0_Implementation_Plan.md)
17. 深入阅读详细设计文档（按需）

### 1.1 文档归档规则

- 根目录只保留 `README.md`
- 其他项目文档全部进入 `docs/`
- `docs/10-implementation/` 根目录只保留 [Implementation_Index.md](./docs/10-implementation/Implementation_Index.md)
- 阶段实施计划统一进入 `docs/10-implementation/phase-plans/`
- 阶段清单统一进入 `docs/10-implementation/checklists/`
- 运行入口、runbook、只读面说明统一进入 `docs/10-implementation/runbooks/`
- 迁移总台账统一进入 `docs/10-implementation/migration-ledger/`
- module notes 统一进入 `docs/10-implementation/module-notes/`
- 新增跨模块架构文档统一进入 `docs/20-architecture/`
- 新增交易执行相关文档统一进入 `docs/30-trading/`
- 新增 Weather 领域文档统一进入 `docs/40-weather/`
- 新增监控、运维、运营类文档统一进入 `docs/50-operations/`

### 2. 核心概念

- **Domain Pack 架构** - Weather、Tech、Crypto 三个独立领域模块
- **Agent 在执行路径之外** - 所有 AI Agent 只做分析，不直接触发交易
- **Canonical Order Contract** - 交易接口统一使用 `RouteAction + time_in_force/expiration`，避免 Router/OMS/CLOB adapter 语义漂移
- **ExecutionContext** - 下单前统一合并 `MarketCapability + AccountTradingCapability + risk gate`
- **Inventory Semantics** - reservation 与库存都按 `wallet_id + token_id + balance_type` 等主键维度闭合
- **Forecast-Resolution Contract** - Weather MVP 统一采用 `station-first`，预测和结算共享同一份站点契约
- **UMA Resolution 监控** - dispute 窗口和 redeem 时机均从链上/协议状态动态读取

### 2.1 开发环境

`P0/P1` 默认使用项目内 `.venv` 管理 Python 依赖，原因是 macOS/Homebrew Python 通常启用了 PEP 668，不能直接向系统解释器写入包。

推荐命令：

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -e .
python3 -m unittest discover -s tests -v
```

### 3. Weather MVP 范围

**目标市场**: 美国城市单日最高温区间市场

**核心流程**:
```
市场发现 → 规则解析 → 预测 → 定价 → 执行 → 结算监控 → 赎回
```

---

## 🏗️ 技术架构

### 当前已落地模块（P4-10）

```
asterion_core/              # 平台核心
├── blockchain/             # read-only observation + chain tx scaffold helpers
│   ├── wallet_state_v1.py
│   └── chain_tx_v1.py
├── clients/                # Polymarket / CLOB public API 客户端
├── contracts/              # canonical contracts / IDs
├── execution/              # execution foundation
│   ├── trade_ticket_v1.py
│   ├── signal_to_order_v1.py
│   ├── capability_refresh_v1.py
│   ├── execution_gate_v1.py
│   ├── order_router_v1.py
│   ├── paper_adapter_v1.py
│   ├── paper_fill_simulator_v1.py
│   ├── oms_state_machine_v1.py
│   └── watch_only_gate_v3.py
├── runtime/                # strategy runtime
│   ├── strategy_base.py
│   └── strategy_engine_v3.py
├── risk/                   # reservation / inventory / reconciliation
│   └── portfolio_v3.py
├── signer/                 # default-off signer shell / official order signing seam
│   └── signer_service_v1.py
├── journal/                # runtime / trading journal
│   └── journal_v3.py
├── monitoring/             # readiness / health
│   ├── health_monitor_v1.py
│   └── readiness_checker_v1.py
├── ui/                     # replica / lite read model / operator surfaces
│   ├── ui_db_replica.py
│   └── ui_lite_db.py
├── storage/                # DB / queue / determinism
└── ws/                     # WS ingest / agg

domains/weather/            # Weather 领域
├── scout/                  # 市场发现
├── spec/                   # 规则解析
├── forecast/               # 预测服务
├── pricing/                # 定价引擎
└── resolution/             # watcher / verification / backfill

dagster_asterion/           # Cold-path orchestration 壳
├── job_map.py              # canonical job/schedule map
├── handlers.py             # 纯 Python orchestration handlers
├── resources.py            # runtime resource factories
└── schedules.py            # 可选 Dagster schedule 壳

agents/                     # AI Agent
└── weather/
    ├── rule2spec_agent.py
    ├── data_qa_agent.py
    └── resolution_agent.py
```

说明：

- 上面只列当前仓库**已经落地**的主干模块
- `order_router_v1.py`、paper adapter、quote-based fill simulator、OMS state machine 已在 `P3` 落地
- `capability_refresh_v1.py` 与 `clients/clob_public.py` 已在 `P4-02` 落地，用于 canonical capability refresh
- `blockchain/wallet_state_v1.py` 与 `runtime.external_balance_observations` 已在 `P4-03` 落地，用于 external wallet state observation
- `signer/signer_service_v1.py`、`meta.signature_audit_logs`、`runtime.submit_attempts` 已在 `P4-04` / `P4-05` 落地，用于 signer shell、official-order-compatible signing 与 sign-only / submit attempt ledger
- `execution/live_submitter_v1.py`、`runtime.external_order_observations` 已在 `P4-06` 落地，用于 canonical submitter dry-run / shadow path
- `blockchain/chain_tx_v1.py`、`runtime.chain_tx_attempts` 已在 `P4-07` 落地，用于 approve-first gas / nonce / signing / shadow-broadcast scaffold
- `runtime.external_fill_observations`、`weather_external_execution_reconciliation` 与 external-aware `trading.reconciliation_results` 已在 `P4-08` 落地，用于 shadow external execution reconciliation
- `ui.live_prereq_execution_summary`、`ui.live_prereq_wallet_summary` 与扩展后的 `ui.execution_*` 已在 `P4-09` 落地，用于 operator live-prereq read model
- `evaluate_p4_live_prereq_readiness(...)` 与 `weather_live_prereq_readiness` 已在 `P4-10` 落地，用于 minimum ops hardening、hourly P4 readiness report 以及 `ui.phase_readiness_summary`
- `weather_chain_tx_smoke` 已成为 `P4-07` 的 canonical chain-tx manual entry；当前只开放 `approve_usdc`
- `weather_signer_audit_smoke`、`weather_order_signing_smoke`、`weather_submitter_smoke` 与 `weather_external_execution_reconciliation` 已成为 `P4` signer / order-signing / submitter / reconciliation 的 canonical entry
- `daily_review_agent.py` 仍未落地；当前只完成 `ui.daily_review_input` 等 review input surface
- `P4` 当前 canonical 实施入口见 [P4_Implementation_Plan.md](./docs/10-implementation/phase-plans/P4_Implementation_Plan.md)
- `P3` 的 canonical closeout 与 runbook 入口见 [P3_Closeout_Checklist.md](./docs/10-implementation/checklists/P3_Closeout_Checklist.md)、[P3_Paper_Execution_Runbook.md](./docs/10-implementation/runbooks/P3_Paper_Execution_Runbook.md)

---

## ✨ 核心特性

### 1. CLOB Order Router（订单路由）

**解决的问题**: 在 Polymarket 官方订单语义下，稳定输出可执行且不会返工的订单动作

**核心组件**:
- Liquidity Estimator - 实时评估订单簿深度
- Fee Calculator - 从 Market Capability Registry 读取每个 market/token 的动态费率
- Slippage Model - 滑点预测
- Routing Engine - 输出唯一 canonical `RouteAction`
- Execution Handoff - 提交前强制构造 `ExecutionContext`

**详细文档**: [CLOB_Order_Router_Design.md](./docs/30-trading/CLOB_Order_Router_Design.md)

### 2. UMA Watcher（结算监控）

**解决的问题**: UMA Optimistic Oracle 的 2 小时 dispute 窗口需要实时监控

**核心组件**:
- UMA Monitor - 提案状态追踪
- Settlement Verifier - 多源数据交叉验证
- Dispute Analyzer - Dispute 收益分析
- Redeem Scheduler - 最优赎回时机
- Replay Layer - finalized block watermark + restart replay + idempotent events

**详细文档**: [UMA_Watcher_Design.md](./docs/40-weather/UMA_Watcher_Design.md)

### 3. Forecast Ensemble（预测集成）

**解决的问题**: 单一数据源不可靠，需要多源预测组合

**核心组件**:
- Multi-source Adapters - Open-Meteo、NWS、Weather.com
- Source Router - 健康检查和自动故障转移
- Station-first Contract - `station_id + lat/lon + timezone + spec_version`
- Ensemble Combiner - 加权平均和置信度评估
- Forecast Cache - TTL 缓存和命中率追踪

**详细文档**: [Forecast_Ensemble_Design.md](./docs/40-weather/Forecast_Ensemble_Design.md)

### 4. Agent Monitor（Agent 监控）

**解决的问题**: AI Agent 性能和质量需要持续监控和改进

**核心组件**:
- Agent Evaluator - 评估准确率和性能
- Agent Monitor - 实时监控和告警
- Human Feedback - 人工反馈收集
- A/B Testing - Prompt 优化测试

**详细文档**: [Agent_Monitor_Design.md](./docs/50-operations/Agent_Monitor_Design.md)

### 5. Gas Manager（Gas 管理）

**解决的问题**: Polygon 链上交易需要优化 Gas 成本和可靠性

**核心组件**:
- Gas Estimator - EIP-1559 Gas 价格估算
- Transaction Batcher - 交易批处理
- Nonce Manager - 线程安全的 nonce 管理
- Transaction Monitor - 交易追踪和自动重试

**详细文档**: [Gas_Manager_Design.md](./docs/30-trading/Gas_Manager_Design.md)

---

## 📊 开发路线图

### Phase 1: Weather MVP（已完成）
- ✅ 项目架构设计
- ✅ 核心模块详细设计
- ✅ 市场发现和规则解析
- ✅ 预测和定价引擎
- ✅ Watch-only 模式
- ✅ UMA watcher replay / settlement verification
- ✅ Operator 只读面（DuckDB + UI replica + runbook）

### Phase 2: Replay / Execution Foundation（已完成）
- ✅ Forecast replay / deterministic recompute
- ✅ Watcher backfill / multi-RPC fallback / continuity
- ✅ Cold-path orchestration
- ✅ Execution foundation
- ✅ Readiness / UI lite / AlphaDesk Exit Gate

### Phase 3: Paper Execution（已完成）
- ✅ Canonical handoff / order router / paper adapter / quote-based fill simulator
- ✅ OMS state machine / reservation / inventory / exposure / reconciliation
- ✅ operator read model / paper run journal / daily ops / review input
- ✅ readiness / closeout / `P4 planning only` entry gates
- closeout 入口：[P3_Closeout_Checklist.md](./docs/10-implementation/checklists/P3_Closeout_Checklist.md)

### Phase 4: Live Prerequisites
- ✅ real data ingress / capability refresh / signer boundary
- ✅ submitter dry-run / shadow path
- ⏳ chain tx scaffold
- ⏳ readiness / controlled rollout criteria
- 当前实施入口：[P4_Implementation_Plan.md](./docs/10-implementation/phase-plans/P4_Implementation_Plan.md)

---

## 🛠️ 技术栈

| 层级 | 技术 | 用途 |
|------|------|------|
| 数据库 | DuckDB | 主数据库 |
| 数据库 | SQLite | Write queue |
| 存储 | Parquet | 原始数据 |
| 区块链 | Web3.py | Polygon 交互 |
| HTTP | HTTPX | 异步 HTTP 客户端 |
| 实时 | WebSocket | 实时行情 |
| 调度 | Dagster（optional extra） | 数据编排 |
| 进程 | Supervisord | 进程管理 |
| UI | Streamlit | Operator UI |
| AI | Anthropic / OpenAI-compatible APIs（经 HTTPX） | Agent provider adapters |

---

## 🔒 安全原则

1. **Agent 隔离** - Agent 永远不接触私钥
2. **签名隔离** - MVP 使用独立 signer 进程 + 环境隔离 + 最小暴露面；生产升级到 KMS / Vault / HSM
3. **签名约束** - UI 不直接调用原始签名接口，Agent 不直接访问私钥
4. **审计日志** - 所有签名请求和敏感操作都记录 `request_id`
5. **权限控制** - Operator UI 身份验证
6. **Event Sourcing** - 所有状态变更可追溯

---

## 📈 关键指标

### 执行质量
- 订单路由决策延迟 < 100ms
- 预估 vs 实际滑点误差 < 10%
- Maker/Taker 比例优化

### 结算监控
- UMA 提案检测延迟 < 1 分钟
- 结算验证准确率 > 99%
- Dispute 决策准确率 > 95%

### 系统性能
- 市场发现延迟 < 5 分钟
- 预测更新频率 1 小时
- 定价计算延迟 < 1 秒

---

## 📞 联系方式

- **项目负责人**: Jay Zhu
- **创建日期**: 2026-03-07
- **文档版本**: v1.0

---

## 📝 版本历史

- **v1.0** (2026-03-08) - 完整项目计划，包含 5 个核心模块详细设计
  - CLOB Order Router - 订单路由和费用优化
  - UMA Watcher - 结算监控和 dispute 分析
  - Forecast Ensemble - 多源预测集成
  - Agent Monitor - AI Agent 监控和持续改进
  - Gas Manager - 区块链交易管理

---

## 📄 许可证

[待定]
