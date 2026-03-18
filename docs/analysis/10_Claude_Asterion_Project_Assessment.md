# Asterion 项目深度评估报告

> Analysis input only.
> Not implementation truth-source.
> Active implementation entry: `docs/10-implementation/versions/v2.0/phase-plans/V2_Implementation_Plan.md`

**状态**: historical assessment snapshot (`2026-03-13`)

**评估日期**: 2026-03-13
**评估模型**: Claude Opus 4.6
**仓库快照**: main branch, commit 2ffb808 (P4-12 closeout)
**评估范围**: 全仓库代码、文档、migration、tests、配置

---

## 执行摘要

Asterion 是一个面向 Polymarket 天气事件市场的研究-定价-执行平台，从 AlphaDesk 底座迁移而来。当前处于 P4 关闭、"controlled live rollout decision boundary" 阶段。

**核心判断**：系统在 watch-only 数据链路和 paper execution 方面已具备相当完整度，但距离"真正能稳定赚钱"仍有显著差距。当前最适合定位为**天气市场信号研究平台**，而非自动化交易系统。

**关键数据**：
- 147 个 Python 文件，43,865 行代码
- 36 个测试文件，14,572 行测试代码（测试代码占比 33%）
- 15 个 SQL migration，722 行 schema 定义
- 5 个阶段（P0-P4）全部声称关闭

---

## A. 项目定位与当前阶段判断

### A.1 当前阶段

项目声称处于 "P4 closed, ready for controlled live rollout decision"。

**代码事实验证**：
- P0-P3 的核心链路代码确实存在且可运行
- P4 的 12 个子任务对应的代码模块均已落地
- 但 P3 和 P4 的 Closeout Checklist 中所有 checkbox 均为 `[ ]`（未勾选），说明 closeout 是文档声明而非经过验证的事实

**判断**：P4 代码已落地，但 closeout 验证未真正执行。文档状态领先于实际验证状态。

### A.2 当前代码真正实现了什么

**已实现且可运行**（基于代码事实）：

1. **天气市场发现**：Gamma API 分页抓取 → 天气市场过滤 → 持久化（`domains/weather/scout/market_discovery.py`, 499 行）
2. **规则解析 (Rule2Spec)**：市场标题正则提取 → 温度区间/来源/舍入规则 → ResolutionSpec（`domains/weather/spec/rule2spec.py`, 505 行）
3. **站点映射**：location → weather station 映射，支持 DB 查询和 catalog 回退（`domains/weather/spec/station_mapper.py`, 262 行）
4. **天气预测**：Open-Meteo + NWS 双源适配器 → 缓存 → 分布构建（`domains/weather/forecast/`, 1,316 行）
5. **定价引擎**：forecast distribution → 区间概率 → fair value → edge 计算 → watch-only 决策（`domains/weather/pricing/engine.py`, 336 行）
6. **UMA 结算监控**：proposal 状态机 → 区块连续性检查 → settlement verification → redeem scheduling（`domains/weather/resolution/`, 1,460 行）
7. **Paper execution 全链路**：strategy engine → trade ticket → execution gate → order router → paper adapter → fill simulator → OMS → reservation → inventory → reconciliation
8. **Signer shell**：EOA/Proxy/Safe 签名上下文 → py-clob-client 集成 → 审计日志（`asterion_core/signer/signer_service_v1.py`, 1,103 行）
9. **Submitter dry-run/shadow**：签名后提交 → dry-run/shadow 模式 → external order/fill observation（`asterion_core/execution/live_submitter_v1.py`, 655 行）
10. **Chain tx scaffold**：approve_usdc 的 gas 估算 → nonce 选择 → 签名 → shadow/controlled-live 广播（`asterion_core/blockchain/chain_tx_v1.py`, 808 行）
11. **Operator Console**：Streamlit 5 页面 UI（Home/Markets/Execution/Agents/System）
12. **AI Agent 系统**：Rule2Spec / DataQA / Resolution 三个 agent，支持 Anthropic/Qwen/Fake 后端
13. **Readiness 系统**：P3/P4 多维度 readiness gate 检查
14. **Cold-path 编排**：Dagster 可选集成，20+ handler 函数

**设计上存在但代码未真正实现**：
- `daily_review_agent.py`：文档多次提及，代码未落地
- 真实 KMS/HSM/Vault 签名后端：当前只有 env var 私钥 + py-clob-client
- 完整产品化 operator UI：当前 Streamlit 是 MVP 级别
- 多领域扩展（Tech pack, Crypto pack）：只有 Weather 域有实现
- 生产级告警体系：readiness checker 存在，但无外部告警集成（PagerDuty/Slack 等）
- WebSocket 实时行情消费：`ws_subscribe.py` 和 `ws_agg_v3.py` 存在但未在主链路中被调用

### A.3 当前边界

**明确的安全边界**（代码级验证）：
- `ChainTxMode` 枚举：`DRY_RUN` / `SHADOW_BROADCAST` / `CONTROLLED_LIVE`，默认 off
- `controlled_live_smoke.json` 限制：只允许 `approve_usdc`，有 wallet allowlist、spender allowlist、amount cap
- Signer 地址校验：签名前验证 `private_key → address` 匹配 `funder`
- 审计日志：所有签名操作写入 `meta.signature_audit_logs`
- Agent 隔离：agent 输出只写 `agent.*` schema，不触碰 `trading.*`

**边界风险**：
- `split` / `merge` / `redeem` 在 `ChainTxKind` 枚举中已定义，但代码中标记为 frozen/未实现
- 私钥通过环境变量传递，无加密存储
- 无 rate limiting 或 circuit breaker 机制

### A.4 是否达到 "ready for controlled live rollout decision"

**结论：部分达到，但有保留。**

达到的条件：
- 代码链路从 market discovery 到 approve_usdc 的 controlled live smoke 已贯通
- 审计日志和 journal 覆盖了关键操作
- Readiness gate 框架存在

未达到的条件：
- Closeout checklist 未实际验证（全部 `[ ]`）
- 无生产环境运行记录
- 无真实市场的 paper execution 回测数据
- 无 external reconciliation 的真实验证
- WebSocket 实时数据链路未接入

---

## B. 系统架构详解

### B.1 总体架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Operator Console (Streamlit)                 │
│  Home │ Markets │ Execution │ Agents │ System                       │
└───────────────────────────┬─────────────────────────────────────────┘
                            │ reads ui.* tables
┌───────────────────────────┴─────────────────────────────────────────┐
│                     UI Read Model Layer                              │
│  ui_lite_db.py → ui_db_replica.py → readiness_checker_v1.py        │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
┌───────────────────────────┴─────────────────────────────────────────┐
│                   Cold-Path Orchestration (Dagster optional)         │
│  handlers.py (20+ jobs) → job_map.py → schedules.py                │
└──┬──────────┬──────────┬──────────┬──────────┬──────────────────────┘
   │          │          │          │          │
   ▼          ▼          ▼          ▼          ▼
┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────────────┐
│Scout │ │ Spec │ │Fcast │ │Price │ │  Resolution  │
│      │ │      │ │      │ │      │ │              │
│market│ │rule2 │ │Open  │ │fair  │ │UMA watcher   │
│disc. │ │spec  │ │Meteo │ │value │ │verification  │
│      │ │      │ │NWS   │ │edge  │ │redeem sched. │
└──┬───┘ └──┬───┘ └──┬───┘ └──┬───┘ └──────┬───────┘
   │        │        │        │             │
   ▼        ▼        ▼        ▼             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Execution Chain (P3 Paper / P4 Live-Prereq)      │
│  strategy_engine → trade_ticket → execution_gate → order_router    │
│  → paper_adapter/live_submitter → OMS → portfolio → journal        │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
┌───────────────────────────┴─────────────────────────────────────────┐
│                    Persistence Layer (DuckDB + SQLite Queue)         │
│  trading.* │ runtime.* │ weather.* │ resolution.* │ agent.* │ meta.*│
└─────────────────────────────────────────────────────────────────────┘
```

### B.2 核心模块划分

| 模块 | 路径 | 行数 | 职责 |
|------|------|------|------|
| Contracts | `asterion_core/contracts/` | 1,547 | 全局数据模型、枚举、ID 生成 |
| Execution | `asterion_core/execution/` | 2,163 | 订单路由、gate、提交、paper adapter |
| Signer | `asterion_core/signer/` | 1,166 | 签名服务、审计日志 |
| Blockchain | `asterion_core/blockchain/` | 1,281 | 钱包状态、链上交易 |
| Runtime | `asterion_core/runtime/` | 418 | 策略引擎、策略基类 |
| Risk | `asterion_core/risk/` | 939 | 预留、库存、对账 |
| Journal | `asterion_core/journal/` | 812 | 事件日志 |
| Monitoring | `asterion_core/monitoring/` | 1,473 | 健康检查、readiness |
| Storage | `asterion_core/storage/` | 1,087 | DuckDB、写队列、writerd |
| UI | `asterion_core/ui/` | 1,868 | 读模型、replica |
| Weather Domain | `domains/weather/` | 4,746 | 市场发现→预测→定价→结算 |
| Agents | `agents/` | ~2,500 | AI agent 系统 |
| Orchestration | `dagster_asterion/` | ~2,000 | 冷路径编排 |

### B.3 Weather 数据链路

```
Gamma API ──→ market_discovery.py ──→ weather.weather_markets
                                          │
                                          ▼
                                     rule2spec.py + station_mapper.py
                                          │
                                          ▼
                                     weather.weather_market_specs
                                          │
                                          ▼
                              ForecastService (OpenMeteo/NWS)
                                          │
                                          ▼
                                     weather.weather_forecast_runs
                                          │
                                          ▼
                              pricing/engine.py (fair value + edge)
                                          │
                                          ▼
                              weather.weather_fair_values
                              weather.weather_watch_only_snapshots
```

**代码验证**：此链路在 `scripts/run_real_weather_chain_smoke.py` 中有端到端实现，可通过 `./start_asterion.sh --data` 触发。

### B.4 Execution / Paper Execution 链路

```
weather.weather_watch_only_snapshots
    │
    ▼
strategy_engine_v3.py → runtime.strategy_runs
    │
    ▼
trade_ticket_v1.py → runtime.trade_tickets
    │
    ▼
signal_to_order_v1.py → capability.execution_contexts
    │
    ▼
execution_gate_v1.py → runtime.gate_decisions
    │
    ▼ (gate pass)
order_router_v1.py → RoutedCanonicalOrder
    │
    ├─ paper_adapter_v1.py → paper_fill_simulator_v1.py (P3)
    │
    └─ signer → submitter → external observation (P4)
    │
    ▼
oms_state_machine_v1.py → trading.orders + trading.order_state_transitions
    │
    ▼
portfolio_v3.py → trading.reservations + trading.inventory_positions
                  + trading.exposure_snapshots + trading.reconciliation_results
    │
    ▼
journal_v3.py → runtime.journal_events
```

**代码验证**：`dagster_asterion/handlers.py` 中的 `run_weather_paper_execution_job()` 实现了完整的 paper execution batch 流程，包含 `PaperExecutionBatchRequest` 的严格参数校验。

### B.5 Live-Prereq 链路

P4 在 paper execution 基础上增加了四个递进模式：

| 模式 | 代码入口 | 真实 side effect |
|------|---------|-----------------|
| `paper_real_data` | `capability_refresh_v1.py` | 读取真实 CLOB 数据，不下单 |
| `sign_only_smoke` | `signer_service_v1.py` | 签名但不提交 |
| `shadow_submit` | `live_submitter_v1.py` | 提交到 shadow 路径 |
| `controlled_live_smoke` | `chain_tx_v1.py` | 仅 approve_usdc，需 8 项前置条件 |

### B.6 UI / Read Model / Readiness 架构

```
DuckDB (canonical) ──copy──→ ui_db_replica ──build──→ ui_lite_db
                                                         │
                                                         ▼
                                                    12 个 ui.* 表
                                                         │
                                                         ▼
                                                  Streamlit app.py
                                                  (5 pages, ~220 行)
```

**12 个 UI 表**（`asterion_core/ui/ui_lite_db.py:22-35`）：
- `ui.market_watch_summary`
- `ui.proposal_resolution_summary`
- `ui.execution_ticket_summary` / `execution_run_summary` / `execution_exception_summary`
- `ui.live_prereq_execution_summary` / `live_prereq_wallet_summary`
- `ui.paper_run_journal_summary`
- `ui.daily_ops_summary` / `daily_review_input`
- `ui.agent_review_summary`
- `ui.phase_readiness_summary`

### B.7 Canonical Persistence 分层

| Schema | 职责 | 关键表 |
|--------|------|--------|
| `trading.*` | Canonical execution ledger | orders, fills, reservations, inventory_positions, exposure_snapshots, reconciliation_results, order_state_transitions |
| `runtime.*` | 运行时/审计层 | strategy_runs, trade_tickets, gate_decisions, journal_events, submit_attempts, external_order_observations, external_fill_observations, chain_tx_attempts, external_balance_observations |
| `meta.*` | 元数据/审计 | ingest_runs, watermarks, domain_events, signature_audit_logs |
| `agent.*` | Agent 输出 | invocations, outputs, reviews, evaluations |
| `weather.*` | 天气领域 | weather_markets, weather_market_specs, weather_forecast_runs, weather_fair_values, weather_watch_only_snapshots, weather_station_map |
| `resolution.*` | 结算监控 | uma_proposals, proposal_state_transitions, processed_uma_events, block_watermarks, settlement_verifications |
| `capability.*` | 能力注册 | market_capabilities, account_capabilities, execution_contexts |
| `ui.*` | 读模型 | 12 个 summary/review 表 |

**分层原则验证**（`AGENTS.md:148-151`）：
- `trading.*` 是 canonical execution ledger — 代码中确实只有 execution chain 写入
- `runtime.*` 是运行时/审计层 — 代码中 journal、submit attempts 等写入此处
- `agent.*` 只保存 review/evaluation — 代码中 agent 输出确实不改写 trading 状态

---

## C. 代码质量评估

### C.1 模块边界清晰度

**评分：8/10 — 优秀**

优点：
- `asterion_core/contracts/` 作为全局数据模型层，所有模块共享同一套 frozen dataclass，无平行 contract
- `trading.*` / `runtime.*` / `agent.*` 的 schema 分层在代码中严格执行
- Weather domain 的 scout → spec → forecast → pricing → resolution 链路边界清晰
- Agent 系统通过 `AgentClient` Protocol 实现后端解耦

不足：
- `dagster_asterion/handlers.py` 是一个 ~1,500 行的巨型文件，承担了所有 job 的实现逻辑，应拆分
- `asterion_core/ui/ui_lite_db.py` 达 1,547 行，SQL 构建和业务逻辑混合
- `asterion_core/signer/signer_service_v1.py` 达 1,103 行，包含了多种签名后端、审计、submit attempt 等多个职责

### C.2 Contract 设计稳健性

**评分：9/10 — 非常好**

优点：
- 所有核心数据模型使用 `@dataclass(frozen=True)` + `__post_init__` 校验
- `CanonicalOrderContract` 强制校验 route_action ↔ time_in_force 一致性（`execution.py:86-95`）
- `ChainTxRequest` 校验 chain_id、nonce、gas 参数合法性
- `SubmitOrderRequest` 强制 `exchange=polymarket_clob`，防止误用
- ID 生成统一通过 `stable_object_id()` 和 `new_request_id()` 等函数

不足：
- `Decimal` 类型在 JSON 序列化时依赖 `safe_json_dumps()`，但部分路径可能遗漏
- 部分 contract 的 `__post_init__` 只做了非空检查，缺少范围校验（如 price 上限）

### C.3 技术债

| 技术债 | 位置 | 严重度 | 说明 |
|--------|------|--------|------|
| handlers.py 过大 | `dagster_asterion/handlers.py` | 中 | ~1,500 行单文件，应按 job 类型拆分 |
| ui_lite_db.py 过大 | `asterion_core/ui/ui_lite_db.py` | 中 | 1,547 行，SQL 和逻辑混合 |
| forecast adapter 过于简单 | `domains/weather/forecast/adapters.py` | 中 | 113 行，只返回单点值而非真实分布 |
| 内存缓存无 TTL | `domains/weather/forecast/cache.py` | 低 | 19 行，`InMemoryForecastCache` 无过期机制 |
| .env.example 不完整 | `.env.example` | 低 | 只有 2 行（ALIBABA_API_KEY, QWEN_MODEL），缺少 DB、RPC、wallet 等配置 |
| WebSocket 模块未集成 | `asterion_core/ws/` | 中 | 代码存在但未在主链路中使用 |
| Closeout checklist 未验证 | `docs/10-implementation/checklists/` | 高 | P3/P4 checklist 全部 `[ ]` |

### C.4 实现质量高的部分

1. **Storage 层**（`asterion_core/storage/`）：
   - `GuardedConnection` 的 reader/writer SQL 校验非常严格（`database.py:75-94`）
   - 禁止 reader 执行 INSERT/UPDATE/DELETE/MERGE 等 30+ 个关键字
   - 单写者模式通过环境变量强制执行
   - Write queue 的 claim/retry/stale-running/archive 机制成熟

2. **Contract 校验**（`asterion_core/contracts/`）：
   - 所有 frozen dataclass 的 `__post_init__` 校验覆盖了关键业务规则
   - `CanonicalOrderContract` 的 route_action ↔ time_in_force 一致性检查是亮点

3. **Signer 安全边界**（`asterion_core/signer/signer_service_v1.py`）：
   - 签名前地址匹配校验
   - 审计日志覆盖 requested/rejected/succeeded 全状态
   - `EnvPrivateKeyTxSigningBackend` 明确禁止 ORDER 签名，只允许 TRANSACTION

4. **Resolution 模块**（`domains/weather/resolution/`）：
   - 区块连续性检查（watermark regression、block gap、duplicate range、invalid range）
   - RPC fallback pool 带优先级和超时
   - 事件幂等性通过 processed_uma_events 保证

5. **Controlled live smoke 安全设计**（`asterion_core/blockchain/chain_tx_v1.py`）：
   - 8 项前置条件检查
   - Wallet allowlist + spender allowlist + amount cap
   - 签名后 payload 清洗（移除 private_key_env_var、raw_transaction 等敏感字段）

### C.5 实现脆弱或复杂度过高的部分

1. **Forecast adapter 返回单点值**（`domains/weather/forecast/adapters.py:40-50`）：
   - OpenMeteo 和 NWS adapter 都只返回 `{temperature: 1.0}` 的退化分布
   - 这意味着 pricing engine 的 `probability_in_bucket()` 实际上只做了 0/1 判断
   - 整个 "概率分布 → fair value" 的设计在当前实现中被退化为确定性判断
   - **这是系统最大的业务逻辑缺陷**

2. **Rule2Spec 正则解析**（`domains/weather/spec/rule2spec.py`）：
   - 依赖正则匹配市场标题，对格式变化敏感
   - 虽然有 AI agent 辅助，但 agent 的 patch 能力有限（只能修改 10 个字段）
   - 解析置信度计算使用硬编码惩罚值（-0.05, -0.15, -0.10）

3. **Paper fill simulator 过于简化**（`asterion_core/execution/paper_fill_simulator_v1.py`）：
   - 118 行，基于 quote 的确定性 fill
   - 不模拟滑点、部分成交、延迟等真实市场行为
   - Paper execution 的回测价值因此大打折扣

### C.6 测试覆盖和验证策略

**测试统计**：
- 36 个测试文件，14,572 行
- 测试代码占总代码的 33%（健康比例）
- 覆盖了从 contract 校验到端到端 smoke 的多个层次

**测试模式**：
- DuckDB integration tests：在测试中创建临时 DB，运行 migration，验证完整链路
- `FakeAgentClient`：可配置响应的 mock agent，避免真实 API 调用
- `patch.dict("os.environ", ...)` 用于隔离环境变量
- Phase closeout tests（`test_p2_closeout.py`, `test_p3_closeout.py`, `test_p4_closeout.py`）

**测试缺口**：
- 无性能测试 / 压力测试
- 无真实 API 集成测试（所有外部调用都被 mock）
- 无并发测试（DuckDB 单写者模式下的竞争条件）
- 无 UI 测试
- forecast adapter 的真实 API 响应解析未被测试

---

## D. 漏洞 / 风险 / 缺陷分析

### D.1 架构风险

| # | 风险 | 位置 | 等级 | 说明 | 修复建议 |
|---|------|------|------|------|---------|
| D1-1 | DuckDB 单写者瓶颈 | `asterion_core/storage/database.py` | 高 | DuckDB 不支持并发写入，所有写操作必须通过 writerd 串行化。生产环境下多个 job 并发写入会导致锁等待 | 短期：保持 writerd 串行；中期：评估 PostgreSQL 迁移 |
| D1-2 | 无高可用设计 | 全局 | 高 | 单进程、单 DB 文件、无 failover。进程崩溃 = 服务中断 | 引入 supervisor 重启策略（start_asterion.sh 已有 nohup，但无健康检查） |
| D1-3 | Dagster 可选依赖 | `dagster_asterion/__init__.py` | 中 | Dagster 是 optional extra，意味着编排层可能不存在。handlers.py 可独立运行，但失去了调度和监控能力 | 明确 Dagster 是否为生产必需 |

### D.2 可靠性风险

| # | 风险 | 位置 | 等级 | 说明 | 修复建议 |
|---|------|------|------|------|---------|
| D2-1 | Forecast 单点值退化 | `domains/weather/forecast/adapters.py` | 严重 | adapter 返回 `{temp: 1.0}` 单点分布，pricing engine 退化为 0/1 判断，无法产生有意义的 edge | 实现真实概率分布（ensemble、历史误差分布、多模型加权） |
| D2-2 | 无重试/断路器 | 全局 HTTP 调用 | 高 | Gamma API、OpenMeteo、NWS 调用无重试逻辑、无 circuit breaker | 引入 tenacity 或类似重试库 |
| D2-3 | 内存缓存无 TTL | `domains/weather/forecast/cache.py` | 中 | `InMemoryForecastCache` 只有 19 行，无过期、无大小限制，长时间运行会内存泄漏 | 加入 TTL 和 maxsize |
| D2-4 | RPC fallback 无持久化 | `domains/weather/resolution/rpc_fallback.py` | 中 | RPC 健康状态只在内存中，重启后丢失 | 持久化 RPC 健康状态 |

### D.3 数据一致性风险

| # | 风险 | 位置 | 等级 | 说明 | 修复建议 |
|---|------|------|------|------|---------|
| D3-1 | Write queue 非事务性 | `asterion_core/storage/write_queue.py` | 高 | SQLite queue → DuckDB 的写入不是原子的。如果 writerd 在批量写入中途崩溃，可能导致部分写入 | 引入 checkpoint/resume 机制 |
| D3-2 | 无 schema 版本锁 | `asterion_core/storage/db_migrate.py` | 中 | migration 无版本锁，并发 migration 可能导致重复执行 | 加入 migration lock table |
| D3-3 | Reconciliation 仅 paper 级别 | `asterion_core/risk/reconciliation_v1.py` | 中 | 当前 reconciliation 主要验证 paper 内部一致性，external reconciliation 虽已实现但未经真实验证 | 在 controlled live 前必须用真实数据验证 |

### D.4 安全风险

| # | 风险 | 位置 | 等级 | 说明 | 修复建议 |
|---|------|------|------|------|---------|
| D4-1 | 私钥通过环境变量传递 | `asterion_core/signer/signer_service_v1.py:358-378` | 严重 | `os.getenv(private_key_env_var)` 直接读取私钥，无加密存储。进程内存 dump 可泄露私钥 | 短期：确保 .env 不入 git（已在 .gitignore）；中期：迁移到 KMS/Vault |
| D4-2 | controlled_live_smoke.json 明文配置 | `config/controlled_live_smoke.json` | 高 | 包含 wallet_id、allowed_spenders、private_key_env_var 名称。虽然不含实际私钥，但暴露了安全架构 | 考虑加密或从 Vault 动态加载 |
| D4-3 | 无 API 认证 | `ui/app.py` | 高 | Streamlit UI 无任何认证机制，任何能访问 8501 端口的人都能看到所有数据 | 加入 Streamlit auth 或反向代理认证 |
| D4-4 | Agent API key 明文 | `.env.example` | 中 | `ALIBABA_API_KEY` 通过环境变量传递，无加密 | 同 D4-1 |
| D4-5 | SQL 注入风险低但存在 | `asterion_core/storage/database.py` | 低 | `GuardedConnection` 的 SQL 校验基于关键字匹配，理论上可被绕过（如通过 CTE 嵌套） | 加入参数化查询强制 |

### D.5 Replay / Idempotency / Side Effect 风险

| # | 风险 | 位置 | 等级 | 说明 | 修复建议 |
|---|------|------|------|------|---------|
| D5-1 | Journal event 无去重 | `asterion_core/journal/journal_v3.py` | 中 | journal event 使用 `stable_object_id` 生成 ID，但 upsert 语义依赖 DuckDB MERGE，重复写入不会报错但可能覆盖 | 加入 event_version 或 idempotency key |
| D5-2 | Chain tx 无 nonce 管理 | `asterion_core/blockchain/chain_tx_v1.py` | 高 | nonce 通过 `eth.get_transaction_count()` 获取，无本地 nonce 管理。并发交易可能 nonce 冲突 | 实现本地 nonce tracker |
| D5-3 | Controlled live smoke 无回滚 | `asterion_core/blockchain/chain_tx_v1.py` | 中 | approve_usdc 一旦广播无法回滚。虽然 amount cap 限制了损失，但无 revoke 机制 | 实现 approve(0) 的 revoke 路径 |

### D.6 Controlled Live 边界安全性

**当前 8 项前置条件**（代码验证）：
1. Readiness report = GO
2. Wallet ready（无 blocker）
3. Environment armed（显式 env flag）
4. Approval token match
5. Wallet in allowlist
6. Spender in allowlist
7. Amount ≤ cap（当前 100 USDC）
8. Private key env var exists

**评估**：前置条件设计合理，但存在以下风险：
- Approval token 是静态字符串匹配，无时间窗口限制
- Amount cap 在 JSON 配置文件中，可被直接修改
- 无 multi-sig 或 multi-party approval 机制
- 无操作频率限制（理论上可连续执行多次 approve）

### D.7 UI / Operator 误导风险

| # | 风险 | 位置 | 等级 | 说明 |
|---|------|------|------|------|
| D7-1 | Readiness 报告可能误导 | `asterion_core/monitoring/readiness_checker_v1.py` | 中 | readiness gate 检查的是表是否存在且有数据，不检查数据质量或时效性 |
| D7-2 | UI 数据可能过时 | `asterion_core/ui/ui_db_replica.py` | 中 | UI 读的是 replica，replica 刷新频率取决于 cron/手动触发 |
| D7-3 | Phase 4 状态显示 | `ui/app.py` | 低 | UI 硬编码显示 "P4 closed"，不从 readiness 动态读取 |

### D.8 文档与代码漂移 (Documentation Drift)

| # | 漂移点 | 说明 |
|---|--------|------|
| DD-1 | P3/P4 Closeout Checklist 全部 `[ ]` | 文档声称 P3/P4 已关闭，但 checklist 未勾选。这是最严重的漂移 |
| DD-2 | README.md Phase 4 状态不一致 | README 第 371-372 行显示 `⏳ chain tx scaffold` 和 `⏳ readiness / controlled rollout criteria`，但 P4 plan 声称已完成 |
| DD-3 | daily_review_agent 多处提及但未实现 | AGENTS.md:106、README:269 提及，但 `agents/weather/` 下无此文件 |
| DD-4 | .env.example 严重不完整 | 只有 2 行，缺少 ASTERION_DB_PATH、RPC URL、wallet 配置等关键变量 |
| DD-5 | Roadmap milestone 编号混乱 | DEVELOPMENT_ROADMAP.md 中 section 编号不连续（4.1→5.1→6.1），milestone 定义与实际 phase 不对应 |
| DD-6 | Gas Manager 设计文档存在但无代码 | `docs/30-trading/Gas_Manager_Design.md` 存在，但 `chain_tx_v1.py` 的 gas 管理是内联实现，非独立模块 |

---

## E. "赚钱能力"与业务可行性分析

### E.1 当前系统具备的可变现能力

基于代码事实（非文档声明）：

| 能力 | 成熟度 | 变现潜力 | 依据 |
|------|--------|---------|------|
| 天气市场自动发现 | 高 | 低（信息优势有限） | `market_discovery.py` 可批量抓取 Gamma 天气市场 |
| 规则结构化解析 | 中高 | 中（减少人工分析时间） | `rule2spec.py` + AI agent 辅助 |
| 天气预测 → 定价 | **低** | **极低（当前实现）** | adapter 返回单点值，fair value 退化为 0/1 |
| Watch-only 信号 | 中 | 中（作为研究工具） | edge 计算存在，但基于退化的 fair value |
| Paper execution | 中 | 低（回测价值有限） | fill simulator 过于简化 |
| UMA 结算监控 | 中高 | 中（避免 dispute 损失） | 完整的 proposal 状态机和 verification |
| Controlled live (approve_usdc) | 低 | 极低（只能 approve，不能交易） | 仅 approve_usdc 一个操作 |

### E.2 当前最适合的商业模式

**推荐排序**（基于代码现实）：

1. **天气市场信号研究平台**（最现实）
   - 代码基础：market discovery + rule2spec + forecast + pricing 链路完整
   - 缺口：forecast 需要真实概率分布
   - 变现方式：为 Polymarket 天气市场参与者提供信号/分析
   - 时间：修复 forecast adapter 后 2-4 周可用

2. **Market intelligence / Weather intelligence**
   - 代码基础：批量市场发现 + 结构化解析 + UMA 监控
   - 变现方式：天气市场数据 API / 结算监控服务
   - 时间：1-2 月

3. **Operator console / Infrastructure**
   - 代码基础：Streamlit UI + readiness + journal
   - 变现方式：为其他 Polymarket 参与者提供运营工具
   - 时间：3-6 月（需要产品化）

4. **自营交易**（最远）
   - 代码基础：paper execution 链路存在
   - 缺口：forecast 分布、真实 fill 模拟、live execution、风控
   - 时间：6-12 月

### E.3 Polymarket 场景最现实的策略路径

**当前最现实的策略**：基于天气预测的 edge 发现 + 手动执行

具体路径：
1. 修复 forecast adapter，引入真实概率分布（ensemble 或历史误差分布）
2. 用 watch-only 链路批量扫描天气市场，发现 edge > 300bps 的机会
3. 人工审核 agent 输出和 fair value
4. 手动在 Polymarket 下单

**为什么不是自动执行**：
- Paper fill simulator 不模拟真实市场行为，回测无参考价值
- 天气市场流动性通常较低，大单会显著影响价格
- 当前无滑点模型、无 maker/taker 策略优化
- 天气市场的 edge 来源是预测准确性，不是执行速度

### E.4 风险收益比分析

| 方向 | 投入 | 预期收益 | 风险 | 推荐 |
|------|------|---------|------|------|
| 修复 forecast 分布 | 1-2 周 | 解锁信号研究能力 | 低 | **最优先** |
| 真实数据回测 | 2-3 周 | 验证策略可行性 | 低 | 高优先 |
| Live execution | 2-3 月 | 自动化交易 | 高（资金风险） | 延后 |
| 多领域扩展 | 3-6 月 | 扩大市场覆盖 | 中（分散精力） | 延后 |

### E.5 离"真正能稳定赚钱"还差什么

**关键缺口**（按重要性排序）：

1. **真实概率分布**：当前 forecast adapter 返回单点值，无法计算有意义的 edge。这是最大的阻塞项。
2. **历史回测框架**：需要用历史天气数据验证策略的 Sharpe ratio、最大回撤、胜率
3. **流动性评估**：天气市场的 orderbook 深度通常很浅，需要评估可执行的仓位大小
4. **真实 fill 模拟**：当前 paper fill 是确定性的，不反映真实市场的滑点和部分成交
5. **风控框架**：无仓位限制、无单日亏损限制、无 drawdown 熔断
6. **Live execution 可靠性**：signer → submitter → chain tx 链路需要生产级可靠性
7. **监控和告警**：需要实时监控 forecast 准确性、execution 质量、P&L

### E.6 已足够支持早期验证的能力

- 市场发现和结构化解析：可以批量发现和分析天气市场
- Watch-only 信号生成：修复 forecast 后可产生有意义的信号
- UMA 结算监控：可以监控 proposal 状态，避免 dispute 损失
- Agent 辅助分析：Rule2Spec agent 可以辅助人工审核
- Operator Console：可以作为日常运营的仪表盘

### E.7 明显缺口

- **无真实概率分布**（最关键）
- **无历史回测**
- **无流动性评估**
- **无 P&L 追踪**
- **无风控框架**
- **无实时数据**（WebSocket 未集成）

---

## F. 策略建议

### F.1 短期建议（1-4 周）

1. **修复 forecast adapter，实现真实概率分布**
   - 文件：`domains/weather/forecast/adapters.py`
   - 方案：基于历史预测误差构建正态/经验分布，而非单点值
   - 优先级：P0（阻塞所有信号研究能力）

2. **验证 P3/P4 Closeout Checklist**
   - 文件：`docs/10-implementation/versions/v1.0/checklists/P3_Closeout_Checklist.md`, `P4_Closeout_Checklist.md`
   - 方案：逐项运行验证，勾选或标注未通过项
   - 优先级：P0（消除文档漂移）

3. **补全 .env.example**
   - 文件：`.env.example`
   - 方案：列出所有 `ASTERION_*` 环境变量及说明
   - 优先级：P1

4. **修复 README.md 中 Phase 4 状态**
   - 文件：`README.md:371-372`
   - 方案：将 `⏳` 改为 `✅`，与 P4 plan 一致
   - 优先级：P1

### F.2 中期建议（1-3 月）

1. **构建历史回测框架**
   - 用历史天气数据 + 历史市场价格验证策略
   - 计算 Sharpe ratio、最大回撤、胜率
   - 这是决定是否进入 live 的关键依据

2. **实现 WebSocket 实时数据链路**
   - `ws_subscribe.py` 和 `ws_agg_v3.py` 已存在，需要集成到主链路
   - 实时 quote 数据对 edge 计算和执行时机至关重要

3. **拆分 handlers.py**
   - 将 `dagster_asterion/handlers.py` 按 job 类型拆分为多个文件
   - 降低维护复杂度

4. **实现基础风控**
   - 仓位限制（per market, per wallet）
   - 单日亏损限制
   - Drawdown 熔断

5. **UI 认证**
   - 为 Streamlit 加入基础认证
   - 或部署在 VPN/内网后

### F.3 长期建议（3-12 月）

1. **数据库迁移评估**
   - DuckDB 适合分析，但不适合高并发写入
   - 评估 PostgreSQL 作为 canonical store，DuckDB 作为分析层

2. **KMS/Vault 集成**
   - 替换环境变量私钥
   - 实现 multi-sig 或 multi-party approval

3. **多领域扩展**
   - Tech pack / Crypto pack 的 domain 实现
   - 复用 `asterion_core` 的 execution/risk/journal 基础设施

4. **生产级监控**
   - Prometheus metrics export
   - Grafana dashboard
   - PagerDuty/Slack 告警集成

### F.4 技术优先级

```
P0: forecast 概率分布修复 → 解锁信号研究
P1: closeout 验证 + 文档修复 → 消除漂移
P2: 历史回测框架 → 验证策略可行性
P3: WebSocket 集成 + 风控 → 支持 live 前置
P4: DB 迁移评估 + KMS → 生产化
```

### F.5 产品优先级

```
P0: 天气信号研究工具（修复 forecast 后即可用）
P1: 结算监控服务（UMA watcher 已较完整）
P2: 手动辅助交易（watch-only + 人工执行）
P3: 半自动交易（paper execution + 人工审批）
P4: 全自动交易（需要完整风控和 live execution）
```

### F.6 风险控制建议

1. **不要在 forecast 修复前进入 live**：当前 fair value 是退化的 0/1 判断，基于此交易等于盲目下注
2. **不要在回测验证前投入真实资金**：paper execution 的 fill simulator 过于简化，不能作为策略验证依据
3. **controlled live 的 amount cap 保持在 100 USDC 以下**：直到有足够的 live 运行数据
4. **优先监控 forecast 准确性**：这是整个系统的 alpha 来源
5. **保持 human-in-the-loop**：至少在前 3 个月

### F.7 商业化建议

**最短路径**：天气市场信号研究 → 手动交易验证 → 信号服务

1. 修复 forecast（1-2 周）
2. 用 watch-only 链路发现 edge（持续）
3. 手动在 Polymarket 执行几笔交易验证 edge 质量（2-4 周）
4. 如果 edge 可验证，考虑：
   - 自营小规模交易（$1K-$10K）
   - 或将信号能力包装为服务

---

## G. 下一阶段建议

如果我是这个项目的技术负责人，接下来最应该做的 10 件事：

### 1. 修复 forecast adapter 的概率分布（最高优先）
**为什么**：这是整个系统的 alpha 来源。当前单点值退化使得 pricing engine 的 fair value 计算毫无意义。没有真实概率分布，所有下游的 edge 计算、策略决策、paper execution 都是空中楼阁。
**文件**：`domains/weather/forecast/adapters.py`
**工作量**：1-2 周

### 2. 验证并勾选 P3/P4 Closeout Checklist
**为什么**：文档声称 P3/P4 已关闭，但 checklist 全部 `[ ]`。这不仅是文档问题，更是项目管理的信任问题。如果 closeout 未真正验证，后续所有基于"P4 已关闭"的决策都缺乏基础。
**文件**：`docs/10-implementation/versions/v1.0/checklists/P3_Closeout_Checklist.md`, `P4_Closeout_Checklist.md`
**工作量**：2-3 天

### 3. 用真实历史数据做一次完整回测
**为什么**：在投入任何真实资金前，必须用历史数据验证策略的预期收益和风险。当前系统有 replay 能力（`domains/weather/forecast/replay.py`），但缺少系统性的回测框架。
**工作量**：2-3 周

### 4. 补全 .env.example 和部署文档
**为什么**：当前 `.env.example` 只有 2 行，新开发者或新环境无法正确配置系统。这是基本的工程卫生。
**工作量**：1 天

### 5. 修复 README.md 中的状态漂移
**为什么**：README 是项目的门面，Phase 4 状态显示 `⏳` 与实际不符。同时 daily_review_agent 被多处提及但未实现，应明确标注。
**工作量**：半天

### 6. 实现 WebSocket 实时数据集成
**为什么**：`ws_subscribe.py` 和 `ws_agg_v3.py` 已存在但未集成。实时 quote 数据对 edge 计算的时效性至关重要，尤其是天气市场在事件临近时价格变化快。
**工作量**：1-2 周

### 7. 实现基础风控框架
**为什么**：当前系统无仓位限制、无亏损限制、无熔断机制。即使是 paper execution，也应该有风控约束来验证策略的风险特征。
**工作量**：1-2 周

### 8. 拆分 handlers.py 和 ui_lite_db.py
**为什么**：这两个文件分别达 1,500+ 行，是维护和测试的瓶颈。拆分后可以独立测试和部署各个 job。
**工作量**：1 周

### 9. 为 Streamlit UI 加入认证
**为什么**：当前 UI 无任何认证，暴露了所有运营数据。即使是内部工具，也应有基本的访问控制。
**工作量**：1-2 天

### 10. 评估 DuckDB → PostgreSQL 迁移路径
**为什么**：DuckDB 的单写者限制在生产环境下会成为瓶颈。不需要立即迁移，但应该评估迁移的工作量和影响，为后续决策做准备。
**工作量**：评估 1 周，迁移 2-4 周

---

## 附录：评估方法论

### 实际检查的代码和文档

**文档**（完整阅读）：
- `README.md`, `AGENTS.md`
- `docs/00-overview/` 下全部 3 个文档
- `docs/10-implementation/Implementation_Index.md`
- `docs/10-implementation/versions/v1.0/phase-plans/P3_Implementation_Plan.md`, `P4_Implementation_Plan.md`
- `docs/10-implementation/versions/v1.0/checklists/P3_Closeout_Checklist.md`, `P4_Closeout_Checklist.md`
- `docs/10-implementation/runbooks/` 下全部 3 个 P4 runbook

**代码**（完整阅读或结构分析）：
- `asterion_core/` 下全部 57 个 Python 文件
- `domains/weather/` 下全部 22 个 Python 文件
- `agents/` 下全部 Python 文件
- `dagster_asterion/` 下全部 6 个模块
- `ui/app.py` 及页面文件
- `scripts/` 下全部脚本
- `sql/migrations/` 下全部 15 个 migration 文件
- `config/` 下全部配置文件
- `tests/` 下全部 36 个测试文件（结构分析）
- `.env.example`, `.gitignore`, `pyproject.toml`, `start_asterion.sh`

### 判断依据说明

| 判断类型 | 标记 |
|---------|------|
| 基于代码事实 | 直接引用文件路径和行号 |
| 基于文档声明 | 标注"文档声称"或"声称" |
| 基于推断 | 标注"推断"或"评估" |
| 基于行业经验 | 标注"建议"或"推荐" |

**本报告中的关键推断**：
- "forecast adapter 返回单点值是系统最大的业务逻辑缺陷" — 基于代码事实（`adapters.py` 返回 `{temp: 1.0}`）+ 行业经验（概率交易需要概率分布）
- "当前最适合定位为天气市场信号研究平台" — 基于代码能力评估 + 商业可行性推断
- "P3/P4 closeout 未真正验证" — 基于代码事实（checklist 全部 `[ ]`）
- "DuckDB 单写者会成为生产瓶颈" — 基于架构分析 + 行业经验
