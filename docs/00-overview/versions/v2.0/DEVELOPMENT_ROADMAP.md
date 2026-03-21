# Asterion Development Roadmap

**版本**: v1.5
**更新日期**: 2026-03-21
**目标**: 将已冻结的设计文档转化为可执行开发顺序，既保留 Weather MVP 与 post-P4 remediation 的历史开发顺序，也为当前 `v2.0 implementation active` 提供单一导航入口。

---

## 1. Roadmap 原则

### 1.1 开发顺序原则

按以下顺序推进：

1. 先实现契约和持久化骨架
2. 再实现 watch-only 数据闭环
3. 再实现 replay / backfill / cold path
4. 再实现 paper execution
5. 最后才讨论 live trading

### 1.2 范围边界

当前 roadmap 只覆盖：

- Weather MVP
- Polymarket CLOB + Polygon + UMA
- watch-only / replay / cold path / paper execution
- AlphaDesk -> Asterion 的代码迁移与适配

当前 roadmap 不覆盖：

- Tech pack
- Crypto pack
- 真实资金 live rollout 细节

### 1.3 项目阶段定义

- `P0`: 契约、schema、基础骨架
- `P1`: watch-only 主链路
- `P2`: replay / cold path / monitor
- `P3`: paper execution
- `P4`: live prerequisites

### 1.4 当前状态

- `P0` 已关闭
- `P1` 已关闭
- `P2` 已关闭
- `P3` 已关闭
- `P4` 已 accepted，相关 closeout checklist 和 runbooks 已转为 archived historical records
- `Phase 5` 到 `Phase 8` 已作为 historical post-P4 remediation 的连续收口阶段 accepted；`Phase 9` 已完成 operator wording / docs truth-source cleanup
- `Post-P4 Phase 10` 已完成 boundary hardening v2；`Post-P4 Phase 11` 到 `Post-P4 Phase 15` 也均已 accepted
- deep audit 后续工作已收口为 [Post_P4_Remediation_Implementation_Plan.md](../../../10-implementation/versions/v1.0-remediation/phase-plans/Post_P4_Remediation_Implementation_Plan.md) 中的 historical accepted remediation record
- 当前 historical closeout / planning artifacts 是：
  - [P4_Closeout_Checklist.md](../../../10-implementation/versions/v1.0/checklists/P4_Closeout_Checklist.md)
  - [P4_Controlled_Rollout_Decision_Runbook.md](../../../10-implementation/versions/v1.0/runbooks/P4_Controlled_Rollout_Decision_Runbook.md)
  - [P3_Closeout_Checklist.md](../../../10-implementation/versions/v1.0/checklists/P3_Closeout_Checklist.md)
  - [P3_Paper_Execution_Runbook.md](../../../10-implementation/versions/v1.0/runbooks/P3_Paper_Execution_Runbook.md)
  - [P4_Implementation_Plan.md](../../../10-implementation/versions/v1.0/phase-plans/P4_Implementation_Plan.md)
- 当前 active implementation entry 是：
  - [V2_Implementation_Plan.md](../../../10-implementation/versions/v2.0/phase-plans/V2_Implementation_Plan.md)
- 最近 accepted tranche records 是：
  - [P8_Implementation_Plan.md](../../../10-implementation/versions/v2.0/phase-plans/P8_Implementation_Plan.md)
  - [P7_Implementation_Plan.md](../../../10-implementation/versions/v2.0/phase-plans/P7_Implementation_Plan.md)
  - [P6_Implementation_Plan.md](../../../10-implementation/versions/v2.0/phase-plans/P6_Implementation_Plan.md)
- 当前 `v2.0` 内部阶段状态：
  - `Phase 0` 到 `Phase 8` 已 accepted
  - `no current tranche-specific plan is open`
  - `P9` 作为 follow-on reservation 保留在 umbrella plan / roadmap 中
- 最近 accepted tranche closeout checklist 是：
  - [P8_Closeout_Checklist.md](../../../10-implementation/versions/v2.0/checklists/P8_Closeout_Checklist.md)
- 当前 historical remediation entry 是：
  - [Post_P4_Remediation_Implementation_Plan.md](../../../10-implementation/versions/v1.0-remediation/phase-plans/Post_P4_Remediation_Implementation_Plan.md)
- 当前 `Post-P4 Phase 10` 到 `Post-P4 Phase 15` supporting design docs 保留为 historical accepted supporting designs：
  - [Controlled_Live_Boundary_Design.md](../../../30-trading/Controlled_Live_Boundary_Design.md)
  - [Execution_Economics_Design.md](../../../30-trading/Execution_Economics_Design.md)
  - [Forecast_Calibration_v2_Design.md](../../../40-weather/Forecast_Calibration_v2_Design.md)
  - [Operator_Console_Truth_Source_Design.md](../../../50-operations/Operator_Console_Truth_Source_Design.md)
  - [UI_Read_Model_Design.md](../../../20-architecture/UI_Read_Model_Design.md)
- 当前验证基线：
  - `.venv/bin/python -m unittest discover -s tests -v` 已通过
  - `python -m unittest tests.test_xxx -v` 与 `discover` 都应保持可用
  - system Python 可能缺少 `duckdb`，但仓库内 `.venv` 是 canonical 验证环境
- `P1` 的 canonical closeout 文档见：
  - [P1_Closeout_Checklist.md](../../../10-implementation/versions/v1.0/checklists/P1_Closeout_Checklist.md)
  - [P1_Watch_Only_Replay_Cold_Path_Runbook.md](../../../10-implementation/versions/v1.0/runbooks/P1_Watch_Only_Replay_Cold_Path_Runbook.md)
- `P2` 的 canonical 开工文档见：
  - [P2_Implementation_Plan.md](../../../10-implementation/versions/v1.0/phase-plans/P2_Implementation_Plan.md)
- `P2` 的 canonical closeout 文档见：
  - [P2_Closeout_Checklist.md](../../../10-implementation/versions/v1.0/checklists/P2_Closeout_Checklist.md)
- `P3` 的 canonical 实施文档见：
  - [P3_Implementation_Plan.md](../../../10-implementation/versions/v1.0/phase-plans/P3_Implementation_Plan.md)

### 1.5 AlphaDesk 迁移原则

Asterion 不是从零写的新系统；它的底座来自 AlphaDesk。
因此 roadmap 必须把“迁移并适配 AlphaDesk 代码”当成显式工作流，而不是隐含前提。

迁移规则：

1. 先建立复用清单，再迁代码
2. 优先迁 domain-neutral runtime 与 platform code
3. 所有迁入代码必须做 Asterion 语义适配
4. 不允许把 AlphaDesk 的 crypto-first 业务假设原样带入 Weather MVP
5. 每一批迁移都要补 contract test 与 regression test

迁移分类：

- `直接复用`
  - 数据接入、写队列、writerd、审计、确定性、监控骨架
- `保留框架，重写内容`
  - universe、cost model、fill model、dashboard、Dagster assets
- `禁止迁入`
  - smart money、wallet features、constraint arb、crypto-specific pages/scripts

---

## 2. 总体阶段图

```text
P0 AlphaDesk 复用清单 + 契约落库与工程骨架
-> P1 Weather watch-only 闭环
-> P2 Replay / Backfill / Cold Path
-> P3 Paper Execution
-> P4 Live Prerequisites
-> Post-P4 Remediation (Phase 5 -> Phase 9 accepted)
-> Deep Audit Improvement Roadmap (Post-P4 Phase 10 -> Post-P4 Phase 15 accepted)
-> v2.0 implementation active
-> Phase 0 -> Phase 7 accepted
-> Phase 0 -> Phase 8 accepted
-> reserved follow-on: Phase 9
```

## 2.1 Deep Audit Improvement Roadmap

当前固定顺序：

1. `Post-P4 Phase 10`: Boundary Hardening v2
2. `Post-P4 Phase 11`: Operator Truth-Source and Surface Hardening
3. `Post-P4 Phase 12`: Execution Economics and Ranking v2
4. `Post-P4 Phase 13`: Calibration v2 and Threshold Probability Quality
5. `Post-P4 Phase 14`: Execution Feedback Loop and Cohort Priors
6. `Post-P4 Phase 15`: UI Read-Model and Truth-Source Refactor

这些阶段的详细交付物、持久化预期、测试与 exit criteria，保留以 [Post_P4_Remediation_Implementation_Plan.md](../../../10-implementation/versions/v1.0-remediation/phase-plans/Post_P4_Remediation_Implementation_Plan.md) 为 historical accepted remediation record；当前 `v2.0` umbrella 规划统一看 [V2_Implementation_Plan.md](../../../10-implementation/versions/v2.0/phase-plans/V2_Implementation_Plan.md)，[P8_Implementation_Plan.md](../../../10-implementation/versions/v2.0/phase-plans/P8_Implementation_Plan.md) 保留为最近 accepted tranche record，而 [P7_Implementation_Plan.md](../../../10-implementation/versions/v2.0/phase-plans/P7_Implementation_Plan.md) 与 [P6_Implementation_Plan.md](../../../10-implementation/versions/v2.0/phase-plans/P6_Implementation_Plan.md) 保留为更早 accepted closeout / baseline records。

---

## 3. AlphaDesk Migration Track

**目标**: 把 AlphaDesk 中可复用的代码有计划地迁入 Asterion，并完成最小语义适配。

### 3.1 代码熟悉结果

AlphaDesk 已完成代码级阅读。结论不是“整个 repo 直接复制”，而是把它拆成三类：

- `直接迁入`: 运行约束和基础设施已经足够 domain-neutral
- `保留壳重写`: 调用时序或对象边界值得保留，但内部字段强绑定旧 schema
- `不要迁`: 旧机会模型、旧 crypto-specific 策略、旧 UI 业务页

#### 3.1.1 Platform Foundation

| AlphaDesk 模块 | 代码级判断 | Asterion 动作 |
| --- | --- | --- |
| `alphadesk/bronze.py` | `BronzeJsonlRollingWriter` 是纯 append-only rolling writer，按 UTC minute finalize `.jsonl`，可直接复用 | 直接迁入，保留文件滚动与原子 rename 语义 |
| `alphadesk/clients/data_api.py` | `fetch_all_pages()` 是通用分页抓取壳 | 直接迁入，改 API endpoint/config 命名 |
| `alphadesk/clients/gamma.py` | `scan_gamma_markets()`、`infer_condition_id()`、事件提取逻辑是 Polymarket 侧通用接入 | 直接迁入，字段映射到 Asterion `MarketCapability` / Weather discovery |
| `alphadesk/database.py` | `GuardedConnection`、`connect_duckdb()`、`meta_* watermark/run log`、reader/write guard 很成熟 | 直接迁入，但全部 env 前缀和 schema allow-list 改成 `ASTERION_*` |
| `alphadesk/write_queue.py` | SQLite queue 的 claim/retry/stale-running/archive 已闭合 | 直接迁入，任务类型保留，payload contract 改成 Asterion 表 |
| `alphadesk/os_queue.py` | `enqueue_upsert_rows_v1()` / `enqueue_update_rows_v1()` 是清晰 producer API | 直接迁入 |
| `alphadesk/writerd.py` | 单写者、allow-list、批量 merge、失败回退单条处理可直接复用 | 直接迁入，但 allow-list 改成 Asterion schema/table |
| `alphadesk/write_guard_audit.py` | 写保护拦截审计完整，适合继续保留 | 直接迁入，改路径/env |
| `alphadesk/determinism.py` | canonical JSON + stable hash 是 Asterion 所需基础能力 | 直接迁入 |
| `alphadesk/ws_subscribe.py` | WS 订阅壳可复用，但尚未在 P0 迁入 | 转入 P1，作为 watch-only/blocking input |
| `alphadesk/ws_agg_v3.py` | WS 聚合与 replay 壳可复用，但尚未在 P0 迁入 | 转入 P1，作为 watch-only/blocking input |

#### 3.1.2 Runtime Skeleton

| AlphaDesk 模块 | 代码级判断 | Asterion 动作 |
| --- | --- | --- |
| `alphadesk/strategies/base.py` | `StrategyContext` + `StrategyV3.generate()` 非常干净，几乎不带 crypto 业务假设 | 直接迁入，字段命名轻微调整 |
| `alphadesk/strategy_engine_v3.py` | 快照读取、稳定排序、run_id 生成可复用；`opportunities_v1/v2/v3`、arb hardening、capital engine 强绑定旧域模型 | 保留外层调度壳，重写输出 contract，禁止迁旧机会表 schema |
| `alphadesk/trade_ticket_v1.py` | provenance/hash/decision payload 设计值得保留，但内部字段还是 `asset_id`、`planned_notional_usd`、`recommended_exec_template_id` | 保留 ticket 构造与哈希机制，改成 Asterion canonical order / execution context 语义 |
| `alphadesk/signal_to_order_v1.py` | 依赖 `exec_plan_v3`、旧 `approved_tickets`、旧 `exec_templates`，不能原样迁 | 只保留 dedup key 与 ticket->plan 转换壳，按 Asterion OMS/Router/Signer 重新实现 |
| `alphadesk/execution_gate_v1.py` | `freshness/fillability/economic/portfolio/degrade/lower_bound` 的分层 gate 很值得保留 | 保留结果对象和判定流水线，重写公式与输入字段，对齐 `ExecutionContext + RouteAction + inventory` |
| `alphadesk/portfolio_v3.py` | 顺序 gate / reserve-on-pass 模式可复用，但当前按 event/topic/notional 聚合，不够 Asterion inventory 精度 | 保留 gate snapshot 思路，重写为 `Reservation + InventoryPosition + ExposureSnapshot` |
| `alphadesk/journal_v3.py` | journal id、queue-backed upsert、observed cost 后处理流水线成熟；但 `opp_id/plan_id/size_usd/order_type` 都是旧 contract | 保留 journaling pipeline，重写 schema 到 `orders/fills/reservations` 与 resolution evidence |
| `alphadesk/watch_only_gate_v3.py` | 单一职责很清楚 | 直接迁入，改配置名和触发条件 |

#### 3.1.3 Ops / UI / Orchestration

| AlphaDesk 模块 | 代码级判断 | Asterion 动作 |
| --- | --- | --- |
| `alphadesk/health_monitor_v1.py` | queue / ws / degrade 健康采集已能直接服务 watch-only | 直接迁入，替换 quote/source 字段名 |
| `alphadesk/readiness_checker_v1.py` | milestone 检查框架可复用，但 M1-M5 指标是 AlphaDesk 专用 | 保留 checker/report 壳，重写 readiness gates 到 Asterion phases |
| `alphadesk/ui_db_replica.py` | DuckDB replica copy/validate/meta 机制成熟 | 直接迁入 |
| `alphadesk/ui_lite_db.py` | 增量构建思路可留，但内部 UI contract 强绑定 AlphaDesk gold tables | 保留 lite DB build 壳，重写表选择与输出 contract |
| `dagster_alphadesk/resources.py` | resource 壳很薄，可直接迁 | 直接迁入 |
| `dagster_alphadesk/schedules.py` | schedule 壳很薄，可直接迁 | 直接迁入 |

#### 3.1.4 明确不要迁的代码语义

- `strategy_engine_v3.py` 里的 `opportunities_v1/v2/v3` 表结构和 arb hardening 逻辑
- `signal_to_order_v1.py` 对 `exec_plan_v3` / `exec_template_id` 的旧依赖
- `portfolio_v3.py` 里按 `event/topic` 聚合的旧 exposure 主键
- `journal_v3.py` 里围绕 `size_usd`、旧 `order_type`、旧 `asset_id` 的 schema 语义
- AlphaDesk 的 capital engine、crypto opportunity typing、smart money / wallet feature / crypto arb

### 3.2 迁移波次

#### Wave A: Platform Foundation

优先迁入：

- `alphadesk/clients/*`
- `alphadesk/bronze.py`
- `alphadesk/database.py`
- `alphadesk/db_migrate.py`
- `alphadesk/write_queue.py`
- `alphadesk/writerd.py`
- `alphadesk/write_guard_audit.py`
- `alphadesk/os_queue.py`
- `alphadesk/determinism.py`

迁入目标：

- `asterion_core/clients/*`
- `asterion_core/ingest/*`
- `asterion_core/storage/*`

说明：

- `ws_subscribe.py` / `ws_agg_v3.py` 原先与 Wave A 同列，但实际未在 `P0` 迁入
- 它们现在统一转入 `P1` 剩余迁移清单，作为 watch-only 的 WS 层阻塞项

#### Wave B: Runtime Skeleton

优先迁入：

- `alphadesk/strategies/base.py`
- `alphadesk/strategy_engine_v3.py`
- `alphadesk/trade_ticket_v1.py`
- `alphadesk/signal_to_order_v1.py`
- `alphadesk/execution_gate_v1.py`
- `alphadesk/portfolio_v3.py`
- `alphadesk/journal_v3.py`
- `alphadesk/watch_only_gate_v3.py`

迁入目标：

- `asterion_core/runtime/*`
- `asterion_core/execution/*`
- `asterion_core/risk/*`
- `asterion_core/journal/*`

#### Wave C: Ops / UI / Orchestration

优先迁入：

- `alphadesk/health_monitor_v1.py`
- `alphadesk/readiness_checker_v1.py`
- `alphadesk/ui_db_replica.py`
- `alphadesk/ui_lite_db.py`
- `dagster_alphadesk/*`

迁入目标：

- `asterion_core/monitoring/*`
- `asterion_core/ui/*`
- `dagster_asterion/*`

#### Wave D: Rewrite-on-port

只复用壳，不复用业务参数：

- `alphadesk/universe_v3.py`
- `alphadesk/cost_model_v3.py`
- `alphadesk/fill_ttf_v3.py`
- `alphadesk/pages/dashboard.py`

处理方式：

- 保留接口形状
- 删除 crypto-specific 参数与语义
- 重新对齐 Weather MVP 的 contract

### 3.3 每波迁移必须完成的适配

1. import path 适配
2. config/env name 适配
3. 日志命名适配
4. 表名 / schema 适配
5. contract 对齐到 Asterion 文档
6. 删除 AlphaDesk 特有的 crypto-first 假设

### 3.4 每波迁移必须产出的模块说明

每个迁入模块必须附一份 1 页以内的 module note，至少写清楚：

- 来源文件
- 迁入目标路径
- 直接保留的类 / 函数
- 删除的 AlphaDesk 假设
- 新接入的 Asterion contract
- 回归测试点

### 3.5 明确禁止迁入

- smart money / wallet feature 相关模块
- crypto arb / constraint 策略
- AlphaDesk 的特定仪表盘业务页
- 与旧数据库语义强耦合但无法复用的脚本

### 3.6 验收条件

- 每个迁入模块都有来源映射与适配说明
- 没有“复制进来以后以后再改”的悬空模块
- 迁入代码能通过 Asterion 的 contract tests

---

## 4. P0: AlphaDesk 复用清单 + 契约落库与工程骨架

**目标**: 先建立 AlphaDesk 复用清单和迁移顺序，再把冻结接口落成工程骨架与 schema，不做完整业务。

**预计时间**: 1-2 周

**实施文档**:

- [P0_Implementation_Plan.md](../../../10-implementation/versions/v1.0/phase-plans/P0_Implementation_Plan.md)

### 4.1 工作包

1. 建立 AlphaDesk -> Asterion 迁移清单
   - 标记 `直接复用`
   - 标记 `重写适配`
   - 标记 `禁止迁入`
   - 为每个模块指定目标路径
   - 为每个模块补 1 页 module note

2. 迁移 Wave A 的 storage / ingest / clients 基础模块
   - 保留最小可运行骨架
   - 去掉不必要的 AlphaDesk 业务依赖

3. 建立核心模块目录与包边界
   - `asterion_core/execution`
   - `asterion_core/signer`
   - `domains/weather/forecast`
   - `domains/weather/resolution`
   - `domains/trading/oms`
   - `domains/markets`

4. 定义共享 domain models
   - `RouteAction`
   - `CanonicalOrderContract`
   - `ResolutionSpec`
   - `ForecastRequest`
   - `ForecastResolutionContract`
   - `MarketCapability`
   - `AccountTradingCapability`
   - `ExecutionContext`
   - `Order`
   - `Fill`
   - `Reservation`
   - `InventoryPosition`
   - `ExposureSnapshot`
   - `UMAProposal`
   - `StateTransition`

5. 落库 schema / migration
   - weather specs / forecast runs
   - market capability / account capability / execution context
   - orders / fills / reservations / inventory_positions / exposure_snapshots
   - uma_proposals / proposal_state_transitions / processed_uma_events / block_watermarks / proposal_evidence_links

6. 建立统一 ID / key 规范
   - `request_id`
   - `client_order_id`
   - `reservation_id`
   - `proposal_id`
   - `event_id`
   - cache key

7. 建立基础测试框架
   - model serialization tests
   - schema compatibility tests
   - enum contract tests
   - migrated module contract tests

### 4.2 交付物

- AlphaDesk 迁移清单
- AlphaDesk module notes
- Wave A 基础模块迁入结果
- 可导入的 typed models
- 首版数据库 migration
- 基础 package layout
- 契约级单元测试

### 4.3 验收条件

- AlphaDesk 可复用模块都有目标落点
- 不同模块不再各自定义重复 contract
- schema 与文档字段一一对应
- 所有核心对象都有稳定 ID 和类型定义

---

## 5. P1: Weather Watch-Only 闭环

**目标**: 跑通“市场发现 -> spec -> forecast -> pricing -> watch-only opportunity”主链路，不触发真实交易。

**预计时间**: 2-3 周

**状态**: 已完成并关闭

**关闭文档**:

- [P1_Closeout_Checklist.md](../../../10-implementation/versions/v1.0/checklists/P1_Closeout_Checklist.md)
- [P1_Watch_Only_Replay_Cold_Path_Runbook.md](../../../10-implementation/versions/v1.0/runbooks/P1_Watch_Only_Replay_Cold_Path_Runbook.md)

### 4.1 工作包 A: 市场发现与规则结构化

前置依赖：

- Wave A clients / ingest 已迁入
- Gamma client 已完成 Asterion 适配

1. 实现 Gamma market discovery
2. 识别 Weather 模板市场
3. 持久化 `weather_markets`
4. 实现 Rule2Spec 工作流
5. 输出闭合 `ResolutionSpec`
6. 引入人工审核入口

### 4.2 工作包 B: Station-first onboarding

1. 实现 `StationMapper.resolve_from_spec_inputs()`
2. 建立 station metadata 落库
3. 将 geocode 限制在 onboarding 流程
4. 形成 `station_id + lat/lon + timezone` 的闭合 spec

### 4.3 工作包 C: Forecast pipeline

1. 实现 Open-Meteo adapter
2. 实现 NWS adapter
3. 实现 adapter router / fallback
4. 实现 forecast cache
5. 落库 `weather_forecast_runs`
6. 记录 `source / model_run / forecast_target_time`

### 4.4 工作包 D: Pricing / opportunity

1. 实现 Weather pricing engine
2. 基于 `ResolutionSpec + ForecastDistribution` 计算 fair value
3. 生成 watch-only opportunities
4. 落库 fair values 和 opportunity snapshots

### 4.5 工作包 E: Watch-only operator surface

前置依赖：

- [AlphaDesk_Migration_Ledger.md](../../../10-implementation/versions/v1.0/migration-ledger/AlphaDesk_Migration_Ledger.md) 中 `health_monitor_v1` / `ui_db_replica` 已迁入，或已完成 Asterion 原生替代

1. 固定 canonical operator 只读面
2. 查看市场、spec、forecast、fair value
3. 查看异常 spec / station mapping
4. 查看 source fallback、freshness、watcher 状态和 redeem suggestion

### 4.6 交付物

- watch-only 市场池
- 已审核的 station-first specs
- 持续运行的 forecast / pricing 任务
- operator 只读面最小闭环（DuckDB + UI replica + runbook）

### 4.7 验收条件

- 新 market 能被发现并结构化
- `ResolutionSpec` 不再缺 `station_id`
- forecast request 不依赖 `city`
- 每个 watch-only opportunity 都能追到 spec / forecast run / source
- 每个 settled proposal 都能追到 verification / evidence package / redeem suggestion

---

## 6. P2: Replay / Backfill / Cold Path

**目标**: 让 watch-only 链路可重放、可补采、可回溯，避免开发后期因恢复逻辑返工。

**预计时间**: 2 周

### 5.1 工作包 A: Forecast replay

1. 实现基于 `spec_version + station_id + source + model_run + forecast_target_time` 的 replay
2. 支持重算 forecast distributions
3. 支持重算 fair values
4. 对 replay 输出做 deterministic checks

### 5.2 工作包 B: UMA watcher replay

1. 实现 `last_finalized_block` 持久化
2. 实现 processed event idempotency
3. 实现 restart replay
4. 实现 multi-RPC fallback
5. 实现 evidence package linkage

### 5.3 工作包 C: 冷路径编排

前置依赖：

- Dagster / writerd / determinism 基础能力已从 AlphaDesk 迁入

1. Dagster assets / jobs 骨架
2. spec parse jobs
3. forecast refresh jobs
4. replay / reconciliation jobs
5. daily report jobs

### 5.4 工作包 D: Agent in the loop

1. 在 deterministic Rule2Spec parser 之上实现 `Rule2SpecValidationResult`
2. 在 replay / provenance / fallback 基础上实现 `ReplayQualityValidationResult`
3. 在 watcher replay / settlement verification / evidence linkage 基础上保留 `Resolution Agent`
4. Resolution Agent 输出保持 human-in-the-loop，并通过 operator review closure 生效

### 5.5 交付物

- 可重复执行的 replay jobs
- finalized-block 驱动的 watcher
- cold-path 编排与补采框架
- deterministic rule/spec validation
- deterministic replay/provenance validation
- Resolution Agent-assisted operator review closure

### 5.6 验收条件

- watcher 重启后不会重复改写状态
- replay 后结果能与原始记录对齐
- source fallback / evidence linkage 可被审计
- validation / agent 失败不会阻塞 replay / pricing / watcher 主链路

---

## 7. P3: Resolution / Watcher / Settlement Verification

**目标**: 跑通 Weather 结算监控链路，但仍保持 human-in-the-loop。

**预计时间**: 2-3 周

### 6.1 工作包 A: Settlement Verifier

1. 实现 verifier contract
2. 按 `authoritative_source` 拉取观测值
3. 使用 `fallback_sources` 补证据
4. 应用 `rounding_rule` / `inclusive_bounds`
5. 输出 evidence package

### 6.2 工作包 B: UMA watcher

1. 实现 proposal event ingestion
2. 实现 on-chain proposal projection
3. 正确记录 `old_status -> new_status`
4. 持久化 proposal 和 transitions
5. 触发 verifier linkage

### 6.3 工作包 C: Redeem scheduling

1. 实现 `RedeemScheduleInput`
2. 实现四态 decision 输出
3. 接入 operator review
4. 区分链上 settled 与本地 safe redeem suggestion

### 6.4 交付物

- watch-only UMA monitor
- verifier evidence packages
- redeem recommendation queue
- operator review 面板
- `Daily Review Agent`

### 6.5 验收条件

- 没有 wall clock 决定 proposal 最终状态的路径
- settled proposal 都能追到 evidence package
- human review 可阻断 redeem readiness
- Daily review 已能消费 paper execution / journal / readiness 输出

---

## 8. Historical Note: Execution Foundation（该范围已在 P2 关闭）

这一段 roadmap 原先把 execution foundation 记在更晚的 phase 中，但当前仓库已经在 `P2` 完成以下范围：

- `strategy_engine_v3 / trade_ticket_v1 / signal_to_order_v1 / execution_gate_v1`
- `portfolio_v3 / journal_v3`
- `readiness_checker_v1 / ui_lite_db`
- `P2 closeout + AlphaDesk Exit Gate`

因此：

- 当前 `P3` 不再重复做 execution foundation
- 相关收口依据以 [P2_Closeout_Checklist.md](../../../10-implementation/versions/v1.0/checklists/P2_Closeout_Checklist.md) 和 [P3_Implementation_Plan.md](../../../10-implementation/versions/v1.0/phase-plans/P3_Implementation_Plan.md) 为准

---

## 9. P3: Paper Execution（已关闭）

**目标**: 在不动真资金的前提下，把 execution path 完整走通。

`P3` 的 canonical 实施与 closeout 入口是：

- [P3_Implementation_Plan.md](../../../10-implementation/versions/v1.0/phase-plans/P3_Implementation_Plan.md)

本阶段唯一主题是：

- `paper execution`

本阶段关键边界是：

- 不做真实下单
- 不接真实 signer RPC
- 不接 KMS / HSM
- 不做真实 wallet / real chain side effects
- 继续复用现有 `RouteAction / CanonicalOrderContract / ExecutionContext / Order / Fill / Reservation / ExposureSnapshot`

本阶段关键工作流是：

1. paper execution orchestration
2. router / paper adapter / quote-based fill simulator
3. OMS state machine completion
4. reservation / inventory / exposure / reconciliation closure
5. operator read model / paper run journal / daily ops
6. `P4` 进入条件的 readiness / closeout

本阶段详细任务编号与数据流定义，统一以 `P3` 实施文档为准，不再在 roadmap 中维护第二份展开版。

当前 `P3` closeout / operator 入口见：

- [P3_Closeout_Checklist.md](../../../10-implementation/versions/v1.0/checklists/P3_Closeout_Checklist.md)
- [P3_Paper_Execution_Runbook.md](../../../10-implementation/versions/v1.0/runbooks/P3_Paper_Execution_Runbook.md)

当前结论：

- `P3` paper execution 已完成 closeout
- readiness `GO` 仅表示 `ready for P4 planning only`

---

## 10. P4: Live Prerequisites

**目标**: 在任何真实下单前，完成 live 的最低安全前置条件。

`P4` 当前唯一实施入口是：

- [P4_Implementation_Plan.md](../../../10-implementation/versions/v1.0/phase-plans/P4_Implementation_Plan.md)

`P4` 才进入以下范围：

- real data ingress / capability refresh
- signer isolation
- KMS / Vault / HSM
- submitter dry-run / shadow path
- chain transaction scaffold
- live rollout checklist
- alerting / ops hardening
- real wallet / real chain side effects

进入 `P4` 的前提是：

- `P3` paper execution 已完成 closeout
- operator read model 与 readiness gates 已稳定
- reconciliation 不再存在系统性漂移

---

## 11. 任务拆解建议

### 10.1 开发顺序

建议按以下任务组并行：

1. Data track
   - AlphaDesk clients / ingest / ws migration
   - market scout
   - Rule2Spec
   - station mapper
   - forecast adapters

2. Resolution track
   - settlement verifier
   - watcher replay
   - evidence package

3. Execution foundation track
   - AlphaDesk runtime skeleton migration
   - capability registry
   - router read-only
   - OMS skeleton

4. Platform track
   - AlphaDesk storage / monitor / UI / Dagster migration
   - schema / migrations
   - jobs / Dagster
   - observability
   - UI

### 10.2 每个工作包必须包含

- contract test
- persistence test
- replay / restart test
- operator-visible debug output

---

## 12. 建议的里程碑

### Milestone 1

完成 P0 + P1 前半：

- AlphaDesk Wave A 迁入
- market discovery
- station-first specs
- forecast contract
- schema migrations

### Milestone 2

完成 P1 + P2：

- watch-only opportunity
- replay / cold path
- deterministic forecast rerun

### Milestone 3

完成 P3：

- UMA watcher
- settlement verifier
- redeem recommendation queue

### Milestone 4

完成 P4 + P5：

- AlphaDesk Wave B / C 关键模块迁入并适配
- capability registry
- OMS skeleton
- paper execution

### Milestone 5

完成 accepted `v2.0 Phase 6` baseline：

- deployable-value-first ranking
- allocator self-sorting / invariant hardening
- action queue v2
- truth-source / startup / docs drift cleanup

### Milestone 6

完成当前 `v2.0 Phase 8`：

- calibration hard gates
- scaling-aware capital discipline
- impacted-market operator surfaces

### Milestone 7

完成当前 `v2.0 Phase 8`：

- calibration ops and scaling discipline
- stronger actionability gating around stale / degraded calibration
- scale-oriented operator surfaces

### Milestone 8

保留 `v2.0 Phase 9`：

- delivery / scaling follow-on
- 待 `P8` accepted 后再具体化

---

## 13. 不应提前做的事情

- 不要在没有迁移清单的情况下随手拷贝 AlphaDesk 代码
- 不要把 AlphaDesk 的 crypto-first 假设直接带进 Asterion
- 不要在 P1 前引入真实签名与真实下单
- 不要在 station-first contract 闭合前做 forecast 热路径优化
- 不要在 finalized-block replay 完成前做 UMA 自动动作
- 不要在 paper reconciliation 稳定前讨论 live

---

## 14. 建议的首批实施顺序

如果从下一步立刻开始开发，建议严格按下面 10 个任务开工：

1. 建立 AlphaDesk -> Asterion 迁移清单
2. 迁入 Wave A 的 clients / storage / ws / ingest 基础模块
3. 建立 shared models 与 enums
4. 写首版 migration
5. 实现 Weather market discovery
6. 实现 Rule2Spec -> ResolutionSpec
7. 实现 StationMapper onboarding path
8. 实现 Open-Meteo adapter + ForecastRequest cache key
9. 实现 UMA watcher watermark + processed events
10. 实现 Settlement Verifier evidence package linkage

---

## 15. Roadmap 结论

后续开发应以 `watch-only first` 为总原则。

最先做的不是 execution，而是：

- AlphaDesk 代码迁移与适配
- contract
- persistence
- station-first forecast
- finalized-block watcher
- replay / cold path

只有这些稳定后，execution / signer / OMS / inventory 才值得进入 paper execution 阶段。
