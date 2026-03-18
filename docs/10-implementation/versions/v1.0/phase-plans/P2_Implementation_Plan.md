# Asterion P2 Implementation Plan

**版本**: v1.0
**更新日期**: 2026-03-09
**阶段**: `P2`
**状态**: 开工中
**目标**: 在 `P1` 已闭合的 Weather `watch-only / replay / cold path` 基础上，继续收口 forecast replay、watcher backfill、cold-path orchestration 与 execution foundation，并开始把 Asterion 从“可独立开发”推进到“可脱离 AlphaDesk 参考仓库”。

---

## 1. P2 定位

`P2` 是从 `watch-only 已闭合` 进入 `可重放、可补采、可调度、可进入 paper foundation` 的阶段。

本阶段只做六类事情：

1. 建立 deterministic forecast replay 与 fair value replay
2. 建立 UMA watcher backfill / restart replay / multi-RPC fallback
3. 建立 cold-path orchestration 的最小编排壳
4. 迁入 execution foundation 的 AlphaDesk 剩余模块，并按 Asterion contract 重写
5. 在 deterministic 底座之上接入首批 agent-in-the-loop 能力
6. 收口 `P2` 时 AlphaDesk Exit Gate 的剩余范围

`P2` 完成后，项目应达到：

- forecast / pricing 可以按固定 key 做 deterministic recompute
- watcher 可以从 finalized watermark 继续 backfill，不依赖单 RPC 或 wall clock 猜状态
- cold path 不再只是“模块函数集合”，而是有统一编排入口
- execution foundation 已有 Asterion 自有 runtime/ticket/gate/risk/journal 主干
- `Rule2Spec Agent`、`Data QA Agent`、`Resolution Agent` 都已有明确落点，但仍保持在执行路径之外
- 后续进入 `P3` paper execution 时，不再需要一边开发一边回看 AlphaDesk 的执行层源码

---

## 2. P2 Source Of Truth

本阶段实施以以下文档为准：

- [Asterion_Project_Plan.md](../../../../00-overview/Asterion_Project_Plan.md)
- [DEVELOPMENT_ROADMAP.md](../../../../00-overview/DEVELOPMENT_ROADMAP.md)
- [Documentation_Index.md](../../../../00-overview/Documentation_Index.md)
- [Implementation_Index.md](../../../Implementation_Index.md)
- [P1_Closeout_Checklist.md](../checklists/P1_Closeout_Checklist.md)
- [P1_Watch_Only_Replay_Cold_Path_Runbook.md](../runbooks/P1_Watch_Only_Replay_Cold_Path_Runbook.md)
- [AlphaDesk_Migration_Ledger.md](../migration-ledger/AlphaDesk_Migration_Ledger.md)
- [AlphaDesk_Migration_Ledger.md](../migration-ledger/AlphaDesk_Migration_Ledger.md)
- [Forecast_Ensemble_Design.md](../../../../40-weather/Forecast_Ensemble_Design.md)
- [UMA_Watcher_Design.md](../../../../40-weather/UMA_Watcher_Design.md)
- [OMS_Design.md](../../../../30-trading/OMS_Design.md)
- [CLOB_Order_Router_Design.md](../../../../30-trading/CLOB_Order_Router_Design.md)
- [Market_Capability_Registry_Design.md](../../../../30-trading/Market_Capability_Registry_Design.md)
- [Signer_Service_Design.md](../../../../30-trading/Signer_Service_Design.md)
- [Hot_Cold_Path_Architecture.md](../../../../20-architecture/Hot_Cold_Path_Architecture.md)

如果实施中发现冲突，优先处理顺序：

1. 对应 subsystem 设计文档
2. `Asterion_Project_Plan.md`
3. `AlphaDesk_Migration_Ledger.md`
4. 本实施文档

---

## 3. P2 退出条件

`P2` 完成必须同时满足：

1. forecast replay 已按 `market_id / station_id / spec_version / source / model_run / forecast_target_time` 闭合
2. replay 后的 `forecast_runs / fair_values / watch_only_snapshots` 能被 deterministic 对账
3. watcher 已支持 block-range backfill、restart replay、multi-RPC fallback、evidence linkage continuity
4. cold-path orchestration 已有最小可运行入口，至少能编排 spec / forecast / replay / reconciliation
5. `strategy_engine_v3`、`trade_ticket_v1`、`signal_to_order_v1`、`execution_gate_v1`、`portfolio_v3`、`journal_v3` 已迁入或已有 Asterion 原生替代
6. `readiness_checker_v1`、`ui_lite_db`、`dagster_asterion/resources.py`、`dagster_asterion/schedules.py` 已迁入或已有 Asterion 原生替代
7. `P2` 的实现不再要求继续通过阅读 AlphaDesk 执行层源码来完成 paper foundation 开发

---

## 4. P2 不做什么

`P2` 不做以下事情：

- 不做真实资金 live
- 不做自动 dispute
- 不做自动 redeem
- 不做 production KMS / Vault / HSM
- 不把 `watch-only` 输出直接接到真实下单
- 不在 `P2` 强行完成完整 operator 产品化 UI

---

## 5. P2 关键代码落点

`P2` 结束时，代码树至少应落到下面这个层次：

```text
asterion_core/
  runtime/
    strategy_base.py
    strategy_engine_v3.py
    trade_ticket_v1.py
    signal_to_order_v1.py
  execution/
    watch_only_gate_v3.py
    execution_gate_v1.py
  risk/
    portfolio_v3.py
  journal/
    journal_v3.py
  monitoring/
    health_monitor_v1.py
    readiness_checker_v1.py
  ui/
    ui_db_replica.py
    ui_lite_db.py

dagster_asterion/
  resources.py
  schedules.py

domains/weather/
  forecast/
    replay.py
  resolution/
    backfill.py
```

说明：

- `P2` 的关键不是一次性把 paper execution 做完，而是把 replay / orchestration / execution foundation 先收成可持续开发的底座
- `P3` 才进入 paper execution 和 execution path 的连续集成

---

## 6. P2 五个工作流

### 6.0 Agent 实施边界

Weather MVP 的 4 个 agent 不应同时上，也不应在 deterministic 底座未闭合前提前实施。

`P2` 的 agent 归属固定如下：

- `Rule2Spec Agent`: `P2` 前半实施
- `Data QA Agent`: `P2` 中后段实施
- `Resolution Agent`: `P2` 中后段实施
- `Daily Review Agent`: 不在 `P2` 实施，推迟到 `P3`

原因：

- `Rule2Spec Agent` 依赖已经闭合的 `weather_markets -> Rule2Spec -> StationMapper -> ResolutionSpec`
- `Data QA Agent` 必须建立在 replay、forecast persistence、pricing provenance 和 watcher backfill 稳定之后
- `Resolution Agent` 必须建立在 watcher replay、settlement verification、evidence linkage 和 redeem suggestion 已闭合之后
- `Daily Review Agent` 需要 `paper execution + journal + readiness + daily ops` 输入，过早放到 `P2` 只会变成摘要器

### 6.1 工作流 A: Forecast Replay

目标：

- 让 `ResolutionSpec -> ForecastRequest -> ForecastDistribution -> ForecastRunRecord -> FairValue` 链路可被 deterministic 重算

必须产出：

- `domains/weather/forecast/replay.py`
- replay input loader
- replay recompute pipeline
- replay persistence / reconciliation
- deterministic replay tests

要求：

- replay key 只依赖 canonical contract 字段
- replay 不允许重新引入 `city-first`
- replay 结果必须能与原始 `weather_forecast_runs` 对账

### 6.2 工作流 B: Watcher Backfill

目标：

- 让 watcher 从“增量 replay”进入“可重启、可补采、可多 RPC 兜底”

必须产出：

- `domains/weather/resolution/backfill.py`
- block-range backfill runner
- multi-RPC fallback policy
- replay continuity checks
- backfill / restart tests

要求：

- proposal 权威状态仍只来自链上事件、链上读取和 finalized watermark
- wall clock 只能用于调度，不参与最终状态推导
- backfill 后不能重复生成 transition 或覆盖已确认状态

### 6.3 工作流 C: Cold-Path Orchestration

目标：

- 把当前散落的模块入口收口成可调度的 cold-path 编排壳

必须产出：

- `dagster_asterion/resources.py`
- `dagster_asterion/schedules.py`
- spec parse / forecast refresh / replay / reconciliation jobs 的最小壳
- orchestration runbook 或 job map

要求：

- 不把调度语义写死到业务模块
- 编排层只负责触发与顺序，不重新定义业务 contract
- Dagster 只是当前推荐壳，不应阻塞业务 contract 收口

### 6.4 工作流 D: Execution Foundation

目标：

- 把 paper execution 前必须存在的 runtime / ticket / gate / risk / journal 主干迁入 Asterion

必须产出：

- `asterion_core/runtime/strategy_engine_v3.py`
- `asterion_core/execution/trade_ticket_v1.py`
- `asterion_core/execution/signal_to_order_v1.py`
- `asterion_core/execution/execution_gate_v1.py`
- `asterion_core/risk/portfolio_v3.py`
- `asterion_core/journal/journal_v3.py`

要求：

- 全部对齐 `ExecutionContext + CanonicalOrderContract + Reservation + InventoryPosition + ExposureSnapshot`
- 禁止把 AlphaDesk 的 `opportunities_v*`、`exec_plan_v3`、`asset_id`、`size_usd`、按 `event/topic` 聚合的 exposure 主键带回来
- execution foundation 在 `P2` 只需能支撑 paper foundation，不进入真实下单

### 6.5 工作流 E: Agent-In-The-Loop

目标：

- 在不进入执行路径的前提下，把首批 3 个 Weather agent 放到已经闭合的 deterministic 链路上

必须产出：

- `agents/weather/rule2spec_agent.py`
- `agents/weather/data_qa_agent.py`
- `agents/weather/resolution_agent.py`
- 对应 invocation / evaluation / review hooks

要求：

- agent 只输出结构化建议，不直接改写 canonical tables
- 所有 agent 输出都需要人工审核或规则验证
- agent 失败不能阻塞 replay、forecast、watcher、pricing 主链路

### 6.6 工作流 F: Ops / Exit Gate

目标：

- 为 `P3` paper execution 和独立仓库准备剩余的 operator / readiness / read model 基础

必须产出：

- `asterion_core/monitoring/readiness_checker_v1.py`
- `asterion_core/ui/ui_lite_db.py`
- 更新后的 AlphaDesk migration ledger
- `P2` closeout checklist

要求：

- readiness gates 以 Asterion phase 定义为准，不复用 AlphaDesk 的 milestone 指标
- ui_lite_db 输出 contract 必须围绕 Asterion 的 canonical tables
- `P2` 结束时要能明确是否满足 AlphaDesk Exit Gate

---

## 7. P2 任务拆解

### P2-01 固定 Forecast Replay 输入契约

输出：

- replay input schema
- `forecast_target_time`、`model_run`、`source`、`spec_version` 的唯一解释

完成条件：

- replay key 无歧义
- 与 `Forecast_Ensemble_Design.md` 保持一致

### P2-02 实现 forecast replay 主干

输出：

- `domains/weather/forecast/replay.py`
- replay loader / recompute / persist

完成条件：

- 可从已存 `weather_market_specs` 重新构造 forecast runs

### P2-03 实现 replay 对账

输出：

- replay vs original diff
- deterministic replay tests

完成条件：

- replay 差异可观测
- 不因 dict 顺序或时间字段漂移产生假差异

### P2-04 实现 watcher block-range backfill

输出：

- `domains/weather/resolution/backfill.py`
- backfill runner

完成条件：

- 可指定 block range 做 event 回放

### P2-05 实现 watcher multi-RPC fallback

输出：

- RPC source policy
- fallback / retry contract

完成条件：

- 单 RPC 故障不阻塞 finalized watermark 驱动

### P2-06 实现 replay continuity checks

输出：

- continuity / gap checks
- restart replay validation

完成条件：

- 重启后无重复 transition
- gap 可被检测和记录

### P2-07 迁入 Dagster resources

输出：

- `dagster_asterion/resources.py`

固定资源：

- `AsterionColdPathSettings`
- `DuckDBResource`
- `WriteQueueResource`
- `ForecastRuntimeResource`
- `WatcherRpcPoolResource`

完成条件：

- cold-path 编排能持有最小资源层
- 未安装 Dagster 时仍可导入 runtime resources
- 安装 Dagster 时可构造 resource wrapper

### P2-08 迁入 Dagster schedules

输出：

- `dagster_asterion/schedules.py`
- `dagster_asterion/__init__.py`

完成条件：

- 至少能表达 forecast refresh / replay / reconciliation 的调度壳
- `weather_forecast_replay` 默认 manual
- Dagster 仅作为可选依赖

### P2-09 构建 cold-path job map

输出：

- `dagster_asterion/job_map.py`
- `docs/10-implementation/versions/v1.0/runbooks/P2_Cold_Path_Orchestration_Job_Map_Runbook.md`

固定 jobs：

- `weather_spec_sync`
- `weather_forecast_refresh`
- `weather_forecast_replay`
- `weather_watcher_backfill`
- `weather_resolution_reconciliation`

完成条件：

- `P2` runbook 可明确每个 job 的输入输出表
- `job_map.py` 是唯一 source of truth

### P2-10 迁入 strategy_engine_v3

输出：

- `asterion_core/runtime/strategy_engine_v3.py`
- module note

完成条件：

- 稳定排序、run_id、调度壳落地
- 不引入旧 `opportunities_v*`

### P2-11 迁入 trade_ticket_v1

输出：

- `asterion_core/execution/trade_ticket_v1.py`
- module note

完成条件：

- ticket provenance / hash / request_id 闭合

### P2-12 迁入 signal_to_order_v1

输出：

- `asterion_core/execution/signal_to_order_v1.py`
- module note

完成条件：

- ticket -> canonical order handoff 闭合
- 不依赖 `exec_plan_v3`

### P2-13 迁入 execution_gate_v1

输出：

- `asterion_core/execution/execution_gate_v1.py`
- module note

完成条件：

- gate pipeline 对齐 `ExecutionContext + inventory`

### P2-14 迁入 portfolio_v3

输出：

- `asterion_core/risk/portfolio_v3.py`
- module note

完成条件：

- reservation / inventory / exposure gate 闭合

### P2-15 迁入 journal_v3

输出：

- `asterion_core/journal/journal_v3.py`
- module note

完成条件：

- orders / fills / reservations journaling 闭合

### P2-16 实现 Rule2Spec Agent

输出：

- `agents/common/runtime.py`
- `agents/common/client.py`
- `agents/common/persistence.py`
- `agents/weather/rule2spec_agent.py`
- agent output schema
- review hook
- `weather_rule2spec_review` manual job

完成条件：

- agent 只生成解析建议与风险标记
- 不替代 deterministic Rule2Spec parser
- 输出保持 station-first，不允许回流 city-first

### P2-17 实现 Data QA Agent

输出：

- `agents/weather/data_qa_agent.py`
- QA findings schema
- replay / provenance review hook
- `weather_data_qa_review` manual job

完成条件：

- 能针对 forecast replay、source fallback、pricing provenance 生成结构化 QA 建议
- 不直接改写 forecast 或 pricing 表

### P2-18 实现 Resolution Agent

输出：

- `agents/weather/resolution_agent.py`
- evidence review schema
- human-review hook
- `weather_resolution_review` manual job

完成条件：

- 能针对 settlement verification / evidence package / redeem suggestion 生成结构化审阅建议
- 不直接发起 dispute 或 redeem
- 继续保持在执行路径之外

### P2-19 迁入 readiness_checker_v1

输出：

- `asterion_core/monitoring/readiness_checker_v1.py`
- module note

完成条件：

- readiness gates 使用 Asterion phases，不使用旧 milestone 体系

### P2-20 迁入 ui_lite_db

输出：

- `asterion_core/ui/ui_lite_db.py`
- module note

完成条件：

- operator read model 可以只依赖 Asterion canonical tables 生成

### P2-21 执行 AlphaDesk Exit Gate 审查

输出：

- 更新后的 migration ledger
- `P2` closeout checklist
- Exit Gate judgement

完成条件：

- 能明确回答“是否还需要 AlphaDesk 作为执行层参考仓库”

---

## 8. P2 测试与验证

必须补的测试类别：

- forecast replay deterministic tests
- replay vs original reconciliation tests
- watcher backfill / restart / multi-RPC tests
- orchestration smoke tests
- agent invocation / evaluation / review contract tests
- strategy runtime / ticket / gate / journal contract tests
- portfolio / inventory / reservation gate tests
- readiness / ui_lite_db smoke tests

最低验证命令：

```bash
source .venv/bin/activate
python3 -m unittest discover -s tests -v
```

如新增 Dagster 或其他独立测试入口，也必须在本文件和 [Implementation_Index.md](../../../Implementation_Index.md) 同步登记。

---

## 9. P2 文档产物要求

`P2` 期间新增的实施文档，统一按下面规则放置：

- 阶段计划放 `docs/10-implementation/phase-plans/`
- 检查清单放 `docs/10-implementation/checklists/`
- runbook 放 `docs/10-implementation/runbooks/`
- 迁移台账放 `docs/10-implementation/versions/v1.0/migration-ledger/`
- module note 放 `docs/10-implementation/versions/v1.0/module-notes/`

`P2` 第一批建议补的 module notes：

- `AlphaDesk_strategy_engine_v3_Module_Note.md`
- `AlphaDesk_trade_ticket_v1_Module_Note.md`
- `AlphaDesk_signal_to_order_v1_Module_Note.md`
- `AlphaDesk_execution_gate_v1_Module_Note.md`
- `AlphaDesk_portfolio_v3_Module_Note.md`
- `AlphaDesk_journal_v3_Module_Note.md`
- `AlphaDesk_readiness_checker_v1_Module_Note.md`
- `AlphaDesk_ui_lite_db_Module_Note.md`
- `AlphaDesk_dagster_resources_Module_Note.md`
- `AlphaDesk_dagster_schedules_Module_Note.md`

---

## 10. P2 开工顺序

建议按下面顺序推进，而不是并行打散：

1. `P2-01` 到 `P2-03`：先收口 forecast replay
2. `P2-04` 到 `P2-06`：再收口 watcher backfill
3. `P2-07` 到 `P2-09`：再把 cold-path orchestration 壳立起来
4. `P2-10` 到 `P2-15`：再迁 execution foundation 主干
5. `P2-16` 到 `P2-18`：在 deterministic 底座上接入 3 个 agent
6. `P2-19` 到 `P2-21`：收尾 readiness / read model / Exit Gate

理由：

- replay 和 backfill 是 `P3` 之前最容易返工的基础设施
- orchestration 必须建立在 replay / backfill 的稳定 contract 之上
- execution foundation 要等 replay 和 operator provenance 基础稳定后再收口
- agent 必须建立在 deterministic parser、replay、verification、journal provenance 之上，而不是反过来定义主链路

---

## 11. P2 完成后的下一步

`P2` 完成后，立刻进入：

1. `P3` paper execution
2. order router / OMS / signer / execution gate 的 paper 集成
3. `Daily Review Agent`
4. readiness gate 联调
5. AlphaDesk Exit Gate 最终确认

也就是开始让 Asterion 从“可独立开发的 watch-only 系统”进入“可 paper execution 的独立系统”。
