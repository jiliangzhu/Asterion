# Asterion（星枢）项目

**版本**: v1.6
**更新日期**: 2026-03-22
**状态**: `P4 accepted; post-P4 remediation accepted; v2.0 implementation active`

---

## 📖 项目简介

**Asterion（星枢）** 是一个源于 AlphaDesk 设计 lineage、但当前运行时代码已完全以本仓库为准的 Polymarket 多领域事件市场统一平台。

**核心定位**: 不是 AlphaDesk 的"天气分支"，也不再依赖 AlphaDesk runtime，而是一个 `operator console + constrained execution infra` 形态的独立事件交易平台。

当前唯一 active implementation entry 已切换到 [V2_Implementation_Plan.md](./docs/10-implementation/versions/v2.0/phase-plans/V2_Implementation_Plan.md)；它作为 umbrella contract，[P11_Implementation_Plan.md](./docs/10-implementation/versions/v2.0/phase-plans/P11_Implementation_Plan.md) 现保留为最近 accepted tranche record，[P11_Closeout_Checklist.md](./docs/10-implementation/versions/v2.0/checklists/P11_Closeout_Checklist.md) 是最近 accepted tranche 的 closeout checklist，[P10_Implementation_Plan.md](./docs/10-implementation/versions/v2.0/phase-plans/P10_Implementation_Plan.md) 与 [P10_Closeout_Checklist.md](./docs/10-implementation/versions/v2.0/checklists/P10_Closeout_Checklist.md) 保留为更早的 accepted tranche / closeout records，而 [P9_Implementation_Plan.md](./docs/10-implementation/versions/v2.0/phase-plans/P9_Implementation_Plan.md)、[P8_Implementation_Plan.md](./docs/10-implementation/versions/v2.0/phase-plans/P8_Implementation_Plan.md)、[P7_Implementation_Plan.md](./docs/10-implementation/versions/v2.0/phase-plans/P7_Implementation_Plan.md) 与 [P6_Implementation_Plan.md](./docs/10-implementation/versions/v2.0/phase-plans/P6_Implementation_Plan.md) 分别保留为更早的 tranche-specific / accepted closeout / baseline records。[P8_Closeout_Checklist.md](./docs/10-implementation/versions/v2.0/checklists/P8_Closeout_Checklist.md) 与 [P7_Closeout_Checklist.md](./docs/10-implementation/versions/v2.0/checklists/P7_Closeout_Checklist.md) 保留为更早的 historical accepted closeout checklists。`P4` 与 [Post_P4_Remediation_Implementation_Plan.md](./docs/10-implementation/versions/v1.0-remediation/phase-plans/Post_P4_Remediation_Implementation_Plan.md) 当前都保留为 historical accepted records。当前系统状态固定表达为 `P4 accepted; post-P4 remediation accepted; v2.0 implementation active`，最近 accepted tranche: `Phase 11`；当前没有比 `Phase 11` 更新的 tranche-specific implementation plan 已打开；`P11` 已 accepted closeout，不表示 unattended live，也不表示 unrestricted live。
当前 execution economics 已进入 feedback-backed 排序阶段：`weather_execution_priors_refresh` 会 nightly materialize market/strategy/wallet cohort priors，并把 feedback suppression 写回统一的 `ranking_score` 主链。

当前 `runtime / UI / paper candidate` 已统一按 penalty-aware `ranking_score` 排序；`edge_bps_executable` 继续保留 raw executable edge 语义。Execution science 读面也已升级到 capture / miss / distortion 视角。

当前 execution economics 已进入 ranking v2：`weather.weather_execution_priors` 作为 serving table 输入 `ExecutionPriorSummary`，`ranking_score` 按 unit-opportunity EV / capture / risk / capital-efficiency 计算，Home / Markets 直接消费同一份 `why_ranked_json`。

当前 operator surface 已进入 truth-source hardening：sidebar 由 readiness / capability 动态生成，核心市场与 execution rows 已显式标记 `canonical / fallback / stale / degraded / derived` source badge，UI 只再把 `Ranking Score` 作为主排序分数。

当前 calibration 已进入 v2：`weather.forecast_calibration_profiles_v2` 作为 bias / regime / threshold-probability quality 的 serving table；adapter correction layer 会把 `distribution_summary_v2` 写入同一条 forecast / pricing 主链，低质量样本会真实压低 `ranking_score`。

当前 UI 默认只监听 `127.0.0.1`；public bind 需要显式设置 `ASTERION_UI_ALLOW_PUBLIC_BIND=true`。controlled-live tx signer 也已固定只按 `wallet_id` 推导 secret scope，不再接受 caller 注入 `private_key_env_var`。

当前 UI read-model 也已完成 Phase 15 收口：`ui.read_model_catalog` 与 `ui.truth_source_checks` 已进入 UI lite build，split loader contracts 和 builder registry 也已把 truth-source、source badge、primary score baseline 固定为可测试 contract。

深度审计提出的后续优化路线，已经收口为 [Post_P4_Remediation_Implementation_Plan.md](./docs/10-implementation/versions/v1.0-remediation/phase-plans/Post_P4_Remediation_Implementation_Plan.md) 中的历史 accepted remediation record；后续新工作继续从 `v2.0` umbrella 入口规划，待新的 tranche-specific plan 明确后再切换入口。

---

## 📚 文档结构

```text
Asterion/
├── README.md                                  # 本文件 - 唯一保留在根目录的导航文档
└── docs/
    ├── 00-overview/
    │   ├── Documentation_Index.md            # 文档索引与分类规范
    │   ├── Version_Index.md                  # 版本导航入口
    │   └── versions/
    │       ├── v1.0/
    │       │   └── Version_Summary.md
    │       ├── v1.0-remediation/
    │       │   └── Version_Summary.md
    │       └── v2.0/
    │           ├── Asterion_Project_Plan.md
    │           └── DEVELOPMENT_ROADMAP.md
    ├── 10-implementation/
    │   ├── Implementation_Index.md          # 实施文档总入口
    │   ├── checklists/
    │   │   ├── Checklist_Index.md
    │   └── versions/
    │       ├── v1.0/
    │       │   ├── phase-plans/
    │       │   ├── checklists/
    │       │   ├── runbooks/
    │       │   ├── migration-ledger/
    │       │   └── module-notes/
    │       ├── v1.0-remediation/
    │       │   ├── phase-plans/
    │       │   └── checklists/
    │       └── v2.0/
    │           ├── phase-plans/
    │           ├── checklists/
    │           └── runbooks/
    ├── 20-architecture/
    │   ├── Database_Architecture_Design.md
    │   ├── Event_Sourcing_Design.md
    │   ├── Hot_Cold_Path_Architecture.md
    │   └── UI_Read_Model_Design.md
    ├── 30-trading/
    │   ├── CLOB_Order_Router_Design.md
    │   ├── OMS_Design.md
    │   ├── Market_Capability_Registry_Design.md
    │   ├── Signer_Service_Design.md
    │   ├── Gas_Manager_Design.md
    │   ├── Controlled_Live_Boundary_Design.md
    │   └── Execution_Economics_Design.md
    ├── 40-weather/
    │   ├── Forecast_Ensemble_Design.md
    │   ├── UMA_Watcher_Design.md
    │   └── Forecast_Calibration_v2_Design.md
    ├── analysis/
    │   ├── Analysis_Index.md
    │   ├── 01_Current_Code_Reassessment.md
    │   ├── 02_Current_Deep_Audit_and_Improvement_Plan.md
    │   └── 10_*.md / 11_*.md / 12_*.md / 13_*.md
    └── 50-operations/
        ├── Agent_Monitor_Design.md
        └── Operator_Console_Truth_Source_Design.md
```

---

## 🎯 快速开始

### 1. 了解项目

**推荐阅读顺序**:
1. 阅读本 README（5 分钟）
2. 阅读 [Documentation_Index.md](./docs/00-overview/Documentation_Index.md)（10 分钟）
3. 阅读 [Version_Index.md](./docs/00-overview/Version_Index.md)（10 分钟）
4. 阅读 [Asterion_Project_Plan.md](./docs/00-overview/versions/v2.0/Asterion_Project_Plan.md)（30 分钟）
5. 阅读 [DEVELOPMENT_ROADMAP.md](./docs/00-overview/versions/v2.0/DEVELOPMENT_ROADMAP.md)（15 分钟）
   - 重点看其中的 `AlphaDesk Migration Track`，这里已经按实际代码语义区分了“直接迁入 / 保留壳重写 / 禁止迁入”
6. 阅读 [Implementation_Index.md](./docs/10-implementation/Implementation_Index.md)
   - 这是所有实施文档的统一入口，后续阶段文档都从这里找
7. 阅读 [V2_Implementation_Plan.md](./docs/10-implementation/versions/v2.0/phase-plans/V2_Implementation_Plan.md)
   - 这是当前唯一 active implementation entry；作为 umbrella contract，已锁定 `Weather-first` v2.0 workstreams、phases、planned interfaces 与 acceptance 结构
8. 阅读 [P10_Implementation_Plan.md](./docs/10-implementation/versions/v2.0/phase-plans/P10_Implementation_Plan.md)
   - 这是最近 accepted tranche record；主题是 `Deterministic ROI Repair and Execution Intelligence Foundation`
9. 阅读 [P10_Closeout_Checklist.md](./docs/10-implementation/versions/v2.0/checklists/P10_Closeout_Checklist.md)
   - 这是最近 accepted tranche 的 closeout checklist；固定锁住 `PRAGMA` regression repair、execution-intelligence、priors grain 与 allocator scheduling uplift 的验收面
10. 阅读 [P11_Implementation_Plan.md](./docs/10-implementation/versions/v2.0/phase-plans/P11_Implementation_Plan.md)
   - 这是当前 tranche implementation plan；主题是 `Opportunity Triage / Execution Intelligence Agent`
11. 如需回看刚完成之前的 operator-surface delivery baseline，再阅读 [P9_Implementation_Plan.md](./docs/10-implementation/versions/v2.0/phase-plans/P9_Implementation_Plan.md)
   - 这是更早的 tranche-specific implementation record；主题是 `Operator Surface Delivery and Throughput Scaling`
12. 如需回看 calibration hard gate / scaling-aware capital discipline closeout，再阅读 [P8_Implementation_Plan.md](./docs/10-implementation/versions/v2.0/phase-plans/P8_Implementation_Plan.md)
   - 这是更早 accepted tranche record；保留 `Phase 8 — Calibration Hard Gates and Scaling-Aware Capital Discipline` 的已交付 closeout 基线
13. 如需回看刚完成的 deployable rerank / allocator v2 / economics closeout，再阅读 [P7_Implementation_Plan.md](./docs/10-implementation/versions/v2.0/phase-plans/P7_Implementation_Plan.md)
   - 这是 `Phase 7` 的 accepted closeout record
14. 再回看 deployable baseline 时，阅读 [P6_Implementation_Plan.md](./docs/10-implementation/versions/v2.0/phase-plans/P6_Implementation_Plan.md)
   - 这是 `Phase 6` 的 accepted tranche baseline record
15. 阅读 [Implementation_Index.md](./docs/10-implementation/Implementation_Index.md) 中的 active / historical 分类
   - 先区分当前 `v2.0 implementation active` 入口与 `P4`、post-P4 remediation 的历史 accepted records
16. 阅读 [Post_P4_Remediation_Implementation_Plan.md](./docs/10-implementation/versions/v1.0-remediation/phase-plans/Post_P4_Remediation_Implementation_Plan.md)
   - 这是 `P4` 之后到 `v2.0` 之前的历史 accepted remediation 记录；current reassessment 与 deep audit 的后续工作已在其中完成收口
17. 阅读 [P4_Implementation_Plan.md](./docs/10-implementation/versions/v1.0/phase-plans/P4_Implementation_Plan.md)
   - 这是 `P4 live prerequisites` 的 historical accepted phase record
18. 阅读 [P4_Closeout_Checklist.md](./docs/10-implementation/versions/v1.0/checklists/P4_Closeout_Checklist.md)
   - 这是 `P4` closeout 的历史审查记录，不再是当前 active closeout 入口
19. 阅读 supporting design docs（按历史 accepted 参考或后续 v2.0 细化需要选择）
   - [Controlled_Live_Boundary_Design.md](./docs/30-trading/Controlled_Live_Boundary_Design.md)
   - [Operator_Console_Truth_Source_Design.md](./docs/50-operations/Operator_Console_Truth_Source_Design.md)
   - [Execution_Economics_Design.md](./docs/30-trading/Execution_Economics_Design.md)
   - [Forecast_Calibration_v2_Design.md](./docs/40-weather/Forecast_Calibration_v2_Design.md)
   - [UI_Read_Model_Design.md](./docs/20-architecture/UI_Read_Model_Design.md)
16. 阅读 [Analysis_Index.md](./docs/analysis/Analysis_Index.md)
   - `docs/analysis/` 现在统一先从这个索引进入；`01-09` 是 current analysis inputs，`10+` 是 historical snapshots，不再混作并列主入口
17. 如需回看 historical operator records，再阅读 [P4_Controlled_Rollout_Decision_Runbook.md](./docs/10-implementation/versions/v1.0/runbooks/P4_Controlled_Rollout_Decision_Runbook.md)
18. 如需回看 historical controlled-live smoke records，再阅读 [P4_Controlled_Live_Smoke_Runbook.md](./docs/10-implementation/versions/v1.0/runbooks/P4_Controlled_Live_Smoke_Runbook.md)
19. 如需回看 historical smoke operator records，再阅读 [P4_Real_Weather_Chain_Smoke_Runbook.md](./docs/10-implementation/versions/v1.0/runbooks/P4_Real_Weather_Chain_Smoke_Runbook.md)
20. 阅读 [P3_Closeout_Checklist.md](./docs/10-implementation/versions/v1.0/checklists/P3_Closeout_Checklist.md)
   - 这是 `P3` 是否具备 closeout 条件、是否可进入 `P4 planning` 的 closeout 审查入口
21. 阅读 [P3_Paper_Execution_Runbook.md](./docs/10-implementation/versions/v1.0/runbooks/P3_Paper_Execution_Runbook.md)
   - 这是 `P3 paper execution` 当前 canonical operator / daily ops / readiness 运行入口
22. 如需进入 `P4` 之前的阶段边界，再阅读 [P3_Implementation_Plan.md](./docs/10-implementation/versions/v1.0/phase-plans/P3_Implementation_Plan.md)
23. 阅读 [Checklist_Index.md](./docs/10-implementation/checklists/Checklist_Index.md)
   - 当前先区分 `v2.0` closeout placeholder、historical accepted checklists 和 archive checklist
24. 阅读 [AlphaDesk_Migration_Ledger.md](./docs/10-implementation/versions/v1.0/migration-ledger/AlphaDesk_Migration_Ledger.md)
   - 如果目标是回看 AlphaDesk exit gate 和剩余迁移结论，现在统一以 migration ledger 为准
25. 阅读 [P2_Closeout_Checklist.md](./docs/10-implementation/versions/v1.0/checklists/P2_Closeout_Checklist.md)
   - 这是 `P2` 是否已经关闭、`P3` 是否可以开工、AlphaDesk Exit Gate 是否通过的唯一关闭依据
26. 阅读 [P1_Watch_Only_Replay_Cold_Path_Runbook.md](./docs/10-implementation/versions/v1.0/runbooks/P1_Watch_Only_Replay_Cold_Path_Runbook.md)
   - 这是 `watch-only / replay / cold path` 当前 canonical 入口和 operator 读路径
27. 阅读 [P2_Cold_Path_Orchestration_Job_Map_Runbook.md](./docs/10-implementation/versions/v1.0/runbooks/P2_Cold_Path_Orchestration_Job_Map_Runbook.md)
   - 这是 `P2-07` 到 `P2-09` 的 canonical job map、schedule 和 handler 入口
28. 如需回看 `P2` 的实施顺序，再阅读 [P2_Implementation_Plan.md](./docs/10-implementation/versions/v1.0/phase-plans/P2_Implementation_Plan.md)
29. 如需回看 `P1` 阶段计划，再阅读 [P1_Implementation_Plan.md](./docs/10-implementation/versions/v1.0/phase-plans/P1_Implementation_Plan.md)
30. 如需回看底座建设，再阅读 [P0_Implementation_Plan.md](./docs/10-implementation/versions/v1.0/phase-plans/P0_Implementation_Plan.md)
31. 深入阅读详细设计文档（按需）

### 1.1 文档归档规则

- 根目录只保留 `README.md`
- 其他项目文档全部进入 `docs/`
- `docs/10-implementation/` 根目录只保留 [Implementation_Index.md](./docs/10-implementation/Implementation_Index.md)
- 当前 active implementation 计划统一进入 `docs/10-implementation/versions/v2.0/phase-plans/`
- historical implementation materials 统一进入 `docs/10-implementation/versions/v1.0*/`
- 全局 checklist 导航保留在 `docs/10-implementation/checklists/Checklist_Index.md`
- 迁移总台账统一进入 `docs/10-implementation/versions/v1.0/migration-ledger/`
- module notes 统一进入 `docs/10-implementation/versions/v1.0/module-notes/`
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
python3 -m unittest tests.test_p4_closeout -v
python3 -m unittest discover -s tests -v
```

其中两类测试入口都应保持可用：

- `python3 -m unittest tests.test_xxx -v`
- `python3 -m unittest discover -s tests -v`

### 2.2 Operator Console 与真实天气链路

当前 UI 已收口成 `Operator Console`，默认以 canonical `ui.*` read models 为主数据源；`real_weather_chain_report.json` 只作为 weather smoke 辅助视图。

推荐启动方式：

```bash
cd /Users/jayzhu/web3/Asterion
./start_asterion.sh --all
```

当前 `--all` 的行为：

- 启动真实天气市场链路 loop
- 默认启用 weather agents
- 尝试刷新 `P4` readiness / UI lite surfaces
- 启动 Streamlit Operator Console

当前 Operator Console 与 controlled-live boundary：

- UI auth 已启用；未配置 `ASTERION_UI_USERNAME` / `ASTERION_UI_PASSWORD_HASH` 时默认拒绝访问
- `./start_asterion.sh --web` 与 operator surface refresh 现在只注入最小只读 UI 环境，不再继承完整 `.env`
- controlled-live secrets 只认独立 env 前缀：
  - `ASTERION_CONTROLLED_LIVE_SECRET_ARMED`
  - `ASTERION_CONTROLLED_LIVE_SECRET_APPROVAL_TOKEN`
  - `ASTERION_CONTROLLED_LIVE_SECRET_PK_<WALLET_ID_UPPER_SNAKE>`
- `GO` 只表示 rollout decision readiness；真正的 capability boundary 以 `data/meta/controlled_live_capability_manifest.json` 为真相源
- submitter backend 当前支持：
  - `disabled`
  - `shadow_stub`
  - `real_clob_submit`
- `real_clob_submit` 与 `controlled live chain-tx` 都要求显式 `ASTERION_CONTROLLED_LIVE_SECRET_ARMED=true`；direct service 调用缺少 arming guard 时会在 shell 层直接拒绝
- `real_clob_submit` 只代表 constrained real submit backend 已存在；仍必须满足 `manual-only + allowlist + readiness GO + approval token + auditable`
- `real_clob_submit` 的 live path 现在要求 shell 提交 `boundary inputs`，并由 backend 校验 approved attestation；live submit 的 allow/block 决策会落到 `runtime.live_boundary_attestations`

Weather smoke 当前默认行为：

- discovery 优先走 `Gamma events weather feed`
- 只抓 `active=true`、`closed=false`、`archived=false` 的市场
- horizon 按 `14 -> 30 -> 60 -> 90` 自适应
- 会批量处理所有当前开盘且能完成站点映射的市场，而不是只处理单一城市
- `rule2spec` 默认走 deterministic validation
- `data_qa` 当前走 deterministic replay / provenance validation；若当前链路无 canonical 输入，会明确显示 `not_run`
- `resolution_agent` 继续保留，但只在存在 canonical resolution 输入时参与 human review closure

UI 当前重点页面：

- `Home`：Decision Center，优先展示 readiness decision、top opportunities、最大 blocker、degraded inputs 与 predicted-vs-realized 摘要
- `Markets`：Opportunity Terminal，按 actionability + ranking score 排序展示所有 open recent markets，并补充 pricing decomposition、input integrity 与 execution reality
- `Execution`：Execution Reality，默认展示 execution science / execution-path evidence，并补充 `watch-only vs executed` capture、execution science cohort、market research 与 calibration health 摘要
- `Agents`：Exception Review，重点展示 human review queue、latest agent exceptions 与 runtime boundary；它不参与主排序，也不参与 readiness gate
- `System`：Readiness Evidence，优先展示 evidence bundle、capability boundary、dependency freshness 与 blockers/warnings

### 3. Weather MVP 范围

**目标市场**: 美国城市单日最高温区间市场

**核心流程**:
```
市场发现 → 规则解析 → 预测 → 定价 → 执行 → 结算监控 → 赎回
```

---

## 🏗️ 技术架构

### 当前已落地模块（P4-12）

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
    └── resolution_agent.py
```

说明：

- 上面只列当前仓库**已经落地**的主干模块
- `rule2spec` / `data_qa` 当前 active reality 已切到 deterministic validation，不再作为默认 LLM review chain
- `order_router_v1.py`、paper adapter、quote-based fill simulator、OMS state machine 已在 `P3` 落地
- `capability_refresh_v1.py` 与 `clients/clob_public.py` 已在 `P4-02` 落地，用于 canonical capability refresh
- `blockchain/wallet_state_v1.py` 与 `runtime.external_balance_observations` 已在 `P4-03` 落地，用于 external wallet state observation
- `signer/signer_service_v1.py`、`meta.signature_audit_logs`、`runtime.submit_attempts` 已在 `P4-04` / `P4-05` 落地，用于 signer shell、official-order-compatible signing 与 sign-only / submit attempt ledger
- `execution/live_submitter_v1.py`、`runtime.external_order_observations` 已在 `P4-06` 落地，用于 canonical submitter dry-run / shadow path
- `blockchain/chain_tx_v1.py`、`runtime.chain_tx_attempts` 已在 `P4-07` 落地，用于 approve-first gas / nonce / signing / shadow-broadcast scaffold
- `runtime.external_fill_observations`、`weather_external_execution_reconciliation` 与 external-aware `trading.reconciliation_results` 已在 `P4-08` 落地，用于 shadow external execution reconciliation
- `ui.live_prereq_execution_summary`、`ui.live_prereq_wallet_summary` 与扩展后的 `ui.execution_*` 已在 `P4-09` 落地，用于 operator live-prereq read model
- `evaluate_p4_live_prereq_readiness(...)` 与 `weather_live_prereq_readiness` 已在 `P4-10` 落地，用于 minimum ops hardening、hourly P4 readiness report 以及 `ui.phase_readiness_summary`
- `weather_controlled_live_smoke`、`config/controlled_live_smoke.json` 与 controlled-live runbook 已在 `P4-11` 落地，用于 `approve_usdc` 的最小真实 side-effect 边界；historical remediation 已进一步补上 capability manifest、独立 secret env 前缀、app-level UI auth gate、UI 最小环境注入，以及 `real_clob_submit` constrained backend，默认仍是 `default-off + manual-only + auditable`
- `P4_Closeout_Checklist.md`、`P4_Controlled_Rollout_Decision_Runbook.md` 与 `P4` closeout doc tests 已在 `P4-12` 落地，用于保留 historical closeout / rollout decision 审查入口；当前 active implementation entry 已切换到 `v2.0`
- `weather_chain_tx_smoke` 已成为 `P4-07` 的 canonical chain-tx manual entry；当前只开放 `approve_usdc`
- `weather_signer_audit_smoke`、`weather_order_signing_smoke`、`weather_submitter_smoke` 与 `weather_external_execution_reconciliation` 已成为 `P4` signer / order-signing / submitter / reconciliation 的 canonical entry
- `daily_review_agent.py` 仍未落地；当前只完成 `ui.daily_review_input` 等 review input surface
- `P4` historical closeout / decision 记录见 [P4_Implementation_Plan.md](./docs/10-implementation/versions/v1.0/phase-plans/P4_Implementation_Plan.md)、[P4_Closeout_Checklist.md](./docs/10-implementation/versions/v1.0/checklists/P4_Closeout_Checklist.md)、[P4_Controlled_Rollout_Decision_Runbook.md](./docs/10-implementation/versions/v1.0/runbooks/P4_Controlled_Rollout_Decision_Runbook.md)；当前 active implementation entry 是 [V2_Implementation_Plan.md](./docs/10-implementation/versions/v2.0/phase-plans/V2_Implementation_Plan.md)，[Post_P4_Remediation_Implementation_Plan.md](./docs/10-implementation/versions/v1.0-remediation/phase-plans/Post_P4_Remediation_Implementation_Plan.md) 保留为 historical accepted remediation record
- `P3` 的 canonical closeout 与 runbook 入口见 [P3_Closeout_Checklist.md](./docs/10-implementation/versions/v1.0/checklists/P3_Closeout_Checklist.md)、[P3_Paper_Execution_Runbook.md](./docs/10-implementation/versions/v1.0/runbooks/P3_Paper_Execution_Runbook.md)

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
- closeout 入口：[P3_Closeout_Checklist.md](./docs/10-implementation/versions/v1.0/checklists/P3_Closeout_Checklist.md)

### Phase 4: Live Prerequisites
- ✅ real data ingress / capability refresh / signer boundary
- ✅ submitter dry-run / shadow path
- ✅ chain tx scaffold
- ✅ readiness / controlled rollout criteria
- historical phase 入口：[P4_Implementation_Plan.md](./docs/10-implementation/versions/v1.0/phase-plans/P4_Implementation_Plan.md)

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
