# AGENTS.md

本文件定义 Asterion 仓库内 AI coding agents 的默认工作规范。

目标不是替代详细设计文档，而是给任何进入本仓库的 agent 一个统一、可执行、低歧义的协作基线。

---

## 1. 项目定位

Asterion 是一个面向 Polymarket 多领域事件市场的统一平台。

当前仓库状态：

- 当前默认开发状态是 `v2.0 implementation active`
- 当前状态固定表达为 `P4 accepted; post-P4 remediation accepted; v2.0 implementation active`
- active implementation entry 是 `docs/10-implementation/versions/v2.0/phase-plans/V2_Implementation_Plan.md`
- 当前仓库是独立的 Asterion 运行时代码仓库
- 当前系统定位是 `operator console + constrained execution infra`，不是 unattended live stack

Agent 在理解任务时，默认遵守以下边界：

- 当前 source of truth 是 **当前仓库代码 + docs 中已冻结的 contract 文档**
- 不得把旧文档中的 aspirational module tree 当作“已落地代码事实”
- 若文档与代码冲突，先以当前代码、migration、tests 为准，再回写文档

---

## 2. Canonical 文档入口

进入任务前，优先阅读以下文档：

1. `README.md`
2. `docs/00-overview/Documentation_Index.md`
3. `docs/00-overview/Version_Index.md`
4. `docs/00-overview/versions/v2.0/Asterion_Project_Plan.md`
5. `docs/00-overview/versions/v2.0/DEVELOPMENT_ROADMAP.md`
6. `docs/10-implementation/Implementation_Index.md`
7. `docs/10-implementation/versions/v2.0/phase-plans/V2_Implementation_Plan.md`
8. `docs/10-implementation/versions/v1.0-remediation/phase-plans/Post_P4_Remediation_Implementation_Plan.md`
9. `docs/10-implementation/versions/v1.0/phase-plans/P4_Implementation_Plan.md`
10. `docs/10-implementation/versions/v1.0/checklists/P4_Closeout_Checklist.md`
11. `docs/10-implementation/versions/v1.0/runbooks/P4_Controlled_Rollout_Decision_Runbook.md`
12. `docs/10-implementation/versions/v1.0/runbooks/P4_Controlled_Live_Smoke_Runbook.md`
13. `docs/10-implementation/versions/v1.0/checklists/P3_Closeout_Checklist.md`
14. `docs/10-implementation/versions/v1.0/runbooks/P3_Paper_Execution_Runbook.md`
15. `docs/10-implementation/versions/v1.0/phase-plans/P3_Implementation_Plan.md`

文档优先级：

1. 当前代码与 migration
2. subsystem 设计文档
3. phase closeout / runbook
4. 当前 phase implementation plan
5. overview 类文档

如果发现漂移：

- 不要静默忽略
- 在实施文档或相关导航文档中显式修正

---

## 3. 当前代码边界

Agent 必须基于当前真实代码结构工作，不得凭设计想象新增平行边界。

当前关键落点：

- `asterion_core/contracts/`
- `asterion_core/runtime/`
- `asterion_core/execution/`
- `asterion_core/risk/`
- `asterion_core/journal/`
- `asterion_core/monitoring/`
- `asterion_core/ui/`
- `domains/weather/`
- `dagster_asterion/`
- `agents/weather/`
- `sql/migrations/`
- `tests/`

当前已落地但仍需继续收口的 execution / live-prereq foundation：

- `strategy_engine_v3.py`
- `trade_ticket_v1.py`
- `signal_to_order_v1.py`
- `capability_refresh_v1.py`
- `blockchain/wallet_state_v1.py`
- `blockchain/chain_tx_v1.py`
- `signer/signer_service_v1.py`
- `execution/live_submitter_v1.py`
- `runtime.submit_attempts` / `weather_order_signing_smoke`
- `runtime.external_order_observations` / `weather_submitter_smoke`
- `runtime.external_fill_observations` / `weather_external_execution_reconciliation`
- `runtime.chain_tx_attempts` / `weather_chain_tx_smoke`
- `evaluate_p4_live_prereq_readiness(...)` / `weather_live_prereq_readiness`
- `config/controlled_live_smoke.json` / `weather_controlled_live_smoke`
- `ui.live_prereq_execution_summary` / `ui.live_prereq_wallet_summary`
- `execution_gate_v1.py`
- `order_router_v1.py`
- paper adapter / quote-based fill simulator
- `oms_state_machine_v1.py`
- `portfolio_v3.py`
- `journal_v3.py`

当前仍属于规划或后续阶段的内容，不应假设已存在可运行实现：

- `agents/weather/daily_review_agent.py`
- real signer backend / KMS / HSM
- 更完整的产品化 operator UI / multi-tenant auth / richer decision workflows
- 更深的 watch-only 研究台、长期 calibration dashboard 与更完整的多用户运营体验

当前 remediation 的 5 条主线固定为：

- 状态治理与验证闭环
- live boundary / secrets / 安全边界
- forecast -> pricing -> ranking 经济核心
- station mapping / calibration / 市场覆盖
- operator UI / readiness evidence / 运维入口

---

## 4. 执行与数据契约原则

任何 agent 在实现执行链路时，必须继续复用现有 canonical contracts，不得引入平行接口。

关键 contract 原则：

- 统一使用 `RouteAction + time_in_force/expiration`
- 统一使用 `CanonicalOrderContract`
- 下单前统一构造 `ExecutionContext`
- inventory / reservation / exposure 继续使用当前 canonical objects
- 不允许再发明第二套 `paper-only` 或 `live-only` execution contract

当前 canonical data flow 基线：

`weather.weather_watch_only_snapshots`
-> `strategy_engine_v3`
-> `runtime.strategy_runs`
-> `trade_ticket_v1`
-> `runtime.trade_tickets`
-> `signal_to_order_v1`
-> `capability.execution_contexts`
-> `execution_gate_v1`
-> `runtime.gate_decisions`
-> router / paper adapter / fill simulator
-> `trading.orders`
-> `trading.order_state_transitions`
-> `trading.reservations`
-> `trading.fills`
-> `trading.inventory_positions`
-> `trading.exposure_snapshots`
-> `trading.reconciliation_results`
-> `runtime.journal_events`
-> `ui.*`

Phase 2 之后，weather 机会链还有一个固定语义约束：

- `weather.weather_fair_values` 继续表示 `model fair value`
- `weather.weather_watch_only_snapshots.fair_value` 表示 `execution-adjusted fair value`
- `weather.weather_watch_only_snapshots.edge_bps` 表示 `executable edge`
- `model edge / fees / slippage / liquidity penalty / ranking` 进入 `pricing_context_json`

持久化原则：

- `trading.*` 是 canonical execution ledger
- `runtime.*` 是运行时 / 审计层
- `agent.*` 只保存 review / evaluation，不改写 canonical execution state
- 不新建 `paper.*` schema，除非有非常强的理由且不会重复表达现有语义

---

## 5. Agent 行为边界

所有 AI agent 必须保持在执行路径之外。

允许：

- 规则解析
- 数据质量审查
- 结算审阅
- 日报 / review / summary
- readiness / reconciliation 分析

不允许：

- 直接下单
- 直接撤单
- 直接改写 canonical execution tables
- 直接访问私钥、原始 signer、wallet secrets
- 通过 agent 输出绕过人工审批或规则 gate

如果引入新的 agent 相关代码：

- 输出必须是结构化建议
- 默认落到 `agent.invocations / outputs / reviews / evaluations`
- 失败不能阻塞主链路

---

## 6. 阶段边界

`P3` 只做 `paper execution`。

明确允许：

- paper order lifecycle
- paper router / paper adapter / quote-based fill simulator
- OMS state machine completion
- reservation / inventory / exposure / reconciliation closure
- operator read model
- paper run journal / daily ops / review flow
- P4 readiness / closeout entry criteria

明确不做：

- 真实下单
- 真实 signer RPC
- 真实链上广播
- KMS / HSM / Vault
- live capital deployment
- 生产级告警体系全量建设

如果任务触及 live side effects，默认应回退到：

- mock
- paper
- deterministic replay
- read-only validation

当前 `v2.0` 阶段，仍必须保持以下从 post-P4 remediation 继承的边界：

- 允许：real data ingress、capability refresh、external observation、signer shell、submitter dry-run / shadow / constrained real submit backend、chain tx scaffold、controlled live readiness
- 允许：在显式 env arm + operator approval + allowlist/cap 下的最小 `approve_usdc` controlled live smoke
- 不允许：默认启用真实下单、默认启用真实 signer side effects、无人值守 live rollout、真实资金自动部署
- 所有真实 side effects 必须保持 `default-off + explicit operator approval + auditable`
- 当前 operator surface 必须始终诚实表达这些边界，不得把系统写成 unattended 或 unrestricted live

Phase 1 起，默认还必须遵守：

- controlled-live secrets 只认独立前缀：
  - `ASTERION_CONTROLLED_LIVE_SECRET_ARMED`
  - `ASTERION_CONTROLLED_LIVE_SECRET_APPROVAL_TOKEN`
  - `ASTERION_CONTROLLED_LIVE_SECRET_PK_<WALLET_ID_UPPER_SNAKE>`
- `config/controlled_live_smoke.json` 只保留 allowlist / cap，不再承载 secret env 映射
- controlled-live capability boundary 以 `data/meta/controlled_live_capability_manifest.json` 为机器真相源
- UI auth 默认拒绝访问；未配置 `ASTERION_UI_USERNAME` / `ASTERION_UI_PASSWORD_HASH` 时不渲染 operator console
- `./start_asterion.sh --web` 与 UI refresh 只应读取最小只读 UI 环境，不得继承 controlled-live secrets

当前 constrained real submitter 的固定边界：

- backend kind 为 `real_clob_submit`
- 只接受现有 `SubmitOrderRequest`
- 只写现有 canonical 表：
  - `runtime.submit_attempts`
  - `runtime.external_order_observations`
  - `runtime.external_fill_observations`
  - `runtime.journal_events`
- 必须同时满足 manifest、readiness、wallet readiness、allowlist 与 operator approval token gate

当前交易能力增强路线固定为：

1. `forecast calibration`
2. `fair value -> executable edge`
3. `expected value / expected PnL` 排序
4. `market quality screen`
5. `predicted vs realized` 闭环

默认不允许跳过上述顺序，直接通过 UI heuristic 或 agent heuristic 推动真钱决策。

---

## 7. 测试与验证

本仓库默认测试入口：

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -e .
python3 -m unittest tests.test_p4_closeout -v
python3 -m unittest discover -s tests -v
```

测试基线：

- 仓库内 `.venv` 是 canonical 验证环境
- `duckdb` 是正式依赖
- system Python 若缺少 `duckdb`，不能据此否定仓库状态
- `tests/` 目录现在是显式 Python package；`python -m unittest tests.test_xxx -v` 与 `discover` 都应保持可用

修改代码时，默认要求：

- 至少运行与改动直接相关的测试
- 若改动 execution / storage / migrations / UI lite / readiness，优先补 DuckDB integration coverage
- 若改动 contract 或 schema，必须同步更新 tests

修改文档时，默认要求：

- 确认入口文档间导航一致
- `git diff --check` 通过

---

## 8. 文档维护规则

根目录文档原则：

- 根目录默认只保留 `README.md`
- `AGENTS.md` 作为仓库协作规范例外保留在根目录
- 其他项目文档统一放在 `docs/`

文档更新规则：

- 新增 phase 文档时，必须同步更新 `README.md`、`Documentation_Index.md`、`Implementation_Index.md`
- overview 文档不得把“未来规划”写成“当前已落地”
- 若当前 active phase plan 与代码状态不符，应优先修正文档而不是继续沿错口径开发
- 当前版本规划统一以 `docs/10-implementation/versions/v2.0/phase-plans/V2_Implementation_Plan.md` 为 active implementation entry
- `docs/10-implementation/versions/v1.0-remediation/phase-plans/Post_P4_Remediation_Implementation_Plan.md` 保留为 historical accepted remediation record
- 若任务落在历史 remediation 主线复核，必须先对照对应 `Phase 0` 到 `Phase 4` 与 `Post-P4 Phase 5` 到 `Post-P4 Phase 15` 的交付物、non-goals 和 exit criteria

---

## 9. 代码风格与实现原则

默认实现原则：

- 优先做 deterministic、可回放、可审计实现
- 优先扩展现有模块，不平行造新层
- 优先复用已有 canonical tables，不重复造表
- 小步修改，避免一次性重构整个子系统
- 代码、migration、tests、文档应一起闭环

提交前应检查：

- 是否引入了新的平行 contract
- 是否破坏了 `runtime.*` / `trading.*` 的分层
- 是否让 agent 进入了执行路径
- 是否把 future design 误写成 current state

---

## 10. 默认工作方式

Agent 接到任务后，默认顺序：

1. 先看当前代码和相关文档
2. 确认当前 phase 和 source of truth
3. 识别是否存在文档漂移
4. 再决定改代码、改文档、还是两者都改
5. 用最小改动完成闭环

如果任务不明确，优先做的不是猜，而是：

- 查当前代码
- 查 migration
- 查 tests
- 查 active phase plan / remediation plan / runbook

本文件是基础稿。后续若项目规范变化，应优先更新本文件，再继续扩展 agent 使用范围。
