# Asterion P4 Implementation Plan

**版本**: v1.0
**更新日期**: 2026-03-11
**阶段**: `P4`
**状态**: implementation active（`P4-01` / `P4-02` / `P4-03` / `P4-04` 已完成）
**目标**: 在 `P3 paper execution` 已关闭的基础上，补齐 `live prerequisites`：真实外部只读数据、capability refresh、signer boundary、submitter dry-run/shadow path、chain transaction scaffolding、external reconciliation、operator/readiness/ops hardening，并保持默认安全边界。

---

## 1. Phase Summary

`P4` 的唯一主题是 `live prerequisites`。

本阶段允许推进的能力是：

1. 在当前 `P3` canonical contracts / ledgers 不变的前提下，接入真实外部只读数据源
2. 把 `signature_type / funder / wallet_type / allowance_targets` 这些已冻结 contract 真正推进到 signer、submitter、chain transaction 的受控实现边界
3. 建立 official-signing-compatible、default-safe 的 order submit / cancel / chain tx scaffold
4. 把本地 `trading.*` ledger 与外部 CLOB / chain observations 接成可审计的 reconciliation / operator surface
5. 形成 `controlled live smoke` 前的 readiness / runbook / closeout 入口

本阶段明确约束：

- source of truth 是**当前 Asterion 仓库代码**与**已冻结设计文档 contract**
- `P4` 不是 production live rollout
- `P4` 不允许把 `paper` 与 `live` 拆成两套 execution contracts
- `P4` 不允许引入与现有 `RouteAction / CanonicalOrderContract / ExecutionContext / Order / Fill / Reservation / ExposureSnapshot` 平行的新执行接口
- `P4` 的任何真实 side effect 都必须是 `default-off + explicit operator approval + auditable`
- `P4` 结束时，结论只能是 `ready for controlled live rollout decision`，不能直接等同于“已经可以无人值守 live”

---

## 2. P4 Source Of Truth

本阶段实施以以下内容为准：

- [README.md](../../../README.md)
- [Asterion_Project_Plan.md](../../00-overview/Asterion_Project_Plan.md)
- [DEVELOPMENT_ROADMAP.md](../../00-overview/DEVELOPMENT_ROADMAP.md)
- [Documentation_Index.md](../../00-overview/Documentation_Index.md)
- [Implementation_Index.md](../Implementation_Index.md)
- [P3_Implementation_Plan.md](./P3_Implementation_Plan.md)
- [P3_Closeout_Checklist.md](../checklists/P3_Closeout_Checklist.md)
- [P3_Paper_Execution_Runbook.md](../runbooks/P3_Paper_Execution_Runbook.md)
- [CLOB_Order_Router_Design.md](../../30-trading/CLOB_Order_Router_Design.md)
- [OMS_Design.md](../../30-trading/OMS_Design.md)
- [Market_Capability_Registry_Design.md](../../30-trading/Market_Capability_Registry_Design.md)
- [Signer_Service_Design.md](../../30-trading/Signer_Service_Design.md)
- [Gas_Manager_Design.md](../../30-trading/Gas_Manager_Design.md)
- [Database_Architecture_Design.md](../../20-architecture/Database_Architecture_Design.md)
- [Hot_Cold_Path_Architecture.md](../../20-architecture/Hot_Cold_Path_Architecture.md)
- `asterion_core/`
- `domains/weather/`
- `dagster_asterion/`
- `sql/migrations/`
- `tests/`

### 2.1 当前代码级验证结论

截至 `2026-03-11`，当前仓库已经由代码与测试支撑以下事实：

- `P3 paper execution` 已关闭
- `.venv/bin/python -m unittest discover -s tests -v` 当前通过 `138` 个测试
- 当前 `paper execution` 主链已经贯通：
  - `runtime.strategy_runs`
  - `runtime.trade_tickets`
  - `capability.execution_contexts`
  - `runtime.gate_decisions`
  - `trading.orders`
  - `trading.order_state_transitions`
  - `trading.reservations`
  - `trading.fills`
  - `trading.inventory_positions`
  - `trading.exposure_snapshots`
  - `trading.reconciliation_results`
  - `runtime.journal_events`
  - `ui.execution_* / ui.daily_* / ui.phase_readiness_summary`
- 当前 repo 已有真实外部只读接入壳，但还没有 canonical 端到端 live-prereq entry：
  - Gamma market discovery: `asterion_core/clients/gamma.py`、`domains/weather/scout/market_discovery.py`
  - real forecast adapters: `domains/weather/forecast/adapters.py`
  - real HTTP client/resource shell: `dagster_asterion/resources.py`
  - watcher replay/backfill contract: `domains/weather/resolution/backfill.py`、`domains/weather/resolution/rpc_fallback.py`
- `P4-01` 已完成：
  - `weather_market_discovery` 已成为 canonical cold-path ingress job
  - `Gamma + OpenMeteo/NWS` 已可生成真实 `weather.weather_watch_only_snapshots`
- `P4-02` 已完成：
  - `weather_capability_refresh` 已成为 canonical capability refresh job
  - `capability.market_capabilities` 已可由 `weather.weather_markets + CLOB public + overrides` 刷新
  - `capability.account_trading_capabilities` 已可由 `wallet_registry.json + minimal chain read + overrides` 刷新
- `P4-03` 已完成：
  - `weather_wallet_state_refresh` 已成为 canonical external wallet state observation job
  - `runtime.external_balance_observations` 已可持久化 native gas、`USDC.e` balance 与 `USDC.e` allowance observation
  - `asterion_core/blockchain/` 已落地为 read-only chain observation helpers
- `P4-04` 已完成：
  - `asterion_core/signer/` 已落地为 default-off signer shell
  - `weather_signer_audit_smoke` 已成为 canonical signer smoke entry
  - `meta.signature_audit_logs` 已接入 signer request / response / payload hash audit path

### 2.2 P4 Start-State Register

`P4` 不是从空白开始，但当前起点也不是“live path 已经实现”。以下差距必须作为 `P4` 起点显式登记：

#### Register A: signer shell 已落地，但 official signing backend 与 live blockchain path 仍未落地

- [Signer_Service_Design.md](../../30-trading/Signer_Service_Design.md) 与 [Gas_Manager_Design.md](../../30-trading/Gas_Manager_Design.md) 已有接口冻结候选
- 当前 repo 中已存在 `asterion_core/signer/`，但 backend 仍保持 `default-off`
- 当前 repo 中的 `asterion_core/blockchain/` 仅包含 read-only wallet observation helpers，还没有 live submit / broadcast / nonce / gas management path

#### Register B: `meta.signature_audit_logs` 已接入 shell audit，但尚未进入 official signing / submit path

- `0010_signature_audit_boundary.sql` 已扩充 `meta.signature_audit_logs`
- 当前 signer shell 已写 request / response / payload hash，但 official signing / submitter 仍未消费该审计链

#### Register C: capability contract 与 canonical refresh path 已闭合，但 downstream live path 仍未消费其全部信号

- `capability.market_capabilities`、`capability.account_trading_capabilities` 已在 schema 中运行
- 当前 `P3/P4` 已消费落库 capability
- canonical 的 `Gamma + CLOB public + chain read + local config` refresh pipeline 已在 `P4-02` 落地
- 但 submitter / live tx scaffold 仍未把这些 refreshed values 全量接入

#### Register D: orchestration job map 已覆盖 signer smoke，但还未覆盖 submitter / chain tx / shadow submit

- 当前 `dagster_asterion/job_map.py` 已包含 `weather_signer_audit_smoke`
- 但仍没有 submitter / chain tx / shadow submit job
- 当前仍没有 canonical 的 `controlled live smoke` job boundary

#### Register E: reconciliation 仍是 paper-local deterministic reconciliation

- 当前 `trading.reconciliation_results` 只比对本地 paper ledger 与 deterministic expectation
- 还没有 external CLOB / chain observations 驱动的 reconciliation

### 2.3 冲突处理顺序

实施中若出现冲突，优先级如下：

1. 当前代码与 migrations
2. subsystem 设计文档
3. [P3_Closeout_Checklist.md](../checklists/P3_Closeout_Checklist.md)
4. 本实施文档
5. overview / roadmap 中的高层总结

---

## 3. Scope / Non-Goals

### 3.1 P4 要做什么

`P4` 必须完成以下能力：

1. real data ingress for paper / shadow execution
2. capability refresh from `Gamma / CLOB public / chain read / local wallet config`
3. signer service shell and official-order-compatible signing path
4. submitter dry-run / shadow path
5. chain transaction gas / nonce / broadcast scaffold
6. external reconciliation against CLOB / chain observations
7. operator live-prereq read model / runbook / readiness / minimum alerting
8. closeout / controlled rollout decision entry criteria

### 3.2 P4 不做什么

`P4` 明确不做：

- 不把系统宣告为 production live
- 不做无人值守 autonomous live trading
- 不默认启用真实资金 deployment
- 不为了 signer / submitter 再发明第二套 order / inventory / reconciliation contract
- 不把 `paper` 和 `live` 切成两条互不兼容的 orchestration path
- 不在 `P4` 中扩展 Tech pack / Crypto pack
- 不在 `P4` 中追求 production-grade 全量告警平台；只做 minimum viable ops hardening

---

## 4. Canonical P4 Operating Model

### 4.1 Mode Ladder

`P4` 的运行模式必须固定为逐级放行，而不是一步跳到 live：

1. `paper_real_data`
   - 使用真实外部只读数据
   - 执行仍停留在 `paper`
   - 无 signer / submit / chain broadcast side effect
2. `sign_only_smoke`
   - 允许 official-order-compatible signing
   - 不做真实 submit / broadcast
   - 主要验证 signer boundary、payload、audit
3. `shadow_submit`
   - 构造真实 submit/cancel payload
   - 允许对外部 order state 做 read-only observation
   - 默认不广播真实资金 side effect
4. `controlled_live_smoke`
   - 只允许显式 operator 批准
   - 单 wallet / 单环境 / 小规模 / 可回溯
   - 仍不等同于 production live

`P4` 不包含 `autonomous_live` 模式。

### 4.2 Canonical Data Flow

`P4` 的主链固定为：

```text
Gamma / CLOB public / Open-Meteo / NWS / Polygon RPC
-> weather.weather_markets / weather.weather_market_specs / weather.weather_forecast_runs / weather.weather_watch_only_snapshots
-> capability.market_capabilities / capability.account_trading_capabilities
-> runtime.strategy_runs
-> runtime.trade_tickets
-> capability.execution_contexts
-> runtime.gate_decisions
-> order_router_v1
-> meta.signature_audit_logs
-> runtime.submit_attempts
-> runtime.external_order_observations
-> runtime.external_fill_observations
-> runtime.external_balance_observations
-> trading.orders / trading.fills / trading.order_state_transitions / trading.reservations / trading.inventory_positions / trading.exposure_snapshots / trading.reconciliation_results
-> runtime.journal_events
-> ui.execution_* / ui.daily_* / ui.live_prereq_* / ui.phase_readiness_summary
```

补充规则：

- `trading.*` 仍是 canonical execution ledger
- `runtime.*` 负责 attempt / observation / journal / readiness audit
- `meta.signature_audit_logs` 负责 signer 审计
- external observations 不能绕开 `trading.*` 直接定义业务真相

### 4.3 Human-In-The-Loop 边界

`P4` 结束时仍必须保持人工介入的动作：

- live smoke enablement
- signer provider / wallet approval
- chain transaction final broadcast
- reconciliation mismatch 处置
- rollout promotion decision

---

## 5. Persistence Model

### 5.1 Canonical Ledger 原则

- `trading.*` 继续作为 canonical execution ledger
- `runtime.*` 继续作为运行时 / attempt / observation / audit 层
- `meta.signature_audit_logs` 继续作为 signer audit ledger
- `capability.*` 继续作为 capability registry
- 不新建 `live.*`、`signer.*`、`shadow.*` schema

### 5.2 P4 优先复用的现有表

`P4` 必须优先复用以下已存在表，而不是再造并行语义：

- `capability.market_capabilities`
- `capability.account_trading_capabilities`
- `capability.execution_contexts`
- `trading.orders`
- `trading.fills`
- `trading.order_state_transitions`
- `trading.reservations`
- `trading.inventory_positions`
- `trading.exposure_snapshots`
- `trading.reconciliation_results`
- `runtime.journal_events`
- `meta.signature_audit_logs`

### 5.3 P4 允许新增的最小表

如确有必要，`P4` 只允许新增以下最小表：

1. `runtime.submit_attempts`
   - 记录 `submit / cancel / replace` attempt
   - 不能替代 `trading.orders`
2. `runtime.chain_tx_attempts`
   - 记录 approve / split / merge / redeem 等链上 tx attempt
   - 不能替代 on-chain receipt truth
3. `runtime.external_order_observations`
   - 记录 external CLOB order state read
   - 不能替代 `trading.orders`
4. `runtime.external_fill_observations`
   - 记录 external fill/trade read
   - 不能替代 `trading.fills`
5. `runtime.external_balance_observations`
   - 记录 remote balance / allowance observations
   - 不能替代 `trading.inventory_positions`

默认情况下，`P4` 不应再新增其他 canonical ledger。

---

## 6. Canonical P4 Workstreams

### 6.1 Workstream A: Real Data Ingress

目标：

- 把真实市场发现、真实 forecast、真实 watcher RPC read path 收口成 canonical ingress
- 让 `paper_real_data` 成为 `P4` 的默认验证模式

### 6.2 Workstream B: Capability Refresh

目标：

- 把 `MarketCapability / AccountTradingCapability` 从 seeded/local-only 升级为 external-refreshable registry
- 让 `ExecutionContext` 能消费真实刷新后的 capability

### 6.3 Workstream C: Signer Boundary

目标：

- 落地 `asterion_core/signer/`
- 把 official-order-compatible signing 和 transaction signing 都纳入受控 RPC / audit boundary

### 6.4 Workstream D: Submitter / Chain Tx Scaffold

目标：

- 建立 submitter dry-run / shadow path
- 建立 gas / nonce / broadcast scaffold
- 不破坏 `P3` 既有 order / fill / inventory ledger

### 6.5 Workstream E: External Reconciliation / Operator Surface

目标：

- 把 external observations 接入 reconciliation
- 扩 operator / daily ops / readiness surface 到 live prerequisites

### 6.6 Workstream F: P4 Readiness / Closeout

目标：

- 固化 `controlled live rollout decision` 前的 readiness gates
- 固化 closeout / runbook / rollout checklist

---

## 7. Task Breakdown

### P4-01 Real Weather / Gamma Ingress Entry Closure

- **goal**: 把真实 `Gamma` market discovery 与 `OpenMeteo / NWS` forecast refresh 收口成 canonical real-data ingress，确保 `paper_real_data` 可稳定驱动现有 `P3` 链路；明确不包含 watcher real RPC。
- **code landing area**: `asterion_core/clients/`、`domains/weather/scout/`、`domains/weather/forecast/`、`dagster_asterion/resources.py`、`dagster_asterion/job_map.py`、`dagster_asterion/handlers.py`
- **input tables**: `weather.weather_markets`、`weather.weather_market_specs`
- **output tables**: `weather.weather_markets`、`weather.weather_market_specs`、`weather.weather_forecast_runs`、`weather.weather_watch_only_snapshots`
- **contracts consumed**: `WeatherMarket`、`ResolutionSpec`、forecast adapters、watch-only snapshot contract
- **tests required**: HTTP client contract tests、forecast refresh integration tests、cold-path orchestration tests、real-ingress-to-paper smoke tests
- **exit criteria**: 使用真实只读 `Gamma + OpenMeteo / NWS` 源时，能不改 execution contract 地生成 watch-only snapshot，并可继续驱动 `weather_paper_execution`

### P4-02 Capability Refresh From Gamma / CLOB / Chain

- **goal**: 建立 canonical capability refresh，把 `tick_size / fee_rate_bps / min_order_size / tradable / allowance_targets / can_trade` 从外部源同步到 capability ledger。
- **code landing area**: `asterion_core/execution/` 下 capability loader / refresh helper、`dagster_asterion/handlers.py`、`dagster_asterion/job_map.py`
- **input tables**: `capability.market_capabilities`、`capability.account_trading_capabilities`、external Gamma/CLOB public/chain read
- **output tables**: `capability.market_capabilities`、`capability.account_trading_capabilities`
- **contracts consumed**: `MarketCapability`、`AccountTradingCapability`、`ExecutionContext`
- **tests required**: loader unit tests、DuckDB refresh tests、drift-detection tests
- **exit criteria**: capability 不再只能靠 seed data；`ExecutionContext` 能消费外部刷新后的 canonical values

### P4-03 External Wallet State Observation

- **goal**: 形成 remote balance / allowance 的 canonical observation path，给 live inventory gating 与 reconciliation 提供真实输入。
- **code landing area**: 新增 `asterion_core/blockchain/` read-only helpers 或 `asterion_core/execution/` 下 live-state helpers、`dagster_asterion/resources.py`
- **input tables**: `capability.account_trading_capabilities`
- **output tables**: `runtime.external_balance_observations`
- **contracts consumed**: `wallet_id`、`funder`、`signature_type`、`allowance_targets`
- **tests required**: chain-read unit tests、mock RPC integration tests、schema persistence tests
- **exit criteria**: 对每个 live-eligible wallet，系统都能读出 remote balances / allowances，并写入统一 observation ledger

### P4-04 Signer Service Shell And Audit Boundary

- **goal**: 落地 `asterion_core/signer/` 与 signer RPC shell，保证任何签名动作都有 `request_id`、payload hash 与审计落点。
- **code landing area**: 新增 `asterion_core/signer/`、`meta.signature_audit_logs` 相关 persistence helper、`dagster_asterion/handlers.py`
- **input tables**: `capability.account_trading_capabilities`、`meta.signature_audit_logs`
- **output tables**: `meta.signature_audit_logs`、`runtime.journal_events`
- **contracts consumed**: `SigningContext`、`SignerRequest`、`SignerResponse`
- **tests required**: signer request validation tests、audit-log persistence tests、forbidden raw-signing tests
- **exit criteria**: repo 内存在 canonical signer shell，且任何签名调用都无法绕开 audit ledger

### P4-05 Official Order Signing Backend

- **goal**: 把 canonical routed order 接到 official-order-compatible signing backend，禁止 defunct-message 或自定义 JSON signing。
- **code landing area**: `asterion_core/signer/`、`asterion_core/execution/order_router_v1.py`、`asterion_core/execution/signal_to_order_v1.py`
- **input tables**: `runtime.trade_tickets`、`capability.execution_contexts`、`meta.signature_audit_logs`
- **output tables**: `meta.signature_audit_logs`、`runtime.submit_attempts`
- **contracts consumed**: `RoutedCanonicalOrder`、`CanonicalOrderContract`、`SigningContext`
- **tests required**: order-sign payload tests、official-lib compatibility tests、dry-run signing smoke tests
- **exit criteria**: canonical order 能生成 official-compatible signed payload，且默认仍不触发真实 submit

### P4-06 Submitter Dry-Run / Shadow Path

- **goal**: 建立 canonical submitter shell，让 routed order 能以 `dry_run` 或 `shadow_submit` 模式进入外部执行适配层。
- **code landing area**: `asterion_core/execution/` 下新增 live submitter / clob adapter 模块、`dagster_asterion/handlers.py`、`dagster_asterion/job_map.py`
- **input tables**: `trading.orders`、`capability.execution_contexts`、`meta.signature_audit_logs`
- **output tables**: `runtime.submit_attempts`、`runtime.external_order_observations`、`runtime.journal_events`
- **contracts consumed**: `RoutedCanonicalOrder`、`Order`、`ExecutionContext`
- **tests required**: submit payload tests、dry-run integration tests、shadow observation tests
- **exit criteria**: submitter 支持 `dry_run`、`shadow_submit` 两种模式；默认不做真实资金 side effect

### P4-07 Chain Transaction Gas / Nonce / Broadcast Scaffold

- **goal**: 为 approve / split / merge / redeem 等链上动作提供 canonical gas / nonce / signing / broadcast scaffold。
- **code landing area**: 新增 `asterion_core/blockchain/`、`asterion_core/signer/`、`dagster_asterion/resources.py`
- **input tables**: `capability.account_trading_capabilities`、`runtime.external_balance_observations`
- **output tables**: `runtime.chain_tx_attempts`、`meta.signature_audit_logs`、`runtime.journal_events`
- **contracts consumed**: `SigningContext`、transaction request/response contract、Gas Manager design
- **tests required**: gas estimator tests、nonce manager tests、transaction signing tests、mock broadcast tests
- **exit criteria**: approve / redeem 等链上动作拥有统一 attempt ledger 与 audit path，但仍保持 `default-off`

### P4-08 External Execution Reconciliation

- **goal**: 将 external order/fill/balance observations 接入当前 reconciliation，使 `trading.reconciliation_results` 能表达本地账本与外部真实状态之间的差异。
- **code landing area**: `asterion_core/risk/reconciliation_v1.py`、`asterion_core/risk/portfolio_v3.py`、`asterion_core/journal/journal_v3.py`
- **input tables**: `trading.orders`、`trading.fills`、`trading.inventory_positions`、`runtime.external_order_observations`、`runtime.external_fill_observations`、`runtime.external_balance_observations`
- **output tables**: `trading.reconciliation_results`、`runtime.journal_events`
- **contracts consumed**: `Order`、`Fill`、`Reservation`、`InventoryPosition`、`ReconciliationResult`
- **tests required**: mismatch-classification unit tests、DuckDB reconciliation integration tests、rerun stability tests
- **exit criteria**: reconciliation 已不再是 paper-local only，并能稳定区分 local-vs-external mismatch

### P4-09 Operator Live-Prereq Read Model

- **goal**: 在现有 `ui.*` 基础上增加 live-prereq 读面，让 operator 看得到 signer health、submit attempts、external mismatches、wallet readiness。
- **code landing area**: `asterion_core/ui/ui_lite_db.py`、`asterion_core/ui/ui_db_replica.py`
- **input tables**: `runtime.submit_attempts`、`runtime.chain_tx_attempts`、`runtime.external_*`、`trading.reconciliation_results`、`meta.signature_audit_logs`
- **output tables**: 扩展 `ui.execution_*`、新增 `ui.live_prereq_wallet_summary`、新增 `ui.live_prereq_execution_summary`、`ui.phase_readiness_summary`
- **contracts consumed**: existing `ui.*` contract plus runtime observation tables
- **tests required**: UI lite contract tests、DuckDB read-model integration tests、attention-required classification tests
- **exit criteria**: operator 不回查 raw runtime tables，也能判断 live-prereq blockers 与 wallet readiness

### P4-10 Minimum Ops Hardening

- **goal**: 为 signer、submitter、RPC、CLOB public/private path 提供最小可运行的 health / alerting / incident surface。
- **code landing area**: `asterion_core/monitoring/health_monitor_v1.py`、`asterion_core/monitoring/readiness_checker_v1.py`、`ui_lite_db.py`
- **input tables**: `runtime.submit_attempts`、`runtime.chain_tx_attempts`、`runtime.external_*`、`meta.signature_audit_logs`
- **output tables**: readiness JSON / markdown、`ui.phase_readiness_summary`
- **contracts consumed**: health / readiness report contract
- **tests required**: health collector tests、readiness gate tests、missing-surface failure tests
- **exit criteria**: signer/RPC/CLOB failures 能进入 canonical readiness / operator surface，而不是只留在日志中

### P4-11 Controlled Live Smoke Boundary

- **goal**: 明确 `controlled_live_smoke` 的手动放行、wallet 范围、notional 限额、runbook 边界与 env gating。
- **code landing area**: `dagster_asterion/job_map.py`、`dagster_asterion/handlers.py`、`README.md`、future runbook/checklist docs
- **input tables**: `capability.account_trading_capabilities`、`runtime.submit_attempts`、`runtime.chain_tx_attempts`
- **output tables**: `runtime.submit_attempts`、`runtime.chain_tx_attempts`、`runtime.journal_events`
- **contracts consumed**: existing canonical order / signer / chain tx contracts
- **tests required**: env gate tests、default-off tests、operator-approval tests
- **exit criteria**: repo 内存在明确、可审计、默认关闭的 controlled live smoke path

### P4-12 Readiness / Closeout / Controlled Rollout Decision

- **goal**: 固化 `P4` closeout、runbook、checklist 与 `controlled live rollout decision` 的进入条件。
- **code landing area**: `tests/`、`docs/10-implementation/checklists/`、`docs/10-implementation/runbooks/`、`asterion_core/monitoring/readiness_checker_v1.py`
- **input tables**: `ui.live_prereq_*`、`ui.phase_readiness_summary`、`trading.reconciliation_results`
- **output tables**: readiness report、closeout checklist、runbook、navigation docs
- **contracts consumed**: P4 readiness report contract
- **tests required**: closeout doc tests、readiness integration tests、navigation sync tests
- **exit criteria**: `P4` 结束时，结论只能是 `ready for controlled live rollout decision`，不能直接跳成“ready for unattended live”

---

## 8. Canonical Job / Entry Model

### 8.1 继续保留的现有入口

以下入口在 `P4` 继续保留，不得被平行实现替代：

- `weather_spec_sync`
- `weather_forecast_refresh`
- `weather_forecast_replay`
- `weather_paper_execution`
- `weather_watcher_backfill`
- `weather_resolution_reconciliation`

### 8.2 P4 新入口的约束

`P4` 如新增 job，只能新增在现有 `dagster_asterion/job_map.py` 的 cold-path map 内，并满足：

- 有唯一 `job_name`
- 不与 `weather_paper_execution` 平行定义另一套 trade ticket / order contract
- `mode` 必须明确区分 `manual` 与 `scheduled`
- `controlled_live_smoke` 相关 job 默认 `manual + disabled-by-default`

建议新增入口：

- `capability_refresh`
- `wallet_state_refresh`
- `submit_shadow_execution`
- `chain_tx_smoke`
- `live_prereq_readiness`

---

## 9. Test Plan

### 9.1 Unit Tests

必须覆盖：

- signer request / response validation
- official order signing payload normalization
- submitter dry-run / shadow mode gating
- gas / nonce helpers
- external reconciliation classification
- readiness / health gate logic

### 9.2 DuckDB Integration Tests

必须覆盖：

- capability refresh into `capability.*`
- signer audit into `meta.signature_audit_logs`
- submit attempts / chain tx attempts into `runtime.*`
- external observations + reconciliation into `trading.reconciliation_results`
- UI lite live-prereq read models

### 9.3 HTTP / RPC Contract Tests

必须覆盖：

- Gamma discovery client
- Open-Meteo / NWS adapters
- CLOB public/private client wrappers
- Polygon RPC fallback / error classification
- official signing backend compatibility

### 9.4 Shadow / Dry-Run End-to-End Tests

必须覆盖：

- `paper_real_data`
- `sign_only_smoke`
- `shadow_submit`
- `controlled_live_smoke` default-off enforcement

### 9.5 Replay / Regression Tests

必须覆盖：

- rerun row count stability
- latest journal stability
- submit attempt / external observation idempotency
- `P3` execution regression 不回退

### 9.6 Operator / Readiness Tests

必须覆盖：

- `ui.live_prereq_wallet_summary`
- `ui.live_prereq_execution_summary`
- readiness `GO / NO-GO`
- `GO` 文案只能表示 `ready for controlled live rollout decision`

---

## 10. Exit Criteria

`P4` 完成时，必须满足以下条件：

1. real-data ingress 已能稳定驱动 current watch-only / paper chain
2. capability refresh 不再依赖纯 seed data
3. signer service 与 official-order-compatible signing 已落地并进入 audit ledger
4. submitter dry-run / shadow path 已落地，且默认安全边界清晰
5. chain transaction scaffold 已存在，且所有 side effect 都可审计
6. reconciliation 已能消费 external observations
7. operator 能直接看到 wallet readiness、submit attempts、external mismatch、readiness blockers
8. readiness gates 能区分：
   - `not ready`
   - `ready for controlled live rollout decision`

同时必须明确：

- 仍然保持 human-in-the-loop 的能力：
  - wallet approval
  - live smoke enablement
  - chain broadcast approval
  - mismatch disposition
  - rollout promotion
- 仍然不能直接进入 unattended live 的能力：
  - autonomous capital deployment
  - default-on broadcast
  - multi-wallet production rollout
  - agent-driven execution approval

---

## 11. Risks / Open Questions

### 11.1 Official Signing Dependency Risk

- `py-clob-client` 或等价官方兼容路径的能力边界、版本稳定性、Proxy/Safe 支持程度仍需确认

### 11.2 Wallet / Funder / Signature-Type Semantics

- 当前 canonical ledger 已绑定 `funder` 与 `signature_type`
- 真正进入 live prerequisite 后，remote balances / allowances / order ownership 如何和这两个字段一一对应，仍需在实现中收紧

### 11.3 External Source Reliability

- Gamma / weather / RPC / CLOB public/private API 的限流、故障与字段漂移将直接影响 capability refresh 和 live readiness

### 11.4 Default-Safe Boundary Risk

- 若 `controlled_live_smoke` 的 env gate、operator approval、task routing 不够清晰，最容易把 `P4` 误推进成“半自动 live”

### 11.5 Reconciliation Semantics Upgrade Risk

- 当前 `trading.reconciliation_results` 在 `P3` 中是 paper-local deterministic reconciliation
- `P4` 把它升级为 external reconciliation 时，必须避免破坏现有 `P3` rerun 稳定性和 operator 读面

---

## 12. Current Planning Conclusion

截至 `2026-03-11`，当前仓库已经满足进入 `P4` 的前提：

- `P3` 已关闭
- canonical execution contracts 已冻结并落地
- operator / readiness / closeout / runbook 已存在

因此 `P4` 的唯一正确起点不是“重新设计 execution”，而是：

1. 继续复用 `P3` execution chain
2. 先补真实外部只读数据与 capability refresh
3. 再补 signer / submitter / chain tx scaffold
4. 最后补 external reconciliation、operator surface、readiness / closeout

`P4` 的成功标准不是“开始 live”，而是“具备可审计、可回滚、默认安全的 controlled live rollout decision 基础”。
