# Asterion 项目全量评估报告

作者视角：顶级技术负责人 / 架构评审人 / 量化交易基础设施顾问 / 安全审计 reviewer  
评估基准：**以真实代码、migrations、tests、runbooks 为准**，文档仅作为辅助上下文。  
结论口径：报告中明确区分 **代码事实**、**测试事实**、**文档事实** 与 **我的判断/推断**。

---

## 1. Executive Summary

### 总判断

Asterion **不是一个“只有 PPT/README 的 demo”**。仓库里已经有相当扎实的基础设施骨架：

- 明确的 canonical contracts（`asterion_core/contracts/*`）
- 分层较好的持久化设计（`trading.* / runtime.* / meta.* / agent.* / ui.* / weather.* / resolution.*`）
- 真正可运行的 paper execution 链路与 ledger 持久化（`tests/test_execution_foundation.py` 通过）
- replay / forecast diff / audit / journal / gate decision / submit attempt / external observation 等可审计对象
- UI replica + UI lite read model 分离
- 单写者（writerd / write queue）思路
- controlled live smoke 的人工边界、审批 token、allowlist、amount cap、journal 与 raw payload scrubbing

但是，这个项目**还不应被视为“已经可以做真正的 controlled live rollout 决策”**。更准确的判断是：

> 它已经到了“**具备受控 live 前置验证/烟雾测试基础设施价值**”的阶段，
> 但还没有到“**可以让 CTO/风险负责人放心签字，把真钱执行链推向受控 live 试点**”的阶段。

### 为什么我不会现在签字放行

最核心的原因不是“代码完全没做完”，而是下面四个更关键的问题：

1. **经济模型太弱**：当前天气链路的 forecast uncertainty、fair value、opportunity ranking 过于粗糙，离“可持续赚钱”差距很大。  
   典型位置：`domains/weather/forecast/adapters.py`、`domains/weather/pricing/engine.py`、`asterion_core/ui/ui_lite_db.py`、`asterion_core/runtime/strategy_engine_v3.py`

2. **“ready for controlled live rollout decision” 的表述被过度前置**：README、UI、项目计划把状态写得很靠前，但 closeout checklist 仍未打勾，且若干关键验证测试当前是坏的。  
   这不是美观问题，而是治理与风控问题。

3. **live boundary 仍然偏“工程约束 + 人工纪律”，而不是“制度化硬边界”**：目前依赖 `.env`、文件策略、手工 arm token、进程内私钥环境变量。  
   对 smoke 足够，对真钱 production 不够。

4. **实际 live submitter 并未落地**：当前有 signer shell、submitter shell、chain tx shell，但 submitter 仍只有 `disabled` / `shadow_stub`，并没有真正的 live order submit backend。  
   换言之，仓库真正到达的是“**受控链上 allowance smoke 边界**”，不是“可交易 submit 边界”。

### 一句话结论

**Asterion 目前最像一个“高质量的、可审计的、面向 operator 的天气事件市场研究/纸交易/受控 live 前置验证平台”，而不是一个已经可持续赚钱、可放心放到真钱生产的自动交易系统。**

---

## 2. Current Project State

### 2.1 已经真实落地了什么

#### 代码事实

1. **canonical contract 层已经存在且较完整**  
   - `asterion_core/contracts/execution.py`：订单、ticket、gate、journal、capability、execution context 等 canonical contract 完整。  
   - `asterion_core/contracts/ids.py`：稳定 deterministic ID。  
   - `asterion_core/contracts/weather.py`：weather market / spec / forecast / fair value / snapshot / resolution 对象存在。

2. **数据库 schema 不是空壳**  
   `sql/migrations/0001_core_meta.sql` 到 `0015_trading_external_reconciliation.sql` 已建立：
   - `meta.*`：ingest、watermark、domain events、signature audit
   - `trading.*`：orders、fills、inventory、reservations、reconciliation
   - `runtime.*`：strategy run、ticket、gate decision、journal、submit attempts、external observations、chain tx attempts
   - `agent.*`：invocation / output / review / evaluation
   - `weather.*`：market、station mapping、spec、forecast、fair value、watch-only snapshot、replay
   - `resolution.*`：UMA proposal / continuity / settlement evidence

3. **paper execution 链路是真实存在的，不是 README 叙述**  
   - `tests/test_execution_foundation.py` 共 21 个测试通过。  
   这证明至少以下链路真实存在：
   - strategy signal → ticket → gate decision → canonical order → paper adapter → order/fill/ledger 持久化
   - runtime 与 trading 表之间的桥接
   - rerun stability / idempotency 的一定程度验证
   - UI 异常/摘要面已接上

4. **weather 冷路径不是空壳**  
   - `domains/weather/scout/market_discovery.py`：真实拉取 Gamma event 数据并筛 weather market。  
   - `domains/weather/spec/rule2spec.py`：title/rule → spec 解析。  
   - `domains/weather/forecast/adapters.py`：Open-Meteo / NWS forecast adapters。  
   - `domains/weather/pricing/engine.py`：由 forecast distribution 生成 binary fair value 和 watch-only snapshot。  
   - `scripts/run_real_weather_chain_smoke.py`：从 discovery → station mapping → forecast → pricing → watch-only snapshot 的真实串联。  
   - `tests/test_real_weather_chain_smoke.py` 9 个测试通过。

5. **controlled live smoke 不是文档概念，而是有代码边界**  
   - `dagster_asterion/handlers.py` 的 `run_weather_controlled_live_smoke_job(...)`：检查 readiness、wallet readiness、arming env、approval token、wallet/tx/spender allowlist、approve cap，最后只允许 `approve_usdc`。  
   - `asterion_core/blockchain/chain_tx_v1.py`：chain tx shell 目前显式只允许 `approve_usdc` 进入当前 phase 的 controlled live。  
   - `tests/test_chain_tx_scaffold.py` 9 个测试通过。

6. **signer / submitter / chain tx shell 是真 scaffold，不是假接口**  
   - `tests/test_signer_shell.py` 14 个测试通过。  
   - `tests/test_submitter_shell.py` 10 个测试通过。  
   - signer 有 audit boundary；chain tx 会 scrub raw tx/private key env var 等敏感字段后再持久化。  
   这部分设计是值得肯定的。

7. **UI 的 read model 分层是有认真设计的**  
   - `asterion_core/ui/ui_db_replica.py`：从 canonical DB 复制到 UI replica。  
   - `asterion_core/ui/ui_lite_db.py`：将 replica 再加工为轻量汇总的 `ui.*` 表。  
   这说明作者不是直接让 Streamlit 去扫 runtime/trading 原表，而是在做 operator-safe read model。

#### 测试事实

我运行并确认通过的代表性测试：

- `tests/test_execution_foundation.py`：通过
- `tests/test_real_weather_chain_smoke.py`：通过
- `tests/test_signer_shell.py`：通过
- `tests/test_submitter_shell.py`：通过
- `tests/test_chain_tx_scaffold.py`：通过

这些通过的测试足以证明：**项目已经具有真实的系统工程价值，不是空仓库。**

### 2.2 尚未真正落地、或只落地到一半的能力

#### 代码事实

1. **没有真正的 live submitter backend**  
   - `dagster_asterion/resources.py` 中 submitter backend 只有 `disabled` 与 `shadow_stub`。  
   - `asterion_core/execution/live_submitter_v1.py` 也只有 dry-run / shadow-submit。

2. **weather forecast uncertainty 明显还是 placeholder 级别**  
   `domains/weather/forecast/adapters.py`：
   - `FORECAST_STD_DEV_1DAY = 3.0`
   - `FORECAST_STD_DEV_3DAY = 4.5`
   - `FORECAST_STD_DEV_7DAY = 6.0`
   然后用简单正态分布构造温度离散概率。  
   这说明现在的 probability model 更接近“工程占位 + smoke 连通性”，不是成熟 alpha 模型。

3. **pricing 和 ranking 仍明显偏启发式**  
   - `domains/weather/pricing/engine.py`：fair value = bucket probability；edge_bps = `(fair_value - reference_price) * 10000`。  
   - `asterion_core/ui/ui_lite_db.py`：`liquidity_proxy`、`confidence_proxy`、`opportunity_score` 都是 UI heuristic。  
   - `asterion_core/runtime/strategy_engine_v3.py`：候选单排序更偏 deterministic operational ordering，而不是 execution-aware EV ranking。

4. **station mapping 高度依赖 override / catalog**  
   - `domains/weather/spec/station_mapper.py` 优先 DB override。  
   - `scripts/run_real_weather_chain_smoke.py` 还依赖 `config/weather_station_smoke_catalog.json`。  
   这更像 smoke/early production scaffold，而不是可扩展的全国级自动映射系统。

5. **UI 认证非常弱**  
   - `ui/auth.py` 默认用户名是 `admin`。  
   - 默认密码 hash 对应明文 `changeme`。  
   这只适合本地开发，不适合任何暴露型 operator console。

### 2.3 文档声称的状态，与代码真实状态的偏差

#### 文档事实

- `README.md` 开头直接写：`P4 closed ... ready for controlled live rollout decision`。
- `AGENTS.md` 把项目定位在 `controlled live rollout decision boundary`。
- `docs/00-overview/Asterion_Project_Plan.md` 也写 `P3 已关闭, P4 已关闭`。
- `docs/10-implementation/phase-plans/P4_Implementation_Plan.md` 标记状态为 `closed`。

#### 与之冲突的文档事实

- `docs/10-implementation/checklists/P3_Closeout_Checklist.md` 顶部写的是 `状态: closeout in progress`，且核查项未完成。  
- `docs/10-implementation/checklists/P4_Closeout_Checklist.md` 顶部写的是 `状态: closeout ready`，不是 closed，而且核查项也未打勾。

#### 测试事实

- `tests/test_controlled_live_smoke.py`、`tests/test_live_prereq_readiness.py`、`tests/test_health_monitor.py` 使用 `from tests.test_p2_closeout import _apply_schema`，但仓库缺少 `tests/__init__.py`，导致 canonical `python -m unittest tests.xxx` 风格命令失效。  
- `tests/test_cold_path_orchestration.py` 中 `_settings()` 没有更新新的 `gamma_tag_slug` 字段，导致 3 个测试失败。

#### 我的判断

**当前项目不适合对外宣称“P4 已经 closed 且 ready for controlled live rollout decision”。**  
更准确的说法应是：

> “P4 的主要 scaffold 已落地，受控 live 决策前置能力大体具备，但 closeout 证据链与测试治理尚未真正闭环。”

### 2.4 是否只是 demo？

**不是 demo。**

但它也**不是成熟可持续盈利系统**。它目前更像：

- 一个扎实的事件市场/weather 垂直基础设施原型
- 一个面向 operator 的研究/审查/纸交易/受控 smoke 平台
- 一个已经把“可审计、可回放、可分层持久化”做得不错的内部系统

### 2.5 当前边界

当前真实边界是：

- 有 watch-only / paper execution / shadow submit / chain tx controlled-live smoke scaffold
- 有 readiness / live-prereq / UI operator read model
- 有 weather market → spec → forecast → fair value → snapshot 的基础链路
- **但没有真正完成受控真钱交易 submit 的闭环，也没有完成足够强的 alpha / execution / calibration 能力**

因此我的状态判断是：

> **“基础设施价值明显高于商业交易价值；受控 smoke readiness 高于真实 live trading readiness。”**

---

## 3. Architecture Review

## 3.1 总体架构是否合理？

### 结论

**总体架构方向是合理的，而且在中小型交易/ops 系统里属于高于平均水平。**  
尤其好的地方在于：

- contract first
- ledger / audit first
- runtime 与 trading 分层
- UI read model 与 canonical DB 隔离
- replay / idempotency / deterministic ID 被作为一等公民处理

但也有几个明显问题：

- 经济逻辑（forecast / pricing / opportunity / actionability）分散在多个层里，边界开始漂移
- scripts 与 Dagster cold path 存在重复编排逻辑
- station mapping / source semantics 存在多处事实源
- controlled-live 是“工程性硬编码边界”，不是“制度化安全边界”

## 3.2 模块划分与分层

### 优点

1. **contracts 层相对干净**  
   `asterion_core/contracts/*` 很好地承担了 canonical schema/contract 的职责。这比把 contract 混在 UI、脚本、Dagster handler 中强很多。

2. **持久化分层是本仓库的亮点**  
   从 migration 可以看出 schema 设计有明确语义：
   - `meta.*`：系统事实、审计、水位
   - `trading.*`：订单/成交/库存/对账
   - `runtime.*`：策略运行、ticket、gate、journal、submit attempt、外部观察
   - `agent.*`：agent runtime 痕迹
   - `ui.*`：只给 operator 的 read model
   - `weather.*`：天气垂域事实
   - `resolution.*`：结算/UMA watcher

   这是“把事实层级分开”的正确做法。

3. **UI/read model 架构是对的**  
   `ui_db_replica -> ui_lite_db -> Streamlit` 这一层次非常合理。  
   好处是：
   - 降低 UI 对 canonical DB 的误写/误锁风险
   - 让 operator 页面读的是稳定汇总，而不是业务原表拼接
   - 为以后接多前端、多 API 层保留扩展空间

4. **Dagster 的 job map 很清晰**  
   `dagster_asterion/job_map.py` 把 scheduled job、manual job、P4 controlled live smoke 等清晰列出。  
   对“冷路径 / 定时编排 / 手工触发边界”是加分项。

### 问题

#### 问题 1：opportunity 相关语义开始在多个层扩散
**风险等级：P1（交易经济风险）**

**受影响文件**
- `domains/weather/pricing/engine.py`
- `asterion_core/ui/ui_lite_db.py`
- `asterion_core/runtime/strategy_engine_v3.py`

**问题描述**

- `pricing.engine` 生成 `edge_bps` 和 BUY/SELL/HOLD 建议
- `ui_lite_db` 再叠加 `liquidity_proxy`、`confidence_proxy`、`actionability_status`、`opportunity_score`
- `strategy_engine_v3` 又用另一套排序规则挑 candidate

也就是说，**“什么算好机会”并没有一个真正稳定的 canonical scoring contract**，而是被分散在 pricing、UI、runtime 三层。

**为什么重要**

这会导致：

- UI 上排名第一的机会，不一定是 runtime 里最先执行的
- pricing 的“边”与 UI 的“机会分”并不统一
- 以后加 fees/slippage/fill probability 时会在多个地方同时改，容易漂移

**修复建议**

- 建立单独的 `OpportunityAssessment` canonical contract，明确区分：
  - model edge
  - executable edge
  - liquidity quality
  - actionability
  - confidence/calibration
  - review status
- runtime 和 UI 都消费同一对象，而不是各自拼 heuristics

**更优替代方案**

把 UI 的 `opportunity_score` 从“主评分”改为“展示层压缩指标”，真实排序一律由 domain 层输出的 execution-aware score 决定。

#### 问题 2：scripts 与 orchestration 层有一定边界重复
**风险等级：P2**

**受影响文件**
- `scripts/run_real_weather_chain_smoke.py`
- `scripts/run_real_weather_chain_loop.py`
- `dagster_asterion/handlers.py`
- `dagster_asterion/job_map.py`

**问题描述**

`run_real_weather_chain_smoke.py` 自己拼 discovery/spec/forecast/pricing/agent/write 的完整链路；这与 Dagster handler 的系统编排存在平行路径。

**为什么重要**

- 同一业务会有两种运维入口
- 行为可能不一致
- 以后很容易发生“脚本能跑、Dagster 不一致”或者“脚本逻辑更新后，job 没更新”的漂移

**修复建议**

- 让脚本只做“thin launcher”，实际调用统一的 service / handler 层
- 对脚本保留 smoke-specific 配置，但不再复制主业务逻辑

## 3.3 canonical contract 是否稳定？

### 结论

**相对稳定，且这是项目最值得保留的骨架之一。**

`asterion_core/contracts/execution.py` 的 contract 边界清晰度，足以支撑后续重构。  
`stable_object_id(...)` 也为 replay / idempotency / deterministic write 提供了很好的基础。

### 但有两个注意点

1. weather 机会语义还未 canonical 化（见上）  
2. station mapping / source authority 的“事实源”还不够单一

## 3.4 persistence 分层是否合理？

### 结论

**合理，而且是本项目最强设计之一。**

尤其下面这组分层很好：

- `trading.*`：真实交易事实
- `runtime.*`：策略运行与执行过程事实
- `meta.*`：系统级事实/审计/水位
- `agent.*`：agent 运行痕迹
- `ui.*`：只读展示事实

这种分层很符合“单一事实来源 + 下游派生”的思路。

### 亮点

- `runtime.trade_tickets` 与 `trading.orders` 分离：好于把 signal/order 混在一张表
- `runtime.submit_attempts`、`runtime.chain_tx_attempts`、`runtime.external_*_observations`：说明作者已经考虑到了“本地状态”和“外部观察”之间的差异
- `trading.reconciliation_results` 在 `0015_trading_external_reconciliation.sql` 中扩展了外部对账字段，方向正确

### 风险

#### 问题 3：writerd / 单写者约束是“良好约定”，不是强隔离
**风险等级：P2**

**受影响文件**
- `asterion_core/storage/database.py`
- `asterion_core/storage/write_queue.py`
- `asterion_core/storage/writerd.py`

**问题描述**

`connect_duckdb(...)` 会根据 env 约束 reader/writer 模式，并在 reader 连接上对 SQL token 做禁写校验；`writerd` 也有 allowlist。  
这很实用，但它的安全性建立在“所有代码都走这些 wrapper”的前提上。

**为什么重要**

任何绕过 wrapper 直接 `import duckdb` 的代码，都可能直接写 canonical DB。  
因此这不是“制度化不可绕过边界”，而是“代码纪律边界”。

**修复建议**

- 把 canonical DB 文件权限/进程角色隔离做得更硬（例如 writer 进程独占目录写权限）
- UI / reader 进程只挂载 replica，不给 canonical DB 路径
- 在 CI 中 grep/禁止直接 `duckdb.connect(...)` 的野写用法

## 3.5 weather 域模型是否合理？

### 总体判断

**域对象拆分是合理的，策略模型本身还不够强。**

合理之处：
- `weather_markets`：市场发现事实
- `weather_market_specs` / `resolution_specs`：规则与结算解释层
- `weather_forecast_runs` / `replays` / `diffs`：预测与 replay
- `weather_fair_values`：定价输出
- `weather_watch_only_snapshots`：操作层 snapshot

这套分层本身没问题。

### 核心缺陷

#### 问题 4：rule2spec 和 station mapping 过度依赖 title heuristics 与人工 override
**风险等级：P1（策略正确性 / 扩展性）**

**受影响文件**
- `domains/weather/spec/rule2spec.py`
- `domains/weather/spec/station_mapper.py`
- `scripts/run_real_weather_chain_smoke.py`
- `config/weather_station_smoke_catalog.json`

**问题描述**

- `rule2spec.py` 主要靠 regex/title 模板解析温度范围、阈值、单位、location/date/source
- `station_mapper.py` 主要靠 DB mapping 和 override
- smoke 脚本还依赖本地 `weather_station_smoke_catalog.json`

**为什么重要**

这会造成：

- 某些市场解析失败或解析错位
- 同城/同机场/同测站不一致时，fair value 直接失真
- 市场覆盖能力高度受人工 catalog 限制

**修复建议**

- 构建 location normalization + geocoding + nearest authoritative station candidate pipeline
- 增加 `mapping_confidence`、`mapping_method`、`override_reason` 的落表
- 建一个回归测试语料库，覆盖真实市场 title 模板

**更优替代方案**

让 `rule2spec` 输出的是“解析候选 + 置信度 + 需要人工确认的字段”，再通过 station resolution service 决定最终 spec，而不是一个步骤里做完所有决定。

## 3.6 UI/read model 架构是否合理？

### 结论

**架构合理，但当前 UI 更偏 readiness / ops，可用于决策支持，但还不是真正围绕 PnL 最大化设计。**

### 好的地方

- UI 与 canonical DB 隔离
- 有 `ui.live_prereq_wallet_summary`、`ui.market_opportunity_summary` 等 operator summary 表
- `ui/app.py` 明确强调 manual-only / default-off / approve-usdc-only，不鼓励误用为 unattended live

### 主要问题

#### 问题 5：UI 机会视图仍是“启发式混合摘要”，不是 execution/PnL 中心
**风险等级：P1**

**受影响文件**
- `asterion_core/ui/ui_lite_db.py`
- `ui/app.py`
- `ui/data_access.py`

**问题描述**

当前 `ui.market_opportunity_summary` 的核心分数由：

- edge 档位
- `liquidity_proxy`
- `confidence_proxy`
- accepting/live-prereq/agent 状态

拼成一个 0-100 的 `opportunity_score`。  
这更像“方便 operator 看板”的 UI heuristic，而不是“真实经济最优”的执行排序。

**为什么重要**

如果 operator 依赖这个 score 进行真钱决策，会被误导：

- 没有 spread / fee / slippage / depth / fill probability
- 没有 close time / stale age / forecast freshness / source disagreement
- 没有仓位约束 / 相关性约束 / realized PnL feedback

**修复建议**

UI 层至少要把这几个量拆开显示，而不是压缩成一个总分：

- 模型 fair value
- 可执行 fair value / expected fill price
- 费用与滑点调整后 edge
- forecast freshness
- mapping confidence
- source disagreement
- size cap / inventory impact
- last successful refresh age

#### 问题 6：UI 在 DB 读失败时倾向静默返回空表，容易误导 operator
**风险等级：P2**

**受影响文件**
- `ui/data_access.py`

**问题描述**

`_read_ui_table(...)` 在读取失败时直接返回空 `DataFrame`，类似静默 degrade。

**为什么重要**

对 operator 来说，空数据既可能意味着“当前没有机会”，也可能意味着“UI 读挂了/数据没刷新/DB 路径错了”。  
这两者风险完全不同。

**修复建议**

- 在 UI 层显式区分 `empty_because_no_data` 与 `empty_because_read_error`
- 所有 summary 页面显示 `data_freshness` 与 `last_refresh_error`
- 读失败时顶部显示红色系统 banner，而不是默默空表

---

## 4. Code Quality Review

## 4.1 代码结构、命名、可读性

### 总体评价

**高于一般脚手架项目。**

好的方面：
- core / domains / dagster / ui / scripts / tests 的目录分层清晰
- contract 与 migration 命名较一致
- 很多对象命名有运营语义，例如 `gate_decisions`、`submit_attempts`、`external_fill_observations`、`watch_only_snapshots`
- 文件名与职责基本一致

### 问题

1. `scripts/` 中一些脚本承担了过多 orchestration 责任  
2. `start_asterion.sh` 作为总入口脚本质量明显低于 Python 主体代码  
3. UI/data access 中存在较多“为了界面顺滑而吞错”的逻辑

## 4.2 测试覆盖与测试治理

### 亮点

- execution foundation 有真实覆盖
- signer / submitter / chain tx scaffold 有真实覆盖
- real weather chain smoke 有覆盖
- 测试不只是 unit 级别，也有一些接近 workflow/closeout 的验证

### 主要问题

#### 问题 7：关键 closeout / readiness 测试当前是坏的，说明治理没有闭环
**风险等级：P1**

**受影响文件**
- `tests/test_controlled_live_smoke.py`
- `tests/test_live_prereq_readiness.py`
- `tests/test_health_monitor.py`
- `tests/test_cold_path_orchestration.py`
- 缺失：`tests/__init__.py`
- `dagster_asterion/resources.py`

**问题描述**

- 多个测试依赖 `from tests.test_p2_closeout import _apply_schema`，但 `tests` 目录不是 package。  
- `tests/test_cold_path_orchestration.py` 中 `_settings()` 未更新 `gamma_tag_slug` 字段，导致与 `AsterionColdPathSettings` 当前代码失配。

**为什么重要**

这不是普通小 bug，而是说明：

- 文档声称的 closeout 验证命令并非当前 repo 真可用
- 关键 readiness/health/controlled-live 证明链条并没有被持续验证

**修复建议**

- 给 `tests/` 加 `__init__.py`，或彻底移除 `tests.*` 互相 import 的模式
- 修复 `_settings()` 以匹配当前 `AsterionColdPathSettings`
- 把 P3/P4 closeout checklist 里的验证命令接到 CI
- 未全部通过前，停止在 README/UI 中宣称 `P4 closed`

## 4.3 文档与代码一致性

### 结论

**一致性中等偏差，且偏差集中在“项目状态陈述”而不是“底层技术描述”。**

换句话说：
- 技术方案文档总体方向没大错
- 真正严重的是**状态治理漂移**，容易造成管理层误判

## 4.4 是否有“临时补丁越来越多、复杂度上升”的趋势？

### 结论

**有这个趋势，而且主要集中在 weather/source/mapping/opportunity/UI 层。**

表现为：
- weather source authority 与 forecast adapter 不完全同构
- station mapping 同时存在 DB override 与本地 smoke catalog
- opportunity score 是逐步叠加 heuristic 的产物
- UI 为空数据、agent 状态、readiness 状态都被混入机会总分

这说明系统开始从“干净骨架”向“功能可跑但复杂度上升”演化。  
如果不在现在做一次结构收敛，后面会越来越难改。

---

## 5. Risk / Security / Vulnerability Review

本节只讲“重要且可行动”的问题。

## 5.1 过度宣称 readiness / closeout，可能造成错误放行

**风险等级：P1**

**受影响文件**
- `README.md`
- `AGENTS.md`
- `docs/00-overview/Asterion_Project_Plan.md`
- `docs/10-implementation/phase-plans/P4_Implementation_Plan.md`
- `docs/10-implementation/checklists/P3_Closeout_Checklist.md`
- `docs/10-implementation/checklists/P4_Closeout_Checklist.md`
- `ui/app.py`

**问题描述**

项目根文档、总览和 UI 明确显示 `P4 closed / ready for controlled live rollout decision`，但 closeout checklist 未闭合，关键测试也未全绿。

**为什么重要**

在真实组织中，最危险的不是“代码里有个 TODO”，而是**状态被错误地宣告为通过**。  
这会让：
- CTO 误判风险已清零
- operator 误以为系统已经进入可真钱试点阶段
- 风险审计无法信任 repo 自我描述

**修复建议**

- 立即把所有对外状态统一改为：`P4 scaffold landed; closeout pending objective verification`
- 只有在 closeout checklist 和 CI 绿色后，才恢复 `closed`
- UI 顶部状态改为从 objective verification artifact 自动生成，而不是硬编码

## 5.2 controlled live 边界过度依赖 env / 文件策略 / 进程内私钥

**风险等级：P1**

**受影响文件**
- `dagster_asterion/handlers.py`
- `asterion_core/signer/signer_service_v1.py`
- `asterion_core/blockchain/chain_tx_v1.py`
- `config/controlled_live_smoke.json`
- `start_asterion.sh`

**问题描述**

目前 controlled live smoke 的关键控制点包括：
- `ASTERION_CONTROLLED_LIVE_SMOKE_ARMED=true`
- approval token env
- repo 中 JSON policy file
- `private_key_env_var` 指向环境变量中的私钥

这对 smoke 足够，对真钱生产边界不够。

**为什么重要**

如果：
- 操作机 shell 被拿到
- `.env` 被泄露
- UI/脚本/agent 进程共享同一个 env
- repo policy file 被意外改动

那么当前边界没有 KMS/HSM/审批服务那种“制度性硬隔离”。

**现状风险**

- 不是立即远程可利用的“漏洞利用链”
- 但属于典型的**生产安全模型过软**
- 对小团队本地 smoke 可接受，对真钱 production 不可接受

**修复建议**

1. 私钥移出进程环境变量，接到 KMS/HSM/远程 signer
2. approval token 改成一次性、可审计、短时效的签名审批对象，而不是静态 env token
3. policy 从 repo 文件移到只读配置服务/密封对象存储
4. UI / reader 进程永不注入 signing secrets

**更优替代方案**

做一个独立的 `approval service + signer service`：
- UI/operator 只能提交审批请求
- signer 在隔离进程/主机中执行
- canonical journal 只记录摘要，不暴露原始签名材料

## 5.3 UI 默认认证过弱

**风险等级：P1（若 UI 暴露到网络则更高）**

**受影响文件**
- `ui/auth.py`

**问题描述**

默认用户名 `admin`，默认密码 hash 对应 `changeme`。

**为什么重要**

如果有人把 Streamlit UI 暴露在内网/公网，这几乎等于没有认证。  
而且 UI 有 operator 语义，可能让使用者以为这是“受保护的控制台”。

**修复建议**

- 移除默认凭证，若未配置则直接拒绝启动 UI
- 接入 SSO / OAuth / 反向代理认证
- 至少加 rate limit、session timeout、审计日志

## 5.4 UI / web 进程存在不必要的 secret 暴露面

**风险等级：P2**

**受影响文件**
- `start_asterion.sh`
- `ui/data_access.py`
- 以及任何复用项目 `.env` 的前端/UI 进程

**问题描述**

`start_asterion.sh` 会 source 项目 `.env`；`ui/data_access.py` 也会尝试加载项目 `.env`。  
如果 `.env` 中同时放了链上私钥、审批 token、RPC 凭证，那么 read-only UI 进程会继承它们。

**为什么重要**

这是典型的**最小权限原则失效**。  
UI 不应该接触签名或 live boundary 相关密钥。

**修复建议**

- 将环境配置按角色分开：`ui.env` / `runtime.env` / `writerd.env` / `signer.env`
- UI 只拿 UI 所需的 DB 路径与展示配置
- 任何 live secret 不进入 Streamlit 进程

## 5.5 runtime / execution 边界并非不可绕过

**风险等级：P2**

**受影响文件**
- `dagster_asterion/resources.py`
- `asterion_core/signer/signer_service_v1.py`
- `asterion_core/blockchain/chain_tx_v1.py`
- `asterion_core/storage/database.py`

**问题描述**

很多安全约束存在于“推荐调用路径”里，而不是进程/系统级隔离。  
例如：
- controlled-live 守卫主要在 handler 中
- reader/writer 守卫主要在 DB wrapper 中

**为什么重要**

对熟悉代码的开发者来说，直接实例化服务、绕过 handler / wrapper 并不难。  
这说明当前安全边界更像“代码架构上的边界”，而不是“系统级边界”。

**修复建议**

- 把 signer / chain tx / writerd 放到独立进程甚至独立主机
- 用进程身份和操作系统权限限制 DB/secret 访问
- 将关键动作走 RPC/IPC，而不是同进程 import service

## 5.6 write queue / writerd 适合单机原型，不适合高可靠生产

**风险等级：P2**

**受影响文件**
- `asterion_core/storage/write_queue.py`
- `asterion_core/storage/writerd.py`

**问题描述**

当前 write queue 基于 SQLite，本质上是单机任务队列。  
对当前阶段足够，但在：
- 多进程
- 多主机
- 更高吞吐
- 更严格 HA

场景下会变成瓶颈与 SPOF。

**为什么重要**

对于交易基础设施，写路径可靠性是核心；一旦 writer 卡死或队列文件损坏，会影响整个系统事实层。

**修复建议**

- 近期：继续保留 SQLite，但加更强的监控、备份、健康检查、队列积压报警
- 中期：迁移到更明确的 durable queue / service boundary
- 长期：writer service 从应用代码库中拆分成独立组件

## 5.7 replay / idempotency 在本地与 smoke 层面不错，但真钱路径尚未被真正证明

**风险等级：P2**

**受影响文件**
- `asterion_core/contracts/ids.py`
- `tests/test_execution_foundation.py`
- `runtime.submit_attempts` / `runtime.chain_tx_attempts` 相关代码

**问题描述**

deterministic ID、paper execution rerun stability 等都做得不错。  
但真实 live submit 尚未存在，因此“外部系统错误重试 / 网络抖动 / 部分成功 / 幂等重放”的 hardest part 其实还没被实际打穿。

**修复建议**

- 在 real submitter 引入前，先把幂等键、external client order id、duplicate suppression 设计完整
- 对外部提交做 fault injection 测试，而不是只测 shadow path

## 5.8 远程 API 依赖存在单点脆弱性

**风险等级：P2**

**受影响文件**
- `domains/weather/scout/market_discovery.py`
- `domains/weather/forecast/adapters.py`
- `scripts/run_real_weather_chain_smoke.py`
- 链上/RPC 相关代码

**问题描述**

系统依赖：
- Gamma / event API
- Open-Meteo / NWS
- Polygon RPC / Web3
- 可能的 CLOB 客户端

虽然已有部分 fallback（例如 forecast router、smoke script 的页面 fallback），但整体上还是典型的第三方 API 依赖链。

**修复建议**

- 建 source health cache 与 source-specific degradation policy
- 记录 source freshness / error rate / stale reason
- UI 上显式显示“当前机会是否基于 degraded source”

---

## 6. Technical Debt and Weaknesses

## 6.1 最明显的缺陷

### 缺陷 1：策略经济学远弱于基础设施工程

**风险等级：P0（资本损失风险，不是软件 exploit）**

**受影响文件**
- `domains/weather/forecast/adapters.py`
- `domains/weather/pricing/engine.py`
- `asterion_core/ui/ui_lite_db.py`
- `asterion_core/runtime/strategy_engine_v3.py`
- `scripts/run_real_weather_chain_smoke.py`

**问题描述**

当前策略链路大致是：

1. market discovery
2. rule2spec
3. station mapping
4. 用单源 forecast point estimate + 固定 sigma 构造概率分布
5. 由 bucket probability 得出 fair value
6. 与 reference price 相比得到 edge_bps
7. 用 heuristic score 排序

这里面最弱的是第 4-7 步。

**为什么重要**

如果把真钱放进去，最大的损失来源不是代码 crash，而是：
- 概率错
- 价格边错
- 机会排序错
- fill 假设错

**修复建议**

详见第 7 节与第 9 节，但核心只有一句：

> **优先补“校准后的概率 + 可执行 edge”，不要继续堆 agent/UI heuristic。**

### 缺陷 2：项目状态治理漂移

**风险等级：P1**

这在前面已展开，不赘述。它会直接影响组织决策质量。

### 缺陷 3：start 脚本质量明显拖后腿

**风险等级：P2**

**受影响文件**
- `start_asterion.sh`

**问题描述**

- `PROJECT_DIR="/Users/jayzhu/web3/Asterion"` 等路径是硬编码
- `--paper` 分支并不启动 paper execution service，而只是打印 paper functions

**为什么重要**

这是典型“运维入口漂移”：名字看似代表系统能力，实际行为不是。  
会让 operator 形成错误心理模型。

**修复建议**

- 去掉硬编码路径，改为基于脚本所在目录自发现
- `--paper` 要么真启动 paper daemon/job，要么重命名为 `--inspect-paper-adapter`
- 所有模式加 config validation 和 fail-fast

## 6.2 会阻碍未来扩展的问题

1. station mapping 高度人工化，市场覆盖会卡死  
2. opportunity ranking 不可解释、不可统一复用  
3. live boundary 过于依赖单进程 env，难以扩展到团队协作与受审计生产  
4. UI 还没有变成真正的 operator decision console，而更像 readiness/status console  
5. 测试和 closeout 状态没有自动绑定，规模一大就会文档漂移

## 6.3 哪些地方“测试虽然通过，但实际生产风险仍高”

1. `test_signer_shell.py` 通过，不代表 env private key 方案适合生产  
2. `test_submitter_shell.py` 通过，不代表已经具备 live order submit 能力  
3. `test_chain_tx_scaffold.py` 通过，不代表具备真钱多种 tx kind 的链上运行安全性  
4. `test_real_weather_chain_smoke.py` 通过，不代表 weather alpha 已经成立  
5. `test_execution_foundation.py` 通过，不代表 execution economics 已经正确

## 6.4 哪些地方“能跑但不够稳”

- real weather chain smoke：能跑，但 market coverage 和 station mapping 不稳
- UI opportunity summary：能用，但会误导真钱排序
- readiness GO：能产出，但不足以构成真正 rollout sign-off
- writerd：能工作，但仍是单机单写者模式

---

## 7. Trading Strategy and Opportunity Assessment

## 7.1 当前天气市场策略链路是否合理？

### 结论

**链路方向合理，策略强度不足。**

方向合理，是因为它遵循了一个正确框架：
- 先发现市场
- 再提取规则/spec
- 再做 forecast
- 再定价
- 再形成 watch-only / actionability

这比“直接让 agent 看标题给买卖建议”高级得多。

但当前的硬伤是：

- forecast uncertainty 过于简化
- fair value 过于理想化
- opportunity score 与 execution reality 脱节
- 没有足够重视市场微观结构

## 7.2 market discovery / rule2spec / forecast / pricing / opportunity 是否足够强？

### 市场筛选逻辑

**评价：中等，足以做 smoke，不足以做高质量 alpha harvesting。**

**代码事实**
- `domains/weather/scout/market_discovery.py` 通过 Gamma events 分页、title/tag/category/slug 等 heuristics 找 weather market
- `scripts/run_real_weather_chain_smoke.py` 中目标市场的挑选更偏“可映射、可支持、近期收盘”的 operational 策略

**问题**
- 缺少真正的 liquidity / spread / depth / tradability screen
- 缺少按 forecast horizon、market quality、历史可执行性做优先级
- 可能会把很多“有 edge 但不可做”与“可做但 edge 假的”混在一起

### rule2spec 逻辑

**评价：对 smoke 有用，对 production 太脆。**

**问题**
- 模板/regex 驱动，容易被新标题模板打穿
- threshold 市场的边界处理容易出现假精确
- authoritative source 的语义与实际 forecast adapter 语义有时不一致

### forecast 逻辑

**评价：当前是全链最值得重做的一环。**

**代码事实**
- `domains/weather/forecast/adapters.py` 用固定 sigma 生成正态离散分布
- Open-Meteo / NWS 实际只取有限的点值信息

**问题**
- 没有基于站点、季节、地理、预报时距、来源、市场类型做误差校准
- 没有 ensemble / disagreement / historical residual model
- 没有 resolution-aware rounding uncertainty

### fair value 计算

**评价：数学上干净，交易上过弱。**

**代码事实**
- `domains/weather/pricing/engine.py` 将 bucket probability 直接作为 fair value

**问题**
- 忽略 fee / spread / slippage / queue position / fill odds
- 忽略 source freshness 与 source disagreement
- 忽略 mapping confidence 和 spec parse confidence
- 忽略 close-time 临近时 forecast jump risk

### opportunity 排序

**评价：当前失真较大。**

**代码事实**
- UI summary 的 `opportunity_score` 混合了 edge/liquidity_proxy/confidence_proxy/live status/agent status
- strategy engine 候选排序主要是 deterministic operational ordering

**问题**
- 真正重要的 expected value 没被显式建模
- actionability 与 profitability 没分开
- confidence proxy 被 agent status 和 review success 显著影响，过于 UI/流程导向

## 7.3 有没有明显 alpha 流失点？

### 有，而且很明显

1. **station mapping 错位 / 置信度不足**  
   一个站点错了，fair value 直接歪。

2. **forecast 分布过于粗糙**  
   用固定 sigma 的正态，会把很多边缘 bucket 概率估错，尤其临界温度市场。

3. **忽略 execution frictions**  
   即使 fair value 对了，实际 fill 价格、深度、手续费也会把 edge 吃掉。

4. **机会排序没有真实 EV 优先**  
   好机会可能排后面，差机会因为 UI proxy 高而排前面。

5. **没有 post-trade feedback loop**  
   看不到“预测 edge”与“实际实现 PnL”之间的偏差校准。

## 7.4 当前策略是否真正有赚钱潜力？

### 我的判断

**有理论上的赚钱潜力，但当前实现离“可持续赚钱”仍然很远。**

原因不是 weather 市场本身没机会，而是当前系统没有把机会转化为可靠执行优势：

- 模型 edge 还不够可信
- execution-aware filtering 太弱
- 机会排序失真
- 缺少 realized feedback

如果现在就追求真钱自动化，亏损概率很高。

## 7.5 如果要提高盈利能力，最应该优化哪些环节？

### 排名 1：forecast uncertainty calibration

**这是最值得投资源的地方。**

建议：
- 按 station / source / horizon / season / market type 统计历史 forecast residual
- 用经验分布或 mixture，而不是固定 sigma 正态
- 给每个 fair value 输出 calibration band

### 排名 2：fair value → executable edge

建议把当前：

`edge = fair_value - reference_price`

改成：

`executable_edge = fair_value - expected_fill_price - fees - slippage - adverse_selection_penalty`

### 排名 3：opportunity ranking 重构

引入分离的几个分数：
- `model_confidence_score`
- `actionability_score`
- `execution_quality_score`
- `expected_value_score`

最终排序以 `expected_value_score` 为主，而不是 UI heuristic 总分。

### 排名 4：market quality / liquidity screen

在最前面就过滤掉：
- 深度太差
- spread 太宽
- price/staleness 异常
- close time 与 forecast freshness 不匹配

### 排名 5：post-trade analytics / calibration loop

必须把：
- predicted edge
- expected fill
- realized fill
- realized PnL
- post-resolution Brier / log-loss / calibration

做闭环。不然系统永远不知道自己哪里在赚钱、哪里在自欺欺人。

## 7.6 agent review 在交易链中的实际价值

### 结论

**当前 agent 不是噱头，但它的价值主要在 review / QA / explanation / exception triage，不在 alpha 核心。**

这是合适的使用边界。

好的方面：
- `agent.*` 持久化结构存在
- UI summary 中 agent 状态被纳入 operator 视图
- smoke 脚本支持 agent 参与 review

不好的方面：
- 当前如果把 `agent_review_status` 过度混入 `opportunity_score`，会高估 agent 的真实经济价值

我的判断是：

> **Asterion 目前最好的 agent 用法，是“辅助 operator 判断是否需要复核/解释/例外处理”，而不是“直接给出买卖决策并主导排序”。**

---

## 8. Commercial Viability Assessment

## 8.1 当前项目更适合什么定位？

按现实程度排序，我认为更适合：

1. **operator console / trading infra**
2. **signal / research platform（聚焦 weather/event market intelligence）**
3. **AI review / agent-assisted trading ops 工具**
4. **自营交易系统（仅限半自动、研究驱动阶段）**
5. **完全自动化真钱交易系统（当前最不现实）**

## 8.2 当前最现实的变现路径

### 路径 A：先做内部自营半自动 weather/event 市场交易台

Asterion 当前最强的是：
- 审计
- replay
- readiness
- watch-only / paper execution
- operator UI

这非常适合作为**内部交易台工具**，让人和系统协同，而不是全自动放行。

### 路径 B：做 weather intelligence / event market research 产品

把：
- 市场发现
- spec 解析
- forecast/fair value
- source provenance
- replay diff
- operator review

包装成研究/情报产品，可能比直接卖“自动赚钱策略”更现实。

### 路径 C：做 AI-assisted trading ops / audit console

Asterion 的 differentiation 不在 agent 本身，而在：
- agent 有审计轨迹
- decisions 与 runtime/trading 事实能对齐
- controlled-live 有边界与 runbook

这对小型交易团队是有产品价值的。

## 8.3 当前离稳定赚钱还差什么？

1. 足够强、可校准的 forecast probability
2. execution-aware pricing 与 ranking
3. 实盘反馈闭环与策略监控
4. 更稳健的 market coverage / station mapping
5. 真正受控且可审计的 live submit/approval boundary

## 8.4 哪些功能足以支持早期验证？

已经足够支持：
- watch-only alpha 验证
- paper trading 验证
- 人工复核式的小规模半自动试验
- weather chain 的端到端 smoke
- operator 控制台原型

## 8.5 哪些关键能力明显不足？

- 预测分布质量
- 流动性与执行成本建模
- 真正 live submit 能力
- UI 的盈利导向设计
- 生产级安全与 secret boundary

---

## 9. Recommended Architecture / Product / Strategy Improvements

## 9.1 更好的架构方式

### 建议 1：把“机会评估”提炼为独立 domain 层

**为什么更好**

现在 opportunity 语义散落在 pricing/UI/runtime。  
抽成单独 domain 可以：
- 保持 SSOT
- 让 UI 和 execution 使用同一事实源
- 降低 heuristic 漂移

**落地方式**

新增类似：
- `asterion_core/contracts/opportunity.py`
- `domains/weather/opportunity/service.py`

输出对象至少包含：
- `model_fair_value`
- `execution_adjusted_fair_value`
- `reference_price`
- `expected_fill_price`
- `fees_bps`
- `slippage_bps`
- `edge_bps_model`
- `edge_bps_executable`
- `confidence_score`
- `actionability_score`
- `ranking_score`
- `rationale`

### 建议 2：把 signer / chain-tx / writerd 进一步进程隔离

**为什么更好**

这会把当前“代码边界”升级为“系统边界”。

**落地方式**

- signer service 独立进程/主机，只接受受控请求
- chain tx service 独立进程，按 wallet/tx kind 策略校验
- writerd 独立部署，应用只写 queue

## 9.2 更好的 UI/read model 组织方式

### 建议 3：把 UI 从“状态中心”升级成“决策中心”

**为什么更好**

当前 UI 主要回答：
- 系统准备好了吗？
- agent review 过了吗？
- 有没有机会？

它还没有很好回答：
- 这笔为什么赚钱？
- 真实可执行 edge 多大？
- 风险来自哪里？
- 不做这笔的机会成本是什么？

**落地方式**

新增几个 operator 核心 panel：
- Edge decomposition（模型边、执行边、费率、滑点、深度）
- Confidence decomposition（forecast calibration、station mapping、source disagreement、parse confidence）
- Realized vs Predicted dashboard
- Staleness / freshness panel
- Portfolio concentration / event risk panel

## 9.3 更好的 opportunity ranking 方式

### 建议 4：放弃单一 heuristic `opportunity_score`，改成分层评分

**为什么更好**

单一分数会混淆盈利性、可执行性、流程就绪性。  
这些应是不同维度。

**落地方式**

建议至少拆成：
- `profitability_score`
- `execution_score`
- `confidence_score`
- `ops_readiness_score`
- `overall_rank_score`（加权但可解释）

其中 `overall_rank_score` 权重不应由 UI 定义，而由策略/执行域定义。

## 9.4 更好的策略优化方式

### 建议 5：建立历史校准数据集

**为什么更好**

没有历史 forecast residual / resolution outcome / realized fill 数据，所有概率与 score 都停留在“看起来合理”。

**落地方式**

落一张或一组校准表：
- station × source × horizon × season × market template
- forecast point estimate / distribution snapshot
- final observed resolution value
- pre-trade market price / fill price
- realized PnL

然后用它来：
- 校准 sigma 或非参数分布
- 学 expected fill model
- 学 opportunity ranking

### 建议 6：引入 execution-aware threshold，而不是固定 300bps

**为什么更好**

`scripts/run_real_weather_chain_smoke.py` 的 `TARGET_THRESHOLD_BPS = 300` 太粗。  
不同市场深度、临近收盘程度、置信度不同，阈值不应一致。

**落地方式**

阈值改为函数：

`required_edge_bps = fees + slippage + uncertainty_penalty + liquidity_penalty + buffer`

## 9.5 更好的 agent 使用边界

### 建议 7：让 agent 专注 exception review，不参与一阶定价

**为什么更好**

agent 在当前系统最擅长的是：
- 解释 spec/market 异常
- 标记 source/mapping 冲突
- 给 operator 提供审核建议

不擅长：
- 提供可靠数值 alpha
- 决定最终执行优先级

**落地方式**

把 agent 限制在：
- 解析置信度低的市场
- source disagreement 高的市场
- readiness/health 异常诊断
- post-trade review

## 9.6 更好的 replay / readiness / controlled live 设计

### 建议 8：把 readiness 从“GO/NO-GO 状态”升级成“证据包”

**为什么更好**

当前 `GO` 太容易被理解成“可以真钱上”。  
更好的方式是让 operator/CTO 看到结构化证据：
- 哪些门通过了
- 哪些是 shadow-only
- 哪些还只是 smoke
- 上次成功时间与依赖 freshness

**落地方式**

输出 machine-readable readiness bundle：
- gate result
- evidence artifact path
- test pass SHA / timestamp
- stale/unknown dependencies
- explicit capability boundary（e.g. `chain_tx: approve_usdc_only`, `submitter: shadow_only`）

## 9.7 更好的测试策略

### 建议 9：让 closeout checklist 由 CI 自动生成

**为什么更好**

这能根治当前“README 写 closed，但 checklist 未闭环”的问题。

**落地方式**

- closeout checklist 只引用自动生成 artifact
- P3/P4 状态以 CI 结果和 release artifact 为准
- 阻止手工把 README/UI 状态写在前面

## 9.8 更好的文档与工程治理

### 建议 10：建立“状态真相源”

**为什么更好**

状态漂移本质上是没有 SSOT。

**落地方式**

- 定义 `docs/status/current_release_status.json`
- 由 CI 更新
- README/UI/Runbook 都从它生成或引用

---

## 10. Prioritized Action Plan

## 10.1 如果我是 CTO，接下来最应该做的 10 件事

1. **立即撤回“P4 closed”类状态宣称，直到 closeout 证据链为真**  
2. **修复测试治理**：`tests/__init__.py` / 互相 import / `gamma_tag_slug` 漂移 / 把 closeout 命令接入 CI  
3. **重做 weather forecast uncertainty calibration**，建立历史误差与 resolution 数据集  
4. **把 fair value 升级为 executable edge**，引入 fee/spread/slippage/fill probability/depth  
5. **重构 opportunity model**，把 ranking 从 UI heuristic 提升为 domain-level canonical object  
6. **把 UI 改造成 PnL/decision 中心**，而不是 readiness/status 中心  
7. **把 private key / approval token 从通用 `.env` 中移走**，引入最小权限与独立 signer  
8. **明确 de-scope 或实现 real submitter**，不要再让 readiness 叙事领先实际能力  
9. **收敛 weather station mapping 与 source authority**，建立自动映射与置信度体系  
10. **清理运维入口**：重写 `start_asterion.sh`，减少平行路径与假语义开关

## 10.2 按优先级的 5 个最高优先修复

### 优先级 1：修正项目状态与验证链
- 修复关键测试
- checklist 接 CI
- README/UI 不再硬写 `P4 closed`

### 优先级 2：重构 forecast → pricing → ranking
- 历史校准
- execution-aware edge
- canonical opportunity object

### 优先级 3：硬化 live boundary 与 secrets
- 移除 env private key 常规路径
- 分角色 env
- signer 隔离

### 优先级 4：修复 station mapping / market coverage
- 自动映射
- 置信度
- regression corpus

### 优先级 5：重做 operator UI
- 从 readiness console 转向 decision console
- 增加 predicted vs realized / staleness / risk decomposition

## 10.3 分阶段实施建议

### Phase 0（1 周内）
- 修测试与文档状态漂移
- 重写 `start_asterion.sh`
- 移除 UI 默认凭证
- 给 UI 加 stale/error banner

### Phase 1（2-4 周）
- 校准 forecast residual
- 重构 opportunity scoring
- 加市场流动性/执行成本 screen
- 增强 station mapping

### Phase 2（4-8 周）
- signer/approval service 隔离
- live boundary capability manifest
- post-trade analytics 与 calibration dashboard

### Phase 3（8 周后）
- 决定是继续做内部半自动交易台，还是外部化为 intelligence / ops 产品
- 若继续真钱交易，再实现 constrained real submitter

---

## 11. Documentation Drift

以下是我认为最明确的文档漂移：

### 漂移 1：P3/P4 “closed” 与 checklist 状态冲突

- `README.md`：写 `P4 closed`
- `docs/00-overview/Asterion_Project_Plan.md`：写 `P3/P4 已关闭`
- `docs/10-implementation/phase-plans/P4_Implementation_Plan.md`：写 `closed`
- 但：
  - `docs/10-implementation/checklists/P3_Closeout_Checklist.md`：`closeout in progress`
  - `docs/10-implementation/checklists/P4_Closeout_Checklist.md`：`closeout ready`
  - 且 checklist 未完成

### 漂移 2：closeout 验证命令与实际测试可运行性冲突

- 文档期待 `python -m unittest tests.xxx` 风格验证
- 仓库当前因为 `tests` 不是 package、互相 import 方式不稳，导致关键测试不能按该口径稳定通过

### 漂移 3：migrations README 已过时

- `sql/migrations/README.md` 仍提到只有 `0001`–`0005`
- 实际 repo 已到 `0015`

### 漂移 4：`start_asterion.sh --paper` 名称与行为不一致

- 名称像是“启动 paper 交易”
- 实际只是打印 paper adapter functions

### 漂移 5：UI 状态文案过强

- `ui/app.py` 仍把项目展示为 `P4 closed / Ready for controlled live rollout decision`
- 这没有绑定 objective verification artifact

### 漂移 6：authority source 与实际 forecast adapter 语义存在混淆

- `rule2spec` 可以把 `weather.com` 作为 authority/source 语义的一部分
- 但当前真实 adapter 主要是 NWS / Open-Meteo
- 对 operator 来说，resolution authority 与 forecast source 需要更清晰分离

---

## 12. Appendix: Code Areas Reviewed

### 12.1 文档/Runbook

- `README.md`
- `AGENTS.md`
- `docs/00-overview/Documentation_Index.md`
- `docs/00-overview/Asterion_Project_Plan.md`
- `docs/00-overview/DEVELOPMENT_ROADMAP.md`
- `docs/10-implementation/Implementation_Index.md`
- `docs/10-implementation/phase-plans/P3_Implementation_Plan.md`
- `docs/10-implementation/phase-plans/P4_Implementation_Plan.md`
- `docs/10-implementation/checklists/P3_Closeout_Checklist.md`
- `docs/10-implementation/checklists/P4_Closeout_Checklist.md`
- `docs/10-implementation/runbooks/P3_Paper_Execution_Runbook.md`
- `docs/10-implementation/runbooks/P4_Controlled_Live_Smoke_Runbook.md`
- `docs/10-implementation/runbooks/P4_Controlled_Rollout_Decision_Runbook.md`
- `docs/10-implementation/runbooks/P4_Real_Weather_Chain_Smoke_Runbook.md`

### 12.2 核心代码

重点审查文件/模块包括：

- `asterion_core/contracts/execution.py`
- `asterion_core/contracts/ids.py`
- `asterion_core/contracts/weather.py`
- `asterion_core/runtime/strategy_engine_v3.py`
- `asterion_core/execution/execution_gate_v1.py`
- `asterion_core/execution/order_router_v1.py`
- `asterion_core/execution/live_submitter_v1.py`
- `asterion_core/blockchain/chain_tx_v1.py`
- `asterion_core/signer/signer_service_v1.py`
- `asterion_core/storage/database.py`
- `asterion_core/storage/write_queue.py`
- `asterion_core/storage/writerd.py`
- `asterion_core/ui/ui_db_replica.py`
- `asterion_core/ui/ui_lite_db.py`
- `asterion_core/monitoring/readiness_checker_v1.py`
- `asterion_core/monitoring/health_monitor_v1.py`
- `domains/weather/scout/market_discovery.py`
- `domains/weather/spec/rule2spec.py`
- `domains/weather/spec/station_mapper.py`
- `domains/weather/forecast/adapters.py`
- `domains/weather/forecast/service.py`
- `domains/weather/forecast/replay.py`
- `domains/weather/pricing/engine.py`
- `dagster_asterion/resources.py`
- `dagster_asterion/job_map.py`
- `dagster_asterion/handlers.py`
- `ui/app.py`
- `ui/auth.py`
- `ui/data_access.py`
- `scripts/run_real_weather_chain_smoke.py`
- `scripts/run_real_weather_chain_loop.py`
- `start_asterion.sh`
- `sql/migrations/*.sql`

### 12.3 测试

重点执行/审阅的测试包括：

**通过的测试**
- `tests/test_execution_foundation.py`
- `tests/test_real_weather_chain_smoke.py`
- `tests/test_signer_shell.py`
- `tests/test_submitter_shell.py`
- `tests/test_chain_tx_scaffold.py`

**暴露漂移/失败的测试**
- `tests/test_cold_path_orchestration.py`（settings drift / `gamma_tag_slug`）
- `tests/test_controlled_live_smoke.py`（`tests.*` import 问题）
- `tests/test_live_prereq_readiness.py`（`tests.*` import 问题）
- `tests/test_health_monitor.py`（`tests.*` import 问题）

---

# 对用户要求中的 10 个关键问题的明确回答

## 1. 当前 Asterion 的整体架构是否合理？

**是，整体架构方向合理，而且 contracts/persistence/read-model/replay 设计明显高于平均水平。**  
真正不合理的不是“大框架”，而是经济模型、状态治理和 live boundary 硬化程度落后于架构骨架。

## 2. 当前设计里最危险的漏洞或隐患是什么？

**最危险的不是单个代码 bug，而是“错误的 readiness 信号 + 过弱的 live/secrets 边界 + 过弱的经济模型”组合在一起。**  
也就是：系统看起来比它真实能力更成熟，这会诱导错误放行。

## 3. 当前系统离“真正可持续赚钱”最远的短板是什么？

**forecast uncertainty calibration + execution-aware pricing/ranking。**  
不是 agent，不是 UI，不是多写几条 runbook。

## 4. 当前 UI / operator console 是否真的围绕决策与盈利设计？

**还没有。**  
它更像 readiness/ops console，而不是真正的 PnL/decision console。

## 5. 当前 agent 的使用方式是否合适，还是噱头多于价值？

**当前 agent 用法总体合适，但它的价值是 review/QA/exception triage，不是 alpha 核心。**  
一旦让 agent 深度参与机会排序，就会有噱头化倾向。

## 6. 当前 forecast / pricing / opportunity 这条链最值得优化的点是什么？

**先优化 forecast 分布校准，再把 fair value 变成 executable edge，然后重做 ranking。**

## 7. 当前 controlled live 边界是否足够稳健？

**对 `approve_usdc` smoke 来说是“有意义且较谨慎的”；对真钱生产来说还不够稳健。**

## 8. 当前哪些模块最应该重构？

1. `domains/weather/forecast/adapters.py` 及其上游校准体系  
2. `domains/weather/pricing/engine.py` 及 opportunity 语义  
3. `asterion_core/ui/ui_lite_db.py` 中机会评分与 operator summary  
4. `domains/weather/spec/station_mapper.py` / market-to-station pipeline  
5. `start_asterion.sh` / 测试治理 / 状态治理链

## 9. 如果你是 CTO，接下来最应该做的 10 件事是什么？

见第 10.1 节。

## 10. 如果你是 PM / founder，最应该押注的产品方向是什么？

**优先押注：weather/event market intelligence + operator console + AI-assisted review 的半自动交易台产品。**  
而不是立刻押注“全自动真钱交易系统”。

---

# 最终判断

Asterion 当前最有价值的地方，不是“已经能自动赚钱”，而是它已经形成了一个**相当像样的、可审计、可回放、可分层持久化、具备 controlled-live smoke 边界的交易基础设施骨架**。  

但如果让我以 CTO 身份做最终判断：

> **我会认为它“值得继续投入并进行收敛重构”，但我不会在当前状态下签字把它作为真正的 controlled live trading rollout 候选。**

因为它最缺的不是更多脚本或更多 agent，而是：

- 更诚实的状态治理
- 更硬的 live/secret 边界
- 更强的 forecast/probability/execution economics
- 更面向决策与盈利的 operator console

