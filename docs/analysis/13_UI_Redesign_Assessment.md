# UI / UX Redesign Assessment for Asterion

> Analysis input only.
> Not implementation truth-source.
> Active implementation entry: `docs/10-implementation/versions/v2.0/phase-plans/V2_Implementation_Plan.md`

**状态**: historical UI assessment snapshot (`2026-03-13`)

**输出文件**: `13_UI_Redesign_Assessment.md`
**分析对象**: Asterion 当前仓库（上传快照）
**分析角色**: 顶级 UI/UX 设计师 + 交易基础设施产品负责人
**结论立场**: 以真实 operator / 交易研究员 / 风控负责人 / 创始人是否能更快做出更好的交易决策、是否更接近赚钱为唯一标准。

---

## 0. 执行摘要

一句话结论：**当前 UI 的最大问题不是“页面不够好看”，而是“信息方向错了、排序错了、重点错了”**。

从代码和文档看，Asterion 当前已经是一个 **P4 closed / ready for controlled live rollout decision** 的系统，但它**不是 unattended live trading product**。这决定了当前 UI 的正确定位不应该是“炫技 dashboard”，也不应该是假装自己已经是完整 live trading terminal，而应该是一个：

1. **诚实反映 controlled-live boundary 的 operator decision console**
2. **能把市场机会按优先级排出来的 research / execution intelligence console**
3. **能把 readiness、wallet、execution、agent review 这些“能不能做”因素与“值不值得做”因素放到同一决策框架里** 的工作台

当前 UI 在第 1 点上做得 **比表面看起来更好**：它真实展示 boundary、不会伪装成 fully live、很多页面也的确围绕 `ui.*` read model 组织，这一点是它最值得保留的地方。
但在第 2、3 点上，当前 UI 明显不足，尤其是 **Markets 页面几乎还是一页“链路检查页”**，而不是“专业交易情报终端”。

最关键的问题集中在这里：

- 当前 Markets 页**没有市场优先级排序模型**
- 没有先回答“**今天最值得看哪几个市场**”
- 没有把“**高 edge 但不可做**”和“**可做但 edge 一般**”明确区分
- 没有把“**agent review 可信度** / **wallet readiness** / **execution capability** / **controlled-live support**”纳入统一的 actionability 框架
- 还把大量低价值、链路型、排障型信息放到了默认视野里

因此，当前 UI 的正确重构方向不是“换一套更酷的视觉”，而是：

> **把 Asterion 从“阶段性工程状态控制台”重构为“机会排序 + 执行可行性 + 风险边界”三位一体的 operator console。**

我对整体产品/UI 的核心建议有四条：

1. **把 Markets 页重做为 Opportunities Terminal**，而不是继续做 chain detail page。
2. **先做 canonical 的 market opportunity read model，再做页面**；不要在 Streamlit 页面里临时拼评分逻辑。
3. **排序要采用“两层模型”**：先分层（Actionable / Blocked / Review Required / No-Trade），再在层内按 edge、confidence、trust 排序。
4. **把天气市场按“城市 + 日期 + 温度 bucket ladder”组织**，而不是把 80+ 个 bucket 市场当 80+ 个完全独立的列表项平均对待。

我的最终判断是：

- **当前 UI 作为 P4 closeout / readiness console：基本合格**
- **当前 UI 作为帮助 operator 找机会和做交易决策的界面：不合格**
- **最应该优先修的不是样式，而是 Markets 页的信息架构、排序逻辑、机会表达和 read model**

---

## 1. 研究范围与方法

### 1.1 已阅读与核对的内容

本次分析不是只看 UI 文件，而是结合了以下真实代码与文档：

- 项目总览与阶段文档：`README.md`、`AGENTS.md`、`docs/00-overview/*`、`docs/10-implementation/*`
- P4 相关 phase plan、closeout checklist、controlled rollout / live smoke / real weather chain smoke runbook
- UI 实现：`ui/app.py`、`ui/data_access.py`、`ui/pages/*`
- UI read model 构建：`asterion_core/ui/ui_lite_db.py`
- execution / monitoring / risk / weather domain 相关实现
- `scripts/run_real_weather_chain_smoke.py`、`scripts/run_real_weather_chain_loop.py`
- `tests/test_ui_data_access.py`、`tests/test_live_prereq_read_model.py` 等测试

### 1.2 本次分析采用的原则

本报告只基于三类东西下判断：

1. **仓库里真实存在的代码与文档契约**
2. **仓库快照里真实存在的数据与 read model 结构**
3. 在这些事实之上的 **产品/UI 推断**

因此，报告中会明确区分：

- **代码事实**：当前真的有什么、没有什么、UI 现在怎么做
- **产品/UI 推断**：在当前阶段最合理的重构方向是什么

### 1.3 当前仓库快照的事实性限制

上传快照里，数据面并不是完整的“最新成功运行态”，这一点很重要：

- `real_weather_chain.duckdb` 中有：
  - `weather.weather_markets = 81`
  - `weather.weather_market_specs = 81`
  - `agent.*` 表有数据
- 但同一快照里没有完整机会链结果：
  - `weather.weather_forecast_runs = 0`
  - `weather.weather_fair_values = 0`
  - `weather.weather_watch_only_snapshots = 0`
- `real_weather_chain_report.json` 当前是 `initializing` 占位报告，不是成功完成后的报告
- 默认 `data/ui/asterion_ui_lite.duckdb`、`data/ui/asterion_ui.duckdb`、`data/asterion.duckdb` 在上传快照中不存在
- 我基于 `real_weather_chain.duckdb` 生成了临时 `ui_lite`，验证了 `ui.market_watch_summary`、`ui.agent_review_summary` 等结构

这意味着：

- 我可以对**架构、read model、页面逻辑、契约边界**做很扎实的分析
- 我不能假装当前快照里已经存在完整的实时机会 ranking 数据
- 所有“机会排序模型”的建议都必须建立在“**当前已有字段 + 应该补哪些 read model**”的基础上，而不是把不存在的数据硬说成已经有

---

## 2. 事实基线：Asterion 当前到底处于什么产品阶段

### 2.1 代码和文档给出的阶段结论

从 `README.md`、`Asterion_Project_Plan.md`、`DEVELOPMENT_ROADMAP.md`、`P4_Implementation_Plan.md`、`P4_Controlled_Rollout_Decision_Runbook.md` 可以清楚确认：

- 当前状态是 **`P4 closed`**
- 当前结论是 **`ready for controlled live rollout decision`**
- 这**不等于** unattended live
- 当前 controlled live 仍然保持：
  - `manual-only`
  - `default-off`
  - `approve_usdc only`

这意味着 UI 的产品目标应该是：

- 帮 operator 判断 **是否 ready / 哪里 blocked**
- 帮 operator 判断 **哪些机会值得看 / 哪些机会理论上有 edge 但当前不能做**
- 帮 operator 在 controlled-live boundary 下推进最小可审计动作

而不是：

- 假装自己已经是完整 live trading cockpit
- 假装策略可以 unattended self-driving
- 假装缺失的数据面（尤其 liquidity / depth / realized edge）已经齐备

### 2.2 当前 UI 的真实定位

`README.md` 已经把当前 UI 明确收口成 **Operator Console**，并指出：

- 主数据源优先是 canonical `ui.*` read models
- `real_weather_chain_report.json` 只是 weather smoke 辅助视图

这一定义本身是对的。问题不在定义，而在**页面落地仍然偏向工程链路说明，而不是 operator 决策优先级**。

### 2.3 当前仓库里确实已经有的关键能力

从代码和 runbook 看，当前系统已经具备：

- real weather ingress
- paper execution
- live-prereq readiness
- controlled live smoke boundary
- operator read models
- agent review data

因此，UI 已经不应再停留在“hello world demo dashboard”的阶段。
它应该能回答更成熟的问题：

- 今天优先看哪些市场？
- 哪些市场有理论 edge？
- 哪些 edge 是可执行的？
- 哪些 edge 只是研究价值，但还不能碰？
- 当前最大的 rollout blocker 是 readiness、wallet、execution 还是 agent trust？

---

## A. 当前 UI 评估

## A.1 总体评价

我的总体评价是：

> **当前 UI 是一个“诚实但重心偏工程”的 P4 控制台；它适合阶段 closeout 和 operator 自检，但还不适合作为真正的交易决策前台。**

更直白一点：

- **作为 phase closeout / readiness console：合格**
- **作为赚钱导向的专业交易终端：不合格**

### A.1.1 为什么说它“诚实”

因为当前 UI 有几个非常好的原则：

- 它反复强调 `manual-only / default-off / approve_usdc only / not unattended live`
- 它没有伪造“全部正常”的健康状态
- 它没有把 agent 说成会直接交易
- 它确实围绕 `ui.phase_readiness_summary`、`ui.live_prereq_wallet_summary`、`ui.live_prereq_execution_summary`、`ui.execution_exception_summary` 这套 P4 runbook 输入面组织页面

这是值得保留的产品气质：**可信、克制、守边界**。

### A.1.2 为什么说它“重心偏工程”

因为默认页面组织里，人最先看到和最容易看到的，仍然是：

- 链路阶段说明
- 文件 / source / station / provider 等 trace 信息
- readiness / wallet / exception 的状态切片
- raw table / raw path / raw config

而不是：

- 这堆市场里什么最值得看
- 什么最能赚钱
- 什么 high edge 但 blocked
- 什么是今天的第一优先级
- 如果我现在只有 15 分钟，我该把注意力放在哪三个市场上

这就导致 UI 虽然“真实”，但还**不是以注意力分配和盈利决策为中心**。

---

## A.2 当前 UI 的优点

### 1. 边界表达非常清楚

这一点在 `ui/app.py`、首页文案、System 页说明里都很一致。对当前阶段来说，这是高价值优点。

### 2. 页面对真实 P4 read model 有一定对齐

当前首页、Execution、System 页的核心输入都与 P4 runbook 中要求 operator 核对的几张表对齐：

- `ui.phase_readiness_summary`
- `ui.live_prereq_wallet_summary`
- `ui.live_prereq_execution_summary`
- `ui.execution_exception_summary`

说明 UI 不是完全拍脑袋搭出来的。

### 3. 当前 Execution 页方向比 Markets 页更正确

Execution 页至少已经比较接近“异常与状态切片”的 operator 工作方式。它虽然还可以更好，但不是方向完全错。

### 4. 当前 Agents 页没有夸大 agent 能力

它明确说明 agent 只做辅助审查，不会直接下单或越过审批边界。这一点非常关键。

### 5. 当前 UI 对缺失数据相对诚实

例如：

- report 未生成时会说 `initializing`
- 表缺失时会说明 `不存在或为空`
- 不会把“无数据”伪装成“健康”

### 6. data access 层已经有“多源回退”的工程意识

`ui/data_access.py` 会在 `ui_lite`、runtime DB、weather smoke report 之间做有限 fallback。虽然它还不够优雅，但说明作者已经在解决“真实 operator 页面不能轻易崩”的问题。

---

## A.3 当前 UI 的主要问题

### 1. 最大问题不是视觉，而是信息层级

当前 UI 的默认组织方式，优先展示的是：

- readiness
- 链路状态
- source/provider/path/config
- raw rows

这更像工程排障控制台，而不是“赚钱相关信息优先”的交易工作台。

### 2. Markets 页的产品模型是错的

Markets 页现在本质上是：

> `discovery -> spec -> forecast -> fair value -> opportunity -> agent review` 的链路分步检查页

这对于工程自测有用，但不是 operator / researcher 最需要的页面模型。真正应该先回答的是：

- 哪些市场最值得优先看？
- 哪些市场最可能有 edge？
- 哪些 edge 可执行？
- 哪些只是研究价值，不宜操作？

### 3. 没有 attention ranking

在上传快照里，仅 `weather.weather_markets` 就有 **81 个市场**；如果按 `location_name + observation_date` 聚合，其实是 **11 个 city-day market family**。
当前 Markets 页却仍然让用户用一个平面表 + 手动 selectbox 去扫市场，这会严重浪费 operator 的注意力。

### 4. 缺少“理论机会”和“可执行机会”的区分

当前 UI 没有把下面几类市场分开：

- 高 edge 且可以行动
- 高 edge 但 wallet / market / review blocked
- 低 edge 但数据完整
- 没有 fair value / 没有 watch-only snapshot 的市场

这会直接损害决策效率。

### 5. 低价值信息占据默认视野

典型例子：

- Markets 页顶部的 `Market Source / Spec Station / Forecast Source`
- System 页的 file paths
- Agents 页顶部的 provider/model/API key
- Markets 页底部重复展示的全局 fair value / watch-only / canonical summary

这些信息不是没用，而是**不该默认占据最贵的屏幕位置**。

### 6. 当前页面的“下一步动作”不清楚

真正好的 operator console 不只告诉你“状态是什么”，还要告诉你“下一步该做什么”。
当前页面大多数地方停留在“展示状态”，没有形成：

- `review this`
- `fix wallet blocker`
- `wait for forecast`
- `no-trade`
- `actionable now`

这种 action-oriented 语言。

### 7. 页面间职责仍有交叉和漂移

- Home 有 readiness、wallet、exception、market coverage、agent activity
- Execution 有 ticket / run / exception / daily ops
- System 也有 readiness 和 runtime surface
- Agents 页既展示 review，又把配置放在最上面

说明当前 IA 还不是围绕 operator 工作流重构过的，而是围绕“现有 read model 有什么就塞什么”。

### 8. 存在一定 schema drift / transitional smell

例如：

- `ui.market_watch_summary` 的真实字段是 `latest_*` 风格，但 `ui/pages/markets.py` 末尾仍在尝试读取 `snapshot_id`、`question`、`forecast_source` 等并不直接对应的列
- `load_market_watch_data()` 的排序字段和当前表字段也不完全对齐
- Home 页的天气市场数量依赖 smoke report 的 `selected_market_count`，而不是 canonical market summary，因此可能与 DB 事实不一致

这不是大 bug，但说明当前 UI 还处在 **过渡期拼接状态**。

---

## A.4 当前 UI 是否匹配 Asterion 现阶段

### 匹配的部分

匹配的部分是：

- 它没有越过 P4 边界去假装是 live trading terminal
- 它确实强调 readiness 和 controlled live boundary
- 它把 wallet / execution / agent / system 这些 operator 关心的面向放到了 UI 中

### 不匹配的部分

不匹配的部分是：

- 既然 P4 已闭环，且已有 real weather ingress、paper execution、operator read model，UI 就不该还主要围绕“链路存在性”组织，而应该开始围绕“**attention allocation + opportunity prioritization**”组织
- 当前 UI 还没有真正把系统推进到“**可以支持 operator 做更好交易判断**”这一层

所以我的判断是：

> **当前 UI 匹配 Asterion 的“工程阶段边界”，但不匹配 Asterion 的“下一步产品成熟度”。**

---

## A.5 当前 UI 是否有“demo 感”

有，而且是两种 demo 感叠加：

### 1. 视觉 demo 感

`ui/app.py` 当前 CSS 使用了较强的米色、渐变、圆角大卡片、较高内边距。这种风格不难看，但更像：

- 品牌展示型 dashboard
- demo day dashboard
- 内部 showcase 页面

而不是：

- 高密度 operator console
- 高可信度风险/执行控制台
- 真正用于连续盯盘和 triage 的专业界面

### 2. 信息 demo 感

更重要的是信息层 demo 感：

- 很多模块在展示“我们有这些链路 / 这些配置 / 这些表”
- 但没有把这些东西收敛成“今天该怎么决策”

典型例子就是 `ui/pages/analytics.py`：它几乎是一个典型 placeholder / demo page，虽然当前不在主导航里，但它也说明整个 UI 体系还没有完全产品化。

所以我的结论不是“这个 UI 很糟”，而是：

> **它已经从 demo 迈向真实 operator console 的路上，但还没有完成从“工程展示”到“决策产品”的跃迁。**

---

## A.6 当前 UI 是否真正帮助用户决策

### 对 rollout decision：有帮助

当前 UI 对下列问题是有帮助的：

- readiness 是 GO 还是 NO-GO？
- wallet 有没有 blocker？
- execution 有没有 live-prereq attention？
- system surface 有没有缺口？

### 对市场决策：帮助明显不足

当前 UI 对下列问题帮助不足：

- 今天哪个市场最值得看？
- 哪个市场 edge 最大？
- 哪个市场 edge 够大但 liquidity / readiness 不够？
- 哪个 market family 值得优先研究？
- 哪些市场根本不值得浪费时间？

### 对赚钱目标：帮助不足

因为赚钱相关最关键的三件事，当前默认 UI 都没有做好：

1. **发现机会**：没有 ranking
2. **判断可做**：没有统一 actionability 状态
3. **分配注意力**：没有“先看什么，后看什么”

---

## A.7 从四类用户视角看当前 UI

| 角色 | 他们最想先回答的问题 | 当前 UI 回答得怎么样 | 核心缺口 |
|---|---|---:|---|
| 真实 operator | 现在能不能安全推进？哪里 blocked？ | 7/10 | 页面没有强 action queue，System/Execution/Home 信息分散 |
| 交易研究员 | 今天先看哪些市场？哪里有边？ | 3/10 | Markets 页没有 ranking、没有 family grouping、没有机会优先级 |
| 风控负责人 | 哪些机会值得看但不能做？blocker 是什么？ | 4/10 | 缺少 signal quality 与 operability 的统一表达 |
| 创始人 | 系统 readiness 如何？机会密度如何？瓶颈在哪？ | 4/10 | 没有 opportunity funnel / blocked funnel / performance surface |

---

## B. 信息架构重构建议

## B.1 重构原则

我建议整个 UI 以这 6 个原则重构：

### 原则 1：先决策，后溯源

默认视图先给结论：

- 值不值得看
- 能不能做
- 下一步做什么

原始 spec / forecast / trace / file path / raw JSON 必须下沉到二级层级。

### 原则 2：先排 attention，再展示全量列表

尤其天气市场这种 bucket 化市场非常多，用户不该被迫平均扫描所有行。
UI 必须先做 attention ranking。

### 原则 3：机会质量和可执行性必须分开表达

不能把 edge、confidence、liquidity、wallet、readiness 全压成一个黑盒分数。
更好的方式是：

- **Opportunity Quality**：值不值得看
- **Operability / Actionability**：能不能做

### 原则 4：默认视图必须匹配 runbook

既然 P4 决策 runbook 已经明确规定了 review 顺序，Execution / Readiness 页面就应该按 runbook 顺序组织。

### 原则 5：市场不是孤立点，而是 family / ladder

天气市场本质上是 `location + date + temperature buckets` 的 ladder。
UI 应该支持 family 视角，而不是只展示单 market 平铺列表。

### 原则 6：高价值信息前置，低价值信息折叠

- 高价值：edge、best action、confidence、blocker、time to close、review status
- 低价值：source trace、file path、raw config、重复全局表格

---

## B.2 我建议的新导航结构

### 默认主导航（面向 operator / researcher）

1. **Overview / Command Center**
2. **Opportunities**（可以保留导航名 `Markets`，但页面目标必须是机会终端）
3. **Execution & Readiness**
4. **Review Queue**（替代现在的 Agents）

### 次级 / debug 导航

5. **System / Debug**（管理员视角，默认不应是核心页面）

### 为什么这么改

因为当前默认工作流其实就是：

1. 先看 readiness / blockers
2. 再看今天机会分布
3. 再看 execution / wallet / live-prereq 是否支撑
4. 再处理 agent review / unresolved items
5. 最后才需要进入 system / file / debug 细节

这更符合 operator 的真实动作顺序。

---

## B.3 页面级重构建议

### 1. 首页 / Command Center

**目标**：一屏回答 4 个问题：

1. 当前 rollout decision 状态是什么？
2. 今天最值得看的市场 / market family 是什么？
3. 当前最大的执行 blocker 是什么？
4. 是否有需要人工 review 的 agent 事项？

**建议保留**：

- readiness 顶部位置
- wallet / execution blocker 视图
- boundary 提示

**建议删除或下沉**：

- “快速入口”这种静态导航提示
- 大段重复边界文案
- 仅展示 market coverage 但不显示优先级的模块

**建议新增**：

- `Top Opportunities` 模块
- `High Edge but Blocked` 模块
- `Review Required` 模块
- `Opportunity Funnel`（discovered -> priced -> signaled -> actionable -> blocked）

### 2. Markets / Opportunities

**目标**：不是讲链路，而是快速发现最值得做的机会。
这是当前最需要重构的页面，后文单独展开。

### 3. Execution & Readiness

**目标**：按 P4 runbook 的顺序让 operator 做出 rollout judgment，并快速定位 blocker。

**建议保留**：

- execution ticket summary
- live-prereq execution summary
- exception summary

**建议重做**：

- 顶部 metrics 改成 action queue 指标，而不是原始数量指标
- 页面顺序改成：Readiness -> Wallet -> Live-Prereq Exec -> Exceptions -> Run Summary / Daily Ops

**建议合并**：

- 将当前 System 页的大部分 readiness 内容合并进此页面

### 4. Agents -> Review Queue

**目标**：从“agent 配置页”改成“人工复核队列”。

**建议保留**：

- 最新 agent 结果
- human review required / review status / verification
- 按 subject_id 查看 agent 结果

**建议下沉**：

- provider/model/API key 等配置到页面下半部分或 expander
- file path 列表不应成为默认内容

### 5. System / Debug

**目标**：只做真正的 debug / infra health / artifact surface。
不应该再是默认 operator 主工作流的一部分。

**建议保留**：

- runtime component existence
- debug artifacts
- file paths

**建议新增**：

- `health_monitor_v1.py` 已有的 queue/signer/submitter/chain-tx/external-execution health surface

**建议弱化**：

- file paths 默认表格

### 6. Analytics

当前 `ui/pages/analytics.py` 明显是 placeholder / demo-like 页面。
建议：

- **当前阶段不应进入主导航**
- 要么删除
- 要么冻结为未来 `Performance & Calibration` 页的占位文件，但不要再用现在这种假指标内容

---

## C. Markets 页面专项设计

## C.1 页面目标

Markets 页的目标不应该是“展示所有市场详情”。
它的目标应该是：

> **在大量天气市场中，最快把 operator 的注意力引导到最值得研究、最可能赚钱、且最可能可执行的那些市场上。**

### 页面首先必须回答的 5 个问题

1. 哪些市场最值得优先看？
2. 哪些市场有最高 edge？
3. 哪些市场有 edge 但当前不可做？
4. 哪些市场虽然可做，但 edge 不够，不值得浪费时间？
5. 哪些 market family（城市/日期）值得整体优先研究？

---

## C.2 当前 Markets 页的问题：为什么它现在不对

### 1. 它是 pipeline inspection page，不是 opportunity terminal

当前 `ui/pages/markets.py` 的组织逻辑是：

- 顶部显示 chain status / source / station / forecast source
- 先展示 discovery/spec/forecast 细节
- 再给一个市场列表
- 再用 selectbox 打开某个市场详情
- 详情按 Discovery / Spec / Forecast / Fair Value / Opportunity / Agent Review 顺序展示
- 最后再重复展示全局 fair value / signals / canonical market watch summary

这完全是工程链路检查逻辑，不是交易决策逻辑。

### 2. 默认表格字段价值太低

当前表格字段是：

- `market_id`
- `question`
- `location_name`
- `station_id`
- `accepting_orders`
- `rule2spec_status`
- `data_qa_status`
- `resolution_status`
- `close_time`

其中真正对“赚钱判断”高价值的只有：

- `accepting_orders`
- `close_time`

其余多数是 trace / pipeline / review 维度，不适合做主表默认字段。

### 3. 没有默认排序逻辑

当前没有：

- edge 排序
- best action 排序
- opportunity rank
- actionability rank
- liquidity rank
- confidence rank

这意味着页面默认不帮用户分配注意力。

### 4. “Agent Status” 过滤器其实很弱

当前 Markets 页所谓 `Agent Status` 实际只对应 `rule2spec_status`，不是一个完整的“市场可信度 / review 状态”过滤器。
这会误导用户，以为自己在按综合 agent review 过滤。

### 5. 选择交互太慢

当前用户需要：

1. 先看表
2. 再去 selectbox 里选一个 market
3. 再读长详情

对于 80+ 市场或 11 个 market family，这个交互效率太低。

### 6. 原始链路信息抢占了最贵位置

`Market Source / Spec Station / Forecast Source` 这些信息应该属于“可信度 trace”层，而不是“页面第一屏最贵区域”。

---

## C.3 Markets 页的正确产品模型：不是“市场列表”，而是“机会终端”

我建议把这个页面的核心模型改成三层：

### 第一层：Market Family Leaderboard（城市 + 日期）

为什么必须先做这一层？
因为上传快照里 81 个市场其实只对应 **11 个 city-day family**：

- Atlanta 2026-03-13 / 2026-03-14
- Chicago 2026-03-13 / 2026-03-14
- Miami 2026-03-13 / 2026-03-14
- New York City 2026-03-13 / 2026-03-14
- Seattle 2026-03-13 / 2026-03-14
- Wellington 2026-03-14

这说明天气市场天然应该按 **family / ladder** 来看，而不是把每个 bucket 当完全独立产品。

**页面先给 family rank**，再展开 family 内 bucket ladder，才是专业且省注意力的设计。

### 第二层：Bucket Opportunity Ladder（family 内 bucket 排序）

展开 family 后，再看具体 bucket：

- 哪个温度区间最偏离 fair value
- 哪个 bucket 最适合买 / 卖
- 哪个 bucket 只是 near-threshold，不值得做

这相当于天气市场的“链式价差观察面板”。

### 第三层：Selected Market Detail（单 market 决策详情）

只有当用户真的点进某个 market，才展示：

- trade thesis
- fair value vs price
- review / trust
- capability / wallet / execution blockers
- spec / source trace / raw data

这比当前的“先讲链路再讲市场”更合理。

---

## C.4 我建议的排序逻辑

这是本报告最关键的部分。

### C.4.1 不建议一上来只做一个黑盒 `Market Score`

原因很简单：

- 当前数据里 **没有完整 liquidity/depth/slippage read model**
- 当前 `ui.market_watch_summary` 还是“一市场一条 latest snapshot”式汇总，不足以支撑严肃 ranking
- 如果现在强行做一个单一总分，很容易把用户带向“伪精确”

所以我建议采用：

> **先做分层（tiering），再做层内排序（ranking）**

这比一个黑盒分数更诚实，也更适合当前阶段。

### C.4.2 两层模型：Tier + Rank

#### 第一层：Actionability Tier

先把所有市场分成 5 类：

1. **Actionable Now**
   - 有有效 signal
   - 市场可交易 / 接单中
   - 至少存在 ready wallet 或具备 paper execution 支撑
   - 没有 unresolved review blocker

2. **High Edge but Blocked**
   - signal 强
   - 但 market capability / wallet readiness / review / controlled-live support 有 blocker

3. **Research First / Review Required**
   - 机会可能存在
   - 但 agent review 未闭合，或信号可信度需要人工确认

4. **Watchlist / Low Priority**
   - 数据完整
   - 但 edge 不高，或接近 threshold

5. **No Trade / Insufficient Data**
   - 没有有效 fair value / 没有 watch-only snapshot / 当前不具备判断基础

这个 tiering 比单纯按 edge 排序更重要，因为它首先回答“**能不能动手**”。

#### 第二层：Tier 内 Opportunity Rank

在每个 tier 内，再按 Opportunity Rank 排序。
**第一版可以只使用当前真实可获得的数据**：

- `abs(best_edge_bps)`
- `best_fair_value - best_reference_price` 的偏差强度
- `forecast confidence`
- `agent trust / review status`
- `time to close`
- `market tradable / accepting_orders`
- `wallet readiness presence`

### C.4.3 推荐的 v1 排序框架

#### Opportunity Quality（值不值得看）

建议用这几个维度：

- **Edge Strength**：绝对 edge_bps，最核心
- **Price Dislocation Clarity**：fair value 与 market price 偏差是否明确
- **Forecast Confidence**：预测置信度
- **Agent Trust**：是否 verified / approved / needs_followup / missing
- **Time Relevance**：距离 close 太远或太近都可能降低优先级

#### Operability（能不能做）

建议用这些维度：

- **Market Status**：是否 active / accepting orders / tradable
- **Capability Status**：market capability 是否允许
- **Wallet Status**：是否有 ready wallet
- **Execution Boundary**：当前 execution/live-prereq 是否支持
- **Liquidity Status**：如果没有数据，必须明确标 `Unknown`，而不是暗含 OK

### C.4.4 具体建议：不要只给一个分，要给 4 个可解释信号

我建议页面默认显示这 4 个：

1. **Opportunity Rank**：综合排序序号
2. **Actionability Status**：能不能做
3. **Confidence / Trust**：信不信得过
4. **Liquidity Status**：流动性充足 / 较薄 / 未知

如果一定要给分数，建议是：

- `Opportunity Score`：只表示“值得看”的程度
- `Actionability Score`：只表示“能不能做”的程度
- `Trust Score`：只表示“信号是否可靠”
- `Liquidity Score`：只有在数据存在时才显示

### C.4.5 我建议的 v1 公式（产品层建议，不是现有代码事实）

第一版我会这样做：

```text
先分层：
- actionable_now
- high_edge_blocked
- review_required
- watchlist
- insufficient_data

层内排名：
Opportunity Rank =
  0.45 * normalized_abs_edge
+ 0.20 * forecast_confidence
+ 0.15 * agent_trust_score
+ 0.10 * time_relevance_score
+ 0.10 * execution_readiness_score
```

其中：

- `normalized_abs_edge`：基于 `abs(edge_bps)` 归一化
- `forecast_confidence`：来自 fair value / forecast
- `agent_trust_score`：基于 `review_status / is_verified / human_review_required`
- `time_relevance_score`：避免太远或太临近的极端情况
- `execution_readiness_score`：用 capability / wallet / controlled-live support 的轻量评分

### C.4.6 关于 liquidity：当前不能假装已有

这是必须明确说清楚的：

**当前 read model 不足以认真回答 liquidity 排名问题。**

现有代码和 read model 里，你能看到：

- `tick_size`
- `fee_rate_bps`
- `min_order_size`
- `tradable`

但看不到严肃的：

- bid/ask spread
- top-of-book size
- depth within X bps
- slippage estimate
- recent traded volume
- quote staleness by token

所以：

- `Liquidity Rank` 这个概念是应该有的
- **但当前不应该伪造**
- 第一版必须明确显示：`Liquidity: Unknown / Not Yet Modeled`

这比给一个假的 liquidity 排名更专业。

### C.4.7 排序要如何向用户解释

我建议在页头提供一个简短说明：

> 排名先按“是否可行动”分层，再按 edge、confidence、review 可信度排序。若流动性数据缺失，会明确标记为 Unknown，不会假装已计入总分。

并在每条 market / family 旁边提供 `Why ranked here?` 的 tooltip：

- `+840bps edge`
- `forecast confidence 0.81`
- `agent review approved`
- `wallet ready = yes`
- `liquidity = unknown`

这是建立用户信任的关键。

---

## C.5 筛选逻辑

当前筛选太弱，必须重做。

### 我建议的默认快速筛选

#### 第一组：行动维度

- All
- Actionable Now
- High Edge but Blocked
- Review Required
- Watchlist
- No Data

#### 第二组：机会维度

- `edge > 300bps`
- `edge > 500bps`
- `edge > 1000bps`
- `forecast confidence >= 0.7`
- `trust = verified / approved`

#### 第三组：时间维度

- closing < 6h
- closing < 24h
- today
- tomorrow

#### 第四组：市场维度

- accepting orders only
- tradable only
- location
- station
- observation date
- family only / flat market list

#### 第五组：review / execution 维度

- human review required
- wallet ready only
- controlled-live supported
- liquidity known only（未来）

### 为什么要这样设计

因为 operator 的真实筛选心智不是“按 station_id 过滤”，而是：

- 先看能做的
- 再看高 edge 的
- 再看今天要到期的
- 再排除 review 未闭合或 wallet blocked 的

---

## C.6 页面布局建议

### 我建议的整体布局

```text
[顶部 KPI 条]
[快速筛选 / 排序条]

左侧 65%：Family Leaderboard + Bucket Table
右侧 35%：Sticky Decision Detail Panel

底部折叠区：Model / Agent / Raw Trace / Debug
```

### 为什么不建议当前这种“表格 + selectbox”二栏结构

因为当前结构需要用户把注意力在：

- 表格
- selectbox
- 长详情

之间来回切换，效率很差。

### 更合理的交互

- 左侧点击一行 family 或 market，右侧直接更新 sticky detail panel
- 支持 `family` 和 `market` 两级 drill-down
- 支持默认高亮排名第一项

---

## C.7 顶部 KPI 设计

我建议顶条 KPI 不再展示 `Market Source / Station / Forecast Source`，而改成这 6 个：

1. **Markets Scanned**
2. **Families Ranked**
3. **Actionable Now**
4. **High Edge but Blocked**
5. **Review Required**
6. **Closing Soon (<24h)**

如果数据可得，还可以补：

7. **Liquidity Known**
8. **Controlled-Live Eligible**

### 这些 KPI 为什么更重要

因为它们直接回答：

- 机会密度如何？
- 有多少是真的可行动？
- 有多少其实被 blocker 卡住？
- 今天是否值得花时间盯这个页面？

---

## C.8 主表格字段建议

### 默认应该展示的字段（Family 视图）

| 字段 | 是否默认展示 | 原因 |
|---|---:|---|
| Rank | 是 | 强制 attention priority |
| Family | 是 | 如 `Seattle · Mar 13`，比长 question 更可读 |
| Best Action | 是 | 一眼知道最优表达是买/卖哪边 |
| Best Edge (bps) | 是 | 最关键机会指标 |
| FV vs Px | 是 | 帮助理解偏差来源 |
| Confidence | 是 | 让用户知道 edge 不是纯拍脑袋 |
| Trust / Review | 是 | 区分 verified / review required |
| Actionability | 是 | 直接回答“能不能做” |
| Time Left | 是 | 决策时效 |
| Markets in Family | 是 | 看 family 的广度 |

### 展开后 market 级字段

| 字段 | 是否默认展示 | 原因 |
|---|---:|---|
| Bucket Label | 是 | 如 `44–45°F` / `50+°F` |
| Best Action | 是 | 具体到单 market |
| Edge (bps) | 是 | 具体 bucket 强度 |
| Market Price | 是 | 交易直觉核心 |
| Fair Value | 是 | 核心参考 |
| Threshold | 是 | 辅助判断是否只是 near-threshold |
| Confidence | 是 | 判断信号强弱 |
| Review Status | 是 | 是否需要人工介入 |
| Actionability | 是 | 当前能否执行 |

### 不应默认展示的字段

- `market_id`
- `condition_id`
- `station_id`
- `authoritative_source`
- `source_trace`
- `rule2spec_status / data_qa_status / resolution_status` 的 raw 三联列

这些不是没用，而是应该在 detail 或 tooltip 中看。

---

## C.9 右侧详情区建议

我建议右侧 detail panel 按“决策优先级”而不是“链路顺序”组织：

### 模块 1：Decision Summary（默认展开）

显示：

- 市场名称（短标题）
- Best Action（例如 `Buy YES` / `Sell NO`）
- Edge / Fair Value / Price
- Actionability badge
- Trust badge
- Time to close
- Operator Next Step

### 模块 2：Why It Matters

一段 concise thesis：

- 为什么这个市场被排到这里
- 主要 edge 来源是什么
- 有没有 review / execution / wallet blocker

### 模块 3：Execution Readiness

显示：

- market tradable?
- accepting orders?
- any ready wallet?
- controlled-live supported?
- liquidity known?
- blocker list

### 模块 4：Agent Review

显示：

- rule2spec / data_qa / resolution 的汇总 verdict
- 是否 human review required
- 是否 verified
- review_status

### 模块 5：Model Inputs（折叠）

- forecast source
- source trace
- bucket range
- station
- authoritative source

### 模块 6：Raw / Debug（默认折叠）

- ids
- raw payload excerpt
- artifact links / table sources

这样才符合 operator 的阅读顺序。

---

## C.10 卡片设计建议

### Family 卡片应该长什么样

每个 family 卡片建议包含：

- `Seattle · 2026-03-13`
- `KSEA`
- `8 markets`
- `Best edge: +840bps`
- `2 actionable / 1 blocked / 1 review required`
- `Best action: Buy YES @ 44–45°F`
- `Closes in 5h 20m`

这会比现在纯 question 列表更适合快速扫一遍。

### “最值得做市场”卡片

首页和 Markets 页顶部都可以放一个横向卡片条：

- #1 Opportunity
- #2 High Edge but Blocked
- #3 Needs Review

因为用户的第一需求不是看全量表，而是看“今天先看哪几个”。

---

## C.11 状态 badge 设计

### 建议统一为三类 badge

#### 1. Actionability badges

- `Actionable`
- `Blocked`
- `Review Required`
- `Watchlist`
- `No Data`

#### 2. Trust badges

- `Verified`
- `Approved`
- `Needs Follow-up`
- `Missing Review`

#### 3. Market / Execution badges

- `Accepting`
- `Tradable`
- `Wallet Ready`
- `Controlled-Live Ready`
- `Liquidity Unknown`
- `Closing Soon`

### 为什么要分三类

因为现在很多状态混在一起，看起来像一锅粥。
把 badge 按语义分类，用户能更快判断：

- 这是机会质量问题
- 还是执行问题
- 还是 review/可信度问题

---

## C.12 空状态与异常状态设计

Markets 页必须非常擅长处理 partial data，这是当前仓库真实状态决定的。

### 空状态 1：Markets discovered, but no forecast yet

文案应明确：

- 已发现市场
- spec 已映射
- forecast 尚未完成
- 当前无法给出机会排序

而不是简单写“暂无数据”。

### 空状态 2：Forecast exists, but no fair value / snapshot

说明：

- 已有 forecast
- pricing / watch-only snapshot 尚未生成
- 该市场暂处于 `Model Incomplete`

### 空状态 3：High edge but no liquidity model

显示：

- 可以给 opportunity rank
- 但 liquidity status = unknown
- 需人工检查 order book / 后续补 read model

### 异常状态 1：report initializing

当前 repo 快照里就是这种情况。
UI 应明确显示：

- 最新 smoke report 正在初始化
- 如果存在最近一份成功 snapshot，可回退展示 last successful snapshot
- 不要把 initializing 等价成“没有市场”

### 异常状态 2：fallback to smoke report

若 canonical `ui.market_opportunity_summary` 缺失，而页面回退到 smoke report，应明确标：

- `Source: smoke report fallback`
- `Ranking quality may be partial`

这是高可信 operator UI 的做法。

---

## C.13 哪些内容应该删除、折叠、弱化

### 当前 Markets 页里低价值甚至干扰用户的内容

#### 建议删除默认展示

- 顶部 `Spec Station` / `Forecast Source` 作为主 KPI
- 末尾重复的全局 `Fair Value` 表
- 末尾重复的全局 `Watch-Only Signals` 表
- 末尾 `Canonical Market Watch Summary` 的弱对齐展示

#### 建议折叠到二级层级

- `Discovery / Spec / Forecast` 的分步表格
- raw `station_id`
- source trace
- raw agent summary 长文本
- full IDs

#### 建议弱化

- raw agent status 三列
- file/source 元信息
- question 原始长标题

### 哪些信息现在反而不够突出

- best action
- best edge
- fair value vs price
- confidence
- review / trust
- actionability
- operator next step
- high edge but blocked

这正是“赚钱相关信息被埋了”的核心问题。

---

## C.14 如何让用户最快找到“最值得做的市场”

我建议一个非常明确的默认流程：

### 第一步：先看 Tier 切片

页头直接显示：

- Actionable Now
- High Edge but Blocked
- Review Required
- Watchlist

用户先点 `Actionable Now`。

### 第二步：看 Family Rank

按 `city + date` family 排序，先看最值得看的 family。

### 第三步：在 family 内看 bucket ladder

快速判断哪个 bucket 最有偏差。

### 第四步：右侧 detail 看 blocker / trust / next step

只在此时才进入细节。

这样用户能在 10–15 秒内完成第一轮 triage。

---

## D. 赚钱能力导向的产品建议

## D.1 从“帮助用户赚钱”的视角，什么信息必须前置

真正与赚钱直接相关的信息，默认应该优先于 pipeline trace：

1. **Best Action**
2. **Edge (bps)**
3. **Fair Value vs Market Price**
4. **Confidence**
5. **Actionability**
6. **Liquidity / Tradability**
7. **Review / Trust**
8. **Time to Close**

现在这些东西没有被放到 UI 最前面，这是 Markets 页最根本的问题。

---

## D.2 现有 UI 哪些地方不利于发现赚钱机会

### 1. 没有机会优先级

用户被迫手工扫列表，这天然降低发现机会的效率。

### 2. 没有 family 视角

天气 bucket ladder 本来就适合横向对比，当前 UI 却把每个 market 当孤立点。

### 3. 没有“blocked opportunity”视图

对交易团队来说，**高 edge 但 blocked** 是很重要的信息，因为它告诉你：

- 未来可以通过补 capability / wallet / review / liquidity 来变现的空间在哪里

当前 UI 没把这类机会显式显示出来。

### 4. 没有 opportunity funnel

创始人和产品负责人看不到：

- 市场发现多少
- 有 fair value 多少
- 有 signal 多少
- 真正 actionable 多少
- 被 blocker 卡住多少

这让你很难判断“赚钱能力的真实瓶颈在哪”。

### 5. 没有 performance / calibration 视图

当前 UI 缺少：

- 预测校准
- signal hit rate
- paper fill capture
- edge realization
- settlement 后的收益归因

这意味着 UI 还停留在“机会发现前端”，尚未形成完整的“赚钱闭环 front-end”。

---

## D.3 如何让用户更快发现高 edge / 高确定性 / 可执行机会

### 1. 对高 edge：把 `edge_bps` 做成默认第一优先数值列

而不是藏在 detail 或底部表格里。

### 2. 对高确定性：把 confidence + trust 拆出来

用户需要同时看到：

- 模型置信度
- 人工/agent review 可信度

### 3. 对可执行：把 actionability 做成主 badge，而不是附属信息

### 4. 对 controlled live 支持：单独显示 boundary compatibility

因为当前阶段不是 unattended live，`controlled-live support` 本身就是决策的一部分。

### 5. 对 high edge but blocked：做成单独榜单

这会非常有产品价值，因为它告诉团队：

- 哪些机会是“系统能力问题”而不是“市场没机会”

---

## D.4 当前缺失但应该补的“赚钱相关”数据面

这是一个必须说清楚的现实问题：

### 当前已有，可用于第一版机会 UI 的数据

- market / spec / forecast / fair value / watch-only signal（路径上已有）
- `edge_bps`
- `threshold_bps`
- `reference_price`
- `fair_value`
- `confidence`
- `agent review`
- `market capability` 的部分维度（tradable / min_order_size / fee_rate / tick_size）
- wallet readiness / execution readiness

### 当前明显缺失或 UI 未消费的关键数据

#### 1. Liquidity / book quality

需要至少补：

- best bid / ask
- spread_bps
- top-of-book size
- depth within 50bps / 100bps
- book timestamp / staleness

#### 2. Opportunity-to-execution conversion

需要至少补：

- 有 signal 的市场数
- 被 gate reject 的数
- 进入 ticket 的数
- 进入 order / fill 的数

#### 3. Realized edge / paper performance

需要至少补：

- expected edge at signal time
- filled price
- slippage_bps
- capture_bps
- settlement outcome / realized pnl

#### 4. Forecast calibration / agent quality by cohort

需要至少补：

- confidence bucket calibration
- rule2spec pass / followup rate
- data_qa false positive / false negative
- resolution verification quality

### 重要结论

**如果这些 read model 不补，UI 再好看，也不可能真正成为赚钱导向终端。**

---

## D.5 一个更成熟的赚钱导向首页应该长什么样

我建议首页以后逐步增加这 4 个板块：

### 1. Opportunity Funnel

```text
Markets discovered -> Priced -> Signaled -> Actionable -> Blocked -> Executed
```

### 2. Top Opportunities

今天最值得看的前 5 个 family / market

### 3. Blocked Opportunity Queue

高 edge 但被 wallet / review / market capability / liquidity 卡住的市场

### 4. Performance & Calibration（中期）

- signal quality
- paper fill capture
- calibration
- post-resolution learning

---

## E. 视觉与交互设计建议

## E.1 整体设计风格建议

我不建议把 Asterion 做成“更花哨”的 dashboard。
我建议的方向是：

> **高可信度、低装饰、强层级、偏专业金融终端的 operator console。**

### 当前视觉问题

- 背景渐变和米色基调偏柔和、偏展示
- 大圆角和高阴影降低了“严肃控制台”感
- metric 卡片高度偏大，压缩了数据密度

### 建议风格

- 弱化渐变
- 降低圆角（更克制）
- 减少阴影
- 提高表格与 badge 的信息密度
- 让主界面更像“专业控制台”，不是品牌展示页

---

## E.2 配色建议

建议使用 **冷静中性浅底**，辅以有限状态色：

- 背景：`#F6F7F9`
- 面板：`#FFFFFF`
- 主文字：`#111827`
- 次文字：`#4B5563`
- 边框：`#E5E7EB`
- 成功：`#0F766E`
- 警告：`#B45309`
- 错误：`#B42318`
- 信息：`#1D4ED8`

### 为什么不建议继续当前的米色/装饰性渐变主基调

因为金融/运营控制台最重要的是：

- 快速扫描
- 状态可信
- 数字清晰

而不是温暖氛围感。

---

## E.3 字体建议

不建议引入花哨字体。建议采用系统字体栈：

- 中文：`PingFang SC` / `Hiragino Sans GB` / `Noto Sans SC`
- 西文数字：`Inter` / `SF Pro Text` / 系统 sans fallback

并对价格、bps、时间、数量启用：

- `font-variant-numeric: tabular-nums;`

这会显著提升表格和 KPI 的专业感。

---

## E.4 密度与层级建议

### 当前问题

- 页面过于“卡片化”
- 垂直空间使用较松
- 对于 80+ market list 不够高密度

### 建议

- 表格行高 36–40px
- KPI 卡片高度降低
- 数值右对齐
- 标题层级减少一级装饰
- 默认更多使用“紧凑面板 + badge + table”，少用大面积 narrative 容器

---

## E.5 badge / table / panel / status system 建议

### badge

- 尺寸统一
- 颜色语义固定
- 同一页面不混用太多形态

### table

- 关键列固定（rank / family / edge / actionability）
- 时间、数值右对齐
- 长 question 不直接默认展开，优先短标题

### panel

- 详情 panel 固定在右侧或二级下方
- summary 在上，raw 在下

### status system

不要只靠颜色。需要同时用：

- label
- icon / shape
- tone

例如：

- `Actionable`（绿色）
- `Blocked`（红色）
- `Review Required`（橙色）
- `Liquidity Unknown`（灰蓝）

---

## E.6 如何让它看起来像高可信度 operator console，而不是 demo 页面

最有效的方式不是换皮，而是：

1. 让默认第一屏只出现最关键的决策信息
2. 减少展示型文案和重复说明
3. 提高表格密度和层级克制感
4. 用统一 badge 系统表达 actionability / trust / readiness
5. 把 raw debug 信息折叠到最下层

换句话说：

> **可信度首先来自信息结构，其次才来自配色。**

---

## E.7 如何兼顾中文阅读体验与金融/数据界面风格

建议采用：

- 中文 section title
- 英文保留金融/执行术语（如 `GO`, `NO-GO`, `BUY`, `SELL`, `bps`, `Actionable`）
- 不做过长中文句子堆叠
- 关键短语尽量词组化

例如：

- `机会排序`
- `可执行性`
- `人工复核`
- `钱包 blocker`
- `Controlled-Live 支持`

这样既保留专业术语的精确性，也保持中文界面的阅读效率。

---

## F. 代码落地建议

## F.1 哪些文件应该重点重构

### 必改文件

- `ui/app.py`
- `ui/data_access.py`
- `ui/pages/home.py`
- `ui/pages/markets.py`
- `ui/pages/execution.py`
- `ui/pages/agents.py`
- `ui/pages/system.py`
- `asterion_core/ui/ui_lite_db.py`

### 建议下沉或删除的文件

- `ui/pages/analytics.py`（当前阶段建议删除或冻结，不进入主导航）

---

## F.2 文件级重构建议

### `ui/app.py`

**现状**：

- 页面壳 + CSS + 导航混在一起
- 视觉偏展示型
- 导航结构还是旧页面心智

**建议**：

- 把样式 token 抽到 `ui/styles.py` 或 `ui/theme.py`
- 调整导航顺序为：Overview -> Opportunities -> Execution & Readiness -> Review Queue -> Debug
- 保留 controlled-live boundary，但变得更紧凑

### `ui/data_access.py`

**现状**：

- 一个大而杂的适配层
- 混合 UI lite、canonical runtime、smoke report
- 部分 presenter 语义在这里和页面之间漂移

**建议**：

拆成：

- `ui/data_access/base.py`
- `ui/data_access/overview.py`
- `ui/data_access/markets.py`
- `ui/data_access/execution.py`
- `ui/data_access/review.py`
- `ui/data_access/system.py`

同时把：

- ranking
- state mapping
- display_title 生成
- family grouping
- operator_next_step 生成

下沉到 dedicated presenter / view-model 层，而不是放在页面里随手拼。

### `ui/pages/home.py`

**建议**：

- 改成真正的 command center
- 增加 top opportunities / blocked opportunities / review queue
- 删掉低价值静态提示

### `ui/pages/markets.py`

**建议**：

- 几乎整体重写
- 当前版本可以拆成：
  - `ui/pages/opportunities.py`（新主页面）
  - `ui/pages/market_debug.py`（保留现有链路 inspection 能力，仅供 debug）

### `ui/pages/execution.py`

**建议**：

- 变成 exception-first / runbook-ordered 页面
- 强化 operator next step

### `ui/pages/agents.py`

**建议**：

- 改名为 `review_queue.py`
- 主体改为按 `subject_id` 看 review / verification / unresolved items
- provider / model / key 移到底部 expander

### `ui/pages/system.py`

**建议**：

- 如果不删，就改成 `debug.py`
- 或保留为 `Readiness & Health` 的 debug tab
- file path 默认折叠

---

## F.3 是否应该继续保持 Streamlit

### 我的结论：短期继续保持

**短期我建议继续用 Streamlit。**

原因是：

1. 当前最大问题不是框架，而是：
   - read model 不够
   - page IA 不对
   - 排序逻辑缺失
2. 用 Streamlit 足够完成：
   - opportunity leaderboard
   - family grouping
   - sticky detail-ish layout
   - compact KPI / badge / table
3. 现在如果直接换前端框架，会把团队注意力从最该修的核心问题上带走

### 什么时候再考虑换

如果中期需要这些能力，再考虑：

- 高级表格交互（固定列、键盘导航、复杂 cell renderer）
- 真正的多面板状态同步
- 更细粒度实时刷新

也就是说：

- **P0/P1 改造阶段：继续 Streamlit**
- **P2/P3 以后：若机会页面复杂度持续上升，再评估 React / custom component**

---

## F.4 `ui/data_access.py` 现在还合理吗

### 结论：作为过渡层合理，作为长期结构不合理

它的问题不在“功能不能跑”，而在：

- 太多不同语义混在一个文件里
- 页面级别的拼装逻辑太多
- smoke report fallback 与 canonical read model 混在一起
- 未来一旦 opportunities ranking 变复杂，这里会迅速变脏

### 推荐方向

`ui/data_access.py` 应该保留为“薄入口”，真正的逻辑迁到：

- read model 层（DuckDB / ui_lite）
- presenter / view-model 层

不要继续在页面里拼机会排序。

---

## F.5 `ui_lite_db` 的数据是否足够支撑更好的 UI

### 对 Execution / Readiness：基本够

当前 `ui_lite_db` 已经足够支撑：

- readiness page
- wallet readiness
- live-prereq execution
- execution exceptions
- agent review summary

### 对专业级 Markets 页：不够

这是最重要的代码事实之一。

当前 `ui.market_watch_summary` 的问题在于：

1. 它是一市场一行的 latest snapshot 汇总
2. 它没有保留“每个 outcome 的最新信号”
3. 它没有统一 best signal / best action / best outcome
4. 它没有 actionability / trust / readiness 融合字段
5. 它没有 liquidity / depth / spread 语义

所以：

> **当前 `ui.market_watch_summary` 只能支撑一个基础 watch view，不能支撑专业机会排序页。**

---

## F.6 应该补哪些 read model，而不是在页面里拼脏逻辑

这是整个重构中最重要的工程建议。

### 1. `ui.market_signal_latest_by_outcome`

**目的**：保留每个 market 每个 outcome 的最新 snapshot。
至少应包含：

- `market_id`
- `condition_id`
- `token_id`
- `outcome`
- `reference_price`
- `fair_value`
- `edge_bps`
- `threshold_bps`
- `decision`
- `side`
- `forecast_confidence`
- `forecast_source`
- `snapshot_created_at`

### 2. `ui.market_opportunity_summary`

**目的**：一市场一行的机会摘要，给 Opportunities 页面主表使用。
建议字段：

- market / title / short_title / location / station / observation_date / close_time
- `best_outcome`
- `best_side`
- `best_reference_price`
- `best_fair_value`
- `best_edge_bps`
- `forecast_confidence`
- `review_status`
- `human_review_required`
- `is_verified`
- `market_tradable`
- `accepting_orders`
- `ready_wallet_count`
- `actionability_status`
- `actionability_reason`
- `liquidity_status`
- `opportunity_score`
- `opportunity_rank`
- `score_components_json`

### 3. `ui.market_family_summary`

**目的**：按 `location_name + observation_date (+ station_id)` 聚合，形成 family 视图。
建议字段：

- `family_id`
- `location_name`
- `observation_date`
- `station_id`
- `market_count`
- `actionable_count`
- `blocked_count`
- `review_required_count`
- `best_market_id`
- `best_edge_bps`
- `best_action`
- `family_rank`

### 4. `ui.market_liquidity_summary`

**目的**：补齐当前缺失的流动性视图。
建议字段：

- `token_id`
- `best_bid`
- `best_ask`
- `mid_price`
- `spread_bps`
- `top_bid_size`
- `top_ask_size`
- `depth_50bps`
- `depth_100bps`
- `book_ts`
- `quote_staleness_ms`
- `liquidity_status`

### 5. `ui.review_queue_summary`

**目的**：支撑新的 Review Queue 页面。
建议字段：

- `subject_id`
- `subject_type`
- `agent_count`
- `human_review_required_count`
- `latest_review_status`
- `latest_verification_status`
- `latest_updated_at`
- `operator_next_step`

### 6. `ui.health_surface_summary`

**目的**：把 `health_monitor_v1.py` 已有 health surface 引进 UI。
建议字段：

- queue health
- signer health
- submitter health
- chain tx health
- external execution health
- last updated

### 7. 中期补：`ui.signal_capture_summary` / `ui.performance_summary`

**目的**：真正把“赚钱能力”做进 UI。
未来可包含：

- expected edge at signal time
- fill slippage
- edge capture
- settlement pnl
- calibration buckets

---

## F.7 现有代码里哪些数据已经能复用到 Markets 重构

虽然 liquidity 还不够，但仍有不少现成数据可以直接利用：

### 来自 weather / pricing / watch-only 的信号数据

- `reference_price`
- `fair_value`
- `edge_bps`
- `threshold_bps`
- `decision`
- `side`
- `confidence`

### 来自 capability 的可执行性数据

- `tradable`
- `min_order_size`
- `tick_size`
- `fee_rate_bps`

### 来自 wallet readiness 的 operator side 数据

- `wallet_readiness_status`
- `attention_required`
- allowance / chain-tx blocker

### 来自 agent review 的信任数据

- `verdict`
- `confidence`
- `human_review_required`
- `review_status`
- `is_verified`

这说明 Markets 重构不是从零开始，只是当前 UI 还没有把这些维度整合成真正的决策界面。

---

## G. 功能增删建议

## G.1 建议新增的功能

### 必增

1. **Top Opportunities Leaderboard**
2. **High Edge but Blocked** 榜单
3. **Family / Ladder 视图**
4. **Actionability 状态系统**
5. **Operator Next Step** 文案 / 列
6. **Review Queue** 页面
7. **Health Surface Summary** 页面/模块
8. **Opportunity Funnel**

### 短中期可增

9. **Liquidity summary**
10. **Performance / Calibration** 页面
11. **Why ranked here?** 解释弹层
12. **Last successful snapshot fallback**

---

## G.2 建议删除的功能 / 内容

### 建议删除或从默认视图移除

1. `ui/pages/analytics.py` 当前 placeholder 内容
2. Markets 页底部重复 fair value / signals / canonical summary
3. System 页默认 file path 全表
4. Agents 页默认顶部 provider/model/API key 作为主 KPI
5. 首页“快速入口”式静态提示

---

## G.3 建议弱化的内容

1. raw source / source trace
2. raw IDs
3. station / path / config
4. 链路分步说明
5. “我们有什么系统模块”式展示

这些都可以保留，但不应该继续霸占默认视野。

---

## G.4 建议重点强化的内容

1. opportunity ranking
2. actionability
3. blocked opportunity
4. review trust
5. family grouping
6. runbook-ordered readiness
7. operator next step
8. partial-data empty state

---

## H. 优先级排序：UI 重构路线图

## P0：必须马上修

这些是**不做就会持续误导用户或浪费注意力**的内容。

### P0-1 重做 Markets 页信息架构

- 从 chain page 改成 opportunities page
- 加入 Actionability Tier
- 加入 Opportunity Rank
- 加入 Top Opportunities
- 把低价值 trace 信息下沉

### P0-2 新增 canonical 机会 read model

至少补：

- `ui.market_signal_latest_by_outcome`
- `ui.market_opportunity_summary`
- `ui.market_family_summary`

### P0-3 首页加入机会模块

- Top Opportunities
- High Edge but Blocked
- Review Required

### P0-4 调整 Agents 页为 Review Queue

- 人工复核优先
- config 下沉

### P0-5 Execution / Readiness 页面按 runbook 顺序重排

- readiness
- wallet
- live-prereq exec
- exceptions
- health/debug

---

## P1：短期高收益优化

### P1-1 引入 health monitor surface

把 `health_monitor_v1.py` 已有的：

- queue
- signer
- submitter
- chain tx
- external execution

正式接进 UI。

### P1-2 建立统一 status badge system

### P1-3 重构 `ui/data_access.py`

拆分数据访问和 presenter 层，降低页面拼逻辑。

### P1-4 优化视觉系统

- 降低装饰感
- 提高表格密度
- 统一数值对齐与 badge

---

## P2：中期增强

### P2-1 增加 liquidity read model

### P2-2 增加 opportunity funnel / blocked funnel

### P2-3 增加 performance / calibration 页

### P2-4 增加 why-ranked explanation 与可解释评分

### P2-5 支持 family 与 market 的 drill-down 缓存 / 记忆选择

---

## P3：以后再做

### P3-1 评估更强前端框架或 custom component

仅当 Streamlit 在：

- 高级表格
- 多面板联动
- 键盘交互
- 大量市场实时更新

方面明显成为瓶颈时再考虑。

### P3-2 更完整的 founder / PM cockpit

- 收益归因
- edge capture vs realized pnl
- agent quality trend
- market family alpha trend

---

## 9. 保留 / 删除 / 重做 / 新增：总表

| 页面 / 模块 | 保留什么 | 删除什么 | 重做什么 | 新增什么 |
|---|---|---|---|---|
| Home | readiness / wallet / exceptions / boundary | 快速入口、低价值 coverage 描述 | 改成 command center | top opportunities, blocked opportunities, review queue, funnel |
| Markets | 市场 drill-down 能力、spec/forecast/raw trace 作为二级信息 | 顶部 source/station KPI、底部重复全局表格 | 完整重做为 Opportunities Terminal | family leaderboard, ladder, rank, actionability |
| Execution | ticket / live-prereq / exceptions | 低价值 run-first 组织方式 | 改成 exception-first + runbook-first | operator next step, blocker clustering |
| Agents | review data 本身 | config 当主 KPI | 改成 Review Queue | unresolved by subject, verification status |
| System | debug / artifact / runtime surface | 默认 file paths 大表 | 降为 Debug 或并入 Readiness & Health | health monitor summary |
| Analytics | 无 | 当前 placeholder 内容 | 不建议当前投入 | 未来 Performance & Calibration |

---

## 10. 我最重要的 10 条 UI/UX 结论

1. **当前 UI 最大的问题不是不好看，而是信息不对、排序不对、重点不对。**
2. **当前 UI 作为 P4 closeout console 基本合格，但作为赚钱导向交易界面不合格。**
3. **Markets 页现在是“市场链路检查页”，不是“机会终端”，必须重做。**
4. **天气市场天然是 city-date bucket ladder，不应该把 80+ 市场平铺平均扫描。**
5. **必须先做 canonical `market opportunity` read model，再做 Markets 页。**
6. **排序不应先上黑盒总分，而应先做 Actionability Tier，再做层内 Opportunity Rank。**
7. **当前 UI 最大的产品缺口是：没有把 edge、trust、wallet、execution、readiness 放进同一个 actionability 框架。**
8. **Agents 页应该从“配置页”转成“Review Queue”；System 页应该降级为 Debug/Health。**
9. **Execution 页方向比 Markets 页更对，应该保留但按 runbook 顺序重排并改成 exception-first。**
10. **短期不用换掉 Streamlit；先把 read model、信息架构和优先级排序做对，收益最大。**

---

## 11. 哪些判断基于代码事实，哪些是产品/UI推断

## 11.1 基于代码事实的判断

以下结论直接来自代码、文档和仓库快照：

1. 当前阶段是 `P4 closed / ready for controlled live rollout decision`，不是 unattended live。
2. controlled live 当前仍是 `manual-only / default-off / approve_usdc only`。
3. 当前 UI 主导航是 `Home / Markets / Execution / Agents / System`。
4. 当前 Home 主要围绕 readiness、wallet、execution exceptions、market coverage、agent activity。
5. 当前 Markets 页主视图仍是 discovery/spec/forecast/fair value/opportunity/agent review 这条链路的 inspection 逻辑。
6. 当前 Markets 页主表没有 edge、confidence、actionability、liquidity、rank 等核心字段。
7. 当前 Markets 页的筛选器只包含 location、station、`rule2spec_status` 风格的 agent 状态、accepting orders。
8. 当前 `ui.market_watch_summary` 是一市场一行的 latest snapshot 汇总，不足以表达 per-outcome 最新信号。
9. 当前 read model 里没有成熟的 liquidity/depth/slippage summary。
10. 当前 Agents 页把 provider/model/API key 作为顶部主要指标。
11. 当前 System 页默认展示 file paths。
12. 当前 `health_monitor_v1.py` 已有 richer health surface，但 UI 还没真正消费。
13. 上传快照里 `weather.weather_markets = 81`、`weather.weather_market_specs = 81`，而 `forecast_runs / fair_values / watch_only_snapshots = 0`。
14. 上传快照里的市场按 `location + observation_date` 可归成 11 个 family，而不是 81 个完全独立交易题。
15. 上传快照里 agent review 只有 5 行，且只有 `rule2spec`，说明 UI 必须处理 partial surfaces。
16. `ui/pages/analytics.py` 当前是 placeholder / demo-like 页面。
17. `ui/data_access.py` 当前是一个过渡性的大适配层，混合了 ui_lite、runtime DB、smoke report。

## 11.2 基于产品/UI推断的判断

以下内容是我基于上述事实给出的设计/产品推断：

1. Markets 页应该重构为 Opportunities Terminal。
2. 首页应该加入 Top Opportunities、Blocked Opportunities、Review Queue、Opportunity Funnel。
3. 机会排序应采用 Tier + Rank，而不是单一黑盒分数。
4. 最好按 city-date family + bucket ladder 组织天气市场。
5. Agents 页应该改名为 Review Queue。
6. System 页应该并入 Readiness & Health 或降为 Debug。
7. 当前视觉风格应从“展示型仪表盘”收敛为“高可信 operator console”。
8. 需要新增 `ui.market_opportunity_summary` 等 read model，而不是继续在 Streamlit 页面拼逻辑。
9. 中期应补 liquidity / performance / calibration 页面，才能真正对“赚钱能力”形成产品闭环。
10. 短期继续用 Streamlit 比立刻换前端框架更划算。

---

## 12. 我对 Markets 页面最关键的 redesign 建议（浓缩版）

如果只允许我给一个最重要的建议，那就是：

> **把 Markets 页从“链路详情页”重做成“机会优先级终端”，并且先按 city-date family 分组，再在组内按 bucket edge 排序。**

具体落地成 4 句话：

1. **默认第一屏不再展示 source/station/forecast trace，而是 Top Opportunities / Actionable / Blocked / Review Required。**
2. **先按 `Actionability Tier` 分层，再按 `Opportunity Rank` 排序。**
3. **天气 bucket 市场先看 family，再看单 bucket，不要把 80+ 行平铺给人手扫。**
4. **右侧 detail panel 默认先给 trade thesis、edge、confidence、blocker、next step，raw trace 全部下沉。**

---

## 13. 最终结论

我对当前 Asterion UI 的最终判断是：

- 它已经从“工程验证页面”进化到“有 operator 意识的控制台”
- 但它还没有进化到“真正帮助交易团队更快发现机会、评估可做性、形成赚钱闭环”的产品阶段

而决定这次重构成败的，不是视觉换皮，而是这三件事：

1. **Markets 页重构为机会终端**
2. **新增 canonical 机会 read model**
3. **用 actionability + trust + readiness 把赚钱相关信息组织起来**

如果这三件事做好，Asterion 的 UI 会从“P4 状态控制台”真正迈向“专业 operator console”。
如果这三件事不做，只做样式优化，那最终仍然只是一个更好看的工程 dashboard。

---

## 附录 A：建议的 Markets 页结构草图（文字版）

```text
[Opportunities]

KPI:
Markets Scanned | Families Ranked | Actionable Now | High Edge but Blocked | Review Required | Closing Soon

Quick Filters:
[Actionable] [Blocked] [Review Required] [High Edge >500bps] [Today] [Accepting Orders] [City]
Sort by: [Opportunity Rank v1]

Left Main:
1) Family Leaderboard
   #1 Seattle · Mar 13  | Best edge +840bps | Actionable | 8 buckets
   #2 NYC · Mar 13      | Best edge +720bps | Review Required | 8 buckets
   #3 Miami · Mar 14    | Best edge +650bps | Blocked | 8 buckets

2) Expanded Family Ladder
   36–37°F | Buy YES | +180bps | Watchlist
   38–39°F | Buy YES | +520bps | Actionable
   40–41°F | Sell YES| -610bps | Actionable
   ...

Right Detail Panel:
- Decision Summary
- Why ranked here?
- Execution Readiness
- Agent Review
- Model Inputs (collapsed)
- Raw Trace (collapsed)
```

---

## 附录 B：建议的 operator next step 文案枚举

建议统一生成以下几类 next step：

- `Actionable now`
- `Review agent output before acting`
- `Wait for forecast / fair value`
- `Fix wallet readiness blocker`
- `Market not tradable`
- `Liquidity check required`
- `No trade: edge below threshold`

这会比只展示状态更像真正的 operator console。
