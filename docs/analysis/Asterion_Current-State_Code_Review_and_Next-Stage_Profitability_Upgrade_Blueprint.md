# Asterion Current-State Code Review and Next-Stage Profitability Upgrade Blueprint

## 1. Executive Summary

这份报告不是基于 README 的高层复述，而是基于我重新核对的当前 HEAD 代码、migrations、active docs，以及一组代表性测试的实际复核结果写成。

先给结论。

### 1.1 当前系统真实状态

Asterion 现在已经具备一条**真实存在且被测试锁住的 Weather-first 交易主链**：

- `forecast -> pricing -> opportunity assessment -> ranking_score -> strategy_engine -> allocation preview / paper execution -> retrospective / execution feedback -> UI action surface`
- 当前仓库状态与 active docs 基本一致：`P4 accepted; post-P4 remediation accepted; v2.0 implementation active`
- 当前系统定位仍然是：`operator console + constrained execution infra`
- 当前系统**不是** unattended live、也不是 unrestricted live、也不是 fully autonomous production trading stack

### 1.2 当前最重要的判断

当前系统的主矛盾已经**不是**“有没有基础设施”，也不是“live boundary 会不会被随便绕过”。

我在本轮重点复核里没有看到旧版那种明显的 caller-trusted submitter/signer 漏洞：

- `RealClobSubmitterBackend` 已经验证 attestation v2 的 issuer / expiry / nonce / MAC / persisted attestation / single-use use-claim
- signer 已经拒绝 payload 自带 `private_key_env_var`，而是按 `wallet_id` 推导 controlled-live secret env var
- chain-tx shell 仍然只允许 `approve_usdc` 受控 live，并对 signed payload 做敏感字段 scrub

所以，**当前最影响 Asterion 下一阶段的，不是 live boundary，而是赚钱链条本身还不够“真钱化”**。

### 1.3 当前最强与最弱的盈利链条

**当前最强的盈利链条**：

`weather forecast -> calibration-aware pricing -> executable edge -> ranking_score v2 -> feedback penalty suppression -> runtime deterministic ordering -> allocation preview -> operator review`

这条链已经不是文档幻想，而是代码里真实接线、测试里真实锁住的行为。

**当前最弱的盈利链条**：

`capital-aware deployment -> deployable dollar PnL ordering -> empirical execution economics -> operator throughput -> realized capture loop`

也就是说，Asterion 现在更像是：

- **研究与决策链已经搭起来了**
- **真钱部署链还没有做到最优**

### 1.4 当前最严重的 10 个问题

1. `ranking_score_v2` 仍然是 **unit-opportunity economics**，不是**可部署资本语义下的真钱排序**
2. `allocator_v1` 依赖调用方传入顺序，自己不重新排序也不强校验
3. 资本部署策略仍偏静态，缺少更强的 budget / concentration / regime-aware sizing
4. execution economics 仍有较大 heuristic 成分（slippage/fill/depth/cancel/latency penalty 仍偏手工）
5. calibration refresh 已经 scheduled，但 calibration ops 还没有成为硬约束型运营闭环
6. operator surface 已经比旧版强很多，但还没有真正围绕“deployable PnL / binding limit / next best action”组织
7. UI / startup / shared truth source 仍有 phase/version copy drift
8. supporting design docs 有明显 stale 内容，与当前 HEAD 安全边界和 calibration reality 冲突
9. UI read-model / loader / fallback 路径仍然复杂，后续继续叠功能会抬高 truth-source drift 风险
10. acceptance 测试对“稳定赚钱”最关键的高阶行为仍然不够——尤其是 capital-aware ordering 与 operator decision safety

### 1.5 如果我来定义下个阶段，最先做的 5 件事

1. **把 ranking 从 unit-opportunity score 升级成 allocation-aware / deployable-value-first 的排序语义**
2. **加固 allocator v1：本地自排序、预算约束可解释化、binding-limit 透明化**
3. **把 execution priors / feedback loop 从“有”升级到“更像真钱执行模型”**
4. **把 operator workflow 改造成 deployable action queue，而不是研究信息集合**
5. **清理 truth-source / stale docs / startup copy / acceptance gaps，防止 v2.0 delivery drift**

### 1.6 哪些事情短期不应抢优先级

- 扩大 live boundary 到 unattended live
- 做 unrestricted live
- 新建一套 `risk.*` schema 平行宇宙
- 增加更多“看起来高级”的 agent 页面
- 过早扩到新 domain，而不是先把 Weather-first 赚钱链打透

---

## 2. Current Code Reality

## 2.1 当前 HEAD 的仓库 reality

### 代码与入口文档一致的部分

以下状态在 README、AGENTS、Implementation Index 与当前代码 reality 之间是对齐的：

- `README.md` 明确写的是 `v1.5`，状态为 `P4 accepted; post-P4 remediation accepted; v2.0 implementation active`
- `AGENTS.md` 明确写 active implementation entry 是 `docs/10-implementation/versions/v2.0/phase-plans/V2_Implementation_Plan.md`
- `docs/10-implementation/Implementation_Index.md` 也明确写 `V2_Implementation_Plan.md` 是当前唯一 active implementation entry
- `docs/00-overview/Version_Index.md` 把 `v1.0` 与 `v1.0-remediation` 都归为 historical accepted records

这意味着：

- `P4` 与 `post-P4 remediation` 当前都应该按**历史 accepted record**理解
- 当前真正 active 的规划入口，确实是 **v2.0 implementation**

### 当前代码 reality 里最值得特别澄清的几点

#### 1) 当前主排序已经是 `ranking_score`

`asterion_core/runtime/strategy_engine_v3.py:220-286` 当前会按：

- strategy priority
- `-ranking_score`
- `-abs(edge_bps)`
- signal ts
- market/token/side/strategy/snapshot

做稳定排序。

这说明：

- `ranking_score` 已经不是 UI 辅助字段
- 它已经是 runtime 主排序字段

#### 2) 当前 execution feedback loop 已经真接入主链

`domains/weather/opportunity/service.py` 当前会：

- 先做 `ranking_v2`
- 再乘 `uncertainty_multiplier`
- 再乘 `(1 - feedback_penalty)`
- 输出 `final_ranking_score`

而 `tests/test_execution_feedback_loop.py` 明确锁住了：

- 有 feedback penalty 时，`ranking_score` 会下降
- `why_ranked_json` 会保留 `pre_feedback_ranking_score`

也就是说，execution feedback 现在已经不是旁路展示，而是真正进入机会排序。

#### 3) calibration refresh 现在不是 manual-only

`dagster_asterion/job_map.py:364-380, 462-468` 明确表明：

- `weather_forecast_calibration_profiles_v2_refresh` 是 `mode="scheduled"`
- 默认 schedule key 是 `weather_forecast_calibration_profiles_v2_nightly`
- `enabled_by_default=True`

所以，任何把 calibration refresh 继续写成“manual-only”的说法，都已经不是当前 HEAD 的事实。

#### 4) Resolution Agent 才是当前 active agent seam

当前代码 reality 非常清楚：

- `domains/weather/spec/rule2spec_validation.py` 是 deterministic validation
- `tests/test_real_weather_chain_smoke.py:40-43` 甚至明确锁住了 smoke script 使用 deterministic `validate_rule2spec_draft(...)`，而不是 LLM Rule2Spec agent
- `ui/pages/agents.py` 明确写：Agents 页只承担 Resolution Agent 的 human review queue
- `tests/test_weather_agents.py` 证明当前 active seam 是 Resolution Agent

所以：

- Rule2Spec / Data QA 已经明显收口为 deterministic seam
- Resolution Agent 才是当前更接近 active 的 agent seam

#### 5) `ui.daily_review_input.item_id` 缺失问题已经修了

当前代码事实：

- `asterion_core/ui/read_model_registry.py:153-160` 要求 `ui.daily_review_input` 的主键与 required columns 都包含 `item_id`
- `asterion_core/ui/ui_lite_db.py:1563-1567` 现在已经显式投影 `ticket.ticket_id AS item_id`

当前测试事实：

- 我实际跑过的 `tests/test_execution_foundation.py` 目标用例已通过
- `tests/test_ui_loader_contracts.py` 通过
- `tests/test_truth_source_checks.py` 通过

所以，旧结论“`ui.daily_review_input.item_id` 缺失、导致 execution foundation 失败”已经**不再是当前 HEAD 事实**。

---

## 2.2 我本轮实际复核过的测试面

我本轮重点阅读并实际跑通了以下代表性测试：

- `tests/test_ranking_score_v2.py` → 5 passed
- `tests/test_execution_feedback_loop.py` → 2 passed
- `tests/test_allocator_v1.py` → 2 passed
- `tests/test_calibration_profile_v2.py` → 3 passed
- `tests/test_ui_loader_contracts.py` → 1 passed
- `tests/test_truth_source_checks.py` → 4 passed
- `tests/test_weather_agents.py` → 2 passed
- `tests/test_resolution_operator_review_closure.py` → 1 passed
- `tests/test_real_weather_chain_smoke.py` → 10 passed
- `tests/test_cold_path_orchestration.py` → 26 passed
- `tests/test_migrations.py` → 3 passed
- 以及 `tests/test_execution_foundation.py` 中与 `ranking_score`、paper execution、allocation artifacts 相关的关键用例 → passed

这意味着这份报告的核心判断不是“只看文件名”，而是有真实的行为复核基础。

---

## 3. What Is Working Well

## 3.1 受控 live boundary 比旧评估里强得多

### 当前代码事实

- `asterion_core/contracts/live_boundary.py:15-18, 182-243` 已经实现了 attestation v2、TTL、nonce、decision fingerprint、MAC
- `asterion_core/execution/live_submitter_v1.py:372-499` 会验证 attestation 是否存在、approved、匹配 request/wallet/backend/fingerprint、未过期、MAC 正确、已持久化、未复用
- `asterion_core/signer/signer_service_v1.py:358-369` 会拒绝 payload 自带 `private_key_env_var`
- `asterion_core/blockchain/chain_tx_v1.py:415-425, 782-796` 仍通过 live side-effect guard 控制 `controlled_live` 广播，并 scrub `raw_transaction_hex` / `private_key_env_var`

### 当前测试事实

- 现有 submitter / signer / controlled-live smoke / migration 测试面已经明显覆盖了这条边界链

### 结论

当前 Asterion 的**最核心 live integrity 基础设施已经成立**。这不意味着它该立即扩成 unattended live；但这意味着下一阶段的主要矛盾已经不在“submitter 有没有 canonical gate”。

---

## 3.2 `ranking_score`、feedback、calibration、UI truth-source 已经不再是“空壳能力”

### 当前代码事实

- `strategy_engine_v3.py` 真按 `ranking_score` 排序
- `service.py` 真的把 calibration / market quality / freshness / feedback penalty 乘进最终排序
- `surface_truth_shared.py` 已建立 shared truth constants：`CURRENT_PHASE_STATUS`、`TRUTH_SOURCE_DOC`、`PRIMARY_SCORE_FIELD`
- `read_model_registry.py` + `ui_lite_db.py` + `truth_source_checks` 已形成 UI read-model contract 体系

### 当前测试事实

- `test_execution_foundation.py` 锁住 ranking 优先于 raw edge
- `test_ranking_score_v2.py` 锁住 feedback / economics ranking behavior
- `test_truth_source_checks.py` 锁住缺关键列会 fail、空表会 warn
- `test_ui_read_model_catalog.py` 锁住 catalog 与 truth check 表存在

### 结论

Asterion 现在的 UI / truth-source / ranking / feedback 已经进入**可依赖的工程面**，而不是 README 才有的能力。

---

## 3.3 当前 persistence discipline 是健康的

### 当前代码事实

- canonical policy / position / reservation / exposure 主要落在 `trading.*`
- runtime allocation / feedback / calibration materialization / retrospective 落在 `runtime.*`
- 例如：
  - `sql/migrations/0026_runtime_allocation_artifacts.sql` → `runtime.capital_allocation_runs` / `runtime.allocation_decisions` / `runtime.position_limit_checks`
  - `sql/migrations/0022_runtime_execution_feedback_materializations.sql` → `runtime.execution_feedback_materializations`
  - `sql/migrations/0020_weather_forecast_calibration_profiles_v2.sql` → `weather.forecast_calibration_profiles_v2`

### 结论

当前 schema discipline 是合理的：

- **policy 与 canonical trading facts 在 `trading.*`**
- **runtime materialization / audit facts 在 `runtime.*`**
- **domain-specific probability artifacts 在 `weather.*`**

这个 discipline 应该继续保留，不应轻率引入新的 `risk.*` 平行 schema。

---

## 4. Critical Issues

下面是我认为当前最值得优先处理的关键问题，按“对稳定赚钱的影响”排序。

| 排名 | 问题 | 优先级 | 核心影响 |
|---|---|---:|---|
| 1 | `ranking_score_v2` 仍是 unit-opportunity economics | P1 | 直接限制真钱排序质量 |
| 2 | allocator v1 不自排序，依赖调用方顺序 | P1 | 预算可能被错误顺序消耗 |
| 3 | 资本部署策略仍偏静态，缺少更强 budget / concentration / sizing | P1 | 直接限制规模化赚钱 |
| 4 | execution economics 仍有较多 heuristic term | P1 | 直接限制稳定盈利质量 |
| 5 | calibration ops 还没成为强运营闭环 | P1 | 影响高置信赚钱 |
| 6 | operator surface 还不是 deployable-action-first | P1 | 影响 operator throughput 与误判率 |
| 7 | UI/startup/shared truth-source 仍有 copy drift | P2 | 影响 operator impression 与交付可信度 |
| 8 | supporting design docs 与 HEAD 冲突 | P2 | 容易误导下一阶段开发 |
| 9 | read-model / loader / fallback 复杂度偏高 | P2 | 容易继续积累 drift |
| 10 | acceptance tests 还没充分锁住“稳定赚钱”的高阶行为 | P2 | 后续 v2.0 delivery risk 偏高 |

---

## 5. Detailed Issue Register

## Issue 1 — `ranking_score_v2` 仍然不是 deployable-value-first 的真钱排序

- **优先级**：P1
- **类型**：Trading
- **受影响文件**：
  - `domains/weather/opportunity/service.py`
  - `asterion_core/runtime/strategy_engine_v3.py`
  - `asterion_core/risk/allocator_v1.py`
  - `ui/pages/home.py`
  - `ui/pages/markets.py`

### 当前代码事实

`domains/weather/opportunity/service.py:651-735` 的 `_ranking_score_v2_decomposition(...)` 当前会计算：

- `gross_unit_edge`
- `capture_probability`
- `expected_dollar_pnl = gross_unit_edge * capture_probability`
- `risk_penalty`
- `capital_efficiency = depth_proxy / unit_capital_cost`
- `ranking_score = economic_score + ops_tie_breaker`

这套逻辑已经明显优于旧版 raw edge 排序，但它本质上仍然是**单位机会级别的经济评分**。

它没有把这些要素真正放进主排序：

- 当前可部署 size
- 预算稀缺性
- 钱包级 capital scarcity
- position limit binding severity
- concentration / diversification 代价
- 订单真正可下的 notional

### 当前测试事实

- `tests/test_ranking_score_v2.py` 锁住了 EV/penalty/feedback 对 `ranking_score` 的影响
- `tests/test_execution_foundation.py` 锁住了 runtime 按 `ranking_score` 排序
- 但当前没有测试证明：**在预算紧张时，排序会优先最大化 deployable PnL**

### 当前文档事实

- `docs/10-implementation/versions/v2.0/phase-plans/V2_Implementation_Plan.md:39-43` 自己也承认：
  - execution economics 仍偏 unit-opportunity
  - allocator / sizing / capital discipline 仍缺位

### 风险描述

当前系统已经能判断“哪个机会看起来更好”，但还没有做到“在真钱预算有限时，哪个机会更值得先下”。

### 对系统的直接影响

- 在预算稀缺时，可能先把资本部署到 unit-score 好但 deployable-value 较低的机会
- `allocator_v1` 只能在排序后 resize/block，不能从根上修正排序目标

### 对“稳定赚钱”的影响

这是当前最妨碍 Asterion 从“合理系统”变成“稳定赚钱系统”的问题之一。

### 具体修复方案

保留当前 `ranking_score` contract，但把它升级成**capital-aware ranking**，而不是引入第二套主分数。

建议做两步：

1. **Ranking v3 不改主字段名，改主语义**
   - 继续输出 `ranking_score`
   - 但将其定义为：
     - `deployable_expected_pnl_after_costs`
     - 再乘 `capture confidence`
     - 再减 `capital concentration penalty`
     - 再减 `binding-limit penalty`

2. **引入 allocation-aware rerank pass**
   - 第一步继续按当前 ranking v2 生成候选机会
   - 第二步根据当前 wallet policy / reservation / exposure / position limit preview，估算：
     - `max_deployable_size`
     - `deployable_notional`
     - `deployable_expected_pnl`
     - `binding_limit_scope`
     - `remaining_budget_after`
   - 最后把 `ranking_score` 重写为 deployable-value-first score

### 需要改哪些模块

- `domains/weather/opportunity/service.py`
- `asterion_core/risk/allocator_v1.py`
- `dagster_asterion/handlers.py`
- `ui/data_access.py`
- `ui/pages/home.py`
- `ui/pages/markets.py`

### 需要补哪些测试

- 预算稀缺下，排序应优先 deployable PnL 更高的机会
- 相同 unit edge 但可部署规模不同，排序必须反映规模差异
- binding limit 更紧的机会，应在同等 EV 下被压后
- allocator preview 与最终 action queue 的排序一致性测试

### 是否需要 migration

**可能需要，但不需要新 schema。**

优先方向：

- 先扩已有 `runtime.allocation_decisions` / UI read-model 列
- 如需要 canonical policy 定义，优先扩 `trading.allocation_policies` 或新增 `trading.capital_budget_policies`
- **不建议默认新建 `risk.*`**

### 修复优先级顺序

**1**

---

## Issue 2 — `allocator_v1` 依赖调用方顺序，不自排序也不强校验

- **优先级**：P1
- **类型**：Bug / Trading / Architecture
- **受影响文件**：
  - `asterion_core/risk/allocator_v1.py`
  - `dagster_asterion/handlers.py`
  - `tests/test_allocator_v1.py`

### 当前代码事实

`asterion_core/risk/allocator_v1.py:202-208` 之后直接：

```python
for decision in decisions:
    ranking_score = _decimal(decision.pricing_context_json.get("ranking_score") or 0)
```

它不会：

- 在 allocator 内部重新排序
- 校验输入是否已按 `ranking_score` / `decision_rank` 排好

当前正确结果依赖于：

- `strategy_engine_v3.py` 先按 `ranking_score` 排序
- 调用方把排好序的 `decisions` 传进 allocator

### 当前测试事实

- `tests/test_allocator_v1.py` 锁住 allocator 的 resize/block/policy 行为
- 但没有看到专门的 regression test 去证明：**即使调用方传入乱序 decision，allocator 也会自我保护**

### 当前文档事实

- active docs 强调 allocator 是 v2.0 重点，但没有明确写 allocator 自身的排序不变量

### 风险描述

当前 allocator 更像“预算裁剪器”，不是“自洽的 capital deployment module”。

### 对系统的直接影响

任何后续新 job / 新入口 / 新 replay harness 如果把 decision 顺序传错，预算就会被错误顺序消耗。

### 对“稳定赚钱”的影响

这会让策略表现变得脆弱：

- 不是模型变差
- 也不是市场变差
- 而是预算被非最优顺序消耗

### 具体修复方案

1. allocator 内部做稳定排序：
   - 主键：`ranking_score DESC`
   - 次键：`decision_rank ASC`
   - 再次键：`decision_id`

2. 如果检测到输入顺序与排序后顺序不一致：
   - 写入 `allocation_run` warning
   - 或在 strict mode 下直接 fail

3. 把排序依据写入 `runtime.capital_allocation_runs` 元信息

### 需要改哪些模块

- `asterion_core/risk/allocator_v1.py`
- `dagster_asterion/handlers.py`

### 需要补哪些测试

- unsorted decisions input -> allocator output order remains ranking-correct
- missing ranking_score -> allocator should fall back deterministically / raise warning
- strict mode invariant test

### 是否需要 migration

不一定需要。若要记录排序来源 / warning，可增补 runtime metadata 字段。

### 修复优先级顺序

**2**

---

## Issue 3 — 当前资本部署策略仍偏静态，缺少更强的 budget / concentration / regime-aware sizing

- **优先级**：P1
- **类型**：Trading / Scale / Architecture
- **受影响文件**：
  - `asterion_core/risk/allocator_v1.py`
  - `asterion_core/risk/portfolio_v3.py`
  - `dagster_asterion/handlers.py`
  - `sql/migrations/0026_runtime_allocation_artifacts.sql`

### 当前代码事实

当前 allocator / portfolio 已经有这些基础：

- `trading.allocation_policies`
- `trading.position_limit_policies`
- `trading.inventory_positions`
- `trading.reservations`
- `trading.exposure_snapshots`
- `runtime.capital_allocation_runs`
- `runtime.allocation_decisions`
- `runtime.position_limit_checks`

这说明 allocator seam 已经存在，不需要从零开始。

但当前还没有明显看到更高阶的资本部署能力：

- regime-aware sizing
- per-cohort / per-station / per-market-class budget
- correlation / concentration budget
- capital scarcity aware throttling
- dynamic sizing tied to execution prior confidence

### 当前测试事实

- `tests/test_allocator_v1.py`
- `tests/test_allocation_preview_persistence.py`
- `tests/test_paper_execution_allocator_integration.py`

这些测试证明 allocator 基础链已经成立，但没有证明 allocator 已经能支撑更大规模、更复杂资本部署。

### 当前文档事实

- active v2.0 doc 也承认 allocator / sizing / capital discipline 仍缺位

### 风险描述

当前系统已经有“能分配一点钱”的能力，但还没有“能把钱分得很聪明”的能力。

### 对系统的直接影响

- 容易过度依赖人工在 UI 里做二次资本判断
- 难以从少量机会扩展到更大机会集

### 对“稳定赚钱”的影响

这是**规模化赚钱**的直接瓶颈。

### 具体修复方案

不要引入新的 `risk.*` schema。优先沿用现有 persistence discipline：

- **canonical policy** 继续放 `trading.*`
- **runtime allocation artifacts** 继续放 `runtime.*`

建议：

1. 扩展 `trading.allocation_policies`
   - 增加 budget regime / capital bucket / max concurrent exposure / max same-station exposure / max same-date exposure 等策略字段

2. 如有必要，新建 `trading.capital_budget_policies`
   - 仅在现有 `allocation_policies` 不适合承载预算 profile 时才加
   - 仍然归入 `trading.*`

3. 运行时仍落：
   - `runtime.capital_allocation_runs`
   - `runtime.allocation_decisions`
   - `runtime.position_limit_checks`

### 需要改哪些模块

- `asterion_core/risk/allocator_v1.py`
- `asterion_core/risk/portfolio_v3.py`
- 相关 handlers
- SQL migrations

### 需要补哪些测试

- regime-aware budget tests
- same-station / same-date concentration tests
- dynamic sizing monotonicity tests
- allocator preview vs paper execution consistency tests

### 是否需要 migration

**需要，但建议落在 `trading.*` / `runtime.*`，不要默认新建 `risk.*`**

### 修复优先级顺序

**3**

---

## Issue 4 — execution economics 仍然有较大 heuristic 成分

- **优先级**：P1
- **类型**：Trading
- **受影响文件**：
  - `domains/weather/opportunity/service.py`
  - `domains/weather/opportunity/execution_feedback.py`
  - `domains/weather/opportunity/execution_priors.py`
  - `domains/weather/opportunity/ranking_retrospective.py`

### 当前代码事实

当前系统已经明显比早期强：

- execution priors 已 materialize
- feedback penalty 已进入主排序
- ranking retrospective harness 已存在

但核心 economics term 仍然有大量 heuristic：

- `service.py:572-577`：`_slippage_bps()` 仍基本是 40 / 80 bucket
- `service.py:580-585`：`_liquidity_penalty_bps()` 仍是 25 / 60 / 999999 bucket
- `service.py:588-597`：`_fill_probability()` 仍是 0.25 / 0.50 / 0.75 / 0.60 bucket
- `service.py:600-605`：`_depth_proxy()` 仍是 0.85 / 0.55 / 0.25 bucket
- `execution_feedback.py:73-90`：feedback penalty 仍是 hand-tuned weight blend

### 当前测试事实

- `tests/test_execution_feedback_loop.py` 锁住 suppression 行为
- `tests/test_execution_priors_feature_space.py` 证明 priors 其实已经拥有更丰富 feature space：
  - `market_age_bucket`
  - `hours_to_close_bucket`
  - `calibration_quality_bucket`
  - `source_freshness_bucket`
  - latency / edge retention 等

### 当前文档事实

- `Execution_Economics_Design.md` 给了方向，但当前实现仍未完全吃满现有 priors 特征空间

### 风险描述

Asterion 已经有 execution science，但还没把 execution science 充分转成真钱 economics。

### 对系统的直接影响

- 排序仍可能对 fill / slippage / latency 的真实分布反应不足
- 高 edge 机会可能在现实执行中被过度乐观地上调

### 对“稳定赚钱”的影响

这是当前最直接影响**盈利稳定性**的问题之一。

### 具体修复方案

1. 把 fallback heuristics 逐步退居次要地位
   - 当 priors 样本足够时，优先使用 empirical priors
   - heuristics 只在 sparse/missing 时作为后备

2. priors v2 增强：
   - 加强对 `source_freshness_bucket`、`calibration_quality_bucket`、`hours_to_close_bucket` 的使用
   - 引入更明确的 `expected_adverse_fill_slippage`、`submit_ack_probability`、`working_timeout_probability`

3. ranking retrospective 直接对 economics 做闭环评估
   - 比较不同 economics term 对 capture ratio / realized pnl 的解释力

### 需要改哪些模块

- `domains/weather/opportunity/service.py`
- `domains/weather/opportunity/execution_priors.py`
- `domains/weather/opportunity/execution_feedback.py`
- `domains/weather/opportunity/ranking_retrospective.py`

### 需要补哪些测试

- priors sufficient -> empirical terms override heuristics
- economics regression vs retrospective rows
- realized edge retention sensitive ranking tests

### 是否需要 migration

可能需要扩 `weather.weather_execution_priors` 或 retrospective runtime rows 的字段。

### 修复优先级顺序

**4**

---

## Issue 5 — calibration 已经接入主链，但 calibration ops 还没有变成强运营闭环

- **优先级**：P1
- **类型**：Trading / Ops / Readiness
- **受影响文件**：
  - `domains/weather/forecast/calibration.py`
  - `dagster_asterion/job_map.py`
  - `ui/data_access.py`
  - `ui/pages/system.py`
  - `asterion_core/monitoring/readiness_checker_v1.py`

### 当前代码事实

- calibration v2 materialization 存在
- nightly schedule 已默认启用
- stale / degraded profile 会降低 uncertainty multiplier
- `runtime.calibration_profile_materializations` 已存在并有物化状态

### 当前测试事实

- `tests/test_calibration_profile_v2.py` 通过
- `tests/test_calibration_materialization_status.py` 通过
- `tests/test_calibration_freshness_penalty.py` 通过

### 当前文档事实

- supporting design `docs/40-weather/Forecast_Calibration_v2_Design.md` 仍写着 `manual profile materialization`，这已与 HEAD 不一致

### 风险描述

calibration 功能已经有了，但还不够“运营化”。

### 对系统的直接影响

- calibration stale 时，系统目前更多是 penalty / status 降级
- 但 operator action surface 还没有把 calibration freshness 变成足够强的日常运营信号

### 对“稳定赚钱”的影响

这主要影响**高置信赚钱**：

- stale calibration 不一定立刻让系统失效
- 但会让概率质量与 threshold edge 的可信度下降

### 具体修复方案

1. 把 calibration freshness 纳入更明确的 operator gate：
   - `fresh`：正常
   - `stale`：明确 review-required
   - `degraded_or_missing + sparse`：进入 `research_only`

2. 在 Home / Markets / System 中明确展示：
   - latest materialized_at
   - age hours
   - fresh/stale/degraded
   - 受影响 markets 数量

3. 为 calibration refresh 增加运营视角 acceptance：
   - 最近一次成功物化时间
   - 最新 profile sample sufficiency
   - stale profile count trend

### 需要改哪些模块

- `domains/weather/forecast/calibration.py`
- `ui/data_access.py`
- `ui/pages/home.py`
- `ui/pages/markets.py`
- `ui/pages/system.py`
- `readiness_checker_v1.py`

### 需要补哪些测试

- stale calibration -> actionability downgrade tests
- calibration materialization freshness shown in UI surfaces
- readiness gate / operator status integration tests

### 是否需要 migration

未必，先用现有 runtime materialization 表与 UI read model 即可。

### 修复优先级顺序

**5**

---

## Issue 6 — operator surface 已经有 action queue，但还不是 deployable-action-first

- **优先级**：P1
- **类型**：UX / Trading
- **受影响文件**：
  - `ui/pages/home.py`
  - `ui/pages/markets.py`
  - `ui/pages/execution.py`
  - `ui/data_access.py`
  - `asterion_core/ui/ui_lite_db.py`

### 当前代码事实

当前 UI 已经显示：

- `ranking_score`
- `recommended_size`
- `allocation_status`
- `budget_impact`
- `why_ranked_json`
- `action_queue`
- `cohort_history`

这比旧版本强很多。

但当前主叙事仍然更像：

- “这些市场为什么好”
- 而不是
- “现在该先下哪一单，预期 deployable pnl 是什么，受什么 limit 约束”

### 当前测试事实

- `tests/test_ui_data_access.py`
- `tests/test_ui_pages.py`
- `tests/test_truth_source_checks.py`

这些测试证明 UI contract 与 source badge 已成立，但没有直接锁住“deployable action quality”。

### 当前文档事实

- active v2.0 plan 也承认 operator throughput 不够高

### 风险描述

当前 UI 已经能帮 operator 看市场，但还没有把**资本部署决策**作为第一组织原则。

### 对系统的直接影响

- operator 要自己脑补：
  - 推荐下多大
  - 为什么这个 limit 绑定
  - 哪个 action 先执行更值钱

### 对“稳定赚钱”的影响

这会降低 operator throughput，增加误判和漏判。

### 具体修复方案

1. 把 Action Queue 做成第一公民
   - 默认按 `deployable_expected_pnl` / `ranking_score` / `binding_limit_severity` 组织
   - 显示：
     - recommended_size
     - deployable_notional
     - deployable_expected_pnl
     - binding_limit_scope/key
     - queue_reason_codes
     - source_badge

2. 把 Markets 页改成“机会 + 资本部署建议”双栏结构
   - 左边：机会质量
   - 右边：部署建议 / risk / next action

3. Execution 页增加“why missed”优先队列
   - 按 miss reason / distortion reason / uncaptured value 排序

### 需要改哪些模块

- `ui/pages/home.py`
- `ui/pages/markets.py`
- `ui/pages/execution.py`
- `ui/data_access.py`
- `asterion_core/ui/ui_lite_db.py`

### 需要补哪些测试

- action queue default ordering acceptance test
- deployable fields required columns test
- miss-reason priority surface test

### 是否需要 migration

可能需要给 UI read models 增补 deployable value / binding limit 明细列。

### 修复优先级顺序

**6**

---

## Issue 7 — UI / startup / shared truth-source 仍有 phase/version drift

- **优先级**：P2
- **类型**：UX / Docs / Truth-Source
- **受影响文件**：
  - `start_asterion.sh`
  - `ui/app.py`
  - `asterion_core/ui/surface_truth_shared.py`
  - `docs/10-implementation/versions/v2.0/phase-plans/V2_Implementation_Plan.md`

### 当前代码事实

- `surface_truth_shared.py:9-13` 已经把当前 truth source 锁成：
  - `TRUTH_SOURCE_DOC = docs/10-implementation/versions/v2.0/phase-plans/V2_Implementation_Plan.md`
  - `CURRENT_PHASE_STATUS = P4 accepted; post-P4 remediation accepted; v2.0 implementation active`
  - `PRIMARY_SCORE_FIELD = ranking_score`
- `ui/app.py:190-197` header copy 基本正确
- 但 `ui/app.py:271-278` sidebar caption 仍写 `Asterion v1.2`
- `start_asterion.sh:20-27` 仍写 `remediation in progress`

### 当前测试事实

- `tests/test_operator_truth_source.py` 锁住 `load_boundary_sidebar_truth()` 应使用 shared truth source
- 但它不覆盖 `start_asterion.sh` 的输出 copy，也不覆盖 `ui/app.py` 里硬编码的 `Asterion v1.2`

### 当前文档事实

- README / AGENTS / Implementation Index 都已经切到 v2.0 implementation active
- 也就是说，这类 drift 不是 docs 主入口的问题，而是 startup/UI surface 的 copy 问题

### 风险描述

这是**operator impression drift**，不是简单文案瑕疵。

### 对系统的直接影响

- 操作员会看到彼此不一致的 phase/version 信息
- reviewer 容易误判当前仓库成熟度

### 对“稳定赚钱”的影响

它不直接改变 pnl，但会伤害：

- 运营判断
- 真相源一致性
- 交付可靠度

### 具体修复方案

1. `start_asterion.sh` 改为从 shared truth / active doc 生成 boundary summary
2. `ui/app.py` 移除 `Asterion v1.2` 硬编码
3. header badge 也改为 shared truth 常量或 runtime-loaded truth
4. 为 startup copy 加 smoke test

### 需要改哪些模块

- `start_asterion.sh`
- `ui/app.py`
- 可能少量 truth-source helper

### 需要补哪些测试

- startup copy truth-source test
- app header / sidebar truth-source consistency test

### 是否需要 migration

不需要。

### 修复优先级顺序

**7**

---

## Issue 8 — supporting design docs 已经与当前 HEAD 冲突

- **优先级**：P2
- **类型**：Docs / Delivery
- **受影响文件**：
  - `docs/30-trading/Controlled_Live_Boundary_Design.md`
  - `docs/40-weather/Forecast_Calibration_v2_Design.md`
  - `docs/10-implementation/versions/v2.0/phase-plans/V2_Implementation_Plan.md`

### 当前代码事实

当前代码已经实现了：

- attestation v2 + expiry + MAC + single-use
- signer 不再信任 payload 的 `private_key_env_var`
- calibration refresh scheduled nightly
- `ui.daily_review_input.item_id` 已修复

### 当前文档事实

但这些 supporting docs 里仍有 stale 说法：

- `Controlled_Live_Boundary_Design.md:21-22, 41, 49` 仍写 attestation caller-trusted、无 expiry/single-use、signer 信任 `private_key_env_var`
- `Forecast_Calibration_v2_Design.md:208` 仍写 `manual profile materialization`
- `V2_Implementation_Plan.md:47-50` 仍把 `ui.daily_review_input.item_id` 缺失、calibration refresh manual-only 当作当前第一波问题；同一文件后面 `277-279` 又承认它们已修复，存在内部时间层叠

### 当前测试事实

- 代表性测试已证明上述旧问题不再是当前 HEAD 事实

### 风险描述

这已经不是“文档稍微落后”，而是**可能直接误导后续 v2.0 开发优先级**。

### 对系统的直接影响

- 可能重复投入到已修复问题
- 可能低估当前经济问题和 operator throughput 问题

### 对“稳定赚钱”的影响

会稀释正确的开发资源分配。

### 具体修复方案

1. 对明显 stale 的 supporting design：
   - 直接更新到 HEAD
   - 或显式加 banner：`historical design snapshot; partially stale relative to HEAD`

2. 对 `V2_Implementation_Plan.md`：
   - 把已修复问题从 “current first-wave issues” 移到 “recently closed baseline repairs”
   - 把当前真实主线改写为：
     - capital-aware ranking
     - allocator/sizing
     - execution economics
     - operator throughput
     - delivery hardening

### 需要改哪些模块

- 上述 docs

### 需要补哪些测试

- doc truth-source checks（至少针对 active implementation entry 的关键 status lines）

### 是否需要 migration

不需要。

### 修复优先级顺序

**8**

---

## Issue 9 — UI read-model / loader / fallback 复杂度仍偏高，后续容易继续漂移

- **优先级**：P2
- **类型**：Architecture / UX
- **受影响文件**：
  - `asterion_core/ui/ui_lite_db.py`
  - `asterion_core/ui/read_model_registry.py`
  - `ui/data_access.py`

### 当前代码事实

当前 read-model 基础是成立的，但文件体量和职责已经很重：

- `ui_lite_db.py` 很大，承担大量 projection 构建
- `ui/data_access.py` 很大，承担 surface loader、fallback source 选择、UI shaping、status synthesis
- fallback / degraded / canonical / derived source 路径比较多

### 当前测试事实

- `test_ui_loader_contracts.py`
- `test_truth_source_checks.py`
- `test_ui_read_model_catalog.py`

这些测试确实让当前系统还维持在可控状态。

### 风险描述

现在问题不是“已经坏了”，而是“再继续堆功能，很容易坏”。

### 对系统的直接影响

- 未来每加一层 UI 指标或 read-model，truth-source drift 风险都在升高
- fallback row 虽然有 `source_badge`，但开发复杂度已经开始抬高

### 对“稳定赚钱”的影响

它主要伤害 delivery speed 和 operator trust，而不是直接伤害 alpha。

### 具体修复方案

1. 继续保留 read-model catalog / truth-source checks，但开始模块化拆分：
   - execution projections
   - action queue projections
   - market opportunity projections
   - readiness projections

2. 对 fallback rows 做统一 adapter
   - 统一 `source_badge`、`source_truth_status`、`is_degraded_source` 生成逻辑

3. 为关键 surfaces 增加 golden acceptance snapshot tests

### 需要改哪些模块

- `asterion_core/ui/ui_lite_db.py`
- `ui/data_access.py`
- 少量 `read_model_registry.py`

### 需要补哪些测试

- per-surface golden loader tests
- fallback adapter consistency tests
- required-column contract tests per surface

### 是否需要 migration

一般不需要，除非新增 read-model 列。

### 修复优先级顺序

**9**

---

## Issue 10 — acceptance tests 仍未充分锁住“稳定赚钱”最关键的高阶行为

- **优先级**：P2
- **类型**：Testing / Trading / Ops
- **受影响文件**：
  - `tests/` 多个模块
  - retrospective / allocator / UI acceptance surfaces

### 当前代码事实

当前测试已经很强，尤其在：

- ranking_score runtime ordering
- feedback suppression
- calibration v2 materialization
- truth-source checks
- resolution review closure
- real weather chain smoke
- cold path orchestration

### 当前测试事实

我本轮实际跑通的测试已经证明当前回归面相当扎实。

但高阶 acceptance 仍欠缺：

1. **capital-aware profitability acceptance**
   - 没有锁住“预算紧时，deployable best action 会先排到前面”

2. **operator decision safety acceptance**
   - 没有锁住“degraded/fallback truth-source 永远不会以和 canonical 同等语义出现”

3. **economics uplift acceptance**
   - 没有锁住某次 economics 变更是否真的提高 retrospective capture / realized quality

4. **smoke != profitability**
   - `run_real_weather_chain_smoke.py` 与其测试更多是 plumbing / chain validation，不是盈利 acceptance

### 当前文档事实

- v2.0 docs 目前也更多强调 contract / phase / workstream，而不是 profitability acceptance criterion

### 风险描述

当前测试能证明“系统没坏”，但还不够证明“系统正在朝更稳定赚钱前进”。

### 对系统的直接影响

- 后续开发可能在不破坏现有 contracts 的前提下，悄悄损伤 deployable profitability

### 对“稳定赚钱”的影响

这是 delivery risk，不是当前 runtime bug，但它会直接影响下一阶段开发质量。

### 具体修复方案

新增 4 类 acceptance：

1. **capital scarcity acceptance**
2. **deployable action queue acceptance**
3. **economics-vs-retrospective uplift acceptance**
4. **truth-source operator safety acceptance**

### 需要改哪些模块

- `tests/test_allocator_v1.py`
- 新增 allocation-aware ranking tests
- `tests/test_ui_pages.py`
- retrospective harness tests

### 需要补哪些测试

见上。

### 是否需要 migration

不需要。

### 修复优先级顺序

**10**

---

## 6. Trading and Profitability Assessment

## 6.1 当前 Weather-first 策略最有希望靠什么赚钱

当前最有希望赚钱的地方，不是 agent，也不是链上功能，而是：

1. **天气阈值市场的 fair value mispricing**
2. **对 execution quality 做过一定折现后的高边际机会**
3. **在 market quality / mapping confidence / freshness 较高的市场里，筛出 operator 值得优先看的机会**

也就是说，Asterion 当前更接近的是：

- **“挑机会”的 alpha 系统**
- 还不是“全自动吃掉最多真钱 alpha”的系统

## 6.2 当前 alpha 最接近真实 edge 的地方在哪里

最接近真实 alpha 的地方是：

- Weather forecast 与 threshold market pricing 的结合
- calibration / threshold probability quality 已进入机会评估链
- execution feedback 已能把坏 cohort 压下来
- market quality / freshness / mapping confidence 也已经不是旁路展示，而会进 penalty

这条链条比很多只看 model fair value 的系统更真实。

## 6.3 当前哪些能力已经足以支撑“赚钱基础”

以下能力我认为已经足以支撑**赚钱基础**：

1. deterministic market discovery / Rule2Spec validation / station mapping
2. forecast + pricing + fair value + threshold quality 链
3. ranking_score 主排序
4. execution feedback suppression
5. allocation preview + paper execution integration
6. retrospective / predicted-vs-realized / execution science read models
7. operator console 的基本 action surface

## 6.4 当前最阻碍稳定赚钱的 5 个问题

按重要性排序：

1. **ranking 还不够 capital-aware**
2. **execution economics 仍有大量 heuristic**
3. **allocator/sizing 还不够强**
4. **operator throughput 还不够高**
5. **calibration ops 还没变成强运营闭环**

## 6.5 当前最阻碍规模化赚钱的 5 个问题

1. capital budgeting / concentration discipline 不够强
2. allocator 仍是 v1，更多像 preview + resize，而不是 portfolio deployment engine
3. deployable-value 不是主排序对象
4. operator surface 还不够“批量处理友好”
5. read-model / loader complexity 若继续上涨，会拖慢迭代速度

## 6.6 当前最阻碍高置信赚钱的 5 个问题

1. execution economics empirical realism 还不够
2. calibration freshness 不是强 gate
3. deployable action queue 还不够强
4. stale docs / truth-source drift 会误导判断
5. retrospective uplift acceptance 还不够

## 6.7 当前最强的盈利链条是什么

**最强链条**：

`forecast quality -> fair value -> calibration-aware opportunity assessment -> feedback-suppressed ranking_score -> deterministic runtime ordering -> operator review`

## 6.8 当前最弱的盈利链条是什么

**最弱链条**：

`ranking_score -> allocator -> deployable order queue -> realized capture / realized pnl -> next ranking iteration`

真正最弱的不是 forecasting 本身，而是**资金部署与执行经济学闭环**。

## 6.9 当前更大的问题到底是什么

不是单一问题，而是一个有主次的叠加：

1. **ranking economics 不够真钱化**
2. **allocation / capital discipline 不够强**
3. **execution capture / execution feedback closure 还不够深**
4. **operator workflow 仍偏研究型**
5. **truth-source delivery 仍有局部 drift**
6. calibration freshness 是问题，但不是第一大问题
7. forecast 质量本身并不是当前头号瓶颈

如果必须只选一个最大的：

**最大的瓶颈是 ranking economics + capital deployment 的组合问题。**

---

## 7. What Most Limits Stable Profitability Today

把当前最妨碍“稳定、规模化、高置信赚钱”的东西压缩成一句话：

**Asterion 现在已经能比较像样地识别机会，但还没有把“有限资本该先部署到哪里、部署多少、为什么”做成第一公民。**

这会带来 4 个具体后果：

1. 好机会排序不一定等于好部署顺序
2. allocator 更多像事后裁剪，而不是资本部署引擎
3. operator 看到很多有价值信息，但还要自己补完最终行动判断
4. 真实盈利闭环已经开始存在，但还不够强到能持续压缩错判和漏判

---

## 8. Recommended Repairs

## 8.1 先修的“前置地基”

这些问题不先收口，后续谈盈利提升会越来越虚：

1. allocator 自排序 / invariant hardening
2. startup + UI truth-source copy cleanup
3. stale supporting design cleanup
4. 增加 allocation-aware / operator safety acceptance tests

## 8.2 接着修的“直接影响赚钱”的主线

1. ranking v3：allocation-aware / deployable-value-first
2. allocator v2：budget / concentration / regime-aware sizing
3. execution priors v2：减少 heuristic，增强 empirical economics
4. operator action queue v2：deployable action first
5. calibration ops gating：让 stale calibration 真正影响 actionability

## 8.3 保留并加强，不建议推翻的基础

这些东西已经做对了，不建议大改：

- `ranking_score` 作为单一 primary score field 的 contract
- `trading.*` + `runtime.*` 的 persistence discipline
- constrained live boundary v2
- read-model catalog + truth-source checks
- Resolution Agent seam
- cold-path orchestration / smoke harness

---

## 9. Next-Stage Profitability Upgrade Plan

## 9.1 下个阶段总目标

把 Asterion 从“已经能较好识别天气机会的 operator console + constrained execution infra”，升级为：

**一个更擅长把有限资本部署到高置信、可执行、可解释机会上的 Weather-first trading system。**

更具体地说，下个阶段的目标是：

1. **更稳定赚钱**：减少排序与真实执行之间的失真
2. **更高置信赚钱**：让 stale calibration、弱 cohort、差 execution quality 更难混进 ready-now 队列
3. **更可规模化赚钱**：让 capital discipline 不再依赖 operator 脑补
4. **更可运营**：让 operator 更快更稳地做对决定
5. **更可审计**：继续保持 deterministic audit trail 与 truth-source discipline

## 9.2 下个阶段的非目标

- 不做 unattended live
- 不做 unrestricted live
- 不把精力优先放在更多 agent 展示层
- 不搞平行 `risk.*` schema
- 不优先扩更多 domain
- 不优先追求 fancy ML，而忽略 deployable economics

---

## 10. Workstreams and Phase Breakdown

## Workstream 1 — Capital-Aware Ranking and Allocation Discipline

### 目标

把当前机会排序升级成**可部署资本语义下的真钱排序**。

### 关键代码模块

- `domains/weather/opportunity/service.py`
- `asterion_core/risk/allocator_v1.py`
- `asterion_core/risk/portfolio_v3.py`
- `dagster_asterion/handlers.py`

### 需要新增或扩展的内容

#### contracts

- 在现有 `pricing_context_json` / `why_ranked_json` / allocation context 中增加：
  - `deployable_expected_pnl`
  - `deployable_notional`
  - `max_deployable_size`
  - `binding_limit_scope`
  - `binding_limit_key`
  - `capital_scarcity_penalty`
  - `concentration_penalty`

#### tables

- 优先扩已有 `runtime.allocation_decisions`
- 如 canonical policy 需要扩展，优先扩：
  - `trading.allocation_policies`
  - `trading.position_limit_policies`
  - 必要时新增 `trading.capital_budget_policies`
- **不建议新建 `risk.*` schema**

#### jobs

- `weather_allocation_preview_refresh` 继续保留
- 可以增加一个 `allocation-aware rerank` step，仍挂在现有 weather allocation / paper execution 工作流下

#### read models

- `ui.action_queue_summary`
- `ui.market_opportunity_summary`
- `ui.home_top_opportunity` 相关 surface

### 验收标准

- 在预算稀缺测试集里，排序能优先 deployable PnL 更高的机会
- allocator 输出与 UI action queue 排序一致
- operator 不再需要自己脑补“为什么推荐下这么多”

---

## Workstream 2 — Execution Economics and Feedback Closure v2

### 目标

把 execution feedback 从“会压分”升级成“更像真实执行经济学”。

### 关键代码模块

- `domains/weather/opportunity/service.py`
- `domains/weather/opportunity/execution_priors.py`
- `domains/weather/opportunity/execution_feedback.py`
- `domains/weather/opportunity/ranking_retrospective.py`

### 需要新增或扩展的内容

#### contracts

- richer prior confidence / prior coverage fields
- explicit `expected_submit_ack_rate`
- explicit `expected_working_timeout_rate`
- explicit `expected_adverse_fill_slippage`
- explicit `expected_edge_retention`

#### tables

- 可能扩 `weather.weather_execution_priors`
- 可能扩 `runtime.ranking_retrospective_rows`

#### jobs

- `weather_execution_priors_refresh` 继续 nightly
- retrospective 仍可先 manual，但建议增加更明确的 operator / research runbook

### 验收标准

- empirically-backed priors 在样本足够时覆盖 heuristics
- retrospective rows 能更清楚解释 capture / miss / distortion
- ranking change 的 uplift 能在 retrospective harness 中被量化

---

## Workstream 3 — Calibration Ops and Probability Quality Ops

### 目标

让 calibration 不只是 penalty 和 UI 指标，而是真正提升 actionability quality 的运营能力。

### 关键代码模块

- `domains/weather/forecast/calibration.py`
- `dagster_asterion/job_map.py`
- `ui/data_access.py`
- `ui/pages/system.py`
- `readiness_checker_v1.py`

### 需要新增或扩展的内容

#### contracts / read models

- calibration freshness summary
- stale/degraded impacted market counts
- per-source / per-station profile quality summary

#### jobs

- 保持 nightly refresh
- 增加 stale profile alerting / operator surfacing

### 验收标准

- stale calibration 会明确改变 actionability
- operator 能在首页/System 页直接看见 calibration freshness 风险
- docs 不再把 calibration refresh 写成 manual-only

---

## Workstream 4 — Operator Workflow and Action Surface Upgrade

### 目标

把 UI 从“研究 + console”进一步推向“高吞吐 deployable action surface”。

### 关键代码模块

- `ui/pages/home.py`
- `ui/pages/markets.py`
- `ui/pages/execution.py`
- `ui/data_access.py`
- `asterion_core/ui/ui_lite_db.py`

### 需要新增或扩展的内容

#### read models

- action queue enriched columns
- deployable-value summaries
- miss / distortion priority queue

#### tests

- operator action queue acceptance
- degraded source safety acceptance
- top action explanation acceptance

### 验收标准

- Top actions 能直接告诉 operator：
  - 下哪边
  - 下多大
  - 预期 deployable pnl
  - 为什么不是另一单
  - 当前主要 binding limit 是什么

---

## Workstream 5 — Delivery Hardening: Truth-Source, Docs, Acceptance

### 目标

防止 v2.0 在继续开发时重新进入 split-brain 和 drift。

### 关键代码模块 / 文档

- `start_asterion.sh`
- `ui/app.py`
- `asterion_core/ui/surface_truth_shared.py`
- active implementation docs
- supporting design docs

### 需要新增或扩展的内容

#### docs

- 更新 stale supporting design
- 清理 `V2_Implementation_Plan.md` 里已修复但仍写成 current issue 的内容

#### tests

- startup truth-source tests
- header/sidebar copy consistency tests
- active docs sanity checks

### 验收标准

- UI / startup / active docs / shared constants 的 phase/version 表达一致
- supporting design 不再把已修复的安全问题写成当前 reality

---

## 建议的阶段拆分

### Phase A — Foundation Repairs for Profitability Work

**目标**：先收口会影响后续盈利工作的基础 drift 与 allocator 不变量。

**交付物**：

- allocator self-sorting / invariant hardening
- startup / UI truth-source cleanup
- stale docs cleanup
- acceptance test baseline 扩展

**不做项**：

- 不扩大 live boundary
- 不做新 domain

**验收标准**：

- 所有 current truth-source surfaces 与 active docs 一致
- allocator 在乱序输入下仍输出正确顺序

### Phase B — Capital-Aware Ranking and Action Queue

**目标**：把排序真正推进到 deployable-value-first。

**交付物**：

- ranking v3 / allocation-aware rerank
- UI action queue v2
- deployable metrics in read models

**验收标准**：

- capital scarcity acceptance tests 通过
- operator 首页默认看到的就是“最值得部署”的 action queue

### Phase C — Execution Economics / Feedback v2

**目标**：减少 heuristic，增强 empirical execution modeling。

**交付物**：

- priors v2
- feedback closure enrichments
- retrospective uplift harness

**验收标准**：

- ranking changes 可用 retrospective harness 验证 uplift

### Phase D — Calibration Ops and Scaling Discipline

**目标**：把 calibration freshness 与 portfolio discipline 运营化。

**交付物**：

- stronger calibration gating
- richer policy fields / capital budget policies（若确有必要）
- scale-oriented operator surfaces

**验收标准**：

- stale calibration 不再只是“信息”，而是影响 actionability 的强信号
- capital deployment 不再主要依赖 operator 脑补

---

## 11. Testing and Acceptance Strategy

## 11.1 当前已较强的测试面

- runtime ordering
- ranking penalty / feedback suppression
- calibration v2 materialization
- truth-source checks
- UI loader contracts
- resolution operator review closure
- cold path orchestration
- real weather chain smoke
- migrations

## 11.2 下个阶段必须新增的 acceptance 面

### A. Capital-aware ranking acceptance

验证：

- 在预算受限时，系统优先 deployable value 更高的机会
- allocator preview 与 UI action queue 一致

### B. Deployable action queue acceptance

验证：

- 首页 / Markets 默认首屏就是最值得做的 action
- action queue 展示 binding limit / deployable pnl / source truth 状态

### C. Economics uplift acceptance

验证：

- execution priors / feedback 参数变化后，retrospective capture 指标确实改善

### D. Calibration ops acceptance

验证：

- stale/degraded calibration 会显式影响 ready_now / research_only 分类

### E. Truth-source operator safety acceptance

验证：

- degraded/fallback rows 绝不会以 canonical 语义出现
- UI / startup copy 与 active docs 不再 split-brain

---

## 12. Top 10 Highest-ROI Improvements

1. **ranking v3：deployable-value-first，不改 `ranking_score` 字段名，只改主语义**
2. **allocator v1 self-sort + invariant hardening**
3. **action queue v2：把 deployable size / pnl / binding limit 变成第一公民**
4. **execution priors v2：让 empirical terms 覆盖 heuristics**
5. **retrospective uplift harness：让 economics 变更可量化验证**
6. **calibration stale gating：把 freshness 变成强 actionability 信号**
7. **UI/startup truth-source cleanup**
8. **supporting design cleanup，特别是 boundary / calibration docs**
9. **read-model / loader modularization 与 fallback adapter 统一**
10. **operator miss/distortion priority queue**

---

## 13. What Not To Prioritize Yet

1. **不要优先做 unattended live**
   - 当前最大瓶颈不是 live boundary，而是 deployable economics

2. **不要默认新建 `risk.*` schema**
   - 当前 `trading.*` / `runtime.*` discipline 是对的
   - 预算 / limits / sizing 应优先在既有 discipline 内扩展

3. **不要优先扩新 domain**
   - 先把 Weather-first 赚钱链打透，ROI 更高

4. **不要把更多 agent surface 当成主线**
   - 当前 active seam 是 Resolution Agent
   - Rule2Spec/Data QA 已 deterministic 化

5. **不要把“更多指标”误当“更强 operator surface”**
   - 下一阶段应该提升行动质量，不是堆更多表

6. **不要把 smoke pass 当成 profitability pass**
   - `run_real_weather_chain_smoke.py` 很有价值，但它验证的是 chain reality，不是稳定赚钱能力

---

## 14. Appendix: Files Reviewed

### Active docs / overview

- `README.md`
- `AGENTS.md`
- `docs/00-overview/Documentation_Index.md`
- `docs/00-overview/Version_Index.md`
- `docs/00-overview/versions/v2.0/Asterion_Project_Plan.md`
- `docs/00-overview/versions/v2.0/DEVELOPMENT_ROADMAP.md`
- `docs/10-implementation/Implementation_Index.md`
- `docs/10-implementation/versions/v2.0/phase-plans/V2_Implementation_Plan.md`

### Supporting designs

- `docs/30-trading/Controlled_Live_Boundary_Design.md`
- `docs/30-trading/Execution_Economics_Design.md`
- `docs/40-weather/Forecast_Calibration_v2_Design.md`
- `docs/50-operations/Operator_Console_Truth_Source_Design.md`
- `docs/20-architecture/UI_Read_Model_Design.md`

### Trading / ranking / execution / risk

- `asterion_core/runtime/strategy_engine_v3.py`
- `asterion_core/execution/execution_gate_v1.py`
- `asterion_core/execution/trade_ticket_v1.py`
- `asterion_core/execution/live_submitter_v1.py`
- `asterion_core/risk/allocator_v1.py`
- `asterion_core/risk/portfolio_v3.py`
- `asterion_core/blockchain/chain_tx_v1.py`
- `asterion_core/signer/signer_service_v1.py`
- `asterion_core/contracts/live_boundary.py`

### Weather alpha / pricing / feedback

- `domains/weather/opportunity/service.py`
- `domains/weather/opportunity/execution_priors.py`
- `domains/weather/opportunity/execution_feedback.py`
- `domains/weather/opportunity/ranking_retrospective.py`
- `domains/weather/pricing/engine.py`

### Forecast / calibration / quality ops

- `domains/weather/forecast/calibration.py`
- `domains/weather/forecast/adapters.py`
- `domains/weather/forecast/service.py`
- `domains/weather/forecast/replay.py`
- `domains/weather/forecast/replay_validation.py`

### Rule/spec / validation / resolution

- `domains/weather/spec/rule2spec_validation.py`
- `domains/weather/resolution/persistence.py`
- `agents/weather/resolution_agent.py`

### UI / truth-source / read model

- `asterion_core/ui/surface_truth_shared.py`
- `asterion_core/ui/ui_lite_db.py`
- `asterion_core/ui/read_model_registry.py`
- `ui/data_access.py`
- `ui/app.py`
- `ui/pages/home.py`
- `ui/pages/markets.py`
- `ui/pages/execution.py`
- `ui/pages/agents.py`
- `ui/pages/system.py`
- `start_asterion.sh`

### Orchestration / readiness / smoke chain

- `dagster_asterion/handlers.py`
- `dagster_asterion/job_map.py`
- `asterion_core/monitoring/readiness_checker_v1.py`
- `scripts/run_real_weather_chain_smoke.py`

### Migrations / tests

- `sql/migrations/*.sql`
- `tests/test_execution_foundation.py`
- `tests/test_ranking_score_v2.py`
- `tests/test_execution_feedback_loop.py`
- `tests/test_execution_priors_feature_space.py`
- `tests/test_ranking_retro_harness.py`
- `tests/test_allocator_v1.py`
- `tests/test_allocation_preview_persistence.py`
- `tests/test_paper_execution_allocator_integration.py`
- `tests/test_calibration_profile_v2.py`
- `tests/test_calibration_materialization_status.py`
- `tests/test_calibration_freshness_penalty.py`
- `tests/test_ui_data_access.py`
- `tests/test_ui_pages.py`
- `tests/test_ui_loader_contracts.py`
- `tests/test_truth_source_checks.py`
- `tests/test_operator_truth_source.py`
- `tests/test_weather_agents.py`
- `tests/test_resolution_operator_review_closure.py`
- `tests/test_resolution_review_ui_actions.py`
- `tests/test_real_weather_chain_smoke.py`
- `tests/test_cold_path_orchestration.py`
- `tests/test_migrations.py`

---

## Final Bottom Line

如果只用一句话概括这次深度审查：

**Asterion 现在最大的提升空间，不在“再做更多基础设施”，而在“把已有 Weather-first 机会识别链，升级成更像真钱资本部署系统的 deployable economics + operator action engine”。**

当前代码已经有非常扎实的骨架：

- constrained execution infra 真实存在
- `ranking_score` / calibration / feedback / read-model 都是真实接线
- allocator seam、resolution seam、truth-source checks 也都已经成立

下一阶段最值得做的，不是推翻重来，而是：

- 让排序更资本敏感
- 让 allocator 更硬
- 让 execution economics 更经验化
- 让 operator surface 更行动导向
- 让 docs / truth-source / acceptance 更不容易漂移

只要这 5 件事做对，Asterion 才更有机会从“结构很像一个严肃系统”真正推进到“更稳定、更高置信地赚钱”。
