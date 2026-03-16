# Post-P4 Remediation Implementation Plan

**版本**: v1.2
**更新日期**: 2026-03-16
**状态**: active  
**定位**: `post-P4 remediation` 的 canonical 实施计划  
**输入来源**:
- [Project_Full_Assessment.md](../../analysis/Project_Full_Assessment.md)
- [Remediation_Plan.md](../../analysis/Remediation_Plan.md)
- [Current_Code_Reassessment.md](../../analysis/Current_Code_Reassessment.md)

---

## 1. 背景与目标

当前仓库已经完成 `Phase 0` 到 `Phase 4` 的 remediation 主体实现，但后续开发不应继续停留在“阶段计划待执行”的静态叙事上，而应进入一个明确的 **post-Phase-4 stabilization + next-work prioritization** 周期。

本计划的目标不是重新评估系统，而是把评估报告中已经识别出的关键问题，收口成一份后续逐阶段开发时可直接引用的 canonical 实施文档。

固定原则：

- 本文档是后续 remediation 开发的唯一实施入口
- `docs/analysis/` 中的报告保留为评估输入和历史材料，不作为实施 truth-source
- 当前系统重点不再是继续扩写 phase 文案，而是：
  - 维持 `Phase 0` 到 `Phase 4` 已落地能力的稳定性
  - 收口 active docs / tests / operator entry 的 truth-source 漂移
  - 在稳定基线之上规划 richer analytics、auth UX 和后续 constrained live decision phase

---

## 2. Current Accepted State

当前仓库已完成并接受的 remediation 能力包括：

- `Phase 0`
  - 状态治理收口
  - `start_asterion.sh` 语义重写
  - UI 全局 stale/error surface
- `Phase 1`
  - controlled-live capability manifest
  - 独立 secret env 前缀
  - app-level UI auth default-deny gate
- `Phase 2`
  - `OpportunityAssessment`
  - executable edge / EV / expected PnL ranking
  - runtime / UI 同源机会排序
- `Phase 3`
  - station mapping confidence / override reason
  - `weather.forecast_calibration_samples`
  - calibration-driven sigma lookup
  - source health / market quality inputs
- `Phase 4`
  - `ReadinessEvidenceBundle`
  - executed-only `ui.predicted_vs_realized_summary`
  - decision-center 形式的 `Home / Markets / Execution / Agents / System`

当前已落地的 canonical 对象至少包括：

- `OpportunityAssessment`
- calibration lookup / persisted calibration samples
- market quality / source health context
- `ReadinessEvidenceBundle`
- executed-only `predicted_vs_realized`
- constrained `real_clob_submit` backend
- UI/web minimal read-only runtime env
- `ui.watch_only_vs_executed_summary`
- `ui.market_research_summary`
- `ui.calibration_health_summary`

---

## 3. Residual Gaps / Next Work

### 3.1 稳定性与真相源

当前最优先的剩余问题不是继续扩能力，而是：

- active remediation plan 仍有“把已完成工作继续写成当前缺口”的漂移
- repo 入口文档对 Asterion / AlphaDesk 当前关系仍有部分冲突口径
- 全量测试必须持续保持全绿，任何 provider/runtime 回归都优先收口
- reassessment 中已确认的后续修复，应统一收口到本文档的 `Phase 5+ Remediation Roadmap`

### 3.2 交易能力下一层

Phase 2 到 Phase 4b 虽已落地，但后续仍有明确的增强空间：

- richer post-trade analytics beyond current summary layer
- 更完整的 watch-only research console
- 更细的 predicted-vs-realized 误差分解
- calibration / quality 的长期观测与 operator 解释面

### 3.3 Residual Gaps Repair Status

以下 residual gaps 已在 post-Phase-4 stabilization 之后继续收口：

- `Constrained Real Submitter v1`
  - 已落地 `real_clob_submit`
  - 仍保持 `manual-only + allowlist + auditable`
- `UI / Web Secret Minimization`
  - UI / web surfaces 只读取最小只读环境
  - 不再自动加载根目录 full `.env`
- `Phase 4b Post-Trade Analytics`
  - 已补 `watch-only vs executed`
  - 已补 `market research`
  - 已补 `calibration health` summaries

---

## 4. 盈利能力提升主线

当前系统最大的资本风险不是代码 exploit，而是交易经济模型过弱。后续 remediation 的交易主线固定按以下 5 条建议推进，并且每条建议都必须映射到具体 phase、交付物和验收标准。

### 4.1 Forecast Calibration First

- 按 `station / source / horizon / season` 建立历史 residual 数据集
- 停止把统一固定 sigma 作为长期方案
- 固定归入 `Phase 3`

### 4.2 Fair Value -> Executable Edge

- 把 `fee / spread / slippage / fill probability / depth` 纳入
- 明确区分：
  - `model fair value`
  - `execution-adjusted edge`
- 固定归入 `Phase 2`

### 4.3 Opportunity Ranking by Expected Value

- 排序主目标改成 `expected value / expected PnL`
- 不再由 UI heuristic 分数主导
- 固定归入 `Phase 2`

### 4.4 Market Quality Screen

- 优先过滤：
  - 深度差
  - spread 宽
  - 价格陈旧
  - mapping/confidence 低
- 固定归入 `Phase 3`

### 4.5 Predicted vs Realized Closed Loop

- 持续比较：
  - 预测 edge
  - 成交价格
  - 实际 PnL
  - 最终 resolution
- 固定归入 `Phase 4`

---

## 5. 阶段拆分

### Phase 0: Governance and Verification Closure

**当前状态**

- accepted

**目标**

- 让系统状态表达、验证链、closeout 口径、运维入口重新一致

**交付物**

- 状态口径收口后的 README / UI / phase 文档
- closeout / orchestration / settings drift 修复
- 可审计的 CI truth-source
- 重写后的 `start_asterion.sh`
- UI stale/error banner

**明确不做**

- 不调整交易经济模型
- 不推进 real submitter
- 不改变 current controlled live boundary 语义

**Exit Criteria**

- 文档状态不再领先于验证链
- closeout 与 orchestration 相关测试全绿
- UI 能明确区分 no data / read error / refresh in progress / degraded source
- `start_asterion.sh` 的 help、模式名和实际行为一致

### Phase 1: Live Boundary Hardening

**当前状态**

- accepted

**目标**

- 把当前 live boundary 从“工程约束”升级为更强的制度化边界

**交付物**

- controlled live capability manifest
- secrets 分层方案
- 无默认 UI 凭证
- signer / chain-tx / writerd 更明确的角色边界
- readiness capability boundary 摘要
- app-level UI auth gate
- controlled-live secret env 前缀切换：
  - `ASTERION_CONTROLLED_LIVE_SECRET_ARMED`
  - `ASTERION_CONTROLLED_LIVE_SECRET_APPROVAL_TOKEN`
  - `ASTERION_CONTROLLED_LIVE_SECRET_PK_<WALLET_ID_UPPER_SNAKE>`

**明确不做**

- 不实现 real live submitter
- 不扩大真钱 side effect 范围

**Exit Criteria**

- controlled-live secrets 不再与普通运行配置混放
- 无 capability manifest 时 controlled live 一律阻断
- 默认 UI 凭证完全移除
- signer / chain-tx / writerd denial path 有测试覆盖

### Phase 2: Opportunity Model Refactor

**当前状态**

- accepted

**目标**

- 建立机会评估的单一事实来源，让 runtime 和 UI 使用同一套执行感知排序

**交付物**

- `OpportunityAssessment` canonical contract
- `Executable Edge`
- `Expected Value / Expected PnL Ranking`
- `ui.market_opportunity_summary` 改为消费 opportunity domain 投影

**明确不做**

- 不引入黑盒 ML 排序
- 不让 UI 层自行决定主排序

**Exit Criteria**

- UI、runtime、paper execution candidate 的机会排序一致
- `opportunity_score` 不再由 UI heuristic 直接定义
- fee / spread / slippage / fill probability / depth 已进入 deterministic executable edge

### Phase 3: Mapping / Calibration / Coverage

**当前状态**

- accepted

**目标**

- 提升市场覆盖、mapping 稳定性和 forecast 校准质量

**交付物**

- `Forecast Calibration`
- `Market Quality Screen`
- station mapping confidence 体系
- title/rule regression corpus
- source freshness / degraded reason 机制
- canonical artifacts / contracts：
  - `weather.forecast_calibration_samples`
  - `weather.source_health_snapshots`
  - `weather.weather_station_map.mapping_method / mapping_confidence / override_reason`
  - `assessment_context_json` 中的 quality / freshness / confidence 字段

**明确不做**

- 不在本阶段推进真钱 submit 路径
- 不把 agent 用作一阶定价器

**Exit Criteria**

- mapping confidence 与 override reason 可落表、可读
- calibration dataset 可回放
- sigma / distribution 已切向 calibration-driven lookup
- market quality filtering 已进入机会链主路径

### Phase 4: Decision Console and Evidence Bundle

**当前状态**

- accepted

**目标**

- 把 UI 从 status console 升级为 decision console，并让 readiness 从状态词变成证据包

**交付物**

- `Predicted vs Realized Closed Loop`
- `ReadinessEvidenceBundle`
- decision-center 形式的 operator console
- predicted vs realized / freshness / decomposition panels

**明确不做**

- 不把 agent 变成交易主排序器
- 不在本阶段推进真钱 submit rollout

**Exit Criteria**

- 首页和 Markets 页都以决策支持为主，而不是状态摘要为主
- readiness 不再只暴露 `GO/NO-GO` 文案
- predicted vs realized 有稳定 read model 和 dashboard 输入

---

## 6. 每阶段详细实施项

### 5.1 Phase 0: 治理与验证链修复

**核心改动**

- 收口 README / UI / phase 文档状态
- 修复 closeout / orchestration / settings drift
- 将 closeout truth-source 接到 CI
- 重写 `start_asterion.sh`
- 给 UI 增加 stale/error banner

**涉及的系统边界**

- 文档导航
- 测试入口与 CI
- 运维启动入口
- UI data freshness / read error surface

**需要新增/重构的接口或 artifact**

- closeout verification artifact
- UI stale/error status surface

**测试与验收**

- closeout/orchestration/docs/UI stale banner 相关测试
- `start_asterion.sh` 模式 smoke test

**文档同步要求**

- README、Documentation Index、Implementation Index、相关 phase/checklist 一并收口

### 5.2 Phase 1: 边界硬化

**核心改动**

- 移出通用 `.env` 的 controlled-live secrets
- 移除默认 UI 认证凭证
- 增加 capability manifest
- 收紧 signer / chain-tx / writerd 运行边界
- `config/controlled_live_smoke.json` 只保留 allowlist / cap，不再声明 secret env mapping

**涉及的系统边界**

- controlled live runtime
- UI auth
- signer / chain tx / storage writer role

**需要新增/重构的接口或 artifact**

- `controlled_live_capability_manifest`
- capability-aware readiness summary
- UI auth default-deny gate

**测试与验收**

- secrets 分层 tests
- 无 manifest 阻断 tests
- 无默认 UI 凭证 tests

**文档同步要求**

- runbooks 中显式写清 capability boundary

### 5.3 Phase 2: 机会评估 SSOT

**核心改动**

- 抽出 `OpportunityAssessment`
- 用 `Executable Edge` 替换单纯 fair value edge
- 用 `Expected Value / Expected PnL Ranking` 替换 UI heuristic 主排序

**涉及的系统边界**

- pricing
- runtime ranking
- UI opportunity summary

**需要新增/重构的接口或 artifact**

- `OpportunityAssessment`
- `ui.market_opportunity_summary` 新投影

**Phase 2 需要固定写清的对象**

`OpportunityAssessment` 至少包含：

- `model_fair_value`
- `execution_adjusted_fair_value`
- `reference_price`
- `fees_bps`
- `slippage_bps`
- `fill_probability`
- `depth_proxy`
- `edge_bps_model`
- `edge_bps_executable`
- `expected_value_score`
- `expected_pnl_score`
- `actionability_status`
- `ranking_score`

**数据来源**

- `weather.weather_fair_values`
- market price / spread / depth / fill proxy
- readiness / market quality / ops readiness inputs

**测试与验收**

- opportunity contract unit tests
- runtime/UI ranking parity tests
- executable-edge decomposition tests

**文档同步要求**

- UI 文档与 strategy/runtime 说明必须改为“排序主目标是 EV / expected PnL”

### 5.4 Phase 3: 天气链能力增强

**核心改动**

- 建立 `Forecast Calibration`
- 建立 `Market Quality Screen`
- 增加 mapping confidence / method / override reason
- 建 regression corpus 与 source freshness / degradation policy

**涉及的系统边界**

- station mapping
- forecast adapters
- market discovery / market screening
- calibration / source health

**需要新增/重构的接口或 artifact**

- forecast calibration dataset
- market quality screen output
- mapping confidence summary

**Phase 3 需要固定写清的对象**

`forecast calibration dataset` / `market quality screen` 至少包含：

- `station_id`
- `source`
- `forecast_horizon_bucket`
- `season_bucket`
- `residual`
- `mapping_confidence`
- `price_staleness_ms`
- `spread_bps`
- `depth_proxy`
- `market_quality_status`

**数据来源**

- historical forecast vs resolved observation
- live market spread/depth/staleness
- station mapping review / override records

**测试与验收**

- station mapping regression corpus
- forecast calibration replay tests
- degraded source/freshness tests

**文档同步要求**

- Weather 领域文档中不再把固定 sigma 视为长期目标

### 5.5 Phase 4: 决策中心与证据包

**核心改动**

- UI 从 status console 升级为 decision console
- readiness 从 `GO/NO-GO` 升级为 `ReadinessEvidenceBundle`
- 增加 `Predicted vs Realized Closed Loop`

**涉及的系统边界**

- operator UI
- readiness artifacts
- post-trade / post-resolution analytics

**需要新增/重构的接口或 artifact**

- `ReadinessEvidenceBundle`
- `predicted_vs_realized` read model
- decision console panels

**Phase 4 需要固定写清的对象**

`predicted_vs_realized` read model / dashboard input 至少包含：

- `predicted_edge_bps`
- `expected_fill_price`
- `realized_fill_price`
- `realized_pnl`
- `resolution_value`
- `forecast_freshness`
- `source_disagreement`
- `post_trade_error`

**数据来源**

- pre-trade opportunity assessment
- realized fills / order execution data
- final resolution / observed value
- forecast freshness / disagreement diagnostics

**测试与验收**

- decision console smoke tests
- readiness evidence bundle tests
- predicted-vs-realized read model tests

**文档同步要求**

- UI runbook / remediation plan / README 都要明确“agent 页只做 exception review”

---

## 7. 阶段间依赖与执行顺序

历史 remediation 实施顺序如下：

1. `Phase 0`
2. `Phase 1`
3. `Phase 2`
4. `Phase 3`
5. `Phase 4`

强制依赖：

- `Phase 0` 不完成，不进入 `Phase 1`
- `Phase 2` 不完成，不进入真钱交易能力讨论
- `Phase 4` 之前，不重新定义 `ready for controlled live rollout decision`
- `Phase 4` 完成前，UI 不得把 readiness 文案等同于真钱放行

执行原则：

- 先修治理与边界，再修经济核心，再修覆盖和 UI
- 不允许跳过 `Phase 2` 直接做“更高级 UI”
- 不允许在 `Phase 3` 之前把 calibration 问题继续留给固定 sigma

---

## 8. 统一验收口径

全局验收原则固定为：

- 文档状态不得领先于验证链
- UI 不得主导机会主评分
- agent 不进入一阶定价/执行排序
- controlled live 仍保持 `default-off + manual-only + auditable`
- readiness 必须由 evidence bundle 支撑
- 任何 “更能赚钱” 的改进，都必须落到：
  - 数据对象
  - 排序逻辑
  - 校准闭环
  - predicted vs realized 验证

---

## 9. 后续实施规则

- 后续每次具体开发，必须显式引用本计划的具体 phase 或子阶段
- 新增 remediation runbook / checklist / read model 时，必须同步更新索引
- 不得再直接把未来目标写成当前状态
- `docs/analysis/` 继续保留为评估与历史输入，不替代本计划
- 从 `Phase 2` 开始，盈利能力主线必须成为正式交付目标，而不是附录性备注
- `Phase 0` 到 `Phase 4` 完成后，新的工作应优先按 stabilization / analytics / auth UX / constrained-live decision 分组，而不是假装 remediation 主体尚未开始

---

## 10. Phase 5+ Remediation Roadmap

### 10.1 Reassessment Intake

本章节基于 [Current_Code_Reassessment.md](../../analysis/Current_Code_Reassessment.md) 增补，目的不是重写 `Phase 0` 到 `Phase 4` 的已完成历史，而是把 reassessment 中仍然成立的剩余问题拆成新的、可逐阶段执行的 remediation phases。

固定解释规则：

- `Accepted State`
  - 仍以本文 `Phase 0` 到 `Phase 4`、`Residual Gaps Repair Status` 为准
- `Residual Gaps`
  - 只指 reassessment 中在当前代码现实下仍然成立的缺口
- `Next Implementation Phases`
  - 只指本章节新增的 `Phase 5` 到 `Phase 9`

当前后续开发默认以本章节为准，不再把 reassessment 中的剩余问题散落保存在分析报告里。

### 10.2 Residual Gaps To Close

Reassessment 中确认仍需继续收口的主线固定为：

- execution math correctness
- constrained live boundary intrinsic enforcement
- capability manifest / submitter truth-source strengthening
- calibration / ranking / executable edge quality
- post-trade execution science
- operator wording / docs truth-source cleanup

这些问题都不应通过新建第二份 canonical 实施文档来处理，而应在本文中继续顺延为新的阶段路线。

### 10.3 Phase 5: Execution Math and Gate Parity

**状态**

- accepted (`2026-03-16`)

**目标**

- 先修 correctness bug 和 boundary parity，不先扩功能

**交付物**

- SELL-side executable edge 方向修复
- BUY/SELL 对称的 `edge_bps / expected_pnl / ranking_score` 定义
- `ARMED=true` 在所有真实 side-effect 路径上的一致 enforcement
- handler 外 direct service call denial-path 覆盖

**明确不做**

- 不改 capability model 结构
- 不扩 `real_clob_submit` 功能面
- 不引入新 analytics

**新增/重构接口**

- execution-side normalization helper
- live side-effect guard helper
- BUY/SELL symmetry decomposition helpers

**测试与验收**

- BUY/SELL executable-edge symmetry tests
- real submit / real chain tx / controlled-live smoke 的 arming parity tests
- direct-call denial-path tests

**Exit Criteria**

- SELL-side 排序、trade ticket、UI decomposition 一致
- 无 `ARMED=true` 时任何 real submit / real chain tx 都 blocked
- direct service path 不能绕过 canonical gate

### 10.4 Phase 6: Intrinsic Submitter Boundary and Capability Attestation

**当前状态**

- accepted

**目标**

- 把 constrained real submit boundary 从 handler-centric 升级成 service-intrinsic and attestable

**交付物**

- submitter shell / backend 自身能力边界校验
- manifest 从“有用 artifact”升级成 submitter 强约束输入之一
- 真实 submit path 的 machine-readable boundary attestation
- manifest + readiness + approval token + arming 的统一 decision record
- `runtime.live_boundary_attestations`

**明确不做**

- 不做 unrestricted live
- 不做 KMS / HSM
- 不做自动资金部署

**新增/重构接口**

- `SubmitterBoundaryAttestation` 或等价 capability decision object
- submitter-side capability verification helper
- submitter decision record schema

**测试与验收**

- manifest / attestation enforcement tests
- submitter self-gating tests
- rejection audit path persistence tests

**Exit Criteria**

- submitter backend 即使被单独调用，也必须自证 boundary 满足
- manifest 缺失/不一致时，backend 自身拒绝
- 所有 rejection 都可审计并落入 canonical runtime audit path

### 10.5 Phase 7: Ranking and Calibration Quality Upgrade

**当前状态**

- accepted (`2026-03-16`)

**目标**

- 把 calibration 和 source quality 从“已接入”提升成“真正影响机会质量和排序”

**交付物**

- calibration health 进入 ranking 主路径
- executable edge 的 uncertainty / confidence penalty
- source freshness、mapping confidence、market quality 的统一 ranking penalty
- 更稳的 deterministic sigma bucket / weight / coverage 策略

**明确不做**

- 不引入黑盒 ML
- 不默认新增 DB schema，除非现有 JSON / read models 无法承载

**新增/重构接口**

- ranking penalty decomposition fields
- calibration confidence / sample sufficiency summary
- uncertainty-adjusted executable edge contract extension
- `assessment_context_json` 中的 calibration health / uncertainty penalty / ranking penalty reasons

**测试与验收**

- ranking penalty / calibration confidence tests
- fallback with degradation tests
- UI / runtime / paper candidate ranking parity regression

**Exit Criteria**

- calibration miss / sparse sample / stale source 会真实影响 ranking
- UI / runtime / paper candidate 继续消费同一排序语义
- 无 calibration 数据时仍可 fallback，但会被显式降权

### 10.6 Phase 8: Post-Trade Execution Science

**当前状态**

- accepted

**目标**

- 把当前 v1 `predicted_vs_realized` 和 `watch-only vs executed` 提升为真实研究与策略改进可用的分析层

**交付物**

- richer executed-only 复盘：
  - partial fills
  - cancel / reject buckets
  - unresolved lag
  - capture ratio by strategy / market / wallet
- watch-only vs executed 的 missed-opportunity analysis
- miss reason taxonomy
- execution / forecast / ranking distortion 三类归因

**明确不做**

- 不新增第六个页面
- 不做全新 BI 系统
- 不把 watch-only 研究面变成独立产品

**新增/重构接口**

- richer `ui.predicted_vs_realized_summary`
- richer `ui.watch_only_vs_executed_summary`
- research-oriented `ui.execution_science_summary` 或等价现有表扩展
- miss-reason / distortion-reason contract

**测试与验收**

- capture ratio / miss reason aggregation tests
- post-trade error decomposition tests
- watch-only 与 executed 分层一致性 tests

**Exit Criteria**

- operator 能区分模型错、排序错、没做、做了但执行差
- analytics 来自 canonical execution / weather / resolution facts
- UI 不发明第二套 analytics heuristic

### 10.7 Phase 9: Operator Surface and Truth-Source Cleanup

- accepted (`2026-03-16`)

**目标**

- 收口 reassessment 里剩余的 UI 文案漂移和文档真相源问题

**交付物**

- Agents / runtime / operator wording drift 修复
- `README / AGENTS / Asterion_Project_Plan` 等入口的边界口径统一
- system / home / agents 页中的边界提示统一
- active docs 不再把已完成 work 写成 current gap

**明确不做**

- 不做大 UI IA 重构
- 不做 marketing 化重写

**新增/重构接口**

- operator boundary wording baseline
- docs truth-source checklist for active entry docs

**测试与验收**

- docs drift scans
- UI wording / operator boundary smoke
- active doc navigation consistency checks

**Exit Criteria**

- 当前入口文档与 UI 首页/系统页边界口径一致
- 不再把已完成 work 写成 current gap
- 不再把当前系统写成 unrestricted live stack
- app shell / Home / Markets / Execution / Agents / System wording 已统一到 `operator console + constrained execution infra`

### 10.8 Cross-Phase Acceptance Rules

从 `Phase 5` 开始，所有后续 remediation phases 固定遵守以下跨阶段验收规则：

- 不新增平行 execution ledger
- 不让 agent 回到主排序
- 不默认扩大真钱边界
- 文档不得领先于代码与测试
- 后续各 phase 的最小测试主线必须显式写入并作为 acceptance 的一部分

后续各 phase 的最小测试主线固定为：

- `Phase 5`
  - BUY/SELL executable-edge symmetry
  - real side-effect arming parity
  - direct-call denial-path
- `Phase 6`
  - manifest / attestation enforcement
  - submitter self-gating
- `Phase 7`
  - ranking penalty / calibration confidence
  - fallback with degradation
- `Phase 8`
  - capture ratio / miss reasons / post-trade error decomposition
- `Phase 9`
  - docs drift
  - UI wording
  - operator boundary smoke
