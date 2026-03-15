# Post-P4 Remediation Implementation Plan

**版本**: v1.0  
**更新日期**: 2026-03-15  
**状态**: active  
**定位**: `post-P4 remediation` 的 canonical 实施计划  
**输入来源**:
- [Project_Full_Assessment.md](../../analysis/Project_Full_Assessment.md)
- [Remediation_Plan.md](../../analysis/Remediation_Plan.md)

---

## 1. 背景与目标

当前仓库已经完成 `P0` 到 `P4` 的主要 scaffold，但后续开发不应继续停留在“阶段已关闭”的静态叙事上，而应进入一个明确的 **post-P4 remediation** 周期。

本计划的目标不是重新评估系统，而是把评估报告中已经识别出的关键问题，收口成一份后续逐阶段开发时可直接引用的 canonical 实施文档。

固定原则：

- 本文档是后续 remediation 开发的唯一实施入口
- `docs/analysis/` 中的报告保留为评估输入和历史材料，不作为实施 truth-source
- 当前系统重点不再是继续扩写 phase 文案，而是逐步修复：
  - 状态治理与验证闭环
  - live boundary / secrets / 安全边界
  - forecast -> pricing -> ranking 的经济核心
  - station mapping / calibration / 市场覆盖
  - operator UI / readiness evidence / 运维入口

---

## 2. 当前问题总览

### 2.1 状态治理与验证闭环

当前代码、测试、README、phase plan、closeout checklist 之间仍存在状态漂移。  
核心风险不是页面文案不一致，而是组织可能在验证链未闭环时，误以为系统已经具备更高阶段的可信度。

### 2.2 Live Boundary / Secrets / 安全边界

当前 controlled live smoke 的工程边界已经存在，但更偏 `env + allowlist + 审批 token + 代码纪律`，尚未升级成更强的制度化运行边界。

### 2.3 Forecast -> Pricing -> Ranking 经济核心

当前天气链真正的资本风险不是代码 crash，而是：

- 概率错
- 边错
- 排序错
- 执行假设错

forecast uncertainty、fair value、机会排序目前仍偏 heuristic / placeholder 级别。

### 2.4 Station Mapping / Calibration / 市场覆盖

station mapping 仍高度依赖 override / catalog；多市场覆盖能力、mapping confidence、forecast calibration 数据都还不够强，容易造成 coverage 不稳与错价。

### 2.5 Operator UI / Readiness Evidence / 运维入口

UI 已经具备较好的 operator console 骨架，但还没有成为真正的决策中心；`start_asterion.sh` 与实际系统能力之间也仍存在语义漂移。

---

## 3. 盈利能力提升主线

当前系统最大的资本风险不是代码 exploit，而是交易经济模型过弱。后续 remediation 的交易主线固定按以下 5 条建议推进，并且每条建议都必须映射到具体 phase、交付物和验收标准。

### 3.1 Forecast Calibration First

- 按 `station / source / horizon / season` 建立历史 residual 数据集
- 停止把统一固定 sigma 作为长期方案
- 固定归入 `Phase 3`

### 3.2 Fair Value -> Executable Edge

- 把 `fee / spread / slippage / fill probability / depth` 纳入
- 明确区分：
  - `model fair value`
  - `execution-adjusted edge`
- 固定归入 `Phase 2`

### 3.3 Opportunity Ranking by Expected Value

- 排序主目标改成 `expected value / expected PnL`
- 不再由 UI heuristic 分数主导
- 固定归入 `Phase 2`

### 3.4 Market Quality Screen

- 优先过滤：
  - 深度差
  - spread 宽
  - 价格陈旧
  - mapping/confidence 低
- 固定归入 `Phase 3`

### 3.5 Predicted vs Realized Closed Loop

- 持续比较：
  - 预测 edge
  - 成交价格
  - 实际 PnL
  - 最终 resolution
- 固定归入 `Phase 4`

---

## 4. 阶段拆分

### Phase 0: Governance and Verification Closure

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

## 5. 每阶段详细实施项

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

## 6. 阶段间依赖与执行顺序

固定顺序如下：

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

## 7. 统一验收口径

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

## 8. 后续实施规则

- 后续每次具体开发，必须显式引用本计划的具体 phase 或子阶段
- 新增 remediation runbook / checklist / read model 时，必须同步更新索引
- 不得再直接把未来目标写成当前状态
- `docs/analysis/` 继续保留为评估与历史输入，不替代本计划
- 从 `Phase 2` 开始，盈利能力主线必须成为正式交付目标，而不是附录性备注
