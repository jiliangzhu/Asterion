# Asterion P11 Implementation Plan

**版本**: v1.0
**更新日期**: 2026-03-23
**阶段**: `v2.0 / Phase 11`
**状态**: accepted closeout record
**主题**: Opportunity Triage / Execution Intelligence Agent

---

> 本文件现保留为 `v2.0 / Phase 11` 的 accepted closeout record。
> umbrella active implementation contract 仍是 [V2_Implementation_Plan.md](./V2_Implementation_Plan.md)。
> 当前仓库阶段状态统一表达为：`P4 accepted; post-P4 remediation accepted; v2.0 implementation active`。
> `Phase 11` 固定建立在 `Phase 10 accepted` 基线上；当前 `Phase 11` 已 accepted 并完成 closeout。
> 当前没有比 `Phase 11` 更新的 tranche-specific implementation plan 已打开；后续新 tranche 打开前，`P11` 保留为最近 accepted tranche record。
> 本文件主要以 [00_0322_Asterion_Assessment.md](../../../../analysis/00_0322_Asterion_Assessment.md) 中的 `Opportunity Triage / Execution Intelligence Agent` 设计为 planning input。

## 1. Summary

`Phase 11` 的目标是在 `P10` deterministic execution-intelligence foundation 之上，引入 advisory-only 的 triage agent，并把 triage overlay 推进到 persisted operator surfaces。

固定目标：

- 帮 operator 做 opportunity prioritization、exception triage、execution interpretation
- 让 triage recommendation 变成 persisted、可回放、可审计、可评估的 second-order evidence
- 在不触碰 canonical execution path 的前提下，继续提升 operator throughput 和 ROI

固定边界：

- agent 不进入 canonical execution path
- agent 不替代 ranking / allocator / gate / readiness
- agent 不改写 `trading.*`
- agent failure 不阻塞主交易链路
- 不新增任何 live side-effect shortcut

## 2. Current Code Reality and Why P11

assessment 已明确指出：在 execution foundation、action queue hygiene、microstructure serving、priors grain、allocator scheduling 没有进一步 deterministic 强化前，引入 agent 的 ROI 不够高。

因此 `P11` 固定建立在 `P10 accepted` 之上，并假定以下基线已经成立：

- execution-intelligence summary 已 deterministic materialized
- operator queue 已经更干净，`blocked` pollution 已收口
- priors serving grain 和 fallback 已更稳
- allocator scheduling 已比当前更接近 capital scheduler

当前代码主干已经进入 `P11`：

- `AgentType.OPPORTUNITY_TRIAGE`
- `OpportunityTriageAgentRequest`
- `OpportunityTriageAgentOutput`
- `agent.operator_review_decisions`
- `weather_opportunity_triage_review`
- `ui.opportunity_triage_summary`
- `Home` 只读 triage overlay
- `Markets + Agents` triage operator actions

在这个前提下，`P11` 不再是纯 planning，也不再是 closeout pending；它已经成为 accepted closeout baseline。

## 3. Workstreams

### WS1. Agent Contract and Invocation Path

目标：

- 在现有 `agent.*` 体系内新增 `Opportunity Triage` agent type

固定实现：

- invocation 只从现有 review / advisory seam 进入
- 不允许触达：
  - order routing
  - signing
  - submitter
  - canonical execution tables
- invocation failure 不阻塞主交易链路
- triage 的 operator accept / ignore / defer 结果落到通用表 `agent.operator_review_decisions`
- batch + rerun 共用同一 handler / job seam：`weather_opportunity_triage_review`

### WS2. Opportunity Triage Input Assembly

目标：

- 新增 `OpportunityTriageAgentRequest`
- 输入必须来自真实 persisted facts，而不是页面拼装文本

固定输入面：

- `ui.market_opportunity_summary`
- `ui.action_queue_summary`
- `ui.market_microstructure_summary`
- `why_ranked_json`
- `pricing_context_json`
- `P10` execution-intelligence summary
- calibration gate / capital policy / delivery status / operator bucket

固定输入约束：

- 必须显式带 source provenance
- 必须显式带 freshness
- 必须显式带 fallback / degraded truth
- 不允许 agent 基于 UI copy 推断 canonical state
- batch job 的 primary input source 固定为 UI replica / persisted surfaces，不是页面层

### WS3. Structured Output, Persistence, and Review

目标：

- agent 输出必须是结构化 triage result，而不是自由文本建议

最小输出：

- `triage_status`
- `priority_band`
- `triage_reason_codes`
- `execution_risk_flags`
- `recommended_operator_action`
- `confidence_band`
- `supporting_evidence_refs`

固定持久化：

- 继续复用：
  - `agent.invocations`
  - `agent.outputs`
  - `agent.reviews`
  - `agent.evaluations`
- operator accept / ignore / defer 决策固定写入：
  - `agent.operator_review_decisions`

### WS4. Operator Surface Integration

目标：

- 新增 `ui.opportunity_triage_summary`
- 让 triage recommendation 进入 Home / Markets / Agents 的 persisted operator surface

固定实现：

- 页面只消费 persisted triage summary
- triage recommendation 必须与 deterministic queue 并排展示：
  - ranking / deployment truth
  - triage recommendation
  - review state
- `Home` 只读 overlay，不提供 accept / ignore / defer
- `Markets + Agents` 提供 accept / ignore / defer
- agent 不会自动执行

### WS5. Replay / Evaluation / Safety Closure

目标：

- 证明 agent 真的有 ROI，而不只是“看起来聪明”

固定比较面：

- baseline deterministic queue
- deterministic + triage agent overlay

固定评价重点：

- operator throughput uplift
- queue cleanliness
- false positive / false escalation
- realized decision quality proxy

固定安全要求：

- replay / evaluation acceptance 先于 default-on operator advisory
- agent advisory-only
- agent outputs 可回放、可审计、可评估
- failure / timeout / parse_error 只影响 triage overlay，不影响 canonical queue / trading path

## 4. Public Interfaces and Persistence

允许新增：

- `OpportunityTriageAgentRequest`
- `OpportunityTriageAgentOutput`
- `agent.operator_review_decisions`
- `ui.opportunity_triage_summary`

继续复用：

- `agent.invocations`
- `agent.outputs`
- `agent.reviews`
- `agent.evaluations`

固定不允许：

- 直接执行型 agent contract
- 改写 `trading.*` 的 agent path
- 任何 live side-effect shortcut

## 5. Tests and Acceptance

固定测试面：

- request assembly contract tests
- structured output / persistence tests
- UI triage summary tests
- replay / evaluation harness tests
- failure isolation tests
- operator review workflow tests

当前 accepted closeout 已落地并需继续守住的 acceptance 文件：

- `tests.test_opportunity_triage_request_assembly`
- `tests.test_opportunity_triage_agent_contract`
- `tests.test_opportunity_triage_job`
- `tests.test_opportunity_triage_summary`
- `tests.test_opportunity_triage_operator_review`
- `tests.test_opportunity_triage_evaluation`
- `tests.test_opportunity_triage_replay_evaluation`
- `tests.test_opportunity_triage_timeout_isolation`
- `tests.test_p11_advisory_gate_acceptance`
- `tests.test_p11_system_runtime_summary`
- `tests.test_p11_operator_surface_acceptance`

固定 acceptance：

- agent advisory-only
- agent failure non-blocking
- agent outputs 可回放、可审计、可评估
- 至少一个 replay / evaluation surface 证明它有潜在 ROI，而不是只靠主观印象
- `Home` 不承担 triage action write path
- `Markets + Agents` 的 operator action 状态必须一致
- triage request 只读 persisted facts，不依赖页面拼装

## 6. Accepted Closeout Summary

`P11` accepted closeout 已固定包含：

1. replay / evaluation 从 proxy 提升为真实闭环
2. advisory gate 显式化，不让未验证 overlay 混入 `Home` 主决策焦点
3. timeout / parse_error / failure isolation acceptance
4. `System` triage runtime summary
5. final doc sync + closeout checklist

accepted closeout checklist 见：

- [P11_Closeout_Checklist.md](../checklists/P11_Closeout_Checklist.md)

## 7. Assumptions and Defaults

- `Phase 11` 固定建立在 `Phase 10 accepted` 基线上
- triage agent 默认严格复用现有 `agent.*` review / evaluation paths
- 若实现中发现需要改写 canonical execution path，则该工作不属于 `P11`
- `P11` 不允许跳过 replay / evaluation acceptance 直接变成 operator hard dependency
- 当前状态是 accepted closeout record，不再承担 current tranche 身份
