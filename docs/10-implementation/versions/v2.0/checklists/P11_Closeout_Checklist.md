# P11 Closeout Checklist

**版本**: v1.0
**更新日期**: 2026-03-23
**阶段**: `v2.0 / Phase 11`
**状态**: accepted closeout checklist
**主题**: Opportunity Triage / Execution Intelligence Agent

---

> 本文件保留为 `Phase 11` 的 accepted closeout checklist。
> `Phase 11` 已 accepted；当前没有比 `Phase 11` 更新的 tranche-specific implementation plan 已打开。
> umbrella active implementation contract 仍是 [V2_Implementation_Plan.md](../phase-plans/V2_Implementation_Plan.md)。

## 1. Closeout Items

1. advisory-only triage contract / persistence / invocation seam 已落地
   - `AgentType.OPPORTUNITY_TRIAGE`
   - `agent.operator_review_decisions`
   - `weather_opportunity_triage_review`
2. triage request assembly 只读 persisted facts
   - `tests.test_opportunity_triage_request_assembly`
   - `tests.test_opportunity_triage_job`
3. `ui.opportunity_triage_summary` 已 roll up latest triage / operator decision state
   - `tests.test_opportunity_triage_summary`
   - `tests.test_opportunity_triage_operator_review`
4. replay / evaluation 不再只有 proxy；`agent.evaluations` 同时存在：
   - `operator_outcome_proxy`
   - `replay_backtest`
   - `tests.test_opportunity_triage_evaluation`
   - `tests.test_opportunity_triage_replay_evaluation`
5. advisory gate 已显式化
   - `Home` gated
   - `Markets + Agents` always visible
   - `tests.test_p11_advisory_gate_acceptance`
   - `tests.test_p11_operator_surface_acceptance`
6. timeout / parse_error / failure 已收口成稳定 failed overlay status
   - `tests.test_opportunity_triage_timeout_isolation`
7. `System` 已直接显示 triage runtime + advisory gate summary
   - `tests.test_p11_system_runtime_summary`
   - `tests.test_ui_pages`
8. `P11` / `V2` / 入口导航文档 closeout wording 已同步
9. `P11_Closeout_Checklist.md` 已进入导航并作为最近 accepted tranche 的 closeout checklist 保留

## 2. Minimum Acceptance

- `replay_backtest` 与 `operator_outcome_proxy` 都有真实 persisted evaluation rows
- `Home` 不会在 gate 未开启时把 triage 当成 default-on advisory
- `Markets + Agents` 的 triage action 状态一致
- `timeout / parse_error / failure` 不影响 canonical queue
- `System` 能直接显示 triage runtime + advisory gate summary
- `P11` / `V2` / 导航文档不再把当前状态写成“只是刚开始实现”
- `git diff --check` 通过
