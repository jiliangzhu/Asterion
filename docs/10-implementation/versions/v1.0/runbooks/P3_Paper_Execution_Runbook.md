# P3 Paper Execution Runbook

**版本**: v1.0
**更新日期**: 2026-03-11
**阶段**: `P3`
**状态**: 生效中

---

## 1. 目标

本 runbook 定义 `P3 paper execution` 的 canonical operator 入口、只读面、排查路径与 readiness 解释方式。

source of truth 顺序：

1. `asterion_core/`、`dagster_asterion/`、`sql/migrations/`
2. [P3_Implementation_Plan.md](../phase-plans/P3_Implementation_Plan.md)
3. 本 runbook

说明：

- `paper execution` 不是 live trading
- 任何 signer / wallet / chain side effects 都不在本 runbook 范围内
- agent 仍在执行路径之外；future `Daily Review Agent` 只消费 `ui.daily_review_input`

---

## 2. Canonical Data Flow

当前 `P3` 的 paper execution 主链固定为：

```text
weather.weather_watch_only_snapshots
-> runtime.strategy_runs
-> runtime.trade_tickets
-> capability.execution_contexts
-> runtime.gate_decisions
-> trading.orders
-> trading.order_state_transitions
-> trading.reservations
-> trading.fills
-> trading.inventory_positions
-> trading.exposure_snapshots
-> trading.reconciliation_results
-> runtime.journal_events
-> ui.execution_ticket_summary
-> ui.execution_run_summary
-> ui.execution_exception_summary
-> ui.paper_run_journal_summary
-> ui.daily_ops_summary
-> ui.daily_review_input
-> ui.phase_readiness_summary
```

---

## 3. Operator 只读面

### `ui.execution_ticket_summary`

用途：

- 看单 ticket 当前状态
- 看 gate/order/fill/reconciliation/latest transition

重点字段：

- `execution_result`
- `order_status`
- `reconciliation_status`
- `operator_attention_required`
- `latest_transition_to_status`
- `latest_journal_event_type`

### `ui.execution_run_summary`

用途：

- 看 run 级聚合结果

重点字段：

- `ticket_count`
- `gate_allowed_count`
- `filled_count`
- `reconciliation_mismatch_count`
- `attention_required_count`

### `ui.execution_exception_summary`

用途：

- 专门看异常 ticket

重点字段：

- `execution_result`
- `gate_reason`
- `reconciliation_status`
- `reconciliation_discrepancy`
- `latest_transition_reason`

### `ui.paper_run_journal_summary`

用途：

- 从 `runtime.journal_events` 聚合 paper run journal

重点字段：

- `event_count`
- `order_event_count`
- `fill_event_count`
- `mismatch_event_count`
- `latest_event_at`

### `ui.daily_ops_summary`

用途：

- 给 daily ops 提供 run 级汇总

重点字段：

- `go_decision`
- `filled_count`
- `rejected_count`
- `reconciliation_mismatch_count`
- `attention_required_count`

### `ui.daily_review_input`

用途：

- 作为 future `Daily Review Agent` 的唯一输入面

说明：

- 该表是 read-model，不是 agent output
- agent 不得反向改写 execution canonical tables

---

## 4. 常见排查路径

### 4.1 Gate Reject

先查：

- `ui.execution_ticket_summary.gate_allowed`
- `ui.execution_ticket_summary.gate_reason`
- `runtime.gate_decisions`

重点看：

- watch-only/degrade gate
- inventory gate
- selector / capability / request 输入是否正确

### 4.2 Order 已创建但未成交

先查：

- `ui.execution_ticket_summary.execution_result`
- `ui.execution_ticket_summary.order_status`
- `trading.order_state_transitions`

当前 `P3` baseline：

- `POST_ONLY_GTC` 默认可能保持 `posted_resting`
- `FAK/FOK` 按 deterministic quote-based rule 处理

### 4.3 Reconciliation Mismatch

先查：

- `ui.execution_exception_summary`
- `trading.reconciliation_results`
- `trading.inventory_positions`
- `trading.exposure_snapshots`

当前 readiness 规则：

- 存在 `reconciliation mismatch` 时，`P3 readiness` 必须为 `NO-GO`

### 4.4 Daily Review Input 检查

先查：

- `ui.daily_review_input`
- `ui.daily_ops_summary`

说明：

- 若 `ui.daily_review_input` 缺失，说明 `P3-10` 读面未正确构建
- 这会进一步影响 `daily_ops_surface` readiness gate

---

## 5. Readiness 解释

当前 `P3` readiness gates 固定为：

- `cold_path_determinism`
- `paper_execution_chain`
- `portfolio_reconciliation`
- `agent_review_surface`
- `operator_surface`
- `daily_ops_surface`

解释规则：

- `GO` = `ready for P4 planning only`
- `NO-GO` = `P3 not ready to close`

明确禁止误读为：

- 可 live
- 可用真实 signer
- 不表示可广播真实链上交易

---

## 6. Canonical 验证命令

当前 `P3` canonical regression 命令：

```bash
.venv/bin/python -m unittest tests.test_execution_foundation tests.test_cold_path_orchestration tests.test_p2_closeout -v
```

说明：

- `.venv` 是 canonical 环境
- 结果应与 closeout checklist 保持一致

---

## 7. Human-In-The-Loop 边界

本阶段仍需人工介入：

- readiness 最终放行
- reconciliation mismatch 处置
- daily review 结论采纳
- 任何 signer / wallet / key material 决策

---

## 8. 非目标

本 runbook 不覆盖：

- live submitter
- signer RPC
- KMS / Vault / HSM
- real wallet side effects
- chain broadcast
