# P3 Closeout Checklist

**版本**: v1.0  
**更新日期**: 2026-03-11  
**阶段**: `P3`  
**状态**: closeout in progress  

---

## 1. 目标

本清单用于确认 `P3 paper execution` 是否已经具备独立 closeout 审查条件。

本清单的结论边界固定为：

- `GO` 仅表示 `ready for P4 planning only`
- 不表示可进入 live
- 不表示可以启用真实 signer / real wallet / real chain side effects

source of truth 顺序：

1. 当前仓库代码与 migrations
2. [P3_Implementation_Plan.md](../phase-plans/P3_Implementation_Plan.md)
3. [P3_Paper_Execution_Runbook.md](../runbooks/P3_Paper_Execution_Runbook.md)
4. 当前测试与 readiness 报告

---

## 2. 当前验证基线

截至 `2026-03-11`，当前 canonical 验证命令为：

```bash
.venv/bin/python -m unittest tests.test_execution_foundation tests.test_cold_path_orchestration tests.test_p2_closeout -v
```

预期结果：

- `46 tests OK`

说明：

- `.venv` 是 canonical 验证环境
- system Python 可能缺少 `duckdb`
- closeout 结论以仓库内 `.venv` 为准

---

## 3. 主链关闭条件

以下项目必须全部满足：

- [ ] `weather.weather_watch_only_snapshots -> runtime.strategy_runs -> runtime.trade_tickets -> capability.execution_contexts -> runtime.gate_decisions -> trading.orders -> trading.fills -> trading.reservations -> trading.inventory_positions -> trading.exposure_snapshots -> trading.reconciliation_results -> runtime.journal_events -> ui.*` 已贯通
- [ ] `capability.execution_contexts` 已作为运行中的一等 handoff table
- [ ] `trading.order_state_transitions` 已作为运行中的 canonical OMS transition ledger
- [ ] `trading.reconciliation_results` 已作为运行中的 reconciliation ledger，而非仅 migration 预留
- [ ] quote-based paper fill 输出 deterministic，rerun 后 row count / latest journal 稳定

---

## 4. Operator / Daily Ops 关闭条件

- [ ] `ui.execution_ticket_summary` 可直接展示 ticket 最新 gate/order/fill/reconciliation 状态
- [ ] `ui.execution_run_summary` 可展示 run 级聚合
- [ ] `ui.execution_exception_summary` 可展示 attention-required ticket
- [ ] `ui.paper_run_journal_summary` 已提供 paper run journal 聚合面
- [ ] `ui.daily_ops_summary` 已提供 daily ops 聚合面
- [ ] `ui.daily_review_input` 已作为 future `Daily Review Agent` 的唯一输入面
- [ ] `ui.phase_readiness_summary` 已能展示 `P3` readiness gates

---

## 5. Readiness / P4 Entry 条件

- [ ] readiness 报告输出 `GO / NO-GO`
- [ ] `GO` 的语义明确为 `ready for P4 planning only`
- [ ] readiness gates 至少包含：
  - `cold_path_determinism`
  - `paper_execution_chain`
  - `portfolio_reconciliation`
  - `agent_review_surface`
  - `operator_surface`
  - `daily_ops_surface`
- [ ] `reconciliation mismatch` 会使 readiness 进入 `NO-GO`
- [ ] 缺失 operator / daily ops 只读表会使 readiness 进入 `NO-GO`

---

## 6. Human-In-The-Loop 边界

以下能力在 `P3` closeout 后仍必须保持人工介入：

- [ ] readiness 最终放行
- [ ] reconciliation exception 处置
- [ ] daily review 结论采纳
- [ ] signer / wallet / key material 相关审批

---

## 7. 明确不进入 Live 的能力

以下能力在 `P3` closeout 时仍必须禁止：

- [ ] 真实 signer 调用
- [ ] 真实 wallet side effects
- [ ] 真实链上广播
- [ ] KMS / Vault / HSM 集成
- [ ] 真实资金 deployment

---

## 8. Closeout 交付物

`P3` closeout 至少需要以下交付物存在且可被审查：

- [ ] [P3_Implementation_Plan.md](../phase-plans/P3_Implementation_Plan.md)
- [ ] [P3_Closeout_Checklist.md](./P3_Closeout_Checklist.md)
- [ ] [P3_Paper_Execution_Runbook.md](../runbooks/P3_Paper_Execution_Runbook.md)
- [ ] readiness JSON / markdown 报告生成路径已稳定
- [ ] `README.md`、`Implementation_Index.md`、`Documentation_Index.md`、`DEVELOPMENT_ROADMAP.md` 已指向 closeout 文档入口

---

## 9. P4 开工前复核

仅当以下条件全部满足时，才允许进入 `P4 planning`：

- [ ] 本清单全部通过
- [ ] 当前 targeted regression 通过
- [ ] `P3` readiness 为 `GO`
- [ ] 团队明确接受 `P3` 仍然不是 live trading
- [ ] `P4` 不反向改写 `P3` canonical contracts / ledgers
