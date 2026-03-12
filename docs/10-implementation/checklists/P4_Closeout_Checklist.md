# P4 Closeout Checklist

**版本**: v1.0  
**更新日期**: 2026-03-12  
**阶段**: `P4`  
**状态**: closeout ready  

---

## 1. 目标与边界

本清单用于确认 `P4 live prerequisites` 是否已经具备独立 closeout 审查条件。

本清单的结论边界固定为：

- `GO` 仅表示 `ready for controlled live rollout decision`
- 不表示 `ready for unattended live`
- 不表示可默认启用真实 signer / submit / broadcast
- `P4` 当前允许的最小真实 side effect 仅限 `approve_usdc` controlled live smoke

source of truth 顺序：

1. 当前仓库代码与 migrations
2. [P4_Implementation_Plan.md](../phase-plans/P4_Implementation_Plan.md)
3. [P4_Controlled_Live_Smoke_Runbook.md](../runbooks/P4_Controlled_Live_Smoke_Runbook.md)
4. [P4_Controlled_Rollout_Decision_Runbook.md](../runbooks/P4_Controlled_Rollout_Decision_Runbook.md)
5. 当前测试与 `P4` readiness 报告

---

## 2. 当前验证基线

截至 `2026-03-12`，当前 canonical 验证命令为：

```bash
.venv/bin/python -m unittest tests.test_live_prereq_readiness tests.test_health_monitor tests.test_cold_path_orchestration tests.test_execution_foundation tests.test_p4_plan_docs -v
```

以及 `P4-11` 的 controlled live smoke 基线：

```bash
.venv/bin/python -m unittest tests.test_controlled_live_smoke tests.test_chain_tx_scaffold tests.test_signer_shell tests.test_cold_path_orchestration tests.test_p4_plan_docs -v
```

说明：

- `.venv` 是 canonical 验证环境
- system Python 可能缺少 `duckdb`
- closeout 结论以仓库内 `.venv` 为准

---

## 3. Live-Prereq 主链关闭条件

以下项目必须全部满足：

- [ ] `weather_market_discovery`
- [ ] `weather_capability_refresh`
- [ ] `weather_wallet_state_refresh`
- [ ] `weather_order_signing_smoke`
- [ ] `weather_submitter_smoke`
- [ ] `weather_chain_tx_smoke`
- [ ] `weather_external_execution_reconciliation`
- [ ] `weather_live_prereq_readiness`
- [ ] `weather_controlled_live_smoke`
- [ ] 对应 canonical ledgers 已进入运行主链：
  - `capability.market_capabilities`
  - `capability.account_trading_capabilities`
  - `runtime.external_balance_observations`
  - `meta.signature_audit_logs`
  - `runtime.submit_attempts`
  - `runtime.external_order_observations`
  - `runtime.external_fill_observations`
  - `runtime.chain_tx_attempts`
  - `trading.reconciliation_results`
  - `runtime.journal_events`

---

## 4. Readiness 关闭条件

- [ ] 最新 `P4` readiness report target 为 `p4_live_prerequisites`
- [ ] `go_decision = GO`
- [ ] 6 个 gate 全部通过：
  - `live_prereq_operator_surface`
  - `signer_path_health`
  - `submitter_shadow_path`
  - `wallet_state_and_allowance`
  - `external_execution_alignment`
  - `ops_queue_and_chain_tx`
- [ ] `decision_reason` 精确保持 `ready for controlled live rollout decision`

---

## 5. Wallet / Execution / Controlled Live 条件

- [ ] 所有 `can_trade=true` wallet 在 `ui.live_prereq_wallet_summary` 中为 `ready`
- [ ] `ui.live_prereq_execution_summary` 不存在以下状态：
  - `sign_rejected`
  - `submit_rejected`
  - `external_unverified`
  - `external_mismatch`
- [ ] 至少存在 1 条 `runtime.chain_tx_attempts` 记录满足：
  - `tx_kind = approve_usdc`
  - `tx_mode = controlled_live`
  - `status = broadcasted`

---

## 6. Human-In-The-Loop 边界

以下动作在 `P4` closeout 后仍必须保持人工介入：

- [ ] readiness 放行
- [ ] approval token 发放
- [ ] env arm
- [ ] controlled live smoke 执行
- [ ] rollout decision 采纳

---

## 7. 明确不进入 Unattended Live 的能力

以下能力在 `P4` closeout 时仍必须禁止：

- [ ] 不自动启用真实 signer / submit / broadcast
- [ ] 不开放真实 order submit
- [ ] 不开放无人值守 live rollout
- [ ] 不开放真实资金自动部署

---

## 8. Closeout 交付物

`P4` closeout 至少需要以下交付物存在且可被审查：

- [ ] [P4_Implementation_Plan.md](../phase-plans/P4_Implementation_Plan.md)
- [ ] [P4_Closeout_Checklist.md](./P4_Closeout_Checklist.md)
- [ ] [P4_Controlled_Live_Smoke_Runbook.md](../runbooks/P4_Controlled_Live_Smoke_Runbook.md)
- [ ] [P4_Controlled_Rollout_Decision_Runbook.md](../runbooks/P4_Controlled_Rollout_Decision_Runbook.md)
- [ ] 最新 `asterion_readiness_p4.json`
- [ ] 最新 `asterion_readiness_p4.md`
- [ ] `README.md`、`Implementation_Index.md`、`Documentation_Index.md`、`DEVELOPMENT_ROADMAP.md` 已指向 closeout 文档入口
