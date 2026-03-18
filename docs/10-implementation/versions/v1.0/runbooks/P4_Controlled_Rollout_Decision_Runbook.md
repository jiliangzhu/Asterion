# P4 Controlled Rollout Decision Runbook

**状态**: archived accepted historical runbook
**阶段**: `P4-12 Readiness / Closeout / Controlled Rollout Decision`
**边界**: 这是 rollout decision runbook，不是操作执行 runbook，不替代 `P4_Controlled_Live_Smoke_Runbook.md`。

---

> Historical accepted record only.
> 该 runbook 保留为 `P4` rollout decision 的历史 operator 记录，不再作为当前 active runbook 入口。

## 1. 目的

本 runbook 用于判断系统是否已经达到：

- `ready for controlled live rollout decision`

它不表示：

- `ready for unattended live`
- 可以绕过 `weather_controlled_live_smoke`
- 可以默认启用更大范围真实 side effect

---

## 2. Canonical 输入

做出 rollout decision 前，固定核对以下输入：

- 最新 `P4` readiness report
- `ui.phase_readiness_summary`
- `ui.live_prereq_wallet_summary`
- `ui.live_prereq_execution_summary`
- `ui.execution_exception_summary`
- latest `runtime.chain_tx_attempts` for controlled live
- latest `meta.signature_audit_logs`

---

## 3. Decision Matrix

### GO

仅当以下条件全部满足时，才允许给出 `GO`：

- readiness = `GO`
- wallet summary 无 blocker
- live-prereq execution 无 `sign_rejected` / `submit_rejected` / `external_unverified` / `external_mismatch`
- 至少 1 次 `approve_usdc` controlled live `broadcasted`

### NO-GO

出现以下任一情况即为 `NO-GO`：

- 任一 readiness gate fail
- 任一 wallet 不是 `ready`
- 任一 execution status 落入：
  - `sign_rejected`
  - `submit_rejected`
  - `external_unverified`
  - `external_mismatch`
- controlled live smoke 缺失，或最近一次为 `blocked` / `rejected`

---

## 4. Operator Review 步骤

固定顺序：

1. 先看 `ui.phase_readiness_summary`
2. 再看 `ui.live_prereq_wallet_summary`
3. 再看 `ui.live_prereq_execution_summary`
4. 再看 `ui.execution_exception_summary`
5. 最后核对 controlled live smoke 的 `runtime.chain_tx_attempts` / `meta.signature_audit_logs`

---

## 5. Decision 结果边界

- `GO` 只表示 `ready for controlled live rollout decision`
- 不等于 production live
- 不允许绕过 `weather_controlled_live_smoke` 进入更大范围真实 side effect

---

## 6. 故障排查入口

优先按以下路径排查：

- signer reject
- submit reject
- wallet readiness blockers
- external mismatch / unverified
- controlled live smoke blocked / rejected
