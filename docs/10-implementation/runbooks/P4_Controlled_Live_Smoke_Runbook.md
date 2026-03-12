# P4 Controlled Live Smoke Runbook

**状态**: active  
**阶段**: `P4-11 Controlled Live Smoke Boundary`  
**边界**: 这是 `controlled live smoke` 入口，不是 unattended live。

---

## 1. 目标

本 runbook 只覆盖一条最小真实 side-effect 路径：

- `approve_usdc`

它的目的不是开始 live 交易，而是验证：

- operator approval boundary
- env arm boundary
- wallet allowlist / spender allowlist / amount cap
- env private key transaction signing
- real transaction broadcast seam
- 审计与 journal 闭环

---

## 2. Canonical 入口

唯一入口：

- `weather_controlled_live_smoke`

固定特征：

- `manual`
- `default-off`
- upstream: `weather_live_prereq_readiness`

本 job 只写：

- `runtime.chain_tx_attempts`
- `meta.signature_audit_logs`
- `runtime.journal_events`

不写：

- `trading.*`
- `runtime.submit_attempts`
- `runtime.external_order_observations`

---

## 3. 放行前提

必须同时满足：

1. 最新 `P4` readiness 报告是 `GO`
2. `ui.live_prereq_wallet_summary.wallet_readiness_status = 'ready'`
3. `ASTERION_CONTROLLED_LIVE_SMOKE_ARMED=true`
4. request 中的 `approval_token` 与 `ASTERION_CONTROLLED_LIVE_SMOKE_APPROVAL_TOKEN` 完全匹配
5. wallet 在 `config/controlled_live_smoke.json` allowlist 中
6. spender 在 wallet allowlist 中
7. `amount <= max_approve_amount`
8. wallet 对应私钥环境变量存在，且地址与 wallet `funder` 匹配

任一条件不满足：

- job 返回 `blocked`
- 不产生真实广播

---

## 4. 配置来源

repo 配置：

- `config/controlled_live_smoke.json`

环境变量：

- `ASTERION_CONTROLLED_LIVE_SMOKE_ARMED`
- `ASTERION_CONTROLLED_LIVE_SMOKE_APPROVAL_TOKEN`
- 每个 wallet 的 `private_key_env_var`

策略边界：

- allowlist / cap / wallet-secret-env mapping 以 repo JSON 为准
- env 只负责 arm、approval token 和 secret

---

## 5. 结果状态

`runtime.chain_tx_attempts.status`：

- `rejected`
- `broadcasted`

`runtime.journal_events`：

- `controlled_live_smoke.requested`
- `controlled_live_smoke.blocked`
- `controlled_live_smoke.broadcasted`
- 以及同一次调用里的 `signer.*` / `chain_tx.*`

---

## 6. 排查路径

### blocked

优先检查：

- readiness JSON 是否 `GO`
- `ui.live_prereq_wallet_summary` 的 `wallet_readiness_status`
- env arm/token 是否正确
- allowlist / spender / amount cap 是否命中

### rejected

优先检查：

- `meta.signature_audit_logs`
- `runtime.chain_tx_attempts.error`
- signer env private key 地址是否和 `funder` 一致

### broadcasted

检查：

- `runtime.chain_tx_attempts.tx_hash`
- `runtime.journal_events`

并确认：

- `tx_payload_json` 中不包含 raw private key
- `tx_payload_json` 中不包含 raw signed tx bytes

---

## 7. 明确非目标

本 runbook 不代表：

- unattended live
- real order submit
- split / merge / redeem live path
- live capital deployment

本阶段结论只能是：

- `ready for controlled live rollout decision`
