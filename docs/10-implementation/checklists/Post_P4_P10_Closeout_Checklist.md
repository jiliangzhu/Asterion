# Post-P4 Phase 10 Closeout Checklist

**阶段**: `Post-P4 Phase 10: Boundary Hardening v2`  
**状态**: accepted (`2026-03-17`)  
**用途**: 作为 `Post-P4 Phase 10` 完成后的唯一 closeout checklist

---

## 1. Phase Objective

把 controlled live boundary 从当前 `attestation v1 + shell/backend gating + minimal UI env isolation`，收口成：

- attestation v2
- signer secret resolution v2
- UI banned-env detection + public bind opt-in

---

## 2. Delivery Lock

必须完成：

- submitter attestation v2
- `runtime.live_boundary_attestation_uses`
- signer `wallet_id -> env key` 固定映射
- UI banned-env detection
- UI public bind opt-in

不得顺带做：

- unattended live
- unrestricted live
- KMS / HSM
- chain-tx attestation v2

---

## 3. Must-Run Tests

- `.venv/bin/python3 -m unittest tests.test_submitter_boundary_attestation_v2 -v`
- `.venv/bin/python3 -m unittest tests.test_live_submitter_backend -v`
- `.venv/bin/python3 -m unittest tests.test_signer_service_v2 -v`
- `.venv/bin/python3 -m unittest tests.test_ui_runtime_env -v`
- `.venv/bin/python3 -m unittest tests.test_controlled_live_smoke -v`

---

## 4. Required Docs Sync

- `Post_P4_Remediation_Implementation_Plan.md`
- `README.md`
- `Implementation_Index.md`
- `Documentation_Index.md`
- `Controlled_Live_Boundary_Design.md`
- `Operator_Console_Truth_Source_Design.md`

---

## 5. Required Migration Review

必须审查：

- `runtime.live_boundary_attestations` 的扩列是否 backward-compatible
- `runtime.live_boundary_attestation_uses` 是否没有重复表达 `runtime.submit_attempts`
- writerd allowlist / migration ordering / rollback path

---

## 6. Explicit Non-Goals Not Violated

- 没有扩大真钱边界
- 没有让 caller 再次注入 secret env var 名称
- 没有让 UI 接触 controlled-live secrets
- 没有新增平行 execution ledger

---

## 7. Acceptance Evidence To Record

记录以下证据：

- migration file path
- targeted test output
- denied replay / reuse attestation output
- approved one-time-use attestation output
- UI banned-env detection denial evidence

---

## 8. Ready To Mark Accepted

仅当以下全部满足，才可把 `Post-P4 Phase 10` 标记 accepted：

- attestation v2 已落 canonical runtime audit path
- signer 不再接受 caller 指定 secret env 名称
- UI public bind 默认仍为 off
- 所有必跑测试通过
