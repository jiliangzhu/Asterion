# Controlled Live Boundary Design

**版本**: v1.1
**更新日期**: 2026-03-17
**状态**: historical accepted supporting design (`Post-P4 Phase 10`)
**对应阶段**: `Post-P4 Phase 10: Boundary Hardening v2`

---

## 1. 背景与问题

`Phase 5` 到 `Phase 9` 已经把 controlled live boundary 收口到：

- shell-level `ARMED=true` parity
- submitter boundary attestation v1
- `runtime.live_boundary_attestations` 审计落库
- UI / web 最小只读环境

但当前深度审计仍指出 3 条边界薄弱点：

1. attestation 仍然是 caller-trusted artifact，缺少 expiry、single-use 和 tamper-resistant proof
2. signer 仍然信任 payload 里的 `private_key_env_var`
3. UI secret minimization 已经 default-safe，但还没有 banned-env detection 和 public bind opt-in

`Post-P4 Phase 10` 的目标不是扩大真钱边界，而是把现有 constrained real submit 再加固一层，避免 direct call、payload tampering、误暴露配置和错误部署形态把 current boundary 弄松。

---

## 2. 当前代码事实

当前代码中与本设计直接相关的落点：

- `asterion_core/contracts/live_boundary.py`
  - `SubmitterBoundaryInputs`
  - `SubmitterBoundaryAttestation`
  - `evaluate_submitter_boundary(...)`
- `asterion_core/execution/live_submitter_v1.py`
  - live submit shell 负责生成并写入 attestation
  - `RealClobSubmitterBackend` 校验 approved attestation
- `asterion_core/signer/signer_service_v1.py`
  - controlled-live signer 仍从 request payload 读取 `private_key_env_var`
- `ui/runtime_env.py`
  - UI 当前只 allowlist 最小只读 env
  - 但还没有 banned-env detector 和 public bind policy

当前事实判断：

- live submit 已经不是 handler-only gate，shell 和 backend 都在参与边界校验
- 但 attestation 还没有 nonce / expiry / consume-once
- signer secret name 仍可被调用方影响
- UI 不再自动读取 full `.env`，但还缺“部署时自检 + 明确拒绝暴露敏感项”的第二层保护

---

## 3. 锁定决策

### 3.1 Submitter Boundary Attestation v2

固定引入 `SubmitterBoundaryAttestationV2` 语义，在现有 attestation v1 基础上补齐：

- `issuer`
- `issued_at`
- `expires_at`
- `nonce`
- `decision_fingerprint`
- `attestation_mac`
- `use_status`

固定语义：

- attestation 只能由 `SubmitterServiceShell` 发行
- attestation 必须有显式过期时间
- attestation 必须一次性消费
- attestation 必须绑定：
  - `request_id`
  - `wallet_id`
  - `submit_mode`
  - `target_backend_kind`
  - `submitter_endpoint_fingerprint`
  - `manifest_hash`
  - `readiness_hash`

### 3.2 Decision Fingerprint and MAC

固定引入：

- `decision_fingerprint`
  - 对 boundary inputs 的稳定 canonical payload 做 hash
- `attestation_mac`
  - 对关键字段和 `decision_fingerprint` 做 HMAC

固定 secret：

- `ASTERION_CONTROLLED_LIVE_SECRET_ATTESTATION_MAC_KEY`

固定规则：

- shell 在 mint attestation 时生成 MAC
- backend 在消费 attestation 时重新计算并校验 MAC
- `ASTERION_CONTROLLED_LIVE_SECRET_ATTESTATION_MAC_KEY` 缺失时，`live_submit` 一律 blocked

### 3.3 Consume-Once

固定新增 `runtime.live_boundary_attestation_uses`，每个 attestation 只能被成功消费一次。

固定规则：

- shell mint success 不代表可无限复用
- backend 在 provider call 前必须 claim attestation use
- 重放同一 attestation 时返回 deterministic rejection

### 3.4 Signer v2 Secret Resolution

固定取消 caller 提供 `private_key_env_var` 的能力。

新增固定 helper：

- `controlled_live_wallet_secret_env_var(wallet_id: str) -> str`

固定映射规则：

- `wallet_id` 正规化为 upper snake
- 最终 env key 形如：
  - `ASTERION_CONTROLLED_LIVE_SECRET_PK_<WALLET_ID_UPPER_SNAKE>`

固定边界：

- signer request 仍可带 `wallet_id`
- signer request 不再允许指定 secret env 名称
- 如果 `wallet_id` 无法映射到 env key，则直接 reject

### 3.5 UI Hardening

固定补两层保护：

1. banned-env detection
2. public bind opt-in

固定 banned key family：

- `ASTERION_CONTROLLED_LIVE_SECRET_*`
- provider API keys
- wallet / signer / submitter raw secrets

固定 public bind policy：

- 默认只允许 loopback bind
- 非 loopback bind 必须显式开启：
  - `ASTERION_UI_ALLOW_PUBLIC_BIND=true`

### 3.6 Agents Surface Boundary

`Agents` 页固定不再显示任何 key-presence、secret-adjacent runtime indicators。

允许保留：

- model/provider name
- queue depth
- invocation health
- exception counts

不允许保留：

- secret 是否存在
- controlled-live secret env 名称
- signer env presence

---

## 4. 接口 / Contract / Schema

### 4.1 SubmitterBoundaryAttestationV2

建议在 `asterion_core/contracts/live_boundary.py` 中扩展：

```python
@dataclass(frozen=True)
class SubmitterBoundaryAttestationV2:
    attestation_id: str
    request_id: str
    wallet_id: str
    submit_mode: str
    target_backend_kind: str
    submitter_endpoint_fingerprint: str | None
    manifest_hash: str | None
    readiness_hash: str | None
    attestation_status: str
    reason_codes: list[str]
    issuer: str
    issued_at: datetime
    expires_at: datetime
    nonce: str
    decision_fingerprint: str
    attestation_mac: str
    attestation_payload_json: dict[str, Any]
```

### 4.2 Schema Changes

固定允许 `Post-P4 Phase 10` 做 runtime-only migration：

1. 扩展 `runtime.live_boundary_attestations`
   - `issuer TEXT`
   - `issued_at TIMESTAMP`
   - `expires_at TIMESTAMP`
   - `nonce TEXT`
   - `decision_fingerprint TEXT`
   - `attestation_mac TEXT`

2. 新增 `runtime.live_boundary_attestation_uses`
   - `use_id TEXT PRIMARY KEY`
   - `attestation_id TEXT NOT NULL`
   - `request_id TEXT NOT NULL`
   - `wallet_id TEXT NOT NULL`
   - `target_backend_kind TEXT NOT NULL`
   - `submitter_endpoint_fingerprint TEXT`
   - `used_at TIMESTAMP NOT NULL`
   - `use_status TEXT NOT NULL`
   - `error TEXT`

固定不改：

- `trading.*`
- `runtime.submit_attempts`
- capability manifest file schema

### 4.3 Signer Contract Changes

固定调整 `SignerRequest.payload` 对 controlled-live transaction signing 的语义：

- 删除：
  - `private_key_env_var`
- 保留：
  - wallet-level identity
  - signing payload
  - signing purpose

新增 helper contract：

```python
def controlled_live_wallet_secret_env_var(wallet_id: str) -> str:
    ...
```

### 4.4 UI Runtime Env Hardening

建议在 `ui/runtime_env.py` 新增：

```python
def is_banned_ui_env_key(key: str) -> bool: ...
def detect_banned_ui_env_keys(env: Mapping[str, str]) -> list[str]: ...
def ui_bind_is_allowed(host: str, *, allow_public_bind: bool) -> bool: ...
```

---

## 5. 数据流

```text
handler / job
-> SubmitterBoundaryInputs
-> SubmitterServiceShell
-> evaluate boundary
-> mint attestation v2
-> write runtime.live_boundary_attestations
-> RealClobSubmitterBackend
-> verify MAC + expiry + fingerprint + single-use
-> write runtime.live_boundary_attestation_uses
-> provider call
```

Signer path:

```text
wallet_id
-> controlled_live_wallet_secret_env_var(wallet_id)
-> resolve env secret
-> sign payload
```

UI path:

```text
root env + .env
-> resolve_ui_runtime_env()
-> detect banned env keys
-> enforce bind policy
-> start streamlit only if policy passes
```

---

## 6. 失败模式与边界

固定失败模式：

- attestation expired
- attestation MAC mismatch
- attestation nonce replay
- decision fingerprint mismatch
- endpoint fingerprint mismatch
- wallet_id to secret-env mapping miss
- UI detects banned env keys
- UI tries public bind without explicit opt-in

固定处理：

- 全部返回 deterministic rejection
- provider call 前必须失败，不允许“先调用再落审计”
- 所有 blocked / rejected 必须进入 `runtime.*` 审计层

固定非目标：

- 不做 unattended live
- 不做 unrestricted live
- 不做 KMS / HSM
- 不做多签资金管理

---

## 7. 测试策略

`Post-P4 Phase 10` 至少补：

- `tests.test_submitter_boundary_attestation_v2`
  - expiry
  - MAC mismatch
  - nonce replay
  - fingerprint mismatch
- `tests.test_live_submitter_backend`
  - consume-once
  - missing attestation key
- `tests.test_signer_service_v2`
  - wallet_id -> env mapping
  - caller cannot inject secret env name
- `tests.test_ui_runtime_env`
  - banned env detection
  - public bind opt-in
- `tests.test_controlled_live_smoke`
  - current allow-path / deny-path not regressed

最小 acceptance：

- `.venv/bin/python3 -m unittest tests.test_submitter_boundary_attestation_v2 -v`
- `.venv/bin/python3 -m unittest tests.test_live_submitter_backend -v`
- `.venv/bin/python3 -m unittest tests.test_signer_service_v2 -v`
- `.venv/bin/python3 -m unittest tests.test_ui_runtime_env -v`

---

## 8. 文档同步要求

实现本设计时必须同步：

- `Post_P4_Remediation_Implementation_Plan.md`
- `README.md`
- `Implementation_Index.md`
- `Documentation_Index.md`
- 如 bind policy 变化影响 operator startup，再更新对应 runbook

---

## 9. Deferred / Non-Goals

本设计明确留到后续阶段：

- KMS / HSM
- multi-operator approval workflow
- chain-tx intrinsic attestation
- secret rotation automation
- production-grade public deployment hardening beyond current operator console scope
