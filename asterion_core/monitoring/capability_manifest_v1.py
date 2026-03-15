from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from asterion_core.blockchain import controlled_live_wallet_secret_env_var, load_controlled_live_smoke_policy


DEFAULT_CONTROLLED_LIVE_CAPABILITY_MANIFEST_PATH = "data/meta/controlled_live_capability_manifest.json"
_CONTROLLED_LIVE_ARMED_ENV = "ASTERION_CONTROLLED_LIVE_SECRET_ARMED"
_CONTROLLED_LIVE_APPROVAL_TOKEN_ENV = "ASTERION_CONTROLLED_LIVE_SECRET_APPROVAL_TOKEN"


def build_controlled_live_capability_manifest(
    *,
    policy_path: str | Path,
    signer_backend_kind: str,
    chain_tx_backend_kind: str,
    submitter_backend_kind: str,
) -> dict[str, Any]:
    blockers: list[str] = []
    manifest_status = "valid"
    allowed_wallet_ids: list[str] = []
    allowed_tx_kinds: list[str] = []
    allowed_spenders_by_wallet: dict[str, list[str]] = {}
    max_approve_amount_by_wallet: dict[str, str] = {}
    required_env_vars = [_CONTROLLED_LIVE_ARMED_ENV, _CONTROLLED_LIVE_APPROVAL_TOKEN_ENV]

    try:
        policy = load_controlled_live_smoke_policy(policy_path)
    except Exception as exc:  # noqa: BLE001
        blockers.append(f"policy_invalid:{exc}")
        manifest_status = "invalid"
        policy = None

    if policy is not None:
        allowed_wallet_ids = [item.wallet_id for item in policy.wallets]
        allowed_tx_kinds = sorted({kind for item in policy.wallets for kind in item.allowed_tx_kinds})
        allowed_spenders_by_wallet = {item.wallet_id: list(item.allowed_spenders) for item in policy.wallets}
        max_approve_amount_by_wallet = {
            item.wallet_id: format(item.max_approve_amount, "f") for item in policy.wallets
        }
        required_env_vars.extend(
            controlled_live_wallet_secret_env_var(item.wallet_id) for item in policy.wallets
        )

    if signer_backend_kind != "env_private_key_tx":
        blockers.append(f"signer_backend_kind_mismatch:{signer_backend_kind}")
        manifest_status = "invalid"
    if chain_tx_backend_kind != "real_broadcast":
        blockers.append(f"chain_tx_backend_kind_mismatch:{chain_tx_backend_kind}")
        manifest_status = "invalid"
    if submitter_backend_kind not in {"disabled", "shadow_stub", "real_clob_submit"}:
        blockers.append(f"submitter_backend_kind_mismatch:{submitter_backend_kind}")
        manifest_status = "invalid"
    submitter_capability = {
        "disabled": "disabled",
        "shadow_stub": "shadow_only",
        "real_clob_submit": "constrained_real_submit",
    }.get(submitter_backend_kind, "unknown")

    if manifest_status == "valid":
        missing_env = [name for name in required_env_vars if not str(os.getenv(name) or "").strip()]
        if missing_env:
            blockers.extend(f"missing_secret_env:{name}" for name in missing_env)
            manifest_status = "blocked"

    return {
        "schema_version": "controlled_live_capability_manifest.v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "manifest_status": manifest_status,
        "controlled_live_mode": "manual_only",
        "default_off": True,
        "submitter_backend_kind": submitter_backend_kind,
        "submitter_capability": submitter_capability,
        "signer_backend_kind": signer_backend_kind,
        "chain_tx_backend_kind": chain_tx_backend_kind,
        "allowed_wallet_ids": allowed_wallet_ids,
        "allowed_tx_kinds": allowed_tx_kinds,
        "allowed_spenders_by_wallet": allowed_spenders_by_wallet,
        "max_approve_amount_by_wallet": max_approve_amount_by_wallet,
        "secret_source": "controlled_live_env_prefix",
        "required_env_vars": required_env_vars,
        "blockers": blockers,
    }


def write_controlled_live_capability_manifest(
    manifest: dict[str, Any],
    *,
    path: str | Path,
) -> str:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(manifest, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return str(output_path)


def load_controlled_live_capability_manifest(path: str | Path) -> dict[str, Any] | None:
    manifest_path = Path(path)
    if not manifest_path.exists():
        return None
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("controlled live capability manifest payload must be an object")
    return payload


def build_capability_boundary_summary(manifest: dict[str, Any] | None) -> dict[str, Any]:
    payload = manifest or {}
    return {
        "manual_only": payload.get("controlled_live_mode") == "manual_only",
        "default_off": bool(payload.get("default_off")),
        "approve_usdc_only": payload.get("allowed_tx_kinds") == ["approve_usdc"],
        "shadow_submitter_only": payload.get("submitter_capability") == "shadow_only",
        "constrained_real_submit_enabled": payload.get("submitter_capability") == "constrained_real_submit",
        "manifest_status": payload.get("manifest_status") or "missing",
    }
