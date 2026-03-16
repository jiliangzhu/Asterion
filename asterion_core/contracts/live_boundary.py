from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
from typing import Any

from .ids import stable_object_id


def _normalize_timestamp(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _stable_payload_hash(payload: dict[str, Any] | None) -> str | None:
    if payload is None:
        return None
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class SubmitterBoundaryInputs:
    request_id: str
    wallet_id: str
    source_attempt_id: str | None
    ticket_id: str | None
    execution_context_id: str | None
    submit_mode: str
    submitter_backend_kind: str
    signer_backend_kind: str
    chain_tx_backend_kind: str
    submitter_endpoint_fingerprint: str | None
    manifest_payload: dict[str, Any] | None
    manifest_path: str | None
    readiness_report_payload: dict[str, Any] | None
    wallet_readiness_status: str | None
    approval_token_matches: bool
    armed: bool
    evaluated_at: datetime


@dataclass(frozen=True)
class SubmitterBoundaryAttestation:
    attestation_id: str
    request_id: str
    wallet_id: str
    submit_mode: str
    target_backend_kind: str
    manifest_hash: str | None
    readiness_hash: str | None
    submitter_endpoint_fingerprint: str | None
    attestation_status: str
    reason_codes: list[str]
    attestation_payload_json: dict[str, Any]
    created_at: datetime

    def __post_init__(self) -> None:
        if self.attestation_status not in {"approved", "blocked"}:
            raise ValueError("attestation_status must be approved or blocked")


def build_submitter_boundary_attestation(
    *,
    request_id: str,
    wallet_id: str,
    submit_mode: str,
    target_backend_kind: str,
    submitter_endpoint_fingerprint: str | None,
    manifest_payload: dict[str, Any] | None,
    readiness_report_payload: dict[str, Any] | None,
    reason_codes: list[str],
    created_at: datetime,
    extra_payload: dict[str, Any] | None = None,
) -> SubmitterBoundaryAttestation:
    normalized_created_at = _normalize_timestamp(created_at)
    status = "approved" if not reason_codes else "blocked"
    payload = {
        "request_id": request_id,
        "wallet_id": wallet_id,
        "submit_mode": submit_mode,
        "target_backend_kind": target_backend_kind,
        "submitter_endpoint_fingerprint": submitter_endpoint_fingerprint,
        "manifest_payload": manifest_payload,
        "readiness_report_payload": readiness_report_payload,
        "reason_codes": list(reason_codes),
    }
    if extra_payload:
        payload.update(extra_payload)
    return SubmitterBoundaryAttestation(
        attestation_id=stable_object_id(
            "sbatt",
            {
                "request_id": request_id,
                "wallet_id": wallet_id,
                "submit_mode": submit_mode,
                "target_backend_kind": target_backend_kind,
                "reason_codes": list(reason_codes),
            },
        ),
        request_id=request_id,
        wallet_id=wallet_id,
        submit_mode=submit_mode,
        target_backend_kind=target_backend_kind,
        manifest_hash=_stable_payload_hash(manifest_payload),
        readiness_hash=_stable_payload_hash(readiness_report_payload),
        submitter_endpoint_fingerprint=submitter_endpoint_fingerprint,
        attestation_status=status,
        reason_codes=list(reason_codes),
        attestation_payload_json=payload,
        created_at=normalized_created_at,
    )


def evaluate_submitter_boundary(inputs: SubmitterBoundaryInputs) -> SubmitterBoundaryAttestation:
    manifest_payload = dict(inputs.manifest_payload) if isinstance(inputs.manifest_payload, dict) else None
    readiness_payload = (
        dict(inputs.readiness_report_payload) if isinstance(inputs.readiness_report_payload, dict) else None
    )
    reason_codes: list[str] = []

    if str(inputs.submit_mode).strip() != "live_submit":
        reason_codes.append("boundary_inputs_missing")
    if manifest_payload is None:
        reason_codes.append("manifest_missing")
    else:
        if str(manifest_payload.get("manifest_status") or "").strip() != "valid":
            reason_codes.append("manifest_invalid")
        if str(manifest_payload.get("controlled_live_mode") or "").strip() != "manual_only":
            reason_codes.append("controlled_live_mode_not_manual_only")
        allowed_wallet_ids = {str(item).strip() for item in list(manifest_payload.get("allowed_wallet_ids") or [])}
        if str(inputs.wallet_id).strip() not in allowed_wallet_ids:
            reason_codes.append("wallet_not_allowlisted")

    if str(inputs.submitter_backend_kind).strip() != "real_clob_submit":
        reason_codes.append("submitter_backend_not_real_clob_submit")
    if str(inputs.signer_backend_kind).strip() != "env_private_key_tx":
        reason_codes.append("signer_backend_not_env_private_key_tx")
    if str(inputs.chain_tx_backend_kind).strip() != "real_broadcast":
        reason_codes.append("chain_tx_backend_not_real_broadcast")
    if not bool(inputs.armed):
        reason_codes.append("live_submit_not_armed")
    if not bool(inputs.approval_token_matches):
        reason_codes.append("approval_token_mismatch")
    if str(inputs.wallet_readiness_status or "").strip() != "ready":
        reason_codes.append("wallet_not_ready")
    if not str(inputs.submitter_endpoint_fingerprint or "").strip():
        reason_codes.append("submitter_endpoint_fingerprint_mismatch")
    if str((readiness_payload or {}).get("go_decision") or "").strip() != "GO":
        reason_codes.append("p4_live_prereq_not_go")

    deduped_reasons: list[str] = []
    for item in reason_codes:
        if item not in deduped_reasons:
            deduped_reasons.append(item)

    return build_submitter_boundary_attestation(
        request_id=inputs.request_id,
        wallet_id=inputs.wallet_id,
        submit_mode=inputs.submit_mode,
        target_backend_kind=inputs.submitter_backend_kind,
        submitter_endpoint_fingerprint=inputs.submitter_endpoint_fingerprint,
        manifest_payload=manifest_payload,
        readiness_report_payload=readiness_payload,
        reason_codes=deduped_reasons,
        created_at=inputs.evaluated_at,
        extra_payload={
            "source_attempt_id": inputs.source_attempt_id,
            "ticket_id": inputs.ticket_id,
            "execution_context_id": inputs.execution_context_id,
            "manifest_path": inputs.manifest_path,
            "signer_backend_kind": inputs.signer_backend_kind,
            "chain_tx_backend_kind": inputs.chain_tx_backend_kind,
            "wallet_readiness_status": inputs.wallet_readiness_status,
            "approval_token_matches": inputs.approval_token_matches,
            "armed": inputs.armed,
        },
    )


__all__ = [
    "SubmitterBoundaryAttestation",
    "SubmitterBoundaryInputs",
    "build_submitter_boundary_attestation",
    "evaluate_submitter_boundary",
]
