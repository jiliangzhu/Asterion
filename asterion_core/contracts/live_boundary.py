from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import hmac
import json
import secrets
from datetime import timedelta
from typing import Any

from .ids import stable_object_id


SUBMITTER_BOUNDARY_ATTESTATION_KIND_V1 = "submitter_live_boundary_v1"
SUBMITTER_BOUNDARY_ATTESTATION_KIND_V2 = "submitter_live_boundary_v2"
SUBMITTER_BOUNDARY_ATTESTATION_V2_ISSUER = "submitter_service_shell_v2"
SUBMITTER_BOUNDARY_ATTESTATION_V2_TTL_SECONDS = 300


def _normalize_timestamp(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _stable_payload_hash(payload: dict[str, Any] | None) -> str | None:
    if payload is None:
        return None
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


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
    attestation_kind: str
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
    issuer: str | None = None
    issued_at: datetime | None = None
    expires_at: datetime | None = None
    nonce: str | None = None
    decision_fingerprint: str | None = None
    attestation_mac: str | None = None

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
    attestation_kind: str = SUBMITTER_BOUNDARY_ATTESTATION_KIND_V1,
    issuer: str | None = None,
    issued_at: datetime | None = None,
    expires_at: datetime | None = None,
    nonce: str | None = None,
    decision_fingerprint: str | None = None,
    attestation_mac: str | None = None,
    attestation_id: str | None = None,
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
        attestation_id=attestation_id
        or stable_object_id(
            "sbatt",
            {
                "request_id": request_id,
                "wallet_id": wallet_id,
                "submit_mode": submit_mode,
                "target_backend_kind": target_backend_kind,
                "reason_codes": list(reason_codes),
                "attestation_kind": attestation_kind,
            },
        ),
        attestation_kind=attestation_kind,
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
        issuer=issuer,
        issued_at=_normalize_timestamp(issued_at) if issued_at is not None else None,
        expires_at=_normalize_timestamp(expires_at) if expires_at is not None else None,
        nonce=nonce,
        decision_fingerprint=decision_fingerprint,
        attestation_mac=attestation_mac,
    )


def compute_boundary_decision_fingerprint(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def compute_boundary_attestation_mac(
    *,
    secret: str,
    issuer: str,
    attestation_id: str,
    nonce: str,
    issued_at: datetime,
    expires_at: datetime,
    decision_fingerprint: str,
) -> str:
    message = "|".join(
        [
            issuer,
            attestation_id,
            nonce,
            _normalize_timestamp(issued_at).isoformat(timespec="seconds"),
            _normalize_timestamp(expires_at).isoformat(timespec="seconds"),
            decision_fingerprint,
        ]
    )
    return hmac.new(secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()


def mint_submitter_boundary_attestation_v2(
    attestation: SubmitterBoundaryAttestation,
    *,
    attestation_secret: str | None,
    issued_at: datetime | None = None,
    ttl_seconds: int = SUBMITTER_BOUNDARY_ATTESTATION_V2_TTL_SECONDS,
    issuer: str = SUBMITTER_BOUNDARY_ATTESTATION_V2_ISSUER,
) -> SubmitterBoundaryAttestation:
    normalized_issued_at = _normalize_timestamp(issued_at or attestation.created_at)
    expires_at = normalized_issued_at + timedelta(seconds=ttl_seconds)
    nonce = secrets.token_hex(16)
    decision_fingerprint = compute_boundary_decision_fingerprint(attestation.attestation_payload_json)
    reason_codes = list(attestation.reason_codes)
    attestation_status = attestation.attestation_status
    if not str(attestation_secret or "").strip():
        if "attestation_secret_missing" not in reason_codes:
            reason_codes.append("attestation_secret_missing")
        attestation_status = "blocked"
    attestation_id = stable_object_id(
        "sbatt",
        {
            "request_id": attestation.request_id,
            "wallet_id": attestation.wallet_id,
            "submit_mode": attestation.submit_mode,
            "target_backend_kind": attestation.target_backend_kind,
            "attestation_kind": SUBMITTER_BOUNDARY_ATTESTATION_KIND_V2,
            "nonce": nonce,
            "issued_at": normalized_issued_at.isoformat(timespec="seconds"),
        },
    )
    attestation_mac = None
    if attestation_status == "approved":
        attestation_mac = compute_boundary_attestation_mac(
            secret=str(attestation_secret),
            issuer=issuer,
            attestation_id=attestation_id,
            nonce=nonce,
            issued_at=normalized_issued_at,
            expires_at=expires_at,
            decision_fingerprint=decision_fingerprint,
        )
    return SubmitterBoundaryAttestation(
        attestation_id=attestation_id,
        attestation_kind=SUBMITTER_BOUNDARY_ATTESTATION_KIND_V2,
        request_id=attestation.request_id,
        wallet_id=attestation.wallet_id,
        submit_mode=attestation.submit_mode,
        target_backend_kind=attestation.target_backend_kind,
        manifest_hash=attestation.manifest_hash,
        readiness_hash=attestation.readiness_hash,
        submitter_endpoint_fingerprint=attestation.submitter_endpoint_fingerprint,
        attestation_status=attestation_status,
        reason_codes=reason_codes,
        attestation_payload_json=dict(attestation.attestation_payload_json),
        created_at=attestation.created_at,
        issuer=issuer,
        issued_at=normalized_issued_at,
        expires_at=expires_at,
        nonce=nonce,
        decision_fingerprint=decision_fingerprint,
        attestation_mac=attestation_mac,
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
    "SUBMITTER_BOUNDARY_ATTESTATION_KIND_V1",
    "SUBMITTER_BOUNDARY_ATTESTATION_KIND_V2",
    "SUBMITTER_BOUNDARY_ATTESTATION_V2_ISSUER",
    "SUBMITTER_BOUNDARY_ATTESTATION_V2_TTL_SECONDS",
    "SubmitterBoundaryAttestation",
    "SubmitterBoundaryInputs",
    "build_submitter_boundary_attestation",
    "compute_boundary_attestation_mac",
    "compute_boundary_decision_fingerprint",
    "evaluate_submitter_boundary",
    "mint_submitter_boundary_attestation_v2",
]
