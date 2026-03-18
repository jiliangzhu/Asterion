from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, ROUND_DOWN
from enum import Enum
import hashlib
import importlib.util
import json
import os
from typing import Any

from asterion_core.contracts import (
    SUBMITTER_BOUNDARY_ATTESTATION_KIND_V2,
    SUBMITTER_BOUNDARY_ATTESTATION_V2_ISSUER,
    ExternalFillObservation,
    ExternalFillObservationKind,
    SubmitterBoundaryAttestation,
    SubmitterBoundaryInputs,
    build_submitter_boundary_attestation,
    compute_boundary_attestation_mac,
    compute_boundary_decision_fingerprint,
    evaluate_submitter_boundary,
    mint_submitter_boundary_attestation_v2,
    stable_object_id,
)
from asterion_core.journal import build_journal_event, enqueue_journal_event_upserts
from asterion_core.live_side_effect_guard_v1 import LiveSideEffectGuard, validate_live_side_effect_guard
from asterion_core.signer import SubmitAttemptRecord, hash_signer_payload
from asterion_core.storage.os_queue import enqueue_upsert_rows_v1
from asterion_core.storage.utils import safe_json_dumps
from asterion_core.storage.write_queue import WriteQueueConfig


RUNTIME_EXTERNAL_ORDER_OBSERVATION_COLUMNS = [
    "observation_id",
    "attempt_id",
    "request_id",
    "ticket_id",
    "order_id",
    "wallet_id",
    "execution_context_id",
    "exchange",
    "observation_kind",
    "submit_mode",
    "canonical_order_hash",
    "external_order_id",
    "external_status",
    "observed_at",
    "error",
    "raw_observation_json",
]

RUNTIME_EXTERNAL_FILL_OBSERVATION_COLUMNS = [
    "observation_id",
    "attempt_id",
    "request_id",
    "ticket_id",
    "order_id",
    "wallet_id",
    "execution_context_id",
    "exchange",
    "observation_kind",
    "external_order_id",
    "external_trade_id",
    "market_id",
    "token_id",
    "outcome",
    "side",
    "price",
    "size",
    "fee",
    "fee_rate_bps",
    "external_status",
    "observed_at",
    "error",
    "raw_observation_json",
]

RUNTIME_LIVE_BOUNDARY_ATTESTATION_COLUMNS = [
    "attestation_id",
    "request_id",
    "run_id",
    "wallet_id",
    "source_attempt_id",
    "ticket_id",
    "execution_context_id",
    "attestation_kind",
    "submit_mode",
    "target_backend_kind",
    "attestation_status",
    "reason_codes_json",
    "attestation_payload_json",
    "created_at",
    "issuer",
    "issued_at",
    "expires_at",
    "nonce",
    "decision_fingerprint",
    "attestation_mac",
]

RUNTIME_LIVE_BOUNDARY_ATTESTATION_USE_COLUMNS = [
    "use_id",
    "attestation_id",
    "request_id",
    "wallet_id",
    "target_backend_kind",
    "submitter_endpoint_fingerprint",
    "use_status",
    "provider_status",
    "error",
    "created_at",
    "completed_at",
]


class SubmitMode(str, Enum):
    DRY_RUN = "dry_run"
    SHADOW_SUBMIT = "shadow_submit"
    LIVE_SUBMIT = "live_submit"


class ShadowFillMode(str, Enum):
    NONE = "none"
    PARTIAL = "partial"
    FULL = "full"


@dataclass(frozen=True)
class SubmitOrderRequest:
    request_id: str
    requester: str
    timestamp: datetime
    submit_mode: SubmitMode
    source_attempt_id: str
    ticket_id: str
    order_id: str | None
    wallet_id: str
    execution_context_id: str
    exchange: str
    canonical_order_hash: str
    signed_payload_json: dict[str, Any]
    shadow_fill_mode: ShadowFillMode = ShadowFillMode.NONE

    def __post_init__(self) -> None:
        if not self.request_id or not self.requester:
            raise ValueError("request_id and requester are required")
        if not self.source_attempt_id or not self.ticket_id or not self.wallet_id:
            raise ValueError("source_attempt_id, ticket_id, and wallet_id are required")
        if self.exchange != "polymarket_clob":
            raise ValueError("submitter only supports exchange=polymarket_clob")
        if not self.execution_context_id or not self.canonical_order_hash:
            raise ValueError("execution_context_id and canonical_order_hash are required")
        if not isinstance(self.signed_payload_json, dict) or not self.signed_payload_json:
            raise ValueError("signed_payload_json must be a non-empty object")
        if self.submit_mode is SubmitMode.DRY_RUN and self.shadow_fill_mode is not ShadowFillMode.NONE:
            raise ValueError("shadow_fill_mode requires submit_mode=shadow_submit")
        if self.submit_mode is SubmitMode.LIVE_SUBMIT and self.shadow_fill_mode is not ShadowFillMode.NONE:
            raise ValueError("shadow_fill_mode is not supported for submit_mode=live_submit")


@dataclass(frozen=True)
class SubmitOrderResult:
    request_id: str
    status: str
    payload_hash: str
    submit_payload_json: dict[str, Any]
    external_order_id: str | None
    error: str | None
    completed_at: datetime

    def __post_init__(self) -> None:
        if self.status not in {"previewed", "accepted", "rejected"}:
            raise ValueError("submitter status must be previewed, accepted, or rejected")
        if not self.payload_hash:
            raise ValueError("payload_hash is required")
        if not isinstance(self.submit_payload_json, dict) or not self.submit_payload_json:
            raise ValueError("submit_payload_json must be a non-empty object")


@dataclass(frozen=True)
class ExternalOrderObservationRecord:
    observation_id: str
    attempt_id: str
    request_id: str
    ticket_id: str
    order_id: str | None
    wallet_id: str
    execution_context_id: str
    exchange: str
    observation_kind: str
    submit_mode: str
    canonical_order_hash: str
    external_order_id: str | None
    external_status: str
    observed_at: datetime
    error: str | None
    raw_observation_json: dict[str, Any]


@dataclass(frozen=True)
class _SubmitShadowFillContext:
    external_order_id: str
    shadow_fill_mode: ShadowFillMode


@dataclass(frozen=True)
class _SubmitterInvocationResult:
    response: SubmitOrderResult
    payload_hash: str
    task_ids: list[str]


@dataclass(frozen=True)
class LiveBoundaryAttestationUseRecord:
    use_id: str
    attestation_id: str
    request_id: str
    wallet_id: str
    target_backend_kind: str
    submitter_endpoint_fingerprint: str
    use_status: str
    provider_status: str | None
    error: str | None
    created_at: datetime
    completed_at: datetime | None


class SubmitterBackend:
    def backend_kind(self) -> str:
        raise NotImplementedError

    def endpoint_fingerprint(self) -> str | None:
        return None

    def submit(
        self,
        request: SubmitOrderRequest,
        *,
        boundary_attestation: SubmitterBoundaryAttestation | None = None,
    ) -> SubmitOrderResult:
        raise NotImplementedError


class DisabledSubmitterBackend(SubmitterBackend):
    def backend_kind(self) -> str:
        return "disabled"

    def submit(
        self,
        request: SubmitOrderRequest,
        *,
        boundary_attestation: SubmitterBoundaryAttestation | None = None,
    ) -> SubmitOrderResult:
        envelope = _build_submit_payload(
            request,
            backend_kind="disabled",
            status="rejected",
            external_order_id=None,
            error="submitter_backend_disabled",
        )
        return SubmitOrderResult(
            request_id=request.request_id,
            status="rejected",
            payload_hash=hash_signer_payload(envelope),
            submit_payload_json=envelope,
            external_order_id=None,
            error="submitter_backend_disabled",
            completed_at=_normalize_timestamp(request.timestamp),
        )


class ShadowSubmitterBackend(SubmitterBackend):
    def backend_kind(self) -> str:
        return "shadow_stub"

    def submit(
        self,
        request: SubmitOrderRequest,
        *,
        boundary_attestation: SubmitterBoundaryAttestation | None = None,
    ) -> SubmitOrderResult:
        should_reject = bool(request.signed_payload_json.get("shadow_reject"))
        status = "rejected" if should_reject else "accepted"
        external_order_id = None
        if status == "accepted":
            external_order_id = stable_object_id(
                "extord",
                {"request_id": request.request_id, "canonical_order_hash": request.canonical_order_hash},
            )
        envelope = _build_submit_payload(
            request,
            backend_kind="shadow_stub",
            status=status,
            external_order_id=external_order_id,
            error="shadow_submit_rejected" if should_reject else None,
        )
        return SubmitOrderResult(
            request_id=request.request_id,
            status=status,
            payload_hash=hash_signer_payload(envelope),
            submit_payload_json=envelope,
            external_order_id=external_order_id,
            error="shadow_submit_rejected" if should_reject else None,
            completed_at=_normalize_timestamp(request.timestamp),
        )


class _SubmitterHttpClient:
    def __init__(self, *, timeout_seconds: float = 10.0) -> None:
        if importlib.util.find_spec("httpx") is None:
            raise RuntimeError("real_clob_submit requires installed dependency httpx")
        import httpx

        self._client = httpx.Client(timeout=timeout_seconds)

    def post_json(self, url: str, *, payload: dict[str, Any]) -> dict[str, Any]:
        response = self._client.post(
            url,
            json=payload,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "asterion-submitter/0.1",
            },
        )
        response.raise_for_status()
        body = response.json()
        if not isinstance(body, dict):
            raise ValueError("real submitter response must be a JSON object")
        return body


class RealClobSubmitterBackend(SubmitterBackend):
    def __init__(self, *, api_base_url: str, client: Any | None = None, db_path: str | None = None) -> None:
        normalized = str(api_base_url or "").strip()
        if not normalized:
            raise ValueError("real_clob_submit requires submitter_api_base_url")
        self._api_base_url = normalized
        self._client = client or _SubmitterHttpClient()
        self._db_path = str(db_path or os.getenv("ASTERION_DB_PATH") or "").strip() or None

    def backend_kind(self) -> str:
        return "real_clob_submit"

    def endpoint_fingerprint(self) -> str | None:
        return hashlib.sha256(self._api_base_url.encode("utf-8")).hexdigest()

    def submit(
        self,
        request: SubmitOrderRequest,
        *,
        boundary_attestation: SubmitterBoundaryAttestation | None = None,
    ) -> SubmitOrderResult:
        if request.submit_mode is not SubmitMode.LIVE_SUBMIT:
            envelope = _build_submit_payload(
                request,
                backend_kind="real_clob_submit",
                status="rejected",
                external_order_id=None,
                error="real_submitter_requires_submit_mode_live_submit",
            )
            return SubmitOrderResult(
                request_id=request.request_id,
                status="rejected",
                payload_hash=hash_signer_payload(envelope),
                submit_payload_json=envelope,
                external_order_id=None,
                error="real_submitter_requires_submit_mode_live_submit",
                completed_at=_normalize_timestamp(request.timestamp),
            )
        if boundary_attestation is None:
            return _build_rejected_result(
                request,
                backend_kind="real_clob_submit",
                error="submitter_boundary_attestation_missing",
            )
        if boundary_attestation.attestation_status != "approved":
            return _build_rejected_result(
                request,
                backend_kind="real_clob_submit",
                error="submitter_boundary_attestation_not_approved",
            )
        if boundary_attestation.request_id != request.request_id:
            return _build_rejected_result(
                request,
                backend_kind="real_clob_submit",
                error="submitter_boundary_attestation_request_mismatch",
            )
        if boundary_attestation.wallet_id != request.wallet_id:
            return _build_rejected_result(
                request,
                backend_kind="real_clob_submit",
                error="submitter_boundary_attestation_wallet_mismatch",
            )
        if str(boundary_attestation.submit_mode).strip() != request.submit_mode.value:
            return _build_rejected_result(
                request,
                backend_kind="real_clob_submit",
                error="submitter_boundary_attestation_submit_mode_mismatch",
            )
        if str(boundary_attestation.target_backend_kind).strip() != self.backend_kind():
            return _build_rejected_result(
                request,
                backend_kind="real_clob_submit",
                error="submitter_boundary_attestation_backend_mismatch",
            )
        if str(boundary_attestation.submitter_endpoint_fingerprint or "").strip() != str(
            self.endpoint_fingerprint() or ""
        ).strip():
            return _build_rejected_result(
                request,
                backend_kind="real_clob_submit",
                error="submitter_endpoint_fingerprint_mismatch",
            )
        if str(boundary_attestation.attestation_kind).strip() != SUBMITTER_BOUNDARY_ATTESTATION_KIND_V2:
            return _build_rejected_result(
                request,
                backend_kind="real_clob_submit",
                error="submitter_boundary_attestation_kind_mismatch",
            )
        if str(boundary_attestation.issuer or "").strip() != SUBMITTER_BOUNDARY_ATTESTATION_V2_ISSUER:
            return _build_rejected_result(
                request,
                backend_kind="real_clob_submit",
                error="attestation_issuer_mismatch",
            )
        if boundary_attestation.issued_at is None or boundary_attestation.expires_at is None or not boundary_attestation.nonce:
            return _build_rejected_result(
                request,
                backend_kind="real_clob_submit",
                error="submitter_boundary_attestation_incomplete",
            )
        if _normalize_timestamp(request.timestamp) > _normalize_timestamp(boundary_attestation.expires_at):
            return _build_rejected_result(
                request,
                backend_kind="real_clob_submit",
                error="attestation_expired",
            )
        expected_decision_fingerprint = compute_boundary_decision_fingerprint(boundary_attestation.attestation_payload_json)
        if str(boundary_attestation.decision_fingerprint or "").strip() != expected_decision_fingerprint:
            return _build_rejected_result(
                request,
                backend_kind="real_clob_submit",
                error="attestation_decision_fingerprint_mismatch",
            )
        attestation_secret = str(os.getenv("ASTERION_CONTROLLED_LIVE_SECRET_ATTESTATION_MAC_KEY") or "").strip()
        if not attestation_secret:
            return _build_rejected_result(
                request,
                backend_kind="real_clob_submit",
                error="attestation_secret_missing",
            )
        expected_mac = compute_boundary_attestation_mac(
            secret=attestation_secret,
            issuer=SUBMITTER_BOUNDARY_ATTESTATION_V2_ISSUER,
            attestation_id=boundary_attestation.attestation_id,
            nonce=boundary_attestation.nonce,
            issued_at=boundary_attestation.issued_at,
            expires_at=boundary_attestation.expires_at,
            decision_fingerprint=expected_decision_fingerprint,
        )
        if str(boundary_attestation.attestation_mac or "").strip() != expected_mac:
            return _build_rejected_result(
                request,
                backend_kind="real_clob_submit",
                error="attestation_mac_invalid",
            )
        persisted_attestation = _load_persisted_live_boundary_attestation(
            db_path=self._db_path,
            attestation_id=boundary_attestation.attestation_id,
        )
        if persisted_attestation is None:
            return _build_rejected_result(
                request,
                backend_kind="real_clob_submit",
                error="attestation_not_persisted",
            )
        if not _persisted_attestation_matches(
            persisted_attestation=persisted_attestation,
            attestation=boundary_attestation,
        ):
            return _build_rejected_result(
                request,
                backend_kind="real_clob_submit",
                error="attestation_decision_fingerprint_mismatch",
            )
        claimed_use = _claim_live_boundary_attestation_use(
            db_path=self._db_path,
            attestation=boundary_attestation,
            request=request,
            endpoint_fingerprint=str(self.endpoint_fingerprint() or ""),
        )
        if claimed_use is None:
            return _build_rejected_result(
                request,
                backend_kind="real_clob_submit",
                error="attestation_reused",
            )

        try:
            provider_payload = self._client.post_json(
                self._api_base_url,
                payload=_build_real_submit_request_payload(request),
            )
        except Exception as exc:  # noqa: BLE001
            error = f"submitter_provider_error:{exc}"
            envelope = _build_submit_payload(
                request,
                backend_kind="real_clob_submit",
                status="rejected",
                external_order_id=None,
                error=error,
                provider_response={"provider_error": str(exc)},
            )
            _finalize_live_boundary_attestation_use(
                db_path=self._db_path,
                use_id=claimed_use.use_id,
                use_status="provider_rejected",
                provider_status="rejected",
                error=error,
                completed_at=_normalize_timestamp(request.timestamp),
            )
            return SubmitOrderResult(
                request_id=request.request_id,
                status="rejected",
                payload_hash=hash_signer_payload(envelope),
                submit_payload_json=envelope,
                external_order_id=None,
                error=error,
                completed_at=_normalize_timestamp(request.timestamp),
            )

        status, external_order_id, error = _normalize_real_submit_provider_response(provider_payload)
        envelope = _build_submit_payload(
            request,
            backend_kind="real_clob_submit",
            status=status,
            external_order_id=external_order_id,
            error=error,
            provider_response=provider_payload,
        )
        _finalize_live_boundary_attestation_use(
            db_path=self._db_path,
            use_id=claimed_use.use_id,
            use_status="provider_completed",
            provider_status=status,
            error=error,
            completed_at=_normalize_timestamp(request.timestamp),
        )
        return SubmitOrderResult(
            request_id=request.request_id,
            status=status,
            payload_hash=hash_signer_payload(envelope),
            submit_payload_json=envelope,
            external_order_id=external_order_id,
            error=error,
            completed_at=_normalize_timestamp(request.timestamp),
        )


class SubmitterServiceShell:
    def __init__(self, backend: SubmitterBackend | None = None) -> None:
        self._backend = backend or DisabledSubmitterBackend()
        self._db_path = str(getattr(self._backend, "_db_path", None) or os.getenv("ASTERION_DB_PATH") or "").strip() or None

    def describe_live_boundary(self) -> dict[str, str | None]:
        return {
            "submitter_backend_kind": self._backend.backend_kind(),
            "submitter_endpoint_fingerprint": self._backend.endpoint_fingerprint(),
        }

    def submit_order(
        self,
        request: SubmitOrderRequest,
        *,
        live_guard: LiveSideEffectGuard | None = None,
        boundary_inputs: SubmitterBoundaryInputs | None = None,
        queue_cfg: WriteQueueConfig,
        run_id: str | None = None,
    ) -> _SubmitterInvocationResult:
        created_at = _normalize_timestamp(request.timestamp)
        task_ids: list[str] = []
        task_ids.extend(
            _append_task_id(
                enqueue_journal_event_upserts(
                    queue_cfg,
                    journal_events=[
                        build_journal_event(
                            event_type="submitter.requested",
                            entity_type="submit_request",
                            entity_id=request.request_id,
                            run_id=run_id or request.request_id,
                            payload_json={
                                "request_id": request.request_id,
                                "submit_mode": request.submit_mode.value,
                                "source_attempt_id": request.source_attempt_id,
                                "ticket_id": request.ticket_id,
                                "execution_context_id": request.execution_context_id,
                                "canonical_order_hash": request.canonical_order_hash,
                            },
                            created_at=created_at,
                        )
                    ],
                    run_id=run_id or request.request_id,
                )
            )
        )
        if request.submit_mode is SubmitMode.DRY_RUN:
            response = _build_dry_run_preview(request)
        elif request.submit_mode is SubmitMode.LIVE_SUBMIT:
            guard_error = validate_live_side_effect_guard(expected_mode="live_submit", guard=live_guard)
            effective_boundary_inputs = boundary_inputs
            if boundary_inputs is not None and guard_error:
                effective_boundary_inputs = SubmitterBoundaryInputs(
                    request_id=boundary_inputs.request_id,
                    wallet_id=boundary_inputs.wallet_id,
                    source_attempt_id=boundary_inputs.source_attempt_id,
                    ticket_id=boundary_inputs.ticket_id,
                    execution_context_id=boundary_inputs.execution_context_id,
                    submit_mode=boundary_inputs.submit_mode,
                    submitter_backend_kind=boundary_inputs.submitter_backend_kind,
                    signer_backend_kind=boundary_inputs.signer_backend_kind,
                    chain_tx_backend_kind=boundary_inputs.chain_tx_backend_kind,
                    submitter_endpoint_fingerprint=boundary_inputs.submitter_endpoint_fingerprint,
                    manifest_payload=boundary_inputs.manifest_payload,
                    manifest_path=boundary_inputs.manifest_path,
                    readiness_report_payload=boundary_inputs.readiness_report_payload,
                    wallet_readiness_status=boundary_inputs.wallet_readiness_status,
                    approval_token_matches=boundary_inputs.approval_token_matches,
                    armed=False,
                    evaluated_at=boundary_inputs.evaluated_at,
                )
            attestation = self._build_live_submit_attestation(
                request,
                boundary_inputs=effective_boundary_inputs,
            )
            task_ids.extend(
                _append_task_id(
                    enqueue_live_boundary_attestation_upserts(
                        queue_cfg,
                        attestations=[attestation],
                        run_id=run_id or request.request_id,
                    )
                )
            )
            _persist_live_boundary_attestation_direct(
                db_path=self._db_path,
                attestation=attestation,
                run_id=run_id or request.request_id,
            )
            if attestation.attestation_status != "approved":
                response = _build_rejected_result(
                    request,
                    backend_kind="submitter_shell",
                    error=attestation.reason_codes[0] if attestation.reason_codes else (guard_error or "submitter_boundary_blocked"),
                )
            else:
                response = self._backend.submit(request, boundary_attestation=attestation)
        else:
            response = self._backend.submit(request)
        final_event = {
            "previewed": "submitter.previewed",
            "accepted": "submitter.accepted",
            "rejected": "submitter.rejected",
        }[response.status]
        task_ids.extend(
            _append_task_id(
                enqueue_journal_event_upserts(
                    queue_cfg,
                    journal_events=[
                        build_journal_event(
                            event_type=final_event,
                            entity_type="submit_request",
                            entity_id=request.request_id,
                            run_id=run_id or request.request_id,
                            payload_json={
                                "request_id": request.request_id,
                                "submit_mode": request.submit_mode.value,
                                "source_attempt_id": request.source_attempt_id,
                                "ticket_id": request.ticket_id,
                                "execution_context_id": request.execution_context_id,
                                "canonical_order_hash": request.canonical_order_hash,
                                "payload_hash": response.payload_hash,
                                "status": response.status,
                                "external_order_id": response.external_order_id,
                                "error": response.error,
                            },
                            created_at=_normalize_timestamp(response.completed_at),
                        )
                    ],
                    run_id=run_id or request.request_id,
                )
            )
        )
        return _SubmitterInvocationResult(response=response, payload_hash=response.payload_hash, task_ids=task_ids)

    def _build_live_submit_attestation(
        self,
        request: SubmitOrderRequest,
        *,
        boundary_inputs: SubmitterBoundaryInputs | None,
    ) -> SubmitterBoundaryAttestation:
        base_attestation: SubmitterBoundaryAttestation
        if boundary_inputs is None:
            base_attestation = build_submitter_boundary_attestation(
                request_id=request.request_id,
                wallet_id=request.wallet_id,
                submit_mode=request.submit_mode.value,
                target_backend_kind=self._backend.backend_kind(),
                submitter_endpoint_fingerprint=self._backend.endpoint_fingerprint(),
                manifest_payload=None,
                readiness_report_payload=None,
                reason_codes=["boundary_inputs_missing"],
                created_at=request.timestamp,
                extra_payload={
                    "source_attempt_id": request.source_attempt_id,
                    "ticket_id": request.ticket_id,
                    "execution_context_id": request.execution_context_id,
                },
            )
        else:
            base_attestation = evaluate_submitter_boundary(boundary_inputs)
        return mint_submitter_boundary_attestation_v2(
            base_attestation,
            attestation_secret=str(os.getenv("ASTERION_CONTROLLED_LIVE_SECRET_ATTESTATION_MAC_KEY") or "").strip(),
            issued_at=request.timestamp,
        )


def build_submit_order_request_from_sign_attempt(
    sign_attempt: SubmitAttemptRecord,
    *,
    requester: str,
    request_id: str,
    timestamp: datetime,
    submit_mode: SubmitMode,
    shadow_fill_mode: ShadowFillMode = ShadowFillMode.NONE,
) -> SubmitOrderRequest:
    if sign_attempt.attempt_kind != "sign_order" or sign_attempt.attempt_mode != "sign_only":
        raise ValueError("submitter requires sign_order/sign_only source attempts")
    if sign_attempt.status != "signed":
        raise ValueError("submitter requires signed source attempts")
    return SubmitOrderRequest(
        request_id=request_id,
        requester=requester,
        timestamp=timestamp,
        submit_mode=submit_mode,
        source_attempt_id=sign_attempt.attempt_id,
        ticket_id=sign_attempt.ticket_id,
        order_id=sign_attempt.order_id,
        wallet_id=sign_attempt.wallet_id,
        execution_context_id=sign_attempt.execution_context_id,
        exchange=sign_attempt.exchange,
        canonical_order_hash=sign_attempt.canonical_order_hash,
        signed_payload_json=dict(sign_attempt.submit_payload_json),
        shadow_fill_mode=shadow_fill_mode,
    )


def build_submit_attempt_from_signed_payload(
    request: SubmitOrderRequest,
    result: SubmitOrderResult,
) -> SubmitAttemptRecord:
    return SubmitAttemptRecord(
        attempt_id=stable_object_id(
            "satt",
            {
                "request_id": request.request_id,
                "attempt_kind": "submit_order",
                "attempt_mode": request.submit_mode.value,
            },
        ),
        request_id=request.request_id,
        ticket_id=request.ticket_id,
        order_id=request.order_id,
        wallet_id=request.wallet_id,
        execution_context_id=request.execution_context_id,
        exchange=request.exchange,
        attempt_kind="submit_order",
        attempt_mode=request.submit_mode.value,
        canonical_order_hash=request.canonical_order_hash,
        payload_hash=result.payload_hash,
        submit_payload_json=result.submit_payload_json,
        signed_payload_ref=request.source_attempt_id,
        status=result.status,
        error=result.error,
        created_at=_normalize_timestamp(result.completed_at),
    )


def build_external_order_observation(
    attempt: SubmitAttemptRecord,
    *,
    observed_at: datetime,
) -> ExternalOrderObservationRecord:
    if attempt.attempt_kind != "submit_order":
        raise ValueError("external order observations require submit_order attempts")
    observation_kind = {
        "dry_run": "dry_run_preview",
        "shadow_submit": "shadow_submit_ack" if attempt.status == "accepted" else "shadow_submit_reject",
        "live_submit": "live_submit_ack" if attempt.status == "accepted" else "live_submit_reject",
    }.get(attempt.attempt_mode)
    if observation_kind is None:
        raise ValueError(f"unsupported submit attempt mode: {attempt.attempt_mode}")
    external_status = {
        "previewed": "preview",
        "accepted": "accepted",
        "rejected": "rejected",
    }.get(attempt.status)
    if external_status is None:
        raise ValueError(f"unsupported submit attempt status: {attempt.status}")
    normalized_observed_at = _normalize_timestamp(observed_at)
    raw_observation_json = {
        "exchange": attempt.exchange,
        "attempt_id": attempt.attempt_id,
        "request_id": attempt.request_id,
        "submit_mode": attempt.attempt_mode,
        "status": attempt.status,
        "payload_hash": attempt.payload_hash,
        "submit_payload": attempt.submit_payload_json,
        "signed_payload_ref": attempt.signed_payload_ref,
        "error": attempt.error,
    }
    return ExternalOrderObservationRecord(
        observation_id=stable_object_id(
            "eordobs",
            {
                "attempt_id": attempt.attempt_id,
                "observation_kind": observation_kind,
                "observed_at": normalized_observed_at.isoformat(timespec="seconds"),
            },
        ),
        attempt_id=attempt.attempt_id,
        request_id=attempt.request_id,
        ticket_id=attempt.ticket_id,
        order_id=attempt.order_id,
        wallet_id=attempt.wallet_id,
        execution_context_id=attempt.execution_context_id,
        exchange=attempt.exchange,
        observation_kind=observation_kind,
        submit_mode=attempt.attempt_mode,
        canonical_order_hash=attempt.canonical_order_hash,
        external_order_id=_extract_external_order_id(attempt.submit_payload_json),
        external_status=external_status,
        observed_at=normalized_observed_at,
        error=attempt.error,
        raw_observation_json=raw_observation_json,
    )


def build_external_fill_observations(
    attempt: SubmitAttemptRecord,
    *,
    observed_at: datetime,
) -> list[ExternalFillObservation]:
    if attempt.attempt_kind != "submit_order" or attempt.attempt_mode != SubmitMode.SHADOW_SUBMIT.value:
        return []
    if attempt.status != "accepted":
        return []
    fill_context = _extract_shadow_fill_context(attempt.submit_payload_json)
    if fill_context is None or fill_context.shadow_fill_mode is ShadowFillMode.NONE:
        return []
    order_payload = _extract_signed_order_payload(attempt.submit_payload_json)
    size = _quantize_decimal(Decimal(str(order_payload["size"])))
    price = _quantize_decimal(Decimal(str(order_payload["price"])))
    fee_rate_bps = int(order_payload["fee_rate_bps"])
    filled_size = size if fill_context.shadow_fill_mode is ShadowFillMode.FULL else _quantize_decimal(size / Decimal("2"))
    fee = _quantize_decimal((price * filled_size * Decimal(fee_rate_bps)) / Decimal("10000"))
    external_status = "filled" if fill_context.shadow_fill_mode is ShadowFillMode.FULL else "partial_filled"
    observation_kind = (
        ExternalFillObservationKind.SHADOW_FILL_FULL
        if fill_context.shadow_fill_mode is ShadowFillMode.FULL
        else ExternalFillObservationKind.SHADOW_FILL_PARTIAL
    )
    normalized_observed_at = _normalize_timestamp(observed_at)
    external_trade_id = stable_object_id(
        "extfill",
        {"attempt_id": attempt.attempt_id, "shadow_fill_mode": fill_context.shadow_fill_mode.value},
    )
    raw_observation_json = {
        "attempt_id": attempt.attempt_id,
        "request_id": attempt.request_id,
        "external_order_id": fill_context.external_order_id,
        "submit_payload": attempt.submit_payload_json,
        "shadow_fill_mode": fill_context.shadow_fill_mode.value,
        "external_status": external_status,
    }
    return [
        ExternalFillObservation(
            observation_id=stable_object_id(
                "efillobs",
                {
                    "attempt_id": attempt.attempt_id,
                    "external_trade_id": external_trade_id,
                    "observed_at": normalized_observed_at.isoformat(timespec="seconds"),
                },
            ),
            attempt_id=attempt.attempt_id,
            request_id=attempt.request_id,
            ticket_id=attempt.ticket_id,
            order_id=attempt.order_id,
            wallet_id=attempt.wallet_id,
            execution_context_id=attempt.execution_context_id,
            exchange=attempt.exchange,
            observation_kind=observation_kind,
            external_order_id=fill_context.external_order_id,
            external_trade_id=external_trade_id,
            market_id=str(order_payload["market_id"]),
            token_id=str(order_payload["token_id"]),
            outcome=str(order_payload["outcome"]),
            side=str(order_payload["side"]),
            price=price,
            size=filled_size,
            fee=fee,
            fee_rate_bps=fee_rate_bps,
            external_status=external_status,
            observed_at=normalized_observed_at,
            error=None,
            raw_observation_json=raw_observation_json,
        )
    ]


def enqueue_external_order_observation_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    observations: list[ExternalOrderObservationRecord],
    run_id: str | None = None,
) -> str | None:
    if not observations:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="runtime.external_order_observations",
        pk_cols=["observation_id"],
        columns=list(RUNTIME_EXTERNAL_ORDER_OBSERVATION_COLUMNS),
        rows=[external_order_observation_to_row(item) for item in observations],
        run_id=run_id,
    )


def enqueue_external_fill_observation_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    observations: list[ExternalFillObservation],
    run_id: str | None = None,
) -> str | None:
    if not observations:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="runtime.external_fill_observations",
        pk_cols=["observation_id"],
        columns=list(RUNTIME_EXTERNAL_FILL_OBSERVATION_COLUMNS),
        rows=[external_fill_observation_to_row(item) for item in observations],
        run_id=run_id,
    )


def enqueue_live_boundary_attestation_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    attestations: list[SubmitterBoundaryAttestation],
    run_id: str | None = None,
) -> str | None:
    if not attestations:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="runtime.live_boundary_attestations",
        pk_cols=["attestation_id"],
        columns=list(RUNTIME_LIVE_BOUNDARY_ATTESTATION_COLUMNS),
        rows=[submitter_boundary_attestation_to_row(item, run_id=run_id or item.request_id) for item in attestations],
        run_id=run_id,
    )


def external_order_observation_to_row(record: ExternalOrderObservationRecord) -> list[object]:
    return [
        record.observation_id,
        record.attempt_id,
        record.request_id,
        record.ticket_id,
        record.order_id,
        record.wallet_id,
        record.execution_context_id,
        record.exchange,
        record.observation_kind,
        record.submit_mode,
        record.canonical_order_hash,
        record.external_order_id,
        record.external_status,
        _sql_timestamp(record.observed_at),
        record.error,
        safe_json_dumps(record.raw_observation_json),
    ]


def external_fill_observation_to_row(record: ExternalFillObservation) -> list[object]:
    return [
        record.observation_id,
        record.attempt_id,
        record.request_id,
        record.ticket_id,
        record.order_id,
        record.wallet_id,
        record.execution_context_id,
        record.exchange,
        record.observation_kind.value,
        record.external_order_id,
        record.external_trade_id,
        record.market_id,
        record.token_id,
        record.outcome,
        record.side,
        _decimal_to_sql(record.price),
        _decimal_to_sql(record.size),
        _decimal_to_sql(record.fee),
        record.fee_rate_bps,
        record.external_status,
        _sql_timestamp(record.observed_at),
        record.error,
        safe_json_dumps(record.raw_observation_json),
    ]


def submitter_boundary_attestation_to_row(
    record: SubmitterBoundaryAttestation,
    *,
    run_id: str,
) -> list[object]:
    payload = dict(record.attestation_payload_json)
    payload.setdefault("manifest_hash", record.manifest_hash)
    payload.setdefault("readiness_hash", record.readiness_hash)
    return [
        record.attestation_id,
        record.request_id,
        run_id,
        record.wallet_id,
        payload.get("source_attempt_id"),
        payload.get("ticket_id"),
        payload.get("execution_context_id"),
        record.attestation_kind,
        record.submit_mode,
        record.target_backend_kind,
        record.attestation_status,
        safe_json_dumps(record.reason_codes),
        safe_json_dumps(payload),
        _sql_timestamp(record.created_at),
        record.issuer,
        _sql_timestamp(record.issued_at),
        _sql_timestamp(record.expires_at),
        record.nonce,
        record.decision_fingerprint,
        record.attestation_mac,
    ]


def live_boundary_attestation_use_to_row(record: LiveBoundaryAttestationUseRecord) -> list[object]:
    return [
        record.use_id,
        record.attestation_id,
        record.request_id,
        record.wallet_id,
        record.target_backend_kind,
        record.submitter_endpoint_fingerprint,
        record.use_status,
        record.provider_status,
        record.error,
        _sql_timestamp(record.created_at),
        _sql_timestamp(record.completed_at),
    ]


def _persist_live_boundary_attestation_direct(
    *,
    db_path: str | None,
    attestation: SubmitterBoundaryAttestation,
    run_id: str,
) -> None:
    if not str(db_path or "").strip():
        return
    try:
        import duckdb
    except ModuleNotFoundError:  # pragma: no cover
        return
    con = duckdb.connect(str(db_path), read_only=False)
    try:
        con.execute("DELETE FROM runtime.live_boundary_attestations WHERE attestation_id = ?", [attestation.attestation_id])
        con.execute(
            """
            INSERT INTO runtime.live_boundary_attestations
            (
                attestation_id,
                request_id,
                run_id,
                wallet_id,
                source_attempt_id,
                ticket_id,
                execution_context_id,
                attestation_kind,
                submit_mode,
                target_backend_kind,
                attestation_status,
                reason_codes_json,
                attestation_payload_json,
                created_at,
                issuer,
                issued_at,
                expires_at,
                nonce,
                decision_fingerprint,
                attestation_mac
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            submitter_boundary_attestation_to_row(attestation, run_id=run_id),
        )
    finally:
        con.close()


def _load_persisted_live_boundary_attestation(
    *,
    db_path: str | None,
    attestation_id: str,
) -> SubmitterBoundaryAttestation | None:
    if not str(db_path or "").strip():
        return None
    try:
        import duckdb
    except ModuleNotFoundError:  # pragma: no cover
        return None
    con = duckdb.connect(str(db_path), read_only=False)
    try:
        row = con.execute(
            """
            SELECT
                request_id,
                wallet_id,
                attestation_kind,
                submit_mode,
                target_backend_kind,
                attestation_status,
                reason_codes_json,
                attestation_payload_json,
                created_at,
                issuer,
                issued_at,
                expires_at,
                nonce,
                decision_fingerprint,
                attestation_mac
            FROM runtime.live_boundary_attestations
            WHERE attestation_id = ?
            """,
            [attestation_id],
        ).fetchone()
    except Exception:  # noqa: BLE001
        con.close()
        return None
    finally:
        con.close()
    if row is None:
        return None
    payload = _safe_json_dict(row[7])
    return SubmitterBoundaryAttestation(
        attestation_id=attestation_id,
        request_id=str(row[0]),
        wallet_id=str(row[1]),
        attestation_kind=str(row[2]),
        submit_mode=str(row[3]),
        target_backend_kind=str(row[4]),
        manifest_hash=payload.get("manifest_hash"),
        readiness_hash=payload.get("readiness_hash"),
        submitter_endpoint_fingerprint=str(payload.get("submitter_endpoint_fingerprint") or ""),
        attestation_status=str(row[5]),
        reason_codes=_safe_json_list(row[6]),
        attestation_payload_json=payload,
        created_at=_coerce_datetime(row[8]),
        issuer=str(row[9]) if row[9] is not None else None,
        issued_at=_coerce_datetime(row[10]),
        expires_at=_coerce_datetime(row[11]),
        nonce=str(row[12]) if row[12] is not None else None,
        decision_fingerprint=str(row[13]) if row[13] is not None else None,
        attestation_mac=str(row[14]) if row[14] is not None else None,
    )


def _persisted_attestation_matches(
    *,
    persisted_attestation: SubmitterBoundaryAttestation,
    attestation: SubmitterBoundaryAttestation,
) -> bool:
    persisted_payload = dict(persisted_attestation.attestation_payload_json)
    persisted_payload.setdefault("manifest_hash", persisted_attestation.manifest_hash)
    persisted_payload.setdefault("readiness_hash", persisted_attestation.readiness_hash)
    current_payload = dict(attestation.attestation_payload_json)
    current_payload.setdefault("manifest_hash", attestation.manifest_hash)
    current_payload.setdefault("readiness_hash", attestation.readiness_hash)
    return (
        persisted_attestation.request_id == attestation.request_id
        and persisted_attestation.wallet_id == attestation.wallet_id
        and persisted_attestation.attestation_kind == attestation.attestation_kind
        and persisted_attestation.submit_mode == attestation.submit_mode
        and persisted_attestation.target_backend_kind == attestation.target_backend_kind
        and persisted_attestation.attestation_status == attestation.attestation_status
        and persisted_attestation.submitter_endpoint_fingerprint == attestation.submitter_endpoint_fingerprint
        and persisted_attestation.issuer == attestation.issuer
        and persisted_attestation.nonce == attestation.nonce
        and persisted_attestation.decision_fingerprint == attestation.decision_fingerprint
        and persisted_attestation.attestation_mac == attestation.attestation_mac
        and compute_boundary_decision_fingerprint(persisted_payload) == compute_boundary_decision_fingerprint(current_payload)
    )


def _claim_live_boundary_attestation_use(
    *,
    db_path: str | None,
    attestation: SubmitterBoundaryAttestation,
    request: SubmitOrderRequest,
    endpoint_fingerprint: str,
) -> LiveBoundaryAttestationUseRecord | None:
    if not str(db_path or "").strip():
        return None
    try:
        import duckdb
    except ModuleNotFoundError:  # pragma: no cover
        return None
    created_at = _normalize_timestamp(request.timestamp)
    record = LiveBoundaryAttestationUseRecord(
        use_id=stable_object_id(
            "sbuse",
            {"attestation_id": attestation.attestation_id, "request_id": request.request_id, "wallet_id": request.wallet_id},
        ),
        attestation_id=attestation.attestation_id,
        request_id=request.request_id,
        wallet_id=request.wallet_id,
        target_backend_kind=attestation.target_backend_kind,
        submitter_endpoint_fingerprint=endpoint_fingerprint,
        use_status="claimed",
        provider_status=None,
        error=None,
        created_at=created_at,
        completed_at=None,
    )
    con = duckdb.connect(str(db_path), read_only=False)
    try:
        con.execute("BEGIN TRANSACTION")
        existing = con.execute(
            "SELECT attestation_id FROM runtime.live_boundary_attestation_uses WHERE attestation_id = ?",
            [attestation.attestation_id],
        ).fetchone()
        if existing is not None:
            con.execute("ROLLBACK")
            return None
        con.execute(
            """
            INSERT INTO runtime.live_boundary_attestation_uses
            (
                use_id,
                attestation_id,
                request_id,
                wallet_id,
                target_backend_kind,
                submitter_endpoint_fingerprint,
                use_status,
                provider_status,
                error,
                created_at,
                completed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            live_boundary_attestation_use_to_row(record),
        )
        con.execute("COMMIT")
        return record
    except Exception:  # noqa: BLE001
        try:
            con.execute("ROLLBACK")
        except Exception:  # noqa: BLE001
            pass
        return None
    finally:
        con.close()


def _finalize_live_boundary_attestation_use(
    *,
    db_path: str | None,
    use_id: str,
    use_status: str,
    provider_status: str | None,
    error: str | None,
    completed_at: datetime,
) -> None:
    if not str(db_path or "").strip():
        return
    try:
        import duckdb
    except ModuleNotFoundError:  # pragma: no cover
        return
    con = duckdb.connect(str(db_path), read_only=False)
    try:
        con.execute(
            """
            UPDATE runtime.live_boundary_attestation_uses
            SET use_status = ?, provider_status = ?, error = ?, completed_at = ?
            WHERE use_id = ?
            """,
            [use_status, provider_status, error, _sql_timestamp(completed_at), use_id],
        )
    finally:
        con.close()


def _build_dry_run_preview(request: SubmitOrderRequest) -> SubmitOrderResult:
    envelope = _build_submit_payload(
        request,
        backend_kind="dry_run",
        status="previewed",
        external_order_id=None,
        error=None,
    )
    return SubmitOrderResult(
        request_id=request.request_id,
        status="previewed",
        payload_hash=hash_signer_payload(envelope),
        submit_payload_json=envelope,
        external_order_id=None,
        error=None,
        completed_at=_normalize_timestamp(request.timestamp),
    )


def _build_rejected_result(
    request: SubmitOrderRequest,
    *,
    backend_kind: str,
    error: str,
) -> SubmitOrderResult:
    envelope = _build_submit_payload(
        request,
        backend_kind=backend_kind,
        status="rejected",
        external_order_id=None,
        error=error,
    )
    return SubmitOrderResult(
        request_id=request.request_id,
        status="rejected",
        payload_hash=hash_signer_payload(envelope),
        submit_payload_json=envelope,
        external_order_id=None,
        error=error,
        completed_at=_normalize_timestamp(request.timestamp),
    )


def _build_submit_payload(
    request: SubmitOrderRequest,
    *,
    backend_kind: str,
    status: str,
    external_order_id: str | None,
    error: str | None,
    provider_response: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "exchange": request.exchange,
        "attempt_kind": "submit_order",
        "attempt_mode": request.submit_mode.value,
        "backend_kind": backend_kind,
        "request_id": request.request_id,
        "source_attempt_id": request.source_attempt_id,
        "ticket_id": request.ticket_id,
        "execution_context_id": request.execution_context_id,
        "wallet_id": request.wallet_id,
        "canonical_order_hash": request.canonical_order_hash,
        "shadow_fill_mode": request.shadow_fill_mode.value,
        "status": status,
        "external_order_id": external_order_id,
        "error": error,
        "provider_response": provider_response or {},
        "signed_payload": request.signed_payload_json,
    }


def _build_real_submit_request_payload(request: SubmitOrderRequest) -> dict[str, Any]:
    return {
        "request_id": request.request_id,
        "source_attempt_id": request.source_attempt_id,
        "ticket_id": request.ticket_id,
        "order_id": request.order_id,
        "wallet_id": request.wallet_id,
        "execution_context_id": request.execution_context_id,
        "canonical_order_hash": request.canonical_order_hash,
        "exchange": request.exchange,
        "signed_payload": dict(request.signed_payload_json),
    }


def _normalize_real_submit_provider_response(payload: dict[str, Any]) -> tuple[str, str | None, str | None]:
    raw_status = str(payload.get("status") or payload.get("result") or "").strip().lower()
    error = str(payload.get("error") or payload.get("message") or "").strip() or None
    external_order_id = payload.get("external_order_id") or payload.get("order_id")
    normalized_external_order_id = str(external_order_id) if external_order_id is not None else None
    if raw_status in {"accepted", "ok", "success", "submitted"}:
        return "accepted", normalized_external_order_id, None
    if raw_status in {"previewed", "preview"}:
        return "previewed", normalized_external_order_id, None
    if raw_status in {"rejected", "error", "failed"}:
        return "rejected", normalized_external_order_id, error or "submitter_provider_rejected"
    if normalized_external_order_id and error is None:
        return "accepted", normalized_external_order_id, None
    return "rejected", normalized_external_order_id, error or "submitter_provider_invalid_response"


def _extract_external_order_id(payload: dict[str, Any]) -> str | None:
    value = payload.get("external_order_id")
    return str(value) if value is not None else None


def _extract_signed_order_payload(payload: dict[str, Any]) -> dict[str, Any]:
    value = payload.get("signed_payload")
    if not isinstance(value, dict):
        raise ValueError("submit payload is missing signed_payload")
    order = value.get("order")
    if not isinstance(order, dict):
        raise ValueError("submit payload is missing signed order payload")
    return dict(order)


def _extract_shadow_fill_context(payload: dict[str, Any]) -> _SubmitShadowFillContext | None:
    external_order_id = _extract_external_order_id(payload)
    if not external_order_id:
        return None
    raw_mode = str(payload.get("shadow_fill_mode") or ShadowFillMode.NONE.value)
    return _SubmitShadowFillContext(external_order_id=external_order_id, shadow_fill_mode=ShadowFillMode(raw_mode))


def _normalize_timestamp(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _normalize_timestamp(value)
    return _normalize_timestamp(datetime.fromisoformat(str(value).replace("Z", "+00:00")))


def _safe_json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if value in {None, ""}:
        return {}
    try:
        payload = json.loads(str(value))
    except Exception:  # noqa: BLE001
        return {}
    return payload if isinstance(payload, dict) else {}


def _safe_json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if value in {None, ""}:
        return []
    try:
        payload = json.loads(str(value))
    except Exception:  # noqa: BLE001
        return []
    if not isinstance(payload, list):
        return []
    return [str(item) for item in payload]


def _sql_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _normalize_timestamp(value).isoformat(sep=" ", timespec="seconds")


def _append_task_id(task_id: str | None) -> list[str]:
    return [task_id] if task_id else []


def _decimal_to_sql(value: Decimal) -> str:
    return format(_quantize_decimal(value), ".8f")


def _quantize_decimal(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
