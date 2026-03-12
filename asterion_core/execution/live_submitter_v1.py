from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from asterion_core.contracts import stable_object_id
from asterion_core.journal import build_journal_event, enqueue_journal_event_upserts
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


class SubmitMode(str, Enum):
    DRY_RUN = "dry_run"
    SHADOW_SUBMIT = "shadow_submit"


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
class _SubmitterInvocationResult:
    response: SubmitOrderResult
    payload_hash: str
    task_ids: list[str]


class SubmitterBackend:
    def submit(self, request: SubmitOrderRequest) -> SubmitOrderResult:
        raise NotImplementedError


class DisabledSubmitterBackend(SubmitterBackend):
    def submit(self, request: SubmitOrderRequest) -> SubmitOrderResult:
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
    def submit(self, request: SubmitOrderRequest) -> SubmitOrderResult:
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


class SubmitterServiceShell:
    def __init__(self, backend: SubmitterBackend | None = None) -> None:
        self._backend = backend or DisabledSubmitterBackend()

    def submit_order(
        self,
        request: SubmitOrderRequest,
        *,
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


def build_submit_order_request_from_sign_attempt(
    sign_attempt: SubmitAttemptRecord,
    *,
    requester: str,
    request_id: str,
    timestamp: datetime,
    submit_mode: SubmitMode,
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


def _build_submit_payload(
    request: SubmitOrderRequest,
    *,
    backend_kind: str,
    status: str,
    external_order_id: str | None,
    error: str | None,
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
        "status": status,
        "external_order_id": external_order_id,
        "error": error,
        "signed_payload": request.signed_payload_json,
    }


def _extract_external_order_id(payload: dict[str, Any]) -> str | None:
    value = payload.get("external_order_id")
    return str(value) if value is not None else None


def _normalize_timestamp(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _sql_timestamp(value: datetime) -> str:
    return _normalize_timestamp(value).isoformat(sep=" ", timespec="seconds")


def _append_task_id(task_id: str | None) -> list[str]:
    return [task_id] if task_id else []
