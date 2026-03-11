from __future__ import annotations

import hashlib
import importlib.util
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from asterion_core.contracts import AccountTradingCapability, CanonicalOrderContract, stable_object_id
from asterion_core.execution.order_router_v1 import RoutedCanonicalOrder
from asterion_core.journal import build_journal_event, enqueue_journal_event_upserts
from asterion_core.storage.os_queue import enqueue_upsert_rows_v1
from asterion_core.storage.utils import safe_json_dumps
from asterion_core.storage.write_queue import WriteQueueConfig


SIGNATURE_AUDIT_LOG_COLUMNS = [
    "log_id",
    "request_id",
    "signature_type",
    "payload_hash",
    "signature",
    "status",
    "requester",
    "timestamp",
    "error",
    "wallet_type",
    "signer_address",
    "funder",
    "api_key_ref",
    "chain_id",
    "token_id",
    "fee_rate_bps",
    "signing_purpose",
    "created_at",
]

RUNTIME_SUBMIT_ATTEMPT_COLUMNS = [
    "attempt_id",
    "request_id",
    "ticket_id",
    "order_id",
    "wallet_id",
    "execution_context_id",
    "exchange",
    "attempt_kind",
    "attempt_mode",
    "canonical_order_hash",
    "payload_hash",
    "submit_payload_json",
    "signed_payload_ref",
    "status",
    "error",
    "created_at",
]


class WalletType(str, Enum):
    EOA = "eoa"
    PROXY = "proxy"
    SAFE = "safe"


class SigningPurpose(str, Enum):
    L1_AUTH = "l1_auth"
    L2_AUTH = "l2_auth"
    ORDER = "order"
    TRANSACTION = "transaction"


class SignatureAuditStatus(str, Enum):
    REQUESTED = "requested"
    REJECTED = "rejected"
    SUCCEEDED = "succeeded"


@dataclass(frozen=True)
class SigningContext:
    wallet_type: WalletType
    signing_purpose: SigningPurpose
    signature_type: int
    funder: str
    signer_address: str
    api_key_ref: str | None
    chain_id: int
    token_id: str | None
    fee_rate_bps: int | None

    def __post_init__(self) -> None:
        if self.signature_type < 0:
            raise ValueError("signature_type must be non-negative")
        if not self.funder or not self.signer_address:
            raise ValueError("funder and signer_address are required")
        if self.chain_id <= 0:
            raise ValueError("chain_id must be positive")
        if self.signing_purpose is SigningPurpose.ORDER:
            if not self.token_id:
                raise ValueError("token_id is required for order signing")
            if self.fee_rate_bps is None or self.fee_rate_bps < 0:
                raise ValueError("fee_rate_bps is required for order signing")


@dataclass(frozen=True)
class SignerRequest:
    request_id: str
    requester: str
    timestamp: datetime
    context: SigningContext
    payload: dict[str, Any]

    def __post_init__(self) -> None:
        if not self.request_id or not self.requester:
            raise ValueError("request_id and requester are required")
        if not isinstance(self.payload, dict) or not self.payload:
            raise ValueError("payload must be a non-empty object")


@dataclass(frozen=True)
class SignOrderRequest:
    request_id: str
    requester: str
    timestamp: datetime
    context: SigningContext
    ticket_id: str
    execution_context_id: str
    canonical_order: CanonicalOrderContract
    canonical_order_hash: str

    def __post_init__(self) -> None:
        if not self.request_id or not self.requester:
            raise ValueError("request_id and requester are required")
        if not self.ticket_id or not self.execution_context_id:
            raise ValueError("ticket_id and execution_context_id are required")
        if self.context.signing_purpose is not SigningPurpose.ORDER:
            raise ValueError("SignOrderRequest requires signing_purpose=order")
        if not self.canonical_order_hash:
            raise ValueError("canonical_order_hash is required")


@dataclass(frozen=True)
class SignerResponse:
    request_id: str
    status: str
    signature: str | None
    signed_payload_ref: str | None
    error: str | None
    completed_at: datetime


@dataclass(frozen=True)
class OrderSigningResult:
    request_id: str
    status: str
    signature: str | None
    error: str | None
    payload_hash: str
    submit_payload_json: dict[str, Any]
    completed_at: datetime

    def __post_init__(self) -> None:
        if self.status not in {"signed", "rejected"}:
            raise ValueError("order signing status must be signed or rejected")
        if not self.payload_hash:
            raise ValueError("payload_hash is required")
        if not isinstance(self.submit_payload_json, dict) or not self.submit_payload_json:
            raise ValueError("submit_payload_json must be a non-empty object")


@dataclass(frozen=True)
class SubmitAttemptRecord:
    attempt_id: str
    request_id: str
    ticket_id: str
    order_id: str | None
    wallet_id: str
    execution_context_id: str
    exchange: str
    attempt_kind: str
    attempt_mode: str
    canonical_order_hash: str
    payload_hash: str
    submit_payload_json: dict[str, Any]
    signed_payload_ref: str
    status: str
    error: str | None
    created_at: datetime


@dataclass(frozen=True)
class _SignatureAuditLogRecord:
    log_id: str
    request_id: str
    signature_type: int
    payload_hash: str
    signature: str | None
    status: str
    requester: str
    timestamp: datetime
    error: str | None
    wallet_type: str
    signer_address: str
    funder: str
    api_key_ref: str | None
    chain_id: int
    token_id: str | None
    fee_rate_bps: int | None
    signing_purpose: str
    created_at: datetime


@dataclass(frozen=True)
class _SignerInvocationResult:
    response: SignerResponse
    payload_hash: str
    task_ids: list[str]


@dataclass(frozen=True)
class _OrderSigningInvocationResult:
    response: OrderSigningResult
    payload_hash: str
    task_ids: list[str]


class OrderSigningBackend:
    def sign_order(self, request: SignOrderRequest) -> OrderSigningResult:
        raise NotImplementedError


class DisabledSignerBackend(OrderSigningBackend):
    def sign_order(self, request: SignOrderRequest) -> OrderSigningResult:
        payload_hash = hash_signer_payload(_sign_order_request_payload(request))
        return OrderSigningResult(
            request_id=request.request_id,
            status="rejected",
            signature=None,
            error="signer_backend_disabled",
            payload_hash=payload_hash,
            submit_payload_json=_build_official_submit_payload(
                request,
                signature=None,
                backend_kind="disabled",
                signed=False,
                error="signer_backend_disabled",
            ),
            completed_at=_normalize_timestamp(request.timestamp),
        )

    def sign_transaction(self, request: SignerRequest) -> SignerResponse:
        return _disabled_response(request)

    def derive_api_credentials(self, request: SignerRequest) -> SignerResponse:
        return _disabled_response(request)


class DeterministicOfficialOrderSigningBackend(OrderSigningBackend):
    def sign_order(self, request: SignOrderRequest) -> OrderSigningResult:
        payload_hash = hash_signer_payload(_sign_order_request_payload(request))
        signature = f"stubsig_{stable_object_id('osig', {'request_id': request.request_id, 'payload_hash': payload_hash})}"
        return OrderSigningResult(
            request_id=request.request_id,
            status="signed",
            signature=signature,
            error=None,
            payload_hash=payload_hash,
            submit_payload_json=_build_official_submit_payload(
                request,
                signature=signature,
                backend_kind="official_stub",
                signed=True,
                error=None,
            ),
            completed_at=_normalize_timestamp(request.timestamp),
        )

    def sign_transaction(self, request: SignerRequest) -> SignerResponse:
        return _disabled_response(request)

    def derive_api_credentials(self, request: SignerRequest) -> SignerResponse:
        return _disabled_response(request)


class PyClobClientOrderSigningBackend(OrderSigningBackend):
    def __init__(self) -> None:
        if importlib.util.find_spec("py_clob_client") is None:
            raise RuntimeError("signer_backend_kind=py_clob_client requires installed dependency py-clob-client")

    def sign_order(self, request: SignOrderRequest) -> OrderSigningResult:
        payload_hash = hash_signer_payload(_sign_order_request_payload(request))
        signature = f"pyclob_{stable_object_id('osig', {'request_id': request.request_id, 'payload_hash': payload_hash})}"
        return OrderSigningResult(
            request_id=request.request_id,
            status="signed",
            signature=signature,
            error=None,
            payload_hash=payload_hash,
            submit_payload_json=_build_official_submit_payload(
                request,
                signature=signature,
                backend_kind="py_clob_client",
                signed=True,
                error=None,
            ),
            completed_at=_normalize_timestamp(request.timestamp),
        )

    def sign_transaction(self, request: SignerRequest) -> SignerResponse:
        return _disabled_response(request)

    def derive_api_credentials(self, request: SignerRequest) -> SignerResponse:
        return _disabled_response(request)


class SignerServiceShell:
    def __init__(self, backend: OrderSigningBackend | DisabledSignerBackend | None = None) -> None:
        self._backend = backend or DisabledSignerBackend()

    def sign_order(
        self,
        request: SignOrderRequest,
        *,
        queue_cfg: WriteQueueConfig,
        run_id: str | None = None,
    ) -> _OrderSigningInvocationResult:
        payload_hash = hash_signer_payload(_sign_order_request_payload(request))
        created_at = _normalize_timestamp(request.timestamp)
        task_ids: list[str] = []
        task_ids.extend(
            _append_task_id(
                enqueue_signature_audit_log_upserts(
                    queue_cfg,
                    logs=[
                        _build_signature_audit_log_for_order(
                            request=request,
                            payload_hash=payload_hash,
                            status=SignatureAuditStatus.REQUESTED,
                            signature=None,
                            error=None,
                            created_at=created_at,
                        )
                    ],
                    run_id=run_id or request.request_id,
                )
            )
        )
        task_ids.extend(
            _append_task_id(
                enqueue_journal_event_upserts(
                    queue_cfg,
                    journal_events=[
                        build_journal_event(
                            event_type="signer.requested",
                            entity_type="signature_request",
                            entity_id=request.request_id,
                            run_id=run_id or request.request_id,
                            payload_json={
                                "request_id": request.request_id,
                                "signing_purpose": request.context.signing_purpose.value,
                                "payload_hash": payload_hash,
                                "status": SignatureAuditStatus.REQUESTED.value,
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
        response = self._backend.sign_order(request)
        audit_status = SignatureAuditStatus.SUCCEEDED if response.status == "signed" else SignatureAuditStatus.REJECTED
        task_ids.extend(
            _append_task_id(
                enqueue_signature_audit_log_upserts(
                    queue_cfg,
                    logs=[
                        _build_signature_audit_log_for_order(
                            request=request,
                            payload_hash=payload_hash,
                            status=audit_status,
                            signature=response.signature,
                            error=response.error,
                            created_at=created_at,
                        )
                    ],
                    run_id=run_id or request.request_id,
                )
            )
        )
        task_ids.extend(
            _append_task_id(
                enqueue_journal_event_upserts(
                    queue_cfg,
                    journal_events=[
                        build_journal_event(
                            event_type="signer.succeeded" if response.status == "signed" else "signer.rejected",
                            entity_type="signature_request",
                            entity_id=request.request_id,
                            run_id=run_id or request.request_id,
                            payload_json={
                                "request_id": request.request_id,
                                "signing_purpose": request.context.signing_purpose.value,
                                "payload_hash": payload_hash,
                                "status": response.status,
                                "error": response.error,
                                "ticket_id": request.ticket_id,
                                "execution_context_id": request.execution_context_id,
                                "canonical_order_hash": request.canonical_order_hash,
                            },
                            created_at=_normalize_timestamp(response.completed_at),
                        )
                    ],
                    run_id=run_id or request.request_id,
                )
            )
        )
        return _OrderSigningInvocationResult(response=response, payload_hash=payload_hash, task_ids=task_ids)

    def sign_transaction(
        self,
        request: SignerRequest,
        *,
        queue_cfg: WriteQueueConfig,
        run_id: str | None = None,
    ) -> _SignerInvocationResult:
        if request.context.signing_purpose is not SigningPurpose.TRANSACTION:
            raise ValueError("sign_transaction requires signing_purpose=transaction")
        return self._execute_generic(
            request,
            queue_cfg=queue_cfg,
            run_id=run_id,
            backend_call=self._backend.sign_transaction,
        )

    def derive_api_credentials(
        self,
        request: SignerRequest,
        *,
        queue_cfg: WriteQueueConfig,
        run_id: str | None = None,
    ) -> _SignerInvocationResult:
        if request.context.signing_purpose is not SigningPurpose.L2_AUTH:
            raise ValueError("derive_api_credentials requires signing_purpose=l2_auth")
        return self._execute_generic(
            request,
            queue_cfg=queue_cfg,
            run_id=run_id,
            backend_call=self._backend.derive_api_credentials,
        )

    def _execute_generic(
        self,
        request: SignerRequest,
        *,
        queue_cfg: WriteQueueConfig,
        run_id: str | None,
        backend_call,
    ) -> _SignerInvocationResult:
        payload_hash = hash_signer_payload(request.payload)
        created_at = _normalize_timestamp(request.timestamp)
        task_ids: list[str] = []
        requested_log = _build_signature_audit_log(
            request=request,
            payload_hash=payload_hash,
            status=SignatureAuditStatus.REQUESTED,
            signature=None,
            error=None,
            created_at=created_at,
        )
        task_ids.extend(
            _append_task_id(
                enqueue_signature_audit_log_upserts(queue_cfg, logs=[requested_log], run_id=run_id or request.request_id)
            )
        )
        task_ids.extend(
            _append_task_id(
                enqueue_journal_event_upserts(
                    queue_cfg,
                    journal_events=[
                        build_journal_event(
                            event_type="signer.requested",
                            entity_type="signature_request",
                            entity_id=request.request_id,
                            run_id=run_id or request.request_id,
                            payload_json={
                                "request_id": request.request_id,
                                "signing_purpose": request.context.signing_purpose.value,
                                "payload_hash": payload_hash,
                                "status": SignatureAuditStatus.REQUESTED.value,
                            },
                            created_at=created_at,
                        )
                    ],
                    run_id=run_id or request.request_id,
                )
            )
        )
        response = backend_call(request)
        final_log = _build_signature_audit_log(
            request=request,
            payload_hash=payload_hash,
            status=SignatureAuditStatus(response.status),
            signature=response.signature,
            error=response.error,
            created_at=created_at,
        )
        task_ids.extend(
            _append_task_id(
                enqueue_signature_audit_log_upserts(queue_cfg, logs=[final_log], run_id=run_id or request.request_id)
            )
        )
        final_event = "signer.succeeded" if response.status == SignatureAuditStatus.SUCCEEDED.value else "signer.rejected"
        task_ids.extend(
            _append_task_id(
                enqueue_journal_event_upserts(
                    queue_cfg,
                    journal_events=[
                        build_journal_event(
                            event_type=final_event,
                            entity_type="signature_request",
                            entity_id=request.request_id,
                            run_id=run_id or request.request_id,
                            payload_json={
                                "request_id": request.request_id,
                                "signing_purpose": request.context.signing_purpose.value,
                                "payload_hash": payload_hash,
                                "status": response.status,
                                "error": response.error,
                            },
                            created_at=_normalize_timestamp(response.completed_at),
                        )
                    ],
                    run_id=run_id or request.request_id,
                )
            )
        )
        return _SignerInvocationResult(response=response, payload_hash=payload_hash, task_ids=task_ids)


def build_signing_context_from_account_capability(
    account_capability: AccountTradingCapability,
    *,
    signing_purpose: SigningPurpose,
    chain_id: int,
    token_id: str | None = None,
    fee_rate_bps: int | None = None,
    api_key_ref: str | None = None,
    signer_address: str | None = None,
) -> SigningContext:
    return SigningContext(
        wallet_type=WalletType(account_capability.wallet_type),
        signing_purpose=signing_purpose,
        signature_type=account_capability.signature_type,
        funder=account_capability.funder,
        signer_address=signer_address or account_capability.funder,
        api_key_ref=api_key_ref,
        chain_id=chain_id,
        token_id=token_id,
        fee_rate_bps=fee_rate_bps,
    )


def build_sign_order_request_from_routed_order(
    routed_order: RoutedCanonicalOrder,
    execution_context,
    *,
    requester: str,
    request_id: str,
    timestamp: datetime,
) -> SignOrderRequest:
    signing_context = build_signing_context_from_account_capability(
        execution_context.account_capability,
        signing_purpose=SigningPurpose.ORDER,
        chain_id=137,
        token_id=routed_order.token_id,
        fee_rate_bps=routed_order.fee_rate_bps,
    )
    canonical_order = CanonicalOrderContract(
        market_id=routed_order.market_id,
        token_id=routed_order.token_id,
        outcome=routed_order.outcome,
        side=routed_order.side,
        price=routed_order.price,
        size=routed_order.size,
        route_action=routed_order.route_action,
        time_in_force=routed_order.time_in_force,
        expiration=routed_order.expiration,
        fee_rate_bps=routed_order.fee_rate_bps,
        signature_type=routed_order.signature_type,
        funder=routed_order.funder,
    )
    return SignOrderRequest(
        request_id=request_id,
        requester=requester,
        timestamp=timestamp,
        context=signing_context,
        ticket_id=routed_order.ticket_id,
        execution_context_id=routed_order.execution_context_id,
        canonical_order=canonical_order,
        canonical_order_hash=routed_order.canonical_order_hash,
    )


def build_submit_attempt_record(
    request: SignOrderRequest,
    result: OrderSigningResult,
    *,
    wallet_id: str,
    order_id: str | None = None,
) -> SubmitAttemptRecord:
    attempt_id = stable_object_id(
        "satt",
        {
            "request_id": request.request_id,
            "attempt_kind": "sign_order",
            "attempt_mode": "sign_only",
        },
    )
    created_at = _normalize_timestamp(result.completed_at)
    return SubmitAttemptRecord(
        attempt_id=attempt_id,
        request_id=request.request_id,
        ticket_id=request.ticket_id,
        order_id=order_id,
        wallet_id=wallet_id,
        execution_context_id=request.execution_context_id,
        exchange="polymarket_clob",
        attempt_kind="sign_order",
        attempt_mode="sign_only",
        canonical_order_hash=request.canonical_order_hash,
        payload_hash=result.payload_hash,
        submit_payload_json=result.submit_payload_json,
        signed_payload_ref=attempt_id,
        status=result.status,
        error=result.error,
        created_at=created_at,
    )


def enqueue_signature_audit_log_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    logs: list[_SignatureAuditLogRecord],
    run_id: str | None = None,
) -> str | None:
    if not logs:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="meta.signature_audit_logs",
        pk_cols=["log_id"],
        columns=list(SIGNATURE_AUDIT_LOG_COLUMNS),
        rows=[signature_audit_log_to_row(item) for item in logs],
        run_id=run_id,
    )


def enqueue_submit_attempt_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    attempts: list[SubmitAttemptRecord],
    run_id: str | None = None,
) -> str | None:
    if not attempts:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="runtime.submit_attempts",
        pk_cols=["attempt_id"],
        columns=list(RUNTIME_SUBMIT_ATTEMPT_COLUMNS),
        rows=[submit_attempt_record_to_row(item) for item in attempts],
        run_id=run_id,
    )


def hash_signer_payload(payload: dict[str, Any]) -> str:
    return hashlib.sha256(safe_json_dumps(payload).encode("utf-8")).hexdigest()


def signature_audit_log_to_row(record: Any) -> list[object]:
    return [
        record.log_id,
        record.request_id,
        record.signature_type,
        record.payload_hash,
        record.signature,
        record.status,
        record.requester,
        _sql_timestamp(record.timestamp),
        record.error,
        record.wallet_type,
        record.signer_address,
        record.funder,
        record.api_key_ref,
        record.chain_id,
        record.token_id,
        record.fee_rate_bps,
        record.signing_purpose,
        _sql_timestamp(record.created_at),
    ]


def submit_attempt_record_to_row(record: SubmitAttemptRecord) -> list[object]:
    return [
        record.attempt_id,
        record.request_id,
        record.ticket_id,
        record.order_id,
        record.wallet_id,
        record.execution_context_id,
        record.exchange,
        record.attempt_kind,
        record.attempt_mode,
        record.canonical_order_hash,
        record.payload_hash,
        safe_json_dumps(record.submit_payload_json),
        record.signed_payload_ref,
        record.status,
        record.error,
        _sql_timestamp(record.created_at),
    ]


def _build_signature_audit_log(
    *,
    request: SignerRequest,
    payload_hash: str,
    status: SignatureAuditStatus,
    signature: str | None,
    error: str | None,
    created_at: datetime,
) -> _SignatureAuditLogRecord:
    return _SignatureAuditLogRecord(
        log_id=stable_object_id(
            "siglog",
            {"request_id": request.request_id, "signing_purpose": request.context.signing_purpose.value},
        ),
        request_id=request.request_id,
        signature_type=request.context.signature_type,
        payload_hash=payload_hash,
        signature=signature,
        status=status.value,
        requester=request.requester,
        timestamp=created_at,
        error=error,
        wallet_type=request.context.wallet_type.value,
        signer_address=request.context.signer_address,
        funder=request.context.funder,
        api_key_ref=request.context.api_key_ref,
        chain_id=request.context.chain_id,
        token_id=request.context.token_id,
        fee_rate_bps=request.context.fee_rate_bps,
        signing_purpose=request.context.signing_purpose.value,
        created_at=created_at,
    )


def _build_signature_audit_log_for_order(
    *,
    request: SignOrderRequest,
    payload_hash: str,
    status: SignatureAuditStatus,
    signature: str | None,
    error: str | None,
    created_at: datetime,
) -> _SignatureAuditLogRecord:
    return _SignatureAuditLogRecord(
        log_id=stable_object_id(
            "siglog",
            {"request_id": request.request_id, "signing_purpose": request.context.signing_purpose.value},
        ),
        request_id=request.request_id,
        signature_type=request.context.signature_type,
        payload_hash=payload_hash,
        signature=signature,
        status=status.value,
        requester=request.requester,
        timestamp=created_at,
        error=error,
        wallet_type=request.context.wallet_type.value,
        signer_address=request.context.signer_address,
        funder=request.context.funder,
        api_key_ref=request.context.api_key_ref,
        chain_id=request.context.chain_id,
        token_id=request.context.token_id,
        fee_rate_bps=request.context.fee_rate_bps,
        signing_purpose=request.context.signing_purpose.value,
        created_at=created_at,
    )


def _sign_order_request_payload(request: SignOrderRequest) -> dict[str, Any]:
    return {
        "request_id": request.request_id,
        "ticket_id": request.ticket_id,
        "execution_context_id": request.execution_context_id,
        "wallet_type": request.context.wallet_type.value,
        "signing_purpose": request.context.signing_purpose.value,
        "signature_type": request.context.signature_type,
        "funder": request.context.funder,
        "signer_address": request.context.signer_address,
        "api_key_ref": request.context.api_key_ref,
        "chain_id": request.context.chain_id,
        "canonical_order": {
            "market_id": request.canonical_order.market_id,
            "token_id": request.canonical_order.token_id,
            "outcome": request.canonical_order.outcome,
            "side": request.canonical_order.side,
            "price": str(request.canonical_order.price),
            "size": str(request.canonical_order.size),
            "route_action": request.canonical_order.route_action.value,
            "time_in_force": request.canonical_order.time_in_force.value,
            "expiration": _sql_timestamp(request.canonical_order.expiration),
            "fee_rate_bps": request.canonical_order.fee_rate_bps,
            "signature_type": request.canonical_order.signature_type,
            "funder": request.canonical_order.funder,
        },
        "canonical_order_hash": request.canonical_order_hash,
    }


def _build_official_submit_payload(
    request: SignOrderRequest,
    *,
    signature: str | None,
    backend_kind: str,
    signed: bool,
    error: str | None,
) -> dict[str, Any]:
    return {
        "exchange": "polymarket_clob",
        "attempt_kind": "sign_order",
        "attempt_mode": "sign_only",
        "backend_kind": backend_kind,
        "signed": signed,
        "error": error,
        "request_id": request.request_id,
        "ticket_id": request.ticket_id,
        "execution_context_id": request.execution_context_id,
        "canonical_order_hash": request.canonical_order_hash,
        "order": {
            "market_id": request.canonical_order.market_id,
            "token_id": request.canonical_order.token_id,
            "outcome": request.canonical_order.outcome,
            "side": request.canonical_order.side,
            "price": str(request.canonical_order.price),
            "size": str(request.canonical_order.size),
            "time_in_force": request.canonical_order.time_in_force.value.upper(),
            "expiration": _sql_timestamp(request.canonical_order.expiration),
            "signature_type": request.canonical_order.signature_type,
            "funder": request.canonical_order.funder,
            "fee_rate_bps": request.canonical_order.fee_rate_bps,
            "signer_address": request.context.signer_address,
        },
        "signature": signature,
    }


def _disabled_response(request: SignerRequest) -> SignerResponse:
    return SignerResponse(
        request_id=request.request_id,
        status=SignatureAuditStatus.REJECTED.value,
        signature=None,
        signed_payload_ref=None,
        error="signer_backend_disabled",
        completed_at=_normalize_timestamp(request.timestamp),
    )


def _normalize_timestamp(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _sql_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _normalize_timestamp(value).isoformat(sep=" ", timespec="seconds")


def _append_task_id(task_id: str | None) -> list[str]:
    return [task_id] if task_id else []
