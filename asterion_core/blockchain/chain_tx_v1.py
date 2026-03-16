from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
import hashlib
import json
from pathlib import Path
from typing import Any, Protocol

from eth_abi import encode as abi_encode
from web3 import Web3

from asterion_core.blockchain.wallet_state_v1 import PolygonChainRegistry
from asterion_core.contracts import AccountTradingCapability, stable_object_id
from asterion_core.journal import build_journal_event, enqueue_journal_event_upserts
from asterion_core.live_side_effect_guard_v1 import LiveSideEffectGuard, validate_live_side_effect_guard
from asterion_core.storage.os_queue import enqueue_upsert_rows_v1
from asterion_core.storage.utils import safe_json_dumps
from asterion_core.storage.write_queue import WriteQueueConfig


RUNTIME_CHAIN_TX_ATTEMPT_COLUMNS = [
    "attempt_id",
    "request_id",
    "wallet_id",
    "tx_kind",
    "tx_mode",
    "chain_id",
    "funder",
    "token_id",
    "allowance_target",
    "nonce",
    "gas_limit",
    "max_fee_per_gas",
    "max_priority_fee_per_gas",
    "payload_hash",
    "tx_payload_json",
    "signed_payload_ref",
    "tx_hash",
    "status",
    "error",
    "created_at",
]


class ChainTxKind(str, Enum):
    APPROVE_USDC = "approve_usdc"
    SPLIT = "split"
    MERGE = "merge"
    REDEEM = "redeem"


class ChainTxMode(str, Enum):
    DRY_RUN = "dry_run"
    SHADOW_BROADCAST = "shadow_broadcast"
    CONTROLLED_LIVE = "controlled_live"


@dataclass(frozen=True)
class GasEstimate:
    gas_limit: int
    max_fee_per_gas: int
    max_priority_fee_per_gas: int

    def __post_init__(self) -> None:
        if self.gas_limit <= 0:
            raise ValueError("gas_limit must be positive")
        if self.max_fee_per_gas <= 0 or self.max_priority_fee_per_gas <= 0:
            raise ValueError("gas fees must be positive")


@dataclass(frozen=True)
class NonceSelection:
    nonce: int

    def __post_init__(self) -> None:
        if self.nonce < 0:
            raise ValueError("nonce must be non-negative")


@dataclass(frozen=True)
class ChainTxRequest:
    request_id: str
    requester: str
    timestamp: datetime
    wallet_id: str
    tx_kind: ChainTxKind
    tx_mode: ChainTxMode
    chain_id: int
    funder: str
    spender: str | None
    token_address: str | None
    token_id: str | None
    amount: Decimal | None
    nonce: int
    gas_estimate: GasEstimate
    approval_id: str | None = None
    approval_reason: str | None = None

    def __post_init__(self) -> None:
        if not self.request_id or not self.requester or not self.wallet_id or not self.funder:
            raise ValueError("request_id, requester, wallet_id, and funder are required")
        if self.chain_id <= 0:
            raise ValueError("chain_id must be positive")
        if self.nonce < 0:
            raise ValueError("nonce must be non-negative")
        if self.tx_kind is ChainTxKind.APPROVE_USDC:
            if not self.spender or not self.token_address or not self.token_id:
                raise ValueError("approve_usdc requires spender, token_address, and token_id")
            if self.amount is None or self.amount <= 0:
                raise ValueError("approve_usdc requires a positive amount")


@dataclass(frozen=True)
class ChainTxAttemptRecord:
    attempt_id: str
    request_id: str
    wallet_id: str
    tx_kind: str
    tx_mode: str
    chain_id: int
    funder: str
    token_id: str | None
    allowance_target: str | None
    nonce: int | None
    gas_limit: int | None
    max_fee_per_gas: int | None
    max_priority_fee_per_gas: int | None
    payload_hash: str
    tx_payload_json: dict[str, Any]
    signed_payload_ref: str | None
    tx_hash: str | None
    status: str
    error: str | None
    created_at: datetime


@dataclass(frozen=True)
class ChainTxResult:
    request_id: str
    status: str
    payload_hash: str
    tx_payload_json: dict[str, Any]
    tx_hash: str | None
    error: str | None
    completed_at: datetime

    def __post_init__(self) -> None:
        if self.status not in {"previewed", "accepted", "rejected", "broadcasted"}:
            raise ValueError("chain tx status must be previewed, accepted, rejected, or broadcasted")
        if not self.payload_hash:
            raise ValueError("payload_hash is required")
        if not isinstance(self.tx_payload_json, dict) or not self.tx_payload_json:
            raise ValueError("tx_payload_json must be a non-empty object")


@dataclass(frozen=True)
class _ChainTxInvocationResult:
    response: ChainTxResult
    payload_hash: str
    task_ids: list[str]


@dataclass(frozen=True)
class ControlledLiveSmokeWalletPolicy:
    wallet_id: str
    allowed_tx_kinds: list[str]
    allowed_spenders: list[str]
    max_approve_amount: Decimal


@dataclass(frozen=True)
class ControlledLiveSmokePolicy:
    chain_id: int
    wallets: list[ControlledLiveSmokeWalletPolicy]

    def wallet_policy(self, wallet_id: str) -> ControlledLiveSmokeWalletPolicy:
        for item in self.wallets:
            if item.wallet_id == wallet_id:
                return item
        raise ValueError(f"wallet is not allowlisted for controlled live smoke: {wallet_id}")


class ChainTxReader(Protocol):
    def select_nonce(self, funder: str) -> NonceSelection:
        ...

    def estimate_approve_usdc_gas(self) -> GasEstimate:
        ...


class ChainTxBackend:
    def broadcast(self, request: ChainTxRequest, *, signed_payload_json: dict[str, Any]) -> ChainTxResult:
        raise NotImplementedError


class PolygonChainTxReader:
    def __init__(self, *, chain_id: int, rpc_urls: list[str], source: str = "polygon_rpc") -> None:
        urls = [str(item).strip() for item in rpc_urls if str(item).strip()]
        if chain_id <= 0:
            raise ValueError("chain_id must be positive")
        if not urls:
            raise ValueError("capability_rpc_urls are required for chain tx scaffold")
        self._chain_id = int(chain_id)
        self._rpc_urls = urls
        self._source = source

    def select_nonce(self, funder: str) -> NonceSelection:
        funder_address = _normalize_address(funder)
        return self._with_web3(
            lambda web3: NonceSelection(
                nonce=int(web3.eth.get_transaction_count(funder_address, "pending"))
            )
        )

    def estimate_approve_usdc_gas(self) -> GasEstimate:
        def _estimate(web3):
            block = web3.eth.get_block("latest")
            base_fee = int(block.get("baseFeePerGas") or 30_000_000_000)
            priority_fee = 2_000_000_000
            return GasEstimate(
                gas_limit=120000,
                max_fee_per_gas=(base_fee * 2) + priority_fee,
                max_priority_fee_per_gas=priority_fee,
            )

        return self._with_web3(_estimate)

    def _with_web3(self, fn):
        last_error: Exception | None = None
        for rpc_url in self._rpc_urls:
            try:
                web3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 10}))
                observed_chain_id = int(web3.eth.chain_id)
                if observed_chain_id != self._chain_id:
                    raise RuntimeError(
                        f"chain tx reader chain_id mismatch: expected {self._chain_id}, got {observed_chain_id}"
                    )
                return fn(web3)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
        raise RuntimeError("chain tx scaffold failed for all configured rpc urls") from last_error


class DisabledChainTxBackend(ChainTxBackend):
    def broadcast(self, request: ChainTxRequest, *, signed_payload_json: dict[str, Any]) -> ChainTxResult:
        envelope = _build_tx_payload_envelope(
            request,
            signed_payload_json=signed_payload_json,
            backend_kind="disabled",
            status="rejected",
            tx_hash=None,
            error="chain_tx_backend_disabled",
        )
        return ChainTxResult(
            request_id=request.request_id,
            status="rejected",
            payload_hash=_hash_payload(envelope),
            tx_payload_json=envelope,
            tx_hash=None,
            error="chain_tx_backend_disabled",
            completed_at=_normalize_timestamp(request.timestamp),
        )


class ShadowBroadcastBackend(ChainTxBackend):
    def broadcast(self, request: ChainTxRequest, *, signed_payload_json: dict[str, Any]) -> ChainTxResult:
        should_reject = bool(signed_payload_json.get("shadow_reject"))
        status = "rejected" if should_reject else "accepted"
        tx_hash = None
        if status == "accepted":
            tx_hash = "0x" + _hash_payload(
                {
                    "request_id": request.request_id,
                    "tx_kind": request.tx_kind.value,
                    "wallet_id": request.wallet_id,
                    "nonce": request.nonce,
                }
            )[:64]
        envelope = _build_tx_payload_envelope(
            request,
            signed_payload_json=signed_payload_json,
            backend_kind="shadow_stub",
            status=status,
            tx_hash=tx_hash,
            error="shadow_broadcast_rejected" if should_reject else None,
        )
        return ChainTxResult(
            request_id=request.request_id,
            status=status,
            payload_hash=_hash_payload(envelope),
            tx_payload_json=envelope,
            tx_hash=tx_hash,
            error="shadow_broadcast_rejected" if should_reject else None,
            completed_at=_normalize_timestamp(request.timestamp),
        )


class RealBroadcastBackend(ChainTxBackend):
    def __init__(self, *, chain_id: int, rpc_urls: list[str]) -> None:
        urls = [str(item).strip() for item in rpc_urls if str(item).strip()]
        if chain_id <= 0:
            raise ValueError("chain_id must be positive")
        if not urls:
            raise ValueError("capability_rpc_urls are required for real_broadcast backend")
        self._chain_id = int(chain_id)
        self._rpc_urls = urls

    def broadcast(self, request: ChainTxRequest, *, signed_payload_json: dict[str, Any]) -> ChainTxResult:
        raw_transaction_hex = str(signed_payload_json.get("raw_transaction_hex") or "").strip()
        if not raw_transaction_hex:
            envelope = _build_tx_payload_envelope(
                request,
                signed_payload_json=signed_payload_json,
                backend_kind="real_broadcast",
                status="rejected",
                tx_hash=None,
                error="missing_raw_transaction_hex",
            )
            return ChainTxResult(
                request_id=request.request_id,
                status="rejected",
                payload_hash=_hash_payload(envelope),
                tx_payload_json=envelope,
                tx_hash=None,
                error="missing_raw_transaction_hex",
                completed_at=_normalize_timestamp(request.timestamp),
            )
        tx_hash_hex = self._broadcast_raw_transaction(raw_transaction_hex)
        envelope = _build_tx_payload_envelope(
            request,
            signed_payload_json=signed_payload_json,
            backend_kind="real_broadcast",
            status="broadcasted",
            tx_hash=tx_hash_hex,
            error=None,
        )
        return ChainTxResult(
            request_id=request.request_id,
            status="broadcasted",
            payload_hash=_hash_payload(envelope),
            tx_payload_json=envelope,
            tx_hash=tx_hash_hex,
            error=None,
            completed_at=_normalize_timestamp(request.timestamp),
        )

    def _broadcast_raw_transaction(self, raw_transaction_hex: str) -> str:
        last_error: Exception | None = None
        for rpc_url in self._rpc_urls:
            try:
                web3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 10}))
                observed_chain_id = int(web3.eth.chain_id)
                if observed_chain_id != self._chain_id:
                    raise RuntimeError(
                        f"real_broadcast chain_id mismatch: expected {self._chain_id}, got {observed_chain_id}"
                    )
                tx_hash = web3.eth.send_raw_transaction(Web3.to_bytes(hexstr=raw_transaction_hex))
                return tx_hash.hex()
            except Exception as exc:  # noqa: BLE001
                last_error = exc
        raise RuntimeError("real broadcast failed for all configured rpc urls") from last_error


class ChainTxServiceShell:
    def __init__(self, backend: ChainTxBackend | None = None) -> None:
        self._backend = backend or DisabledChainTxBackend()

    def submit_transaction(
        self,
        request: ChainTxRequest,
        *,
        signed_payload_json: dict[str, Any],
        live_guard: LiveSideEffectGuard | None = None,
        queue_cfg: WriteQueueConfig,
        run_id: str | None = None,
    ) -> _ChainTxInvocationResult:
        created_at = _normalize_timestamp(request.timestamp)
        task_ids: list[str] = []
        task_ids.extend(
            _append_task_id(
                enqueue_journal_event_upserts(
                    queue_cfg,
                    journal_events=[
                        build_journal_event(
                            event_type="chain_tx.requested",
                            entity_type="chain_tx_request",
                            entity_id=request.request_id,
                            run_id=run_id or request.request_id,
                            payload_json={
                                "request_id": request.request_id,
                                "wallet_id": request.wallet_id,
                                "tx_kind": request.tx_kind.value,
                                "tx_mode": request.tx_mode.value,
                                "payload_hash": _hash_payload(_build_unsigned_tx_payload(request)),
                            },
                            created_at=created_at,
                        )
                    ],
                    run_id=run_id or request.request_id,
                )
            )
        )
        if request.tx_kind is not ChainTxKind.APPROVE_USDC:
            response = _build_rejected_result(
                request,
                signed_payload_json=signed_payload_json,
                backend_kind="chain_tx_shell",
                error="tx_kind_not_enabled_in_p4_07",
            )
        elif request.tx_mode is ChainTxMode.DRY_RUN:
            response = _build_dry_run_preview(request, signed_payload_json=signed_payload_json)
        elif request.tx_mode is ChainTxMode.CONTROLLED_LIVE:
            guard_error = validate_live_side_effect_guard(expected_mode="controlled_live", guard=live_guard)
            if guard_error:
                response = _build_rejected_result(
                    request,
                    signed_payload_json=signed_payload_json,
                    backend_kind="chain_tx_shell",
                    error=guard_error,
                )
            else:
                response = self._backend.broadcast(request, signed_payload_json=signed_payload_json)
        else:
            response = self._backend.broadcast(request, signed_payload_json=signed_payload_json)
        final_event = {
            "previewed": "chain_tx.previewed",
            "accepted": "chain_tx.accepted",
            "rejected": "chain_tx.rejected",
            "broadcasted": "chain_tx.broadcasted",
        }[response.status]
        task_ids.extend(
            _append_task_id(
                enqueue_journal_event_upserts(
                    queue_cfg,
                    journal_events=[
                        build_journal_event(
                            event_type=final_event,
                            entity_type="chain_tx_request",
                            entity_id=request.request_id,
                            run_id=run_id or request.request_id,
                            payload_json={
                                "request_id": request.request_id,
                                "wallet_id": request.wallet_id,
                                "tx_kind": request.tx_kind.value,
                                "tx_mode": request.tx_mode.value,
                                "payload_hash": response.payload_hash,
                                "status": response.status,
                                "tx_hash": response.tx_hash,
                                "error": response.error,
                            },
                            created_at=_normalize_timestamp(response.completed_at),
                        )
                    ],
                    run_id=run_id or request.request_id,
                )
            )
        )
        return _ChainTxInvocationResult(response=response, payload_hash=response.payload_hash, task_ids=task_ids)


def build_approve_usdc_request(
    *,
    account_capability: AccountTradingCapability,
    chain_registry: PolygonChainRegistry,
    chain_tx_reader: ChainTxReader,
    requester: str,
    request_id: str,
    timestamp: datetime,
    tx_mode: ChainTxMode,
    spender: str,
    amount: Decimal,
    approval_id: str | None = None,
    approval_reason: str | None = None,
) -> ChainTxRequest:
    normalized_spender = _normalize_address(spender)
    allowed_spenders = {_normalize_address(item) for item in account_capability.allowance_targets}
    registry_spenders = {_normalize_address(item) for item in chain_registry.allowance_targets.values()}
    if normalized_spender not in allowed_spenders or normalized_spender not in registry_spenders:
        raise ValueError("spender must exist in both wallet capability allowance_targets and chain registry")
    if amount <= 0:
        raise ValueError("amount must be positive")
    nonce = chain_tx_reader.select_nonce(account_capability.funder)
    gas_estimate = chain_tx_reader.estimate_approve_usdc_gas()
    return ChainTxRequest(
        request_id=request_id,
        requester=requester,
        timestamp=timestamp,
        wallet_id=account_capability.wallet_id,
        tx_kind=ChainTxKind.APPROVE_USDC,
        tx_mode=tx_mode,
        chain_id=chain_registry.chain_id,
        funder=_normalize_address(account_capability.funder),
        spender=normalized_spender,
        token_address=_normalize_address(chain_registry.usdc_e_contract_address),
        token_id=chain_registry.usdc_e_token_id,
        amount=amount,
        nonce=nonce.nonce,
        gas_estimate=gas_estimate,
        approval_id=approval_id,
        approval_reason=approval_reason,
    )


def build_transaction_signer_request(request: ChainTxRequest, account_capability: AccountTradingCapability):
    from asterion_core.signer import SignerRequest, SigningPurpose, build_signing_context_from_account_capability

    return SignerRequest(
        request_id=request.request_id,
        requester=request.requester,
        timestamp=request.timestamp,
        context=build_signing_context_from_account_capability(
            account_capability,
            signing_purpose=SigningPurpose.TRANSACTION,
            chain_id=request.chain_id,
        ),
        payload=_build_unsigned_tx_payload(request),
    )


def build_chain_tx_attempt_record(
    request: ChainTxRequest,
    result: ChainTxResult,
    *,
    signed_payload_ref: str | None,
) -> ChainTxAttemptRecord:
    return ChainTxAttemptRecord(
        attempt_id=stable_object_id(
            "ctxatt",
            {
                "request_id": request.request_id,
                "tx_kind": request.tx_kind.value,
                "tx_mode": request.tx_mode.value,
            },
        ),
        request_id=request.request_id,
        wallet_id=request.wallet_id,
        tx_kind=request.tx_kind.value,
        tx_mode=request.tx_mode.value,
        chain_id=request.chain_id,
        funder=request.funder,
        token_id=request.token_id,
        allowance_target=request.spender,
        nonce=request.nonce,
        gas_limit=request.gas_estimate.gas_limit,
        max_fee_per_gas=request.gas_estimate.max_fee_per_gas,
        max_priority_fee_per_gas=request.gas_estimate.max_priority_fee_per_gas,
        payload_hash=result.payload_hash,
        tx_payload_json=result.tx_payload_json,
        signed_payload_ref=signed_payload_ref,
        tx_hash=result.tx_hash,
        status=result.status,
        error=result.error,
        created_at=_normalize_timestamp(result.completed_at),
    )


def chain_tx_attempt_to_row(record: ChainTxAttemptRecord) -> list[object]:
    return [
        record.attempt_id,
        record.request_id,
        record.wallet_id,
        record.tx_kind,
        record.tx_mode,
        record.chain_id,
        record.funder,
        record.token_id,
        record.allowance_target,
        record.nonce,
        record.gas_limit,
        record.max_fee_per_gas,
        record.max_priority_fee_per_gas,
        record.payload_hash,
        safe_json_dumps(record.tx_payload_json),
        record.signed_payload_ref,
        record.tx_hash,
        record.status,
        record.error,
        _sql_timestamp(record.created_at),
    ]


def enqueue_chain_tx_attempt_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    attempts: list[ChainTxAttemptRecord],
    run_id: str | None = None,
) -> str | None:
    if not attempts:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="runtime.chain_tx_attempts",
        pk_cols=["attempt_id"],
        columns=list(RUNTIME_CHAIN_TX_ATTEMPT_COLUMNS),
        rows=[chain_tx_attempt_to_row(item) for item in attempts],
        run_id=run_id,
    )


def load_latest_wallet_state_gate(con, *, wallet_id: str, chain_registry: PolygonChainRegistry, spender: str) -> dict[str, Decimal]:
    rows = con.execute(
        """
        SELECT
            asset_type,
            token_id,
            observation_kind,
            allowance_target,
            observed_quantity
        FROM runtime.external_balance_observations
        WHERE wallet_id = ?
        ORDER BY observed_at DESC, observation_id DESC
        """,
        [wallet_id],
    ).fetchall()
    seen: dict[tuple[str, str | None, str, str | None], Decimal] = {}
    for row in rows:
        key = (str(row[0]), str(row[1]) if row[1] is not None else None, str(row[2]), str(row[3]) if row[3] is not None else None)
        if key in seen:
            continue
        seen[key] = Decimal(str(row[4]))
    normalized_spender = _normalize_address(spender)
    required = {
        ("native_gas", None, "wallet_balance", None): "native_gas_balance",
        (chain_registry.usdc_e_asset_type, chain_registry.usdc_e_token_id, "wallet_balance", None): "usdc_e_balance",
        (chain_registry.usdc_e_asset_type, chain_registry.usdc_e_token_id, "token_allowance", normalized_spender): "usdc_e_allowance",
    }
    result: dict[str, Decimal] = {}
    missing: list[str] = []
    for key, label in required.items():
        if key not in seen:
            missing.append(label)
        else:
            result[label] = seen[key]
    if missing:
        raise ValueError("missing required wallet state observations: " + ", ".join(sorted(missing)))
    return result


def _build_dry_run_preview(request: ChainTxRequest, *, signed_payload_json: dict[str, Any]) -> ChainTxResult:
    envelope = _build_tx_payload_envelope(
        request,
        signed_payload_json=signed_payload_json,
        backend_kind="dry_run_preview",
        status="previewed",
        tx_hash=None,
        error=None,
    )
    return ChainTxResult(
        request_id=request.request_id,
        status="previewed",
        payload_hash=_hash_payload(envelope),
        tx_payload_json=envelope,
        tx_hash=None,
        error=None,
        completed_at=_normalize_timestamp(request.timestamp),
    )


def _build_rejected_result(
    request: ChainTxRequest,
    *,
    signed_payload_json: dict[str, Any],
    backend_kind: str,
    error: str,
) -> ChainTxResult:
    envelope = _build_tx_payload_envelope(
        request,
        signed_payload_json=signed_payload_json,
        backend_kind=backend_kind,
        status="rejected",
        tx_hash=None,
        error=error,
    )
    return ChainTxResult(
        request_id=request.request_id,
        status="rejected",
        payload_hash=_hash_payload(envelope),
        tx_payload_json=envelope,
        tx_hash=None,
        error=error,
        completed_at=_normalize_timestamp(request.timestamp),
    )


def _build_tx_payload_envelope(
    request: ChainTxRequest,
    *,
    signed_payload_json: dict[str, Any],
    backend_kind: str,
    status: str,
    tx_hash: str | None,
    error: str | None,
) -> dict[str, Any]:
    return {
        "backend_kind": backend_kind,
        "status": status,
        "request_id": request.request_id,
        "wallet_id": request.wallet_id,
        "tx_kind": request.tx_kind.value,
        "tx_mode": request.tx_mode.value,
        "unsigned_tx": _build_unsigned_tx_payload(request),
        "signed_payload": _sanitize_signed_payload_json(signed_payload_json),
        "tx_hash": tx_hash,
        "error": error,
    }


def _build_unsigned_tx_payload(request: ChainTxRequest) -> dict[str, Any]:
    amount = request.amount or Decimal("0")
    token_decimals = 6 if request.token_id == "usdc_e" else 18
    amount_raw_int = int(amount * (Decimal(10) ** token_decimals))
    return {
        "request_id": request.request_id,
        "wallet_id": request.wallet_id,
        "tx_kind": request.tx_kind.value,
        "chain_id": request.chain_id,
        "from": request.funder,
        "to": request.token_address,
        "token_id": request.token_id,
        "spender": request.spender,
        "amount": str(amount),
        "amount_raw": str(amount_raw_int),
        "nonce": request.nonce,
        "gas_limit": request.gas_estimate.gas_limit,
        "max_fee_per_gas": request.gas_estimate.max_fee_per_gas,
        "max_priority_fee_per_gas": request.gas_estimate.max_priority_fee_per_gas,
        "value": "0",
        "data": _encode_approve_usdc_call_data(request.spender, amount_raw_int)
        if request.tx_kind is ChainTxKind.APPROVE_USDC
        else None,
        "approval_id": request.approval_id,
        "approval_reason": request.approval_reason,
        "method": "approve(address,uint256)" if request.tx_kind is ChainTxKind.APPROVE_USDC else request.tx_kind.value,
    }


def load_controlled_live_smoke_policy(path: str | Path) -> ControlledLiveSmokePolicy:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    chain_id = int(payload.get("chain_id") or 0)
    if chain_id <= 0:
        raise ValueError("controlled live smoke policy requires positive chain_id")
    wallets_raw = payload.get("wallets")
    if not isinstance(wallets_raw, list) or not wallets_raw:
        raise ValueError("controlled live smoke policy requires non-empty wallets")
    wallets: list[ControlledLiveSmokeWalletPolicy] = []
    for item in wallets_raw:
        if not isinstance(item, dict):
            raise ValueError("controlled live smoke wallet entries must be objects")
        max_approve_amount = Decimal(str(item.get("max_approve_amount") or "0"))
        if max_approve_amount <= 0:
            raise ValueError("controlled live smoke wallet policy requires positive max_approve_amount")
        wallets.append(
            ControlledLiveSmokeWalletPolicy(
                wallet_id=str(item.get("wallet_id") or "").strip(),
                allowed_tx_kinds=[str(value).strip() for value in list(item.get("allowed_tx_kinds") or []) if str(value).strip()],
                allowed_spenders=[_normalize_address(value) for value in list(item.get("allowed_spenders") or []) if str(value).strip()],
                max_approve_amount=max_approve_amount,
            )
        )
    invalid_wallets = [
        item.wallet_id
        for item in wallets
        if not item.wallet_id or not item.allowed_tx_kinds or not item.allowed_spenders
    ]
    if invalid_wallets:
        raise ValueError("controlled live smoke policy contains incomplete wallet entries")
    return ControlledLiveSmokePolicy(chain_id=chain_id, wallets=wallets)


def controlled_live_wallet_secret_env_var(wallet_id: str) -> str:
    normalized = "".join(char if char.isalnum() else "_" for char in str(wallet_id).strip())
    normalized = "_".join(part for part in normalized.upper().split("_") if part)
    if not normalized:
        raise ValueError("wallet_id is required to derive controlled live secret env var")
    return f"ASTERION_CONTROLLED_LIVE_SECRET_PK_{normalized}"


def _sanitize_signed_payload_json(signed_payload_json: dict[str, Any]) -> dict[str, Any]:
    def _scrub(value: Any) -> Any:
        if isinstance(value, dict):
            cleaned = {}
            for key, item in value.items():
                if key in {"raw_transaction_hex", "raw_transaction", "raw_signed_tx", "private_key_env_var"}:
                    continue
                cleaned[key] = _scrub(item)
            return cleaned
        if isinstance(value, list):
            return [_scrub(item) for item in value]
        return value

    persisted = _scrub(dict(signed_payload_json))
    return persisted


def _encode_approve_usdc_call_data(spender: str | None, amount_raw_int: int) -> str | None:
    if not spender:
        return None
    selector = Web3.keccak(text="approve(address,uint256)")[:4]
    encoded_args = abi_encode(["address", "uint256"], [_normalize_address(spender), amount_raw_int])
    return "0x" + (selector + encoded_args).hex()


def _normalize_timestamp(value: datetime) -> datetime:
    if value.tzinfo is not None:
        return value.astimezone(UTC).replace(tzinfo=None)
    return value


def _normalize_address(value: str) -> str:
    return Web3.to_checksum_address(str(value).strip())


def _append_task_id(task_id: str | None) -> list[str]:
    return [task_id] if task_id is not None else []


def _hash_payload(payload: dict[str, Any]) -> str:
    return hashlib.sha256(safe_json_dumps(payload).encode("utf-8")).hexdigest()


def _sql_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _normalize_timestamp(value).isoformat(sep=" ", timespec="seconds")
