from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
import time
from typing import Any

import httpx
from web3 import Web3
from web3._utils.events import get_event_data

from asterion_core.clients.gamma import infer_condition_id
from asterion_core.contracts import stable_object_id

from .watcher_replay import UMAEvent


POLYGON_OPTIMISTIC_ORACLE_ADDRESSES = [
    "0xeE3Afe347D5C74317041E2618C49534dAf887c24",
    "0xBb1A8db2D4350976a11cdfA60A1d43f97710Da49",
    "0x2C0367a9DB231dDeBd88a94b4f6461a6e47C58B1",
]

POLYGON_ADAPTER_ADDRESSES = [
    "0x6A9D222616C90FcA5754cd1333cFD9b7fb6a4F74",
    "0x157Ce2d672854c848c9b79C49a8Cc6cc89176a49",
    "0x65070BE91477460D8A7AeEb94ef92fe056C2f2A7",
    "0x2F5e3684cb1F318ec51b00Edba38d79Ac2c0aA9d",
    "0x69c47De9D4D3Dad79590d61b9e05918E03775f24",
]

PROPOSE_PRICE_EVENT_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": True, "internalType": "address", "name": "requester", "type": "address"},
        {"indexed": True, "internalType": "address", "name": "proposer", "type": "address"},
        {"indexed": False, "internalType": "bytes32", "name": "identifier", "type": "bytes32"},
        {"indexed": False, "internalType": "uint256", "name": "timestamp", "type": "uint256"},
        {"indexed": False, "internalType": "bytes", "name": "ancillaryData", "type": "bytes"},
        {"indexed": False, "internalType": "int256", "name": "proposedPrice", "type": "int256"},
        {"indexed": False, "internalType": "uint256", "name": "expirationTimestamp", "type": "uint256"},
        {"indexed": False, "internalType": "address", "name": "currency", "type": "address"},
    ],
    "name": "ProposePrice",
    "type": "event",
}

QUESTION_RESOLVED_EVENT_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": True, "internalType": "bytes32", "name": "questionID", "type": "bytes32"},
        {"indexed": True, "internalType": "int256", "name": "settledPrice", "type": "int256"},
        {"indexed": False, "internalType": "uint256[]", "name": "payouts", "type": "uint256[]"},
    ],
    "name": "QuestionResolved",
    "type": "event",
}

PROPOSE_PRICE_TOPIC = Web3.to_hex(Web3.keccak(text="ProposePrice(address,address,bytes32,uint256,bytes,int256,uint256,address)"))
QUESTION_RESOLVED_TOPIC = Web3.to_hex(Web3.keccak(text="QuestionResolved(bytes32,int256,uint256[])"))


@dataclass(frozen=True)
class _MarketRef:
    market_id: str
    condition_id: str


class PolygonRealtimeWatcherRpcClient:
    def __init__(
        self,
        *,
        rpc_url: str,
        timeout_seconds: float = 10.0,
        headers: dict[str, str] | None = None,
        clob_base_url: str = "https://clob.polymarket.com",
        min_request_interval_seconds: float = 0.4,
        max_retries: int = 2,
        retry_backoff_seconds: float = 1.0,
        allow_remote_market_lookup: bool = False,
    ) -> None:
        rpc_url = str(rpc_url).strip()
        if not rpc_url:
            raise ValueError("rpc_url is required")
        self._rpc_url = rpc_url
        self._headers = dict(headers or {})
        self._client = httpx.Client(timeout=float(timeout_seconds), headers=self._headers or None)
        self._timeout_seconds = float(timeout_seconds)
        self._clob_base_url = clob_base_url.rstrip("/")
        self._min_request_interval_seconds = max(0.0, float(min_request_interval_seconds))
        self._max_retries = max(0, int(max_retries))
        self._retry_backoff_seconds = max(0.0, float(retry_backoff_seconds))
        self._allow_remote_market_lookup = bool(allow_remote_market_lookup)
        self._last_rpc_started_at = 0.0
        self._codec = Web3().codec
        self._market_by_question_id: dict[str, _MarketRef | None] = {}
        self._block_timestamps: dict[int, datetime] = {}

    def get_finalized_block_number(self) -> int:
        payload = self._rpc("eth_blockNumber", [])
        return int(str(payload), 16)

    def get_events(self, from_block: int, to_block: int) -> list[UMAEvent]:
        events: list[UMAEvent] = []
        for block_start in range(int(from_block), int(to_block) + 1, 1000):
            block_end = min(int(to_block), block_start + 999)
            events.extend(self._load_propose_price_events(block_start, block_end))
            events.extend(self._load_question_resolved_events(block_start, block_end))
        return sorted(events, key=lambda item: (item.block_number, item.log_index))

    def get_proposal_state(
        self,
        *,
        proposal_id: str | None = None,
        tx_hash: str | None = None,
        condition_id: str | None = None,
    ) -> dict[str, Any] | None:
        del proposal_id, tx_hash, condition_id
        return None

    def seed_market_refs(self, refs: dict[str, dict[str, str] | tuple[str, str]]) -> None:
        for question_id, payload in dict(refs or {}).items():
            question_id_hex = str(question_id or "").strip().lower()
            if not question_id_hex:
                continue
            if isinstance(payload, tuple):
                market_id, condition_id = payload
            else:
                market_id = str(payload.get("market_id") or "").strip()
                condition_id = str(payload.get("condition_id") or "").strip()
            if not market_id or not condition_id:
                continue
            self._market_by_question_id[question_id_hex] = _MarketRef(market_id=market_id, condition_id=condition_id)

    def _load_propose_price_events(self, from_block: int, to_block: int) -> list[UMAEvent]:
        logs = self._eth_get_logs(
            from_block=from_block,
            to_block=to_block,
            addresses=POLYGON_OPTIMISTIC_ORACLE_ADDRESSES,
            topic0=PROPOSE_PRICE_TOPIC,
        )
        out: list[UMAEvent] = []
        for log in logs:
            decoded = get_event_data(self._codec, PROPOSE_PRICE_EVENT_ABI, log)
            ancillary_data = bytes(decoded["args"]["ancillaryData"])
            question_id = Web3.keccak(ancillary_data).hex()
            market_ref = self._load_market_by_question_id(question_id)
            if market_ref is None:
                continue
            request_timestamp = datetime.fromtimestamp(int(decoded["args"]["timestamp"]), tz=UTC).replace(tzinfo=None)
            proposed_price = int(decoded["args"]["proposedPrice"])
            out.append(
                UMAEvent(
                    tx_hash=decoded["transactionHash"].hex(),
                    log_index=int(decoded["logIndex"]),
                    block_number=int(decoded["blockNumber"]),
                    event_type="proposal_created",
                    proposal_id=stable_object_id("umaprop", {"question_id": question_id}),
                    market_id=market_ref.market_id,
                    condition_id=market_ref.condition_id,
                    proposer=str(decoded["args"]["proposer"]),
                    proposed_outcome="YES" if proposed_price > 0 else "NO",
                    proposal_bond=0.0,
                    dispute_bond=0.0,
                    proposal_timestamp=request_timestamp,
                    on_chain_settled_at=None,
                    safe_redeem_after=None,
                    human_review_required=False,
                )
            )
        return out

    def _load_question_resolved_events(self, from_block: int, to_block: int) -> list[UMAEvent]:
        logs = self._eth_get_logs(
            from_block=from_block,
            to_block=to_block,
            addresses=POLYGON_ADAPTER_ADDRESSES,
            topic0=QUESTION_RESOLVED_TOPIC,
        )
        out: list[UMAEvent] = []
        for log in logs:
            decoded = get_event_data(self._codec, QUESTION_RESOLVED_EVENT_ABI, log)
            question_id = decoded["args"]["questionID"].hex()
            market_ref = self._load_market_by_question_id(question_id)
            if market_ref is None:
                continue
            settled_at = self._load_block_timestamp(int(decoded["blockNumber"]))
            out.append(
                UMAEvent(
                    tx_hash=decoded["transactionHash"].hex(),
                    log_index=int(decoded["logIndex"]),
                    block_number=int(decoded["blockNumber"]),
                    event_type="proposal_settled",
                    proposal_id=stable_object_id("umaprop", {"question_id": question_id}),
                    market_id=market_ref.market_id,
                    condition_id=market_ref.condition_id,
                    proposer=None,
                    proposed_outcome=None,
                    proposal_bond=None,
                    dispute_bond=None,
                    proposal_timestamp=None,
                    on_chain_settled_at=settled_at,
                    safe_redeem_after=settled_at,
                    human_review_required=False,
                )
            )
        return out

    def _eth_get_logs(self, *, from_block: int, to_block: int, addresses: list[str], topic0: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for address in addresses:
            params = [
                {
                    "fromBlock": hex(int(from_block)),
                    "toBlock": hex(int(to_block)),
                    "address": str(address),
                    "topics": [topic0],
                }
            ]
            payload = self._rpc("eth_getLogs", params)
            out.extend(self._normalize_log(item) for item in list(payload or []) if isinstance(item, dict))
        return out

    def _load_market_by_question_id(self, question_id_hex: str) -> _MarketRef | None:
        question_id_hex = str(question_id_hex).strip().lower()
        cached = self._market_by_question_id.get(question_id_hex)
        if question_id_hex in self._market_by_question_id:
            return cached
        if not self._allow_remote_market_lookup:
            self._market_by_question_id[question_id_hex] = None
            return None
        url = f"{self._clob_base_url}/markets-by-question-id/{question_id_hex}"
        response = self._client.get(url)
        if response.status_code == 404:
            self._market_by_question_id[question_id_hex] = None
            return None
        response.raise_for_status()
        payload = response.json()
        market_payload = payload[0] if isinstance(payload, list) and payload else payload
        if not isinstance(market_payload, dict):
            self._market_by_question_id[question_id_hex] = None
            return None
        market_id = str(market_payload.get("id") or "").strip()
        condition_id = str(infer_condition_id(market_payload) or "").strip()
        if not market_id or not condition_id:
            self._market_by_question_id[question_id_hex] = None
            return None
        record = _MarketRef(market_id=market_id, condition_id=condition_id)
        self._market_by_question_id[question_id_hex] = record
        return record

    def _load_block_timestamp(self, block_number: int) -> datetime:
        cached = self._block_timestamps.get(int(block_number))
        if cached is not None:
            return cached
        payload = self._rpc("eth_getBlockByNumber", [hex(int(block_number)), False])
        timestamp = datetime.fromtimestamp(int(str(payload["timestamp"]), 16), tz=UTC).replace(tzinfo=None)
        self._block_timestamps[int(block_number)] = timestamp
        return timestamp

    def _rpc(self, method: str, params: list[Any]) -> Any:
        last_error: Exception | None = None
        for attempt in range(self._max_retries + 1):
            self._apply_rpc_pacing()
            try:
                response = httpx.post(
                    self._rpc_url,
                    headers=self._headers or None,
                    timeout=self._timeout_seconds,
                    json={"id": 1, "jsonrpc": "2.0", "method": method, "params": list(params)},
                )
                response.raise_for_status()
                payload = response.json()
                if isinstance(payload, dict) and payload.get("error") is not None:
                    raise RuntimeError(f"{method} failed: {payload['error']}")
                if not isinstance(payload, dict) or "result" not in payload:
                    raise RuntimeError(f"{method} returned invalid payload: {json.dumps(payload)[:500]}")
                return payload["result"]
            except (httpx.HTTPStatusError, httpx.TransportError) as exc:
                last_error = exc
                status_code = getattr(getattr(exc, "response", None), "status_code", None)
                retryable = isinstance(exc, httpx.TransportError) or status_code in {408, 429, 500, 502, 503, 504}
                if not retryable or attempt >= self._max_retries:
                    raise
                time.sleep(self._retry_backoff_seconds * float(attempt + 1))
        if last_error is not None:
            raise last_error
        raise RuntimeError(f"{method} failed without response")

    def _apply_rpc_pacing(self) -> None:
        if self._min_request_interval_seconds <= 0:
            self._last_rpc_started_at = time.monotonic()
            return
        now = time.monotonic()
        elapsed = now - self._last_rpc_started_at
        wait_seconds = self._min_request_interval_seconds - elapsed
        if wait_seconds > 0:
            time.sleep(wait_seconds)
        self._last_rpc_started_at = time.monotonic()

    def _normalize_log(self, raw: dict[str, Any]) -> dict[str, Any]:
        return {
            "address": Web3.to_checksum_address(str(raw["address"])),
            "topics": [Web3.to_bytes(hexstr=str(item)) for item in list(raw.get("topics") or [])],
            "data": Web3.to_bytes(hexstr=str(raw.get("data") or "0x")),
            "blockNumber": int(str(raw["blockNumber"]), 16),
            "transactionHash": Web3.to_bytes(hexstr=str(raw["transactionHash"])),
            "transactionIndex": int(str(raw.get("transactionIndex") or "0x0"), 16),
            "blockHash": Web3.to_bytes(hexstr=str(raw["blockHash"])),
            "logIndex": int(str(raw["logIndex"]), 16),
            "removed": bool(raw.get("removed", False)),
        }
