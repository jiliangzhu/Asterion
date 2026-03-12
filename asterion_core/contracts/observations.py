from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum


class ExternalBalanceObservationKind(str, Enum):
    WALLET_BALANCE = "wallet_balance"
    TOKEN_ALLOWANCE = "token_allowance"


class ExternalFillObservationKind(str, Enum):
    SHADOW_FILL_PARTIAL = "shadow_fill_partial"
    SHADOW_FILL_FULL = "shadow_fill_full"


@dataclass(frozen=True)
class ExternalBalanceObservation:
    observation_id: str
    wallet_id: str
    funder: str
    signature_type: int
    asset_type: str
    token_id: str | None
    market_id: str | None
    outcome: str | None
    observation_kind: ExternalBalanceObservationKind
    allowance_target: str | None
    chain_id: int
    block_number: int | None
    observed_quantity: Decimal
    source: str
    observed_at: datetime
    raw_observation_json: dict[str, object]

    def __post_init__(self) -> None:
        if not self.observation_id:
            raise ValueError("observation_id is required")
        if not self.wallet_id:
            raise ValueError("wallet_id is required")
        if not self.funder:
            raise ValueError("funder is required")
        if self.signature_type < 0:
            raise ValueError("signature_type must be non-negative")
        if not self.asset_type:
            raise ValueError("asset_type is required")
        if self.chain_id <= 0:
            raise ValueError("chain_id must be positive")
        if self.block_number is not None and self.block_number < 0:
            raise ValueError("block_number must be non-negative")
        if self.observed_quantity < 0:
            raise ValueError("observed_quantity must be non-negative")
        if not self.source:
            raise ValueError("source is required")
        if self.observation_kind is ExternalBalanceObservationKind.TOKEN_ALLOWANCE and not self.allowance_target:
            raise ValueError("allowance_target is required for token_allowance observations")


@dataclass(frozen=True)
class ExternalFillObservation:
    observation_id: str
    attempt_id: str
    request_id: str
    ticket_id: str
    order_id: str | None
    wallet_id: str
    execution_context_id: str
    exchange: str
    observation_kind: ExternalFillObservationKind
    external_order_id: str | None
    external_trade_id: str
    market_id: str
    token_id: str
    outcome: str
    side: str
    price: Decimal
    size: Decimal
    fee: Decimal
    fee_rate_bps: int
    external_status: str
    observed_at: datetime
    error: str | None
    raw_observation_json: dict[str, object]

    def __post_init__(self) -> None:
        if not self.observation_id or not self.attempt_id or not self.request_id:
            raise ValueError("observation_id, attempt_id, and request_id are required")
        if not self.ticket_id or not self.wallet_id or not self.execution_context_id:
            raise ValueError("ticket_id, wallet_id, and execution_context_id are required")
        if not self.exchange or not self.external_trade_id:
            raise ValueError("exchange and external_trade_id are required")
        if not self.market_id or not self.token_id or not self.outcome or not self.side:
            raise ValueError("market_id, token_id, outcome, and side are required")
        if self.price <= 0 or self.size <= 0:
            raise ValueError("price and size must be positive")
        if self.fee < 0 or self.fee_rate_bps < 0:
            raise ValueError("fee and fee_rate_bps must be non-negative")
        if self.external_status not in {"partial_filled", "filled"}:
            raise ValueError("external_status must be partial_filled or filled")
