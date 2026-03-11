from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum


class ExternalBalanceObservationKind(str, Enum):
    WALLET_BALANCE = "wallet_balance"
    TOKEN_ALLOWANCE = "token_allowance"


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
