from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum

from .execution import RouteAction, TimeInForce


class OrderStatus(str, Enum):
    CREATED = "created"
    RESERVED = "reserved"
    POSTED = "posted"
    PARTIAL_FILLED = "partial_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    REJECTED = "rejected"


class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class ReservationStatus(str, Enum):
    OPEN = "open"
    PARTIALLY_CONSUMED = "partially_consumed"
    RELEASED = "released"
    CONVERTED = "converted"


class BalanceType(str, Enum):
    AVAILABLE = "available"
    RESERVED = "reserved"
    SETTLED = "settled"
    REDEEMABLE = "redeemable"


def reservation_asset_type_for_side(side: OrderSide) -> str:
    return "usdc_e" if side is OrderSide.BUY else "outcome_token"


@dataclass(frozen=True)
class Order:
    order_id: str
    client_order_id: str
    wallet_id: str
    market_id: str
    token_id: str
    outcome: str
    side: OrderSide
    price: Decimal
    size: Decimal
    route_action: RouteAction
    time_in_force: TimeInForce
    expiration: datetime | None
    fee_rate_bps: int
    signature_type: int
    funder: str
    status: OrderStatus
    filled_size: Decimal
    remaining_size: Decimal
    avg_fill_price: Decimal | None
    reservation_id: str | None
    exchange_order_id: str | None
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        if not self.order_id:
            raise ValueError("order_id is required")
        if not self.client_order_id:
            raise ValueError("client_order_id is required")
        if not self.wallet_id:
            raise ValueError("wallet_id is required")
        if not self.market_id:
            raise ValueError("market_id is required")
        if not self.token_id:
            raise ValueError("token_id is required")
        if not self.outcome:
            raise ValueError("outcome is required")
        if self.price <= 0:
            raise ValueError("price must be positive")
        if self.size <= 0:
            raise ValueError("size must be positive")
        if self.filled_size < 0 or self.remaining_size < 0:
            raise ValueError("filled_size and remaining_size must be non-negative")
        if self.filled_size + self.remaining_size > self.size:
            raise ValueError("filled_size + remaining_size cannot exceed size")
        if self.fee_rate_bps < 0 or self.signature_type < 0:
            raise ValueError("fee_rate_bps and signature_type must be non-negative")
        if not self.funder:
            raise ValueError("funder is required")


@dataclass(frozen=True)
class Fill:
    fill_id: str
    order_id: str
    wallet_id: str
    market_id: str
    token_id: str
    outcome: str
    side: OrderSide
    price: Decimal
    size: Decimal
    fee: Decimal
    fee_rate_bps: int
    trade_id: str
    exchange_order_id: str
    filled_at: datetime

    def __post_init__(self) -> None:
        if not self.fill_id:
            raise ValueError("fill_id is required")
        if not self.order_id:
            raise ValueError("order_id is required")
        if self.price <= 0 or self.size <= 0:
            raise ValueError("price and size must be positive")
        if self.fee < 0 or self.fee_rate_bps < 0:
            raise ValueError("fee and fee_rate_bps must be non-negative")
        if not self.trade_id:
            raise ValueError("trade_id is required")
        if not self.exchange_order_id:
            raise ValueError("exchange_order_id is required")


@dataclass(frozen=True)
class Reservation:
    reservation_id: str
    order_id: str
    wallet_id: str
    asset_type: str
    token_id: str | None
    market_id: str | None
    outcome: str | None
    funder: str
    signature_type: int
    reserved_quantity: Decimal
    remaining_quantity: Decimal
    reserved_notional: Decimal
    status: ReservationStatus
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        if not self.reservation_id:
            raise ValueError("reservation_id is required")
        if not self.order_id:
            raise ValueError("order_id is required")
        if not self.wallet_id:
            raise ValueError("wallet_id is required")
        if not self.asset_type:
            raise ValueError("asset_type is required")
        if self.signature_type < 0:
            raise ValueError("signature_type must be non-negative")
        if not self.funder:
            raise ValueError("funder is required")
        if self.reserved_quantity < 0 or self.remaining_quantity < 0 or self.reserved_notional < 0:
            raise ValueError("reservation quantities must be non-negative")
        if self.remaining_quantity > self.reserved_quantity:
            raise ValueError("remaining_quantity cannot exceed reserved_quantity")


@dataclass(frozen=True)
class InventoryPosition:
    wallet_id: str
    asset_type: str
    token_id: str | None
    market_id: str | None
    outcome: str | None
    balance_type: BalanceType
    quantity: Decimal
    funder: str
    signature_type: int
    updated_at: datetime

    def __post_init__(self) -> None:
        if not self.wallet_id:
            raise ValueError("wallet_id is required")
        if not self.asset_type:
            raise ValueError("asset_type is required")
        if self.quantity < 0:
            raise ValueError("quantity must be non-negative")
        if not self.funder:
            raise ValueError("funder is required")
        if self.signature_type < 0:
            raise ValueError("signature_type must be non-negative")


def inventory_position_key(position: InventoryPosition) -> tuple[str, str, str | None, str | None, str | None, BalanceType]:
    return (
        position.wallet_id,
        position.asset_type,
        position.token_id,
        position.market_id,
        position.outcome,
        position.balance_type,
    )


@dataclass(frozen=True)
class ExposureSnapshot:
    snapshot_id: str
    wallet_id: str
    funder: str
    signature_type: int
    market_id: str
    token_id: str
    outcome: str
    open_order_size: Decimal
    reserved_notional_usdc: Decimal
    filled_position_size: Decimal
    settled_position_size: Decimal
    redeemable_size: Decimal
    captured_at: datetime

    def __post_init__(self) -> None:
        if not self.snapshot_id:
            raise ValueError("snapshot_id is required")
        if not self.wallet_id:
            raise ValueError("wallet_id is required")
        if not self.market_id or not self.token_id or not self.outcome:
            raise ValueError("market_id, token_id, and outcome are required")
        if not self.funder:
            raise ValueError("funder is required")
        if self.signature_type < 0:
            raise ValueError("signature_type must be non-negative")
        numeric_fields = (
            self.open_order_size,
            self.reserved_notional_usdc,
            self.filled_position_size,
            self.settled_position_size,
            self.redeemable_size,
        )
        if any(value < 0 for value in numeric_fields):
            raise ValueError("exposure values must be non-negative")

