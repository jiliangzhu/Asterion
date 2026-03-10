from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from asterion_core.contracts import (
    BalanceType,
    ExposureSnapshot,
    Fill,
    InventoryPosition,
    Order,
    OrderSide,
    OrderStatus,
    Reservation,
    ReservationStatus,
    inventory_position_key,
    stable_object_id,
)

_CASH_TOKEN_ID = "usdc_e"
_CASH_MARKET_ID = "cash"
_CASH_OUTCOME = "cash"


def build_reservation(
    order: Order,
    *,
    created_at: datetime | None = None,
) -> Reservation:
    timestamp = created_at or datetime.now(UTC)
    asset_type, token_id, market_id, outcome, reserved_quantity, reserved_notional = reservation_requirements_for_order(order)
    return Reservation(
        reservation_id=stable_object_id("res", {"client_order_id": order.client_order_id, "order_id": order.order_id}),
        order_id=order.order_id,
        wallet_id=order.wallet_id,
        asset_type=asset_type,
        token_id=token_id,
        market_id=market_id,
        outcome=outcome,
        funder=order.funder,
        signature_type=order.signature_type,
        reserved_quantity=reserved_quantity,
        remaining_quantity=reserved_quantity,
        reserved_notional=reserved_notional,
        status=ReservationStatus.OPEN,
        created_at=timestamp,
        updated_at=timestamp,
    )


def reservation_requirements_for_order(
    order: Order,
) -> tuple[str, str | None, str | None, str | None, Decimal, Decimal]:
    side = _side_value(order.side)
    if side == "buy":
        reserved_quantity = order.price * order.size
        return ("usdc_e", None, None, None, reserved_quantity, reserved_quantity)
    return ("outcome_token", order.token_id, order.market_id, order.outcome, order.size, order.price * order.size)


def apply_reservation_to_inventory(
    positions: list[InventoryPosition],
    reservation: Reservation,
    *,
    observed_at: datetime | None = None,
) -> list[InventoryPosition]:
    timestamp = observed_at or datetime.now(UTC)
    positions_by_key = {inventory_position_key(item): item for item in positions}
    positions_by_key = {_normalize_inventory_key(key): value for key, value in positions_by_key.items()}
    available_key = _position_key_for_reservation(reservation, BalanceType.AVAILABLE)
    reserved_key = _position_key_for_reservation(reservation, BalanceType.RESERVED)
    current_available = positions_by_key.get(
        available_key,
        InventoryPosition(
            wallet_id=reservation.wallet_id,
            asset_type=reservation.asset_type,
            token_id=available_key[2],
            market_id=available_key[3],
            outcome=available_key[4],
            balance_type=BalanceType.AVAILABLE,
            quantity=Decimal("0"),
            funder=reservation.funder,
            signature_type=reservation.signature_type,
            updated_at=timestamp,
        ),
    )
    current_reserved = positions_by_key.get(
        reserved_key,
        InventoryPosition(
            wallet_id=reservation.wallet_id,
            asset_type=reservation.asset_type,
            token_id=reserved_key[2],
            market_id=reserved_key[3],
            outcome=reserved_key[4],
            balance_type=BalanceType.RESERVED,
            quantity=Decimal("0"),
            funder=reservation.funder,
            signature_type=reservation.signature_type,
            updated_at=timestamp,
        ),
    )
    if current_available.quantity < reservation.reserved_quantity:
        raise ValueError("insufficient available inventory for reservation")
    positions_by_key[available_key] = InventoryPosition(
        wallet_id=current_available.wallet_id,
        asset_type=current_available.asset_type,
        token_id=available_key[2],
        market_id=available_key[3],
        outcome=available_key[4],
        balance_type=current_available.balance_type,
        quantity=current_available.quantity - reservation.reserved_quantity,
        funder=current_available.funder,
        signature_type=current_available.signature_type,
        updated_at=timestamp,
    )
    positions_by_key[reserved_key] = InventoryPosition(
        wallet_id=current_reserved.wallet_id,
        asset_type=current_reserved.asset_type,
        token_id=reserved_key[2],
        market_id=reserved_key[3],
        outcome=reserved_key[4],
        balance_type=current_reserved.balance_type,
        quantity=current_reserved.quantity + reservation.reserved_quantity,
        funder=current_reserved.funder,
        signature_type=current_reserved.signature_type,
        updated_at=timestamp,
    )
    return list(positions_by_key.values())


def apply_fill_to_reservation(
    reservation: Reservation,
    fill: Fill,
    *,
    observed_at: datetime | None = None,
) -> Reservation:
    consumed = fill.price * fill.size if reservation.asset_type == "usdc_e" else fill.size
    remaining = max(Decimal("0"), reservation.remaining_quantity - consumed)
    status = ReservationStatus.PARTIALLY_CONSUMED if remaining > 0 else ReservationStatus.CONVERTED
    return Reservation(
        reservation_id=reservation.reservation_id,
        order_id=reservation.order_id,
        wallet_id=reservation.wallet_id,
        asset_type=reservation.asset_type,
        token_id=reservation.token_id,
        market_id=reservation.market_id,
        outcome=reservation.outcome,
        funder=reservation.funder,
        signature_type=reservation.signature_type,
        reserved_quantity=reservation.reserved_quantity,
        remaining_quantity=remaining,
        reserved_notional=reservation.reserved_notional,
        status=status,
        created_at=reservation.created_at,
        updated_at=observed_at or datetime.now(UTC),
    )


def finalize_reservation(
    reservation: Reservation,
    *,
    order_status: OrderStatus,
    observed_at: datetime | None = None,
) -> Reservation:
    if order_status in {OrderStatus.CANCELLED, OrderStatus.EXPIRED, OrderStatus.REJECTED}:
        status = ReservationStatus.RELEASED
    elif order_status is OrderStatus.FILLED:
        status = ReservationStatus.CONVERTED
    else:
        status = reservation.status
    remaining = Decimal("0") if status in {ReservationStatus.RELEASED, ReservationStatus.CONVERTED} else reservation.remaining_quantity
    return Reservation(
        reservation_id=reservation.reservation_id,
        order_id=reservation.order_id,
        wallet_id=reservation.wallet_id,
        asset_type=reservation.asset_type,
        token_id=reservation.token_id,
        market_id=reservation.market_id,
        outcome=reservation.outcome,
        funder=reservation.funder,
        signature_type=reservation.signature_type,
        reserved_quantity=reservation.reserved_quantity,
        remaining_quantity=remaining,
        reserved_notional=reservation.reserved_notional,
        status=status,
        created_at=reservation.created_at,
        updated_at=observed_at or datetime.now(UTC),
    )


def build_exposure_snapshot(
    order: Order,
    *,
    positions: list[InventoryPosition],
    reservation: Reservation | None = None,
    captured_at: datetime | None = None,
) -> ExposureSnapshot:
    timestamp = captured_at or datetime.now(UTC)
    positions_by_key = {inventory_position_key(item): item for item in positions}
    available_token = positions_by_key.get(
        (
            order.wallet_id,
            "outcome_token",
            order.token_id,
            order.market_id,
            order.outcome,
            BalanceType.AVAILABLE,
        )
    )
    settled_token = positions_by_key.get(
        (
            order.wallet_id,
            "outcome_token",
            order.token_id,
            order.market_id,
            order.outcome,
            BalanceType.SETTLED,
        )
    )
    redeemable_token = positions_by_key.get(
        (
            order.wallet_id,
            "outcome_token",
            order.token_id,
            order.market_id,
            order.outcome,
            BalanceType.REDEEMABLE,
        )
    )
    reserved_notional = Decimal("0")
    if reservation is not None:
        reserved_notional = (
            reservation.remaining_quantity
            if reservation.asset_type == "usdc_e"
            else reservation.remaining_quantity * order.price
        )
    open_statuses = {
        OrderStatus.CREATED,
        OrderStatus.RESERVED,
        OrderStatus.POSTED,
        OrderStatus.PARTIAL_FILLED,
    }
    return ExposureSnapshot(
        snapshot_id=stable_object_id(
            "expo",
            {
                "market_id": order.market_id,
                "order_id": order.order_id,
                "status": str(order.status),
                "token_id": order.token_id,
                "updated_at": timestamp.isoformat(),
            },
        ),
        wallet_id=order.wallet_id,
        funder=order.funder,
        signature_type=order.signature_type,
        market_id=order.market_id,
        token_id=order.token_id,
        outcome=order.outcome,
        open_order_size=order.remaining_size if order.status in open_statuses else Decimal("0"),
        reserved_notional_usdc=reserved_notional,
        filled_position_size=available_token.quantity if available_token is not None else Decimal("0"),
        settled_position_size=settled_token.quantity if settled_token is not None else Decimal("0"),
        redeemable_size=redeemable_token.quantity if redeemable_token is not None else Decimal("0"),
        captured_at=timestamp,
    )


def _position_key_for_reservation(
    reservation: Reservation,
    balance_type: BalanceType,
) -> tuple[str, str, str | None, str | None, str | None, BalanceType]:
    return (
        reservation.wallet_id,
        reservation.asset_type,
        *_normalize_inventory_dimensions(
            asset_type=reservation.asset_type,
            token_id=reservation.token_id,
            market_id=reservation.market_id,
            outcome=reservation.outcome,
        ),
        balance_type,
    )


def _side_value(value: object) -> str:
    if isinstance(value, OrderSide):
        return value.value
    return str(value).lower()


def _normalize_inventory_key(
    key: tuple[str, str, str | None, str | None, str | None, BalanceType],
) -> tuple[str, str, str | None, str | None, str | None, BalanceType]:
    return (
        key[0],
        key[1],
        *_normalize_inventory_dimensions(asset_type=key[1], token_id=key[2], market_id=key[3], outcome=key[4]),
        key[5],
    )


def _normalize_inventory_dimensions(
    *,
    asset_type: str,
    token_id: str | None,
    market_id: str | None,
    outcome: str | None,
) -> tuple[str | None, str | None, str | None]:
    if asset_type == "usdc_e":
        return (
            token_id or _CASH_TOKEN_ID,
            market_id or _CASH_MARKET_ID,
            outcome or _CASH_OUTCOME,
        )
    return (token_id, market_id, outcome)
