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
    TradeTicket,
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


def apply_fill_to_inventory(
    positions: list[InventoryPosition],
    *,
    order: Order,
    reservation: Reservation,
    fill: Fill,
    observed_at: datetime | None = None,
) -> list[InventoryPosition]:
    timestamp = observed_at or datetime.now(UTC)
    positions_by_key = _positions_by_key(positions)

    reserved_key = _position_key_for_reservation(reservation, BalanceType.RESERVED)
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
    consumed = fill.price * fill.size if reservation.asset_type == "usdc_e" else fill.size
    if current_reserved.quantity < consumed:
        raise ValueError("insufficient reserved inventory for fill")
    positions_by_key[reserved_key] = InventoryPosition(
        wallet_id=current_reserved.wallet_id,
        asset_type=current_reserved.asset_type,
        token_id=reserved_key[2],
        market_id=reserved_key[3],
        outcome=reserved_key[4],
        balance_type=BalanceType.RESERVED,
        quantity=current_reserved.quantity - consumed,
        funder=current_reserved.funder,
        signature_type=current_reserved.signature_type,
        updated_at=timestamp,
    )

    settled_key, settled_quantity = _settled_position_for_fill(order=order, fill=fill)
    current_settled = positions_by_key.get(
        settled_key,
        InventoryPosition(
            wallet_id=settled_key[0],
            asset_type=settled_key[1],
            token_id=settled_key[2],
            market_id=settled_key[3],
            outcome=settled_key[4],
            balance_type=settled_key[5],
            quantity=Decimal("0"),
            funder=order.funder,
            signature_type=order.signature_type,
            updated_at=timestamp,
        ),
    )
    positions_by_key[settled_key] = InventoryPosition(
        wallet_id=current_settled.wallet_id,
        asset_type=current_settled.asset_type,
        token_id=settled_key[2],
        market_id=settled_key[3],
        outcome=settled_key[4],
        balance_type=BalanceType.SETTLED,
        quantity=current_settled.quantity + settled_quantity,
        funder=current_settled.funder,
        signature_type=current_settled.signature_type,
        updated_at=timestamp,
    )
    return list(positions_by_key.values())


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


def release_reservation_to_inventory(
    positions: list[InventoryPosition],
    reservation: Reservation,
    *,
    observed_at: datetime | None = None,
) -> list[InventoryPosition]:
    timestamp = observed_at or datetime.now(UTC)
    positions_by_key = _positions_by_key(positions)
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
    if current_reserved.quantity < reservation.remaining_quantity:
        raise ValueError("insufficient reserved inventory for release")
    positions_by_key[reserved_key] = InventoryPosition(
        wallet_id=current_reserved.wallet_id,
        asset_type=current_reserved.asset_type,
        token_id=reserved_key[2],
        market_id=reserved_key[3],
        outcome=reserved_key[4],
        balance_type=BalanceType.RESERVED,
        quantity=current_reserved.quantity - reservation.remaining_quantity,
        funder=current_reserved.funder,
        signature_type=current_reserved.signature_type,
        updated_at=timestamp,
    )
    positions_by_key[available_key] = InventoryPosition(
        wallet_id=current_available.wallet_id,
        asset_type=current_available.asset_type,
        token_id=available_key[2],
        market_id=available_key[3],
        outcome=available_key[4],
        balance_type=BalanceType.AVAILABLE,
        quantity=current_available.quantity + reservation.remaining_quantity,
        funder=current_available.funder,
        signature_type=current_available.signature_type,
        updated_at=timestamp,
    )
    return list(positions_by_key.values())


def available_inventory_quantity_for_ticket(
    positions: list[InventoryPosition],
    *,
    ticket: TradeTicket,
) -> Decimal:
    positions_by_key = _positions_by_key(positions)
    side = str(ticket.side).lower()
    if side == "buy":
        key = _normalize_inventory_key(
            (
                ticket.wallet_id or "",
                "usdc_e",
                _CASH_TOKEN_ID,
                _CASH_MARKET_ID,
                _CASH_OUTCOME,
                BalanceType.AVAILABLE,
            )
        )
    else:
        key = _normalize_inventory_key(
            (
                ticket.wallet_id or "",
                "outcome_token",
                ticket.token_id,
                ticket.market_id,
                ticket.outcome,
                BalanceType.AVAILABLE,
            )
        )
    position = positions_by_key.get(key)
    if position is None:
        return Decimal("0")
    return position.quantity


def load_inventory_positions(con, *, wallet_id: str) -> list[InventoryPosition]:
    rows = con.execute(
        """
        SELECT
            wallet_id,
            asset_type,
            token_id,
            market_id,
            outcome,
            balance_type,
            quantity,
            funder,
            signature_type,
            updated_at
        FROM trading.inventory_positions
        WHERE wallet_id = ?
        """,
        [wallet_id],
    ).fetchall()
    return [
        InventoryPosition(
            wallet_id=str(row[0]),
            asset_type=str(row[1]),
            token_id=str(row[2]) if row[2] is not None else None,
            market_id=str(row[3]) if row[3] is not None else None,
            outcome=str(row[4]) if row[4] is not None else None,
            balance_type=BalanceType(str(row[5])),
            quantity=Decimal(str(row[6])),
            funder=str(row[7]),
            signature_type=int(row[8]),
            updated_at=row[9],
        )
        for row in rows
    ]


def load_reservation_for_order(con, *, order_id: str) -> Reservation | None:
    row = con.execute(
        """
        SELECT
            reservation_id,
            order_id,
            wallet_id,
            asset_type,
            token_id,
            market_id,
            outcome,
            funder,
            signature_type,
            reserved_quantity,
            remaining_quantity,
            reserved_notional,
            status,
            created_at,
            updated_at
        FROM trading.reservations
        WHERE order_id = ?
        """,
        [order_id],
    ).fetchone()
    if row is None:
        return None
    return Reservation(
        reservation_id=str(row[0]),
        order_id=str(row[1]),
        wallet_id=str(row[2]),
        asset_type=str(row[3]),
        token_id=str(row[4]) if row[4] is not None else None,
        market_id=str(row[5]) if row[5] is not None else None,
        outcome=str(row[6]) if row[6] is not None else None,
        funder=str(row[7]),
        signature_type=int(row[8]),
        reserved_quantity=Decimal(str(row[9])),
        remaining_quantity=Decimal(str(row[10])),
        reserved_notional=Decimal(str(row[11])),
        status=ReservationStatus(str(row[12])),
        created_at=row[13],
        updated_at=row[14],
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
    open_order_size = order.remaining_size if order.status in open_statuses else Decimal("0")
    filled_position_size = available_token.quantity if available_token is not None else Decimal("0")
    settled_position_size = settled_token.quantity if settled_token is not None else Decimal("0")
    redeemable_size = redeemable_token.quantity if redeemable_token is not None else Decimal("0")
    return ExposureSnapshot(
        snapshot_id=stable_object_id(
            "expo",
            {
                "order_id": order.order_id,
            },
        ),
        wallet_id=order.wallet_id,
        funder=order.funder,
        signature_type=order.signature_type,
        market_id=order.market_id,
        token_id=order.token_id,
        outcome=order.outcome,
        open_order_size=open_order_size,
        reserved_notional_usdc=reserved_notional,
        filled_position_size=filled_position_size,
        settled_position_size=settled_position_size,
        redeemable_size=redeemable_size,
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


def _positions_by_key(
    positions: list[InventoryPosition],
) -> dict[tuple[str, str, str | None, str | None, str | None, BalanceType], InventoryPosition]:
    raw = {inventory_position_key(item): item for item in positions}
    return {_normalize_inventory_key(key): value for key, value in raw.items()}


def _settled_position_for_fill(
    *,
    order: Order,
    fill: Fill,
) -> tuple[tuple[str, str, str | None, str | None, str | None, BalanceType], Decimal]:
    side = _side_value(order.side)
    if side == "buy":
        return (
            _normalize_inventory_key(
                (
                    order.wallet_id,
                    "outcome_token",
                    order.token_id,
                    order.market_id,
                    order.outcome,
                    BalanceType.SETTLED,
                )
            ),
            fill.size,
        )
    net_proceeds = max(Decimal("0"), (fill.price * fill.size) - fill.fee)
    return (
        _normalize_inventory_key(
            (
                order.wallet_id,
                "usdc_e",
                _CASH_TOKEN_ID,
                _CASH_MARKET_ID,
                _CASH_OUTCOME,
                BalanceType.SETTLED,
            )
        ),
        net_proceeds,
    )
