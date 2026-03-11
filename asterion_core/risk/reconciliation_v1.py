from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal, ROUND_DOWN

from asterion_core.contracts import (
    BalanceType,
    ExposureSnapshot,
    Fill,
    InventoryPosition,
    Order,
    ReconciliationResult,
    ReconciliationStatus,
    Reservation,
    stable_object_id,
)

_CASH_TOKEN_ID = "usdc_e"
_CASH_MARKET_ID = "cash"


def build_reconciliation_result(
    *,
    order: Order,
    reservation: Reservation,
    fills: list[Fill],
    positions: list[InventoryPosition],
    exposure_snapshot: ExposureSnapshot,
    created_at: datetime | None = None,
) -> ReconciliationResult:
    local_quantity, remote_quantity, asset_type, token_id, market_id, balance_type = _reconciliation_bucket(
        order=order,
        fills=fills,
        positions=positions,
    )
    status = classify_reconciliation_status(
        order=order,
        reservation=reservation,
        fills=fills,
        positions=positions,
        exposure_snapshot=exposure_snapshot,
        local_quantity=local_quantity,
        remote_quantity=remote_quantity,
    )
    discrepancy = _q(abs(local_quantity - remote_quantity))
    return ReconciliationResult(
        reconciliation_id=stable_object_id("recon", {"order_id": order.order_id}),
        wallet_id=order.wallet_id,
        funder=order.funder,
        signature_type=order.signature_type,
        asset_type=asset_type,
        token_id=token_id,
        market_id=market_id,
        balance_type=balance_type,
        local_quantity=local_quantity,
        remote_quantity=remote_quantity,
        discrepancy=discrepancy,
        status=status,
        resolution="paper_local_match" if status is ReconciliationStatus.OK else "manual_review_required",
        created_at=created_at or datetime.now(UTC),
    )


def reconciliation_journal_payload(
    *,
    result: ReconciliationResult,
    order: Order,
    ticket_id: str,
    request_id: str,
) -> dict[str, object]:
    return {
        "reconciliation_id": result.reconciliation_id,
        "order_id": order.order_id,
        "ticket_id": ticket_id,
        "request_id": request_id,
        "wallet_id": result.wallet_id,
        "asset_type": result.asset_type,
        "token_id": result.token_id,
        "market_id": result.market_id,
        "balance_type": result.balance_type.value,
        "local_quantity": _fmt(result.local_quantity),
        "remote_quantity": _fmt(result.remote_quantity),
        "discrepancy": _fmt(result.discrepancy),
        "status": result.status.value,
        "resolution": result.resolution,
    }


def classify_reconciliation_status(
    *,
    order: Order,
    reservation: Reservation,
    fills: list[Fill],
    positions: list[InventoryPosition],
    exposure_snapshot: ExposureSnapshot,
    local_quantity: Decimal,
    remote_quantity: Decimal,
) -> ReconciliationStatus:
    fill_total = _q(sum((fill.size for fill in fills), Decimal("0")))
    if fill_total != _q(order.filled_size):
        return ReconciliationStatus.FILL_MISMATCH
    if _expected_reservation_remaining(order=order) != _q(reservation.remaining_quantity):
        return ReconciliationStatus.RESERVATION_MISMATCH
    if local_quantity != remote_quantity:
        return ReconciliationStatus.INVENTORY_MISMATCH
    if not _exposure_matches(order=order, reservation=reservation, positions=positions, exposure_snapshot=exposure_snapshot):
        return ReconciliationStatus.EXPOSURE_MISMATCH
    return ReconciliationStatus.OK


def _reconciliation_bucket(
    *,
    order: Order,
    fills: list[Fill],
    positions: list[InventoryPosition],
) -> tuple[Decimal, Decimal, str, str | None, str | None, BalanceType]:
    side = order.side.value
    if side == "buy":
        asset_type = "outcome_token"
        token_id = order.token_id
        market_id = order.market_id
        balance_type = BalanceType.SETTLED
        remote_quantity = _q(sum((fill.size for fill in fills), Decimal("0")))
    else:
        asset_type = "usdc_e"
        token_id = _CASH_TOKEN_ID
        market_id = _CASH_MARKET_ID
        balance_type = BalanceType.SETTLED
        remote_quantity = _q(sum(((fill.price * fill.size) - fill.fee for fill in fills), Decimal("0")))
    local_quantity = _position_quantity(
        positions=positions,
        wallet_id=order.wallet_id,
        asset_type=asset_type,
        token_id=token_id,
        market_id=market_id,
        outcome=order.outcome if asset_type == "outcome_token" else "cash",
        balance_type=balance_type,
    )
    return local_quantity, remote_quantity, asset_type, token_id, market_id, balance_type


def _expected_reservation_remaining(*, order: Order) -> Decimal:
    if order.status.value in {"cancelled", "expired", "rejected", "filled"}:
        return Decimal("0")
    if order.side.value == "buy":
        return _q(order.remaining_size * order.price)
    return _q(order.remaining_size)


def _exposure_matches(
    *,
    order: Order,
    reservation: Reservation,
    positions: list[InventoryPosition],
    exposure_snapshot: ExposureSnapshot,
) -> bool:
    expected_reserved_notional = _expected_reservation_remaining(order=order)
    expected_open_order_size = _q(order.remaining_size if order.status.value in {"created", "reserved", "posted", "partial_filled"} else Decimal("0"))
    expected_filled_position = _position_quantity(
        positions=positions,
        wallet_id=order.wallet_id,
        asset_type="outcome_token",
        token_id=order.token_id,
        market_id=order.market_id,
        outcome=order.outcome,
        balance_type=BalanceType.AVAILABLE,
    )
    expected_settled_position = _position_quantity(
        positions=positions,
        wallet_id=order.wallet_id,
        asset_type="outcome_token" if order.side.value == "buy" else "usdc_e",
        token_id=order.token_id if order.side.value == "buy" else _CASH_TOKEN_ID,
        market_id=order.market_id if order.side.value == "buy" else _CASH_MARKET_ID,
        outcome=order.outcome if order.side.value == "buy" else "cash",
        balance_type=BalanceType.SETTLED,
    )
    return (
        _q(exposure_snapshot.open_order_size) == expected_open_order_size
        and _q(exposure_snapshot.reserved_notional_usdc) == expected_reserved_notional
        and _q(exposure_snapshot.filled_position_size) == expected_filled_position
        and _q(exposure_snapshot.settled_position_size) == expected_settled_position
    )


def _position_quantity(
    *,
    positions: list[InventoryPosition],
    wallet_id: str,
    asset_type: str,
    token_id: str | None,
    market_id: str | None,
    outcome: str | None,
    balance_type: BalanceType,
) -> Decimal:
    for item in positions:
        if (
            item.wallet_id == wallet_id
            and item.asset_type == asset_type
            and item.token_id == token_id
            and item.market_id == market_id
            and item.outcome == outcome
            and item.balance_type is balance_type
        ):
            return _q(item.quantity)
    return Decimal("0")


def _q(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)


def _fmt(value: Decimal) -> str:
    return format(_q(value), ".8f")
