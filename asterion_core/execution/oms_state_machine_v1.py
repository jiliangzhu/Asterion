from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal, ROUND_DOWN

from asterion_core.contracts import Fill, Order, OrderStateTransition, OrderStatus, RouteAction, stable_object_id

_ALLOWED_TRANSITIONS: dict[OrderStatus, set[OrderStatus]] = {
    OrderStatus.CREATED: {OrderStatus.POSTED},
    OrderStatus.POSTED: {
        OrderStatus.PARTIAL_FILLED,
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
    },
    OrderStatus.PARTIAL_FILLED: {OrderStatus.FILLED},
}


def validate_order_transition(from_status: OrderStatus, to_status: OrderStatus) -> None:
    allowed = _ALLOWED_TRANSITIONS.get(from_status, set())
    if to_status not in allowed:
        raise ValueError(f"invalid OMS transition: {from_status.value} -> {to_status.value}")


def transition_order_to_posted(
    order: Order,
    *,
    timestamp: datetime | None = None,
    reason: str = "paper_adapter_posted",
) -> tuple[Order, OrderStateTransition]:
    observed_at = timestamp or datetime.now(UTC)
    validate_order_transition(order.status, OrderStatus.POSTED)
    updated_order = replace(
        order,
        status=OrderStatus.POSTED,
        updated_at=observed_at,
    )
    return updated_order, _build_transition(
        order_id=order.order_id,
        from_status=order.status,
        to_status=OrderStatus.POSTED,
        reason=reason,
        timestamp=observed_at,
    )


def apply_fills_to_order(
    order: Order,
    *,
    fills: list[Fill],
    timestamp: datetime | None = None,
) -> tuple[Order, OrderStateTransition | None]:
    observed_at = timestamp or datetime.now(UTC)
    if order.status not in {OrderStatus.POSTED, OrderStatus.PARTIAL_FILLED}:
        raise ValueError("OMS fill application expects a POSTED or PARTIAL_FILLED order")

    if not fills:
        if order.route_action is RouteAction.POST_ONLY_GTC:
            return order, None
        cancelled_order = replace(
            order,
            status=OrderStatus.CANCELLED,
            remaining_size=Decimal("0"),
            updated_at=observed_at,
        )
        validate_order_transition(order.status, OrderStatus.CANCELLED)
        return cancelled_order, _build_transition(
            order_id=order.order_id,
            from_status=order.status,
            to_status=OrderStatus.CANCELLED,
            reason="paper_no_fill",
            timestamp=observed_at,
        )

    additional_filled = _quantize_decimal(sum((fill.size for fill in fills), Decimal("0")))
    total_filled = _quantize_decimal(order.filled_size + additional_filled)
    if total_filled > order.size:
        raise ValueError("fills exceed order size")
    remaining_size = _quantize_decimal(order.size - total_filled)
    avg_fill_price = _weighted_average_fill_price(order, fills, total_filled)
    exchange_order_id = fills[-1].exchange_order_id

    if remaining_size == Decimal("0"):
        next_status = OrderStatus.FILLED
        reason = "paper_full_fill"
    else:
        next_status = OrderStatus.PARTIAL_FILLED
        reason = "paper_partial_fill"

    validate_order_transition(order.status, next_status)
    updated_order = replace(
        order,
        status=next_status,
        filled_size=total_filled,
        remaining_size=remaining_size,
        avg_fill_price=avg_fill_price,
        exchange_order_id=exchange_order_id,
        updated_at=observed_at,
    )
    return updated_order, _build_transition(
        order_id=order.order_id,
        from_status=order.status,
        to_status=next_status,
        reason=reason,
        timestamp=observed_at,
    )


def cancel_order(
    order: Order,
    *,
    reason: str,
    timestamp: datetime | None = None,
) -> tuple[Order, OrderStateTransition]:
    observed_at = timestamp or datetime.now(UTC)
    validate_order_transition(order.status, OrderStatus.CANCELLED)
    updated_order = replace(
        order,
        status=OrderStatus.CANCELLED,
        remaining_size=Decimal("0"),
        updated_at=observed_at,
    )
    return updated_order, _build_transition(
        order_id=order.order_id,
        from_status=order.status,
        to_status=OrderStatus.CANCELLED,
        reason=reason,
        timestamp=observed_at,
    )


def order_status_journal_payload(
    *,
    order: Order,
    ticket_id: str,
    request_id: str,
    reason: str,
) -> dict[str, object]:
    return {
        "order_id": order.order_id,
        "ticket_id": ticket_id,
        "request_id": request_id,
        "wallet_id": order.wallet_id,
        "status": order.status.value,
        "filled_size": str(order.filled_size),
        "remaining_size": str(order.remaining_size),
        "avg_fill_price": str(order.avg_fill_price) if order.avg_fill_price is not None else None,
        "exchange_order_id": order.exchange_order_id,
        "reason": reason,
    }


def _build_transition(
    *,
    order_id: str,
    from_status: OrderStatus,
    to_status: OrderStatus,
    reason: str | None,
    timestamp: datetime,
) -> OrderStateTransition:
    return OrderStateTransition(
        transition_id=stable_object_id(
            "otrans",
            {
                "order_id": order_id,
                "from_status": from_status.value,
                "to_status": to_status.value,
                "reason": reason,
            },
        ),
        order_id=order_id,
        from_status=from_status,
        to_status=to_status,
        reason=reason,
        timestamp=timestamp,
    )


def _weighted_average_fill_price(order: Order, fills: list[Fill], total_filled: Decimal) -> Decimal:
    if total_filled <= 0:
        raise ValueError("total_filled must be positive")
    existing_notional = Decimal("0")
    if order.avg_fill_price is not None and order.filled_size > 0:
        existing_notional = order.avg_fill_price * order.filled_size
    new_notional = sum((fill.price * fill.size for fill in fills), Decimal("0"))
    return _quantize_decimal((existing_notional + new_notional) / total_filled)


def _quantize_decimal(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
