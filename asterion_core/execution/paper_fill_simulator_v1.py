from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal, ROUND_DOWN

from asterion_core.contracts import (
    Fill,
    Order,
    OrderStateTransition,
    OrderStatus,
    RouteAction,
    TradeTicket,
    stable_object_id,
)


@dataclass(frozen=True)
class PaperFillSimulationResult:
    updated_order: Order
    fills: list[Fill]
    transitions: list[OrderStateTransition]
    outcome_reason: str


def simulate_quote_based_fill(
    *,
    order: Order,
    ticket: TradeTicket,
    observed_at: datetime | None = None,
) -> PaperFillSimulationResult:
    timestamp = observed_at or datetime.now(UTC)
    if order.status is not OrderStatus.POSTED:
        raise ValueError("paper fill simulator expects a POSTED order")
    if ticket.ticket_id == "":
        raise ValueError("ticket_id is required")

    route_action = order.route_action
    fill_fraction = _base_fill_fraction(ticket)
    if route_action is RouteAction.POST_ONLY_GTC:
        return PaperFillSimulationResult(
            updated_order=order,
            fills=[],
            transitions=[],
            outcome_reason="post_only_rests",
        )
    if route_action is RouteAction.FOK:
        fill_fraction = Decimal("1") if fill_fraction == Decimal("1") else Decimal("0")

    fill_size = _quantize_size(order.size * fill_fraction)
    if fill_size <= 0:
        cancelled_order = replace(
            order,
            status=OrderStatus.CANCELLED,
            remaining_size=Decimal("0"),
            updated_at=timestamp,
        )
        return PaperFillSimulationResult(
            updated_order=cancelled_order,
            fills=[],
            transitions=[
                build_fill_transition(
                    order_id=order.order_id,
                    from_status=OrderStatus.POSTED,
                    to_status=OrderStatus.CANCELLED,
                    reason="paper_no_fill",
                    timestamp=timestamp,
                )
            ],
            outcome_reason="no_fill",
        )

    fill_price = order.price
    fee = _quantize_money(fill_price * fill_size * Decimal(order.fee_rate_bps) / Decimal("10000"))
    exchange_order_id = order.exchange_order_id or stable_object_id("pord", {"order_id": order.order_id})
    fill = Fill(
        fill_id=stable_object_id(
            "fill",
            {
                "order_id": order.order_id,
                "fill_price": str(fill_price),
                "fill_size": str(fill_size),
                "route_action": route_action.value,
            },
        ),
        order_id=order.order_id,
        wallet_id=order.wallet_id,
        market_id=order.market_id,
        token_id=order.token_id,
        outcome=order.outcome,
        side=order.side,
        price=fill_price,
        size=fill_size,
        fee=fee,
        fee_rate_bps=order.fee_rate_bps,
        trade_id=stable_object_id("trade", {"order_id": order.order_id, "fill_id": order.order_id, "size": str(fill_size)}),
        exchange_order_id=exchange_order_id,
        filled_at=timestamp,
    )
    remaining_size = _quantize_size(order.size - fill_size)
    if remaining_size <= 0:
        updated_order = replace(
            order,
            status=OrderStatus.FILLED,
            filled_size=order.size,
            remaining_size=Decimal("0"),
            avg_fill_price=fill_price,
            exchange_order_id=exchange_order_id,
            updated_at=timestamp,
        )
        transition = build_fill_transition(
            order_id=order.order_id,
            from_status=OrderStatus.POSTED,
            to_status=OrderStatus.FILLED,
            reason="paper_full_fill",
            timestamp=timestamp,
        )
        return PaperFillSimulationResult(
            updated_order=updated_order,
            fills=[fill],
            transitions=[transition],
            outcome_reason="full_fill",
        )

    updated_order = replace(
        order,
        status=OrderStatus.PARTIAL_FILLED,
        filled_size=fill_size,
        remaining_size=remaining_size,
        avg_fill_price=fill_price,
        exchange_order_id=exchange_order_id,
        updated_at=timestamp,
    )
    transition = build_fill_transition(
        order_id=order.order_id,
        from_status=OrderStatus.POSTED,
        to_status=OrderStatus.PARTIAL_FILLED,
        reason="paper_partial_fill",
        timestamp=timestamp,
    )
    return PaperFillSimulationResult(
        updated_order=updated_order,
        fills=[fill],
        transitions=[transition],
        outcome_reason="partial_fill",
    )


def build_fill_transition(
    *,
    order_id: str,
    from_status: OrderStatus,
    to_status: OrderStatus,
    reason: str,
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


def fill_journal_payload(*, fill: Fill, ticket_id: str, request_id: str) -> dict[str, object]:
    return {
        "fill_id": fill.fill_id,
        "order_id": fill.order_id,
        "ticket_id": ticket_id,
        "request_id": request_id,
        "wallet_id": fill.wallet_id,
        "market_id": fill.market_id,
        "token_id": fill.token_id,
        "outcome": fill.outcome,
        "side": fill.side.value,
        "price": str(fill.price),
        "size": str(fill.size),
        "fee": str(fill.fee),
        "fee_rate_bps": fill.fee_rate_bps,
        "trade_id": fill.trade_id,
        "exchange_order_id": fill.exchange_order_id,
    }


def order_fill_status_journal_payload(
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


def _base_fill_fraction(ticket: TradeTicket) -> Decimal:
    threshold = max(1, abs(int(ticket.threshold_bps)))
    edge_ratio = Decimal(abs(int(ticket.edge_bps))) / Decimal(threshold)
    if edge_ratio < Decimal("1"):
        return Decimal("0")
    if edge_ratio < Decimal("1.5"):
        return Decimal("0.5")
    return Decimal("1")


def _quantize_size(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)


def _quantize_money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
