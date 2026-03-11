from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from asterion_core.contracts import (
    CanonicalOrderContract,
    ExecutionContext,
    RouteAction,
    TimeInForce,
    TradeTicket,
    post_only_for_route_action,
    stable_object_id,
    time_in_force_for_route_action,
)


@dataclass(frozen=True)
class RoutedCanonicalOrder:
    ticket_id: str
    request_id: str
    wallet_id: str
    execution_context_id: str
    market_id: str
    token_id: str
    outcome: str
    side: str
    price: Decimal
    size: Decimal
    route_action: RouteAction
    time_in_force: TimeInForce
    expiration: datetime | None
    fee_rate_bps: int
    signature_type: int
    funder: str
    post_only: bool
    canonical_order_hash: str
    router_reason: str

    def __post_init__(self) -> None:
        if not self.ticket_id or not self.request_id:
            raise ValueError("ticket_id and request_id are required")
        if not self.wallet_id or not self.execution_context_id:
            raise ValueError("wallet_id and execution_context_id are required")
        if not self.market_id or not self.token_id or not self.outcome:
            raise ValueError("market_id, token_id, and outcome are required")
        if self.side not in {"buy", "sell"}:
            raise ValueError("side must be buy or sell")
        if self.price <= 0 or self.size <= 0:
            raise ValueError("price and size must be positive")
        if not self.canonical_order_hash or not self.router_reason:
            raise ValueError("canonical_order_hash and router_reason are required")


def route_trade_ticket(ticket: TradeTicket, execution_context: ExecutionContext) -> RoutedCanonicalOrder:
    _validate_routing_inputs(ticket, execution_context)
    if ticket.route_action is RouteAction.POST_ONLY_GTD:
        raise ValueError("POST_ONLY_GTD remains blocked in P3-03 until a canonical expiration source is implemented")

    canonical_order = CanonicalOrderContract(
        market_id=ticket.market_id,
        token_id=ticket.token_id,
        outcome=ticket.outcome,
        side=ticket.side,
        price=ticket.reference_price,
        size=ticket.size,
        route_action=ticket.route_action,
        time_in_force=time_in_force_for_route_action(ticket.route_action),
        expiration=None,
        fee_rate_bps=execution_context.fee_rate_bps,
        signature_type=execution_context.signature_type,
        funder=execution_context.funder,
    )
    post_only = post_only_for_route_action(canonical_order.route_action)
    canonical_order_hash = stable_object_id(
        "coh",
        {
            "market_id": canonical_order.market_id,
            "token_id": canonical_order.token_id,
            "outcome": canonical_order.outcome,
            "side": canonical_order.side,
            "price": str(canonical_order.price),
            "size": str(canonical_order.size),
            "route_action": canonical_order.route_action.value,
            "time_in_force": canonical_order.time_in_force.value,
            "expiration": _sql_timestamp(canonical_order.expiration),
            "fee_rate_bps": canonical_order.fee_rate_bps,
            "signature_type": canonical_order.signature_type,
            "funder": canonical_order.funder,
            "post_only": post_only,
        },
    )
    return RoutedCanonicalOrder(
        ticket_id=ticket.ticket_id,
        request_id=ticket.request_id,
        wallet_id=ticket.wallet_id or "",
        execution_context_id=ticket.execution_context_id or "",
        market_id=canonical_order.market_id,
        token_id=canonical_order.token_id,
        outcome=canonical_order.outcome,
        side=canonical_order.side,
        price=canonical_order.price,
        size=canonical_order.size,
        route_action=canonical_order.route_action,
        time_in_force=canonical_order.time_in_force,
        expiration=canonical_order.expiration,
        fee_rate_bps=canonical_order.fee_rate_bps,
        signature_type=canonical_order.signature_type,
        funder=canonical_order.funder,
        post_only=post_only,
        canonical_order_hash=canonical_order_hash,
        router_reason="route_action_normalized",
    )


def route_trade_ticket_from_handoff(con, *, ticket_id: str) -> RoutedCanonicalOrder:
    from asterion_core.execution.signal_to_order_v1 import hydrate_execution_context, load_execution_context_record
    from asterion_core.execution.trade_ticket_v1 import load_trade_ticket

    ticket = load_trade_ticket(con, ticket_id=ticket_id)
    if ticket.execution_context_id is None:
        raise ValueError("ticket.execution_context_id is required for routed handoff rebuild")
    record = load_execution_context_record(con, execution_context_id=ticket.execution_context_id)
    execution_context = hydrate_execution_context(con, record=record)
    return route_trade_ticket(ticket, execution_context)


def canonical_order_router_payload(result: RoutedCanonicalOrder) -> dict[str, Any]:
    return {
        "ticket_id": result.ticket_id,
        "request_id": result.request_id,
        "wallet_id": result.wallet_id,
        "execution_context_id": result.execution_context_id,
        "market_id": result.market_id,
        "token_id": result.token_id,
        "outcome": result.outcome,
        "side": result.side,
        "price": str(result.price),
        "size": str(result.size),
        "route_action": result.route_action.value,
        "time_in_force": result.time_in_force.value,
        "expiration": _sql_timestamp(result.expiration),
        "fee_rate_bps": result.fee_rate_bps,
        "signature_type": result.signature_type,
        "funder": result.funder,
        "post_only": result.post_only,
        "canonical_order_hash": result.canonical_order_hash,
        "router_reason": result.router_reason,
    }


def canonical_order_router_hash(result: RoutedCanonicalOrder) -> str:
    return stable_object_id("coh", _canonical_hash_payload(result))


def _canonical_hash_payload(result: RoutedCanonicalOrder) -> dict[str, object]:
    return {
        "market_id": result.market_id,
        "token_id": result.token_id,
        "outcome": result.outcome,
        "side": result.side,
        "price": str(result.price),
        "size": str(result.size),
        "route_action": result.route_action.value,
        "time_in_force": result.time_in_force.value,
        "expiration": _sql_timestamp(result.expiration),
        "fee_rate_bps": result.fee_rate_bps,
        "signature_type": result.signature_type,
        "funder": result.funder,
        "post_only": result.post_only,
    }


def _validate_routing_inputs(ticket: TradeTicket, execution_context: ExecutionContext) -> None:
    if ticket.wallet_id is None:
        raise ValueError("ticket.wallet_id is required for canonical routing")
    if ticket.execution_context_id is None:
        raise ValueError("ticket.execution_context_id is required for canonical routing")
    if ticket.route_action is not execution_context.route_action:
        raise ValueError("ticket.route_action must match execution_context.route_action")
    market_capability = execution_context.market_capability
    account_capability = execution_context.account_capability
    if ticket.market_id != market_capability.market_id:
        raise ValueError("ticket.market_id must match execution_context.market_capability.market_id")
    if ticket.token_id != market_capability.token_id:
        raise ValueError("ticket.token_id must match execution_context.market_capability.token_id")
    if ticket.outcome != market_capability.outcome:
        raise ValueError("ticket.outcome must match execution_context.market_capability.outcome")
    if ticket.wallet_id != account_capability.wallet_id:
        raise ValueError("ticket.wallet_id must match execution_context.account_capability.wallet_id")
    if ticket.reference_price <= 0:
        raise ValueError("ticket.price must be positive")
    if ticket.size <= 0:
        raise ValueError("ticket.size must be positive")
    if not _is_tick_aligned(ticket.reference_price, execution_context.tick_size):
        raise ValueError("ticket.price must align with execution_context.tick_size")
    if ticket.size < market_capability.min_order_size:
        raise ValueError("ticket.size must be >= execution_context.market_capability.min_order_size")
    if not market_capability.tradable:
        raise ValueError("execution_context.market_capability must be tradable")
    if not account_capability.can_trade:
        raise ValueError("execution_context.account_capability must be trade-enabled")


def _is_tick_aligned(value: Decimal, tick_size: Decimal) -> bool:
    quotient = value / tick_size
    return quotient == quotient.to_integral_value()


def _sql_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    normalized = value
    if normalized.tzinfo is not None:
        normalized = normalized.astimezone(UTC).replace(tzinfo=None)
    return normalized.isoformat(sep=" ", timespec="seconds")
