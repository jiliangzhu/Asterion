from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from asterion_core.contracts import GateDecision, SignalOrderIntent, TradeTicket, stable_object_id


def evaluate_execution_gate(
    *,
    ticket: TradeTicket,
    intent: SignalOrderIntent,
    watch_only_active: bool,
    degrade_active: bool,
    available_quantity: Decimal,
    min_edge_bps: int | None = None,
    created_at: datetime | None = None,
) -> GateDecision:
    reason_codes: list[str] = []
    metrics = {
        "account_gate": "pass",
        "economic_gate": "pass",
        "inventory_gate": "pass",
        "market_gate": "pass",
        "watch_only_gate": "pass",
    }

    if watch_only_active:
        reason_codes.append("watch_only_active")
        metrics["watch_only_gate"] = "fail"
    if degrade_active:
        reason_codes.append("degrade_active")
        metrics["watch_only_gate"] = "fail"

    market_capability = intent.execution_context.market_capability
    account_capability = intent.execution_context.account_capability
    if not market_capability.tradable:
        reason_codes.append("market_not_tradable")
        metrics["market_gate"] = "fail"
    if ticket.size < market_capability.min_order_size:
        reason_codes.append("below_min_order_size")
        metrics["market_gate"] = "fail"

    if not account_capability.can_trade:
        reason_codes.append("account_cannot_trade")
        metrics["account_gate"] = "fail"
    if account_capability.restricted_reason:
        metrics["restricted_reason"] = account_capability.restricted_reason

    required_quantity = reservation_required_quantity(ticket)
    metrics["available_quantity"] = str(available_quantity)
    metrics["required_quantity"] = str(required_quantity)
    if available_quantity < required_quantity:
        reason_codes.append("insufficient_inventory")
        metrics["inventory_gate"] = "fail"

    min_required_edge = int(ticket.threshold_bps if min_edge_bps is None else min_edge_bps)
    metrics["min_required_edge_bps"] = min_required_edge
    metrics["edge_bps"] = int(ticket.edge_bps)
    if abs(int(ticket.edge_bps)) < min_required_edge:
        reason_codes.append("economic_edge_below_threshold")
        metrics["economic_gate"] = "fail"

    allowed = not reason_codes
    reason = "allowed" if allowed else ",".join(sorted(set(reason_codes)))
    return GateDecision(
        gate_id=stable_object_id(
            "gate",
            {
                "allowed": allowed,
                "reason_codes": sorted(set(reason_codes)),
                "ticket_id": ticket.ticket_id,
            },
        ),
        ticket_id=ticket.ticket_id,
        allowed=allowed,
        reason=reason,
        reason_codes=sorted(set(reason_codes)),
        metrics_json=metrics,
        created_at=created_at or datetime.now(UTC),
    )


def reservation_required_quantity(ticket: TradeTicket) -> Decimal:
    if ticket.side == "buy":
        return ticket.reference_price * ticket.size
    return ticket.size
