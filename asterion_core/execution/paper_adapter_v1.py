from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

from asterion_core.contracts import (
    GateDecision,
    Order,
    OrderStateTransition,
    OrderStatus,
    SignalOrderIntent,
    stable_object_id,
)
from asterion_core.execution.signal_to_order_v1 import build_order_from_intent


def build_paper_order(
    *,
    intent: SignalOrderIntent,
    wallet_id: str,
    gate_decision: GateDecision,
    created_at: datetime | None = None,
) -> Order:
    if not gate_decision.allowed:
        raise ValueError("paper adapter requires an allowed gate_decision")
    timestamp = created_at or datetime.now(UTC)
    base_order = build_order_from_intent(
        intent,
        wallet_id=wallet_id,
        created_at=timestamp,
        status=OrderStatus.CREATED,
    )
    return replace(
        base_order,
        status=OrderStatus.POSTED,
        updated_at=timestamp,
    )


def build_order_state_transition(
    *,
    order: Order,
    from_status: OrderStatus,
    to_status: OrderStatus,
    reason: str | None,
    timestamp: datetime | None = None,
) -> OrderStateTransition:
    observed_at = timestamp or order.updated_at
    return OrderStateTransition(
        transition_id=stable_object_id(
            "otrans",
            {
                "order_id": order.order_id,
                "from_status": from_status.value,
                "to_status": to_status.value,
                "reason": reason,
            },
        ),
        order_id=order.order_id,
        from_status=from_status,
        to_status=to_status,
        reason=reason,
        timestamp=observed_at,
    )


def paper_order_journal_payload(*, order: Order, ticket_id: str, request_id: str) -> dict[str, object]:
    return paper_order_journal_payload_with_status(
        order=order,
        ticket_id=ticket_id,
        request_id=request_id,
        status=order.status,
    )


def paper_order_journal_payload_with_status(
    *,
    order: Order,
    ticket_id: str,
    request_id: str,
    status: OrderStatus,
) -> dict[str, object]:
    return {
        "order_id": order.order_id,
        "client_order_id": order.client_order_id,
        "ticket_id": ticket_id,
        "request_id": request_id,
        "wallet_id": order.wallet_id,
        "market_id": order.market_id,
        "token_id": order.token_id,
        "outcome": order.outcome,
        "side": order.side.value,
        "price": str(order.price),
        "size": str(order.size),
        "route_action": order.route_action.value,
        "time_in_force": order.time_in_force.value,
        "expiration": _sql_timestamp(order.expiration),
        "fee_rate_bps": order.fee_rate_bps,
        "signature_type": order.signature_type,
        "funder": order.funder,
        "status": status.value,
        "reservation_id": order.reservation_id,
        "exchange_order_id": order.exchange_order_id,
        "adapter_kind": "paper",
    }


def gate_rejection_journal_payload(
    *,
    ticket_id: str,
    request_id: str,
    wallet_id: str,
    gate_decision: GateDecision,
) -> dict[str, object]:
    return {
        "ticket_id": ticket_id,
        "request_id": request_id,
        "wallet_id": wallet_id,
        "gate_id": gate_decision.gate_id,
        "reason": gate_decision.reason,
        "reason_codes": list(gate_decision.reason_codes),
        "metrics": dict(gate_decision.metrics_json),
    }


def _sql_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    normalized = value
    if normalized.tzinfo is not None:
        normalized = normalized.astimezone(UTC).replace(tzinfo=None)
    return normalized.isoformat(sep=" ", timespec="seconds")
