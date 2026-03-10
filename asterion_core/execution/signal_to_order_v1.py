from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

from asterion_core.contracts import (
    AccountTradingCapability,
    CanonicalOrderContract,
    ExecutionContext,
    MarketCapability,
    Order,
    OrderSide,
    OrderStatus,
    SignalOrderIntent,
    TimeInForce,
    TradeTicket,
    stable_object_id,
    time_in_force_for_route_action,
)


def load_market_capability(con, *, token_id: str) -> MarketCapability:
    row = con.execute(
        """
        SELECT
            market_id,
            condition_id,
            token_id,
            outcome,
            tick_size,
            fee_rate_bps,
            neg_risk,
            min_order_size,
            tradable,
            fees_enabled,
            data_sources,
            updated_at
        FROM capability.market_capabilities
        WHERE token_id = ?
        """,
        [token_id],
    ).fetchone()
    if row is None:
        raise LookupError(f"market capability not found for token_id={token_id}")
    return MarketCapability(
        market_id=str(row[0]),
        condition_id=str(row[1]),
        token_id=str(row[2]),
        outcome=str(row[3]),
        tick_size=Decimal(str(row[4])),
        fee_rate_bps=int(row[5]),
        neg_risk=bool(row[6]),
        min_order_size=Decimal(str(row[7])),
        tradable=bool(row[8]),
        fees_enabled=bool(row[9]),
        data_sources=_json_list(row[10]),
        updated_at=row[11],
    )


def load_account_trading_capability(con, *, wallet_id: str) -> AccountTradingCapability:
    row = con.execute(
        """
        SELECT
            wallet_id,
            wallet_type,
            signature_type,
            funder,
            allowance_targets,
            can_use_relayer,
            can_trade,
            restricted_reason
        FROM capability.account_trading_capabilities
        WHERE wallet_id = ?
        """,
        [wallet_id],
    ).fetchone()
    if row is None:
        raise LookupError(f"account trading capability not found for wallet_id={wallet_id}")
    return AccountTradingCapability(
        wallet_id=str(row[0]),
        wallet_type=str(row[1]),
        signature_type=int(row[2]),
        funder=str(row[3]),
        allowance_targets=_json_list(row[4]),
        can_use_relayer=bool(row[5]),
        can_trade=bool(row[6]),
        restricted_reason=str(row[7]) if row[7] is not None else None,
    )


def build_execution_context(
    *,
    market_capability: MarketCapability,
    account_capability: AccountTradingCapability,
    route_action,
    risk_gate_result: str = "pending_gate",
) -> ExecutionContext:
    return ExecutionContext(
        market_capability=market_capability,
        account_capability=account_capability,
        token_id=market_capability.token_id,
        route_action=route_action,
        fee_rate_bps=market_capability.fee_rate_bps,
        tick_size=market_capability.tick_size,
        signature_type=account_capability.signature_type,
        funder=account_capability.funder,
        risk_gate_result=risk_gate_result,
    )


def build_signal_order_intent(
    ticket: TradeTicket,
    *,
    market_capability: MarketCapability,
    account_capability: AccountTradingCapability,
    expiration: datetime | None = None,
    risk_gate_result: str = "pending_gate",
) -> SignalOrderIntent:
    execution_context = build_execution_context(
        market_capability=market_capability,
        account_capability=account_capability,
        route_action=ticket.route_action,
        risk_gate_result=risk_gate_result,
    )
    time_in_force = time_in_force_for_route_action(ticket.route_action)
    if ticket.route_action.name == "POST_ONLY_GTD" and expiration is None:
        raise ValueError("expiration is required for POST_ONLY_GTD intents")
    canonical_order = CanonicalOrderContract(
        market_id=ticket.market_id,
        token_id=ticket.token_id,
        outcome=ticket.outcome,
        side=ticket.side,
        price=ticket.reference_price,
        size=ticket.size,
        route_action=ticket.route_action,
        time_in_force=TimeInForce(time_in_force.value),
        expiration=expiration,
        fee_rate_bps=execution_context.fee_rate_bps,
        signature_type=execution_context.signature_type,
        funder=execution_context.funder,
    )
    return SignalOrderIntent(
        ticket_id=ticket.ticket_id,
        request_id=ticket.request_id,
        canonical_order=canonical_order,
        execution_context=execution_context,
    )


def build_order_from_intent(
    intent: SignalOrderIntent,
    *,
    wallet_id: str,
    created_at: datetime | None = None,
    status: OrderStatus = OrderStatus.CREATED,
    reservation_id: str | None = None,
) -> Order:
    timestamp = created_at or datetime.now(UTC)
    payload = {
        "request_id": intent.request_id,
        "ticket_id": intent.ticket_id,
        "wallet_id": wallet_id,
    }
    order_id = stable_object_id("ordr", payload)
    client_order_id = stable_object_id("ord", payload)
    order = intent.canonical_order
    return Order(
        order_id=order_id,
        client_order_id=client_order_id,
        wallet_id=wallet_id,
        market_id=order.market_id,
        token_id=order.token_id,
        outcome=order.outcome,
        side=OrderSide(str(order.side).lower()),
        price=order.price,
        size=order.size,
        route_action=order.route_action,
        time_in_force=order.time_in_force,
        expiration=order.expiration,
        fee_rate_bps=order.fee_rate_bps,
        signature_type=order.signature_type,
        funder=order.funder,
        status=status,
        filled_size=Decimal("0"),
        remaining_size=order.size,
        avg_fill_price=None,
        reservation_id=reservation_id,
        exchange_order_id=None,
        created_at=timestamp,
        updated_at=timestamp,
    )


def _json_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    decoded = json.loads(str(value))
    if not isinstance(decoded, list):
        raise ValueError("expected JSON list")
    return [str(item) for item in decoded]
