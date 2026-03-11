from __future__ import annotations

import json
from dataclasses import dataclass
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
from asterion_core.storage.os_queue import enqueue_upsert_rows_v1
from asterion_core.storage.write_queue import WriteQueueConfig


CAPABILITY_EXECUTION_CONTEXT_COLUMNS = [
    "execution_context_id",
    "wallet_id",
    "token_id",
    "route_action",
    "fee_rate_bps",
    "tick_size",
    "signature_type",
    "funder",
    "risk_gate_result",
    "market_capability_ref",
    "account_capability_ref",
    "created_at",
]


@dataclass(frozen=True)
class ExecutionContextRecord:
    execution_context_id: str
    wallet_id: str
    execution_context: ExecutionContext
    market_capability_ref: str
    account_capability_ref: str
    created_at: datetime

    def __post_init__(self) -> None:
        if not self.execution_context_id or not self.wallet_id:
            raise ValueError("execution_context_id and wallet_id are required")
        if not self.market_capability_ref or not self.account_capability_ref:
            raise ValueError("capability refs are required")
        if self.wallet_id != self.execution_context.account_capability.wallet_id:
            raise ValueError("wallet_id must match execution_context.account_capability.wallet_id")


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


def build_execution_context_record(
    *,
    wallet_id: str,
    execution_context: ExecutionContext,
    created_at: datetime | None = None,
    market_capability_ref: str | None = None,
    account_capability_ref: str | None = None,
) -> ExecutionContextRecord:
    resolved_market_ref = market_capability_ref or execution_context.market_capability.token_id
    resolved_account_ref = account_capability_ref or wallet_id
    execution_context_id = stable_object_id(
        "ectx",
        {
            "wallet_id": wallet_id,
            "token_id": execution_context.token_id,
            "route_action": execution_context.route_action.value,
            "fee_rate_bps": execution_context.fee_rate_bps,
            "tick_size": str(execution_context.tick_size),
            "signature_type": execution_context.signature_type,
            "funder": execution_context.funder,
            "risk_gate_result": execution_context.risk_gate_result,
            "market_capability_ref": resolved_market_ref,
            "account_capability_ref": resolved_account_ref,
        },
    )
    return ExecutionContextRecord(
        execution_context_id=execution_context_id,
        wallet_id=wallet_id,
        execution_context=execution_context,
        market_capability_ref=resolved_market_ref,
        account_capability_ref=resolved_account_ref,
        created_at=created_at or datetime.now(UTC),
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


def enqueue_execution_context_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    execution_contexts: list[ExecutionContextRecord],
    run_id: str | None = None,
) -> str | None:
    if not execution_contexts:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="capability.execution_contexts",
        pk_cols=["execution_context_id"],
        columns=list(CAPABILITY_EXECUTION_CONTEXT_COLUMNS),
        rows=[execution_context_record_to_row(item) for item in execution_contexts],
        run_id=run_id,
    )


def execution_context_record_to_row(record: ExecutionContextRecord) -> list[object]:
    ctx = record.execution_context
    return [
        record.execution_context_id,
        record.wallet_id,
        ctx.token_id,
        ctx.route_action.value,
        ctx.fee_rate_bps,
        format(ctx.tick_size, "f"),
        ctx.signature_type,
        ctx.funder,
        ctx.risk_gate_result,
        record.market_capability_ref,
        record.account_capability_ref,
        _sql_timestamp(record.created_at),
    ]


def _json_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    decoded = json.loads(str(value))
    if not isinstance(decoded, list):
        raise ValueError("expected JSON list")
    return [str(item) for item in decoded]


def _sql_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    normalized = value
    if normalized.tzinfo is not None:
        normalized = normalized.astimezone(UTC).replace(tzinfo=None)
    return normalized.isoformat(sep=" ", timespec="seconds")
