from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from asterion_core.contracts import (
    ExposureSnapshot,
    ExternalBalanceObservation,
    Fill,
    GateDecision,
    InventoryPosition,
    JournalEvent,
    Order,
    OrderStateTransition,
    ReconciliationResult,
    Reservation,
    StrategyRun,
    TradeTicket,
    stable_object_id,
)
from asterion_core.storage.os_queue import enqueue_upsert_rows_v1
from asterion_core.storage.utils import safe_json_dumps
from asterion_core.storage.write_queue import WriteQueueConfig


RUNTIME_STRATEGY_RUN_COLUMNS = [
    "run_id",
    "data_snapshot_id",
    "universe_snapshot_id",
    "asof_ts_ms",
    "dq_level",
    "strategy_ids_json",
    "decision_count",
    "created_at",
]

RUNTIME_TRADE_TICKET_COLUMNS = [
    "ticket_id",
    "run_id",
    "strategy_id",
    "strategy_version",
    "market_id",
    "token_id",
    "outcome",
    "side",
    "reference_price",
    "fair_value",
    "edge_bps",
    "threshold_bps",
    "route_action",
    "size",
    "signal_ts_ms",
    "forecast_run_id",
    "watch_snapshot_id",
    "request_id",
    "ticket_hash",
    "wallet_id",
    "execution_context_id",
    "provenance_json",
    "created_at",
]

RUNTIME_GATE_DECISION_COLUMNS = [
    "gate_id",
    "ticket_id",
    "allowed",
    "reason",
    "reason_codes_json",
    "metrics_json",
    "created_at",
]

RUNTIME_JOURNAL_EVENT_COLUMNS = [
    "event_id",
    "event_type",
    "entity_type",
    "entity_id",
    "run_id",
    "payload_json",
    "created_at",
]

TRADING_ORDER_COLUMNS = [
    "order_id",
    "client_order_id",
    "wallet_id",
    "market_id",
    "token_id",
    "outcome",
    "side",
    "price",
    "size",
    "route_action",
    "time_in_force",
    "expiration",
    "fee_rate_bps",
    "signature_type",
    "funder",
    "status",
    "filled_size",
    "remaining_size",
    "avg_fill_price",
    "reservation_id",
    "exchange_order_id",
    "created_at",
    "submitted_at",
    "updated_at",
]

TRADING_FILL_COLUMNS = [
    "fill_id",
    "order_id",
    "wallet_id",
    "market_id",
    "token_id",
    "outcome",
    "side",
    "price",
    "size",
    "fee",
    "fee_rate_bps",
    "trade_id",
    "exchange_order_id",
    "filled_at",
]

TRADING_ORDER_STATE_TRANSITION_COLUMNS = [
    "transition_id",
    "order_id",
    "from_status",
    "to_status",
    "reason",
    "timestamp",
]

TRADING_RESERVATION_COLUMNS = [
    "reservation_id",
    "order_id",
    "wallet_id",
    "asset_type",
    "token_id",
    "market_id",
    "outcome",
    "funder",
    "signature_type",
    "reserved_quantity",
    "remaining_quantity",
    "reserved_notional",
    "status",
    "created_at",
    "updated_at",
]

TRADING_INVENTORY_POSITION_COLUMNS = [
    "wallet_id",
    "asset_type",
    "token_id",
    "market_id",
    "outcome",
    "balance_type",
    "quantity",
    "funder",
    "signature_type",
    "updated_at",
]

TRADING_EXPOSURE_COLUMNS = [
    "snapshot_id",
    "wallet_id",
    "funder",
    "signature_type",
    "market_id",
    "token_id",
    "outcome",
    "open_order_size",
    "reserved_notional_usdc",
    "filled_position_size",
    "settled_position_size",
    "redeemable_size",
    "captured_at",
]

TRADING_RECONCILIATION_COLUMNS = [
    "reconciliation_id",
    "wallet_id",
    "funder",
    "signature_type",
    "asset_type",
    "token_id",
    "market_id",
    "balance_type",
    "local_quantity",
    "remote_quantity",
    "discrepancy",
    "status",
    "resolution",
    "created_at",
]

RUNTIME_EXTERNAL_BALANCE_OBSERVATION_COLUMNS = [
    "observation_id",
    "wallet_id",
    "funder",
    "signature_type",
    "asset_type",
    "token_id",
    "market_id",
    "outcome",
    "observation_kind",
    "allowance_target",
    "chain_id",
    "block_number",
    "observed_quantity",
    "source",
    "observed_at",
    "raw_observation_json",
]


def build_journal_event(
    *,
    event_type: str,
    entity_type: str,
    entity_id: str,
    payload_json: dict[str, Any],
    run_id: str | None = None,
    created_at: datetime | None = None,
) -> JournalEvent:
    timestamp = created_at or datetime.now(UTC)
    return JournalEvent(
        event_id=stable_object_id(
            "jevt",
            {
                "entity_id": entity_id,
                "entity_type": entity_type,
                "event_type": event_type,
                "run_id": run_id,
            },
        ),
        event_type=event_type,
        entity_type=entity_type,
        entity_id=entity_id,
        run_id=run_id,
        payload_json=payload_json,
        created_at=timestamp,
    )


def enqueue_strategy_run_upserts(queue_cfg: WriteQueueConfig, *, runs: list[StrategyRun], run_id: str | None = None) -> str | None:
    if not runs:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="runtime.strategy_runs",
        pk_cols=["run_id"],
        columns=list(RUNTIME_STRATEGY_RUN_COLUMNS),
        rows=[strategy_run_to_row(item) for item in runs],
        run_id=run_id,
    )


def enqueue_trade_ticket_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    tickets: list[TradeTicket],
    run_id: str | None = None,
) -> str | None:
    if not tickets:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="runtime.trade_tickets",
        pk_cols=["ticket_id"],
        columns=list(RUNTIME_TRADE_TICKET_COLUMNS),
        rows=[trade_ticket_to_row(item) for item in tickets],
        run_id=run_id,
    )


def enqueue_gate_decision_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    gate_decisions: list[GateDecision],
    run_id: str | None = None,
) -> str | None:
    if not gate_decisions:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="runtime.gate_decisions",
        pk_cols=["gate_id"],
        columns=list(RUNTIME_GATE_DECISION_COLUMNS),
        rows=[gate_decision_to_row(item) for item in gate_decisions],
        run_id=run_id,
    )


def enqueue_journal_event_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    journal_events: list[JournalEvent],
    run_id: str | None = None,
) -> str | None:
    if not journal_events:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="runtime.journal_events",
        pk_cols=["event_id"],
        columns=list(RUNTIME_JOURNAL_EVENT_COLUMNS),
        rows=[journal_event_to_row(item) for item in journal_events],
        run_id=run_id,
    )


def enqueue_order_upserts(queue_cfg: WriteQueueConfig, *, orders: list[Order], run_id: str | None = None) -> str | None:
    if not orders:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="trading.orders",
        pk_cols=["order_id"],
        columns=list(TRADING_ORDER_COLUMNS),
        rows=[order_to_row(item) for item in orders],
        run_id=run_id,
    )


def enqueue_fill_upserts(queue_cfg: WriteQueueConfig, *, fills: list[Fill], run_id: str | None = None) -> str | None:
    if not fills:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="trading.fills",
        pk_cols=["fill_id"],
        columns=list(TRADING_FILL_COLUMNS),
        rows=[fill_to_row(item) for item in fills],
        run_id=run_id,
    )


def enqueue_order_state_transition_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    transitions: list[OrderStateTransition],
    run_id: str | None = None,
) -> str | None:
    if not transitions:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="trading.order_state_transitions",
        pk_cols=["transition_id"],
        columns=list(TRADING_ORDER_STATE_TRANSITION_COLUMNS),
        rows=[order_state_transition_to_row(item) for item in transitions],
        run_id=run_id,
    )


def enqueue_reservation_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    reservations: list[Reservation],
    run_id: str | None = None,
) -> str | None:
    if not reservations:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="trading.reservations",
        pk_cols=["reservation_id"],
        columns=list(TRADING_RESERVATION_COLUMNS),
        rows=[reservation_to_row(item) for item in reservations],
        run_id=run_id,
    )


def enqueue_inventory_position_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    positions: list[InventoryPosition],
    run_id: str | None = None,
) -> str | None:
    if not positions:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="trading.inventory_positions",
        pk_cols=["wallet_id", "asset_type", "token_id", "market_id", "outcome", "balance_type"],
        columns=list(TRADING_INVENTORY_POSITION_COLUMNS),
        rows=[inventory_position_to_row(item) for item in positions],
        run_id=run_id,
    )


def enqueue_exposure_snapshot_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    snapshots: list[ExposureSnapshot],
    run_id: str | None = None,
) -> str | None:
    if not snapshots:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="trading.exposure_snapshots",
        pk_cols=["snapshot_id"],
        columns=list(TRADING_EXPOSURE_COLUMNS),
        rows=[exposure_snapshot_to_row(item) for item in snapshots],
        run_id=run_id,
    )


def enqueue_reconciliation_result_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    results: list[ReconciliationResult],
    run_id: str | None = None,
) -> str | None:
    if not results:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="trading.reconciliation_results",
        pk_cols=["reconciliation_id"],
        columns=list(TRADING_RECONCILIATION_COLUMNS),
        rows=[reconciliation_result_to_row(item) for item in results],
        run_id=run_id,
    )


def enqueue_external_balance_observation_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    observations: list[ExternalBalanceObservation],
    run_id: str | None = None,
) -> str | None:
    if not observations:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="runtime.external_balance_observations",
        pk_cols=["observation_id"],
        columns=list(RUNTIME_EXTERNAL_BALANCE_OBSERVATION_COLUMNS),
        rows=[external_balance_observation_to_row(item) for item in observations],
        run_id=run_id,
    )


def strategy_run_to_row(record: StrategyRun) -> list[Any]:
    return [
        record.run_id,
        record.data_snapshot_id,
        record.universe_snapshot_id,
        record.asof_ts_ms,
        record.dq_level,
        safe_json_dumps(record.strategy_ids),
        record.decision_count,
        _sql_timestamp(record.created_at),
    ]


def trade_ticket_to_row(record: TradeTicket) -> list[Any]:
    return [
        record.ticket_id,
        record.run_id,
        record.strategy_id,
        record.strategy_version,
        record.market_id,
        record.token_id,
        record.outcome,
        record.side,
        _decimal_to_sql(record.reference_price),
        _decimal_to_sql(record.fair_value),
        record.edge_bps,
        record.threshold_bps,
        record.route_action.value,
        _decimal_to_sql(record.size),
        record.signal_ts_ms,
        record.forecast_run_id,
        record.watch_snapshot_id,
        record.request_id,
        record.ticket_hash,
        record.wallet_id,
        record.execution_context_id,
        safe_json_dumps(record.provenance_json),
        _sql_timestamp(record.created_at),
    ]


def gate_decision_to_row(record: GateDecision) -> list[Any]:
    return [
        record.gate_id,
        record.ticket_id,
        record.allowed,
        record.reason,
        safe_json_dumps(record.reason_codes),
        safe_json_dumps(record.metrics_json),
        _sql_timestamp(record.created_at),
    ]


def journal_event_to_row(record: JournalEvent) -> list[Any]:
    return [
        record.event_id,
        record.event_type,
        record.entity_type,
        record.entity_id,
        record.run_id,
        safe_json_dumps(record.payload_json),
        _sql_timestamp(record.created_at),
    ]


def order_to_row(record: Order) -> list[Any]:
    return [
        record.order_id,
        record.client_order_id,
        record.wallet_id,
        record.market_id,
        record.token_id,
        record.outcome,
        _enum_value(record.side),
        _decimal_to_sql(record.price),
        _decimal_to_sql(record.size),
        _enum_value(record.route_action),
        _enum_value(record.time_in_force),
        _sql_timestamp(record.expiration),
        record.fee_rate_bps,
        record.signature_type,
        record.funder,
        _enum_value(record.status),
        _decimal_to_sql(record.filled_size),
        _decimal_to_sql(record.remaining_size),
        _decimal_to_sql(record.avg_fill_price),
        record.reservation_id,
        record.exchange_order_id,
        _sql_timestamp(record.created_at),
        None,
        _sql_timestamp(record.updated_at),
    ]


def fill_to_row(record: Fill) -> list[Any]:
    return [
        record.fill_id,
        record.order_id,
        record.wallet_id,
        record.market_id,
        record.token_id,
        record.outcome,
        _enum_value(record.side),
        _decimal_to_sql(record.price),
        _decimal_to_sql(record.size),
        _decimal_to_sql(record.fee),
        record.fee_rate_bps,
        record.trade_id,
        record.exchange_order_id,
        _sql_timestamp(record.filled_at),
    ]


def order_state_transition_to_row(record: OrderStateTransition) -> list[Any]:
    return [
        record.transition_id,
        record.order_id,
        _enum_value(record.from_status),
        _enum_value(record.to_status),
        record.reason,
        _sql_timestamp(record.timestamp),
    ]


def reservation_to_row(record: Reservation) -> list[Any]:
    return [
        record.reservation_id,
        record.order_id,
        record.wallet_id,
        record.asset_type,
        record.token_id,
        record.market_id,
        record.outcome,
        record.funder,
        record.signature_type,
        _decimal_to_sql(record.reserved_quantity),
        _decimal_to_sql(record.remaining_quantity),
        _decimal_to_sql(record.reserved_notional),
        _enum_value(record.status),
        _sql_timestamp(record.created_at),
        _sql_timestamp(record.updated_at),
    ]


def inventory_position_to_row(record: InventoryPosition) -> list[Any]:
    return [
        record.wallet_id,
        record.asset_type,
        record.token_id,
        record.market_id,
        record.outcome,
        _enum_value(record.balance_type),
        _decimal_to_sql(record.quantity),
        record.funder,
        record.signature_type,
        _sql_timestamp(record.updated_at),
    ]


def exposure_snapshot_to_row(record: ExposureSnapshot) -> list[Any]:
    return [
        record.snapshot_id,
        record.wallet_id,
        record.funder,
        record.signature_type,
        record.market_id,
        record.token_id,
        record.outcome,
        _decimal_to_sql(record.open_order_size),
        _decimal_to_sql(record.reserved_notional_usdc),
        _decimal_to_sql(record.filled_position_size),
        _decimal_to_sql(record.settled_position_size),
        _decimal_to_sql(record.redeemable_size),
        _sql_timestamp(record.captured_at),
    ]


def reconciliation_result_to_row(record: ReconciliationResult) -> list[Any]:
    return [
        record.reconciliation_id,
        record.wallet_id,
        record.funder,
        record.signature_type,
        record.asset_type,
        record.token_id,
        record.market_id,
        _enum_value(record.balance_type),
        _decimal_to_sql(record.local_quantity),
        _decimal_to_sql(record.remote_quantity),
        _decimal_to_sql(record.discrepancy),
        _enum_value(record.status),
        record.resolution,
        _sql_timestamp(record.created_at),
    ]


def external_balance_observation_to_row(record: ExternalBalanceObservation) -> list[Any]:
    return [
        record.observation_id,
        record.wallet_id,
        record.funder,
        record.signature_type,
        record.asset_type,
        record.token_id,
        record.market_id,
        record.outcome,
        _enum_value(record.observation_kind),
        record.allowance_target,
        record.chain_id,
        record.block_number,
        _decimal_to_sql(record.observed_quantity),
        record.source,
        _sql_timestamp(record.observed_at),
        safe_json_dumps(record.raw_observation_json),
    ]


def _enum_value(value: object) -> Any:
    return getattr(value, "value", value)


def _decimal_to_sql(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def _sql_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    normalized = value
    if normalized.tzinfo is not None:
        normalized = normalized.astimezone(UTC).replace(tzinfo=None)
    return normalized.isoformat(sep=" ", timespec="seconds")
