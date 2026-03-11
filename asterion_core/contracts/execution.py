from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any


class TimeInForce(str, Enum):
    GTC = "gtc"
    GTD = "gtd"
    FAK = "fak"
    FOK = "fok"


class RouteAction(str, Enum):
    POST_ONLY_GTC = "post_only_gtc"
    POST_ONLY_GTD = "post_only_gtd"
    FAK = "fak"
    FOK = "fok"


@dataclass(frozen=True)
class RouteActionCLOBMapping:
    order_type: str
    post_only: bool


_ROUTE_ACTION_TO_TIF: dict[RouteAction, TimeInForce] = {
    RouteAction.POST_ONLY_GTC: TimeInForce.GTC,
    RouteAction.POST_ONLY_GTD: TimeInForce.GTD,
    RouteAction.FAK: TimeInForce.FAK,
    RouteAction.FOK: TimeInForce.FOK,
}

_ROUTE_ACTION_TO_CLOB_MAPPING: dict[RouteAction, RouteActionCLOBMapping] = {
    action: RouteActionCLOBMapping(
        order_type=time_in_force.value.upper(),
        post_only=action in {RouteAction.POST_ONLY_GTC, RouteAction.POST_ONLY_GTD},
    )
    for action, time_in_force in _ROUTE_ACTION_TO_TIF.items()
}


def time_in_force_for_route_action(route_action: RouteAction) -> TimeInForce:
    return _ROUTE_ACTION_TO_TIF[route_action]


def post_only_for_route_action(route_action: RouteAction) -> bool:
    return _ROUTE_ACTION_TO_CLOB_MAPPING[route_action].post_only


def clob_mapping_for_route_action(route_action: RouteAction) -> RouteActionCLOBMapping:
    return _ROUTE_ACTION_TO_CLOB_MAPPING[route_action]


@dataclass(frozen=True)
class CanonicalOrderContract:
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

    def __post_init__(self) -> None:
        if not self.market_id:
            raise ValueError("market_id is required")
        if not self.token_id:
            raise ValueError("token_id is required")
        if not self.outcome:
            raise ValueError("outcome is required")
        if not self.side:
            raise ValueError("side is required")
        if self.price <= 0:
            raise ValueError("price must be positive")
        if self.size <= 0:
            raise ValueError("size must be positive")
        expected_tif = time_in_force_for_route_action(self.route_action)
        if self.time_in_force is not expected_tif:
            raise ValueError(
                f"time_in_force={self.time_in_force.value!r} does not match "
                f"route_action={self.route_action.value!r}"
            )
        if self.route_action is RouteAction.POST_ONLY_GTD and self.expiration is None:
            raise ValueError("expiration is required for POST_ONLY_GTD")
        if self.route_action in {RouteAction.FAK, RouteAction.FOK} and self.expiration is not None:
            raise ValueError("expiration must be None for FAK/FOK")
        if self.fee_rate_bps < 0:
            raise ValueError("fee_rate_bps must be non-negative")
        if self.signature_type < 0:
            raise ValueError("signature_type must be non-negative")
        if not self.funder:
            raise ValueError("funder is required")


@dataclass(frozen=True)
class MarketCapability:
    market_id: str
    condition_id: str
    token_id: str
    outcome: str
    tick_size: Decimal
    fee_rate_bps: int
    neg_risk: bool
    min_order_size: Decimal
    tradable: bool
    fees_enabled: bool
    data_sources: list[str]
    updated_at: datetime

    def __post_init__(self) -> None:
        if not self.market_id:
            raise ValueError("market_id is required")
        if not self.condition_id:
            raise ValueError("condition_id is required")
        if not self.token_id:
            raise ValueError("token_id is required")
        if self.tick_size <= 0:
            raise ValueError("tick_size must be positive")
        if self.min_order_size <= 0:
            raise ValueError("min_order_size must be positive")
        if self.fee_rate_bps < 0:
            raise ValueError("fee_rate_bps must be non-negative")


@dataclass(frozen=True)
class AccountTradingCapability:
    wallet_id: str
    wallet_type: str
    signature_type: int
    funder: str
    allowance_targets: list[str]
    can_use_relayer: bool
    can_trade: bool
    restricted_reason: str | None

    def __post_init__(self) -> None:
        if not self.wallet_id:
            raise ValueError("wallet_id is required")
        if not self.wallet_type:
            raise ValueError("wallet_type is required")
        if self.signature_type < 0:
            raise ValueError("signature_type must be non-negative")
        if not self.funder:
            raise ValueError("funder is required")


@dataclass(frozen=True)
class ExecutionContext:
    market_capability: MarketCapability
    account_capability: AccountTradingCapability
    token_id: str
    route_action: RouteAction
    fee_rate_bps: int
    tick_size: Decimal
    signature_type: int
    funder: str
    risk_gate_result: str

    def __post_init__(self) -> None:
        if self.token_id != self.market_capability.token_id:
            raise ValueError("token_id must match market_capability.token_id")
        if self.fee_rate_bps != self.market_capability.fee_rate_bps:
            raise ValueError("fee_rate_bps must come from market_capability")
        if self.tick_size != self.market_capability.tick_size:
            raise ValueError("tick_size must come from market_capability")
        if self.signature_type != self.account_capability.signature_type:
            raise ValueError("signature_type must come from account_capability")
        if self.funder != self.account_capability.funder:
            raise ValueError("funder must come from account_capability")
        if not self.risk_gate_result:
            raise ValueError("risk_gate_result is required")


@dataclass(frozen=True)
class StrategyRun:
    run_id: str
    data_snapshot_id: str
    universe_snapshot_id: str | None
    asof_ts_ms: int
    dq_level: str
    strategy_ids: list[str]
    decision_count: int
    created_at: datetime

    def __post_init__(self) -> None:
        if not self.run_id:
            raise ValueError("run_id is required")
        if not self.data_snapshot_id:
            raise ValueError("data_snapshot_id is required")
        if self.universe_snapshot_id == "":
            raise ValueError("universe_snapshot_id must be None or non-empty")
        if self.asof_ts_ms < 0:
            raise ValueError("asof_ts_ms must be non-negative")
        if self.decision_count < 0:
            raise ValueError("decision_count must be non-negative")
        if not self.strategy_ids:
            raise ValueError("strategy_ids is required")


@dataclass(frozen=True)
class StrategyDecision:
    decision_id: str
    run_id: str
    decision_rank: int
    strategy_id: str
    strategy_version: str
    market_id: str
    token_id: str
    outcome: str
    side: str
    signal_ts_ms: int
    reference_price: Decimal
    fair_value: Decimal
    edge_bps: int
    threshold_bps: int
    route_action: RouteAction
    size: Decimal
    forecast_run_id: str
    watch_snapshot_id: str

    def __post_init__(self) -> None:
        if not self.decision_id or not self.run_id:
            raise ValueError("decision_id and run_id are required")
        if self.decision_rank <= 0:
            raise ValueError("decision_rank must be positive")
        if not self.strategy_id or not self.strategy_version:
            raise ValueError("strategy_id and strategy_version are required")
        if not self.market_id or not self.token_id or not self.outcome:
            raise ValueError("market_id, token_id, and outcome are required")
        if self.side not in {"buy", "sell"}:
            raise ValueError("side must be buy or sell")
        if self.signal_ts_ms < 0:
            raise ValueError("signal_ts_ms must be non-negative")
        if self.reference_price <= 0 or self.fair_value < 0:
            raise ValueError("reference_price must be positive and fair_value must be non-negative")
        if self.size <= 0:
            raise ValueError("size must be positive")
        if not self.forecast_run_id or not self.watch_snapshot_id:
            raise ValueError("forecast_run_id and watch_snapshot_id are required")


@dataclass(frozen=True)
class TradeTicket:
    ticket_id: str
    run_id: str
    strategy_id: str
    strategy_version: str
    market_id: str
    token_id: str
    outcome: str
    side: str
    reference_price: Decimal
    fair_value: Decimal
    edge_bps: int
    threshold_bps: int
    route_action: RouteAction
    size: Decimal
    signal_ts_ms: int
    forecast_run_id: str
    watch_snapshot_id: str
    request_id: str
    ticket_hash: str
    provenance_json: dict[str, Any]
    created_at: datetime
    wallet_id: str | None = None
    execution_context_id: str | None = None

    def __post_init__(self) -> None:
        if not self.ticket_id or not self.run_id or not self.strategy_id:
            raise ValueError("ticket_id, run_id, and strategy_id are required")
        if not self.strategy_version:
            raise ValueError("strategy_version is required")
        if not self.market_id or not self.token_id or not self.outcome:
            raise ValueError("market_id, token_id, and outcome are required")
        if self.side not in {"buy", "sell"}:
            raise ValueError("side must be buy or sell")
        if self.reference_price <= 0 or self.fair_value < 0:
            raise ValueError("reference_price must be positive and fair_value must be non-negative")
        if self.size <= 0:
            raise ValueError("size must be positive")
        if self.signal_ts_ms < 0:
            raise ValueError("signal_ts_ms must be non-negative")
        if not self.forecast_run_id or not self.watch_snapshot_id:
            raise ValueError("forecast_run_id and watch_snapshot_id are required")
        if not self.request_id or not self.ticket_hash:
            raise ValueError("request_id and ticket_hash are required")
        if not isinstance(self.provenance_json, dict):
            raise ValueError("provenance_json must be a dictionary")
        if self.wallet_id == "":
            raise ValueError("wallet_id must be None or non-empty")
        if self.execution_context_id == "":
            raise ValueError("execution_context_id must be None or non-empty")


@dataclass(frozen=True)
class SignalOrderIntent:
    ticket_id: str
    request_id: str
    canonical_order: CanonicalOrderContract
    execution_context: ExecutionContext

    def __post_init__(self) -> None:
        if not self.ticket_id or not self.request_id:
            raise ValueError("ticket_id and request_id are required")
        if self.canonical_order.token_id != self.execution_context.token_id:
            raise ValueError("canonical_order.token_id must match execution_context.token_id")


@dataclass(frozen=True)
class GateDecision:
    gate_id: str
    ticket_id: str
    allowed: bool
    reason: str
    reason_codes: list[str]
    metrics_json: dict[str, Any]
    created_at: datetime

    def __post_init__(self) -> None:
        if not self.gate_id or not self.ticket_id:
            raise ValueError("gate_id and ticket_id are required")
        if not self.reason:
            raise ValueError("reason is required")
        if not isinstance(self.reason_codes, list):
            raise ValueError("reason_codes must be a list")
        if not isinstance(self.metrics_json, dict):
            raise ValueError("metrics_json must be a dictionary")


@dataclass(frozen=True)
class JournalEvent:
    event_id: str
    event_type: str
    entity_type: str
    entity_id: str
    run_id: str | None
    payload_json: dict[str, Any]
    created_at: datetime

    def __post_init__(self) -> None:
        if not self.event_id or not self.event_type:
            raise ValueError("event_id and event_type are required")
        if not self.entity_type or not self.entity_id:
            raise ValueError("entity_type and entity_id are required")
        if not isinstance(self.payload_json, dict):
            raise ValueError("payload_json must be a dictionary")
