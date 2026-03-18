from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

from asterion_core.contracts import RouteAction, StrategyDecision, TradeTicket, stable_object_id


def build_trade_ticket(
    decision: StrategyDecision,
    *,
    created_at: datetime | None = None,
    size_override: Decimal | None = None,
    allocation_context: dict[str, object] | None = None,
) -> TradeTicket:
    effective_size = size_override if size_override is not None else decision.size
    provenance = {
        "decision_id": decision.decision_id,
        "decision_rank": decision.decision_rank,
        "forecast_run_id": decision.forecast_run_id,
        "pricing_context": dict(decision.pricing_context_json),
        "route_action": decision.route_action.value,
        "strategy_id": decision.strategy_id,
        "strategy_version": decision.strategy_version,
        "watch_snapshot_id": decision.watch_snapshot_id,
    }
    if allocation_context:
        provenance.update(
            {
                "requested_size": str(decision.size),
                "recommended_size": str(allocation_context.get("recommended_size") or effective_size),
                "allocation_status": allocation_context.get("allocation_status"),
                "allocation_decision_id": allocation_context.get("allocation_decision_id"),
                "allocation_reason_codes": list(allocation_context.get("allocation_reason_codes") or []),
                "budget_impact": dict(allocation_context.get("budget_impact") or {}),
                "policy_id": allocation_context.get("policy_id"),
                "policy_version": allocation_context.get("policy_version"),
            }
        )
    semantic_payload = {
        "allocation_decision_id": allocation_context.get("allocation_decision_id") if allocation_context else None,
        "allocation_status": allocation_context.get("allocation_status") if allocation_context else None,
        "decision_rank": decision.decision_rank,
        "edge_bps": decision.edge_bps,
        "fair_value": str(decision.fair_value),
        "market_id": decision.market_id,
        "outcome": decision.outcome,
        "reference_price": str(decision.reference_price),
        "route_action": decision.route_action.value,
        "run_id": decision.run_id,
        "side": decision.side,
        "signal_ts_ms": decision.signal_ts_ms,
        "size": str(effective_size),
        "strategy_id": decision.strategy_id,
        "strategy_version": decision.strategy_version,
        "threshold_bps": decision.threshold_bps,
        "token_id": decision.token_id,
    }
    ticket_hash = stable_object_id("thash", semantic_payload)
    ticket_id = stable_object_id("tt", {"decision_id": decision.decision_id, "ticket_hash": ticket_hash})
    request_id = stable_object_id("req", {"ticket_id": ticket_id, "ticket_hash": ticket_hash})
    return TradeTicket(
        ticket_id=ticket_id,
        run_id=decision.run_id,
        strategy_id=decision.strategy_id,
        strategy_version=decision.strategy_version,
        market_id=decision.market_id,
        token_id=decision.token_id,
        outcome=decision.outcome,
        side=decision.side,
        reference_price=decision.reference_price,
        fair_value=decision.fair_value,
        edge_bps=decision.edge_bps,
        threshold_bps=decision.threshold_bps,
        route_action=decision.route_action,
        size=effective_size,
        signal_ts_ms=decision.signal_ts_ms,
        forecast_run_id=decision.forecast_run_id,
        watch_snapshot_id=decision.watch_snapshot_id,
        request_id=request_id,
        ticket_hash=ticket_hash,
        provenance_json=provenance,
        created_at=created_at or datetime.now(UTC),
    )


def bind_trade_ticket_handoff(
    ticket: TradeTicket,
    *,
    wallet_id: str,
    execution_context_id: str,
) -> TradeTicket:
    if not wallet_id:
        raise ValueError("wallet_id is required")
    if not execution_context_id:
        raise ValueError("execution_context_id is required")
    return replace(
        ticket,
        wallet_id=wallet_id,
        execution_context_id=execution_context_id,
    )


def load_trade_ticket(con, *, ticket_id: str) -> TradeTicket:
    row = con.execute(
        """
        SELECT
            ticket_id,
            run_id,
            strategy_id,
            strategy_version,
            market_id,
            token_id,
            outcome,
            side,
            reference_price,
            fair_value,
            edge_bps,
            threshold_bps,
            route_action,
            size,
            signal_ts_ms,
            forecast_run_id,
            watch_snapshot_id,
            request_id,
            ticket_hash,
            provenance_json,
            created_at,
            wallet_id,
            execution_context_id
        FROM runtime.trade_tickets
        WHERE ticket_id = ?
        """,
        [ticket_id],
    ).fetchone()
    if row is None:
        raise LookupError(f"trade ticket not found for ticket_id={ticket_id}")
    return TradeTicket(
        ticket_id=str(row[0]),
        run_id=str(row[1]),
        strategy_id=str(row[2]),
        strategy_version=str(row[3]),
        market_id=str(row[4]),
        token_id=str(row[5]),
        outcome=str(row[6]),
        side=str(row[7]),
        reference_price=Decimal(str(row[8])),
        fair_value=Decimal(str(row[9])),
        edge_bps=int(row[10]),
        threshold_bps=int(row[11]),
        route_action=RouteAction(str(row[12])),
        size=Decimal(str(row[13])),
        signal_ts_ms=int(row[14]),
        forecast_run_id=str(row[15]),
        watch_snapshot_id=str(row[16]),
        request_id=str(row[17]),
        ticket_hash=str(row[18]),
        provenance_json=_json_dict(row[19]),
        created_at=row[20],
        wallet_id=str(row[21]) if row[21] is not None else None,
        execution_context_id=str(row[22]) if row[22] is not None else None,
    )


def _json_dict(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return {str(key): value[key] for key in value}
    decoded = json.loads(str(value))
    if not isinstance(decoded, dict):
        raise ValueError("provenance_json must decode to an object")
    return {str(key): decoded[key] for key in decoded}
