from __future__ import annotations

from datetime import UTC, datetime

from asterion_core.contracts import StrategyDecision, TradeTicket, stable_object_id


def build_trade_ticket(
    decision: StrategyDecision,
    *,
    created_at: datetime | None = None,
) -> TradeTicket:
    provenance = {
        "decision_id": decision.decision_id,
        "decision_rank": decision.decision_rank,
        "forecast_run_id": decision.forecast_run_id,
        "route_action": decision.route_action.value,
        "strategy_id": decision.strategy_id,
        "strategy_version": decision.strategy_version,
        "watch_snapshot_id": decision.watch_snapshot_id,
    }
    semantic_payload = {
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
        "size": str(decision.size),
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
        size=decision.size,
        signal_ts_ms=decision.signal_ts_ms,
        forecast_run_id=decision.forecast_run_id,
        watch_snapshot_id=decision.watch_snapshot_id,
        request_id=request_id,
        ticket_hash=ticket_hash,
        provenance_json=provenance,
        created_at=created_at or datetime.now(UTC),
    )
