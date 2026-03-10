from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal

from asterion_core.contracts import RouteAction, StrategyDecision, StrategyRun, WatchOnlySnapshotRecord, stable_object_id

from .strategy_base import StrategyContext


@dataclass(frozen=True)
class StrategyRegistration:
    strategy_id: str
    strategy_version: str
    priority: int
    route_action: RouteAction
    size: Decimal
    min_edge_bps: int | None = None
    params: dict[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.strategy_id or not self.strategy_version:
            raise ValueError("strategy_id and strategy_version are required")
        if self.priority < 0:
            raise ValueError("priority must be non-negative")
        if self.size <= 0:
            raise ValueError("size must be positive")


def load_watch_only_snapshots(
    con,
    *,
    limit: int | None = None,
) -> tuple[list[WatchOnlySnapshotRecord], dict[str, int]]:
    sql = """
        SELECT
            snapshot_id,
            fair_value_id,
            run_id,
            market_id,
            condition_id,
            token_id,
            outcome,
            reference_price,
            fair_value,
            edge_bps,
            threshold_bps,
            decision,
            side,
            rationale,
            pricing_context_json,
            created_at
        FROM weather.weather_watch_only_snapshots
        ORDER BY created_at ASC, snapshot_id ASC
    """
    params: list[object] = []
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    rows = con.execute(sql, params).fetchall()
    snapshots: list[WatchOnlySnapshotRecord] = []
    signal_ts_ms: dict[str, int] = {}
    for row in rows:
        snapshots.append(
            WatchOnlySnapshotRecord(
                snapshot_id=str(row[0]),
                fair_value_id=str(row[1]),
                run_id=str(row[2]),
                market_id=str(row[3]),
                condition_id=str(row[4]),
                token_id=str(row[5]),
                outcome=str(row[6]),
                reference_price=float(row[7]),
                fair_value=float(row[8]),
                edge_bps=int(row[9]),
                threshold_bps=int(row[10]),
                decision=str(row[11]),
                side=str(row[12]),
                rationale=str(row[13]),
                pricing_context=_json_dict(row[14]),
            )
        )
        created_at = row[15]
        if isinstance(created_at, datetime):
            signal_ts_ms[str(row[0])] = int(created_at.replace(tzinfo=UTC).timestamp() * 1000)
        else:
            signal_ts_ms[str(row[0])] = 0
    return snapshots, signal_ts_ms


def run_strategy_engine(
    *,
    ctx: StrategyContext,
    snapshots: list[WatchOnlySnapshotRecord],
    strategies: list[StrategyRegistration],
    snapshot_signal_ts_ms: dict[str, int] | None = None,
    created_at: datetime | None = None,
) -> tuple[StrategyRun, list[StrategyDecision]]:
    ordered_strategies = sorted(strategies, key=lambda item: (item.priority, item.strategy_id, item.strategy_version))
    signal_ts_lookup = snapshot_signal_ts_ms or {}
    candidate_pairs: list[tuple[StrategyRegistration, WatchOnlySnapshotRecord]] = []
    for strategy in ordered_strategies:
        for snapshot in snapshots:
            if str(snapshot.decision).upper() in {"HOLD", "NO_TRADE"}:
                continue
            if str(snapshot.side).lower() not in {"buy", "sell"}:
                continue
            if strategy.min_edge_bps is not None and abs(int(snapshot.edge_bps)) < int(strategy.min_edge_bps):
                continue
            candidate_pairs.append((strategy, snapshot))

    run_id = stable_object_id(
        "srun",
        {
            "asof_ts_ms": ctx.asof_ts_ms,
            "data_snapshot_id": ctx.data_snapshot_id,
            "dq_level": ctx.dq_level,
            "snapshot_ids": [snapshot.snapshot_id for _, snapshot in candidate_pairs],
            "strategies": [
                {
                    "priority": strategy.priority,
                    "route_action": strategy.route_action.value,
                    "size": str(strategy.size),
                    "strategy_id": strategy.strategy_id,
                    "strategy_version": strategy.strategy_version,
                }
                for strategy in ordered_strategies
            ],
            "universe_snapshot_id": ctx.universe_snapshot_id,
        },
    )

    sorted_pairs = sorted(
        candidate_pairs,
        key=lambda item: (
            item[0].priority,
            _signal_ts_ms(item[1], signal_ts_lookup),
            item[1].market_id,
            item[1].token_id,
            str(item[1].side).lower(),
            item[0].strategy_id,
            item[1].snapshot_id,
        ),
    )
    decisions: list[StrategyDecision] = []
    for rank, (strategy, snapshot) in enumerate(sorted_pairs, start=1):
        payload = {
            "edge_bps": int(snapshot.edge_bps),
            "market_id": snapshot.market_id,
            "route_action": strategy.route_action.value,
            "run_id": run_id,
            "side": str(snapshot.side).lower(),
            "strategy_id": strategy.strategy_id,
            "token_id": snapshot.token_id,
            "watch_snapshot_id": snapshot.snapshot_id,
        }
        decisions.append(
            StrategyDecision(
                decision_id=stable_object_id("sdec", payload),
                run_id=run_id,
                decision_rank=rank,
                strategy_id=strategy.strategy_id,
                strategy_version=strategy.strategy_version,
                market_id=snapshot.market_id,
                token_id=snapshot.token_id,
                outcome=snapshot.outcome,
                side=str(snapshot.side).lower(),
                signal_ts_ms=_signal_ts_ms(snapshot, signal_ts_lookup),
                reference_price=Decimal(str(snapshot.reference_price)),
                fair_value=Decimal(str(snapshot.fair_value)),
                edge_bps=int(snapshot.edge_bps),
                threshold_bps=int(snapshot.threshold_bps),
                route_action=strategy.route_action,
                size=strategy.size,
                forecast_run_id=snapshot.run_id,
                watch_snapshot_id=snapshot.snapshot_id,
            )
        )

    run = StrategyRun(
        run_id=run_id,
        data_snapshot_id=ctx.data_snapshot_id,
        universe_snapshot_id=ctx.universe_snapshot_id,
        asof_ts_ms=ctx.asof_ts_ms,
        dq_level=ctx.dq_level,
        strategy_ids=[item.strategy_id for item in ordered_strategies],
        decision_count=len(decisions),
        created_at=created_at or datetime.now(UTC),
    )
    return run, decisions


def _signal_ts_ms(snapshot: WatchOnlySnapshotRecord, signal_ts_lookup: dict[str, int]) -> int:
    if snapshot.snapshot_id in signal_ts_lookup:
        return int(signal_ts_lookup[snapshot.snapshot_id])
    raw = snapshot.pricing_context.get("signal_ts_ms")
    try:
        return max(0, int(raw))
    except Exception:  # noqa: BLE001
        return 0


def _json_dict(value: object) -> dict[str, object]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return {str(key): value[key] for key in value}
    decoded = json.loads(str(value))
    if not isinstance(decoded, dict):
        raise ValueError("pricing_context_json must decode to an object")
    return {str(key): decoded[key] for key in decoded}
