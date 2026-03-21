from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import duckdb

from asterion_core.contracts import RouteAction, StrategyDecision
from asterion_core.execution.trade_ticket_v1 import build_trade_ticket
from asterion_core.risk import materialize_capital_allocation
from asterion_core.risk.allocator_v1 import (
    RUNTIME_ALLOCATION_DECISION_COLUMNS,
    allocation_decision_to_row,
)
from asterion_core.ui.builders.opportunity_builder import _create_action_queue_summary


HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required")
class AllocatorRerankSurfaceConsistencyTest(unittest.TestCase):
    def _apply_migrations(self, db_path: str) -> None:
        bootstrap = duckdb.connect(db_path)
        migrations_dir = Path(__file__).resolve().parents[1] / "sql" / "migrations"
        for path in sorted(migrations_dir.glob("*.sql")):
            sql = path.read_text(encoding="utf-8").strip()
            if sql:
                bootstrap.execute(sql)
        bootstrap.close()

    def _insert_market_spec(self, con, *, market_id: str) -> None:
        con.execute(
            """
            INSERT INTO weather.weather_market_specs (
                market_id, condition_id, location_name, station_id, latitude, longitude, timezone,
                observation_date, observation_window_local, metric, unit, bucket_min_value, bucket_max_value,
                authoritative_source, fallback_sources, rounding_rule, inclusive_bounds, spec_version,
                parse_confidence, risk_flags_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                market_id,
                f"cond_{market_id}",
                "Seattle",
                "KSEA",
                47.61,
                -122.33,
                "America/Los_Angeles",
                "2026-03-20",
                "daily_max",
                "temperature_max",
                "fahrenheit",
                50.0,
                59.0,
                "weather.com",
                "[]",
                "identity",
                True,
                "spec_v1",
                0.9,
                "[]",
                "2026-03-19 00:00:00",
                "2026-03-19 00:00:00",
            ],
        )

    def _seed_policy(self, con) -> None:
        con.execute(
            """
            INSERT INTO trading.allocation_policies VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "policy_exact",
                "wallet_weather_1",
                "weather_primary",
                "active",
                "alloc_v1",
                7.0,
                10.0,
                1.0,
                1.0,
                "2026-03-19 00:00:00",
                "2026-03-19 00:00:00",
            ],
        )
        con.execute(
            """
            INSERT INTO trading.inventory_positions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "wallet_weather_1",
                "usdc_e",
                "usdc_e",
                "cash",
                "cash",
                "available",
                Decimal("100"),
                "0xfunder",
                1,
                "2026-03-19 00:00:00",
            ],
        )

    def _decision(
        self,
        *,
        decision_id: str,
        market_id: str,
        token_id: str,
        rank: int,
        ranking_score: float,
        expected_dollar_pnl: float,
        size: str,
    ) -> StrategyDecision:
        return StrategyDecision(
            decision_id=decision_id,
            run_id="run_weather_1",
            decision_rank=rank,
            strategy_id="weather_primary",
            strategy_version="v2",
            market_id=market_id,
            token_id=token_id,
            outcome="YES",
            side="buy",
            signal_ts_ms=1710000000000 + rank,
            reference_price=Decimal("1.0"),
            fair_value=Decimal("0.65"),
            edge_bps=900,
            threshold_bps=500,
            route_action=RouteAction.FAK,
            size=Decimal(size),
            forecast_run_id="frun_weather_1",
            watch_snapshot_id=f"snap_{decision_id}",
            pricing_context_json={
                "ranking_score": ranking_score,
                "expected_dollar_pnl": expected_dollar_pnl,
            },
        )

    def test_allocator_paper_path_and_action_queue_share_same_reranked_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "rerank_surface.duckdb")
            self._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                self._insert_market_spec(con, market_id="mkt_base_high")
                self._insert_market_spec(con, market_id="mkt_deployable")
                self._seed_policy(con)
                source_decisions = [
                    self._decision(
                        decision_id="dec_base_high",
                        market_id="mkt_base_high",
                        token_id="tok_high",
                        rank=1,
                        ranking_score=0.95,
                        expected_dollar_pnl=0.30,
                        size="1",
                    ),
                    self._decision(
                        decision_id="dec_deployable",
                        market_id="mkt_deployable",
                        token_id="tok_deployable",
                        rank=2,
                        ranking_score=0.80,
                        expected_dollar_pnl=0.22,
                        size="6",
                    ),
                ]
                _, allocation_decisions, _ = materialize_capital_allocation(
                    con,
                    decisions=source_decisions,
                    wallet_id="wallet_weather_1",
                    run_id="run_weather_1",
                    source_kind="paper_execution",
                    created_at=datetime(2026, 3, 19, 9, 0, tzinfo=UTC),
                )

                decision_by_id = {item.decision_id: item for item in source_decisions}
                approved_or_resized = [
                    item for item in allocation_decisions if item.allocation_status in {"approved", "resized"} and item.recommended_size > 0.0
                ]
                tickets = [
                    build_trade_ticket(
                        decision_by_id[item.decision_id],
                        created_at=datetime(2026, 3, 19, 9, 5, tzinfo=UTC),
                        size_override=Decimal(str(item.recommended_size)),
                        allocation_context={
                            "requested_size": item.requested_size,
                            "recommended_size": item.recommended_size,
                            "allocation_status": item.allocation_status,
                            "allocation_decision_id": item.allocation_decision_id,
                            "allocation_reason_codes": list(item.reason_codes),
                            "budget_impact": dict(item.budget_impact),
                            "base_ranking_score": item.base_ranking_score,
                            "deployable_expected_pnl": item.deployable_expected_pnl,
                            "deployable_notional": item.deployable_notional,
                            "max_deployable_size": item.max_deployable_size,
                            "capital_scarcity_penalty": item.capital_scarcity_penalty,
                            "concentration_penalty": item.concentration_penalty,
                            "pre_budget_deployable_size": item.pre_budget_deployable_size,
                            "pre_budget_deployable_notional": item.pre_budget_deployable_notional,
                            "pre_budget_deployable_expected_pnl": item.pre_budget_deployable_expected_pnl,
                            "rerank_position": item.rerank_position,
                            "rerank_reason_codes": list(item.rerank_reason_codes),
                            "binding_limit_scope": item.binding_limit_scope,
                            "binding_limit_key": item.binding_limit_key,
                        },
                    )
                    for item in approved_or_resized
                ]

                con.execute("ATTACH ':memory:' AS src")
                con.execute("CREATE SCHEMA ui")
                con.execute("CREATE SCHEMA src.runtime")
                con.execute(
                    """
                    CREATE TABLE src.runtime.allocation_decisions AS
                    SELECT * FROM runtime.allocation_decisions WHERE 1=0
                    """
                )
                for item in allocation_decisions:
                    con.execute(
                        f"""
                        INSERT INTO src.runtime.allocation_decisions ({", ".join(RUNTIME_ALLOCATION_DECISION_COLUMNS)})
                        VALUES ({", ".join(["?"] * len(RUNTIME_ALLOCATION_DECISION_COLUMNS))})
                        """,
                        allocation_decision_to_row(item),
                    )

                con.execute(
                    """
                    CREATE TABLE ui.market_opportunity_summary(
                        market_id TEXT,
                        location_name TEXT,
                        question TEXT,
                        best_side TEXT,
                        ranking_score DOUBLE,
                        base_ranking_score DOUBLE,
                        expected_dollar_pnl DOUBLE,
                        deployable_expected_pnl DOUBLE,
                        deployable_notional DOUBLE,
                        max_deployable_size DOUBLE,
                        pre_budget_deployable_size DOUBLE,
                        pre_budget_deployable_notional DOUBLE,
                        pre_budget_deployable_expected_pnl DOUBLE,
                        preview_binding_limit_scope TEXT,
                        preview_binding_limit_key TEXT,
                        requested_size DOUBLE,
                        requested_notional DOUBLE,
                        recommended_size DOUBLE,
                        allocation_status TEXT,
                        actionability_status TEXT,
                        agent_review_status TEXT,
                        feedback_status TEXT,
                        feedback_penalty DOUBLE,
                        calibration_freshness_status TEXT,
                        market_quality_status TEXT,
                        source_freshness_status TEXT,
                        source_badge TEXT,
                        source_truth_status TEXT,
                        live_prereq_status TEXT,
                        signal_created_at TIMESTAMP,
                        primary_score_label TEXT
                    )
                    """
                )
                for item in allocation_decisions:
                    preview = (item.budget_impact or {}).get("preview") if isinstance(item.budget_impact, dict) else {}
                    if not isinstance(preview, dict):
                        preview = {}
                    con.execute(
                        """
                        INSERT INTO ui.market_opportunity_summary VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            item.market_id,
                            "Seattle",
                            f"Question for {item.market_id}",
                            "BUY",
                            item.ranking_score,
                            item.base_ranking_score,
                            item.base_ranking_score,
                            item.deployable_expected_pnl,
                            item.deployable_notional,
                            item.max_deployable_size,
                            item.pre_budget_deployable_size,
                            item.pre_budget_deployable_notional,
                            item.pre_budget_deployable_expected_pnl,
                            preview.get("preview_binding_limit_scope"),
                            preview.get("preview_binding_limit_key"),
                            item.requested_size,
                            item.requested_notional,
                            item.recommended_size,
                            item.allocation_status,
                            "actionable",
                            "passed",
                            "healthy",
                            0.0,
                            "fresh",
                            "pass",
                            "fresh",
                            "canonical",
                            "canonical",
                            "ready",
                            "2026-03-19 09:00:00",
                            "ranking_score",
                        ],
                    )

                counts: dict[str, int] = {}
                _create_action_queue_summary(con, table_row_counts=counts)
                queue_rows = con.execute(
                    """
                    SELECT market_id, ranking_score, recommended_size
                    FROM ui.action_queue_summary
                    ORDER BY queue_priority, ranking_score DESC, deployable_expected_pnl DESC
                    """
                ).fetchall()
            finally:
                con.close()

        allocator_order = [item.market_id for item in allocation_decisions if item.allocation_status in {"approved", "resized"} and item.recommended_size > 0.0]
        paper_order = [ticket.market_id for ticket in tickets]
        queue_order = [row[0] for row in queue_rows]
        self.assertEqual(counts["ui.action_queue_summary"], 2)
        self.assertEqual(allocator_order, ["mkt_deployable", "mkt_base_high"])
        self.assertEqual(paper_order, allocator_order)
        self.assertEqual(queue_order, allocator_order)
        self.assertEqual(float(queue_rows[0][2]), float(tickets[0].size))
        self.assertEqual(float(queue_rows[1][2]), float(tickets[1].size))


if __name__ == "__main__":
    unittest.main()
