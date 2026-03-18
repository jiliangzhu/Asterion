from __future__ import annotations

import importlib.util
import tempfile
import unittest
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import duckdb

from asterion_core.contracts import RouteAction, StrategyDecision
from asterion_core.risk import materialize_capital_allocation


HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required")
class AllocatorV1Test(unittest.TestCase):
    def _apply_migrations(self, db_path: str) -> None:
        bootstrap = duckdb.connect(db_path)
        migrations_dir = Path(__file__).resolve().parents[1] / "sql" / "migrations"
        for path in sorted(migrations_dir.glob("*.sql")):
            sql = path.read_text(encoding="utf-8").strip()
            if sql:
                bootstrap.execute(sql)
        bootstrap.close()

    def _insert_market_spec(self, con, *, market_id: str, station_id: str) -> None:
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
                station_id,
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

    def _insert_policy(
        self,
        con,
        *,
        policy_id: str,
        wallet_id: str,
        strategy_id: str | None,
        max_buy_notional_per_run: float,
        max_buy_notional_per_ticket: float,
        min_recommended_size: float,
        size_rounding_increment: float,
    ) -> None:
        con.execute(
            """
            INSERT INTO trading.allocation_policies VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                policy_id,
                wallet_id,
                strategy_id,
                "active",
                "alloc_v1",
                max_buy_notional_per_run,
                max_buy_notional_per_ticket,
                min_recommended_size,
                size_rounding_increment,
                "2026-03-19 00:00:00",
                "2026-03-19 00:00:00",
            ],
        )

    def _insert_limit(
        self,
        con,
        *,
        limit_id: str,
        policy_id: str,
        wallet_id: str,
        scope: str,
        scope_key: str,
        max_gross_notional: float | None,
        max_position_quantity: float | None,
    ) -> None:
        con.execute(
            """
            INSERT INTO trading.position_limit_policies VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                limit_id,
                policy_id,
                wallet_id,
                scope,
                scope_key,
                max_gross_notional,
                max_position_quantity,
                "active",
                "2026-03-19 00:00:00",
                "2026-03-19 00:00:00",
            ],
        )

    def _insert_cash_inventory(self, con, *, wallet_id: str, quantity: str) -> None:
        con.execute(
            """
            INSERT INTO trading.inventory_positions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                wallet_id,
                "usdc_e",
                "usdc_e",
                "cash",
                "cash",
                "available",
                Decimal(quantity),
                "0xfunder",
                1,
                "2026-03-19 00:00:00",
            ],
        )

    def _decision(
        self,
        *,
        decision_id: str,
        strategy_id: str,
        market_id: str,
        token_id: str,
        outcome: str,
        side: str,
        rank: int,
        ranking_score: float,
        size: str,
        reference_price: str = "1.0",
    ) -> StrategyDecision:
        return StrategyDecision(
            decision_id=decision_id,
            run_id="run_weather_1",
            decision_rank=rank,
            strategy_id=strategy_id,
            strategy_version="v2",
            market_id=market_id,
            token_id=token_id,
            outcome=outcome,
            side=side,
            signal_ts_ms=1710000000000 + rank,
            reference_price=Decimal(reference_price),
            fair_value=Decimal("0.65"),
            edge_bps=900,
            threshold_bps=500,
            route_action=RouteAction.FAK,
            size=Decimal(size),
            forecast_run_id="frun_weather_1",
            watch_snapshot_id=f"snap_{decision_id}",
            pricing_context_json={"ranking_score": ranking_score},
        )

    def test_allocator_consumes_run_budget_in_ranking_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "alloc.duckdb")
            self._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                self._insert_market_spec(con, market_id="mkt_a", station_id="KSEA")
                self._insert_market_spec(con, market_id="mkt_b", station_id="KSEA")
                self._insert_policy(
                    con,
                    policy_id="policy_exact",
                    wallet_id="wallet_weather_1",
                    strategy_id="weather_primary",
                    max_buy_notional_per_run=7.0,
                    max_buy_notional_per_ticket=10.0,
                    min_recommended_size=1.0,
                    size_rounding_increment=1.0,
                )
                self._insert_cash_inventory(con, wallet_id="wallet_weather_1", quantity="100")
                run, decisions, checks = materialize_capital_allocation(
                    con,
                    decisions=[
                        self._decision(
                            decision_id="dec_high",
                            strategy_id="weather_primary",
                            market_id="mkt_a",
                            token_id="tok_yes_a",
                            outcome="YES",
                            side="buy",
                            rank=1,
                            ranking_score=0.9,
                            size="5",
                        ),
                        self._decision(
                            decision_id="dec_low",
                            strategy_id="weather_primary",
                            market_id="mkt_b",
                            token_id="tok_yes_b",
                            outcome="YES",
                            side="buy",
                            rank=2,
                            ranking_score=0.4,
                            size="5",
                        ),
                    ],
                    wallet_id="wallet_weather_1",
                    run_id="run_weather_1",
                    source_kind="allocation_preview",
                    created_at=datetime(2026, 3, 19, 9, 0, tzinfo=UTC),
                )
            finally:
                con.close()

        self.assertEqual(run.approved_count, 1)
        self.assertEqual(run.resized_count, 1)
        self.assertEqual(run.blocked_count, 0)
        self.assertEqual(run.policy_missing_count, 0)
        self.assertEqual(len(checks), 0)
        by_id = {item.decision_id: item for item in decisions}
        self.assertEqual(by_id["dec_high"].allocation_status, "approved")
        self.assertEqual(by_id["dec_high"].recommended_size, 5.0)
        self.assertEqual(by_id["dec_low"].allocation_status, "resized")
        self.assertEqual(by_id["dec_low"].recommended_size, 2.0)
        self.assertIn("buy_budget_exhausted", by_id["dec_low"].reason_codes)

    def test_allocator_respects_station_limit_and_blocks_below_min_size(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "alloc.duckdb")
            self._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                self._insert_market_spec(con, market_id="mkt_station", station_id="KSEA")
                self._insert_policy(
                    con,
                    policy_id="policy_station",
                    wallet_id="wallet_weather_1",
                    strategy_id="weather_primary",
                    max_buy_notional_per_run=20.0,
                    max_buy_notional_per_ticket=20.0,
                    min_recommended_size=3.0,
                    size_rounding_increment=1.0,
                )
                self._insert_limit(
                    con,
                    limit_id="limit_station",
                    policy_id="policy_station",
                    wallet_id="wallet_weather_1",
                    scope="station",
                    scope_key="KSEA",
                    max_gross_notional=2.0,
                    max_position_quantity=None,
                )
                self._insert_cash_inventory(con, wallet_id="wallet_weather_1", quantity="100")
                run, decisions, checks = materialize_capital_allocation(
                    con,
                    decisions=[
                        self._decision(
                            decision_id="dec_station",
                            strategy_id="weather_primary",
                            market_id="mkt_station",
                            token_id="tok_station",
                            outcome="YES",
                            side="buy",
                            rank=1,
                            ranking_score=0.8,
                            size="5",
                        )
                    ],
                    wallet_id="wallet_weather_1",
                    run_id="run_weather_1",
                    source_kind="allocation_preview",
                    created_at=datetime(2026, 3, 19, 9, 0, tzinfo=UTC),
                )
            finally:
                con.close()

        self.assertEqual(run.blocked_count, 1)
        self.assertEqual(len(decisions), 1)
        self.assertEqual(len(checks), 1)
        decision = decisions[0]
        self.assertEqual(decision.allocation_status, "blocked")
        self.assertEqual(decision.recommended_size, 0.0)
        self.assertIn("station_limit_exceeded", decision.reason_codes)
        self.assertIn("below_min_recommended_size", decision.reason_codes)
        self.assertEqual(decision.budget_impact["binding_limit_scope"], "station")
        self.assertEqual(checks[0].check_status, "fail")


if __name__ == "__main__":
    unittest.main()
