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
class AllocatorV1P10SchedulingTest(unittest.TestCase):
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

    def _insert_policy(self, con) -> None:
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
                30.0,
                20.0,
                1.0,
                1.0,
                "2026-03-19 00:00:00",
                "2026-03-19 00:00:00",
            ],
        )

    def _insert_cash_inventory(self, con) -> None:
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
        quality_confidence_multiplier: float,
        execution_intelligence_score: float,
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
            size=Decimal("10"),
            forecast_run_id="frun_weather_1",
            watch_snapshot_id=f"snap_{decision_id}",
            pricing_context_json={
                "ranking_score": ranking_score,
                "expected_dollar_pnl": expected_dollar_pnl,
                "quality_confidence_multiplier": quality_confidence_multiplier,
                "execution_intelligence_score": execution_intelligence_score,
            },
        )

    def test_uncertain_and_microstructure_weak_ticket_is_sized_down(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "alloc_p10_acceptance.duckdb")
            self._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                self._insert_market_spec(con, market_id="mkt_stable", station_id="KSEA")
                self._insert_market_spec(con, market_id="mkt_noisy", station_id="KBOS")
                self._insert_policy(con)
                self._insert_cash_inventory(con)
                _, decisions, _ = materialize_capital_allocation(
                    con,
                    decisions=[
                        self._decision(
                            decision_id="dec_noisy",
                            market_id="mkt_noisy",
                            token_id="tok_noisy",
                            rank=1,
                            ranking_score=1.05,
                            expected_dollar_pnl=1.05,
                            quality_confidence_multiplier=0.45,
                            execution_intelligence_score=0.20,
                        ),
                        self._decision(
                            decision_id="dec_stable",
                            market_id="mkt_stable",
                            token_id="tok_stable",
                            rank=2,
                            ranking_score=1.00,
                            expected_dollar_pnl=1.00,
                            quality_confidence_multiplier=0.95,
                            execution_intelligence_score=0.95,
                        ),
                    ],
                    wallet_id="wallet_weather_1",
                    run_id="run_weather_1",
                    source_kind="allocation_preview",
                    created_at=datetime(2026, 3, 19, 9, 0, tzinfo=UTC),
                )
            finally:
                con.close()

        by_id = {item.decision_id: item for item in decisions}
        noisy = by_id["dec_noisy"]
        stable = by_id["dec_stable"]

        self.assertLess(noisy.pre_budget_deployable_size, stable.pre_budget_deployable_size)
        self.assertLess(noisy.recommended_size, stable.recommended_size)
        self.assertLess(noisy.ranking_score, stable.ranking_score)
        self.assertIn("uncertainty_sizing_tighten", noisy.capital_scaling_reason_codes)
        self.assertIn("execution_intelligence_tighten", noisy.capital_scaling_reason_codes)
        self.assertGreater(noisy.budget_impact["sizing"]["uncertainty_sizing_penalty"], 0.0)
        self.assertGreater(noisy.budget_impact["sizing"]["execution_intelligence_penalty"], 0.0)


if __name__ == "__main__":
    unittest.main()
