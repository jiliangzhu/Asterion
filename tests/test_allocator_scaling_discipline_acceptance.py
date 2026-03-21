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
class AllocatorScalingDisciplineAcceptanceTest(unittest.TestCase):
    def _apply_migrations(self, db_path: str) -> None:
        con = duckdb.connect(db_path)
        try:
            migrations_dir = Path(__file__).resolve().parents[1] / "sql" / "migrations"
            for path in sorted(migrations_dir.glob("*.sql")):
                sql = path.read_text(encoding="utf-8").strip()
                if sql:
                    con.execute(sql)
        finally:
            con.close()

    def _decision(self, *, market_id: str, decision_id: str, gate: str, regime_bucket: str) -> StrategyDecision:
        return StrategyDecision(
            decision_id=decision_id,
            run_id="run_weather_1",
            decision_rank=1,
            strategy_id="weather_primary",
            strategy_version="v2",
            market_id=market_id,
            token_id=f"tok_{market_id}",
            outcome="YES",
            side="buy",
            signal_ts_ms=1710000000000,
            reference_price=Decimal("1.0"),
            fair_value=Decimal("0.65"),
            edge_bps=900,
            threshold_bps=500,
            route_action=RouteAction.FAK,
            size=Decimal("5"),
            forecast_run_id="frun_weather_1",
            watch_snapshot_id=f"snap_{decision_id}",
            pricing_context_json={
                "ranking_score": 0.9,
                "regime_bucket": regime_bucket,
                "calibration_gate_status": gate,
            },
        )

    def test_review_required_gate_fail_closes_allocation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "alloc_p8.duckdb")
            self._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                con.execute(
                    """
                    INSERT INTO weather.weather_market_specs (
                        market_id, condition_id, location_name, station_id, latitude, longitude, timezone,
                        observation_date, observation_window_local, metric, unit, bucket_min_value, bucket_max_value,
                        authoritative_source, fallback_sources, rounding_rule, inclusive_bounds, spec_version,
                        parse_confidence, risk_flags_json, created_at, updated_at
                    ) VALUES (
                        'mkt_a', 'cond_mkt_a', 'Seattle', 'KSEA', 47.61, -122.33, 'America/Los_Angeles',
                        '2026-03-20', 'daily_max', 'temperature_max', 'fahrenheit', 50.0, 59.0,
                        'weather.com', '[]', 'identity', TRUE, 'spec_v1', 0.9, '[]', '2026-03-20 00:00:00', '2026-03-20 00:00:00'
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO trading.allocation_policies VALUES
                    ('policy_exact', 'wallet_weather_1', 'weather_primary', 'active', 'alloc_v1', 10.0, 10.0, 1.0, 1.0, '2026-03-20 00:00:00', '2026-03-20 00:00:00')
                    """
                )
                con.execute(
                    """
                    INSERT INTO trading.capital_budget_policies VALUES
                    ('cap_review', 'wallet_weather_1', 'weather_primary', 'warm', 'review_required', 'active', 'cap_v1', 5.0, 2.0, 2, 1, 1.0, '2026-03-20 00:00:00', '2026-03-20 00:00:00')
                    """
                )
                con.execute(
                    """
                    INSERT INTO trading.inventory_positions VALUES
                    ('wallet_weather_1', 'usdc_e', 'usdc_e', 'cash', 'cash', 'available', 100.0, '0xfunder', 1, '2026-03-20 00:00:00')
                    """
                )
                _, decisions, _ = materialize_capital_allocation(
                    con,
                    decisions=[self._decision(market_id="mkt_a", decision_id="dec_review", gate="review_required", regime_bucket="warm")],
                    wallet_id="wallet_weather_1",
                    run_id="run_weather_1",
                    source_kind="allocation_preview",
                    created_at=datetime(2026, 3, 20, 9, 0, tzinfo=UTC),
                )
            finally:
                con.close()

        self.assertEqual(len(decisions), 1)
        decision = decisions[0]
        self.assertEqual(decision.allocation_status, "blocked")
        self.assertEqual(decision.recommended_size, 0.0)
        self.assertEqual(decision.calibration_gate_status, "review_required")
        self.assertEqual(decision.capital_policy_id, "cap_review")
        self.assertIn("calibration_gate_review_required", decision.capital_scaling_reason_codes)


if __name__ == "__main__":
    unittest.main()
