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
class AllocatorV2RerankTest(unittest.TestCase):
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
                6.0,
                10.0,
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

    def test_allocator_pass1_reranks_by_pre_budget_deployable_pnl(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "alloc_rerank.duckdb")
            self._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                self._insert_market_spec(con, market_id="mkt_base_high", station_id="KSEA")
                self._insert_market_spec(con, market_id="mkt_deployable", station_id="KSEA")
                self._insert_policy(con)
                self._insert_cash_inventory(con)
                _, decisions, _ = materialize_capital_allocation(
                    con,
                    decisions=[
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
                            token_id="tok_low",
                            rank=2,
                            ranking_score=0.80,
                            expected_dollar_pnl=0.22,
                            size="6",
                        ),
                    ],
                    wallet_id="wallet_weather_1",
                    run_id="run_weather_1",
                    source_kind="allocation_preview",
                    created_at=datetime(2026, 3, 19, 9, 0, tzinfo=UTC),
                )
            finally:
                con.close()

        self.assertEqual([item.decision_id for item in decisions], ["dec_deployable", "dec_base_high"])
        by_id = {item.decision_id: item for item in decisions}
        self.assertEqual(by_id["dec_deployable"].rerank_position, 1)
        self.assertEqual(by_id["dec_base_high"].rerank_position, 2)
        self.assertAlmostEqual(by_id["dec_deployable"].pre_budget_deployable_expected_pnl, 1.32)
        self.assertAlmostEqual(by_id["dec_base_high"].pre_budget_deployable_expected_pnl, 0.30)
        self.assertEqual(by_id["dec_deployable"].allocation_status, "approved")
        self.assertEqual(by_id["dec_base_high"].allocation_status, "blocked")
        self.assertIn("buy_budget_exhausted", by_id["dec_base_high"].reason_codes)
        self.assertIn("reranked_vs_base_order", by_id["dec_deployable"].rerank_reason_codes)


if __name__ == "__main__":
    unittest.main()
