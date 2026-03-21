from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import duckdb

from asterion_core.contracts import BalanceType, InventoryPosition, RouteAction, StrategyDecision
from asterion_core.risk import materialize_capital_allocation


class AllocatorV1Phase6Test(unittest.TestCase):
    def _apply_migrations(self, db_path: str) -> None:
        bootstrap = duckdb.connect(db_path)
        migrations_dir = Path(__file__).resolve().parents[1] / "sql" / "migrations"
        for path in sorted(migrations_dir.glob("*.sql")):
            sql = path.read_text(encoding="utf-8").strip()
            if sql:
                bootstrap.execute(sql)
        bootstrap.close()

    def _insert_market_spec(self, con, *, market_id: str, station_id: str = "KSEA") -> None:
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
        policy_id: str = "policy_exact",
        wallet_id: str = "wallet_weather_1",
        strategy_id: str = "weather_primary",
        max_buy_notional_per_run: float = 20.0,
        max_buy_notional_per_ticket: float = 20.0,
        min_recommended_size: float = 1.0,
        size_rounding_increment: float = 1.0,
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

    def _decision(
        self,
        *,
        decision_id: str,
        rank: int,
        market_id: str,
        token_id: str,
        side: str = "buy",
        size: str = "5",
        ranking_score: float = 0.9,
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
            side=side,
            signal_ts_ms=1_710_000_000_000 + rank,
            reference_price=Decimal("1.0"),
            fair_value=Decimal("0.65"),
            edge_bps=900,
            threshold_bps=500,
            route_action=RouteAction.FAK,
            size=Decimal(size),
            forecast_run_id="frun_weather_1",
            watch_snapshot_id=f"snap_{decision_id}",
            pricing_context_json={"ranking_score": ranking_score, "expected_dollar_pnl": ranking_score},
        )

    def test_allocator_self_sort_is_stable_under_shuffled_input(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "alloc_phase6.duckdb")
            self._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                self._insert_market_spec(con, market_id="mkt_a")
                self._insert_market_spec(con, market_id="mkt_b")
                self._insert_policy(con, max_buy_notional_per_run=7.0)
                con.execute(
                    "INSERT INTO trading.inventory_positions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    ["wallet_weather_1", "usdc_e", "usdc_e", "cash", "cash", "available", Decimal("100"), "0xfunder", 1, "2026-03-19 00:00:00"],
                )
                ordered = [
                    self._decision(decision_id="dec_high", rank=1, market_id="mkt_a", token_id="tok_a", ranking_score=0.9),
                    self._decision(decision_id="dec_low", rank=2, market_id="mkt_b", token_id="tok_b", ranking_score=0.4),
                ]
                shuffled = [ordered[1], ordered[0]]
                _, ordered_decisions, _ = materialize_capital_allocation(
                    con,
                    decisions=ordered,
                    wallet_id="wallet_weather_1",
                    run_id="run_weather_1",
                    source_kind="allocation_preview",
                    created_at=datetime(2026, 3, 19, 9, 0, tzinfo=UTC),
                )
                _, shuffled_decisions, _ = materialize_capital_allocation(
                    con,
                    decisions=shuffled,
                    wallet_id="wallet_weather_1",
                    run_id="run_weather_2",
                    source_kind="allocation_preview",
                    created_at=datetime(2026, 3, 19, 9, 5, tzinfo=UTC),
                )
            finally:
                con.close()

        ordered_map = {item.decision_id: item for item in ordered_decisions}
        shuffled_map = {item.decision_id: item for item in shuffled_decisions}
        self.assertEqual([item.decision_id for item in ordered_decisions], ["dec_high", "dec_low"])
        self.assertEqual([item.decision_id for item in shuffled_decisions], ["dec_high", "dec_low"])
        self.assertEqual(ordered_map["dec_low"].recommended_size, shuffled_map["dec_low"].recommended_size)
        self.assertEqual(ordered_map["dec_low"].binding_limit_scope, "run_budget")
        self.assertEqual(shuffled_map["dec_low"].binding_limit_scope, "run_budget")

    def test_allocator_fails_fast_on_duplicate_decision_rank_or_id(self) -> None:
        decision_a = self._decision(decision_id="dup", rank=1, market_id="mkt_a", token_id="tok_a")
        decision_b = self._decision(decision_id="dup", rank=2, market_id="mkt_b", token_id="tok_b")
        with self.assertRaisesRegex(ValueError, "duplicate decision_id"):
            materialize_capital_allocation(
                duckdb.connect(":memory:"),
                decisions=[decision_a, decision_b],
                wallet_id="wallet_weather_1",
                run_id="run_weather_1",
                source_kind="allocation_preview",
            )
        decision_c = self._decision(decision_id="dec_a", rank=1, market_id="mkt_a", token_id="tok_a")
        decision_d = self._decision(decision_id="dec_b", rank=1, market_id="mkt_b", token_id="tok_b")
        with self.assertRaisesRegex(ValueError, "duplicate decision_rank"):
            materialize_capital_allocation(
                duckdb.connect(":memory:"),
                decisions=[decision_c, decision_d],
                wallet_id="wallet_weather_1",
                run_id="run_weather_1",
                source_kind="allocation_preview",
            )

    def test_binding_limit_scope_covers_per_ticket_and_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "alloc_phase6_limits.duckdb")
            self._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                self._insert_market_spec(con, market_id="mkt_buy")
                self._insert_market_spec(con, market_id="mkt_sell")
                self._insert_policy(
                    con,
                    max_buy_notional_per_run=20.0,
                    max_buy_notional_per_ticket=2.0,
                    min_recommended_size=1.0,
                )
                inventory_positions = [
                    InventoryPosition(
                        wallet_id="wallet_weather_1",
                        asset_type="usdc_e",
                        token_id=None,
                        market_id=None,
                        outcome=None,
                        balance_type=BalanceType.AVAILABLE,
                        quantity=Decimal("100"),
                        funder="0xfunder",
                        signature_type=1,
                        updated_at=datetime(2026, 3, 19, 9, 0, tzinfo=UTC),
                    ),
                    InventoryPosition(
                        wallet_id="wallet_weather_1",
                        asset_type="outcome_token",
                        token_id="tok_sell",
                        market_id="mkt_sell",
                        outcome="YES",
                        balance_type=BalanceType.AVAILABLE,
                        quantity=Decimal("2"),
                        funder="0xfunder",
                        signature_type=1,
                        updated_at=datetime(2026, 3, 19, 9, 0, tzinfo=UTC),
                    ),
                ]
                _, decisions, _ = materialize_capital_allocation(
                    con,
                    decisions=[
                        self._decision(decision_id="dec_buy", rank=1, market_id="mkt_buy", token_id="tok_buy", side="buy", size="5"),
                        self._decision(decision_id="dec_sell", rank=2, market_id="mkt_sell", token_id="tok_sell", side="sell", size="5"),
                    ],
                    wallet_id="wallet_weather_1",
                    run_id="run_weather_1",
                    source_kind="allocation_preview",
                    current_inventory_positions=inventory_positions,
                    created_at=datetime(2026, 3, 19, 9, 0, tzinfo=UTC),
                )
            finally:
                con.close()

        by_id = {item.decision_id: item for item in decisions}
        self.assertEqual(by_id["dec_buy"].binding_limit_scope, "per_ticket")
        self.assertEqual(by_id["dec_buy"].binding_limit_key, "policy_exact")
        self.assertEqual(by_id["dec_buy"].recommended_size, 2.0)
        self.assertEqual(by_id["dec_sell"].binding_limit_scope, "inventory")
        self.assertEqual(by_id["dec_sell"].binding_limit_key, "tok_sell")
        self.assertEqual(by_id["dec_sell"].recommended_size, 2.0)


if __name__ == "__main__":
    unittest.main()
