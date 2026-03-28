from __future__ import annotations

import importlib.util
import json
import os
import tempfile
import unittest
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import duckdb

from asterion_core.storage.database import DuckDBConfig, connect_duckdb
from asterion_core.storage.db_migrate import MigrationConfig, apply_migrations
from asterion_core.storage.write_queue import WriteQueueConfig
from asterion_core.storage.writerd import process_one
from dagster_asterion.handlers import run_weather_paper_execution_job


HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required")
class PaperExecutionAllocatorIntegrationTest(unittest.TestCase):
    def _setup_db(self, db_path: str) -> None:
        duckdb.connect(db_path).close()
        with patch.dict(
            os.environ,
            {
                "ASTERION_DB_ROLE": "writer",
                "ASTERION_STRICT_SINGLE_WRITER": "1",
                "WRITERD": "1",
            },
            clear=False,
        ):
            apply_migrations(
                MigrationConfig(
                    db_path=db_path,
                    migrations_dir=str(Path(__file__).resolve().parents[1] / "sql" / "migrations"),
                )
            )

    def _seed_common(
        self,
        con,
        *,
        include_policy: bool,
        max_buy_notional_per_run: float | None = None,
        reference_price: float = 0.40,
        min_order_size: float = 1.0,
    ) -> None:
        con.execute(
            """
            INSERT INTO capability.market_capabilities (
                token_id, market_id, condition_id, outcome, tick_size, fee_rate_bps, neg_risk,
                min_order_size, tradable, fees_enabled, data_sources, updated_at
            ) VALUES (
                'tok_yes', 'mkt_weather_1', 'cond_weather_1', 'YES', 0.01, 30, FALSE, ?, TRUE, TRUE,
                '["gamma","clob_public"]', '2026-03-10 10:00:00'
            )
            """,
            [min_order_size],
        )
        con.execute(
            """
            INSERT INTO capability.account_trading_capabilities VALUES (
                'wallet_weather_1', 'eoa', 1, '0xfunder', '["0xrelayer"]', TRUE, TRUE, NULL, '2026-03-10 10:00:00'
            )
            """
        )
        con.execute(
            """
            INSERT INTO weather.weather_watch_only_snapshots (
                snapshot_id, fair_value_id, run_id, market_id, condition_id, token_id, outcome,
                reference_price, fair_value, edge_bps, threshold_bps, decision, side, rationale,
                pricing_context_json, created_at
            ) VALUES (
                'snap_yes', 'fv_yes', 'frun_weather_1', 'mkt_weather_1', 'cond_weather_1', 'tok_yes', 'YES',
                ?, 0.60, 800, 500, 'TAKE', 'BUY', 'allocator test', '{"ranking_score": 0.9}', '2026-03-10 10:00:00'
            )
            """,
            [reference_price],
        )
        con.execute(
            """
            INSERT INTO weather.weather_market_specs (
                market_id, condition_id, location_name, station_id, latitude, longitude, timezone,
                observation_date, observation_window_local, metric, unit, bucket_min_value, bucket_max_value,
                authoritative_source, fallback_sources, rounding_rule, inclusive_bounds, spec_version,
                parse_confidence, risk_flags_json, created_at, updated_at
            ) VALUES (
                'mkt_weather_1', 'cond_weather_1', 'Seattle', 'KSEA', 47.61, -122.33, 'America/Los_Angeles',
                '2026-03-10', 'daily_max', 'temperature_max', 'fahrenheit', 50.0, 59.0,
                'weather.com', '[]', 'identity', TRUE, 'spec_v1', 0.9, '[]', '2026-03-10 00:00:00', '2026-03-10 00:00:00'
            )
            """
        )
        con.execute(
            """
            INSERT INTO trading.inventory_positions VALUES
                ('wallet_weather_1', 'usdc_e', 'usdc_e', 'cash', 'cash', 'available', 100.0, '0xfunder', 1, '2026-03-10 10:00:00')
            """
        )
        if include_policy:
            con.execute(
                """
                INSERT INTO trading.allocation_policies VALUES (
                    'policy_exact', 'wallet_weather_1', 'weather_primary', 'active', 'alloc_v1', ?, ?, 1.0, 1.0,
                    '2026-03-10 00:00:00', '2026-03-10 00:00:00'
                )
                """,
                [max_buy_notional_per_run, max_buy_notional_per_run],
            )

    def _drain_queue(self, *, db_path: str, queue_path: str) -> None:
        allow_tables = ",".join(
            [
                "runtime.strategy_runs",
                "runtime.capital_allocation_runs",
                "runtime.allocation_decisions",
                "runtime.position_limit_checks",
                "runtime.trade_tickets",
                "runtime.gate_decisions",
                "trading.orders",
                "trading.reservations",
                "trading.fills",
                "trading.inventory_positions",
                "trading.order_state_transitions",
                "trading.exposure_snapshots",
                "trading.reconciliation_results",
                "capability.execution_contexts",
                "runtime.journal_events",
            ]
        )
        with patch.dict(
            os.environ,
            {
                "ASTERION_DB_PATH": db_path,
                "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables,
            },
            clear=False,
        ):
            while process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False):
                pass

    def test_paper_execution_consumes_resized_recommended_size(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "paper_alloc.duckdb")
            queue_path = str(Path(tmpdir) / "write_queue.sqlite")
            self._setup_db(db_path)
            seed_con = duckdb.connect(db_path)
            try:
                self._seed_common(seed_con, include_policy=True, max_buy_notional_per_run=3.2)
            finally:
                seed_con.close()
            reader_env = {
                "ASTERION_STRICT_SINGLE_WRITER": "1",
                "ASTERION_DB_ROLE": "reader",
                "WRITERD": "0",
            }
            with patch.dict("os.environ", reader_env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    result = run_weather_paper_execution_job(
                        con,
                        WriteQueueConfig(path=queue_path),
                        params_json={
                            "wallet_id": "wallet_weather_1",
                            "strategy_registrations": [
                                {
                                    "strategy_id": "weather_primary",
                                    "strategy_version": "v1",
                                    "priority": 1,
                                    "route_action": "FAK",
                                    "size": "10",
                                    "min_edge_bps": 500,
                                }
                            ],
                            "snapshot_ids": ["snap_yes"],
                        },
                        observed_at=datetime(2026, 3, 10, 10, 6, tzinfo=UTC),
                    )
                finally:
                    con.close()

            self._drain_queue(db_path=db_path, queue_path=queue_path)
            with patch.dict("os.environ", reader_env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    ticket_size, provenance_json = con.execute(
                        "SELECT size, provenance_json FROM runtime.trade_tickets"
                    ).fetchone()
                    order_size = con.execute("SELECT size FROM trading.orders").fetchone()[0]
                    allocation_status, recommended_size = con.execute(
                        "SELECT allocation_status, recommended_size FROM runtime.allocation_decisions"
                    ).fetchone()
                finally:
                    con.close()

        provenance = json.loads(provenance_json)
        self.assertEqual(result.metadata["resized_allocation_count"], 1)
        self.assertEqual(float(ticket_size), 8.0)
        self.assertEqual(float(order_size), 8.0)
        self.assertEqual(allocation_status, "resized")
        self.assertEqual(float(recommended_size), 8.0)
        self.assertEqual(provenance["allocation_status"], "resized")
        self.assertEqual(provenance["recommended_size"], "8.0")
        self.assertIn("base_ranking_score", provenance)
        self.assertIn("deployable_expected_pnl", provenance)
        self.assertIn("pre_budget_deployable_size", provenance)
        self.assertIn("pre_budget_deployable_expected_pnl", provenance)
        self.assertIn("preview_binding_limit_scope", provenance)
        self.assertIn("preview_binding_limit_key", provenance)
        self.assertIn("rerank_position", provenance)
        self.assertIn("binding_limit_scope", provenance)
        self.assertIn("capital_policy_id", provenance)
        self.assertIn("capital_policy_version", provenance)
        self.assertIn("capital_scaling_reason_codes", provenance)
        self.assertIn("regime_bucket", provenance)
        self.assertIn("calibration_gate_status", provenance)

    def test_paper_execution_floors_ticket_size_to_market_min_order_size(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "paper_alloc_floor.duckdb")
            queue_path = str(Path(tmpdir) / "write_queue.sqlite")
            self._setup_db(db_path)
            seed_con = duckdb.connect(db_path)
            try:
                self._seed_common(
                    seed_con,
                    include_policy=True,
                    max_buy_notional_per_run=1.6,
                    reference_price=0.40,
                    min_order_size=5.0,
                )
            finally:
                seed_con.close()
            reader_env = {
                "ASTERION_STRICT_SINGLE_WRITER": "1",
                "ASTERION_DB_ROLE": "reader",
                "WRITERD": "0",
            }
            with patch.dict("os.environ", reader_env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    result = run_weather_paper_execution_job(
                        con,
                        WriteQueueConfig(path=queue_path),
                        params_json={
                            "wallet_id": "wallet_weather_1",
                            "strategy_registrations": [
                                {
                                    "strategy_id": "weather_primary",
                                    "strategy_version": "v1",
                                    "priority": 1,
                                    "route_action": "FAK",
                                    "size": "10",
                                    "min_edge_bps": 500,
                                }
                            ],
                            "snapshot_ids": ["snap_yes"],
                        },
                        observed_at=datetime(2026, 3, 10, 10, 6, tzinfo=UTC),
                    )
                finally:
                    con.close()

            self._drain_queue(db_path=db_path, queue_path=queue_path)
            with patch.dict("os.environ", reader_env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    ticket_size, provenance_json = con.execute(
                        "SELECT size, provenance_json FROM runtime.trade_tickets"
                    ).fetchone()
                    order_size = con.execute("SELECT size FROM trading.orders").fetchone()[0]
                    allocation_status, recommended_size = con.execute(
                        "SELECT allocation_status, recommended_size FROM runtime.allocation_decisions"
                    ).fetchone()
                finally:
                    con.close()

        provenance = json.loads(provenance_json)
        self.assertEqual(result.metadata["resized_allocation_count"], 1)
        self.assertEqual(allocation_status, "resized")
        self.assertEqual(float(recommended_size), 4.0)
        self.assertEqual(float(ticket_size), 5.0)
        self.assertEqual(float(order_size), 5.0)
        self.assertTrue(provenance["min_order_size_floor_applied"])
        self.assertEqual(provenance["recommended_size"], "4.0")
        self.assertEqual(provenance["min_order_size"], "5.00000000")

    def test_policy_missing_blocks_order_creation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "paper_alloc.duckdb")
            queue_path = str(Path(tmpdir) / "write_queue.sqlite")
            self._setup_db(db_path)
            seed_con = duckdb.connect(db_path)
            try:
                self._seed_common(seed_con, include_policy=False)
            finally:
                seed_con.close()
            reader_env = {
                "ASTERION_STRICT_SINGLE_WRITER": "1",
                "ASTERION_DB_ROLE": "reader",
                "WRITERD": "0",
            }
            with patch.dict("os.environ", reader_env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    run_weather_paper_execution_job(
                        con,
                        WriteQueueConfig(path=queue_path),
                        params_json={
                            "wallet_id": "wallet_weather_1",
                            "strategy_registrations": [
                                {
                                    "strategy_id": "weather_primary",
                                    "strategy_version": "v1",
                                    "priority": 1,
                                    "route_action": "FAK",
                                    "size": "10",
                                    "min_edge_bps": 500,
                                }
                            ],
                            "snapshot_ids": ["snap_yes"],
                        },
                        observed_at=datetime(2026, 3, 10, 10, 6, tzinfo=UTC),
                    )
                finally:
                    con.close()

            self._drain_queue(db_path=db_path, queue_path=queue_path)
            with patch.dict("os.environ", reader_env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    orders = con.execute("SELECT COUNT(*) FROM trading.orders").fetchone()[0]
                    gate = con.execute("SELECT allowed, reason_codes_json, metrics_json FROM runtime.gate_decisions").fetchone()
                    allocation = con.execute("SELECT allocation_status FROM runtime.allocation_decisions").fetchone()[0]
                finally:
                    con.close()

        reason_codes = json.loads(gate[1])
        metrics = json.loads(gate[2])
        self.assertEqual(orders, 0)
        self.assertFalse(gate[0])
        self.assertIn("allocation_blocked", reason_codes)
        self.assertEqual(allocation, "policy_missing")
        self.assertEqual(metrics["allocation_status"], "policy_missing")

    def test_paper_execution_aligns_reference_price_to_tick_size(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "paper_align.duckdb")
            queue_path = str(Path(tmpdir) / "write_queue.sqlite")
            self._setup_db(db_path)
            seed_con = duckdb.connect(db_path)
            try:
                self._seed_common(
                    seed_con,
                    include_policy=True,
                    max_buy_notional_per_run=10.0,
                    reference_price=0.655,
                )
            finally:
                seed_con.close()
            reader_env = {
                "ASTERION_STRICT_SINGLE_WRITER": "1",
                "ASTERION_DB_ROLE": "reader",
                "WRITERD": "0",
            }
            with patch.dict("os.environ", reader_env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    result = run_weather_paper_execution_job(
                        con,
                        WriteQueueConfig(path=queue_path),
                        params_json={
                            "wallet_id": "wallet_weather_1",
                            "strategy_registrations": [
                                {
                                    "strategy_id": "weather_primary",
                                    "strategy_version": "v1",
                                    "priority": 1,
                                    "route_action": "FAK",
                                    "size": "10",
                                    "min_edge_bps": 500,
                                }
                            ],
                            "snapshot_ids": ["snap_yes"],
                        },
                        observed_at=datetime(2026, 3, 10, 10, 6, tzinfo=UTC),
                    )
                finally:
                    con.close()

            self._drain_queue(db_path=db_path, queue_path=queue_path)
            with patch.dict("os.environ", reader_env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    ticket_price, provenance_json = con.execute(
                        "SELECT reference_price, provenance_json FROM runtime.trade_tickets"
                    ).fetchone()
                    order_price = con.execute("SELECT price FROM trading.orders").fetchone()[0]
                finally:
                    con.close()

        provenance = json.loads(provenance_json)
        self.assertEqual(result.metadata["allowed_order_count"], 1)
        self.assertEqual(float(ticket_price), 0.65)
        self.assertEqual(float(order_price), 0.65)
        self.assertEqual(Decimal(provenance["reference_price_unaligned"]), Decimal("0.655"))
        self.assertEqual(Decimal(provenance["reference_price_aligned"]), Decimal("0.65"))


if __name__ == "__main__":
    unittest.main()
