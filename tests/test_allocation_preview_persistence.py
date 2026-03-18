from __future__ import annotations

import importlib.util
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from asterion_core.storage.write_queue import WriteQueueConfig
from asterion_core.storage.writerd import process_one
from dagster_asterion.handlers import run_weather_allocation_preview_refresh_job


HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required")
class AllocationPreviewPersistenceTest(unittest.TestCase):
    def _apply_migrations(self, db_path: str) -> None:
        bootstrap = duckdb.connect(db_path)
        migrations_dir = Path(__file__).resolve().parents[1] / "sql" / "migrations"
        for path in sorted(migrations_dir.glob("*.sql")):
            sql = path.read_text(encoding="utf-8").strip()
            if sql:
                bootstrap.execute(sql)
        bootstrap.close()

    def _seed_snapshot_case(self, con) -> None:
        con.execute(
            """
            INSERT INTO weather.weather_watch_only_snapshots (
                snapshot_id, fair_value_id, run_id, market_id, condition_id, token_id, outcome,
                reference_price, fair_value, edge_bps, threshold_bps, decision, side, rationale,
                pricing_context_json, created_at
            ) VALUES
                (
                    'snap_high', 'fv_high', 'frun_1', 'mkt_alloc_1', 'cond_1', 'tok_yes_1', 'YES',
                    0.40, 0.60, 800, 500, 'TAKE', 'BUY', 'high',
                    '{"ranking_score": 0.9}', '2026-03-15 09:00:00'
                ),
                (
                    'snap_low', 'fv_low', 'frun_1', 'mkt_alloc_2', 'cond_2', 'tok_yes_2', 'YES',
                    0.40, 0.58, 700, 500, 'TAKE', 'BUY', 'low',
                    '{"ranking_score": 0.4}', '2026-03-15 09:01:00'
                )
            """
        )
        for market_id in ("mkt_alloc_1", "mkt_alloc_2"):
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
                    "2026-03-15 00:00:00",
                    "2026-03-15 00:00:00",
                ],
            )
        con.execute(
            """
            INSERT INTO trading.inventory_positions VALUES
                ('wallet_weather_1', 'usdc_e', 'usdc_e', 'cash', 'cash', 'available', 100.0, '0xfunder', 1, '2026-03-15 09:00:00')
            """
        )
        con.execute(
            """
            INSERT INTO trading.allocation_policies VALUES
                ('policy_default', 'wallet_weather_1', NULL, 'active', 'alloc_v1', 12.0, 12.0, 1.0, 1.0, '2026-03-15 00:00:00', '2026-03-15 00:00:00'),
                ('policy_exact', 'wallet_weather_1', 'weather_primary', 'active', 'alloc_v1_exact', 6.0, 6.0, 1.0, 1.0, '2026-03-15 00:00:00', '2026-03-15 01:00:00')
            """
        )

    def _drain_queue(self, *, db_path: str, queue_path: str) -> None:
        with patch.dict(
            os.environ,
            {
                "ASTERION_DB_PATH": db_path,
                "ASTERION_WRITERD_ALLOWED_TABLES": "runtime.capital_allocation_runs,runtime.allocation_decisions,runtime.position_limit_checks",
            },
            clear=False,
        ):
            while process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False):
                pass

    def test_allocation_preview_persists_runtime_artifacts_and_prefers_exact_policy(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "alloc_preview.duckdb")
            queue_path = str(Path(tmpdir) / "write_queue.sqlite")
            self._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                self._seed_snapshot_case(con)
                result = run_weather_allocation_preview_refresh_job(
                    con,
                    WriteQueueConfig(path=queue_path),
                    params_json={
                        "wallet_id": "wallet_weather_1",
                        "strategy_registrations": [
                            {
                                "strategy_id": "weather_primary",
                                "strategy_version": "v2",
                                "priority": 1,
                                "route_action": "FAK",
                                "size": "10",
                                "min_edge_bps": 500,
                            }
                        ],
                        "snapshot_ids": ["snap_high", "snap_low"],
                    },
                    run_id="alloc_preview_manual",
                )
            finally:
                con.close()

            self._drain_queue(db_path=db_path, queue_path=queue_path)
            qcon = duckdb.connect(db_path)
            try:
                run_count = qcon.execute("SELECT COUNT(*) FROM runtime.capital_allocation_runs").fetchone()[0]
                decision_rows = qcon.execute(
                    """
                    SELECT allocation_status, policy_id, recommended_size
                    FROM runtime.allocation_decisions
                    ORDER BY ranking_score DESC, allocation_decision_id ASC
                    """
                ).fetchall()
            finally:
                qcon.close()

        self.assertEqual(result.metadata["approved_count"], 1)
        self.assertEqual(result.metadata["resized_count"], 1)
        self.assertEqual(result.metadata["blocked_count"], 0)
        self.assertEqual(run_count, 1)
        self.assertEqual(len(decision_rows), 2)
        self.assertEqual(decision_rows[0][1], "policy_exact")
        self.assertEqual(decision_rows[0][0], "approved")
        self.assertEqual(decision_rows[0][2], 10.0)
        self.assertEqual(decision_rows[1][0], "resized")
        self.assertEqual(decision_rows[1][2], 5.0)

    def test_allocation_preview_is_idempotent_for_same_run_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "alloc_preview.duckdb")
            queue_path = str(Path(tmpdir) / "write_queue.sqlite")
            self._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                self._seed_snapshot_case(con)
                for _ in range(2):
                    run_weather_allocation_preview_refresh_job(
                        con,
                        WriteQueueConfig(path=queue_path),
                        params_json={
                            "wallet_id": "wallet_weather_1",
                            "strategy_registrations": [
                                {
                                    "strategy_id": "weather_primary",
                                    "strategy_version": "v2",
                                    "priority": 1,
                                    "route_action": "FAK",
                                    "size": "10",
                                    "min_edge_bps": 500,
                                }
                            ],
                            "snapshot_ids": ["snap_high", "snap_low"],
                        },
                        run_id="alloc_preview_manual",
                    )
            finally:
                con.close()

            self._drain_queue(db_path=db_path, queue_path=queue_path)
            qcon = duckdb.connect(db_path)
            try:
                run_count = qcon.execute("SELECT COUNT(*) FROM runtime.capital_allocation_runs").fetchone()[0]
                decision_count = qcon.execute("SELECT COUNT(*) FROM runtime.allocation_decisions").fetchone()[0]
            finally:
                qcon.close()

        self.assertEqual(run_count, 1)
        self.assertEqual(decision_count, 2)


if __name__ == "__main__":
    unittest.main()
