from __future__ import annotations

import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from asterion_core.storage.write_queue import WriteQueueConfig
from asterion_core.storage.writerd import process_one
from dagster_asterion.handlers import run_weather_ranking_retrospective_refresh_job
from domains.weather.opportunity.ranking_retrospective import materialize_ranking_retrospective


HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required")
class RankingRetrospectiveHarnessTest(unittest.TestCase):
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
                    'snap_exec', 'fv_exec', 'frun_1', 'mkt_1', 'cond_1', 'tok_yes', 'YES',
                    0.40, 0.60, 800, 500, 'TAKE', 'BUY', 'unit',
                    ?, '2026-03-15 09:00:00'
                ),
                (
                    'snap_watch', 'fv_watch', 'frun_1', 'mkt_1', 'cond_1', 'tok_no', 'NO',
                    0.42, 0.55, 300, 500, 'WATCH', 'BUY', 'unit',
                    ?, '2026-03-15 09:05:00'
                )
            """,
            [
                json.dumps({"ranking_score": 0.42, "edge_bps_executable": 800, "expected_dollar_pnl": 0.08}),
                json.dumps({"ranking_score": 0.09, "edge_bps_executable": 300, "expected_dollar_pnl": 0.03}),
            ],
        )
        con.execute(
            """
            INSERT INTO runtime.trade_tickets (
                ticket_id, run_id, strategy_id, strategy_version, market_id, token_id, outcome, side,
                reference_price, fair_value, edge_bps, threshold_bps, route_action, size, signal_ts_ms,
                forecast_run_id, watch_snapshot_id, request_id, ticket_hash, provenance_json, created_at,
                wallet_id, execution_context_id
            ) VALUES (
                'tt_exec', 'run_1', 'weather_primary', 'v2', 'mkt_1', 'tok_yes', 'YES', 'BUY',
                0.40, 0.60, 800, 500, 'FAK', 10.0, 1710000000000,
                'frun_1', 'snap_exec', 'req_exec', 'hash_exec', '{}', '2026-03-15 09:00:10',
                'wallet_weather_1', 'ectx_exec'
            )
            """
        )
        con.execute(
            """
            INSERT INTO runtime.submit_attempts (
                attempt_id, request_id, ticket_id, order_id, wallet_id, execution_context_id,
                exchange, attempt_kind, attempt_mode, canonical_order_hash, payload_hash,
                submit_payload_json, signed_payload_ref, status, error, created_at
            ) VALUES (
                'submit_exec', 'submit_req_exec', 'tt_exec', 'ord_exec', 'wallet_weather_1', 'ectx_exec',
                'polymarket_clob', 'submit_order', 'live_submit', 'coh_exec', 'ph_exec', '{}', 'sign_exec',
                'accepted', NULL, '2026-03-15 09:00:12'
            )
            """
        )
        con.execute(
            """
            INSERT INTO trading.fills (
                fill_id, order_id, wallet_id, market_id, token_id, outcome, side, price, size, fee,
                fee_rate_bps, trade_id, exchange_order_id, filled_at
            ) VALUES (
                'fill_exec', 'ord_exec', 'wallet_weather_1', 'mkt_1', 'tok_yes', 'YES', 'BUY',
                0.41, 10.0, 0.10, 30, 'trade_exec', 'paper_exec', '2026-03-15 09:01:00'
            )
            """
        )
        con.execute(
            """
            INSERT INTO resolution.settlement_verifications (
                verification_id, proposal_id, market_id, proposed_outcome, expected_outcome, is_correct,
                confidence, discrepancy_details, sources_checked, evidence_package, created_at
            ) VALUES (
                'ver_exec', 'prop_exec', 'mkt_1', 'YES', 'YES', TRUE, 0.95, NULL, '[]', '{}', '2026-03-16 10:00:00'
            )
            """
        )
        con.execute(
            """
            INSERT INTO weather.weather_forecast_replay_diffs (
                diff_id, replay_id, entity_type, entity_key, original_entity_id, replayed_entity_id, status,
                diff_summary_json, created_at
            ) VALUES (
                'diff_exec', 'replay_exec', 'watch_only_snapshot', 'mkt_1:YES', 'snap_exec', 'snap_exec_replayed',
                'DIFFERENT', '{"changed": true}', '2026-03-16 12:00:00'
            )
            """
        )

    def _drain_queue(self, *, db_path: str, queue_path: str) -> None:
        with patch.dict(
            os.environ,
            {
                "ASTERION_DB_PATH": db_path,
                "ASTERION_WRITERD_ALLOWED_TABLES": "runtime.ranking_retrospective_runs,runtime.ranking_retrospective_rows",
            },
            clear=False,
        ):
            while process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False):
                pass

    def test_materializer_builds_deterministic_retrospective_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "retro.duckdb")
            self._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                self._seed_snapshot_case(con)
                retrospective_run, rows, summary = materialize_ranking_retrospective(
                    con,
                    lookback_days=30,
                    run_id="ranking_retro_manual",
                )
            finally:
                con.close()

        self.assertEqual(retrospective_run.run_id, "ranking_retro_manual")
        self.assertEqual(summary.snapshot_count, 2)
        self.assertGreaterEqual(len(rows), 2)
        top_row = next(item for item in rows if item.ranking_decile == 1)
        self.assertGreater(top_row.submitted_capture_ratio, 0.0)
        self.assertGreater(top_row.fill_capture_ratio, 0.0)
        self.assertGreater(top_row.resolution_capture_ratio, 0.0)
        self.assertGreater(top_row.forecast_replay_change_rate, 0.0)

    def test_manual_handler_is_idempotent_for_same_run_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "retro.duckdb")
            queue_path = str(Path(tmpdir) / "write_queue.sqlite")
            self._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                self._seed_snapshot_case(con)
                run_weather_ranking_retrospective_refresh_job(
                    con,
                    WriteQueueConfig(path=queue_path),
                    lookback_days=30,
                    run_id="ranking_retro_manual",
                )
                run_weather_ranking_retrospective_refresh_job(
                    con,
                    WriteQueueConfig(path=queue_path),
                    lookback_days=30,
                    run_id="ranking_retro_manual",
                )
            finally:
                con.close()

            self._drain_queue(db_path=db_path, queue_path=queue_path)
            qcon = duckdb.connect(db_path)
            try:
                run_count = qcon.execute("SELECT COUNT(*) FROM runtime.ranking_retrospective_runs").fetchone()[0]
                row_count = qcon.execute("SELECT COUNT(*) FROM runtime.ranking_retrospective_rows").fetchone()[0]
            finally:
                qcon.close()

        self.assertEqual(run_count, 1)
        self.assertGreaterEqual(row_count, 2)


if __name__ == "__main__":
    unittest.main()
