from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

import duckdb

from asterion_core.ui import build_ui_lite_db_once


HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required")
class PredictedVsRealizedSummaryTest(unittest.TestCase):
    def _prepare_db(self, db_path: str, *, with_resolution: bool) -> None:
        bootstrap = duckdb.connect(db_path)
        migrations_dir = Path(__file__).resolve().parents[1] / "sql" / "migrations"
        for path in sorted(migrations_dir.glob("*.sql")):
            sql = path.read_text(encoding="utf-8").strip()
            if sql:
                bootstrap.execute(sql)
        bootstrap.close()
        con = duckdb.connect(db_path)
        try:
            con.execute(
                """
                INSERT INTO runtime.trade_tickets (
                    ticket_id, run_id, strategy_id, strategy_version, market_id, token_id, outcome, side, reference_price,
                    fair_value, edge_bps, threshold_bps, route_action, size, signal_ts_ms, forecast_run_id,
                    watch_snapshot_id, request_id, ticket_hash, provenance_json, created_at, wallet_id, execution_context_id
                ) VALUES (
                    'tt_1', 'run_1', 'weather_primary', 'v1', 'mkt_1', 'tok_yes', 'YES', 'BUY', 0.40, 0.55,
                    900, 100, 'FAK', 10.0, 1710000000000, 'frun_1', 'snap_1', 'req_1', 'thash_1', ?,
                    '2026-03-15 09:00:00', 'wallet_weather_1', 'ectx_1'
                )
                """,
                [
                    json.dumps(
                        {
                            "watch_snapshot_id": "snap_1",
                            "pricing_context": {
                                "edge_bps_executable": 900,
                                "reference_price": 0.40,
                                "source_freshness_status": "fresh",
                            },
                        }
                    )
                ],
            )
            con.execute(
                """
                INSERT INTO runtime.gate_decisions (
                    gate_id, ticket_id, allowed, reason, reason_codes_json, metrics_json, created_at
                ) VALUES (
                    'gate_1', 'tt_1', TRUE, 'passed', '[]', '{}', '2026-03-15 09:00:01'
                )
                """
            )
            con.execute(
                """
                INSERT INTO trading.orders (
                    order_id, client_order_id, wallet_id, market_id, token_id, outcome, side, price, size, route_action,
                    time_in_force, expiration, fee_rate_bps, signature_type, funder, status, filled_size, remaining_size,
                    avg_fill_price, reservation_id, exchange_order_id, created_at, submitted_at, updated_at
                ) VALUES (
                    'ord_1', 'client_1', 'wallet_weather_1', 'mkt_1', 'tok_yes', 'YES', 'BUY', 0.40, 10.0, 'FAK',
                    'FAK', NULL, 30, 1, '0xfunder', 'filled', 10.0, 0.0, 0.42, 'res_1', 'paper_ord_1',
                    '2026-03-15 09:00:10', '2026-03-15 09:00:10', '2026-03-15 09:01:00'
                )
                """
            )
            con.execute(
                """
                INSERT INTO trading.fills (
                    fill_id, order_id, wallet_id, market_id, token_id, outcome, side, price, size, fee,
                    fee_rate_bps, trade_id, exchange_order_id, filled_at
                ) VALUES (
                    'fill_1', 'ord_1', 'wallet_weather_1', 'mkt_1', 'tok_yes', 'YES', 'BUY',
                    0.42, 10.0, 0.10, 30, 'trade_1', 'paper_ord_1', '2026-03-15 09:01:00'
                )
                """
            )
            con.execute(
                """
                INSERT INTO runtime.journal_events (
                    event_id, event_type, entity_type, entity_id, run_id, payload_json, created_at
                ) VALUES
                ('jevt_ticket_1', 'trade_ticket.created', 'trade_ticket', 'tt_1', 'run_1', ?, '2026-03-15 09:00:00'),
                ('jevt_order_1', 'order.created', 'order', 'ord_1', 'run_1', ?, '2026-03-15 09:00:10')
                """,
                [
                    json.dumps({"request_id": "req_1", "ticket_id": "tt_1"}),
                    json.dumps({"request_id": "req_1", "ticket_id": "tt_1", "client_order_id": "client_1"}),
                ],
            )
            con.execute(
                """
                INSERT INTO weather.weather_forecast_replays (
                    replay_id, market_id, condition_id, station_id, source, model_run, forecast_target_time,
                    spec_version, replay_key, replay_reason, original_run_id, replayed_run_id, created_at
                ) VALUES (
                    'replay_1', 'mkt_1', 'cond_1', 'KSEA', 'openmeteo', '2026-03-15T00:00Z', '2026-03-15 12:00:00',
                    'spec_1', 'key_1', 'unit_test', 'frun_old', 'frun_new', '2026-03-15 09:02:00'
                )
                """
            )
            con.execute(
                """
                INSERT INTO weather.weather_forecast_replay_diffs (
                    diff_id, replay_id, entity_type, entity_key, original_entity_id, replayed_entity_id, status, diff_summary_json, created_at
                ) VALUES (
                    'diff_1', 'replay_1', 'forecast_run', 'mkt_1', 'frun_old', 'frun_new', 'DIFFERENT', ?, '2026-03-15 09:02:01'
                )
                """,
                [json.dumps({"changed_fields": ["temperature_distribution", "pricing_context"]})],
            )
            if with_resolution:
                con.execute(
                    """
                    INSERT INTO resolution.settlement_verifications (
                        verification_id, proposal_id, market_id, proposed_outcome, expected_outcome, is_correct,
                        confidence, discrepancy_details, sources_checked, evidence_package, created_at
                    ) VALUES (
                        'ver_1', 'prop_1', 'mkt_1', 'YES', 'YES', TRUE, 0.95, NULL, '[]', '{}', '2026-03-15 10:00:00'
                    )
                    """
                )
        finally:
            con.close()

    def test_projection_builds_resolved_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            lite_path = str(Path(tmpdir) / "ui_lite.duckdb")
            self._prepare_db(db_path, with_resolution=True)
            result = build_ui_lite_db_once(src_db_path=db_path, dst_db_path=lite_path)
            self.assertTrue(result.ok, result.error)

            con = duckdb.connect(lite_path)
            try:
                row = con.execute(
                    """
                    SELECT predicted_edge_bps, realized_fill_price, resolution_value, realized_pnl, source_disagreement, evaluation_status
                    FROM ui.predicted_vs_realized_summary
                    """
                ).fetchone()
            finally:
                con.close()
        assert row is not None
        self.assertEqual(row[0], 900)
        self.assertAlmostEqual(float(row[1]), 0.42, places=6)
        self.assertAlmostEqual(float(row[2]), 1.0, places=6)
        self.assertEqual(row[4], "different")
        self.assertEqual(row[5], "resolved")

    def test_projection_keeps_pending_resolution_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            lite_path = str(Path(tmpdir) / "ui_lite.duckdb")
            self._prepare_db(db_path, with_resolution=False)
            result = build_ui_lite_db_once(src_db_path=db_path, dst_db_path=lite_path)
            self.assertTrue(result.ok, result.error)

            con = duckdb.connect(lite_path)
            try:
                row = con.execute(
                    """
                    SELECT resolution_value, realized_pnl, evaluation_status
                    FROM ui.predicted_vs_realized_summary
                    """
                ).fetchone()
            finally:
                con.close()
        assert row is not None
        self.assertIsNone(row[0])
        self.assertIsNone(row[1])
        self.assertEqual(row[2], "pending_resolution")


if __name__ == "__main__":
    unittest.main()
