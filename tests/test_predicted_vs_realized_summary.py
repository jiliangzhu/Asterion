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
    def _apply_migrations(self, db_path: str) -> None:
        bootstrap = duckdb.connect(db_path)
        migrations_dir = Path(__file__).resolve().parents[1] / "sql" / "migrations"
        for path in sorted(migrations_dir.glob("*.sql")):
            sql = path.read_text(encoding="utf-8").strip()
            if sql:
                bootstrap.execute(sql)
        bootstrap.close()

    def _insert_ticket_case(
        self,
        con,
        *,
        ticket_id: str,
        market_id: str,
        watch_snapshot_id: str,
        gate_allowed: bool = True,
        order_id: str | None = None,
        order_status: str | None = None,
        fill_size: float = 0.0,
        fill_price: float = 0.42,
        with_resolution: bool = False,
        sign_status: str | None = None,
        submit_status: str | None = None,
        external_order_status: str | None = None,
    ) -> None:
        request_id = f"req_{ticket_id}"
        execution_context_id = f"ectx_{ticket_id}"
        provenance = json.dumps(
            {
                "watch_snapshot_id": watch_snapshot_id,
                "pricing_context": {
                    "edge_bps_executable": 900,
                    "reference_price": 0.40,
                    "source_freshness_status": "fresh",
                },
            }
        )
        con.execute(
            """
            INSERT INTO runtime.trade_tickets (
                ticket_id, run_id, strategy_id, strategy_version, market_id, token_id, outcome, side, reference_price,
                fair_value, edge_bps, threshold_bps, route_action, size, signal_ts_ms, forecast_run_id,
                watch_snapshot_id, request_id, ticket_hash, provenance_json, created_at, wallet_id, execution_context_id
            ) VALUES (
                ?, 'run_1', 'weather_primary', 'v1', ?, 'tok_yes', 'YES', 'BUY', 0.40, 0.55,
                900, 100, 'FAK', 10.0, 1710000000000, 'frun_1', ?, ?, ?, ?, '2026-03-15 09:00:00',
                'wallet_weather_1', ?
            )
            """,
            [ticket_id, market_id, watch_snapshot_id, request_id, f"hash_{ticket_id}", provenance, execution_context_id],
        )
        con.execute(
            """
            INSERT INTO runtime.gate_decisions (
                gate_id, ticket_id, allowed, reason, reason_codes_json, metrics_json, created_at
            ) VALUES (?, ?, ?, ?, '[]', '{}', '2026-03-15 09:00:01')
            """,
            [f"gate_{ticket_id}", ticket_id, gate_allowed, "passed" if gate_allowed else "blocked"],
        )
        con.execute(
            """
            INSERT INTO runtime.journal_events (
                event_id, event_type, entity_type, entity_id, run_id, payload_json, created_at
            ) VALUES (?, 'trade_ticket.created', 'trade_ticket', ?, 'run_1', ?, '2026-03-15 09:00:00')
            """,
            [f"jevt_ticket_{ticket_id}", ticket_id, json.dumps({"request_id": request_id, "ticket_id": ticket_id})],
        )

        if sign_status is not None:
            con.execute(
                """
                INSERT INTO runtime.submit_attempts (
                    attempt_id, request_id, ticket_id, order_id, wallet_id, execution_context_id,
                    exchange, attempt_kind, attempt_mode, canonical_order_hash, payload_hash,
                    submit_payload_json, signed_payload_ref, status, error, created_at
                ) VALUES (?, ?, ?, ?, 'wallet_weather_1', ?, 'polymarket_clob', 'sign_order', 'sign_only',
                    ?, ?, '{}', ?, ?, NULL, '2026-03-15 09:00:05')
                """,
                [
                    f"sign_{ticket_id}",
                    f"sigreq_{ticket_id}",
                    ticket_id,
                    order_id,
                    execution_context_id,
                    f"coh_{ticket_id}",
                    f"phash_sign_{ticket_id}",
                    f"sign_{ticket_id}",
                    sign_status,
                ],
            )
        if submit_status is not None:
            con.execute(
                """
                INSERT INTO runtime.submit_attempts (
                    attempt_id, request_id, ticket_id, order_id, wallet_id, execution_context_id,
                    exchange, attempt_kind, attempt_mode, canonical_order_hash, payload_hash,
                    submit_payload_json, signed_payload_ref, status, error, created_at
                ) VALUES (?, ?, ?, ?, 'wallet_weather_1', ?, 'polymarket_clob', 'submit_order', 'live_submit',
                    ?, ?, '{}', ?, ?, NULL, '2026-03-15 09:00:06')
                """,
                [
                    f"submit_{ticket_id}",
                    f"subreq_{ticket_id}",
                    ticket_id,
                    order_id,
                    execution_context_id,
                    f"coh_{ticket_id}",
                    f"phash_submit_{ticket_id}",
                    f"sign_{ticket_id}",
                    submit_status,
                ],
            )
        if external_order_status is not None:
            con.execute(
                """
                INSERT INTO runtime.external_order_observations VALUES (
                    ?, ?, ?, ?, ?, 'wallet_weather_1', ?, 'polymarket_clob', 'unit_test', 'live_submit',
                    ?, ?, ?, '2026-03-15 09:00:07', NULL, '{}'
                )
                """,
                [
                    f"obs_{ticket_id}",
                    f"submit_{ticket_id}",
                    f"subreq_{ticket_id}",
                    ticket_id,
                    order_id,
                    execution_context_id,
                    f"coh_{ticket_id}",
                    f"ext_{ticket_id}",
                    external_order_status,
                ],
            )

        if order_id and order_status:
            filled_size = fill_size if fill_size > 0 else (10.0 if order_status == "filled" else 0.0)
            avg_fill_price = fill_price if filled_size > 0 else None
            remaining_size = max(10.0 - filled_size, 0.0)
            con.execute(
                """
                INSERT INTO trading.orders (
                    order_id, client_order_id, wallet_id, market_id, token_id, outcome, side, price, size, route_action,
                    time_in_force, expiration, fee_rate_bps, signature_type, funder, status, filled_size, remaining_size,
                    avg_fill_price, reservation_id, exchange_order_id, created_at, submitted_at, updated_at
                ) VALUES (
                    ?, ?, 'wallet_weather_1', ?, 'tok_yes', 'YES', 'BUY', 0.40, 10.0, 'FAK',
                    'FAK', NULL, 30, 1, '0xfunder', ?, ?, ?, ?, ?, ?, '2026-03-15 09:00:10',
                    '2026-03-15 09:00:10', '2026-03-15 09:01:00'
                )
                """,
                [
                    order_id,
                    f"client_{ticket_id}",
                    market_id,
                    order_status,
                    filled_size,
                    remaining_size,
                    avg_fill_price,
                    f"res_{ticket_id}",
                    f"paper_{ticket_id}",
                ],
            )
            con.execute(
                """
                INSERT INTO runtime.journal_events (
                    event_id, event_type, entity_type, entity_id, run_id, payload_json, created_at
                ) VALUES (?, 'order.created', 'order', ?, 'run_1', ?, '2026-03-15 09:00:10')
                """,
                [
                    f"jevt_order_{ticket_id}",
                    order_id,
                    json.dumps({"request_id": request_id, "ticket_id": ticket_id, "client_order_id": f"client_{ticket_id}"}),
                ],
            )
        if order_id and fill_size > 0:
            con.execute(
                """
                INSERT INTO trading.fills (
                    fill_id, order_id, wallet_id, market_id, token_id, outcome, side, price, size, fee,
                    fee_rate_bps, trade_id, exchange_order_id, filled_at
                ) VALUES (
                    ?, ?, 'wallet_weather_1', ?, 'tok_yes', 'YES', 'BUY',
                    ?, ?, 0.10, 30, ?, ?, '2026-03-15 09:01:00'
                )
                """,
                [f"fill_{ticket_id}", order_id, market_id, fill_price, fill_size, f"trade_{ticket_id}", f"paper_{ticket_id}"],
            )

        con.execute(
            """
            INSERT INTO weather.weather_forecast_replays (
                replay_id, market_id, condition_id, station_id, source, model_run, forecast_target_time,
                spec_version, replay_key, replay_reason, original_run_id, replayed_run_id, created_at
            ) VALUES (
                ?, ?, 'cond_1', 'KSEA', 'openmeteo', '2026-03-15T00:00Z', '2026-03-15 12:00:00',
                'spec_1', ?, 'unit_test', 'frun_old', 'frun_new', '2026-03-15 09:02:00'
            )
            """,
            [f"replay_{ticket_id}", market_id, f"key_{ticket_id}"],
        )
        con.execute(
            """
            INSERT INTO weather.weather_forecast_replay_diffs (
                diff_id, replay_id, entity_type, entity_key, original_entity_id, replayed_entity_id, status, diff_summary_json, created_at
            ) VALUES (
                ?, ?, 'forecast_run', ?, 'frun_old', 'frun_new', 'DIFFERENT', ?, '2026-03-15 09:02:01'
            )
            """,
            [f"diff_{ticket_id}", f"replay_{ticket_id}", market_id, json.dumps({"changed_fields": ["temperature_distribution", "pricing_context"]})],
        )
        if with_resolution:
            con.execute(
                """
                INSERT INTO resolution.settlement_verifications (
                    verification_id, proposal_id, market_id, proposed_outcome, expected_outcome, is_correct,
                    confidence, discrepancy_details, sources_checked, evidence_package, created_at
                ) VALUES (
                    ?, ?, ?, 'YES', 'YES', TRUE, 0.95, NULL, '[]', '{}', '2026-03-15 10:00:00'
                )
                """,
                [f"ver_{ticket_id}", f"prop_{ticket_id}", market_id],
            )

    def _prepare_db(self, db_path: str, *, with_resolution: bool) -> None:
        self._apply_migrations(db_path)
        con = duckdb.connect(db_path)
        try:
            self._insert_ticket_case(
                con,
                ticket_id="tt_1",
                market_id="mkt_1",
                watch_snapshot_id="snap_1",
                order_id="ord_1",
                order_status="filled",
                fill_size=10.0,
                with_resolution=with_resolution,
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
                    SELECT predicted_edge_bps, realized_fill_price, resolution_value, source_disagreement,
                           evaluation_status, execution_lifecycle_stage, miss_reason_bucket
                    FROM ui.predicted_vs_realized_summary
                    """
                ).fetchone()
            finally:
                con.close()
        assert row is not None
        self.assertEqual(row[0], 900)
        self.assertAlmostEqual(float(row[1]), 0.42, places=6)
        self.assertAlmostEqual(float(row[2]), 1.0, places=6)
        self.assertEqual(row[3], "different")
        self.assertEqual(row[4], "resolved")
        self.assertEqual(row[5], "resolved")
        self.assertEqual(row[6], "captured_resolved")

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
                    SELECT resolution_value, realized_pnl, evaluation_status, execution_lifecycle_stage, miss_reason_bucket
                    FROM ui.predicted_vs_realized_summary
                    """
                ).fetchone()
            finally:
                con.close()
        assert row is not None
        self.assertIsNone(row[0])
        self.assertIsNone(row[1])
        self.assertEqual(row[2], "pending_resolution")
        self.assertEqual(row[3], "filled_unresolved")
        self.assertEqual(row[4], "captured_unresolved")

    def test_projection_includes_gate_rejected_and_sign_rejected_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            lite_path = str(Path(tmpdir) / "ui_lite.duckdb")
            self._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                self._insert_ticket_case(
                    con,
                    ticket_id="tt_gate",
                    market_id="mkt_gate",
                    watch_snapshot_id="snap_gate",
                    gate_allowed=False,
                )
                self._insert_ticket_case(
                    con,
                    ticket_id="tt_sign",
                    market_id="mkt_sign",
                    watch_snapshot_id="snap_sign",
                    sign_status="rejected",
                )
            finally:
                con.close()
            result = build_ui_lite_db_once(src_db_path=db_path, dst_db_path=lite_path)
            self.assertTrue(result.ok, result.error)

            con = duckdb.connect(lite_path)
            try:
                rows = con.execute(
                    """
                    SELECT market_id, evaluation_status, execution_lifecycle_stage, miss_reason_bucket
                    FROM ui.predicted_vs_realized_summary
                    ORDER BY market_id
                    """
                ).fetchall()
            finally:
                con.close()
        self.assertEqual(
            rows,
            [
                ("mkt_gate", "pending_fill", "gate_rejected", "gate_rejected"),
                ("mkt_sign", "pending_fill", "sign_rejected", "sign_rejected"),
            ],
        )

    def test_projection_classifies_submit_rejected_and_working_unfilled(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            lite_path = str(Path(tmpdir) / "ui_lite.duckdb")
            self._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                self._insert_ticket_case(
                    con,
                    ticket_id="tt_submit",
                    market_id="mkt_submit",
                    watch_snapshot_id="snap_submit",
                    sign_status="signed",
                    submit_status="rejected",
                    external_order_status="rejected",
                )
                self._insert_ticket_case(
                    con,
                    ticket_id="tt_working",
                    market_id="mkt_working",
                    watch_snapshot_id="snap_working",
                    sign_status="signed",
                    submit_status="accepted",
                    external_order_status="accepted",
                    order_id="ord_working",
                    order_status="posted",
                )
            finally:
                con.close()
            result = build_ui_lite_db_once(src_db_path=db_path, dst_db_path=lite_path)
            self.assertTrue(result.ok, result.error)

            con = duckdb.connect(lite_path)
            try:
                rows = con.execute(
                    """
                    SELECT market_id, execution_lifecycle_stage, miss_reason_bucket, evaluation_status
                    FROM ui.predicted_vs_realized_summary
                    ORDER BY market_id
                    """
                ).fetchall()
            finally:
                con.close()
        self.assertEqual(
            rows,
            [
                ("mkt_submit", "submit_rejected", "submit_rejected", "pending_fill"),
                ("mkt_working", "submitted_ack", "not_submitted", "pending_fill"),
            ],
        )


if __name__ == "__main__":
    unittest.main()
