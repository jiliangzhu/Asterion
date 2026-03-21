from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import duckdb

from domains.weather.opportunity import compare_retrospective_uplift
from domains.weather.opportunity.ranking_retrospective import materialize_ranking_retrospective


HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required")
class RetrospectiveUpliftIntegrationTest(unittest.TestCase):
    def _apply_migrations(self, db_path: str) -> None:
        bootstrap = duckdb.connect(db_path)
        migrations_dir = Path(__file__).resolve().parents[1] / "sql" / "migrations"
        for path in sorted(migrations_dir.glob("*.sql")):
            sql = path.read_text(encoding="utf-8").strip()
            if sql:
                bootstrap.execute(sql)
        bootstrap.close()

    def _seed_case(self, con, *, improve_second_snapshot: bool) -> None:
        con.execute(
            """
            INSERT INTO weather.weather_watch_only_snapshots (
                snapshot_id, fair_value_id, run_id, market_id, condition_id, token_id, outcome,
                reference_price, fair_value, edge_bps, threshold_bps, decision, side, rationale,
                pricing_context_json, created_at
            ) VALUES
                ('snap_exec', 'fv_exec', 'frun_1', 'mkt_1', 'cond_1', 'tok_yes', 'YES', 0.40, 0.60, 800, 500, 'TAKE', 'BUY', 'unit', ?, '2026-03-15 09:00:00'),
                ('snap_watch', 'fv_watch', 'frun_1', 'mkt_2', 'cond_2', 'tok_no', 'NO', 0.42, 0.55, 300, 500, 'TAKE', 'BUY', 'unit', ?, '2026-03-15 09:05:00')
            """,
            [
                json.dumps({"ranking_score": 0.42, "edge_bps_executable": 800, "expected_dollar_pnl": 0.08}),
                json.dumps({"ranking_score": 0.30, "edge_bps_executable": 600, "expected_dollar_pnl": 0.06}),
            ],
        )
        con.execute(
            """
            INSERT INTO runtime.trade_tickets (
                ticket_id, run_id, strategy_id, strategy_version, market_id, token_id, outcome, side,
                reference_price, fair_value, edge_bps, threshold_bps, route_action, size, signal_ts_ms,
                forecast_run_id, watch_snapshot_id, request_id, ticket_hash, provenance_json, created_at,
                wallet_id, execution_context_id
            ) VALUES
                ('tt_exec', 'run_1', 'weather_primary', 'v2', 'mkt_1', 'tok_yes', 'YES', 'BUY', 0.40, 0.60, 800, 500, 'FAK', 10.0, 1710000000000, 'frun_1', 'snap_exec', 'req_exec', 'hash_exec', '{}', '2026-03-15 09:00:10', 'wallet_weather_1', 'ectx_exec'),
                ('tt_watch', 'run_1', 'weather_primary', 'v2', 'mkt_2', 'tok_no', 'NO', 'BUY', 0.42, 0.55, 600, 500, 'FAK', 10.0, 1710000000100, 'frun_1', 'snap_watch', 'req_watch', 'hash_watch', '{}', '2026-03-15 09:00:20', 'wallet_weather_1', 'ectx_watch')
            """
        )
        con.execute(
            """
            INSERT INTO runtime.submit_attempts (
                attempt_id, request_id, ticket_id, order_id, wallet_id, execution_context_id,
                exchange, attempt_kind, attempt_mode, canonical_order_hash, payload_hash,
                submit_payload_json, signed_payload_ref, status, error, created_at
            ) VALUES
                ('submit_exec', 'submit_req_exec', 'tt_exec', 'ord_exec', 'wallet_weather_1', 'ectx_exec', 'polymarket_clob', 'submit_order', 'live_submit', 'coh_exec', 'ph_exec', '{}', 'sign_exec', 'accepted', NULL, '2026-03-15 09:00:12')
            """
        )
        if improve_second_snapshot:
            con.execute(
                """
                INSERT INTO runtime.submit_attempts (
                    attempt_id, request_id, ticket_id, order_id, wallet_id, execution_context_id,
                    exchange, attempt_kind, attempt_mode, canonical_order_hash, payload_hash,
                    submit_payload_json, signed_payload_ref, status, error, created_at
                ) VALUES
                    ('submit_watch', 'submit_req_watch', 'tt_watch', 'ord_watch', 'wallet_weather_1', 'ectx_watch', 'polymarket_clob', 'submit_order', 'live_submit', 'coh_watch', 'ph_watch', '{}', 'sign_watch', 'accepted', NULL, '2026-03-15 09:00:22')
                """
            )
        con.execute(
            """
            INSERT INTO trading.fills (
                fill_id, order_id, wallet_id, market_id, token_id, outcome, side, price, size, fee,
                fee_rate_bps, trade_id, exchange_order_id, filled_at
            ) VALUES
                ('fill_exec', 'ord_exec', 'wallet_weather_1', 'mkt_1', 'tok_yes', 'YES', 'BUY', 0.41, 10.0, 0.10, 30, 'trade_exec', 'paper_exec', '2026-03-15 09:01:00')
            """
        )
        if improve_second_snapshot:
            con.execute(
                """
                INSERT INTO trading.fills (
                    fill_id, order_id, wallet_id, market_id, token_id, outcome, side, price, size, fee,
                    fee_rate_bps, trade_id, exchange_order_id, filled_at
                ) VALUES
                    ('fill_watch', 'ord_watch', 'wallet_weather_1', 'mkt_2', 'tok_no', 'NO', 'BUY', 0.43, 10.0, 0.10, 30, 'trade_watch', 'paper_watch', '2026-03-15 09:01:10')
                """
            )
        con.execute(
            """
            INSERT INTO resolution.settlement_verifications (
                verification_id, proposal_id, market_id, proposed_outcome, expected_outcome, is_correct,
                confidence, discrepancy_details, sources_checked, evidence_package, created_at
            ) VALUES
                ('ver_exec', 'prop_exec', 'mkt_1', 'YES', 'YES', TRUE, 0.95, NULL, '[]', '{}', '2026-03-16 10:00:00')
            """
        )
        if improve_second_snapshot:
            con.execute(
                """
                INSERT INTO resolution.settlement_verifications (
                    verification_id, proposal_id, market_id, proposed_outcome, expected_outcome, is_correct,
                    confidence, discrepancy_details, sources_checked, evidence_package, created_at
                ) VALUES
                    ('ver_watch', 'prop_watch', 'mkt_2', 'NO', 'NO', TRUE, 0.95, NULL, '[]', '{}', '2026-03-16 10:05:00')
                """
            )
        con.execute(
            """
            INSERT INTO weather.weather_forecast_replay_diffs (
                diff_id, replay_id, entity_type, entity_key, original_entity_id, replayed_entity_id, status,
                diff_summary_json, created_at
            ) VALUES
                ('diff_exec', 'replay_exec', 'watch_only_snapshot', 'mkt_1:YES', 'snap_exec', 'snap_exec_replayed', 'DIFFERENT', '{"changed": true}', '2026-03-16 12:00:00')
            """
        )

    def _materialize(self, *, improve_second_snapshot: bool, baseline_version: str) -> tuple[object, list[object], object]:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / f"{baseline_version}.duckdb")
            self._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                self._seed_case(con, improve_second_snapshot=improve_second_snapshot)
                return materialize_ranking_retrospective(
                    con,
                    lookback_days=30,
                    baseline_version=baseline_version,
                    run_id=f"retro_{baseline_version}",
                    as_of=datetime(2026, 3, 20, 0, 0, 0),
                )
            finally:
                con.close()

    def test_compare_uplift_consumes_real_materialized_retrospective_outputs(self) -> None:
        _, baseline_rows, baseline_summary = self._materialize(
            improve_second_snapshot=False,
            baseline_version="baseline_v1",
        )
        _, candidate_rows, candidate_summary = self._materialize(
            improve_second_snapshot=True,
            baseline_version="candidate_v2",
        )

        uplift = compare_retrospective_uplift(
            baseline_summary=baseline_summary,
            candidate_summary=candidate_summary,
            baseline_rows=baseline_rows,
            candidate_rows=candidate_rows,
        )

        self.assertEqual(uplift["baseline_version"], "baseline_v1")
        self.assertEqual(uplift["candidate_version"], "candidate_v2")
        self.assertTrue(uplift["candidate_outperformed"])
        self.assertGreaterEqual(float(uplift["top_decile_fill_capture_uplift"]), 0.0)
        self.assertGreaterEqual(float(uplift["top_decile_resolution_capture_uplift"]), 0.0)
        self.assertGreaterEqual(float(uplift["top_row_fill_capture_uplift"]), 0.0)


if __name__ == "__main__":
    unittest.main()
