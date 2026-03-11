from __future__ import annotations

import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from asterion_core.monitoring import (
    ReadinessConfig,
    ReadinessReport,
    evaluate_p3_readiness,
    write_readiness_report,
)
from asterion_core.storage.db_migrate import MigrationConfig, apply_migrations
from asterion_core.ui import build_ui_lite_db_once, refresh_ui_db_replica_once, validate_ui_lite_db


HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None
ROOT = Path(__file__).resolve().parents[1]


def _apply_schema(db_path: str) -> None:
    migrations_dir = ROOT / "sql" / "migrations"
    with patch.dict(
        os.environ,
        {
            "ASTERION_STRICT_SINGLE_WRITER": "1",
            "ASTERION_DB_ROLE": "writer",
            "WRITERD": "1",
        },
        clear=False,
    ):
        apply_migrations(MigrationConfig(db_path=db_path, migrations_dir=str(migrations_dir)))


def _seed_canonical_state(db_path: str) -> None:
    import duckdb

    con = duckdb.connect(db_path)
    try:
        con.execute(
            """
            INSERT INTO weather.weather_markets (
                market_id, condition_id, event_id, slug, title, description, rules, status, active, closed, archived,
                accepting_orders, enable_order_book, tags_json, outcomes_json, token_ids_json, close_time, end_date,
                raw_market_json, created_at, updated_at
            ) VALUES (
                'mkt_weather_1', 'cond_weather_1', 'evt_1', 'nyc-temp', 'NYC temperature',
                'desc', 'rules', 'active', TRUE, FALSE, FALSE, TRUE, TRUE,
                ?, ?, ?, '2026-03-12 00:00:00', '2026-03-12 00:00:00', ?, '2026-03-10 00:00:00', '2026-03-10 00:00:00'
            )
            """,
            [
                json.dumps(["weather"]),
                json.dumps(["YES", "NO"]),
                json.dumps(["tok_yes", "tok_no"]),
                json.dumps({"slug": "nyc-temp"}),
            ],
        )
        con.execute(
            """
            INSERT INTO weather.weather_market_specs (
                market_id, condition_id, location_name, station_id, latitude, longitude, timezone, observation_date,
                observation_window_local, metric, unit, bucket_min_value, bucket_max_value, authoritative_source,
                fallback_sources, rounding_rule, inclusive_bounds, spec_version, parse_confidence, risk_flags_json,
                created_at, updated_at
            ) VALUES (
                'mkt_weather_1', 'cond_weather_1', 'New York City', 'KNYC', 40.7128, -74.0060, 'America/New_York',
                '2026-03-12', 'daily_max', 'temperature_max', 'fahrenheit', 50.0, 59.0, 'weather.com',
                ?, 'identity', TRUE, 'spec_v1', 0.95, ?, '2026-03-10 00:00:00', '2026-03-10 00:00:00'
            )
            """,
            [json.dumps(["openmeteo", "nws"]), json.dumps([])],
        )
        con.execute(
            """
            INSERT INTO weather.weather_forecast_runs (
                run_id, market_id, condition_id, station_id, source, model_run, forecast_target_time, observation_date,
                metric, latitude, longitude, timezone, spec_version, cache_key, source_trace_json, fallback_used,
                from_cache, confidence, forecast_payload_json, raw_payload_json, created_at
            ) VALUES
            (
                'frun_weather_1', 'mkt_weather_1', 'cond_weather_1', 'KNYC', 'openmeteo', '2026-03-10T00:00Z',
                '2026-03-10 00:00:00', '2026-03-12', 'temperature_max', 40.7128, -74.0060, 'America/New_York',
                'spec_v1', 'fck_1', ?, FALSE, FALSE, 0.98, ?, ?, '2026-03-10 00:00:00'
            ),
            (
                'frun_replayed', 'mkt_weather_1', 'cond_weather_1', 'KNYC', 'openmeteo', '2026-03-10T00:00Z',
                '2026-03-10 00:00:00', '2026-03-12', 'temperature_max', 40.7128, -74.0060, 'America/New_York',
                'spec_v1', 'fck_1', ?, FALSE, FALSE, 0.98, ?, ?, '2026-03-10 01:00:00'
            )
            """,
            [
                json.dumps(["openmeteo"]),
                json.dumps({"temperature_distribution": {"55": 1.0}}),
                json.dumps({"provider": "openmeteo"}),
                json.dumps(["openmeteo"]),
                json.dumps({"temperature_distribution": {"55": 1.0}}),
                json.dumps({"provider": "openmeteo", "replay": True}),
            ],
        )
        con.execute(
            """
            INSERT INTO weather.weather_forecast_replays (
                replay_id, market_id, condition_id, station_id, source, model_run, forecast_target_time,
                spec_version, replay_key, replay_reason, original_run_id, replayed_run_id, created_at
            ) VALUES (
                'freplay_1', 'mkt_weather_1', 'cond_weather_1', 'KNYC', 'openmeteo', '2026-03-10T00:00Z',
                '2026-03-10 00:00:00', 'spec_v1', 'mkt_weather_1|KNYC|spec_v1|openmeteo|2026-03-10T00:00Z',
                'cold_path_audit', 'frun_weather_1', 'frun_replayed', '2026-03-10 01:30:00'
            )
            """
        )
        con.execute(
            """
            INSERT INTO weather.weather_forecast_replay_diffs (
                diff_id, replay_id, entity_type, entity_key, original_entity_id, replayed_entity_id, status,
                diff_summary_json, created_at
            ) VALUES (
                'fdiff_1', 'freplay_1', 'forecast_run', 'mkt_weather_1:KNYC', 'frun_weather_1', 'frun_replayed',
                'MATCH', ?, '2026-03-10 01:30:00'
            )
            """,
            [json.dumps({"status": "MATCH"})],
        )
        con.execute(
            """
            INSERT INTO weather.weather_fair_values (
                fair_value_id, run_id, market_id, condition_id, token_id, outcome, fair_value, confidence, priced_at
            ) VALUES (
                'fv_yes_1', 'frun_replayed', 'mkt_weather_1', 'cond_weather_1', 'tok_yes', 'YES', 0.72, 0.98,
                '2026-03-10 01:00:00'
            )
            """
        )
        con.execute(
            """
            INSERT INTO weather.weather_watch_only_snapshots (
                snapshot_id, fair_value_id, run_id, market_id, condition_id, token_id, outcome, reference_price,
                fair_value, edge_bps, threshold_bps, decision, side, rationale, pricing_context_json, created_at
            ) VALUES (
                'wsnap_1', 'fv_yes_1', 'frun_replayed', 'mkt_weather_1', 'cond_weather_1', 'tok_yes', 'YES',
                0.50, 0.72, 2200, 100, 'TAKE', 'BUY', 'pricing_edge', ?, '2026-03-10 01:05:00'
            )
            """,
            [json.dumps({"forecast_run_id": "frun_replayed"})],
        )
        con.execute(
            """
            INSERT INTO resolution.uma_proposals (
                proposal_id, market_id, condition_id, proposer, proposed_outcome, proposal_bond, dispute_bond,
                proposal_tx_hash, proposal_block_number, proposal_timestamp, status, on_chain_settled_at,
                safe_redeem_after, human_review_required, created_at, updated_at
            ) VALUES (
                'prop_1', 'mkt_weather_1', 'cond_weather_1', '0xabc', 'YES', 100.0, NULL, '0xhash', 100,
                '2026-03-09 00:00:00', 'settled', '2026-03-10 00:00:00', '2026-03-10 04:00:00', FALSE,
                '2026-03-09 00:00:00', '2026-03-10 00:00:00'
            )
            """
        )
        con.execute(
            """
            INSERT INTO resolution.block_watermarks (
                chain_id, last_processed_block, last_finalized_block, updated_at
            ) VALUES (137, 120, 120, '2026-03-10 00:30:00')
            """
        )
        con.execute(
            """
            INSERT INTO resolution.watcher_continuity_checks (
                check_id, chain_id, from_block, to_block, last_known_finalized_block, status, gap_count, details_json, created_at
            ) VALUES (
                'wcheck_1', 137, 110, 120, 120, 'OK', 0, ?, '2026-03-10 00:40:00'
            )
            """,
            [json.dumps({"rpc": "primary"})],
        )
        con.execute(
            """
            INSERT INTO resolution.settlement_verifications (
                verification_id, proposal_id, market_id, proposed_outcome, expected_outcome, is_correct,
                confidence, discrepancy_details, sources_checked, evidence_package, created_at
            ) VALUES (
                'verify_1', 'prop_1', 'mkt_weather_1', 'YES', 'YES', TRUE, 0.97, NULL, ?, ?, '2026-03-10 00:50:00'
            )
            """,
            [json.dumps(["weather.com"]), json.dumps({"evidence_package_id": "evidence_1"})],
        )
        con.execute(
            """
            INSERT INTO resolution.proposal_evidence_links (
                proposal_id, verification_id, evidence_package_id, linked_at
            ) VALUES ('prop_1', 'verify_1', 'evidence_1', '2026-03-10 00:55:00')
            """
        )
        con.execute(
            """
            INSERT INTO resolution.redeem_readiness_suggestions (
                suggestion_id, proposal_id, decision, reason, on_chain_settled_at, safe_redeem_after,
                human_review_required, created_at
            ) VALUES (
                'redeem_1', 'prop_1', 'ready_for_redeem', 'settlement verified', '2026-03-10 00:00:00',
                '2026-03-10 04:00:00', FALSE, '2026-03-10 01:00:00'
            )
            """
        )
        con.execute(
            """
            INSERT INTO runtime.strategy_runs (
                run_id, data_snapshot_id, universe_snapshot_id, asof_ts_ms, dq_level, strategy_ids_json,
                decision_count, created_at
            ) VALUES (
                'srun_1', 'snap_1', 'uni_1', 1710020000000, 'PASS', ?, 2, '2026-03-10 01:10:00'
            )
            """,
            [json.dumps(["weather_primary"])],
        )
        con.execute(
            """
            INSERT INTO runtime.trade_tickets (
                ticket_id, run_id, strategy_id, strategy_version, market_id, token_id, outcome, side, reference_price,
                fair_value, edge_bps, threshold_bps, route_action, size, signal_ts_ms, forecast_run_id,
                watch_snapshot_id, request_id, ticket_hash, provenance_json, created_at
            ) VALUES (
                'tt_1', 'srun_1', 'weather_primary', 'v1', 'mkt_weather_1', 'tok_yes', 'YES', 'buy', 0.50, 0.72,
                2200, 100, 'fak', 10.0, 1710020000000, 'frun_replayed', 'wsnap_1', 'req_1', 'thash_1', ?,
                '2026-03-10 01:11:00'
            ), (
                'tt_2', 'srun_1', 'weather_primary', 'v1', 'mkt_weather_1', 'tok_no', 'NO', 'sell', 0.48, 0.31,
                -1700, 100, 'fok', 8.0, 1710020001000, 'frun_replayed', 'wsnap_2', 'req_2', 'thash_2', ?,
                '2026-03-10 01:11:30'
            )
            """,
            [json.dumps({"watch_snapshot_id": "wsnap_1"}), json.dumps({"watch_snapshot_id": "wsnap_2"})],
        )
        con.execute(
            """
            INSERT INTO runtime.gate_decisions (
                gate_id, ticket_id, allowed, reason, reason_codes_json, metrics_json, created_at
            ) VALUES (
                'gate_1', 'tt_1', TRUE, 'passed', ?, ?, '2026-03-10 01:12:00'
            ), (
                'gate_2', 'tt_2', FALSE, 'inventory_gate', ?, ?, '2026-03-10 01:12:30'
            )
            """,
            [
                json.dumps(["passed"]),
                json.dumps({"edge_bps": 2200}),
                json.dumps(["insufficient_inventory"]),
                json.dumps({"edge_bps": -1700}),
            ],
        )
        con.execute(
            """
            INSERT INTO trading.orders (
                order_id, client_order_id, wallet_id, market_id, token_id, outcome, side, price, size, route_action,
                time_in_force, expiration, fee_rate_bps, signature_type, funder, status, filled_size, remaining_size,
                avg_fill_price, reservation_id, exchange_order_id, created_at, submitted_at, updated_at
            ) VALUES (
                'ordr_1', 'ord_1', 'wallet_weather_1', 'mkt_weather_1', 'tok_yes', 'YES', 'buy', 0.50, 10.0, 'fak',
                'fak', NULL, 30, 1, '0xfunder', 'created', 0.0, 10.0, NULL, 'res_1', NULL,
                '2026-03-10 01:13:00', '2026-03-10 01:13:00', '2026-03-10 01:13:00'
            ), (
                'ordr_2', 'ord_2', 'wallet_weather_1', 'mkt_weather_1', 'tok_no', 'NO', 'sell', 0.48, 8.0, 'fok',
                'fok', NULL, 30, 1, '0xfunder', 'created', 0.0, 8.0, NULL, 'res_2', NULL,
                '2026-03-10 01:13:30', '2026-03-10 01:13:30', '2026-03-10 01:13:30'
            )
            """
        )
        con.execute(
            """
            INSERT INTO trading.reservations (
                reservation_id, order_id, wallet_id, asset_type, token_id, market_id, outcome, funder, signature_type,
                reserved_quantity, remaining_quantity, reserved_notional, status, created_at, updated_at
            ) VALUES (
                'res_1', 'ordr_1', 'wallet_weather_1', 'usdc_e', 'usdc_e', 'mkt_weather_1', 'YES',
                '0xfunder', 1, 10.0, 10.0, 5.0, 'active', '2026-03-10 01:13:00', '2026-03-10 01:13:00'
            ), (
                'res_2', 'ordr_2', 'wallet_weather_1', 'erc1155_token', 'tok_no', 'mkt_weather_1', 'NO',
                '0xfunder', 1, 8.0, 8.0, 3.84, 'active', '2026-03-10 01:13:30', '2026-03-10 01:13:30'
            )
            """
        )
        con.execute(
            """
            INSERT INTO trading.inventory_positions (
                wallet_id, asset_type, token_id, market_id, outcome, balance_type, quantity, funder, signature_type, updated_at
            ) VALUES (
                'wallet_weather_1', 'erc1155_token', 'tok_yes', 'mkt_weather_1', 'YES', 'available', 50.0,
                '0xfunder', 1, '2026-03-10 01:13:00'
            )
            """
        )
        con.execute(
            """
            INSERT INTO trading.exposure_snapshots (
                snapshot_id, wallet_id, funder, signature_type, market_id, token_id, outcome, open_order_size,
                reserved_notional_usdc, filled_position_size, settled_position_size, redeemable_size, captured_at
            ) VALUES (
                'exposure_1', 'wallet_weather_1', '0xfunder', 1, 'mkt_weather_1', 'tok_yes', 'YES',
                10.0, 5.0, 0.0, 0.0, 0.0, '2026-03-10 01:14:00'
            )
            """
        )
        con.execute(
            """
            INSERT INTO trading.fills (
                fill_id, order_id, wallet_id, market_id, token_id, outcome, side, price, size, fee,
                fee_rate_bps, trade_id, exchange_order_id, filled_at
            ) VALUES (
                'fill_1', 'ordr_1', 'wallet_weather_1', 'mkt_weather_1', 'tok_yes', 'YES', 'buy',
                0.50, 10.0, 0.15, 30, 'trade_1', 'paper_ordr_1', '2026-03-10 01:14:30'
            )
            """
        )
        con.execute(
            """
            INSERT INTO trading.order_state_transitions (
                transition_id, order_id, from_status, to_status, reason, timestamp
            ) VALUES
            ('otrans_1', 'ordr_1', 'created', 'posted', 'paper_adapter_posted', '2026-03-10 01:13:10'),
            ('otrans_2', 'ordr_1', 'posted', 'filled', 'paper_fill_full', '2026-03-10 01:14:30')
            """
        )
        con.execute(
            """
            INSERT INTO trading.reconciliation_results (
                reconciliation_id, wallet_id, funder, signature_type, asset_type, token_id, market_id, balance_type,
                local_quantity, remote_quantity, discrepancy, status, resolution, created_at
            ) VALUES (
                'recon_1', 'wallet_weather_1', '0xfunder', 1, 'outcome_token', 'tok_yes', 'mkt_weather_1', 'settled',
                10.0, 10.0, 0.0, 'ok', 'paper_local_match', '2026-03-10 01:15:00'
            )
            """
        )
        con.execute(
            """
            INSERT INTO runtime.journal_events (
                event_id, event_type, entity_type, entity_id, run_id, payload_json, created_at
            ) VALUES
            ('jevt_ticket_1', 'trade_ticket.created', 'trade_ticket', 'tt_1', 'srun_1', ?, '2026-03-10 01:11:00'),
            ('jevt_ticket_2', 'trade_ticket.created', 'trade_ticket', 'tt_2', 'srun_1', ?, '2026-03-10 01:11:30'),
            ('jevt_order_1', 'order.created', 'order', 'ordr_1', 'srun_1', ?, '2026-03-10 01:13:00'),
            ('jevt_order_2', 'order.created', 'order', 'ordr_2', 'srun_1', ?, '2026-03-10 01:13:30'),
            ('jevt_res_1', 'reservation.created', 'reservation', 'res_1', 'srun_1', ?, '2026-03-10 01:13:00'),
            ('jevt_res_2', 'reservation.created', 'reservation', 'res_2', 'srun_1', ?, '2026-03-10 01:13:30')
            """,
            [
                json.dumps({"request_id": "req_1", "ticket_id": "tt_1"}),
                json.dumps({"request_id": "req_2", "ticket_id": "tt_2"}),
                json.dumps({"client_order_id": "ord_1", "request_id": "req_1", "ticket_id": "tt_1"}),
                json.dumps({"client_order_id": "ord_2", "request_id": "req_2", "ticket_id": "tt_2"}),
                json.dumps({"reservation_id": "res_1", "request_id": "req_1", "ticket_id": "tt_1"}),
                json.dumps({"reservation_id": "res_2", "request_id": "req_2", "ticket_id": "tt_2"}),
            ],
        )
        con.execute(
            """
            INSERT INTO agent.invocations (
                invocation_id, agent_type, agent_version, prompt_version, subject_type, subject_id, input_hash,
                model_provider, model_name, status, started_at, ended_at, latency_ms, error_message, input_payload_json
            ) VALUES (
                'ainv_1', 'rule2spec', 'v1', 'p1', 'weather_market', 'mkt_weather_1', 'hash_1',
                'fake', 'fake-model', 'success', '2026-03-10 01:20:00', '2026-03-10 01:20:01', 1000, NULL, ?
            )
            """,
            [json.dumps({"market_id": "mkt_weather_1"})],
        )
        con.execute(
            """
            INSERT INTO agent.outputs (
                output_id, invocation_id, verdict, confidence, summary, findings_json, structured_output_json,
                human_review_required, created_at
            ) VALUES (
                'aout_1', 'ainv_1', 'pass', 0.9, 'looks good', ?, ?, FALSE, '2026-03-10 01:20:01'
            )
            """,
            [json.dumps([]), json.dumps({"verdict": "pass"})],
        )
        con.execute(
            """
            INSERT INTO agent.reviews (
                review_id, invocation_id, review_status, reviewer_id, review_notes, review_payload_json, reviewed_at
            ) VALUES (
                'arev_1', 'ainv_1', 'approved', 'operator_1', 'approved', ?, '2026-03-10 01:30:00'
            )
            """,
            [json.dumps({"review": "approved"})],
        )
        con.execute(
            """
            INSERT INTO agent.evaluations (
                evaluation_id, invocation_id, verification_method, score_json, is_verified, notes, created_at
            ) VALUES (
                'aeval_1', 'ainv_1', 'human_ground_truth', ?, TRUE, 'verified', '2026-03-10 01:40:00'
            )
            """,
            [json.dumps({"station_id_match": True})],
        )
    finally:
        con.close()


def _prepare_replica_and_lite(
    *,
    db_path: str,
    replica_db_path: str,
    replica_meta_path: str,
    lite_db_path: str,
    lite_meta_path: str,
    report_json_path: str | None = None,
) -> None:
    with patch.dict(os.environ, {"ASTERION_UI_REPLICA_COPY_MODE": "copy"}, clear=False):
        result = refresh_ui_db_replica_once(
            src_db_path=db_path,
            dst_db_path=replica_db_path,
            meta_path=replica_meta_path,
        )
    assert result.ok, result.error
    lite_result = build_ui_lite_db_once(
        src_db_path=replica_db_path,
        dst_db_path=lite_db_path,
        meta_path=lite_meta_path,
        readiness_report_json_path=report_json_path,
    )
    assert lite_result.ok, lite_result.error


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required for P2 closeout tests")
class ReadinessAndUiLiteIntegrationTest(unittest.TestCase):
    def test_readiness_returns_no_go_when_required_table_is_missing(self) -> None:
        import duckdb

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            _apply_schema(db_path)
            _seed_canonical_state(db_path)
            con = duckdb.connect(db_path)
            try:
                con.execute("DROP TABLE weather.weather_forecast_replays")
            finally:
                con.close()
            replica_db = str(Path(tmpdir) / "ui.duckdb")
            replica_meta = str(Path(tmpdir) / "ui.meta.json")
            lite_db = str(Path(tmpdir) / "ui_lite.duckdb")
            lite_meta = str(Path(tmpdir) / "ui_lite.meta.json")
            with patch.dict(os.environ, {"ASTERION_UI_REPLICA_COPY_MODE": "copy"}, clear=False):
                refresh_ui_db_replica_once(src_db_path=db_path, dst_db_path=replica_db, meta_path=replica_meta)
            report = evaluate_p3_readiness(
                ReadinessConfig(
                    db_path=db_path,
                    ui_replica_db_path=replica_db,
                    ui_replica_meta_path=replica_meta,
                    ui_lite_db_path=lite_db,
                    ui_lite_meta_path=lite_meta,
                )
            )
            self.assertEqual(report.go_decision, "NO-GO")
            self.assertIn("cold_path_determinism", report.decision_reason)
            cold_gate = next(item for item in report.gate_results if item.gate_name == "cold_path_determinism")
            self.assertTrue(any("weather.weather_forecast_replays" in item for item in cold_gate.violations))

    def test_readiness_fails_when_latest_continuity_is_rpc_incomplete(self) -> None:
        import duckdb

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            _apply_schema(db_path)
            _seed_canonical_state(db_path)
            con = duckdb.connect(db_path)
            try:
                con.execute("UPDATE resolution.watcher_continuity_checks SET status = 'RPC_INCOMPLETE' WHERE check_id = 'wcheck_1'")
            finally:
                con.close()
            replica_db = str(Path(tmpdir) / "ui.duckdb")
            replica_meta = str(Path(tmpdir) / "ui.meta.json")
            lite_db = str(Path(tmpdir) / "ui_lite.duckdb")
            lite_meta = str(Path(tmpdir) / "ui_lite.meta.json")
            _prepare_replica_and_lite(
                db_path=db_path,
                replica_db_path=replica_db,
                replica_meta_path=replica_meta,
                lite_db_path=lite_db,
                lite_meta_path=lite_meta,
            )
            report = evaluate_p3_readiness(
                ReadinessConfig(
                    db_path=db_path,
                    ui_replica_db_path=replica_db,
                    ui_replica_meta_path=replica_meta,
                    ui_lite_db_path=lite_db,
                    ui_lite_meta_path=lite_meta,
                )
            )
            self.assertEqual(report.go_decision, "NO-GO")
            cold_gate = next(item for item in report.gate_results if item.gate_name == "cold_path_determinism")
            self.assertTrue(any("RPC_INCOMPLETE" in item for item in cold_gate.violations))

    def test_readiness_fails_when_ui_lite_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            _apply_schema(db_path)
            _seed_canonical_state(db_path)
            replica_db = str(Path(tmpdir) / "ui.duckdb")
            replica_meta = str(Path(tmpdir) / "ui.meta.json")
            with patch.dict(os.environ, {"ASTERION_UI_REPLICA_COPY_MODE": "copy"}, clear=False):
                result = refresh_ui_db_replica_once(src_db_path=db_path, dst_db_path=replica_db, meta_path=replica_meta)
            self.assertTrue(result.ok, result.error)
            report = evaluate_p3_readiness(
                ReadinessConfig(
                    db_path=db_path,
                    ui_replica_db_path=replica_db,
                    ui_replica_meta_path=replica_meta,
                    ui_lite_db_path=str(Path(tmpdir) / "ui_lite.duckdb"),
                    ui_lite_meta_path=str(Path(tmpdir) / "ui_lite.meta.json"),
                )
            )
            self.assertEqual(report.go_decision, "NO-GO")
            operator_gate = next(item for item in report.gate_results if item.gate_name == "operator_surface")
            self.assertFalse(operator_gate.passed)
            self.assertTrue(any("UI lite" in item for item in operator_gate.violations))

    def test_readiness_fails_when_reconciliation_mismatches_present(self) -> None:
        import duckdb

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            _apply_schema(db_path)
            _seed_canonical_state(db_path)
            con = duckdb.connect(db_path)
            try:
                con.execute("UPDATE trading.reconciliation_results SET status = 'inventory_mismatch', discrepancy = 1.0")
            finally:
                con.close()
            replica_db = str(Path(tmpdir) / "ui.duckdb")
            replica_meta = str(Path(tmpdir) / "ui.meta.json")
            lite_db = str(Path(tmpdir) / "ui_lite.duckdb")
            lite_meta = str(Path(tmpdir) / "ui_lite.meta.json")
            _prepare_replica_and_lite(
                db_path=db_path,
                replica_db_path=replica_db,
                replica_meta_path=replica_meta,
                lite_db_path=lite_db,
                lite_meta_path=lite_meta,
            )
            report = evaluate_p3_readiness(
                ReadinessConfig(
                    db_path=db_path,
                    ui_replica_db_path=replica_db,
                    ui_replica_meta_path=replica_meta,
                    ui_lite_db_path=lite_db,
                    ui_lite_meta_path=lite_meta,
                )
            )
            self.assertEqual(report.go_decision, "NO-GO")
            portfolio_gate = next(item for item in report.gate_results if item.gate_name == "portfolio_reconciliation")
            self.assertFalse(portfolio_gate.passed)
            self.assertTrue(any("reconciliation mismatches present" in item for item in portfolio_gate.violations))

    def test_all_gates_pass_and_readiness_report_flows_into_ui_phase_summary(self) -> None:
        import duckdb

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            replica_db = str(Path(tmpdir) / "asterion_ui.duckdb")
            replica_meta = str(Path(tmpdir) / "asterion_ui.meta.json")
            lite_db = str(Path(tmpdir) / "asterion_ui_lite.duckdb")
            lite_meta = str(Path(tmpdir) / "asterion_ui_lite.meta.json")
            report_json = str(Path(tmpdir) / "asterion_readiness_p3.json")
            report_md = str(Path(tmpdir) / "asterion_readiness_p3.md")
            _apply_schema(db_path)
            _seed_canonical_state(db_path)
            _prepare_replica_and_lite(
                db_path=db_path,
                replica_db_path=replica_db,
                replica_meta_path=replica_meta,
                lite_db_path=lite_db,
                lite_meta_path=lite_meta,
                report_json_path=report_json,
            )
            report = evaluate_p3_readiness(
                ReadinessConfig(
                    db_path=db_path,
                    ui_replica_db_path=replica_db,
                    ui_replica_meta_path=replica_meta,
                    ui_lite_db_path=lite_db,
                    ui_lite_meta_path=lite_meta,
                    readiness_report_json_path=report_json,
                )
            )
            self.assertEqual(report.go_decision, "GO")
            self.assertIn("ready for P4 planning only", report.decision_reason)
            write_readiness_report(report, json_path=report_json, markdown_path=report_md)
            rebuilt = build_ui_lite_db_once(
                src_db_path=replica_db,
                dst_db_path=lite_db,
                meta_path=lite_meta,
                readiness_report_json_path=report_json,
            )
            self.assertTrue(rebuilt.ok, rebuilt.error)
            self.assertTrue(Path(report_md).exists())
            report_roundtrip = ReadinessReport.from_dict(json.loads(Path(report_json).read_text(encoding="utf-8")))
            self.assertEqual(report_roundtrip.go_decision, "GO")
            lite_counts = validate_ui_lite_db(lite_db)
            self.assertGreaterEqual(lite_counts["ui.market_watch_summary"], 1)
            self.assertGreaterEqual(lite_counts["ui.proposal_resolution_summary"], 1)
            self.assertEqual(lite_counts["ui.execution_ticket_summary"], 2)
            self.assertGreaterEqual(lite_counts["ui.agent_review_summary"], 1)
            self.assertEqual(lite_counts["ui.phase_readiness_summary"], 6)
            con = duckdb.connect(lite_db, read_only=True)
            try:
                self.assertEqual(
                    con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'gold'").fetchone()[0],
                    0,
                )
                self.assertEqual(
                    con.execute("SELECT COUNT(*) FROM ui.phase_readiness_summary WHERE go_decision = 'GO'").fetchone()[0],
                    6,
                )
                rows = con.execute(
                    """
                    SELECT ticket_id, order_id, reservation_id
                    FROM ui.execution_ticket_summary
                    ORDER BY ticket_id
                    """
                ).fetchall()
                self.assertEqual(rows, [("tt_1", "ordr_1", "res_1"), ("tt_2", "ordr_2", "res_2")])
            finally:
                con.close()

    def test_ui_lite_build_failure_does_not_replace_previous_db(self) -> None:
        import duckdb

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            replica_db = str(Path(tmpdir) / "asterion_ui.duckdb")
            replica_meta = str(Path(tmpdir) / "asterion_ui.meta.json")
            lite_db = str(Path(tmpdir) / "asterion_ui_lite.duckdb")
            lite_meta = str(Path(tmpdir) / "asterion_ui_lite.meta.json")
            report_json = str(Path(tmpdir) / "asterion_readiness_p3.json")
            _apply_schema(db_path)
            _seed_canonical_state(db_path)
            _prepare_replica_and_lite(
                db_path=db_path,
                replica_db_path=replica_db,
                replica_meta_path=replica_meta,
                lite_db_path=lite_db,
                lite_meta_path=lite_meta,
                report_json_path=report_json,
            )
            before = validate_ui_lite_db(lite_db)
            Path(report_json).write_text("{bad json", encoding="utf-8")
            failed = build_ui_lite_db_once(
                src_db_path=replica_db,
                dst_db_path=lite_db,
                meta_path=lite_meta,
                readiness_report_json_path=report_json,
            )
            self.assertFalse(failed.ok)
            after = validate_ui_lite_db(lite_db)
            self.assertEqual(before, after)
            con = duckdb.connect(lite_db, read_only=True)
            try:
                self.assertGreaterEqual(con.execute("SELECT COUNT(*) FROM ui.market_watch_summary").fetchone()[0], 1)
            finally:
                con.close()


class ExitGateAuditTest(unittest.TestCase):
    def test_runtime_code_has_no_alphadesk_runtime_refs(self) -> None:
        roots = [
            ROOT / "asterion_core",
            ROOT / "agents",
            ROOT / "domains",
            ROOT / "dagster_asterion",
            ROOT / "sql",
        ]
        offenders: list[str] = []
        needles = ("import alphadesk", "from alphadesk", "ALPHADESK_")
        for base in roots:
            for path in base.rglob("*"):
                if not path.is_file():
                    continue
                if path.suffix not in {".py", ".md", ".sql", ".toml"}:
                    continue
                if "__pycache__" in path.parts:
                    continue
                text = path.read_text(encoding="utf-8", errors="ignore")
                for needle in needles:
                    if needle in text:
                        offenders.append(f"{path}:{needle}")
        self.assertEqual(offenders, [])

    def test_migration_ledger_has_no_pending_reusable_modules(self) -> None:
        ledger_path = ROOT / "docs" / "10-implementation" / "migration-ledger" / "AlphaDesk_Migration_Ledger.md"
        lines = ledger_path.read_text(encoding="utf-8").splitlines()
        pending: list[str] = []
        for line in lines:
            if not line.startswith("| `"):
                continue
            parts = [item.strip() for item in line.strip().strip("|").split("|")]
            if len(parts) < 5:
                continue
            source_module, _target, classification, status = parts[:4]
            if classification not in {"direct_reuse", "keep_shell_rewrite_content"}:
                continue
            if status not in {"ported", "do_not_port"}:
                pending.append(f"{source_module}:{status}")
        self.assertEqual(pending, [])

    def test_closeout_targets_exist(self) -> None:
        expected = [
            ROOT / "asterion_core" / "monitoring" / "readiness_checker_v1.py",
            ROOT / "asterion_core" / "ui" / "ui_lite_db.py",
            ROOT / "dagster_asterion" / "job_map.py",
            ROOT / "dagster_asterion" / "resources.py",
            ROOT / "dagster_asterion" / "schedules.py",
            ROOT / "docs" / "10-implementation" / "checklists" / "P2_Closeout_Checklist.md",
            ROOT / "docs" / "10-implementation" / "module-notes" / "AlphaDesk_readiness_checker_v1_Module_Note.md",
            ROOT / "docs" / "10-implementation" / "module-notes" / "AlphaDesk_ui_lite_db_Module_Note.md",
        ]
        missing = [str(path) for path in expected if not path.exists()]
        self.assertEqual(missing, [])


if __name__ == "__main__":
    unittest.main()
