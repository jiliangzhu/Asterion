from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from asterion_core.ui import build_ui_lite_db_once
from tests import test_predicted_vs_realized_summary as predicted_vs_realized_summary_test


HAS_DUCKDB = predicted_vs_realized_summary_test.HAS_DUCKDB


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required")
class PostTradeAnalyticsProjectionTest(unittest.TestCase):
    def test_watch_only_vs_executed_and_market_research_and_calibration_health(self) -> None:
        helper = predicted_vs_realized_summary_test.PredictedVsRealizedSummaryTest()
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            lite_path = str(Path(tmpdir) / "ui_lite.duckdb")
            helper._prepare_db(db_path, with_resolution=True)
            con = duckdb.connect(db_path)
            try:
                con.execute(
                    """
                    INSERT INTO weather.weather_markets (
                        market_id, condition_id, event_id, slug, title, description, rules, status, active, closed, archived,
                        accepting_orders, enable_order_book, tags_json, outcomes_json, token_ids_json, close_time, end_date,
                        raw_market_json, created_at, updated_at
                    ) VALUES (
                        'mkt_1', 'cond_1', 'evt_1', 'sea-temp', 'Seattle temperature',
                        'desc', 'rules', 'active', TRUE, FALSE, FALSE, TRUE, TRUE,
                        ?, ?, ?, '2026-03-15 23:00:00', '2026-03-15 23:00:00', ?, '2026-03-15 08:00:00', '2026-03-15 08:00:00'
                    )
                    """,
                    [
                        '["weather"]',
                        '["YES","NO"]',
                        '["tok_yes","tok_no"]',
                        '{"slug":"sea-temp"}',
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
                        'mkt_1', 'cond_1', 'Seattle', 'KSEA', 47.6062, -122.3321, 'America/Los_Angeles', '2026-03-15',
                        'daily_max', 'temperature_max', 'fahrenheit', 40.0, 49.0, 'weather.com',
                        ?, 'identity', TRUE, 'spec_v1', 0.95, ?, '2026-03-15 08:00:00', '2026-03-15 08:00:00'
                    )
                    """,
                    ['["openmeteo","nws"]', "[]"],
                )
                con.execute(
                    """
                    INSERT INTO weather.weather_forecast_runs (
                        run_id, market_id, condition_id, station_id, source, model_run, forecast_target_time, observation_date,
                        metric, latitude, longitude, timezone, spec_version, cache_key, source_trace_json, fallback_used,
                        from_cache, confidence, forecast_payload_json, raw_payload_json, created_at
                    ) VALUES (
                        'frun_1', 'mkt_1', 'cond_1', 'KSEA', 'openmeteo', '2026-03-15T00:00Z',
                        '2026-03-15 12:00:00', '2026-03-15', 'temperature_max', 47.6062, -122.3321, 'America/Los_Angeles',
                        'spec_v1', 'cache_1', ?, FALSE, FALSE, 0.98, ?, ?, '2026-03-15 08:30:00'
                    )
                    """,
                    ['["openmeteo"]', '{"distribution":{"42":0.5,"43":0.5}}', '{"raw":"payload"}'],
                )
                con.execute(
                    """
                    INSERT INTO weather.weather_fair_values (
                        fair_value_id, run_id, market_id, condition_id, token_id, outcome, fair_value, confidence, priced_at
                    ) VALUES (
                        'fv_1', 'frun_1', 'mkt_1', 'cond_1', 'tok_yes', 'YES', 0.55, 0.98, '2026-03-15 08:31:00'
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO weather.weather_watch_only_snapshots (
                        snapshot_id, fair_value_id, run_id, market_id, condition_id, token_id, outcome, reference_price,
                        fair_value, edge_bps, threshold_bps, decision, side, rationale, pricing_context_json, created_at
                    ) VALUES (
                        'snap_1', 'fv_1', 'frun_1', 'mkt_1', 'cond_1', 'tok_yes', 'YES',
                        0.40, 0.55, 900, 100, 'TAKE', 'BUY', 'edge', ?, '2026-03-15 08:32:00'
                    )
                    """,
                    [
                        '{"model_fair_value":0.55,"execution_adjusted_fair_value":0.534,"edge_bps_model":1500,"edge_bps_executable":900,"fees_bps":0,"slippage_bps":40,"liquidity_penalty_bps":25,"mapping_confidence":0.92,"source_freshness_status":"fresh","market_quality_status":"pass","price_staleness_ms":60000}'
                    ],
                )
                con.execute(
                    """
                    INSERT INTO weather.forecast_calibration_samples (
                        sample_id, market_id, station_id, source, forecast_horizon_bucket, season_bucket, metric,
                        forecast_target_time, forecast_mean, observed_value, residual, created_at
                    ) VALUES
                    ('sample_1', 'mkt_1', 'KSEA', 'openmeteo', '0-1', 'winter', 'temperature_f', '2026-03-15 12:00:00', 42.0, 43.0, 1.0, '2026-03-15 13:00:00'),
                    ('sample_2', 'mkt_2', 'KSEA', 'openmeteo', '0-1', 'winter', 'temperature_f', '2026-03-16 12:00:00', 41.0, 43.0, 2.0, '2026-03-16 13:00:00'),
                    ('sample_3', 'mkt_3', 'KSEA', 'openmeteo', '0-1', 'winter', 'temperature_f', '2026-03-17 12:00:00', 40.0, 43.0, 3.0, '2026-03-17 13:00:00'),
                    ('sample_4', 'mkt_4', 'KSEA', 'openmeteo', '0-1', 'winter', 'temperature_f', '2026-03-18 12:00:00', 39.0, 43.0, 4.0, '2026-03-18 13:00:00'),
                    ('sample_5', 'mkt_5', 'KSEA', 'openmeteo', '0-1', 'winter', 'temperature_f', '2026-03-19 12:00:00', 38.0, 43.0, 5.0, '2026-03-19 13:00:00')
                    """
                )
            finally:
                con.close()

            result = build_ui_lite_db_once(src_db_path=db_path, dst_db_path=lite_path)
            self.assertTrue(result.ok, result.error)

            con = duckdb.connect(lite_path)
            try:
                capture = con.execute(
                    """
                    SELECT market_id, executed_ticket_count, execution_capture_ratio, miss_reason_bucket
                    FROM ui.watch_only_vs_executed_summary
                    WHERE market_id = 'mkt_1'
                    """
                ).fetchone()
                research = con.execute(
                    """
                    SELECT market_id, executed_evidence_status, resolved_trade_count
                    FROM ui.market_research_summary
                    WHERE market_id = 'mkt_1'
                    """
                ).fetchone()
                calibration = con.execute(
                    """
                    SELECT station_id, source, sample_count, calibration_health_status
                    FROM ui.calibration_health_summary
                    WHERE station_id = 'KSEA' AND source = 'openmeteo'
                    """
                ).fetchone()
            finally:
                con.close()

        self.assertEqual(capture, ("mkt_1", 1, 1.0, "captured"))
        self.assertEqual(research, ("mkt_1", "executed", 1))
        self.assertEqual(calibration, ("KSEA", "openmeteo", 5, "watch"))
