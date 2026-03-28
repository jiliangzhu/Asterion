from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb

from domains.weather.opportunity.execution_priors import (
    WEATHER_EXECUTION_PRIOR_COLUMNS,
    execution_prior_row_to_row,
    load_execution_prior_summary,
    materialize_execution_priors,
)


HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required")
class ExecutionPriorsMaterializationTest(unittest.TestCase):
    def _apply_migrations(self, db_path: str) -> None:
        bootstrap = duckdb.connect(db_path)
        migrations_dir = Path(__file__).resolve().parents[1] / "sql" / "migrations"
        for path in sorted(migrations_dir.glob("*.sql")):
            sql = path.read_text(encoding="utf-8").strip()
            if sql:
                bootstrap.execute(sql)
        bootstrap.close()

    def _insert_market_spec(self, con) -> None:
        con.execute(
            """
            INSERT INTO weather.weather_markets (
                market_id, condition_id, event_id, slug, title, description, rules, status,
                active, closed, archived, accepting_orders, enable_order_book, tags_json,
                outcomes_json, token_ids_json, close_time, end_date, created_at, updated_at, raw_market_json
            ) VALUES (
                'mkt_exec', 'cond_exec', 'evt_exec', 'mkt_exec', 'mkt_exec', 'desc', 'rules', 'active',
                TRUE, FALSE, FALSE, TRUE, TRUE, '["Weather"]', '["YES","NO"]', '["tok_yes","tok_no"]',
                '2026-03-16 12:00:00', '2026-03-16 12:00:00', '2026-03-15 00:00:00', '2026-03-15 00:00:00', '{}'
            )
            """
        )
        con.execute(
            """
            INSERT INTO weather.weather_market_specs (
                market_id, condition_id, location_name, station_id, latitude, longitude, timezone, observation_date,
                observation_window_local, metric, unit, bucket_min_value, bucket_max_value, authoritative_source,
                fallback_sources, rounding_rule, inclusive_bounds, spec_version, parse_confidence, risk_flags_json,
                created_at, updated_at
            ) VALUES (
                'mkt_exec', 'cond_exec', 'Seattle', 'KSEA', 47.6062, -122.3321, 'America/Los_Angeles', '2026-03-16',
                'daily_max', 'temperature_max', 'fahrenheit', 40.0, 49.0, 'weather.com',
                ?, 'identity', TRUE, 'spec_v1', 0.95, ?, '2026-03-15 08:00:00', '2026-03-15 08:00:00'
            )
            """,
            ['["openmeteo","nws"]', "[]"],
        )

    def _insert_ticket_case(self, con, *, ticket_num: int, fill_price: float = 0.42) -> None:
        ticket_id = f"tt_{ticket_num}"
        order_id = f"ord_{ticket_num}"
        request_id = f"req_{ticket_num}"
        execution_context_id = f"ectx_{ticket_num}"
        provenance = json.dumps(
            {
                "pricing_context": {
                    "forecast_target_time": "2026-03-15T12:00:00+00:00",
                    "depth_proxy": 0.90,
                    "spread_bps": 40,
                    "edge_bps_executable": 900,
                    "reference_price": 0.40,
                    "calibration_bias_quality": "healthy",
                    "source_freshness_status": "fresh",
                }
            }
        )
        con.execute(
            """
            INSERT INTO runtime.trade_tickets (
                ticket_id, run_id, strategy_id, strategy_version, market_id, token_id, outcome, side, reference_price,
                fair_value, edge_bps, threshold_bps, route_action, size, signal_ts_ms, forecast_run_id,
                watch_snapshot_id, request_id, ticket_hash, provenance_json, created_at, wallet_id, execution_context_id
            ) VALUES (
                ?, 'run_1', 'weather_primary', 'v1', 'mkt_exec', 'tok_yes', 'YES', 'BUY', 0.40, 0.55,
                900, 100, 'FAK', 10.0, 1710000000000, 'frun_1', ?, ?, ?, ?, '2026-03-15 09:00:00',
                'wallet_weather_1', ?
            )
            """,
            [ticket_id, f"snap_{ticket_num}", request_id, f"hash_{ticket_num}", provenance, execution_context_id],
        )
        con.execute(
            """
            INSERT INTO runtime.submit_attempts (
                attempt_id, request_id, ticket_id, order_id, wallet_id, execution_context_id,
                exchange, attempt_kind, attempt_mode, canonical_order_hash, payload_hash,
                submit_payload_json, signed_payload_ref, status, error, created_at
            ) VALUES (?, ?, ?, ?, 'wallet_weather_1', ?, 'polymarket_clob', 'submit_order', 'live_submit',
                ?, ?, '{}', ?, 'accepted', NULL, '2026-03-15 09:00:06')
            """,
            [f"submit_{ticket_num}", f"subreq_{ticket_num}", ticket_id, order_id, execution_context_id, f"coh_{ticket_num}", f"phash_{ticket_num}", f"sign_{ticket_num}"],
        )
        con.execute(
            """
            INSERT INTO trading.orders (
                order_id, client_order_id, wallet_id, market_id, token_id, outcome, side, price, size, route_action,
                time_in_force, expiration, fee_rate_bps, signature_type, funder, status, filled_size, remaining_size,
                avg_fill_price, reservation_id, exchange_order_id, created_at, submitted_at, updated_at
            ) VALUES (
                ?, ?, 'wallet_weather_1', 'mkt_exec', 'tok_yes', 'YES', 'BUY', 0.40, 10.0, 'FAK',
                'FAK', NULL, 30, 1, '0xfunder', 'filled', 10.0, 0.0,
                ?, ?, ?, '2026-03-15 09:00:10', '2026-03-15 09:00:10', '2026-03-15 09:01:00'
            )
            """,
            [order_id, f"client_{ticket_num}", fill_price, f"res_{ticket_num}", f"paper_{ticket_num}"],
        )
        con.execute(
            """
            INSERT INTO trading.fills (
                fill_id, order_id, wallet_id, market_id, token_id, outcome, side, price, size, fee,
                fee_rate_bps, trade_id, exchange_order_id, filled_at
            ) VALUES (
                ?, ?, 'wallet_weather_1', 'mkt_exec', 'tok_yes', 'YES', 'BUY',
                ?, 10.0, 0.10, 30, ?, ?, '2026-03-15 09:01:00'
            )
            """,
            [f"fill_{ticket_num}", order_id, fill_price, f"trade_{ticket_num}", f"paper_{ticket_num}"],
        )
        con.execute(
            """
            INSERT INTO resolution.settlement_verifications (
                verification_id, proposal_id, market_id, proposed_outcome, expected_outcome, is_correct,
                confidence, discrepancy_details, sources_checked, evidence_package, created_at
            ) VALUES (
                ?, ?, 'mkt_exec', 'YES', 'YES', TRUE, 0.95, NULL, '[]', '{}', '2026-03-15 10:00:00'
            )
            """,
            [f"ver_{ticket_num}", f"prop_{ticket_num}"],
        )

    def test_materializer_produces_ready_prior_and_loader_reads_it(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "phase12.duckdb")
            self._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                self._insert_market_spec(con)
                for index in range(10):
                    self._insert_ticket_case(con, ticket_num=index + 1, fill_price=0.42)

                priors = materialize_execution_priors(con)
                self.assertEqual(len(priors), 3)
                prior = next(item for item in priors if item.cohort_type == "market")
                self.assertEqual(prior.market_id, "mkt_exec")
                self.assertEqual(prior.side, "BUY")
                self.assertEqual(prior.horizon_bucket, "0-1")
                self.assertEqual(prior.liquidity_bucket, "deep")
                self.assertEqual(prior.station_id, "KSEA")
                self.assertEqual(prior.metric, "temperature_max")
                self.assertEqual(prior.market_age_bucket, "new")
                self.assertEqual(prior.hours_to_close_bucket, "24-72")
                self.assertEqual(prior.sample_count, 10)
                self.assertEqual(prior.prior_quality_status, "ready")
                self.assertGreater(prior.fill_rate, 0.0)
                self.assertGreater(prior.resolution_rate, 0.0)
                self.assertGreater(prior.submit_latency_ms_p50 or 0.0, 0.0)
                self.assertGreater(prior.fill_latency_ms_p50 or 0.0, 0.0)
                self.assertEqual(prior.cohort_key, "mkt_exec")
                self.assertEqual(prior.feedback_status, "sparse")
                self.assertGreaterEqual(prior.feedback_penalty, 0.0)

                placeholders = ",".join(["?"] * len(WEATHER_EXECUTION_PRIOR_COLUMNS))
                con.executemany(
                    f"INSERT INTO weather.weather_execution_priors ({', '.join(WEATHER_EXECUTION_PRIOR_COLUMNS)}) VALUES ({placeholders})",
                    [execution_prior_row_to_row(item) for item in priors],
                )
                loaded = load_execution_prior_summary(
                    con,
                    market_id="mkt_exec",
                    side="BUY",
                    forecast_target_time=datetime(2026, 3, 15, 12, 0, tzinfo=UTC),
                    observation_date=date(2026, 3, 16),
                    depth_proxy=0.90,
                    spread_bps=40,
                    strategy_id="weather_primary",
                    wallet_id="wallet_weather_1",
                )
            finally:
                con.close()

            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(loaded.prior_quality_status, "ready")
            self.assertEqual(loaded.sample_count, 10)
            self.assertAlmostEqual(loaded.submit_ack_rate, 1.0)
            self.assertAlmostEqual(loaded.fill_rate, 1.0)
            self.assertEqual(loaded.prior_lookup_mode, "exact_market")
            self.assertIn("lookup_mode", loaded.prior_feature_scope)
            self.assertIsNotNone(loaded.feedback_prior)
            assert loaded.feedback_prior is not None
            self.assertEqual(loaded.feedback_prior.feedback_status, "sparse")
            self.assertIn("market", loaded.feedback_prior.scope_breakdown)
            self.assertIn("strategy", loaded.feedback_prior.scope_breakdown)
            self.assertIn("wallet", loaded.feedback_prior.scope_breakdown)

    def test_loader_returns_none_when_prior_table_is_missing(self) -> None:
        con = duckdb.connect(":memory:")
        try:
            loaded = load_execution_prior_summary(
                con,
                market_id="mkt_missing",
                side="BUY",
                depth_proxy=0.75,
                spread_bps=50,
            )
        finally:
            con.close()
        self.assertIsNone(loaded)

    def test_sparse_single_sample_still_materializes_prior(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "phase12_sparse.duckdb")
            self._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                self._insert_market_spec(con)
                self._insert_ticket_case(con, ticket_num=1, fill_price=0.43)
                priors = materialize_execution_priors(con)
            finally:
                con.close()

        self.assertEqual(len(priors), 3)
        self.assertTrue(all(item.feedback_status == "sparse" for item in priors))
        self.assertTrue(all(item.sample_count == 1 for item in priors))

    def test_loader_can_use_strategy_scope_across_markets(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "phase12_strategy_scope.duckdb")
            self._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                self._insert_market_spec(con)
                for index in range(2):
                    self._insert_ticket_case(con, ticket_num=index + 1, fill_price=0.42)
                priors = materialize_execution_priors(con)
                placeholders = ",".join(["?"] * len(WEATHER_EXECUTION_PRIOR_COLUMNS))
                con.executemany(
                    f"INSERT INTO weather.weather_execution_priors ({', '.join(WEATHER_EXECUTION_PRIOR_COLUMNS)}) VALUES ({placeholders})",
                    [execution_prior_row_to_row(item) for item in priors],
                )
                loaded = load_execution_prior_summary(
                    con,
                    market_id="mkt_other",
                    side="BUY",
                    forecast_target_time=datetime(2026, 3, 15, 12, 0, tzinfo=UTC),
                    observation_date=date(2026, 3, 16),
                    depth_proxy=0.90,
                    spread_bps=40,
                    strategy_id="weather_primary",
                    wallet_id="wallet_weather_1",
                    station_id="KSEA",
                    metric="temperature_max",
                    market_age_bucket="new",
                    hours_to_close_bucket="24-72",
                    calibration_quality_bucket="healthy",
                    source_freshness_bucket="fresh",
                )
            finally:
                con.close()

        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertEqual(loaded.prior_lookup_mode, "exact_strategy")
        self.assertEqual(loaded.feedback_prior.feedback_status, "sparse")

    def test_materializer_uses_forecast_run_target_time_when_provenance_lacks_it(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "phase12_forecast_run_fallback.duckdb")
            self._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                self._insert_market_spec(con)
                self._insert_ticket_case(con, ticket_num=1, fill_price=0.42)
                con.execute(
                    """
                    INSERT INTO weather.weather_forecast_runs (
                        run_id, market_id, condition_id, station_id, source, model_run, forecast_target_time, observation_date,
                        metric, latitude, longitude, timezone, spec_version, cache_key, source_trace_json,
                        fallback_used, from_cache, confidence, forecast_payload_json, raw_payload_json, created_at
                    ) VALUES (
                        'frun_1', 'mkt_exec', 'cond_exec', 'KSEA', 'openmeteo', '2026-03-15T12:00:00Z', '2026-03-15 12:00:00',
                        '2026-03-16', 'temperature_max', 47.6062, -122.3321, 'America/Los_Angeles', 'spec_v1', 'cache_1', '[]',
                        FALSE, FALSE, 0.9, '{}', '{}', '2026-03-15 09:00:00'
                    )
                    """
                )
                con.execute(
                    """
                    UPDATE runtime.trade_tickets
                    SET provenance_json = '{"pricing_context":{"depth_proxy":0.90,"spread_bps":40,"edge_bps_executable":900,"reference_price":0.40,"calibration_bias_quality":"healthy","source_freshness_status":"fresh"}}'
                    WHERE ticket_id = 'tt_1'
                    """
                )
                priors = materialize_execution_priors(con)
            finally:
                con.close()

        self.assertEqual(len(priors), 3)
        self.assertTrue(all(item.horizon_bucket == "0-1" for item in priors))


if __name__ == "__main__":
    unittest.main()
