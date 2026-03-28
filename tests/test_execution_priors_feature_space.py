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
class ExecutionPriorsFeatureSpaceTest(unittest.TestCase):
    def _apply_migrations(self, db_path: str) -> None:
        bootstrap = duckdb.connect(db_path)
        migrations_dir = Path(__file__).resolve().parents[1] / "sql" / "migrations"
        for path in sorted(migrations_dir.glob("*.sql")):
            sql = path.read_text(encoding="utf-8").strip()
            if sql:
                bootstrap.execute(sql)
        bootstrap.close()

    def _insert_market(self, con, *, market_id: str, station_id: str = "KSEA", created_at: str = "2026-03-15 00:00:00", close_time: str = "2026-03-16 12:00:00") -> None:
        con.execute(
            """
            INSERT INTO weather.weather_markets (
                market_id, condition_id, event_id, slug, title, description, rules, status,
                active, closed, archived, accepting_orders, enable_order_book, tags_json,
                outcomes_json, token_ids_json, close_time, end_date, created_at, updated_at, raw_market_json
            ) VALUES (
                ?, ?, ?, ?, ?, 'weather market', 'rules', 'active',
                TRUE, FALSE, FALSE, TRUE, TRUE, '["Weather"]',
                '["YES","NO"]', '["tok_yes","tok_no"]', ?, ?, ?, ?, '{}'
            )
            """,
            [market_id, f"cond_{market_id}", f"evt_{market_id}", market_id, market_id, close_time, close_time, created_at, created_at],
        )
        con.execute(
            """
            INSERT INTO weather.weather_market_specs (
                market_id, condition_id, location_name, station_id, latitude, longitude, timezone, observation_date,
                observation_window_local, metric, unit, bucket_min_value, bucket_max_value, authoritative_source,
                fallback_sources, rounding_rule, inclusive_bounds, spec_version, parse_confidence, risk_flags_json,
                created_at, updated_at
            ) VALUES (
                ?, ?, 'Seattle', ?, 47.6062, -122.3321, 'America/Los_Angeles', '2026-03-16',
                'daily_max', 'temperature_max', 'fahrenheit', 40.0, 49.0, 'weather.com',
                '["openmeteo","nws"]', 'identity', TRUE, 'spec_v1', 0.95, '[]', ?, ?
            )
            """,
            [market_id, f"cond_{market_id}", station_id, created_at, created_at],
        )

    def _insert_ticket_case(self, con, *, market_id: str, ticket_num: int, ticket_created_at: str, submit_created_at: str, submitted_at: str, fill_at: str, fill_price: float = 0.42, calibration_quality: str = "healthy", source_freshness: str = "fresh") -> None:
        ticket_id = f"{market_id}_tt_{ticket_num}"
        order_id = f"{market_id}_ord_{ticket_num}"
        request_id = f"{market_id}_req_{ticket_num}"
        execution_context_id = f"{market_id}_ectx_{ticket_num}"
        provenance = json.dumps(
            {
                "pricing_context": {
                    "forecast_target_time": "2026-03-15T12:00:00+00:00",
                    "depth_proxy": 0.90,
                    "spread_bps": 40,
                    "edge_bps_executable": 900,
                    "reference_price": 0.40,
                    "calibration_bias_quality": calibration_quality,
                    "source_freshness_status": source_freshness,
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
                ?, 'run_1', 'weather_primary', 'v1', ?, 'tok_yes', 'YES', 'BUY', 0.40, 0.55,
                900, 100, 'FAK', 10.0, 1710000000000, 'frun_1', ?, ?, ?, ?, ?, 'wallet_weather_1', ?
            )
            """,
            [ticket_id, market_id, f"snap_{ticket_id}", request_id, f"hash_{ticket_id}", provenance, ticket_created_at, execution_context_id],
        )
        con.execute(
            """
            INSERT INTO runtime.submit_attempts (
                attempt_id, request_id, ticket_id, order_id, wallet_id, execution_context_id,
                exchange, attempt_kind, attempt_mode, canonical_order_hash, payload_hash,
                submit_payload_json, signed_payload_ref, status, error, created_at
            ) VALUES (?, ?, ?, ?, 'wallet_weather_1', ?, 'polymarket_clob', 'submit_order', 'live_submit',
                ?, ?, '{}', ?, 'accepted', NULL, ?)
            """,
            [f"submit_{ticket_id}", f"subreq_{ticket_id}", ticket_id, order_id, execution_context_id, f"coh_{ticket_id}", f"phash_{ticket_id}", f"sign_{ticket_id}", submit_created_at],
        )
        con.execute(
            """
            INSERT INTO trading.orders (
                order_id, client_order_id, wallet_id, market_id, token_id, outcome, side, price, size, route_action,
                time_in_force, expiration, fee_rate_bps, signature_type, funder, status, filled_size, remaining_size,
                avg_fill_price, reservation_id, exchange_order_id, created_at, submitted_at, updated_at
            ) VALUES (
                ?, ?, 'wallet_weather_1', ?, 'tok_yes', 'YES', 'BUY', 0.40, 10.0, 'FAK',
                'FAK', NULL, 30, 1, '0xfunder', 'filled', 10.0, 0.0,
                ?, ?, ?, ?, ?, ?
            )
            """,
            [order_id, f"client_{ticket_id}", market_id, fill_price, f"res_{ticket_id}", f"paper_{ticket_id}", ticket_created_at, submitted_at, fill_at],
        )
        con.execute(
            """
            INSERT INTO trading.fills (
                fill_id, order_id, wallet_id, market_id, token_id, outcome, side, price, size, fee,
                fee_rate_bps, trade_id, exchange_order_id, filled_at
            ) VALUES (
                ?, ?, 'wallet_weather_1', ?, 'tok_yes', 'YES', 'BUY',
                ?, 10.0, 0.10, 30, ?, ?, ?
            )
            """,
            [f"fill_{ticket_id}", order_id, market_id, fill_price, f"trade_{ticket_id}", f"paper_{ticket_id}", fill_at],
        )
        con.execute(
            """
            INSERT INTO resolution.settlement_verifications (
                verification_id, proposal_id, market_id, proposed_outcome, expected_outcome, is_correct,
                confidence, discrepancy_details, sources_checked, evidence_package, created_at
            ) VALUES (
                ?, ?, ?, 'YES', 'YES', TRUE, 0.95, NULL, '[]', '{}', '2026-03-16 14:00:00'
            )
            """,
            [f"ver_{ticket_id}", f"prop_{ticket_id}", market_id],
        )

    def test_materializer_emits_phase1_feature_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "phase1_priors.duckdb")
            self._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                self._insert_market(con, market_id="mkt_exec")
                for index in range(10):
                    self._insert_ticket_case(
                        con,
                        market_id="mkt_exec",
                        ticket_num=index + 1,
                        ticket_created_at="2026-03-15 09:00:00",
                        submit_created_at="2026-03-15 09:00:06",
                        submitted_at="2026-03-15 09:00:08",
                        fill_at="2026-03-15 09:01:00",
                    )
                priors = materialize_execution_priors(con)
            finally:
                con.close()

        market_prior = next(item for item in priors if item.cohort_type == "market" and item.market_id == "mkt_exec")
        self.assertEqual(market_prior.station_id, "KSEA")
        self.assertEqual(market_prior.metric, "temperature_max")
        self.assertEqual(market_prior.market_age_bucket, "new")
        self.assertEqual(market_prior.hours_to_close_bucket, "24-72")
        self.assertEqual(market_prior.calibration_quality_bucket, "healthy")
        self.assertEqual(market_prior.source_freshness_bucket, "fresh")
        self.assertAlmostEqual(market_prior.submit_latency_ms_p50 or 0.0, 6000.0)
        self.assertAlmostEqual(market_prior.fill_latency_ms_p50 or 0.0, 54000.0)
        self.assertGreater(market_prior.realized_edge_retention_bps_p50 or 0.0, 0.0)

    def test_loader_uses_station_metric_fallback_when_exact_market_prior_is_sparse(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "phase1_priors_fallback.duckdb")
            self._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                self._insert_market(con, market_id="mkt_hist")
                self._insert_market(con, market_id="mkt_sparse")
                for index in range(10):
                    self._insert_ticket_case(
                        con,
                        market_id="mkt_hist",
                        ticket_num=index + 1,
                        ticket_created_at="2026-03-15 09:00:00",
                        submit_created_at="2026-03-15 09:00:05",
                        submitted_at="2026-03-15 09:00:06",
                        fill_at="2026-03-15 09:01:00",
                    )
                self._insert_ticket_case(
                    con,
                    market_id="mkt_sparse",
                    ticket_num=1,
                    ticket_created_at="2026-03-15 09:00:00",
                    submit_created_at="2026-03-15 09:00:05",
                    submitted_at="2026-03-15 09:00:06",
                    fill_at="2026-03-15 09:01:00",
                    source_freshness="stale",
                )
                priors = materialize_execution_priors(con)
                placeholders = ",".join(["?"] * len(WEATHER_EXECUTION_PRIOR_COLUMNS))
                con.executemany(
                    f"INSERT INTO weather.weather_execution_priors ({', '.join(WEATHER_EXECUTION_PRIOR_COLUMNS)}) VALUES ({placeholders})",
                    [execution_prior_row_to_row(item) for item in priors],
                )
                loaded = load_execution_prior_summary(
                    con,
                    market_id="mkt_sparse",
                    station_id="KSEA",
                    metric="temperature_max",
                    side="BUY",
                    forecast_target_time=datetime(2026, 3, 15, 12, 0, tzinfo=UTC),
                    observation_date=date(2026, 3, 16),
                    depth_proxy=0.90,
                    spread_bps=40,
                )
            finally:
                con.close()

        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertEqual(loaded.prior_lookup_mode, "station_metric_fallback")
        self.assertGreaterEqual(loaded.sample_count, 10)
        self.assertEqual(loaded.prior_key.market_id, None)
        self.assertEqual(loaded.prior_key.station_id, "KSEA")
        self.assertEqual(loaded.prior_key.metric, "temperature_max")
        self.assertGreaterEqual(int(loaded.prior_feature_scope.get("matched_market_count") or 0), 1)

    def test_loader_prefers_exact_strategy_prior_before_station_metric_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "phase1_priors_strategy.duckdb")
            self._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                self._insert_market(con, market_id="mkt_strategy")
                for index in range(10):
                    self._insert_ticket_case(
                        con,
                        market_id="mkt_strategy",
                        ticket_num=index + 1,
                        ticket_created_at="2026-03-15 09:00:00",
                        submit_created_at="2026-03-15 09:00:05",
                        submitted_at="2026-03-15 09:00:06",
                        fill_at="2026-03-15 09:01:00",
                    )
                priors = materialize_execution_priors(con)
                strategy_rows = [item for item in priors if item.cohort_type == "strategy"]
                placeholders = ",".join(["?"] * len(WEATHER_EXECUTION_PRIOR_COLUMNS))
                con.executemany(
                    f"INSERT INTO weather.weather_execution_priors ({', '.join(WEATHER_EXECUTION_PRIOR_COLUMNS)}) VALUES ({placeholders})",
                    [execution_prior_row_to_row(item) for item in strategy_rows],
                )
                loaded = load_execution_prior_summary(
                    con,
                    market_id="mkt_strategy",
                    station_id="KSEA",
                    metric="temperature_max",
                    side="BUY",
                    strategy_id="weather_primary",
                    wallet_id="wallet_weather_1",
                    forecast_target_time=datetime(2026, 3, 15, 12, 0, tzinfo=UTC),
                    observation_date=date(2026, 3, 16),
                    depth_proxy=0.90,
                    spread_bps=40,
                )
            finally:
                con.close()

        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertEqual(loaded.prior_lookup_mode, "exact_strategy")
        self.assertEqual(loaded.prior_key.strategy_id, "weather_primary")
        self.assertEqual(loaded.prior_key.source_freshness_bucket, "fresh")

    def test_loader_discriminates_source_freshness_feature_bucket(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "phase1_priors_feature_bucket.duckdb")
            self._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                self._insert_market(con, market_id="mkt_bucket")
                for index in range(10):
                    self._insert_ticket_case(
                        con,
                        market_id="mkt_bucket",
                        ticket_num=index + 1,
                        ticket_created_at="2026-03-15 09:00:00",
                        submit_created_at="2026-03-15 09:00:05",
                        submitted_at="2026-03-15 09:00:06",
                        fill_at="2026-03-15 09:01:00",
                        source_freshness="fresh",
                    )
                for index in range(10, 20):
                    self._insert_ticket_case(
                        con,
                        market_id="mkt_bucket",
                        ticket_num=index + 1,
                        ticket_created_at="2026-03-15 09:00:00",
                        submit_created_at="2026-03-15 09:00:05",
                        submitted_at="2026-03-15 09:00:06",
                        fill_at="2026-03-15 09:01:00",
                        source_freshness="stale",
                    )
                priors = materialize_execution_priors(con)
                placeholders = ",".join(["?"] * len(WEATHER_EXECUTION_PRIOR_COLUMNS))
                con.executemany(
                    f"INSERT INTO weather.weather_execution_priors ({', '.join(WEATHER_EXECUTION_PRIOR_COLUMNS)}) VALUES ({placeholders})",
                    [execution_prior_row_to_row(item) for item in priors],
                )
                fresh_loaded = load_execution_prior_summary(
                    con,
                    market_id="mkt_bucket",
                    station_id="KSEA",
                    metric="temperature_max",
                    side="BUY",
                    forecast_target_time=datetime(2026, 3, 15, 12, 0, tzinfo=UTC),
                    observation_date=date(2026, 3, 16),
                    depth_proxy=0.90,
                    spread_bps=40,
                    source_freshness_bucket="fresh",
                )
                stale_loaded = load_execution_prior_summary(
                    con,
                    market_id="mkt_bucket",
                    station_id="KSEA",
                    metric="temperature_max",
                    side="BUY",
                    forecast_target_time=datetime(2026, 3, 15, 12, 0, tzinfo=UTC),
                    observation_date=date(2026, 3, 16),
                    depth_proxy=0.90,
                    spread_bps=40,
                    source_freshness_bucket="stale",
                )
            finally:
                con.close()

        self.assertIsNotNone(fresh_loaded)
        self.assertIsNotNone(stale_loaded)
        assert fresh_loaded is not None
        assert stale_loaded is not None
        self.assertEqual(fresh_loaded.prior_key.source_freshness_bucket, "fresh")
        self.assertEqual(stale_loaded.prior_key.source_freshness_bucket, "stale")
        self.assertNotEqual(fresh_loaded.prior_key.source_freshness_bucket, stale_loaded.prior_key.source_freshness_bucket)


if __name__ == "__main__":
    unittest.main()
