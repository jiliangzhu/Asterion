from __future__ import annotations

import importlib.util
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import duckdb

from asterion_core.contracts import WeatherMarket, WeatherMarketSpecRecord
from asterion_core.storage.write_queue import WriteQueueConfig
from domains.weather.forecast import build_forecast_calibration_sample, enqueue_forecast_calibration_sample_upserts, enqueue_forecast_run_upserts
from domains.weather.scout.market_discovery import enqueue_weather_market_upserts
from domains.weather.spec.rule2spec import enqueue_weather_market_spec_upserts


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "run_real_weather_chain_smoke.py"


def _load_smoke_module():
    spec = importlib.util.spec_from_file_location("real_weather_chain_smoke_bootstrap", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load run_real_weather_chain_smoke.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RealWeatherChainCalibrationBootstrapTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.smoke = _load_smoke_module()

    def test_bootstrap_materializes_samples_from_matured_forecasts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "canonical.duckdb"
            queue_path = Path(tmpdir) / "canonical_write_queue.sqlite"
            self.smoke.apply_schema(db_path)
            con = duckdb.connect(str(db_path))
            try:
                observation_date = (datetime.now(UTC) - timedelta(days=2)).date()
                con.execute(
                    """
                    INSERT INTO weather.weather_forecast_runs (
                        run_id, market_id, condition_id, station_id, source, model_run, forecast_target_time, observation_date,
                        metric, latitude, longitude, timezone, spec_version, cache_key, source_trace_json, fallback_used,
                        from_cache, confidence, forecast_payload_json, raw_payload_json, created_at
                    ) VALUES (
                        'run_bootstrap_1', 'mkt_1', 'cond_1', 'KSEA', 'openmeteo', '2026-03-20T00:00Z',
                        ?, ?, 'temperature_max', 47.6062, -122.3321, 'America/Los_Angeles',
                        'spec_v1', 'cache_bootstrap_1', '["openmeteo"]', FALSE, FALSE, 0.97,
                        '{"temperature_distribution":{"70":1.0}}', '{"raw":"payload"}', ?
                    )
                    """,
                    [
                        datetime.combine(observation_date, datetime.min.time()).replace(hour=12),
                        observation_date,
                        datetime.now(UTC).replace(tzinfo=None),
                    ],
                )
            finally:
                con.close()

            with patch.object(self.smoke, "_fetch_observed_value_from_archive", return_value=71.0):
                result = self.smoke._bootstrap_calibration_samples_from_archive(
                    db_path=db_path,
                    queue_cfg=WriteQueueConfig(path=str(queue_path)),
                    allow_tables=list(self.smoke.FULL_ALLOW_TABLES),
                )

            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["matured_forecast_count"], 1)
            self.assertGreaterEqual(result["calibration_sample_count"], 1)
            self.assertEqual(result["matured_missing_sample_count"], 1)

            con = duckdb.connect(str(db_path), read_only=True)
            try:
                count = con.execute("SELECT COUNT(*) FROM weather.forecast_calibration_samples").fetchone()
            finally:
                con.close()
            self.assertGreaterEqual(int(count[0]), 1)

    def test_bootstrap_backfills_from_supported_historical_closed_markets_when_no_matured_forecasts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "canonical.duckdb"
            queue_path = Path(tmpdir) / "canonical_write_queue.sqlite"
            self.smoke.apply_schema(db_path)

            historical_market = WeatherMarket(
                market_id="hist_mkt_1",
                condition_id="cond_hist_1",
                event_id="evt_hist_1",
                slug="nyc-high-temp-feb-2",
                title="Will the highest temperature in NYC be between 33-34°F on February 2?",
                description=None,
                rules=None,
                status="closed",
                active=False,
                closed=True,
                archived=True,
                accepting_orders=False,
                enable_order_book=True,
                tags=["Weather"],
                outcomes=["YES", "NO"],
                token_ids=["tok_yes", "tok_no"],
                close_time=datetime(2025, 2, 2, 12, 0, tzinfo=UTC),
                end_date=datetime(2025, 2, 2, 12, 0, tzinfo=UTC),
                raw_market={},
            )

            with (
                patch.object(
                    self.smoke,
                    "_discover_supported_closed_weather_markets_for_calibration",
                    return_value=[historical_market],
                ),
                patch.object(
                    self.smoke,
                    "build_station_mapping_for_market",
                    return_value={
                        "station_id": "KLGA",
                        "station_name": "LaGuardia",
                        "latitude": 40.7769,
                        "longitude": -73.874,
                        "timezone": "America/New_York",
                    },
                ),
                patch.object(self.smoke, "_fetch_historical_forecast_value", return_value=34.4),
                patch.object(self.smoke, "_fetch_observed_value_from_archive", return_value=35.0),
            ):
                result = self.smoke._bootstrap_calibration_samples_from_archive(
                    db_path=db_path,
                    queue_cfg=WriteQueueConfig(path=str(queue_path)),
                    allow_tables=list(self.smoke.FULL_ALLOW_TABLES),
                    lookback_days=365,
                )

            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["matured_forecast_count"], 0)
            self.assertEqual(result["historical_supported_market_count"], 1)
            self.assertEqual(result["historical_forecast_run_count"], 1)
            self.assertEqual(result["historical_spec_count"], 1)
            self.assertEqual(result["calibration_sample_count"], 1)

            con = duckdb.connect(str(db_path), read_only=True)
            try:
                sample_count = con.execute("SELECT COUNT(*) FROM weather.forecast_calibration_samples").fetchone()
                forecast_count = con.execute("SELECT COUNT(*) FROM weather.weather_forecast_runs").fetchone()
                spec_count = con.execute("SELECT COUNT(*) FROM weather.weather_market_specs").fetchone()
            finally:
                con.close()
            self.assertEqual(int(sample_count[0]), 1)
            self.assertEqual(int(forecast_count[0]), 1)
            self.assertEqual(int(spec_count[0]), 1)

    def test_bootstrap_falls_back_to_frozen_real_market_when_historical_discovery_is_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "canonical.duckdb"
            queue_path = Path(tmpdir) / "canonical_write_queue.sqlite"
            self.smoke.apply_schema(db_path)

            with (
                patch.object(
                    self.smoke,
                    "_discover_supported_closed_weather_markets_for_calibration",
                    return_value=[],
                ),
                patch.object(self.smoke, "_fetch_historical_forecast_value", return_value=61.2),
                patch.object(self.smoke, "_fetch_observed_value_from_archive", return_value=60.0),
            ):
                result = self.smoke._bootstrap_calibration_samples_from_archive(
                    db_path=db_path,
                    queue_cfg=WriteQueueConfig(path=str(queue_path)),
                    allow_tables=list(self.smoke.FULL_ALLOW_TABLES),
                    lookback_days=365,
                )

            self.assertEqual(result["status"], "ok")
            self.assertTrue(result["used_frozen_market_fallback"])
            self.assertEqual(result["historical_supported_market_count"], 1)
            self.assertEqual(result["historical_forecast_run_count"], 1)
            self.assertEqual(result["historical_spec_count"], 1)
            self.assertEqual(result["calibration_sample_count"], 1)

            con = duckdb.connect(str(db_path), read_only=True)
            try:
                sample_count = con.execute("SELECT COUNT(*) FROM weather.forecast_calibration_samples").fetchone()
                forecast_count = con.execute("SELECT COUNT(*) FROM weather.weather_forecast_runs").fetchone()
                spec_count = con.execute("SELECT COUNT(*) FROM weather.weather_market_specs").fetchone()
            finally:
                con.close()
            self.assertEqual(int(sample_count[0]), 1)
            self.assertEqual(int(forecast_count[0]), 1)
            self.assertEqual(int(spec_count[0]), 1)

    def test_bootstrap_synthesizes_active_station_coverage_when_profiles_are_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "canonical.duckdb"
            queue_path = Path(tmpdir) / "canonical_write_queue.sqlite"
            self.smoke.apply_schema(db_path)
            queue_cfg = WriteQueueConfig(path=str(queue_path))
            active_market = WeatherMarket(
                market_id="active_mkt_1",
                condition_id="cond_active_1",
                event_id="evt_active_1",
                slug="seattle-high-temp-active",
                title="Will the highest temperature in Seattle be between 52-53°F on March 28?",
                description=None,
                rules=None,
                status="active",
                active=True,
                closed=False,
                archived=False,
                accepting_orders=True,
                enable_order_book=True,
                tags=["Weather"],
                outcomes=["YES", "NO"],
                token_ids=["tok_yes", "tok_no"],
                close_time=datetime(2026, 3, 28, 12, 0, tzinfo=UTC),
                end_date=datetime(2026, 3, 28, 12, 0, tzinfo=UTC),
                raw_market={},
            )
            active_spec = WeatherMarketSpecRecord(
                market_id="active_mkt_1",
                condition_id="cond_active_1",
                location_name="Seattle",
                station_id="KSEA",
                latitude=47.4502,
                longitude=-122.3088,
                timezone="America/Los_Angeles",
                observation_date=datetime(2026, 3, 28, 12, 0, tzinfo=UTC).date(),
                observation_window_local="daily_high",
                metric="temperature_max",
                unit="F",
                bucket_min_value=52.0,
                bucket_max_value=53.0,
                authoritative_source="unknown",
                fallback_sources=["open-meteo"],
                rounding_rule="nearest",
                inclusive_bounds=True,
                spec_version="spec_active_1",
                parse_confidence=1.0,
                risk_flags=[],
            )
            enqueue_weather_market_upserts(queue_cfg, markets=[active_market], run_id="test_active_market")
            enqueue_weather_market_spec_upserts(queue_cfg, specs=[active_spec], run_id="test_active_spec")
            self.smoke.drain_queue(
                queue_path=str(queue_path),
                db_path=str(db_path),
                allow_tables=list(self.smoke.FULL_ALLOW_TABLES),
            )

            with (
                patch.object(
                    self.smoke,
                    "_discover_supported_closed_weather_markets_for_calibration",
                    return_value=[],
                ),
                patch.object(
                    self.smoke,
                    "_fetch_historical_forecast_series",
                    return_value={
                        datetime(2025, 3, 8, 12, 0, tzinfo=UTC).date(): 55.0,
                        datetime(2025, 3, 9, 12, 0, tzinfo=UTC).date(): 56.0,
                    },
                ),
                patch.object(
                    self.smoke,
                    "_fetch_observed_series_from_archive",
                    return_value={
                        datetime(2025, 3, 8, 12, 0, tzinfo=UTC).date(): 54.0,
                        datetime(2025, 3, 9, 12, 0, tzinfo=UTC).date(): 57.0,
                    },
                ),
            ):
                result = self.smoke._bootstrap_calibration_samples_from_archive(
                    db_path=db_path,
                    queue_cfg=queue_cfg,
                    allow_tables=list(self.smoke.FULL_ALLOW_TABLES),
                    lookback_days=365,
                )

            self.assertGreater(result["synthetic_spec_count"], 0)
            self.assertGreater(result["synthetic_forecast_run_count"], 0)
            self.assertGreater(result["synthetic_sample_count"], 0)

            con = duckdb.connect(str(db_path), read_only=True)
            try:
                synthetic_specs = con.execute(
                    "SELECT COUNT(*) FROM weather.weather_market_specs WHERE spec_version = 'synthetic_calibration_bootstrap_v1'"
                ).fetchone()
                synthetic_runs = con.execute(
                    "SELECT COUNT(*) FROM weather.weather_forecast_runs WHERE model_run = 'synthetic_calibration_bootstrap_v3'"
                ).fetchone()
            finally:
                con.close()
            self.assertGreater(int(synthetic_specs[0]), 0)
            self.assertGreater(int(synthetic_runs[0]), 0)

    def test_bootstrap_reports_quality_gap_without_rebuilding_when_coverage_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "canonical.duckdb"
            queue_path = Path(tmpdir) / "canonical_write_queue.sqlite"
            self.smoke.apply_schema(db_path)
            queue_cfg = WriteQueueConfig(path=str(queue_path))
            active_market = WeatherMarket(
                market_id="active_mkt_q1",
                condition_id="cond_active_q1",
                event_id="evt_active_q1",
                slug="seattle-quality-gap",
                title="Will the highest temperature in Seattle be between 52-53°F on March 28?",
                description=None,
                rules=None,
                status="active",
                active=True,
                closed=False,
                archived=False,
                accepting_orders=True,
                enable_order_book=True,
                tags=["Weather"],
                outcomes=["YES", "NO"],
                token_ids=["tok_yes_q", "tok_no_q"],
                close_time=datetime(2026, 3, 28, 12, 0, tzinfo=UTC),
                end_date=datetime(2026, 3, 28, 12, 0, tzinfo=UTC),
                raw_market={},
            )
            active_spec = WeatherMarketSpecRecord(
                market_id="active_mkt_q1",
                condition_id="cond_active_q1",
                location_name="Seattle",
                station_id="KSEA",
                latitude=47.4502,
                longitude=-122.3088,
                timezone="America/Los_Angeles",
                observation_date=datetime(2026, 3, 28, 12, 0, tzinfo=UTC).date(),
                observation_window_local="daily_high",
                metric="temperature_max",
                unit="F",
                bucket_min_value=52.0,
                bucket_max_value=53.0,
                authoritative_source="unknown",
                fallback_sources=["open-meteo"],
                rounding_rule="nearest",
                inclusive_bounds=True,
                spec_version="spec_active_q1",
                parse_confidence=1.0,
                risk_flags=[],
            )
            enqueue_weather_market_upserts(queue_cfg, markets=[active_market], run_id="test_quality_market")
            enqueue_weather_market_spec_upserts(queue_cfg, specs=[active_spec], run_id="test_quality_spec")
            self.smoke.drain_queue(
                queue_path=str(queue_path),
                db_path=str(db_path),
                allow_tables=list(self.smoke.FULL_ALLOW_TABLES),
            )

            con = duckdb.connect(str(db_path))
            try:
                con.execute(
                    """
                    INSERT INTO weather.forecast_calibration_profiles_v2 (
                        profile_key, station_id, source, metric, forecast_horizon_bucket, season_bucket, regime_bucket,
                        sample_count, mean_bias, mean_abs_residual, p90_abs_residual, empirical_coverage_50,
                        empirical_coverage_80, empirical_coverage_95, regime_stability_score, residual_quantiles_json,
                        threshold_probability_profile_json, calibration_health_status, window_start, window_end, materialized_at
                    ) VALUES (
                        'degraded_profile_q1', 'KSEA', 'openmeteo', 'temperature_max', '0-1', 'spring', 'cold',
                        30, 0.2, 1.1, 1.5, 0.10, 0.20, 0.30, 0.10, '{}',
                        '{\"0-10\":{\"sample_count\":30,\"predicted_prob_mean\":0.05,\"realized_hit_rate\":0.05,\"quality_status\":\"healthy\"}}',
                        'degraded', ?, ?, ?
                    )
                    """,
                    [
                        datetime(2026, 3, 1, 0, 0),
                        datetime(2026, 3, 20, 0, 0),
                        datetime(2026, 3, 20, 0, 0),
                    ],
                )
            finally:
                con.close()

            with (
                patch.object(
                    self.smoke,
                    "_discover_supported_closed_weather_markets_for_calibration",
                    return_value=[],
                ),
                patch.object(
                    self.smoke,
                    "_fetch_historical_forecast_series",
                    return_value={
                        datetime(2025, 3, 8, 12, 0, tzinfo=UTC).date(): 55.0,
                        datetime(2025, 3, 9, 12, 0, tzinfo=UTC).date(): 56.0,
                    },
                ),
                patch.object(
                    self.smoke,
                    "_fetch_observed_series_from_archive",
                    return_value={
                        datetime(2025, 3, 8, 12, 0, tzinfo=UTC).date(): 54.0,
                        datetime(2025, 3, 9, 12, 0, tzinfo=UTC).date(): 57.0,
                    },
                ),
            ):
                result = self.smoke._bootstrap_calibration_samples_from_archive(
                    db_path=db_path,
                    queue_cfg=queue_cfg,
                    allow_tables=list(self.smoke.FULL_ALLOW_TABLES),
                    lookback_days=365,
                )

            self.assertGreater(result["active_calibration_quality_gap_count"], 0)
            self.assertEqual(result["active_calibration_coverage_gap_count"], 0)
            self.assertEqual(result["synthetic_spec_count"], 0)
            self.assertEqual(result["synthetic_forecast_run_count"], 0)
            self.assertEqual(result["synthetic_sample_count"], 0)
            self.assertEqual(result["synthetic_rebuild_mode"], "none")
            self.assertEqual(result["bootstrap_mode"], "incremental")

    def test_purge_synthetic_bootstrap_artifacts_removes_superseded_versions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "canonical.duckdb"
            queue_path = Path(tmpdir) / "canonical_write_queue.sqlite"
            self.smoke.apply_schema(db_path)
            queue_cfg = WriteQueueConfig(path=str(queue_path))
            template_spec = WeatherMarketSpecRecord(
                market_id="active_mkt_cleanup_1",
                condition_id="cond_cleanup_1",
                location_name="Seattle",
                station_id="KSEA",
                latitude=47.4502,
                longitude=-122.3088,
                timezone="America/Los_Angeles",
                observation_date=datetime(2026, 3, 28, 12, 0, tzinfo=UTC).date(),
                observation_window_local="daily_high",
                metric="temperature_max",
                unit="F",
                bucket_min_value=52.0,
                bucket_max_value=53.0,
                authoritative_source="unknown",
                fallback_sources=["open-meteo"],
                rounding_rule="nearest",
                inclusive_bounds=True,
                spec_version="spec_cleanup_1",
                parse_confidence=1.0,
                risk_flags=[],
            )
            synthetic_spec = self.smoke._build_synthetic_calibration_spec(
                template_spec,
                observation_date=datetime(2025, 3, 8, 12, 0, tzinfo=UTC).date(),
            )
            synthetic_run = self.smoke._build_synthetic_bootstrap_forecast_run(
                synthetic_spec,
                forecast_point_value=55.0,
                forecast_std_dev=1.5,
            )
            synthetic_sample = build_forecast_calibration_sample(
                forecast_run=synthetic_run,
                observed_value=56.0,
            )

            enqueue_weather_market_spec_upserts(queue_cfg, specs=[synthetic_spec], run_id="test_cleanup_spec")
            enqueue_forecast_run_upserts(queue_cfg, forecast_runs=[synthetic_run], run_id="test_cleanup_run")
            enqueue_forecast_calibration_sample_upserts(queue_cfg, samples=[synthetic_sample], run_id="test_cleanup_sample")
            self.smoke.drain_queue(
                queue_path=str(queue_path),
                db_path=str(db_path),
                allow_tables=list(self.smoke.FULL_ALLOW_TABLES),
            )

            result = self.smoke._purge_synthetic_bootstrap_artifacts(
                db_path=db_path,
                station_metric_pairs={("KSEA", "temperature_max")},
            )

            self.assertEqual(result["deleted_sample_count"], 1)
            self.assertEqual(result["deleted_forecast_run_count"], 1)

            con = duckdb.connect(str(db_path), read_only=True)
            try:
                forecast_count = con.execute(
                    "SELECT COUNT(*) FROM weather.weather_forecast_runs WHERE model_run LIKE 'synthetic_calibration_bootstrap_%'"
                ).fetchone()
                sample_count = con.execute(
                    "SELECT COUNT(*) FROM weather.forecast_calibration_samples WHERE market_id = ?",
                    [synthetic_spec.market_id],
                ).fetchone()
            finally:
                con.close()
            self.assertEqual(int(forecast_count[0]), 0)
            self.assertEqual(int(sample_count[0]), 0)

    def test_bootstrap_incremental_second_run_skips_existing_synthetic_dates(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "canonical.duckdb"
            queue_path = Path(tmpdir) / "canonical_write_queue.sqlite"
            self.smoke.apply_schema(db_path)
            queue_cfg = WriteQueueConfig(path=str(queue_path))
            active_market = WeatherMarket(
                market_id="active_mkt_inc_1",
                condition_id="cond_active_inc_1",
                event_id="evt_active_inc_1",
                slug="seattle-incremental-gap",
                title="Will the highest temperature in Seattle be between 52-53°F on March 28?",
                description=None,
                rules=None,
                status="active",
                active=True,
                closed=False,
                archived=False,
                accepting_orders=True,
                enable_order_book=True,
                tags=["Weather"],
                outcomes=["YES", "NO"],
                token_ids=["tok_yes_inc", "tok_no_inc"],
                close_time=datetime(2026, 3, 28, 12, 0, tzinfo=UTC),
                end_date=datetime(2026, 3, 28, 12, 0, tzinfo=UTC),
                raw_market={},
            )
            active_spec = WeatherMarketSpecRecord(
                market_id="active_mkt_inc_1",
                condition_id="cond_active_inc_1",
                location_name="Seattle",
                station_id="KSEA",
                latitude=47.4502,
                longitude=-122.3088,
                timezone="America/Los_Angeles",
                observation_date=datetime(2026, 3, 28, 12, 0, tzinfo=UTC).date(),
                observation_window_local="daily_high",
                metric="temperature_max",
                unit="F",
                bucket_min_value=52.0,
                bucket_max_value=53.0,
                authoritative_source="unknown",
                fallback_sources=["open-meteo"],
                rounding_rule="nearest",
                inclusive_bounds=True,
                spec_version="spec_active_inc_1",
                parse_confidence=1.0,
                risk_flags=[],
            )
            enqueue_weather_market_upserts(queue_cfg, markets=[active_market], run_id="test_incremental_market")
            enqueue_weather_market_spec_upserts(queue_cfg, specs=[active_spec], run_id="test_incremental_spec")
            self.smoke.drain_queue(
                queue_path=str(queue_path),
                db_path=str(db_path),
                allow_tables=list(self.smoke.FULL_ALLOW_TABLES),
            )

            forecast_series = {
                datetime(2025, 3, 8, 12, 0, tzinfo=UTC).date(): 55.0,
                datetime(2025, 3, 9, 12, 0, tzinfo=UTC).date(): 56.0,
            }
            observed_series = {
                datetime(2025, 3, 8, 12, 0, tzinfo=UTC).date(): 54.0,
                datetime(2025, 3, 9, 12, 0, tzinfo=UTC).date(): 57.0,
            }

            with (
                patch.object(self.smoke, "_discover_supported_closed_weather_markets_for_calibration", return_value=[]),
                patch.object(self.smoke, "_fetch_historical_forecast_series", return_value=forecast_series),
                patch.object(self.smoke, "_fetch_observed_series_from_archive", return_value=observed_series),
            ):
                first = self.smoke._bootstrap_calibration_samples_from_archive(
                    db_path=db_path,
                    queue_cfg=queue_cfg,
                    allow_tables=list(self.smoke.FULL_ALLOW_TABLES),
                    lookback_days=365,
                )
                second = self.smoke._bootstrap_calibration_samples_from_archive(
                    db_path=db_path,
                    queue_cfg=queue_cfg,
                    allow_tables=list(self.smoke.FULL_ALLOW_TABLES),
                    lookback_days=365,
                )

            self.assertGreater(first["synthetic_forecast_run_count"], 0)
            self.assertGreater(first["synthetic_sample_count"], 0)
            self.assertEqual(first["synthetic_rebuild_mode"], "incremental_only")
            self.assertEqual(second["synthetic_forecast_run_count"], 0)
            self.assertEqual(second["synthetic_sample_count"], 0)
            self.assertEqual(second["synthetic_missing_date_count"], 0)
            self.assertEqual(second["purged_synthetic_sample_count"], 0)
            self.assertEqual(second["purged_synthetic_forecast_run_count"], 0)
            self.assertEqual(second["synthetic_rebuild_mode"], "incremental_only")


if __name__ == "__main__":
    unittest.main()
