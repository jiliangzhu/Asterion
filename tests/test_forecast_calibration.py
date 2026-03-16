from __future__ import annotations

import importlib.util
import os
import tempfile
import unittest
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import patch

from asterion_core.contracts import ForecastRunRecord
from asterion_core.storage.database import DuckDBConfig, connect_duckdb
from asterion_core.storage.db_migrate import MigrationConfig, apply_migrations
from asterion_core.storage.write_queue import WriteQueueConfig
from asterion_core.storage.writerd import process_one
from domains.weather.forecast import (
    DuckDBForecastStdDevProvider,
    build_forecast_calibration_sample,
    build_forecast_run_record,
    calibration_confidence_from_metrics,
    enqueue_forecast_calibration_sample_upserts,
    forecast_distribution_mean,
    resolve_std_dev_summary,
)
from domains.weather.forecast.adapters import OpenMeteoAdapter
from domains.weather.forecast.service import ForecastDistribution


HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


def _forecast_run() -> ForecastRunRecord:
    return ForecastRunRecord(
        run_id="frun_1",
        market_id="mkt_1",
        condition_id="cond_1",
        station_id="KSEA",
        source="openmeteo",
        model_run="2026-03-10T12:00Z",
        forecast_target_time=datetime(2026, 3, 10, 12, 0, tzinfo=UTC),
        observation_date=date(2026, 3, 11),
        metric="temperature_max",
        latitude=47.6062,
        longitude=-122.3321,
        timezone="America/Los_Angeles",
        spec_version="spec_v1",
        cache_key="cache_1",
        source_trace=["openmeteo"],
        fallback_used=False,
        from_cache=False,
        confidence=0.72,
        forecast_payload={"temperature_distribution": {50: 0.2, 52: 0.6, 54: 0.2}},
        raw_payload={"daily": {"temperature_2m_max": [52.0]}},
    )


class ForecastCalibrationUnitTest(unittest.TestCase):
    def test_build_forecast_calibration_sample(self) -> None:
        sample = build_forecast_calibration_sample(forecast_run=_forecast_run(), observed_value=56.0)
        self.assertEqual(sample.station_id, "KSEA")
        self.assertEqual(sample.forecast_horizon_bucket, "0-1")
        self.assertEqual(sample.season_bucket, "spring")
        self.assertAlmostEqual(sample.forecast_mean, forecast_distribution_mean({50: 0.2, 52: 0.6, 54: 0.2}))
        self.assertAlmostEqual(sample.residual, 4.0)

    def test_calibration_confidence_summary_marks_lookup_missing(self) -> None:
        summary = calibration_confidence_from_metrics(
            sample_count=0,
            mean_abs_residual=None,
            p90_abs_residual=None,
            lookup_hit=False,
        )
        self.assertEqual(summary.calibration_health_status, "lookup_missing")
        self.assertEqual(summary.calibration_confidence_multiplier, 0.50)
        self.assertIn("calibration_lookup_missing", summary.reason_codes)


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required for forecast calibration tests")
class ForecastCalibrationPersistenceTest(unittest.TestCase):
    def test_provider_uses_persisted_calibration_samples(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            queue_path = str(Path(tmpdir) / "queue.sqlite")
            migrations_dir = str(Path(__file__).resolve().parents[1] / "sql" / "migrations")
            with patch.dict(
                os.environ,
                {"ASTERION_STRICT_SINGLE_WRITER": "1", "ASTERION_DB_ROLE": "writer", "WRITERD": "1"},
                clear=False,
            ):
                apply_migrations(MigrationConfig(db_path=db_path, migrations_dir=migrations_dir))

            queue_cfg = WriteQueueConfig(path=queue_path)
            run = _forecast_run()
            samples = [
                build_forecast_calibration_sample(forecast_run=run, observed_value=56.0),
                build_forecast_calibration_sample(
                    forecast_run=ForecastRunRecord(**{**run.__dict__, "run_id": "frun_2"}),
                    observed_value=48.0,
                ),
            ]
            enqueue_forecast_calibration_sample_upserts(queue_cfg, samples=samples, run_id="run_calibration")
            with patch.dict(
                os.environ,
                {"ASTERION_DB_PATH": db_path, "ASTERION_WRITERD_ALLOWED_TABLES": "weather.forecast_calibration_samples"},
                clear=False,
            ):
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))

            provider = DuckDBForecastStdDevProvider(db_path)
            resolved = provider.resolve_std_dev(
                station_id="KSEA",
                source="openmeteo",
                observation_date=date(2026, 3, 11),
                forecast_target_time=datetime(2026, 3, 10, 12, 0, tzinfo=UTC),
                metric="temperature_max",
            )
            self.assertIsNotNone(resolved)
            self.assertGreater(float(resolved), 0.0)
            summary = provider.resolve_confidence_summary(
                station_id="KSEA",
                source="openmeteo",
                observation_date=date(2026, 3, 11),
                forecast_target_time=datetime(2026, 3, 10, 12, 0, tzinfo=UTC),
                metric="temperature_max",
            )
            self.assertIsNotNone(summary)
            self.assertEqual(summary.sample_count, 2)

    def test_adapter_falls_back_when_lookup_missing(self) -> None:
        class _Client:
            def get_json(self, url: str, *, context: dict[str, object]) -> object:
                del url, context
                return {"daily": {"temperature_2m_max": [55.0]}}

        adapter = OpenMeteoAdapter(client=_Client(), std_dev_provider=DuckDBForecastStdDevProvider("/tmp/does-not-exist.duckdb"))
        request = build_forecast_run_record(
            ForecastDistribution(
                market_id="mkt_1",
                condition_id="cond_1",
                station_id="KSEA",
                source="openmeteo",
                model_run="2026-03-10T12:00Z",
                forecast_target_time=datetime(2026, 3, 10, 12, 0, tzinfo=UTC),
                observation_date=date(2026, 3, 11),
                metric="temperature_max",
                latitude=47.6062,
                longitude=-122.3321,
                timezone="America/Los_Angeles",
                spec_version="spec_v1",
                temperature_distribution={55: 1.0},
                source_trace=["openmeteo"],
                raw_payload={},
                from_cache=False,
                fallback_used=False,
                cache_key="cache_1",
            )
        )
        distribution = adapter.fetch_forecast(
            type(
                "Req",
                (),
                {
                    "market_id": request.market_id,
                    "condition_id": request.condition_id,
                    "station_id": request.station_id,
                    "source": request.source,
                    "model_run": request.model_run,
                    "forecast_target_time": request.forecast_target_time,
                    "observation_date": request.observation_date,
                    "metric": request.metric,
                    "latitude": request.latitude,
                    "longitude": request.longitude,
                    "timezone": request.timezone,
                    "spec_version": request.spec_version,
                },
            )()
        )
        self.assertAlmostEqual(sum(distribution.temperature_distribution.values()), 1.0)
        summary = resolve_std_dev_summary(
            type(
                "Req",
                (),
                {
                    "market_id": request.market_id,
                    "condition_id": request.condition_id,
                    "station_id": request.station_id,
                    "source": request.source,
                    "model_run": request.model_run,
                    "forecast_target_time": request.forecast_target_time,
                    "observation_date": request.observation_date,
                    "metric": request.metric,
                    "latitude": request.latitude,
                    "longitude": request.longitude,
                    "timezone": request.timezone,
                    "spec_version": request.spec_version,
                },
            )(),
            DuckDBForecastStdDevProvider("/tmp/does-not-exist.duckdb"),
        )
        self.assertFalse(summary.lookup_hit)
        self.assertEqual(summary.calibration_health_status, "lookup_missing")


if __name__ == "__main__":
    unittest.main()
