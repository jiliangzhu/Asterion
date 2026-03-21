from __future__ import annotations

import importlib.util
import os
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from asterion_core.storage.write_queue import WriteQueueConfig
from asterion_core.storage.writerd import process_one
from dagster_asterion.handlers import run_weather_forecast_calibration_profiles_v2_refresh_job
from domains.weather.forecast.calibration import CalibrationProfileV2


HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required")
class CalibrationMaterializationStatusTest(unittest.TestCase):
    def _apply_migrations(self, db_path: str) -> None:
        bootstrap = duckdb.connect(db_path)
        migrations_dir = Path(__file__).resolve().parents[1] / "sql" / "migrations"
        for path in sorted(migrations_dir.glob("*.sql")):
            sql = path.read_text(encoding="utf-8").strip()
            if sql:
                bootstrap.execute(sql)
        bootstrap.close()

    def _drain_queue(self, *, db_path: str, queue_path: str) -> None:
        with unittest.mock.patch.dict(
            os.environ,
            {
                "ASTERION_DB_PATH": db_path,
                "ASTERION_WRITERD_ALLOWED_TABLES": "weather.forecast_calibration_profiles_v2,runtime.calibration_profile_materializations",
            },
            clear=False,
        ):
            while process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False):
                pass

    def test_refresh_job_persists_runtime_materialization_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "phase3_calibration.duckdb")
            queue_path = str(Path(tmpdir) / "write_queue.sqlite")
            self._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                con.execute(
                    """
                    INSERT INTO weather.forecast_calibration_samples (
                        sample_id,
                        market_id,
                        station_id,
                        source,
                        forecast_horizon_bucket,
                        season_bucket,
                        metric,
                        forecast_target_time,
                        forecast_mean,
                        observed_value,
                        residual,
                        created_at
                    ) VALUES (
                        's1','mkt_1','KNYC','openmeteo','0-1','spring','temperature_max',
                        '2026-03-17 12:00:00',65.0,66.0,1.0,'2026-03-17 13:00:00'
                    )
                    """
                )
                materialized_at = datetime(2026, 3, 18, 3, 15, tzinfo=UTC)
                profile = CalibrationProfileV2(
                    profile_key="cpv2_1",
                    station_id="KNYC",
                    source="openmeteo",
                    metric="temperature_max",
                    forecast_horizon_bucket="0-1",
                    season_bucket="spring",
                    regime_bucket="warm",
                    sample_count=12,
                    mean_bias=0.3,
                    mean_abs_residual=1.2,
                    p90_abs_residual=2.0,
                    empirical_coverage_50=0.5,
                    empirical_coverage_80=0.8,
                    empirical_coverage_95=0.95,
                    regime_stability_score=0.9,
                    residual_quantiles_json={"p50": 1.0, "p90": 2.0},
                    threshold_probability_profile_json={"60-75": {"quality_status": "healthy", "sample_count": 10}},
                    calibration_health_status="healthy",
                    window_start=datetime(2025, 9, 19, 3, 15, tzinfo=UTC),
                    window_end=datetime(2026, 3, 18, 2, 0, tzinfo=UTC),
                    materialized_at=materialized_at,
                )
                with unittest.mock.patch(
                    "dagster_asterion.handlers.materialize_forecast_calibration_profiles_v2",
                    return_value=[profile],
                ):
                    result = run_weather_forecast_calibration_profiles_v2_refresh_job(
                        con,
                        WriteQueueConfig(path=queue_path),
                        lookback_days=180,
                        as_of=materialized_at,
                        run_id="run_calibration_phase3",
                    )
            finally:
                con.close()

            self._drain_queue(db_path=db_path, queue_path=queue_path)
            qcon = duckdb.connect(db_path)
            try:
                row = qcon.execute(
                    """
                    SELECT materialization_id, run_id, status, output_profile_count, fresh_profile_count, stale_profile_count, degraded_profile_count
                    FROM runtime.calibration_profile_materializations
                    """
                ).fetchone()
            finally:
                qcon.close()

        self.assertIsNotNone(row)
        self.assertEqual(result.metadata["materialization_id"], row[0])
        self.assertEqual(row[1], "run_calibration_phase3")
        self.assertEqual(row[2], "ok")
        self.assertEqual(row[3], 1)
        self.assertEqual(row[4], 1)
        self.assertEqual(row[5], 0)
        self.assertEqual(row[6], 0)


if __name__ == "__main__":
    unittest.main()
