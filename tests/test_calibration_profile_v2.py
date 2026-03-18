from __future__ import annotations

import importlib.util
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from domains.weather.forecast.calibration import (
    calibration_regime_bucket,
    materialize_forecast_calibration_profiles_v2,
)

HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required for calibration profile v2 tests")
class CalibrationProfileV2Test(unittest.TestCase):
    def test_regime_bucket_assignment(self) -> None:
        self.assertEqual(calibration_regime_bucket(35.0), "cold")
        self.assertEqual(calibration_regime_bucket(45.0), "mild")
        self.assertEqual(calibration_regime_bucket(72.0), "warm")
        self.assertEqual(calibration_regime_bucket(83.0), "hot")

    def test_materializes_profile_rows_from_samples(self) -> None:
        import duckdb

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "phase13.duckdb"
            con = duckdb.connect(str(db_path))
            con.execute("CREATE SCHEMA weather")
            con.execute(
                """
                CREATE TABLE weather.forecast_calibration_samples (
                    market_id TEXT,
                    station_id TEXT,
                    source TEXT,
                    metric TEXT,
                    forecast_horizon_bucket TEXT,
                    season_bucket TEXT,
                    forecast_target_time TIMESTAMP,
                    forecast_mean DOUBLE,
                    observed_value DOUBLE,
                    residual DOUBLE,
                    created_at TIMESTAMP
                )
                """
            )
            con.execute(
                """
                CREATE TABLE weather.weather_forecast_runs (
                    market_id TEXT,
                    station_id TEXT,
                    source TEXT,
                    forecast_target_time TIMESTAMP,
                    forecast_payload_json TEXT
                )
                """
            )
            con.execute(
                """
                CREATE TABLE weather.weather_market_specs (
                    market_id TEXT,
                    bucket_min_value DOUBLE,
                    bucket_max_value DOUBLE,
                    inclusive_bounds BOOLEAN
                )
                """
            )
            now = datetime(2026, 3, 17, 12, 0, tzinfo=UTC).replace(tzinfo=None)
            target_time = now - timedelta(days=1)
            con.execute(
                "INSERT INTO weather.weather_market_specs VALUES ('mkt_1', 60.0, 69.0, TRUE)"
            )
            for idx, observed in enumerate([66.0, 67.0, 64.0, 62.0, 70.0, 69.0, 65.0, 63.0, 68.0, 66.0], start=1):
                con.execute(
                    """
                    INSERT INTO weather.forecast_calibration_samples VALUES (?, 'KNYC', 'openmeteo', 'temperature_max', '0-1', 'spring', ?, 65.0, ?, ?, ?)
                    """,
                    [f"mkt_1", target_time, observed, observed - 65.0, now - timedelta(hours=idx)],
                )
                con.execute(
                    """
                    INSERT INTO weather.weather_forecast_runs VALUES ('mkt_1', 'KNYC', 'openmeteo', ?, ?)
                    """,
                    [target_time, '{"temperature_distribution":{"63":0.2,"65":0.6,"67":0.2}}'],
                )
            materialized = materialize_forecast_calibration_profiles_v2(con, as_of=now, lookback_days=180)
            con.close()

            self.assertEqual(len(materialized), 1)
            profile = materialized[0]
            self.assertEqual(profile.station_id, "KNYC")
            self.assertEqual(profile.source, "openmeteo")
            self.assertEqual(profile.metric, "temperature_max")
            self.assertEqual(profile.regime_bucket, "warm")
            self.assertEqual(profile.sample_count, 10)
            self.assertIsNotNone(profile.threshold_probability_profile_json)
            self.assertIn("90-100", profile.threshold_probability_profile_json)


if __name__ == "__main__":
    unittest.main()
