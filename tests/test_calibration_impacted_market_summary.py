from __future__ import annotations

import unittest

import duckdb

from asterion_core.ui.ui_lite_db import _create_calibration_health_summary


class CalibrationImpactedMarketSummaryTest(unittest.TestCase):
    def test_calibration_health_summary_aggregates_impacted_market_counts(self) -> None:
        con = duckdb.connect(":memory:")
        try:
            con.execute("ATTACH ':memory:' AS src")
            con.execute("CREATE SCHEMA ui")
            con.execute("CREATE SCHEMA src.weather")
            con.execute(
                """
                CREATE TABLE src.weather.forecast_calibration_profiles_v2 (
                    profile_key TEXT,
                    station_id TEXT,
                    source TEXT,
                    metric TEXT,
                    forecast_horizon_bucket TEXT,
                    season_bucket TEXT,
                    regime_bucket TEXT,
                    sample_count BIGINT,
                    mean_bias DOUBLE,
                    mean_abs_residual DOUBLE,
                    p90_abs_residual DOUBLE,
                    empirical_coverage_50 DOUBLE,
                    empirical_coverage_80 DOUBLE,
                    empirical_coverage_95 DOUBLE,
                    regime_stability_score DOUBLE,
                    residual_quantiles_json TEXT,
                    threshold_probability_profile_json TEXT,
                    calibration_health_status TEXT,
                    window_start TIMESTAMP,
                    window_end TIMESTAMP,
                    materialized_at TIMESTAMP
                )
                """
            )
            con.execute(
                """
                INSERT INTO src.weather.forecast_calibration_profiles_v2 VALUES
                ('cp_1','KSEA','openmeteo','temperature_max','0-1','spring','warm',12,0.2,1.1,2.0,0.5,0.8,0.95,0.9,'{}','{}','healthy','2026-03-18 00:00:00','2026-03-18 02:00:00','2026-03-18 03:15:00')
                """
            )
            con.execute(
                """
                CREATE TABLE ui.market_opportunity_summary(
                    market_id TEXT,
                    station_id TEXT,
                    calibration_gate_status TEXT,
                    calibration_impacted_market BOOLEAN
                )
                """
            )
            con.execute(
                """
                INSERT INTO ui.market_opportunity_summary VALUES
                ('mkt_1','KSEA','review_required', TRUE),
                ('mkt_2','KSEA','research_only', TRUE),
                ('mkt_3','KSEA','clear', FALSE)
                """
            )
            counts: dict[str, int] = {}
            _create_calibration_health_summary(con, table_row_counts=counts)
            row = con.execute(
                """
                SELECT impacted_market_count, hard_gate_market_count, review_required_market_count, research_only_market_count
                FROM ui.calibration_health_summary
                """
            ).fetchone()
        finally:
            con.close()

        self.assertEqual(counts["ui.calibration_health_summary"], 1)
        self.assertEqual(tuple(int(value) for value in row), (2, 2, 1, 1))


if __name__ == "__main__":
    unittest.main()
