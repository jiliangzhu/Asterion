from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from asterion_core.ui.builders.execution_builder import _create_cohort_history_summary


class UiCohortHistorySummaryTest(unittest.TestCase):
    def test_cohort_history_summary_uses_latest_run_and_overlay_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "cohort_history.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute("ATTACH ':memory:' AS src")
                con.execute("CREATE SCHEMA ui")
                con.execute("CREATE SCHEMA src.runtime")
                con.execute(
                    """
                    CREATE TABLE src.runtime.ranking_retrospective_runs(
                        run_id TEXT,
                        window_end TIMESTAMP,
                        created_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO src.runtime.ranking_retrospective_runs VALUES
                    ('retro_old', '2026-03-17 00:00:00', '2026-03-17 01:00:00'),
                    ('retro_new', '2026-03-18 00:00:00', '2026-03-18 01:00:00')
                    """
                )
                con.execute(
                    """
                    CREATE TABLE src.runtime.ranking_retrospective_rows(
                        row_id TEXT,
                        run_id TEXT,
                        market_id TEXT,
                        strategy_id TEXT,
                        side TEXT,
                        ranking_decile BIGINT,
                        top_k_bucket TEXT,
                        evaluation_status TEXT,
                        window_start TIMESTAMP,
                        window_end TIMESTAMP,
                        submitted_capture_ratio DOUBLE,
                        fill_capture_ratio DOUBLE,
                        resolution_capture_ratio DOUBLE,
                        avg_ranking_score DOUBLE,
                        avg_edge_bps_executable DOUBLE,
                        avg_realized_pnl DOUBLE,
                        avg_predicted_vs_realized_gap DOUBLE,
                        forecast_replay_change_rate DOUBLE,
                        top_rank_share_of_realized_pnl DOUBLE,
                        created_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO src.runtime.ranking_retrospective_rows VALUES
                    ('row_old', 'retro_old', 'mkt_1', 'weather_primary', 'BUY', 2, 'top_10', 'resolved', '2026-03-16 00:00:00', '2026-03-17 00:00:00', 0.4, 0.4, 0.4, 0.10, 80.0, 0.01, 0.02, 0.0, 0.20, '2026-03-17 01:00:00'),
                    ('row_new', 'retro_new', 'mkt_1', 'weather_primary', 'BUY', 1, 'top_5', 'resolved', '2026-03-17 00:00:00', '2026-03-18 00:00:00', 0.9, 0.8, 0.7, 0.42, 120.0, 0.08, 0.01, 0.05, 0.70, '2026-03-18 01:00:00')
                    """
                )
                con.execute(
                    """
                    CREATE TABLE ui.execution_science_summary(
                        cohort_type TEXT,
                        cohort_key TEXT,
                        feedback_status TEXT,
                        feedback_penalty DOUBLE,
                        cohort_prior_version TEXT,
                        source_badge TEXT,
                        source_truth_status TEXT,
                        primary_score_label TEXT
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.execution_science_summary VALUES
                    ('strategy', 'weather_primary', 'watch', 0.12, 'feedback_v1', 'derived', 'derived', 'ranking_score')
                    """
                )
                con.execute(
                    """
                    CREATE TABLE ui.market_opportunity_summary(
                        market_id TEXT,
                        calibration_freshness_status TEXT,
                        source_badge TEXT,
                        source_truth_status TEXT,
                        primary_score_label TEXT
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.market_opportunity_summary VALUES
                    ('mkt_1', 'stale', 'canonical', 'canonical', 'ranking_score')
                    """
                )

                counts: dict[str, int] = {}
                _create_cohort_history_summary(con, table_row_counts=counts)
                row = con.execute(
                    """
                    SELECT run_id, history_row_id, feedback_status, feedback_penalty, cohort_prior_version,
                           calibration_freshness_status, source_badge, source_truth_status
                    FROM ui.cohort_history_summary
                    """
                ).fetchone()
            finally:
                con.close()

        self.assertEqual(counts["ui.cohort_history_summary"], 1)
        self.assertEqual(str(row[0]), "retro_new")
        self.assertEqual(str(row[1]), "row_new")
        self.assertEqual(str(row[2]), "watch")
        self.assertAlmostEqual(float(row[3]), 0.12)
        self.assertEqual(str(row[4]), "feedback_v1")
        self.assertEqual(str(row[5]), "stale")
        self.assertEqual(str(row[6]), "derived")
        self.assertEqual(str(row[7]), "derived")


if __name__ == "__main__":
    unittest.main()
