from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from tests.ui_read_model_test_utils import build_minimal_ui_read_model_db


class TruthSourceChecksTest(unittest.TestCase):
    def test_truth_source_checks_are_ok_when_contract_is_complete(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ui_truth_ok.duckdb"
            build_minimal_ui_read_model_db(db_path)
            con = duckdb.connect(str(db_path), read_only=True)
            try:
                failed = con.execute("SELECT COUNT(*) FROM ui.truth_source_checks WHERE check_status = 'fail'").fetchone()
            finally:
                con.close()
        self.assertEqual(int(failed[0]), 0)

    def test_truth_source_checks_fail_when_critical_columns_are_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ui_truth_fail.duckdb"
            build_minimal_ui_read_model_db(
                db_path,
                skip_columns={"ui.market_opportunity_summary": {"primary_score_label"}},
            )
            con = duckdb.connect(str(db_path), read_only=True)
            try:
                rows = con.execute(
                    """
                    SELECT surface_id, table_name, check_status, issues_json
                    FROM ui.truth_source_checks
                    WHERE table_name = 'ui.market_opportunity_summary'
                    ORDER BY surface_id
                    """
                ).fetchall()
            finally:
                con.close()
        self.assertTrue(rows)
        self.assertTrue(all(str(row[2]) == "fail" for row in rows))
        self.assertTrue(any("primary_score_label" in str(row[3]) for row in rows))

    def test_truth_source_checks_warn_when_table_is_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ui_truth_warn.duckdb"
            build_minimal_ui_read_model_db(db_path, empty_tables={"ui.execution_science_summary"})
            con = duckdb.connect(str(db_path), read_only=True)
            try:
                rows = con.execute(
                    """
                    SELECT check_status
                    FROM ui.truth_source_checks
                    WHERE surface_id = 'execution' AND table_name = 'ui.execution_science_summary'
                    """
                ).fetchall()
            finally:
                con.close()
        self.assertEqual({str(row[0]) for row in rows}, {"warn"})

    def test_truth_source_checks_cover_phase4_workflow_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ui_truth_phase4.duckdb"
            build_minimal_ui_read_model_db(db_path)
            con = duckdb.connect(str(db_path), read_only=True)
            try:
                rows = con.execute(
                    """
                    SELECT surface_id, table_name, check_status
                    FROM ui.truth_source_checks
                    WHERE table_name IN ('ui.action_queue_summary', 'ui.cohort_history_summary')
                    ORDER BY surface_id, table_name
                    """
                ).fetchall()
            finally:
                con.close()
        self.assertIn(("execution", "ui.cohort_history_summary", "ok"), rows)
        self.assertIn(("home", "ui.action_queue_summary", "ok"), rows)
        self.assertIn(("markets", "ui.action_queue_summary", "ok"), rows)
        self.assertIn(("markets", "ui.cohort_history_summary", "ok"), rows)

    def test_truth_source_checks_lock_p8_gate_and_scaling_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ui_truth_p8.duckdb"
            build_minimal_ui_read_model_db(
                db_path,
                skip_columns={
                    "ui.market_opportunity_summary": {"calibration_impacted_market"},
                    "ui.action_queue_summary": {"capital_policy_id"},
                    "ui.calibration_health_summary": {"research_only_market_count"},
                },
            )
            con = duckdb.connect(str(db_path), read_only=True)
            try:
                rows = con.execute(
                    """
                    SELECT table_name, check_status, issues_json
                    FROM ui.truth_source_checks
                    WHERE table_name IN (
                        'ui.market_opportunity_summary',
                        'ui.action_queue_summary',
                        'ui.calibration_health_summary'
                    )
                    ORDER BY table_name
                    """
                ).fetchall()
            finally:
                con.close()
        self.assertTrue(all(str(row[1]) == "fail" for row in rows))
        self.assertTrue(any("calibration_impacted_market" in str(row[2]) for row in rows))
        self.assertTrue(any("capital_policy_id" in str(row[2]) for row in rows))
        self.assertTrue(any("research_only_market_count" in str(row[2]) for row in rows))

    def test_truth_source_checks_lock_p9_delivery_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ui_truth_p9.duckdb"
            build_minimal_ui_read_model_db(
                db_path,
                skip_columns={
                    "ui.market_opportunity_summary": {"surface_delivery_status", "surface_delivery_reason_codes_json"},
                    "ui.action_queue_summary": {"surface_fallback_origin", "surface_delivery_reason_codes_json"},
                    "ui.surface_delivery_summary": {"degraded_reason_codes_json"},
                    "ui.system_runtime_summary": {"degraded_surface_count"},
                },
            )
            con = duckdb.connect(str(db_path), read_only=True)
            try:
                rows = con.execute(
                    """
                    SELECT table_name, check_status, issues_json
                    FROM ui.truth_source_checks
                    WHERE table_name IN (
                        'ui.market_opportunity_summary',
                        'ui.action_queue_summary',
                        'ui.surface_delivery_summary',
                        'ui.system_runtime_summary'
                    )
                    ORDER BY table_name
                    """
                ).fetchall()
            finally:
                con.close()
        self.assertTrue(rows)
        self.assertTrue(all(str(row[1]) == "fail" for row in rows))
        self.assertTrue(any("surface_delivery_status" in str(row[2]) for row in rows))
        self.assertTrue(any("surface_fallback_origin" in str(row[2]) for row in rows))
        self.assertTrue(any("surface_delivery_reason_codes_json" in str(row[2]) for row in rows))
        self.assertTrue(any("degraded_reason_codes_json" in str(row[2]) for row in rows))
        self.assertTrue(any("degraded_surface_count" in str(row[2]) for row in rows))

    def test_truth_source_checks_lock_p11_triage_closeout_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ui_truth_p11.duckdb"
            build_minimal_ui_read_model_db(
                db_path,
                skip_columns={
                    "ui.opportunity_triage_summary": {"advisory_gate_reason_codes_json", "latest_evaluation_verified"},
                    "ui.system_runtime_summary": {"triage_advisory_gate_status", "triage_failed_count"},
                },
            )
            con = duckdb.connect(str(db_path), read_only=True)
            try:
                rows = con.execute(
                    """
                    SELECT table_name, check_status, issues_json
                    FROM ui.truth_source_checks
                    WHERE table_name IN (
                        'ui.opportunity_triage_summary',
                        'ui.system_runtime_summary'
                    )
                    ORDER BY table_name
                    """
                ).fetchall()
            finally:
                con.close()
        self.assertTrue(rows)
        self.assertTrue(all(str(row[1]) == "fail" for row in rows))
        self.assertTrue(any("advisory_gate_reason_codes_json" in str(row[2]) for row in rows))
        self.assertTrue(any("latest_evaluation_verified" in str(row[2]) for row in rows))
        self.assertTrue(any("triage_advisory_gate_status" in str(row[2]) for row in rows))
        self.assertTrue(any("triage_failed_count" in str(row[2]) for row in rows))


if __name__ == "__main__":
    unittest.main()
