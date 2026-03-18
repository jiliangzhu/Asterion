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


if __name__ == "__main__":
    unittest.main()
