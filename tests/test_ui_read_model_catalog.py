from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from asterion_core.ui import validate_ui_lite_db
from asterion_core.ui.read_model_registry import get_read_model_catalog_record, iter_read_model_catalog_records
from tests.ui_read_model_test_utils import build_minimal_ui_read_model_db


class UiReadModelCatalogTest(unittest.TestCase):
    def test_catalog_contains_all_registered_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ui_read_model.duckdb"
            build_minimal_ui_read_model_db(db_path)
            con = duckdb.connect(str(db_path), read_only=True)
            try:
                rows = con.execute(
                    "SELECT table_name, schema_version, builder_name, primary_score_column FROM ui.read_model_catalog ORDER BY table_name"
                ).fetchall()
            finally:
                con.close()

        self.assertEqual(len(rows), len(tuple(iter_read_model_catalog_records())))
        self.assertIn(("ui.action_queue_summary", "v1", "opportunity_builder", "ranking_score"), rows)
        self.assertIn(("ui.cohort_history_summary", "v1", "execution_builder", None), rows)
        self.assertIn(("ui.market_opportunity_summary", "v1", "opportunity_builder", "ranking_score"), rows)
        self.assertIn(("ui.surface_delivery_summary", "v1", "catalog_builder", None), rows)
        self.assertIn(("ui.system_runtime_summary", "v1", "catalog_builder", None), rows)
        self.assertIn(("ui.truth_source_checks", "v1", "catalog_builder", None), rows)

        market_record = get_read_model_catalog_record("ui.market_opportunity_summary")
        assert market_record is not None
        self.assertIn("surface_delivery_reason_codes_json", market_record.required_columns)

        queue_record = get_read_model_catalog_record("ui.action_queue_summary")
        assert queue_record is not None
        self.assertIn("surface_delivery_reason_codes_json", queue_record.required_columns)

        delivery_record = get_read_model_catalog_record("ui.surface_delivery_summary")
        assert delivery_record is not None
        self.assertIn("fallback_origin", delivery_record.required_columns)
        self.assertIn("degraded_reason_codes_json", delivery_record.required_columns)

    def test_validate_ui_lite_db_accepts_catalog_and_truth_checks(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ui_read_model_validate.duckdb"
            build_minimal_ui_read_model_db(db_path)
            counts = validate_ui_lite_db(str(db_path))
        self.assertIn("ui.read_model_catalog", counts)
        self.assertIn("ui.truth_source_checks", counts)


if __name__ == "__main__":
    unittest.main()
