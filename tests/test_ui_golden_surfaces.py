from __future__ import annotations

import unittest

from asterion_core.ui.read_model_registry import get_read_model_catalog_record
from ui.surface_truth import load_boundary_sidebar_summary, load_primary_score_descriptor, load_surface_truth_descriptors


class UiGoldenSurfacesTest(unittest.TestCase):
    def test_primary_score_and_badge_contracts_are_pinned(self) -> None:
        descriptor = load_primary_score_descriptor()
        self.assertEqual(descriptor.primary_score, "ranking_score")
        self.assertEqual(descriptor.primary_score_label, "Ranking Score")

        surfaces = load_surface_truth_descriptors()
        self.assertTrue(surfaces["home_top_opportunities"].supports_source_badges)
        self.assertEqual(surfaces["markets_coverage"].primary_score, "ranking_score")

        record = get_read_model_catalog_record("ui.market_opportunity_summary")
        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record.primary_score_column, "ranking_score")
        self.assertIn("base_ranking_score", record.required_columns)
        self.assertIn("deployable_expected_pnl", record.required_columns)
        self.assertIn("calibration_gate_status", record.required_columns)
        self.assertIn("source_badge", record.required_columns)
        self.assertIn("surface_delivery_reason_codes_json", record.required_columns)
        self.assertIn("primary_score_label", record.required_columns)

        action_queue_record = get_read_model_catalog_record("ui.action_queue_summary")
        self.assertIsNotNone(action_queue_record)
        assert action_queue_record is not None
        self.assertEqual(action_queue_record.primary_score_column, "ranking_score")
        self.assertIn("operator_bucket", action_queue_record.required_columns)
        self.assertIn("deployable_expected_pnl", action_queue_record.required_columns)
        self.assertIn("calibration_gate_status", action_queue_record.required_columns)
        self.assertIn("binding_limit_scope", action_queue_record.required_columns)
        self.assertIn("surface_delivery_status", action_queue_record.required_columns)
        self.assertIn("surface_fallback_origin", action_queue_record.required_columns)
        self.assertIn("surface_delivery_reason_codes_json", action_queue_record.required_columns)

        cohort_history_record = get_read_model_catalog_record("ui.cohort_history_summary")
        self.assertIsNotNone(cohort_history_record)
        assert cohort_history_record is not None
        self.assertIsNone(cohort_history_record.primary_score_column)
        self.assertIn("submitted_capture_ratio", cohort_history_record.required_columns)

        delivery_record = get_read_model_catalog_record("ui.surface_delivery_summary")
        self.assertIsNotNone(delivery_record)
        assert delivery_record is not None
        self.assertIn("delivery_status", delivery_record.required_columns)
        self.assertIn("fallback_origin", delivery_record.required_columns)
        self.assertIn("truth_check_issue_count", delivery_record.required_columns)
        self.assertIn("degraded_reason_codes_json", delivery_record.required_columns)

        system_record = get_read_model_catalog_record("ui.system_runtime_summary")
        self.assertIsNotNone(system_record)
        assert system_record is not None
        self.assertIn("latest_surface_refresh_status", system_record.required_columns)
        self.assertIn("degraded_surface_count", system_record.required_columns)

    def test_boundary_copy_baseline_remains_pinned(self) -> None:
        summary = load_boundary_sidebar_summary()
        self.assertEqual(summary.system_positioning, "operator console + constrained execution infra")
        self.assertIn("not unattended live", summary.live_negations)


if __name__ == "__main__":
    unittest.main()
