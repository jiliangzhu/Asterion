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
        self.assertIn("source_badge", record.required_columns)
        self.assertIn("primary_score_label", record.required_columns)

    def test_boundary_copy_baseline_remains_pinned(self) -> None:
        summary = load_boundary_sidebar_summary()
        self.assertEqual(summary.system_positioning, "operator console + constrained execution infra")
        self.assertIn("not unattended live", summary.live_negations)


if __name__ == "__main__":
    unittest.main()
