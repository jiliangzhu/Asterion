from __future__ import annotations

import unittest

from asterion_core.ui.read_model_registry import builder_names_in_order, builder_registry


class UiLiteBuilderRegistryTest(unittest.TestCase):
    def test_builder_registry_order_and_ownership_are_stable(self) -> None:
        self.assertEqual(
            builder_names_in_order(),
            (
                "readiness_builder",
                "opportunity_builder",
                "execution_builder",
                "ops_review_builder",
                "catalog_builder",
            ),
        )
        registry = builder_registry()
        self.assertIn("ui.market_opportunity_summary", registry["opportunity_builder"])
        self.assertIn("ui.action_queue_summary", registry["opportunity_builder"])
        self.assertIn("ui.execution_science_summary", registry["execution_builder"])
        self.assertIn("ui.cohort_history_summary", registry["execution_builder"])
        self.assertEqual(registry["catalog_builder"], ("ui.read_model_catalog", "ui.truth_source_checks"))


if __name__ == "__main__":
    unittest.main()
