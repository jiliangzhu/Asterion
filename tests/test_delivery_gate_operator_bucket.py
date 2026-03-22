from __future__ import annotations

import unittest

from asterion_core.ui.builders.opportunity_builder import _classify_operator_bucket


class DeliveryGateOperatorBucketTest(unittest.TestCase):
    def test_degraded_delivery_forces_review_required(self) -> None:
        bucket, priority, reasons = _classify_operator_bucket(
            actionability_status="actionable",
            allocation_status="approved",
            agent_review_status="passed",
            market_quality_status="pass",
            calibration_freshness_status="fresh",
            calibration_gate_status="clear",
            source_freshness_status="fresh",
            feedback_status="healthy",
            source_truth_status="canonical",
            live_prereq_status="ready",
            surface_delivery_status="degraded_source",
        )
        self.assertEqual(bucket, "review_required")
        self.assertEqual(priority, 3)
        self.assertIn("surface_delivery:degraded_source", reasons)

    def test_missing_delivery_blocks_ready_now(self) -> None:
        bucket, priority, reasons = _classify_operator_bucket(
            actionability_status="actionable",
            allocation_status="approved",
            agent_review_status="passed",
            market_quality_status="pass",
            calibration_freshness_status="fresh",
            calibration_gate_status="clear",
            source_freshness_status="fresh",
            feedback_status="healthy",
            source_truth_status="canonical",
            live_prereq_status="ready",
            surface_delivery_status="missing",
        )
        self.assertEqual(bucket, "blocked")
        self.assertEqual(priority, 4)
        self.assertIn("surface_delivery:missing", reasons)


if __name__ == "__main__":
    unittest.main()
