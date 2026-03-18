from __future__ import annotations

import unittest
from unittest.mock import patch

from ui.data_access import load_boundary_sidebar_truth
from ui.surface_truth import (
    CURRENT_PHASE_STATUS,
    PRIMARY_SCORE_FIELD,
    SYSTEM_POSITIONING,
    TRUTH_SOURCE_DOC,
    load_primary_score_descriptor,
    load_surface_truth_descriptors,
)


class OperatorTruthSourceTest(unittest.TestCase):
    def test_boundary_sidebar_summary_uses_readiness_boundary(self) -> None:
        readiness = {
            "capability_boundary_summary": {
                "manual_only": True,
                "default_off": True,
                "approve_usdc_only": True,
                "constrained_real_submit_enabled": True,
            }
        }
        evidence = {"capability_boundary_summary": readiness["capability_boundary_summary"]}
        runtime_status = {"capability_boundary_summary": readiness["capability_boundary_summary"]}
        surface_status = {
            "readiness": {
                "status": "degraded_source",
                "label": "Readiness 降级",
                "detail": "using fallback",
                "source": "ui_lite",
                "updated_at": None,
            }
        }
        with patch("ui.data_access.load_readiness_summary", return_value=readiness), patch(
            "ui.data_access.load_readiness_evidence_bundle", return_value=evidence
        ), patch("ui.data_access.load_system_runtime_status", return_value=runtime_status), patch(
            "ui.data_access.load_operator_surface_status", return_value=surface_status
        ):
            summary = load_boundary_sidebar_truth()
        self.assertEqual(summary["system_positioning"], SYSTEM_POSITIONING)
        self.assertEqual(summary["current_phase_status"], CURRENT_PHASE_STATUS)
        self.assertEqual(summary["truth_source_doc"], TRUTH_SOURCE_DOC)
        self.assertIn("manual-only", summary["capability_boundary"])
        self.assertIn("default-off", summary["capability_boundary"])
        self.assertIn("approve_usdc only", summary["capability_boundary"])
        self.assertIn("constrained real submit", summary["capability_boundary"])
        self.assertIn("not unattended live", summary["live_negations"])
        self.assertEqual(summary["status"], "degraded_source")

    def test_surface_truth_descriptors_and_primary_score_are_locked(self) -> None:
        descriptors = load_surface_truth_descriptors()
        self.assertEqual(descriptors["home_top_opportunities"].primary_score, PRIMARY_SCORE_FIELD)
        self.assertTrue(descriptors["markets_coverage"].supports_source_badges)
        descriptor = load_primary_score_descriptor()
        self.assertEqual(descriptor.primary_score, PRIMARY_SCORE_FIELD)
        self.assertEqual(descriptor.primary_score_label, "Ranking Score")
        self.assertIn("expected_value_score", descriptor.diagnostics)


if __name__ == "__main__":
    unittest.main()
