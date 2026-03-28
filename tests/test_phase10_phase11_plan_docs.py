from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class Phase10Phase11PlanDocsTest(unittest.TestCase):
    def test_v2_marks_p10_and_p11_accepted(self) -> None:
        v2 = (
            ROOT
            / "docs"
            / "10-implementation"
            / "versions"
            / "v2.0"
            / "phase-plans"
            / "V2_Implementation_Plan.md"
        ).read_text(encoding="utf-8")
        self.assertIn("Phase 9. Operator Surface Delivery and Throughput Scaling", v2)
        self.assertIn("Phase 10. Deterministic ROI Repair and Execution Intelligence Foundation", v2)
        self.assertIn("Phase 11. Opportunity Triage / Execution Intelligence Agent", v2)
        self.assertIn("accepted closeout record", v2)
        self.assertIn("P11_Closeout_Checklist.md", v2)
        self.assertIn("Phase 11 — Opportunity Triage / Execution Intelligence Agent", v2)
        self.assertIn("当前还没有比 `Phase 11` 更新的 tranche-specific implementation plan 已打开", v2)

    def test_phase10_phase11_docs_capture_assessment_split(self) -> None:
        p10 = (
            ROOT
            / "docs"
            / "10-implementation"
            / "versions"
            / "v2.0"
            / "phase-plans"
            / "P10_Implementation_Plan.md"
        ).read_text(encoding="utf-8")
        p11 = (
            ROOT
            / "docs"
            / "10-implementation"
            / "versions"
            / "v2.0"
            / "phase-plans"
            / "P11_Implementation_Plan.md"
        ).read_text(encoding="utf-8")

        self.assertIn("accepted closeout record", p10)
        self.assertIn("pre-agent deterministic", p10)
        self.assertIn("execution-intelligence", p10)
        self.assertIn("run_operator_surface_refresh", p10)
        self.assertIn("`blocked` pollution", p10)
        self.assertIn("ExecutionIntelligenceSummary", p10)
        self.assertIn("runtime.execution_intelligence_summaries", p10)
        self.assertIn("tests.test_execution_intelligence_summary", p10)
        self.assertIn("accepted closeout record", p11)
        self.assertIn("当前没有比 `Phase 11` 更新的 tranche-specific implementation plan 已打开", p11)
        self.assertIn("accepted closeout baseline", p11)
        self.assertIn("advisory-only", p11)
        self.assertIn("agent.*", p11)
        self.assertIn("agent 不进入 canonical execution path", p11)


if __name__ == "__main__":
    unittest.main()
