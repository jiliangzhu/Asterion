from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class P4PlanDocsTest(unittest.TestCase):
    def test_p4_plan_targets_exist(self) -> None:
        expected = [
            ROOT / "docs" / "10-implementation" / "versions" / "v1.0" / "phase-plans" / "P4_Implementation_Plan.md",
            ROOT / "docs" / "00-overview" / "versions" / "v2.0" / "DEVELOPMENT_ROADMAP.md",
            ROOT / "docs" / "10-implementation" / "Implementation_Index.md",
            ROOT / "docs" / "00-overview" / "Documentation_Index.md",
            ROOT / "README.md",
        ]
        missing = [str(path) for path in expected if not path.exists()]
        self.assertEqual(missing, [])

    def test_navigation_docs_reference_p4_plan(self) -> None:
        expected_refs = {
            ROOT / "README.md": [
                "P4_Implementation_Plan.md",
                "P4_Closeout_Checklist.md",
                "P4_Controlled_Rollout_Decision_Runbook.md",
            ],
            ROOT / "docs" / "10-implementation" / "Implementation_Index.md": [
                "P4_Implementation_Plan.md",
                "P4_Closeout_Checklist.md",
                "P4_Controlled_Rollout_Decision_Runbook.md",
            ],
            ROOT / "docs" / "00-overview" / "Documentation_Index.md": [
                "P4_Implementation_Plan.md",
                "P4_Closeout_Checklist.md",
                "P4_Controlled_Rollout_Decision_Runbook.md",
            ],
            ROOT / "docs" / "00-overview" / "versions" / "v2.0" / "DEVELOPMENT_ROADMAP.md": [
                "P4_Implementation_Plan.md",
                "P4_Closeout_Checklist.md",
                "P4_Controlled_Rollout_Decision_Runbook.md",
            ],
            ROOT / "docs" / "00-overview" / "versions" / "v2.0" / "Asterion_Project_Plan.md": ["P4_Implementation_Plan.md"],
        }
        missing_refs: list[str] = []
        for path, refs in expected_refs.items():
            text = path.read_text(encoding="utf-8")
            for ref in refs:
                if ref not in text:
                    missing_refs.append(f"{path}:{ref}")
        self.assertEqual(missing_refs, [])

    def test_p4_plan_keeps_live_prereq_boundary(self) -> None:
        plan = (ROOT / "docs" / "10-implementation" / "versions" / "v1.0" / "phase-plans" / "P4_Implementation_Plan.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("P4` 不是 production live rollout", plan)
        self.assertIn("default-off + explicit operator approval + auditable", plan)
        self.assertIn("ready for controlled live rollout decision", plan)
        self.assertIn("evaluate_p4_live_prereq_readiness", plan)
        self.assertIn("weather_live_prereq_readiness", plan)
        self.assertIn("weather_controlled_live_smoke", plan)
        self.assertIn("P4_Closeout_Checklist.md", plan)
        self.assertIn("P4_Controlled_Rollout_Decision_Runbook.md", plan)

    def test_p4_docs_reflect_v2_implementation_active_and_historical_status(self) -> None:
        expected = {
            ROOT / "README.md": "v2.0 implementation active",
            ROOT / "docs" / "00-overview" / "versions" / "v2.0" / "DEVELOPMENT_ROADMAP.md": "v2.0 implementation active",
            ROOT / "docs" / "00-overview" / "versions" / "v2.0" / "Asterion_Project_Plan.md": "v2.0 implementation active",
            ROOT / "docs" / "10-implementation" / "versions" / "v1.0-remediation" / "phase-plans" / "Post_P4_Remediation_Implementation_Plan.md": "archived accepted historical remediation record",
        }
        missing: list[str] = []
        for path, needle in expected.items():
            text = path.read_text(encoding="utf-8")
            if needle not in text:
                missing.append(f"{path}:{needle}")
            if path != ROOT / "docs" / "10-implementation" / "versions" / "v1.0-remediation" / "phase-plans" / "Post_P4_Remediation_Implementation_Plan.md" and "v2.0 planning" in text:
                missing.append(f"{path}:v2.0 planning")
            if "ready for unattended live" in text and "不表示 `ready for unattended live`" not in text:
                missing.append(f"{path}:ready for unattended live")
        self.assertEqual(missing, [])

    def test_p4_docs_reference_controlled_live_smoke_runbook(self) -> None:
        expected_refs = {
            ROOT / "README.md": ["P4_Controlled_Live_Smoke_Runbook.md"],
            ROOT / "docs" / "10-implementation" / "Implementation_Index.md": ["P4_Controlled_Live_Smoke_Runbook.md"],
            ROOT / "docs" / "00-overview" / "Documentation_Index.md": ["P4_Controlled_Live_Smoke_Runbook.md"],
        }
        missing_refs: list[str] = []
        for path, refs in expected_refs.items():
            text = path.read_text(encoding="utf-8")
            for ref in refs:
                if ref not in text:
                    missing_refs.append(f"{path}:{ref}")
        self.assertEqual(missing_refs, [])


if __name__ == "__main__":
    unittest.main()
