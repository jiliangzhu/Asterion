from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class P4CloseoutDocsTest(unittest.TestCase):
    def test_p4_closeout_targets_exist(self) -> None:
        expected = [
            ROOT / "docs" / "10-implementation" / "versions" / "v1.0" / "checklists" / "P4_Closeout_Checklist.md",
            ROOT / "docs" / "10-implementation" / "versions" / "v1.0" / "runbooks" / "P4_Controlled_Rollout_Decision_Runbook.md",
            ROOT / "docs" / "10-implementation" / "versions" / "v1.0" / "runbooks" / "P4_Controlled_Live_Smoke_Runbook.md",
            ROOT / "docs" / "10-implementation" / "versions" / "v1.0" / "phase-plans" / "P4_Implementation_Plan.md",
            ROOT / "asterion_core" / "monitoring" / "readiness_checker_v1.py",
            ROOT / "asterion_core" / "ui" / "ui_lite_db.py",
        ]
        missing = [str(path) for path in expected if not path.exists()]
        self.assertEqual(missing, [])

    def test_navigation_docs_reference_archived_p4_closeout_materials(self) -> None:
        expected_refs = {
            ROOT / "README.md": ["P4_Closeout_Checklist.md", "P4_Controlled_Rollout_Decision_Runbook.md"],
            ROOT / "docs" / "10-implementation" / "Implementation_Index.md": [
                "P4_Closeout_Checklist.md",
                "P4_Controlled_Rollout_Decision_Runbook.md",
            ],
            ROOT / "docs" / "00-overview" / "Documentation_Index.md": [
                "P4_Closeout_Checklist.md",
                "P4_Controlled_Rollout_Decision_Runbook.md",
            ],
            ROOT / "docs" / "00-overview" / "versions" / "v2.0" / "DEVELOPMENT_ROADMAP.md": [
                "P4_Closeout_Checklist.md",
                "P4_Controlled_Rollout_Decision_Runbook.md",
            ],
        }
        missing_refs: list[str] = []
        for path, refs in expected_refs.items():
            text = path.read_text(encoding="utf-8")
            for ref in refs:
                if ref not in text:
                    missing_refs.append(f"{path}:{ref}")
        self.assertEqual(missing_refs, [])

    def test_p4_closeout_docs_keep_controlled_live_boundary(self) -> None:
        checklist = (
            ROOT / "docs" / "10-implementation" / "versions" / "v1.0" / "checklists" / "P4_Closeout_Checklist.md"
        ).read_text(encoding="utf-8")
        runbook = (
            ROOT / "docs" / "10-implementation" / "versions" / "v1.0" / "runbooks" / "P4_Controlled_Rollout_Decision_Runbook.md"
        ).read_text(encoding="utf-8")
        self.assertIn("ready for controlled live rollout decision", checklist)
        self.assertIn("ready for controlled live rollout decision", runbook)
        self.assertIn("不表示 `ready for unattended live`", checklist)
        self.assertIn("不等于 production live", runbook)
        self.assertIn("weather_controlled_live_smoke", checklist)
        self.assertIn("approve_usdc", checklist)

    def test_p4_closeout_docs_are_archived_historical_records(self) -> None:
        checklist = (
            ROOT / "docs" / "10-implementation" / "versions" / "v1.0" / "checklists" / "P4_Closeout_Checklist.md"
        ).read_text(encoding="utf-8")
        runbook = (
            ROOT / "docs" / "10-implementation" / "versions" / "v1.0" / "runbooks" / "P4_Controlled_Rollout_Decision_Runbook.md"
        ).read_text(encoding="utf-8")
        self.assertIn("archived accepted historical closeout record", checklist)
        self.assertIn("historical closeout record", checklist)
        self.assertIn("archived accepted historical runbook", runbook)


if __name__ == "__main__":
    unittest.main()
