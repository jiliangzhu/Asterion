from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class P3CloseoutDocsTest(unittest.TestCase):
    def test_p3_closeout_targets_exist(self) -> None:
        expected = [
            ROOT / "docs" / "10-implementation" / "checklists" / "P3_Closeout_Checklist.md",
            ROOT / "docs" / "10-implementation" / "runbooks" / "P3_Paper_Execution_Runbook.md",
            ROOT / "docs" / "10-implementation" / "phase-plans" / "P3_Implementation_Plan.md",
            ROOT / "asterion_core" / "monitoring" / "readiness_checker_v1.py",
            ROOT / "asterion_core" / "ui" / "ui_lite_db.py",
        ]
        missing = [str(path) for path in expected if not path.exists()]
        self.assertEqual(missing, [])

    def test_navigation_docs_reference_p3_closeout_materials(self) -> None:
        expected_refs = {
            ROOT / "README.md": ["P3_Closeout_Checklist.md", "P3_Paper_Execution_Runbook.md"],
            ROOT / "docs" / "10-implementation" / "Implementation_Index.md": ["P3_Closeout_Checklist.md", "P3_Paper_Execution_Runbook.md"],
            ROOT / "docs" / "00-overview" / "Documentation_Index.md": ["P3_Closeout_Checklist.md", "P3_Paper_Execution_Runbook.md"],
            ROOT / "docs" / "00-overview" / "DEVELOPMENT_ROADMAP.md": ["P3_Closeout_Checklist.md", "P3_Paper_Execution_Runbook.md"],
        }
        missing_refs: list[str] = []
        for path, refs in expected_refs.items():
            text = path.read_text(encoding="utf-8")
            for ref in refs:
                if ref not in text:
                    missing_refs.append(f"{path}:{ref}")
        self.assertEqual(missing_refs, [])

    def test_p3_closeout_docs_keep_p4_planning_only_boundary(self) -> None:
        checklist = (ROOT / "docs" / "10-implementation" / "checklists" / "P3_Closeout_Checklist.md").read_text(encoding="utf-8")
        runbook = (ROOT / "docs" / "10-implementation" / "runbooks" / "P3_Paper_Execution_Runbook.md").read_text(encoding="utf-8")
        self.assertIn("ready for P4 planning only", checklist)
        self.assertIn("ready for P4 planning only", runbook)
        self.assertIn("不表示可进入 live", checklist)
        self.assertIn("不表示可广播真实链上交易", runbook)


if __name__ == "__main__":
    unittest.main()
