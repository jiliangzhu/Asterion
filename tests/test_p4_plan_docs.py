from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class P4PlanDocsTest(unittest.TestCase):
    def test_p4_plan_targets_exist(self) -> None:
        expected = [
            ROOT / "docs" / "10-implementation" / "phase-plans" / "P4_Implementation_Plan.md",
            ROOT / "docs" / "00-overview" / "DEVELOPMENT_ROADMAP.md",
            ROOT / "docs" / "10-implementation" / "Implementation_Index.md",
            ROOT / "docs" / "00-overview" / "Documentation_Index.md",
            ROOT / "README.md",
        ]
        missing = [str(path) for path in expected if not path.exists()]
        self.assertEqual(missing, [])

    def test_navigation_docs_reference_p4_plan(self) -> None:
        expected_refs = {
            ROOT / "README.md": ["P4_Implementation_Plan.md"],
            ROOT / "docs" / "10-implementation" / "Implementation_Index.md": ["P4_Implementation_Plan.md"],
            ROOT / "docs" / "00-overview" / "Documentation_Index.md": ["P4_Implementation_Plan.md"],
            ROOT / "docs" / "00-overview" / "DEVELOPMENT_ROADMAP.md": ["P4_Implementation_Plan.md"],
            ROOT / "docs" / "00-overview" / "Asterion_Project_Plan.md": ["P4_Implementation_Plan.md"],
        }
        missing_refs: list[str] = []
        for path, refs in expected_refs.items():
            text = path.read_text(encoding="utf-8")
            for ref in refs:
                if ref not in text:
                    missing_refs.append(f"{path}:{ref}")
        self.assertEqual(missing_refs, [])

    def test_p4_plan_keeps_live_prereq_boundary(self) -> None:
        plan = (ROOT / "docs" / "10-implementation" / "phase-plans" / "P4_Implementation_Plan.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("P4` 不是 production live rollout", plan)
        self.assertIn("default-off + explicit operator approval + auditable", plan)
        self.assertIn("ready for controlled live rollout decision", plan)


if __name__ == "__main__":
    unittest.main()
