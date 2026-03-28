from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class P10CloseoutDocsTest(unittest.TestCase):
    def test_p10_and_entry_docs_reflect_accepted_reality(self) -> None:
        expectations = {
            ROOT / "docs" / "10-implementation" / "versions" / "v2.0" / "phase-plans" / "P10_Implementation_Plan.md": [
                "accepted closeout record",
                "Phase 0` 到 `Phase 10` 已 accepted",
                "没有发现新的功能性疏漏",
                "runtime.execution_intelligence_summaries",
                "tests.test_execution_intelligence_summary",
                "tests.test_allocator_v1_p10_scheduling",
            ],
            ROOT / "docs" / "10-implementation" / "versions" / "v2.0" / "phase-plans" / "V2_Implementation_Plan.md": [
                "Phase 10. Deterministic ROI Repair and Execution Intelligence Foundation",
                "accepted closeout record",
                "P11_Closeout_Checklist.md",
                "当前还没有比 `Phase 11` 更新的 tranche-specific implementation plan 已打开",
            ],
            ROOT / "README.md": [
                "最近 accepted tranche: `Phase 11`",
                "P10_Closeout_Checklist.md",
            ],
            ROOT / "docs" / "10-implementation" / "versions" / "v2.0" / "checklists" / "P10_Closeout_Checklist.md": [
                "accepted closeout checklist",
                "tests.test_home_action_queue_excludes_blocked_items",
                "tests.test_execution_intelligence_summary",
            ],
            ROOT / "docs" / "10-implementation" / "Implementation_Index.md": [
                "最近 accepted tranche record",
                "P10_Closeout_Checklist.md",
            ],
        }
        missing: list[str] = []
        for path, needles in expectations.items():
            text = path.read_text(encoding="utf-8")
            for needle in needles:
                if needle not in text:
                    missing.append(f"{path}:{needle}")
        self.assertEqual(missing, [])


if __name__ == "__main__":
    unittest.main()
