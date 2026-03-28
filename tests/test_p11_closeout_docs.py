from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class P11CloseoutDocsTest(unittest.TestCase):
    def test_p11_and_entry_docs_reflect_accepted_reality(self) -> None:
        expectations = {
            ROOT / "docs" / "10-implementation" / "versions" / "v2.0" / "phase-plans" / "P11_Implementation_Plan.md": [
                "accepted closeout record",
                "Accepted Closeout Summary",
                "accepted closeout baseline",
                "tests.test_opportunity_triage_replay_evaluation",
                "tests.test_opportunity_triage_timeout_isolation",
                "P11_Closeout_Checklist.md",
            ],
            ROOT / "docs" / "10-implementation" / "versions" / "v2.0" / "phase-plans" / "V2_Implementation_Plan.md": [
                "Phase 11. Opportunity Triage / Execution Intelligence Agent",
                "accepted closeout record",
                "P11_Closeout_Checklist.md",
                "当前还没有比 `Phase 11` 更新的 tranche-specific implementation plan 已打开",
            ],
            ROOT / "README.md": [
                "P11_Closeout_Checklist.md",
                "最近 accepted tranche: `Phase 11`",
                "当前没有比 `Phase 11` 更新的 tranche-specific implementation plan 已打开",
            ],
            ROOT / "docs" / "10-implementation" / "versions" / "v2.0" / "checklists" / "P11_Closeout_Checklist.md": [
                "accepted closeout checklist",
                "tests.test_opportunity_triage_replay_evaluation",
                "tests.test_p11_system_runtime_summary",
            ],
            ROOT / "docs" / "10-implementation" / "Implementation_Index.md": [
                "P11_Closeout_Checklist.md",
                "最近 accepted tranche 的 closeout checklist",
            ],
            ROOT / "docs" / "10-implementation" / "checklists" / "Checklist_Index.md": [
                "P11_Closeout_Checklist.md",
                "最近 accepted tranche 的 closeout checklist",
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
