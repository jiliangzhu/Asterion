from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class Phase9WordingBaselineTest(unittest.TestCase):
    def test_ui_surfaces_share_operator_boundary_baseline(self) -> None:
        ui_expectations = {
            ROOT / "ui" / "app.py": [
                "Operator Console for Constrained Execution",
                "Post-P4 remediation active",
                "Closeout pending objective verification",
                "Exception Review",
                "Readiness Evidence",
                "`manual-only`",
                "`default-off`",
                "`constrained real submit`",
                "`not unattended live`",
            ],
            ROOT / "ui" / "pages" / "home.py": [
                "Decision Console",
                "operator console + constrained execution infra",
                "exception-review evidence",
            ],
            ROOT / "ui" / "pages" / "system.py": [
                "Readiness Evidence",
                "constrained execution boundary",
                "unrestricted live",
            ],
            ROOT / "ui" / "pages" / "agents.py": [
                "### Exception Review",
                "exception review / human queue",
                "review activity",
            ],
            ROOT / "ui" / "pages" / "execution.py": [
                "execution science",
                "execution-path evidence",
                "execution / live-prereq exceptions",
            ],
            ROOT / "ui" / "pages" / "markets.py": [
                "constrained execution boundary",
                "executed evidence 与 research decomposition",
            ],
        }
        missing: list[str] = []
        for path, needles in ui_expectations.items():
            text = path.read_text(encoding="utf-8")
            for needle in needles:
                if needle not in text:
                    missing.append(f"{path}:{needle}")
        self.assertEqual(missing, [])

    def test_ui_surfaces_drop_stale_phase4_wording(self) -> None:
        forbidden = {
            ROOT / "ui" / "app.py": [
                "P4 scaffold landed",
                "Agent Workbench",
            ],
            ROOT / "ui" / "pages" / "execution.py": [
                "executed-only predicted-vs-realized",
            ],
        }
        hits: list[str] = []
        for path, needles in forbidden.items():
            text = path.read_text(encoding="utf-8")
            for needle in needles:
                if needle in text:
                    hits.append(f"{path}:{needle}")
        self.assertEqual(hits, [])

    def test_entry_docs_share_current_truth_source(self) -> None:
        doc_expectations = {
            ROOT / "README.md": [
                "post-P4 remediation active (`Phase 0` / `Phase 9` accepted; closeout pending objective verification)",
                "operator console + constrained execution infra",
                "不表示 unattended live",
            ],
            ROOT / "AGENTS.md": [
                "post-P4 remediation active",
                "closeout pending objective verification",
                "operator console + constrained execution infra",
            ],
            ROOT / "docs" / "00-overview" / "Asterion_Project_Plan.md": [
                "operator console + constrained execution infra",
                "Post_P4_Remediation_Implementation_Plan.md",
            ],
            ROOT / "docs" / "00-overview" / "DEVELOPMENT_ROADMAP.md": [
                "Phase 5` 到 `Phase 8` 已作为 post-P4 remediation 的连续收口阶段落地；当前 `Phase 9` 只负责 operator wording / docs truth-source cleanup",
            ],
            ROOT / "docs" / "00-overview" / "Documentation_Index.md": [
                "当前状态与 operator 边界的 truth-source",
            ],
            ROOT / "docs" / "10-implementation" / "Implementation_Index.md": [
                "accepted `Phase 0` 到 `Phase 9`",
            ],
            ROOT / "docs" / "10-implementation" / "phase-plans" / "Post_P4_Remediation_Implementation_Plan.md": [
                "### 10.7 Phase 9: Operator Surface and Truth-Source Cleanup",
                "accepted (`2026-03-16`)",
                "operator console + constrained execution infra",
            ],
        }
        missing: list[str] = []
        for path, needles in doc_expectations.items():
            text = path.read_text(encoding="utf-8")
            for needle in needles:
                if needle not in text:
                    missing.append(f"{path}:{needle}")
        self.assertEqual(missing, [])


if __name__ == "__main__":
    unittest.main()
