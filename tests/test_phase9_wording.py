from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class Phase9WordingBaselineTest(unittest.TestCase):
    def test_ui_surfaces_share_operator_boundary_baseline(self) -> None:
        ui_expectations = {
            ROOT / "ui" / "app.py": [
                "Operator Console for Constrained Execution",
                "v2.0 implementation active",
                "P4/remediation accepted",
                "Exception Review",
                "Readiness Evidence",
                "load_boundary_sidebar_truth",
            ],
            ROOT / "ui" / "surface_truth.py": [
                "manual-only",
                "default-off",
                "constrained real submit",
                "not unattended live",
                "not unrestricted live",
            ],
            ROOT / "ui" / "pages" / "home.py": [
                "Decision Console",
                "operator console + constrained execution infra",
                "exception-review evidence",
                "v2.0 implementation active",
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
                "P4 accepted; post-P4 remediation accepted; v2.0 implementation active",
                "V2_Implementation_Plan.md",
                "当前唯一 active implementation entry",
                "operator console + constrained execution infra",
                "不表示 unattended live",
            ],
            ROOT / "AGENTS.md": [
                "v2.0 implementation active",
                "V2_Implementation_Plan.md",
                "active implementation entry",
                "operator console + constrained execution infra",
            ],
            ROOT / "docs" / "00-overview" / "versions" / "v2.0" / "Asterion_Project_Plan.md": [
                "operator console + constrained execution infra",
                "V2_Implementation_Plan.md",
                "active implementation entry",
                "Post_P4_Remediation_Implementation_Plan.md",
            ],
            ROOT / "docs" / "00-overview" / "versions" / "v2.0" / "DEVELOPMENT_ROADMAP.md": [
                "当前 active implementation entry",
                "V2_Implementation_Plan.md",
                "Post-P4 Phase 10 -> Post-P4 Phase 15 accepted",
            ],
            ROOT / "docs" / "00-overview" / "Documentation_Index.md": [
                "当前状态与 operator 边界的 truth-source",
                "V2_Implementation_Plan.md",
                "active implementation entry",
            ],
            ROOT / "docs" / "10-implementation" / "Implementation_Index.md": [
                "V2_Implementation_Plan.md",
                "当前唯一 active implementation entry",
                "historical accepted remediation record",
            ],
            ROOT / "docs" / "10-implementation" / "versions" / "v1.0-remediation" / "phase-plans" / "Post_P4_Remediation_Implementation_Plan.md": [
                "### 11.4 Post-P4 Phase 11: Operator Truth-Source and Surface Hardening",
                "archived accepted historical remediation record",
                "不再承担 active implementation entry 身份",
            ],
        }
        missing: list[str] = []
        for path, needles in doc_expectations.items():
            text = path.read_text(encoding="utf-8")
            for needle in needles:
                if needle not in text:
                    missing.append(f"{path}:{needle}")
            if path != ROOT / "docs" / "10-implementation" / "versions" / "v1.0-remediation" / "phase-plans" / "Post_P4_Remediation_Implementation_Plan.md" and "v2.0 planning" in text:
                missing.append(f"{path}:v2.0 planning")
        self.assertEqual(missing, [])


if __name__ == "__main__":
    unittest.main()
