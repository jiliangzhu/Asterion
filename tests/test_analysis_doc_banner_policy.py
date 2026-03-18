from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class AnalysisDocBannerPolicyTest(unittest.TestCase):
    def test_all_analysis_docs_include_truth_source_banner(self) -> None:
        analysis_dir = ROOT / "docs" / "analysis"
        missing: list[str] = []
        for path in sorted(analysis_dir.glob("*.md")):
            text = path.read_text(encoding="utf-8")
            if "Analysis input only." not in text:
                missing.append(f"{path}:missing-analysis-banner")
            if "Not implementation truth-source." not in text:
                missing.append(f"{path}:missing-truth-source-warning")
            if "V2_Implementation_Plan.md" not in text:
                missing.append(f"{path}:missing-active-entry")
        self.assertEqual(missing, [])


if __name__ == "__main__":
    unittest.main()
