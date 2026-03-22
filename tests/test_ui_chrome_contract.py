from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class UiChromeContractTest(unittest.TestCase):
    def test_app_shell_copy_and_navigation_baseline(self) -> None:
        text = (ROOT / "ui" / "app.py").read_text(encoding="utf-8")
        self.assertIn("Operator Console for Constrained Execution", text)
        self.assertIn("research console", text)
        self.assertIn("页面导航", text)
        self.assertIn('label_visibility="collapsed"', text)
        self.assertNotIn("当前侧栏是 research desk 的 command rail", text)
        self.assertIn("Home", text)
        self.assertIn("Markets", text)
        self.assertIn("Execution", text)
        self.assertIn("Agents", text)
        self.assertIn("System", text)
        self.assertNotIn("Asterion v2.0 / Phase 9 in progress", text)
        self.assertIn("section[data-testid=\"stSidebar\"] [data-testid=\"stMarkdownContainer\"] code", text)


if __name__ == "__main__":
    unittest.main()
