from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class StreamlitShellConfigTest(unittest.TestCase):
    def test_default_streamlit_sidebar_navigation_is_disabled(self) -> None:
        text = (ROOT / ".streamlit" / "config.toml").read_text(encoding="utf-8")
        self.assertIn("[client]", text)
        self.assertIn("showSidebarNavigation = false", text)


if __name__ == "__main__":
    unittest.main()
