from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class SidebarBoundarySummaryTest(unittest.TestCase):
    def test_app_uses_dynamic_boundary_sidebar_summary(self) -> None:
        text = (ROOT / "ui" / "app.py").read_text(encoding="utf-8")
        self.assertIn("load_boundary_sidebar_truth", text)
        self.assertNotIn('st.sidebar.markdown("- `manual-only`")', text)
        self.assertNotIn('st.sidebar.markdown("- `default-off`")', text)
        self.assertNotIn('st.sidebar.markdown("- `approve_usdc only`")', text)
        self.assertNotIn('st.sidebar.markdown("- `constrained real submit`")', text)
        self.assertNotIn('st.sidebar.markdown("- `not unattended live`")', text)


if __name__ == "__main__":
    unittest.main()
