from __future__ import annotations

import unittest
from pathlib import Path

import pandas as pd

from ui.components import _safe_text


ROOT = Path(__file__).resolve().parents[1]


class UiComponentsContractTest(unittest.TestCase):
    def test_components_use_native_streamlit_rendering(self) -> None:
        text = (ROOT / "ui" / "components.py").read_text(encoding="utf-8")
        self.assertIn("def render_page_intro(", text)
        self.assertIn("def render_section_header(", text)
        self.assertIn("def render_detail_key_value(", text)
        self.assertIn("with st.container(border=True)", text)
        self.assertNotIn("unsafe_allow_html=True", text)

    def test_safe_text_handles_pandas_na(self) -> None:
        self.assertEqual(_safe_text(pd.NA), "N/A")
        self.assertEqual(_safe_text(None), "N/A")
        self.assertEqual(_safe_text(""), "N/A")


if __name__ == "__main__":
    unittest.main()
