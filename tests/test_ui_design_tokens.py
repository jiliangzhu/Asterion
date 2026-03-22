from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class UiDesignTokensTest(unittest.TestCase):
    def test_app_css_tokens_and_fonts_are_pinned(self) -> None:
        text = (ROOT / "ui" / "app.py").read_text(encoding="utf-8")
        for needle in [
            "@import url('https://fonts.googleapis.com/css2?family=Geist",
            "IBM+Plex+Mono",
            "--panel-strong",
            "--shadow",
            ".ui-page-intro",
            ".ui-kv-grid",
            ".ui-reason-chip",
            ".console-shell",
            "font-variant-numeric: tabular-nums",
        ]:
            self.assertIn(needle, text)

    def test_shared_components_contract_is_present(self) -> None:
        text = (ROOT / "ui" / "components.py").read_text(encoding="utf-8")
        for needle in [
            "def render_page_intro(",
            "def render_section_header(",
            "def render_kpi_band(",
            "def render_state_card(",
            "def render_detail_key_value(",
            "def render_empty_state(",
            "def render_delivery_badge(",
            "def render_reason_chip_row(",
        ]:
            self.assertIn(needle, text)


if __name__ == "__main__":
    unittest.main()
