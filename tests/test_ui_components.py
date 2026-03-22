from __future__ import annotations

import unittest
from unittest.mock import patch

from ui import components


class UiComponentsMarkupTest(unittest.TestCase):
    def test_html_helpers_emit_unindented_markup_with_unsafe_allow_html(self) -> None:
        captured: list[tuple[str, bool]] = []

        def _capture(body: str, *, unsafe_allow_html: bool = False, **_: object) -> None:
            captured.append((body, unsafe_allow_html))

        with patch("ui.components.st.markdown", side_effect=_capture):
            components.render_page_intro("Title", "Summary", kicker="Kicker")
            components.render_section_header("Section", subtitle="Subtitle")
            components.render_state_card("State", "Body")
            components.render_detail_key_value([("Question", "Value")])
            components.render_empty_state("Empty", "Body")

        self.assertTrue(captured)
        for body, unsafe in captured:
            self.assertTrue(unsafe)
            self.assertFalse(body.startswith("\n"))
            self.assertTrue(body.startswith("<"))


if __name__ == "__main__":
    unittest.main()
