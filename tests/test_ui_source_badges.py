from __future__ import annotations

import unittest

import pandas as pd

from ui.surface_truth import annotate_frame_with_source_truth, build_opportunity_row_source_badge


class UiSourceBadgesTest(unittest.TestCase):
    def test_badge_family_covers_canonical_fallback_stale_degraded_and_derived(self) -> None:
        self.assertEqual(
            build_opportunity_row_source_badge(source_origin="ui_lite", source_freshness_status="fresh").source_badge,
            "canonical",
        )
        self.assertEqual(
            build_opportunity_row_source_badge(source_origin="smoke_report", source_freshness_status="fresh").source_badge,
            "fallback",
        )
        self.assertEqual(
            build_opportunity_row_source_badge(source_origin="ui_lite", source_freshness_status="stale").source_badge,
            "stale",
        )
        self.assertEqual(
            build_opportunity_row_source_badge(source_origin="ui_lite", source_freshness_status="degraded").source_badge,
            "degraded",
        )
        self.assertEqual(
            build_opportunity_row_source_badge(source_origin="ui_lite", derived=True).source_badge,
            "derived",
        )

    def test_annotate_frame_with_source_truth_adds_primary_score_and_badges(self) -> None:
        frame = pd.DataFrame(
            [
                {"market_id": "mkt_1", "ranking_score": 88.0, "source_freshness_status": "fresh"},
                {"market_id": "mkt_2", "ranking_score": 55.0, "source_freshness_status": "stale"},
            ]
        )
        annotated = annotate_frame_with_source_truth(
            frame,
            source_origin="ui_lite",
            derived=False,
            freshness_column="source_freshness_status",
        )
        self.assertEqual(annotated.iloc[0]["source_badge"], "canonical")
        self.assertEqual(annotated.iloc[1]["source_badge"], "stale")
        self.assertTrue(bool(annotated.iloc[1]["is_degraded_source"]))
        self.assertEqual(annotated.iloc[0]["primary_score_label"], "ranking_score")


if __name__ == "__main__":
    unittest.main()
