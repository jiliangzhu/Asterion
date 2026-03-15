from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from domains.weather.opportunity import build_market_quality_assessment, build_source_health_snapshot


class MarketQualityScreenTest(unittest.TestCase):
    def test_low_mapping_confidence_requires_review(self) -> None:
        assessment = build_market_quality_assessment(
            market_id="mkt_1",
            accepting_orders=True,
            enable_order_book=True,
            reference_price=0.45,
            mapping_confidence=0.60,
            price_staleness_ms=60_000,
            source_freshness_status="fresh",
            depth_proxy=0.85,
        )
        self.assertEqual(assessment.market_quality_status, "review_required")
        self.assertIn("mapping_confidence_review", assessment.market_quality_reason_codes)

    def test_stale_price_can_block_market(self) -> None:
        assessment = build_market_quality_assessment(
            market_id="mkt_2",
            accepting_orders=True,
            enable_order_book=True,
            reference_price=0.45,
            mapping_confidence=0.95,
            price_staleness_ms=3_700_000,
            source_freshness_status="fresh",
            depth_proxy=0.85,
        )
        self.assertEqual(assessment.market_quality_status, "blocked")
        self.assertIn("price_staleness_blocked", assessment.market_quality_reason_codes)

    def test_source_health_snapshot_marks_degraded_when_forecast_is_old(self) -> None:
        now = datetime(2026, 3, 15, 12, 0, tzinfo=UTC)
        snapshot = build_source_health_snapshot(
            market_id="mkt_3",
            station_id="KSEA",
            source="openmeteo",
            market_updated_at=now - timedelta(minutes=5),
            forecast_created_at=now - timedelta(hours=3),
            snapshot_created_at=now - timedelta(minutes=1),
            now=now,
        )
        self.assertEqual(snapshot.source_freshness_status, "degraded")
        self.assertIn("forecast_degraded", snapshot.degraded_reason_codes)


if __name__ == "__main__":
    unittest.main()
