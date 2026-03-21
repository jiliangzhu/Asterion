from __future__ import annotations

import unittest
from datetime import UTC, date, datetime

from asterion_core.contracts import (
    ForecastReplayDiffRecord,
    ForecastReplayRecord,
    ForecastRunRecord,
    WeatherFairValueRecord,
    WeatherMarketSpecRecord,
    WatchOnlySnapshotRecord,
)
from domains.weather.forecast import validate_replay_quality


def _spec() -> WeatherMarketSpecRecord:
    return WeatherMarketSpecRecord(
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        location_name="Seattle",
        station_id="KSEA",
        latitude=47.45,
        longitude=-122.31,
        timezone="America/Los_Angeles",
        observation_date=date(2026, 3, 8),
        observation_window_local="daily_max",
        metric="temperature_max",
        unit="fahrenheit",
        bucket_min_value=50.0,
        bucket_max_value=59.0,
        authoritative_source="weather.com",
        fallback_sources=["nws"],
        rounding_rule="identity",
        inclusive_bounds=True,
        spec_version="spec_v1",
        parse_confidence=0.95,
        risk_flags=[],
    )


def _run(*, run_id: str, fallback_used: bool = False, timezone: str = "America/Los_Angeles") -> ForecastRunRecord:
    return ForecastRunRecord(
        run_id=run_id,
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        station_id="KSEA",
        source="nws",
        model_run="nws_gfs",
        forecast_target_time=datetime(2026, 3, 8, 12, 0, tzinfo=UTC),
        observation_date=date(2026, 3, 8),
        metric="temperature_max",
        latitude=47.45,
        longitude=-122.31,
        timezone=timezone,
        spec_version="spec_v1",
        cache_key="cache_key_1",
        source_trace=["nws"],
        fallback_used=fallback_used,
        from_cache=False,
        confidence=0.91,
        forecast_payload={"max_temp_f": 55.0},
        raw_payload={"source": "nws"},
    )


def _replay() -> ForecastReplayRecord:
    return ForecastReplayRecord(
        replay_id="replay_1",
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        station_id="KSEA",
        source="nws",
        model_run="nws_gfs",
        forecast_target_time=datetime(2026, 3, 8, 12, 0, tzinfo=UTC),
        spec_version="spec_v1",
        replay_key="replay_key_1",
        replay_reason="operator_audit",
        original_run_id="run_original",
        replayed_run_id="run_replayed",
        created_at=datetime(2026, 3, 10, 0, 0, tzinfo=UTC),
    )


def _fair_value() -> WeatherFairValueRecord:
    return WeatherFairValueRecord(
        fair_value_id="fv_1",
        run_id="run_replayed",
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        token_id="token_yes",
        outcome="YES",
        fair_value=0.61,
        confidence=0.84,
    )


def _snapshot() -> WatchOnlySnapshotRecord:
    return WatchOnlySnapshotRecord(
        snapshot_id="snap_1",
        fair_value_id="fv_1",
        run_id="run_replayed",
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        token_id="token_yes",
        outcome="YES",
        reference_price=0.53,
        fair_value=0.61,
        edge_bps=800,
        threshold_bps=500,
        decision="BUY",
        side="BUY",
        rationale="positive edge",
        pricing_context={"source": "nws"},
    )


class ReplayQualityValidationTest(unittest.TestCase):
    def test_validation_passes_for_canonical_replay(self) -> None:
        result = validate_replay_quality(
            _replay(),
            spec=_spec(),
            original_run=_run(run_id="run_original"),
            replayed_run=_run(run_id="run_replayed"),
            replay_diffs=[
                ForecastReplayDiffRecord(
                    diff_id="diff_1",
                    replay_id="replay_1",
                    entity_type="forecast_run",
                    entity_key="run_replayed",
                    original_entity_id="run_original",
                    replayed_entity_id="run_replayed",
                    status="MATCH",
                    diff_summary_json={},
                    created_at=datetime(2026, 3, 10, 0, 5, tzinfo=UTC),
                )
            ],
            fair_values=[_fair_value()],
            watch_snapshots=[_snapshot()],
        )
        self.assertEqual(result.verdict, "pass")
        self.assertFalse(result.human_review_required)

    def test_validation_requires_review_for_timezone_drift_and_fallback(self) -> None:
        result = validate_replay_quality(
            _replay(),
            spec=_spec(),
            original_run=_run(run_id="run_original"),
            replayed_run=_run(run_id="run_replayed", fallback_used=True, timezone="UTC"),
            replay_diffs=[
                ForecastReplayDiffRecord(
                    diff_id="diff_2",
                    replay_id="replay_1",
                    entity_type="forecast_run",
                    entity_key="run_replayed",
                    original_entity_id="run_original",
                    replayed_entity_id="run_replayed",
                    status="DIFFERENT",
                    diff_summary_json={"field": "timezone"},
                    created_at=datetime(2026, 3, 10, 0, 5, tzinfo=UTC),
                )
            ],
            fair_values=[_fair_value()],
            watch_snapshots=[_snapshot()],
        )
        self.assertEqual(result.verdict, "review")
        self.assertTrue(result.human_review_required)
        self.assertTrue(any("timezone mismatch" in item or "fallback" in item for item in result.findings))

    def test_validation_blocks_when_canonical_inputs_are_missing(self) -> None:
        result = validate_replay_quality(
            _replay(),
            spec=None,
            original_run=None,
            replayed_run=None,
            replay_diffs=[],
            fair_values=[],
            watch_snapshots=[],
        )
        self.assertEqual(result.verdict, "block")
        self.assertTrue(result.human_review_required)
        self.assertIn("market spec missing for replay", result.findings)


if __name__ == "__main__":
    unittest.main()
