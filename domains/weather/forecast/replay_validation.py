from __future__ import annotations

from asterion_core.contracts import (
    ForecastReplayDiffRecord,
    ForecastReplayRecord,
    ForecastRunRecord,
    ReplayQualityValidationResult,
    WeatherFairValueRecord,
    WeatherMarketSpecRecord,
    WatchOnlySnapshotRecord,
)


def validate_replay_quality(
    replay: ForecastReplayRecord,
    *,
    spec: WeatherMarketSpecRecord | None,
    original_run: ForecastRunRecord | None,
    replayed_run: ForecastRunRecord | None,
    replay_diffs: list[ForecastReplayDiffRecord],
    fair_values: list[WeatherFairValueRecord],
    watch_snapshots: list[WatchOnlySnapshotRecord],
) -> ReplayQualityValidationResult:
    findings: list[str] = []

    if spec is None:
        findings.append("market spec missing for replay")
    if original_run is None:
        findings.append("original forecast run missing")
    if replayed_run is None:
        findings.append("replayed forecast run missing")

    if spec is not None and replayed_run is not None:
        if spec.station_id != replayed_run.station_id:
            findings.append("station_id mismatch between spec and replayed run")
        if spec.metric != replayed_run.metric:
            findings.append("metric mismatch between spec and replayed run")
        if spec.timezone != replayed_run.timezone:
            findings.append("timezone mismatch between spec and replayed run")
        if spec.unit not in {"fahrenheit", "celsius", "inch", "inches", "mm"}:
            findings.append("unsupported unit in market spec")

    if original_run is not None and replayed_run is not None:
        if original_run.station_id != replayed_run.station_id:
            findings.append("station_id drift between original and replayed runs")
        if original_run.timezone != replayed_run.timezone:
            findings.append("timezone drift between original and replayed runs")
        if replayed_run.fallback_used:
            findings.append("replayed run used fallback source")

    non_match_diffs = [diff for diff in replay_diffs if str(diff.status) != "MATCH"]
    if non_match_diffs:
        findings.append(f"{len(non_match_diffs)} replay diff rows are non-matching")
    if not fair_values:
        findings.append("fair value rows missing for replay")
    if not watch_snapshots:
        findings.append("watch-only snapshot rows missing for replay")
    if replay.source != (replayed_run.source if replayed_run is not None else replay.source):
        findings.append("replay source does not match replayed run source")

    if spec is None or original_run is None or replayed_run is None:
        verdict = "block"
    elif non_match_diffs or any("mismatch" in item or "fallback" in item for item in findings):
        verdict = "review"
    else:
        verdict = "pass"

    if verdict == "pass":
        summary = "deterministic replay quality validation passed"
    elif verdict == "block":
        summary = "deterministic replay quality validation blocked due to missing canonical facts"
    else:
        summary = "deterministic replay quality validation requires operator review"
    return ReplayQualityValidationResult(
        verdict=verdict,
        findings=findings,
        human_review_required=verdict != "pass",
        summary=summary,
    )
