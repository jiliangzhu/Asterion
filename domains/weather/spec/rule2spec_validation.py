from __future__ import annotations

from asterion_core.contracts import Rule2SpecDraft, Rule2SpecValidationResult, StationMetadata, WeatherMarketSpecRecord


_VALID_METRICS = {
    "temperature_max",
    "temperature_min",
    "precipitation_total",
    "snowfall_total",
}
_VALID_UNITS = {"fahrenheit", "celsius", "inch", "inches", "mm"}


def validate_rule2spec_draft(
    draft: Rule2SpecDraft,
    *,
    current_spec: WeatherMarketSpecRecord | None,
    station_metadata: StationMetadata | None,
) -> Rule2SpecValidationResult:
    risk_flags = list(draft.risk_flags)
    violations: list[str] = []

    if station_metadata is None:
        risk_flags.append("missing_station_metadata")
        violations.append("station metadata missing for parsed location")
    else:
        if station_metadata.location_name and station_metadata.location_name != draft.location_name:
            risk_flags.append("station_location_mismatch")
            violations.append("station metadata location_name does not match parsed draft")
        if not station_metadata.timezone:
            risk_flags.append("station_timezone_missing")
            violations.append("station metadata timezone missing")

    if draft.metric not in _VALID_METRICS:
        risk_flags.append("invalid_metric")
        violations.append(f"metric {draft.metric!r} is not supported")
    if draft.unit not in _VALID_UNITS:
        risk_flags.append("invalid_unit")
        violations.append(f"unit {draft.unit!r} is not supported")
    if (
        draft.bucket_min_value is not None
        and draft.bucket_max_value is not None
        and float(draft.bucket_min_value) > float(draft.bucket_max_value)
    ):
        risk_flags.append("invalid_threshold_range")
        violations.append("bucket_min_value exceeds bucket_max_value")
    if float(draft.parse_confidence) < 0.8:
        risk_flags.append("low_parse_confidence")
    if float(draft.parse_confidence) < 0.5:
        violations.append("parse_confidence below block threshold")

    if current_spec is not None:
        if current_spec.station_id and station_metadata is not None and current_spec.station_id != station_metadata.station_id:
            violations.append("current spec station_id differs from current station mapping")
        if current_spec.metric != draft.metric:
            violations.append(f"metric drift detected: {current_spec.metric} -> {draft.metric}")
        if current_spec.unit != draft.unit:
            violations.append(f"unit drift detected: {current_spec.unit} -> {draft.unit}")
        if current_spec.authoritative_source != draft.authoritative_source:
            violations.append(
                f"authoritative_source drift detected: {current_spec.authoritative_source} -> {draft.authoritative_source}"
            )
        if (
            current_spec.bucket_min_value != draft.bucket_min_value
            or current_spec.bucket_max_value != draft.bucket_max_value
        ):
            violations.append("threshold bucket differs from current spec")

    if float(draft.parse_confidence) < 0.5:
        verdict = "block"
    elif violations or "missing_station_metadata" in risk_flags:
        verdict = "review"
    else:
        verdict = "pass"

    if verdict == "pass":
        summary = "deterministic rule2spec validation passed"
    elif verdict == "block":
        summary = "deterministic rule2spec validation blocked the parsed draft"
    else:
        summary = "deterministic rule2spec validation requires operator review"
    return Rule2SpecValidationResult(
        verdict=verdict,
        risk_flags=sorted(set(risk_flags)),
        violations=violations,
        human_review_required=verdict != "pass",
        summary=summary,
    )
