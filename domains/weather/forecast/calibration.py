from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
import json
import math
from pathlib import Path
from typing import Any, Protocol

from asterion_core.contracts import CalibrationLookupKey, ForecastCalibrationSampleRecord, ForecastRunRecord, stable_object_id


def forecast_horizon_bucket(*, observation_date: date, forecast_target_time: datetime) -> str:
    horizon_days = max(0, (observation_date - forecast_target_time.date()).days)
    if horizon_days <= 1:
        return "0-1"
    if horizon_days <= 3:
        return "2-3"
    if horizon_days <= 7:
        return "4-7"
    return "8+"


def season_bucket(value: date | datetime) -> str:
    month = value.month
    if month in {12, 1, 2}:
        return "winter"
    if month in {3, 4, 5}:
        return "spring"
    if month in {6, 7, 8}:
        return "summer"
    return "autumn"


def calibration_regime_bucket(raw_mean: float) -> str:
    normalized = float(raw_mean)
    if normalized < 40.0:
        return "cold"
    if normalized < 60.0:
        return "mild"
    if normalized < 80.0:
        return "warm"
    return "hot"


def threshold_probability_bucket(probability: float) -> str:
    normalized = max(0.0, min(1.0, float(probability)))
    if normalized < 0.10:
        return "0-10"
    if normalized < 0.25:
        return "10-25"
    if normalized < 0.40:
        return "25-40"
    if normalized < 0.60:
        return "40-60"
    if normalized < 0.75:
        return "60-75"
    if normalized < 0.90:
        return "75-90"
    return "90-100"


def build_calibration_lookup_key(
    *,
    station_id: str,
    source: str,
    observation_date: date,
    forecast_target_time: datetime,
    metric: str,
) -> CalibrationLookupKey:
    return CalibrationLookupKey(
        station_id=station_id,
        source=str(source).strip().lower(),
        forecast_horizon_bucket=forecast_horizon_bucket(
            observation_date=observation_date,
            forecast_target_time=forecast_target_time,
        ),
        season_bucket=season_bucket(observation_date),
        metric=metric,
    )


def forecast_distribution_mean(distribution: dict[int, float]) -> float:
    if not distribution:
        raise ValueError("temperature distribution is required")
    total = sum(float(value) for value in distribution.values())
    if total <= 0:
        raise ValueError("temperature distribution total must be positive")
    return sum(float(temp) * float(prob) for temp, prob in distribution.items()) / total


def forecast_distribution_std_dev(distribution: dict[int, float]) -> float:
    mean = forecast_distribution_mean(distribution)
    total = sum(float(value) for value in distribution.values())
    variance = sum(((float(temp) - mean) ** 2) * float(prob) for temp, prob in distribution.items()) / total
    return math.sqrt(max(0.0, variance))


def build_forecast_calibration_sample(
    *,
    forecast_run: ForecastRunRecord,
    observed_value: float,
    created_at: datetime | None = None,
) -> ForecastCalibrationSampleRecord:
    mean = forecast_distribution_mean(forecast_run.forecast_payload.get("temperature_distribution") or {})
    normalized_created_at = created_at or datetime.now(UTC)
    lookup_key = build_calibration_lookup_key(
        station_id=forecast_run.station_id,
        source=forecast_run.source,
        observation_date=forecast_run.observation_date,
        forecast_target_time=forecast_run.forecast_target_time,
        metric=forecast_run.metric,
    )
    return ForecastCalibrationSampleRecord(
        sample_id=stable_object_id(
            "fcal",
            {
                "market_id": forecast_run.market_id,
                "run_id": forecast_run.run_id,
                "observed_value": float(observed_value),
            },
        ),
        market_id=forecast_run.market_id,
        station_id=forecast_run.station_id,
        source=forecast_run.source,
        forecast_horizon_bucket=lookup_key.forecast_horizon_bucket,
        season_bucket=lookup_key.season_bucket,
        metric=forecast_run.metric,
        forecast_target_time=forecast_run.forecast_target_time,
        forecast_mean=mean,
        observed_value=float(observed_value),
        residual=float(observed_value) - mean,
        created_at=normalized_created_at,
    )


class ForecastStdDevProvider(Protocol):
    def resolve_std_dev(self, *, station_id: str, source: str, observation_date: date, forecast_target_time: datetime, metric: str) -> float | None:
        ...


@dataclass(frozen=True)
class CalibrationConfidenceSummary:
    sample_count: int
    mean_abs_residual: float | None
    p90_abs_residual: float | None
    calibration_health_status: str
    calibration_confidence_multiplier: float
    reason_codes: list[str]

    def __post_init__(self) -> None:
        if self.sample_count < 0:
            raise ValueError("sample_count must be non-negative")
        if not (0.0 <= float(self.calibration_confidence_multiplier) <= 1.0):
            raise ValueError("calibration_confidence_multiplier must be between 0 and 1")
        if not self.calibration_health_status:
            raise ValueError("calibration_health_status is required")
        if not isinstance(self.reason_codes, list):
            raise ValueError("reason_codes must be a list")


@dataclass(frozen=True)
class ThresholdProbabilityProfile:
    threshold_bucket: str
    sample_count: int
    predicted_prob_mean: float
    realized_hit_rate: float
    brier_score: float | None
    reliability_gap: float | None
    quality_status: str


@dataclass(frozen=True)
class CalibrationProfileV2:
    profile_key: str
    station_id: str
    source: str
    metric: str
    forecast_horizon_bucket: str
    season_bucket: str
    regime_bucket: str
    sample_count: int
    mean_bias: float
    mean_abs_residual: float
    p90_abs_residual: float
    empirical_coverage_50: float | None
    empirical_coverage_80: float | None
    empirical_coverage_95: float | None
    regime_stability_score: float
    residual_quantiles_json: dict[str, float]
    threshold_probability_profile_json: dict[str, dict[str, float | int | str]] | None
    calibration_health_status: str
    window_start: datetime
    window_end: datetime
    materialized_at: datetime


@dataclass(frozen=True)
class CalibrationProfileMaterializationStatus:
    materialization_id: str
    run_id: str
    job_name: str
    status: str
    lookback_days: int
    source_window_start: datetime
    source_window_end: datetime
    input_sample_count: int
    output_profile_count: int
    fresh_profile_count: int
    stale_profile_count: int
    degraded_profile_count: int
    materialized_at: datetime
    error: str | None = None


@dataclass(frozen=True)
class ForecastCalibrationV2Summary:
    lookup_hit: bool
    sample_count: int
    raw_mean: float
    raw_std_dev: float
    corrected_mean: float
    corrected_std_dev: float
    regime_bucket: str
    calibration_health_status: str
    bias_quality_status: str
    threshold_probability_quality_status: str
    regime_stability_score: float
    quantiles_json: dict[str, float]
    empirical_coverage_json: dict[str, float | None]
    threshold_probability_summary_json: dict[str, dict[str, float | int | str]] | None
    profile_materialized_at: str | None
    profile_window_end: str | None
    calibration_freshness_status: str
    profile_age_hours: float | None
    reason_codes: list[str]

    def to_json(self) -> dict[str, Any]:
        return {
            "raw_mean": round(self.raw_mean, 6),
            "raw_std_dev": round(self.raw_std_dev, 6),
            "corrected_mean": round(self.corrected_mean, 6),
            "corrected_std_dev": round(self.corrected_std_dev, 6),
            "lookup_hit": bool(self.lookup_hit),
            "sample_count": int(self.sample_count),
            "regime_bucket": self.regime_bucket,
            "calibration_health_status": self.calibration_health_status,
            "bias_quality_status": self.bias_quality_status,
            "threshold_probability_quality_status": self.threshold_probability_quality_status,
            "regime_stability_score": round(self.regime_stability_score, 6),
            "quantiles_json": dict(self.quantiles_json),
            "empirical_coverage_json": dict(self.empirical_coverage_json),
            "threshold_probability_summary_json": None if self.threshold_probability_summary_json is None else dict(self.threshold_probability_summary_json),
            "profile_materialized_at": self.profile_materialized_at,
            "profile_window_end": self.profile_window_end,
            "calibration_freshness_status": self.calibration_freshness_status,
            "profile_age_hours": None if self.profile_age_hours is None else round(self.profile_age_hours, 4),
            "reason_codes": list(self.reason_codes),
        }


def calibration_profile_age_hours(
    materialized_at: datetime | None,
    *,
    as_of: datetime | None = None,
) -> float | None:
    if materialized_at is None:
        return None
    reference = _normalize_timestamp(as_of or datetime.now(UTC))
    normalized_materialized_at = _normalize_timestamp(materialized_at)
    age_seconds = (reference - normalized_materialized_at).total_seconds()
    return max(0.0, age_seconds / 3600.0)


def calibration_profile_freshness_status(
    materialized_at: datetime | None,
    *,
    as_of: datetime | None = None,
) -> str:
    age_hours = calibration_profile_age_hours(materialized_at, as_of=as_of)
    if age_hours is None:
        return "degraded_or_missing"
    if age_hours <= 36.0:
        return "fresh"
    if age_hours <= 96.0:
        return "stale"
    return "degraded_or_missing"


def calibration_confidence_from_metrics(
    *,
    sample_count: int,
    mean_abs_residual: float | None,
    p90_abs_residual: float | None,
    lookup_hit: bool,
) -> CalibrationConfidenceSummary:
    normalized_sample_count = max(0, int(sample_count))
    normalized_mean = float(mean_abs_residual) if mean_abs_residual is not None else None
    normalized_p90 = float(p90_abs_residual) if p90_abs_residual is not None else None
    reasons: list[str] = []

    if not lookup_hit:
        reasons.append("calibration_lookup_missing")
        return CalibrationConfidenceSummary(
            sample_count=0,
            mean_abs_residual=None,
            p90_abs_residual=None,
            calibration_health_status="lookup_missing",
            calibration_confidence_multiplier=0.50,
            reason_codes=reasons,
        )
    if normalized_sample_count < 5:
        reasons.append("calibration_insufficient_samples")
        return CalibrationConfidenceSummary(
            sample_count=normalized_sample_count,
            mean_abs_residual=normalized_mean,
            p90_abs_residual=normalized_p90,
            calibration_health_status="insufficient_samples",
            calibration_confidence_multiplier=0.55,
            reason_codes=reasons,
        )
    if normalized_sample_count < 20:
        reasons.append("calibration_limited_samples")
        return CalibrationConfidenceSummary(
            sample_count=normalized_sample_count,
            mean_abs_residual=normalized_mean,
            p90_abs_residual=normalized_p90,
            calibration_health_status="limited_samples",
            calibration_confidence_multiplier=0.75,
            reason_codes=reasons,
        )
    if (normalized_mean or 0.0) <= 1.5:
        return CalibrationConfidenceSummary(
            sample_count=normalized_sample_count,
            mean_abs_residual=normalized_mean,
            p90_abs_residual=normalized_p90,
            calibration_health_status="healthy",
            calibration_confidence_multiplier=1.0,
            reason_codes=reasons,
        )
    if (normalized_mean or 0.0) <= 3.0:
        reasons.append("calibration_watch")
        return CalibrationConfidenceSummary(
            sample_count=normalized_sample_count,
            mean_abs_residual=normalized_mean,
            p90_abs_residual=normalized_p90,
            calibration_health_status="watch",
            calibration_confidence_multiplier=0.85,
            reason_codes=reasons,
        )
    reasons.append("calibration_degraded")
    return CalibrationConfidenceSummary(
        sample_count=normalized_sample_count,
        mean_abs_residual=normalized_mean,
        p90_abs_residual=normalized_p90,
        calibration_health_status="degraded",
        calibration_confidence_multiplier=0.60,
        reason_codes=reasons,
    )


def threshold_probability_profile_for_probability(
    profile_json: dict[str, dict[str, float | int | str]] | None,
    probability: float | None,
) -> ThresholdProbabilityProfile | None:
    if profile_json is None or probability is None:
        return None
    bucket = threshold_probability_bucket(probability)
    entry = profile_json.get(bucket)
    if not isinstance(entry, dict):
        return None
    return ThresholdProbabilityProfile(
        threshold_bucket=bucket,
        sample_count=int(entry.get("sample_count") or 0),
        predicted_prob_mean=float(entry.get("predicted_prob_mean") or 0.0),
        realized_hit_rate=float(entry.get("realized_hit_rate") or 0.0),
        brier_score=None if entry.get("brier_score") is None else float(entry.get("brier_score")),
        reliability_gap=None if entry.get("reliability_gap") is None else float(entry.get("reliability_gap")),
        quality_status=str(entry.get("quality_status") or "lookup_missing"),
    )


def calibration_v2_context_for_probability(
    summary_json: dict[str, Any] | None,
    *,
    probability: float | None,
) -> dict[str, Any]:
    if not isinstance(summary_json, dict):
        return {}
    profile = threshold_probability_profile_for_probability(
        summary_json.get("threshold_probability_summary_json"),
        probability,
    )
    out = {
        "calibration_v2_mode": "profile_v2" if bool(summary_json.get("lookup_hit")) else "sigma_fallback",
        "corrected_mean": summary_json.get("corrected_mean"),
        "corrected_std_dev": summary_json.get("corrected_std_dev"),
        "calibration_health_status": summary_json.get("calibration_health_status"),
        "sample_count": summary_json.get("sample_count"),
        "bias_quality_status": summary_json.get("bias_quality_status"),
        "regime_bucket": summary_json.get("regime_bucket"),
        "regime_stability_score": summary_json.get("regime_stability_score"),
        "profile_materialized_at": summary_json.get("profile_materialized_at"),
        "profile_window_end": summary_json.get("profile_window_end"),
        "calibration_freshness_status": summary_json.get("calibration_freshness_status"),
        "profile_age_hours": summary_json.get("profile_age_hours"),
        "threshold_probability_summary_json": summary_json.get("threshold_probability_summary_json"),
        "calibration_reason_codes": list(summary_json.get("reason_codes") or []),
    }
    if profile is not None:
        out.update(
            {
                "threshold_probability_bucket": profile.threshold_bucket,
                "threshold_probability_quality_status": profile.quality_status,
                "threshold_probability_reliability_gap": profile.reliability_gap,
                "threshold_probability_sample_count": profile.sample_count,
            }
        )
    else:
        out["threshold_probability_quality_status"] = summary_json.get("threshold_probability_quality_status")
    return out


def materialize_forecast_calibration_profiles_v2(
    con,
    *,
    as_of: datetime,
    lookback_days: int = 180,
) -> list[CalibrationProfileV2]:
    window_end = _normalize_timestamp(as_of)
    window_start = window_end - timedelta(days=max(1, int(lookback_days)))
    samples = con.execute(
        """
        SELECT
            market_id,
            station_id,
            source,
            metric,
            forecast_horizon_bucket,
            season_bucket,
            forecast_target_time,
            forecast_mean,
            observed_value,
            residual,
            created_at
        FROM weather.forecast_calibration_samples
        WHERE created_at >= ? AND created_at <= ?
        """,
        [window_start, window_end],
    ).fetchall()
    if not samples:
        return []

    forecast_run_rows = con.execute(
        """
        WITH sample_keys AS (
            SELECT DISTINCT
                market_id,
                station_id,
                source,
                forecast_target_time
            FROM weather.forecast_calibration_samples
            WHERE created_at >= ? AND created_at <= ?
        )
        SELECT
            fr.market_id,
            fr.station_id,
            fr.source,
            fr.forecast_target_time,
            fr.forecast_payload_json
        FROM weather.weather_forecast_runs fr
        JOIN sample_keys sk
          ON fr.market_id = sk.market_id
         AND fr.station_id = sk.station_id
         AND fr.source = sk.source
         AND fr.forecast_target_time = sk.forecast_target_time
        ORDER BY fr.forecast_target_time DESC
        """,
        [window_start, window_end],
    ).fetchall()
    run_lookup: dict[tuple[str, str, str, datetime], dict[str, Any]] = {}
    for row in forecast_run_rows:
        key = (str(row[0]), str(row[1]), str(row[2]), _normalize_timestamp(row[3]))
        run_lookup.setdefault(key, _json_dict(row[4]))

    spec_rows = con.execute(
        """
        SELECT market_id, bucket_min_value, bucket_max_value, inclusive_bounds
        FROM weather.weather_market_specs
        """
    ).fetchall()
    spec_lookup = {str(row[0]): (row[1], row[2], bool(row[3])) for row in spec_rows}

    grouped: dict[tuple[str, str, str, str, str, str], list[dict[str, Any]]] = {}
    for row in samples:
        forecast_mean = float(row[7])
        group_key = (
            str(row[1]),
            str(row[2]),
            str(row[3]),
            str(row[4]),
            str(row[5]),
            calibration_regime_bucket(forecast_mean),
        )
        keyed_run = run_lookup.get((str(row[0]), str(row[1]), str(row[2]), _normalize_timestamp(row[6])))
        distribution = {}
        raw_std_dev = None
        predicted_yes_probability = None
        if keyed_run:
            distribution = _temperature_distribution_from_payload(keyed_run)
            if distribution:
                raw_std_dev = forecast_distribution_std_dev(distribution)
            spec = spec_lookup.get(str(row[0]))
            if distribution and spec is not None:
                predicted_yes_probability = _probability_in_bucket(
                    distribution,
                    bucket_min=spec[0],
                    bucket_max=spec[1],
                    inclusive_bounds=spec[2],
                )
        observed_hit = None
        spec = spec_lookup.get(str(row[0]))
        if spec is not None:
            observed_hit = _observed_hit(
                observed_value=float(row[8]),
                bucket_min=spec[0],
                bucket_max=spec[1],
                inclusive_bounds=spec[2],
            )
        grouped.setdefault(group_key, []).append(
            {
                "market_id": str(row[0]),
                "forecast_mean": forecast_mean,
                "observed_value": float(row[8]),
                "residual": float(row[9]),
                "raw_std_dev": raw_std_dev,
                "predicted_yes_probability": predicted_yes_probability,
                "observed_hit": observed_hit,
            }
        )

    out: list[CalibrationProfileV2] = []
    for group_key, rows in sorted(grouped.items()):
        station_id, source, metric, horizon_bucket, season_name, regime_bucket = group_key
        residuals = [float(item["residual"]) for item in rows]
        abs_residuals = [abs(item) for item in residuals]
        sample_count = len(rows)
        mean_bias = _mean(residuals)
        mean_abs_residual = _mean(abs_residuals)
        p90_abs_residual = _quantile(abs_residuals, 0.9)
        coverage_50 = _empirical_coverage(rows, z=0.67449)
        coverage_80 = _empirical_coverage(rows, z=1.28155)
        coverage_95 = _empirical_coverage(rows, z=1.95996)
        residual_quantiles = {
            "p50": round(_quantile(abs_residuals, 0.50), 6),
            "p80": round(_quantile(abs_residuals, 0.80), 6),
            "p90": round(p90_abs_residual, 6),
            "p95": round(_quantile(abs_residuals, 0.95), 6),
        }
        threshold_profile_json = _build_threshold_probability_profile_json(rows)
        confidence = calibration_confidence_from_metrics(
            sample_count=sample_count,
            mean_abs_residual=mean_abs_residual,
            p90_abs_residual=p90_abs_residual,
            lookup_hit=True,
        )
        regime_stability_score = _regime_stability_score(
            sample_count=sample_count,
            empirical_coverage_80=coverage_80,
        )
        calibration_health_status = _calibration_profile_health_status(
            confidence.calibration_health_status,
            regime_stability_score=regime_stability_score,
            sample_count=sample_count,
        )
        profile = CalibrationProfileV2(
            profile_key=stable_object_id(
                "calv2",
                {
                    "station_id": station_id,
                    "source": source,
                    "metric": metric,
                    "forecast_horizon_bucket": horizon_bucket,
                    "season_bucket": season_name,
                    "regime_bucket": regime_bucket,
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                },
            ),
            station_id=station_id,
            source=source,
            metric=metric,
            forecast_horizon_bucket=horizon_bucket,
            season_bucket=season_name,
            regime_bucket=regime_bucket,
            sample_count=sample_count,
            mean_bias=round(mean_bias, 6),
            mean_abs_residual=round(mean_abs_residual, 6),
            p90_abs_residual=round(p90_abs_residual, 6),
            empirical_coverage_50=coverage_50,
            empirical_coverage_80=coverage_80,
            empirical_coverage_95=coverage_95,
            regime_stability_score=round(regime_stability_score, 6),
            residual_quantiles_json=residual_quantiles,
            threshold_probability_profile_json=threshold_profile_json,
            calibration_health_status=calibration_health_status,
            window_start=window_start,
            window_end=window_end,
            materialized_at=window_end,
        )
        out.append(profile)
    return out


@dataclass(frozen=True)
class DuckDBForecastStdDevProvider:
    db_path: str | Path

    def resolve_std_dev(
        self,
        *,
        station_id: str,
        source: str,
        observation_date: date,
        forecast_target_time: datetime,
        metric: str,
    ) -> float | None:
        summary = self.resolve_confidence_summary(
            station_id=station_id,
            source=source,
            observation_date=observation_date,
            forecast_target_time=forecast_target_time,
            metric=metric,
        )
        if summary is None:
            return None
        lookup_key = build_calibration_lookup_key(
            station_id=station_id,
            source=source,
            observation_date=observation_date,
            forecast_target_time=forecast_target_time,
            metric=metric,
        )
        row = self._query_lookup_metrics(lookup_key=lookup_key)
        if row is None:
            return None
        residual_stddev = float(row[2] or 0.0)
        avg_abs_residual = float(row[1] or 0.0)
        if residual_stddev > 0.0:
            return residual_stddev
        if avg_abs_residual > 0.0:
            return avg_abs_residual
        return None

    def resolve_confidence_summary(
        self,
        *,
        station_id: str,
        source: str,
        observation_date: date,
        forecast_target_time: datetime,
        metric: str,
    ) -> CalibrationConfidenceSummary | None:
        lookup_key = build_calibration_lookup_key(
            station_id=station_id,
            source=source,
            observation_date=observation_date,
            forecast_target_time=forecast_target_time,
            metric=metric,
        )
        row = self._query_lookup_metrics(lookup_key=lookup_key)
        if row is None:
            return calibration_confidence_from_metrics(
                sample_count=0,
                mean_abs_residual=None,
                p90_abs_residual=None,
                lookup_hit=False,
            )
        sample_count = int(row[0] or 0)
        if sample_count <= 0:
            return calibration_confidence_from_metrics(
                sample_count=0,
                mean_abs_residual=None,
                p90_abs_residual=None,
                lookup_hit=False,
            )
        return calibration_confidence_from_metrics(
            sample_count=sample_count,
            mean_abs_residual=float(row[1] or 0.0),
            p90_abs_residual=float(row[3] or 0.0),
            lookup_hit=True,
        )

    def resolve_profile_v2(
        self,
        *,
        station_id: str,
        source: str,
        observation_date: date,
        forecast_target_time: datetime,
        metric: str,
        regime_bucket: str,
    ) -> CalibrationProfileV2 | None:
        row = self._query_profile_v2(
            station_id=station_id,
            source=source,
            metric=metric,
            forecast_horizon_bucket=forecast_horizon_bucket(
                observation_date=observation_date,
                forecast_target_time=forecast_target_time,
            ),
            season_name=season_bucket(observation_date),
            regime_bucket=regime_bucket,
        )
        if row is None:
            return None
        return CalibrationProfileV2(
            profile_key=str(row[0]),
            station_id=str(row[1]),
            source=str(row[2]),
            metric=str(row[3]),
            forecast_horizon_bucket=str(row[4]),
            season_bucket=str(row[5]),
            regime_bucket=str(row[6]),
            sample_count=int(row[7]),
            mean_bias=float(row[8]),
            mean_abs_residual=float(row[9]),
            p90_abs_residual=float(row[10]),
            empirical_coverage_50=_optional_float(row[11]),
            empirical_coverage_80=_optional_float(row[12]),
            empirical_coverage_95=_optional_float(row[13]),
            regime_stability_score=float(row[14]),
            residual_quantiles_json=_json_dict(row[15]),
            threshold_probability_profile_json=_json_threshold_profile(row[16]),
            calibration_health_status=str(row[17]),
            window_start=_normalize_timestamp(row[18]),
            window_end=_normalize_timestamp(row[19]),
            materialized_at=_normalize_timestamp(row[20]),
        )

    def _query_lookup_metrics(self, *, lookup_key: CalibrationLookupKey) -> tuple[object, object, object, object] | None:
        con = _connect_read_only_duckdb(self.db_path)
        if con is None:
            return None
        try:
            return con.execute(
                """
                SELECT
                    COUNT(*) AS sample_count,
                    AVG(ABS(residual)) AS avg_abs_residual,
                    STDDEV_SAMP(residual) AS residual_stddev,
                    quantile_cont(ABS(residual), 0.9) AS p90_abs_residual
                FROM weather.forecast_calibration_samples
                WHERE station_id = ?
                  AND source = ?
                  AND forecast_horizon_bucket = ?
                  AND season_bucket = ?
                  AND metric = ?
                """,
                [
                    lookup_key.station_id,
                    lookup_key.source,
                    lookup_key.forecast_horizon_bucket,
                    lookup_key.season_bucket,
                    lookup_key.metric,
                ],
            ).fetchone()
        except Exception:  # noqa: BLE001
            return None
        finally:
            con.close()

    def _query_profile_v2(
        self,
        *,
        station_id: str,
        source: str,
        metric: str,
        forecast_horizon_bucket: str,
        season_name: str,
        regime_bucket: str,
    ) -> tuple[Any, ...] | None:
        con = _connect_read_only_duckdb(self.db_path)
        if con is None:
            return None
        try:
            if not _table_exists(con, "weather.forecast_calibration_profiles_v2"):
                return None
            return con.execute(
                """
                SELECT
                    profile_key,
                    station_id,
                    source,
                    metric,
                    forecast_horizon_bucket,
                    season_bucket,
                    regime_bucket,
                    sample_count,
                    mean_bias,
                    mean_abs_residual,
                    p90_abs_residual,
                    empirical_coverage_50,
                    empirical_coverage_80,
                    empirical_coverage_95,
                    regime_stability_score,
                    residual_quantiles_json,
                    threshold_probability_profile_json,
                    calibration_health_status,
                    window_start,
                    window_end,
                    materialized_at
                FROM weather.forecast_calibration_profiles_v2
                WHERE station_id = ?
                  AND source = ?
                  AND metric = ?
                  AND forecast_horizon_bucket = ?
                  AND season_bucket = ?
                  AND regime_bucket = ?
                ORDER BY materialized_at DESC
                LIMIT 1
                """,
                [
                    station_id,
                    source,
                    metric,
                    forecast_horizon_bucket,
                    season_name,
                    regime_bucket,
                ],
            ).fetchone()
        except Exception:  # noqa: BLE001
            return None
        finally:
            con.close()


def _build_threshold_probability_profile_json(rows: list[dict[str, Any]]) -> dict[str, dict[str, float | int | str]] | None:
    grouped: dict[str, list[dict[str, float]]] = {}
    for item in rows:
        probability = item.get("predicted_yes_probability")
        observed_hit = item.get("observed_hit")
        if probability is None or observed_hit is None:
            continue
        bucket = threshold_probability_bucket(float(probability))
        grouped.setdefault(bucket, []).append(
            {
                "predicted_yes_probability": float(probability),
                "observed_hit": float(observed_hit),
            }
        )
    if not grouped:
        return None
    out: dict[str, dict[str, float | int | str]] = {}
    for bucket, bucket_rows in grouped.items():
        probs = [row["predicted_yes_probability"] for row in bucket_rows]
        hits = [row["observed_hit"] for row in bucket_rows]
        sample_count = len(bucket_rows)
        predicted_prob_mean = _mean(probs)
        realized_hit_rate = _mean(hits)
        reliability_gap = abs(realized_hit_rate - predicted_prob_mean)
        brier_score = _mean([(prob - hit) ** 2 for prob, hit in zip(probs, hits, strict=True)])
        out[bucket] = {
            "sample_count": sample_count,
            "predicted_prob_mean": round(predicted_prob_mean, 6),
            "realized_hit_rate": round(realized_hit_rate, 6),
            "brier_score": round(brier_score, 6),
            "reliability_gap": round(reliability_gap, 6),
            "quality_status": _threshold_quality_status(sample_count=sample_count, reliability_gap=reliability_gap),
        }
    return out


def _threshold_quality_status(*, sample_count: int, reliability_gap: float) -> str:
    if sample_count < 10:
        return "sparse"
    if reliability_gap <= 0.05:
        return "healthy"
    if reliability_gap <= 0.10:
        return "watch"
    return "degraded"


def _bias_quality_status(mean_bias: float, sample_count: int) -> str:
    if sample_count < 10:
        return "sparse"
    normalized = abs(float(mean_bias))
    if normalized <= 0.75:
        return "healthy"
    if normalized <= 1.5:
        return "watch"
    return "degraded"


def _threshold_profile_quality_status(profile_json: dict[str, dict[str, float | int | str]] | None) -> str:
    if not profile_json:
        return "lookup_missing"
    statuses = [str(item.get("quality_status") or "lookup_missing") for item in profile_json.values()]
    if "degraded" in statuses:
        return "degraded"
    if "watch" in statuses:
        return "watch"
    if all(status == "sparse" for status in statuses):
        return "sparse"
    if "healthy" in statuses:
        return "healthy"
    return "lookup_missing"


def _calibration_profile_health_status(base_status: str, *, regime_stability_score: float, sample_count: int) -> str:
    if sample_count < 10:
        return "sparse"
    if regime_stability_score < 0.60:
        return "degraded"
    if base_status in {"healthy", "watch", "degraded"}:
        return base_status
    if base_status in {"limited_samples", "insufficient_samples"}:
        return "sparse"
    return "lookup_missing"


def _regime_stability_score(*, sample_count: int, empirical_coverage_80: float | None) -> float:
    if empirical_coverage_80 is None:
        return 0.50 if sample_count >= 10 else 0.40
    coverage_gap = abs(float(empirical_coverage_80) - 0.80)
    score = max(0.0, min(1.0, 1.0 - (coverage_gap * 2.5)))
    if sample_count < 10:
        return min(score, 0.55)
    return score


def _empirical_coverage(rows: list[dict[str, Any]], *, z: float) -> float | None:
    eligible = [item for item in rows if item.get("raw_std_dev") is not None and float(item["raw_std_dev"]) > 0.0]
    if not eligible:
        return None
    covered = 0
    for item in eligible:
        if abs(float(item["residual"])) <= z * float(item["raw_std_dev"]):
            covered += 1
    return round(covered / len(eligible), 6)


def _observed_hit(*, observed_value: float, bucket_min: float | None, bucket_max: float | None, inclusive_bounds: bool) -> int | None:
    if bucket_min is None or bucket_max is None:
        return None
    lower_ok = observed_value >= float(bucket_min) if inclusive_bounds else observed_value > float(bucket_min)
    upper_ok = observed_value <= float(bucket_max) if inclusive_bounds else observed_value < float(bucket_max)
    return 1 if lower_ok and upper_ok else 0


def _probability_in_bucket(
    distribution: dict[int, float],
    *,
    bucket_min: float | None,
    bucket_max: float | None,
    inclusive_bounds: bool,
) -> float:
    if bucket_min is None or bucket_max is None:
        return 0.0
    total = 0.0
    for raw_temp, raw_prob in distribution.items():
        temp = float(raw_temp)
        prob = float(raw_prob)
        lower_ok = temp >= float(bucket_min) if inclusive_bounds else temp > float(bucket_min)
        upper_ok = temp <= float(bucket_max) if inclusive_bounds else temp < float(bucket_max)
        if lower_ok and upper_ok:
            total += prob
    return max(0.0, min(1.0, total))


def _temperature_distribution_from_payload(payload: dict[str, Any]) -> dict[int, float]:
    distribution = payload.get("temperature_distribution")
    if not isinstance(distribution, dict):
        return {}
    out: dict[int, float] = {}
    for raw_key, raw_value in distribution.items():
        try:
            out[int(float(raw_key))] = float(raw_value)
        except (TypeError, ValueError):
            continue
    return out


def _quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    position = max(0.0, min(1.0, float(q))) * (len(ordered) - 1)
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(float(value) for value in values) / len(values)


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _json_threshold_profile(value: Any) -> dict[str, dict[str, float | int | str]] | None:
    parsed = _json_dict(value)
    if not parsed:
        return None
    out: dict[str, dict[str, float | int | str]] = {}
    for key, raw in parsed.items():
        if isinstance(raw, dict):
            out[str(key)] = dict(raw)
    return out or None


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(UTC).replace(tzinfo=None)
        return value.replace(tzinfo=None)
    raise TypeError("expected datetime")


def _connect_read_only_duckdb(db_path: str | Path):
    try:
        import duckdb
    except ModuleNotFoundError:  # pragma: no cover
        return None
    path = Path(db_path)
    if not path.exists():
        return None
    try:
        return duckdb.connect(str(path), read_only=True)
    except Exception:  # noqa: BLE001
        return None


def _table_exists(con, table_name: str) -> bool:
    try:
        row = con.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE lower(table_schema || '.' || table_name) = lower(?)
            """,
            [table_name],
        ).fetchone()
    except Exception:  # noqa: BLE001
        return False
    return bool(row and int(row[0]) > 0)


__all__ = [
    "CalibrationConfidenceSummary",
    "CalibrationProfileV2",
    "CalibrationProfileMaterializationStatus",
    "DuckDBForecastStdDevProvider",
    "ForecastCalibrationV2Summary",
    "ForecastStdDevProvider",
    "ThresholdProbabilityProfile",
    "build_calibration_lookup_key",
    "calibration_profile_age_hours",
    "calibration_profile_freshness_status",
    "build_forecast_calibration_sample",
    "calibration_confidence_from_metrics",
    "calibration_regime_bucket",
    "calibration_v2_context_for_probability",
    "forecast_distribution_mean",
    "forecast_distribution_std_dev",
    "forecast_horizon_bucket",
    "materialize_forecast_calibration_profiles_v2",
    "season_bucket",
    "threshold_probability_bucket",
    "threshold_probability_profile_for_probability",
]
