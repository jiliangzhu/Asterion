from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any

from asterion_core.clients.shared import build_url
from asterion_core.contracts import ForecastRequest

from .calibration import (
    CalibrationConfidenceSummary,
    ForecastCalibrationV2Summary,
    ForecastStdDevProvider,
    calibration_regime_bucket,
    forecast_distribution_mean,
    forecast_distribution_std_dev,
)
from .service import ForecastDistribution

# Historical forecast error standard deviations (in Fahrenheit)
# Based on typical weather forecast accuracy
FORECAST_STD_DEV_1DAY = 3.0
FORECAST_STD_DEV_3DAY = 4.5
FORECAST_STD_DEV_7DAY = 6.0


def build_normal_distribution(mean: float, std_dev: float, min_temp: int = -100, max_temp: int = 200) -> dict[int, float]:
    """Build temperature probability distribution based on normal distribution.

    Args:
        mean: Forecast mean temperature
        std_dev: Standard deviation based on historical forecast error
        min_temp: Minimum temperature to consider
        max_temp: Maximum temperature to consider

    Returns:
        Dictionary mapping temperature to probability, summing to 1.0
    """
    distribution = {}
    total_prob = 0.0

    for temp in range(min_temp, max_temp + 1):
        z = (temp - mean) / std_dev
        prob = math.exp(-0.5 * z * z) / (std_dev * math.sqrt(2 * math.pi))
        distribution[temp] = prob
        total_prob += prob

    # Normalize
    if total_prob > 0:
        distribution = {t: p / total_prob for t, p in distribution.items()}

    return distribution


def _forecast_std_dev(request: ForecastRequest) -> float:
    horizon_days = max(0, (request.observation_date - request.forecast_target_time.date()).days)
    if horizon_days <= 1:
        return FORECAST_STD_DEV_1DAY
    if horizon_days <= 3:
        return FORECAST_STD_DEV_3DAY
    return FORECAST_STD_DEV_7DAY


@dataclass(frozen=True)
class StdDevResolutionSummary:
    resolved_std_dev: float
    lookup_hit: bool
    sample_count: int
    calibration_health_status: str


@dataclass(frozen=True)
class ForecastDistributionSummaryV2:
    raw_mean: float
    raw_std_dev: float
    corrected_mean: float
    corrected_std_dev: float
    quantiles_json: dict[str, float]
    empirical_coverage_json: dict[str, float | None]
    threshold_probability_summary_json: dict[str, dict[str, float | int | str]] | None
    lookup_hit: bool
    sample_count: int
    regime_bucket: str
    calibration_health_status: str
    bias_quality_status: str
    threshold_probability_quality_status: str
    regime_stability_score: float
    reason_codes: list[str]

    def to_json(self) -> dict[str, Any]:
        return ForecastCalibrationV2Summary(
            lookup_hit=self.lookup_hit,
            sample_count=self.sample_count,
            raw_mean=self.raw_mean,
            raw_std_dev=self.raw_std_dev,
            corrected_mean=self.corrected_mean,
            corrected_std_dev=self.corrected_std_dev,
            regime_bucket=self.regime_bucket,
            calibration_health_status=self.calibration_health_status,
            bias_quality_status=self.bias_quality_status,
            threshold_probability_quality_status=self.threshold_probability_quality_status,
            regime_stability_score=self.regime_stability_score,
            quantiles_json=self.quantiles_json,
            empirical_coverage_json=self.empirical_coverage_json,
            threshold_probability_summary_json=self.threshold_probability_summary_json,
            reason_codes=self.reason_codes,
        ).to_json()


def resolve_std_dev_summary(request: ForecastRequest, provider: ForecastStdDevProvider | None) -> StdDevResolutionSummary:
    fallback = StdDevResolutionSummary(
        resolved_std_dev=_forecast_std_dev(request),
        lookup_hit=False,
        sample_count=0,
        calibration_health_status="lookup_missing",
    )
    if provider is None:
        return fallback
    resolved = provider.resolve_std_dev(
        station_id=request.station_id,
        source=request.source,
        observation_date=request.observation_date,
        forecast_target_time=request.forecast_target_time,
        metric=request.metric,
    )
    summary: CalibrationConfidenceSummary | None = None
    if hasattr(provider, "resolve_confidence_summary"):
        maybe_summary = provider.resolve_confidence_summary(
            station_id=request.station_id,
            source=request.source,
            observation_date=request.observation_date,
            forecast_target_time=request.forecast_target_time,
            metric=request.metric,
        )
        if isinstance(maybe_summary, CalibrationConfidenceSummary):
            summary = maybe_summary
    if resolved is None or float(resolved) <= 0.0:
        return fallback if summary is None else StdDevResolutionSummary(
            resolved_std_dev=fallback.resolved_std_dev,
            lookup_hit=False,
            sample_count=summary.sample_count,
            calibration_health_status=summary.calibration_health_status,
        )
    return StdDevResolutionSummary(
        resolved_std_dev=float(resolved),
        lookup_hit=True,
        sample_count=0 if summary is None else int(summary.sample_count),
        calibration_health_status="healthy" if summary is None else str(summary.calibration_health_status),
    )


def _resolve_std_dev(request: ForecastRequest, provider: ForecastStdDevProvider | None) -> float:
    return resolve_std_dev_summary(request, provider).resolved_std_dev


def resolve_distribution_summary_v2(
    request: ForecastRequest,
    provider: ForecastStdDevProvider | None,
    *,
    raw_distribution: dict[int, float],
) -> ForecastDistributionSummaryV2:
    raw_mean = forecast_distribution_mean(raw_distribution)
    raw_std_dev = max(forecast_distribution_std_dev(raw_distribution), 0.0001)
    regime_bucket = calibration_regime_bucket(raw_mean)
    reason_codes: list[str] = []
    profile = None
    if provider is not None and hasattr(provider, "resolve_profile_v2"):
        profile = provider.resolve_profile_v2(
            station_id=request.station_id,
            source=request.source,
            observation_date=request.observation_date,
            forecast_target_time=request.forecast_target_time,
            metric=request.metric,
            regime_bucket=regime_bucket,
        )
    if profile is None:
        reason_codes.append("calibration_v2_lookup_missing")
        summary = resolve_std_dev_summary(request, provider)
        return ForecastDistributionSummaryV2(
            raw_mean=raw_mean,
            raw_std_dev=raw_std_dev,
            corrected_mean=raw_mean,
            corrected_std_dev=max(summary.resolved_std_dev, raw_std_dev),
            quantiles_json=_normal_quantiles(raw_mean, max(summary.resolved_std_dev, raw_std_dev)),
            empirical_coverage_json={"coverage_50": None, "coverage_80": None, "coverage_95": None},
            threshold_probability_summary_json=None,
            lookup_hit=False,
            sample_count=summary.sample_count,
            regime_bucket=regime_bucket,
            calibration_health_status=summary.calibration_health_status,
            bias_quality_status="lookup_missing",
            threshold_probability_quality_status="lookup_missing",
            regime_stability_score=0.5,
            reason_codes=reason_codes,
        )
    corrected_std_dev = max(raw_std_dev, profile.p90_abs_residual / 1.645 if profile.p90_abs_residual > 0 else raw_std_dev, profile.mean_abs_residual)
    if profile.regime_stability_score < 0.60:
        corrected_std_dev *= 1.15
        reason_codes.append("regime_unstable")
    if profile.sample_count < 10:
        reason_codes.append("calibration_v2_sparse")
    threshold_quality = _threshold_quality_status_from_profile(profile.threshold_probability_profile_json)
    if profile.threshold_probability_profile_json is None:
        reason_codes.append("threshold_profile_missing")
    return ForecastDistributionSummaryV2(
        raw_mean=raw_mean,
        raw_std_dev=raw_std_dev,
        corrected_mean=raw_mean + float(profile.mean_bias),
        corrected_std_dev=max(corrected_std_dev, 0.0001),
        quantiles_json=_normal_quantiles(raw_mean + float(profile.mean_bias), max(corrected_std_dev, 0.0001)),
        empirical_coverage_json={
            "coverage_50": profile.empirical_coverage_50,
            "coverage_80": profile.empirical_coverage_80,
            "coverage_95": profile.empirical_coverage_95,
        },
        threshold_probability_summary_json=profile.threshold_probability_profile_json,
        lookup_hit=True,
        sample_count=profile.sample_count,
        regime_bucket=profile.regime_bucket,
        calibration_health_status=profile.calibration_health_status,
        bias_quality_status=_bias_quality_status_from_profile(profile.mean_bias, profile.sample_count),
        threshold_probability_quality_status=threshold_quality,
        regime_stability_score=profile.regime_stability_score,
        reason_codes=reason_codes,
    )


def _apply_calibration_correction_layer(summary: ForecastDistributionSummaryV2) -> dict[int, float]:
    return build_normal_distribution(summary.corrected_mean, max(summary.corrected_std_dev, 0.0001))


@dataclass(frozen=True)
class OpenMeteoAdapter:
    client: Any
    base_url: str = "https://api.open-meteo.com"
    forecast_endpoint: str = "/v1/forecast"
    source_name: str = "openmeteo"
    std_dev_provider: ForecastStdDevProvider | None = None

    def fetch_forecast(self, request: ForecastRequest) -> ForecastDistribution:
        variable = _openmeteo_variable_for_metric(request.metric)
        url = build_url(
            self.base_url,
            self.forecast_endpoint,
            {
                "latitude": request.latitude,
                "longitude": request.longitude,
                "timezone": request.timezone,
                "daily": variable,
            },
        )
        payload = self.client.get_json(url, context={"source": self.source_name, "station_id": request.station_id})
        daily = payload.get("daily")
        if not isinstance(daily, dict):
            raise ValueError("open-meteo daily payload missing")
        values = daily.get(variable)
        if not isinstance(values, list) or not values:
            raise ValueError(f"open-meteo variable missing:{variable}")
        point_value = float(values[0])
        raw_std_dev = _resolve_std_dev(request, self.std_dev_provider)
        raw_distribution = build_normal_distribution(point_value, raw_std_dev)
        summary_v2 = resolve_distribution_summary_v2(request, self.std_dev_provider, raw_distribution=raw_distribution)
        distribution = _apply_calibration_correction_layer(summary_v2)
        return _build_distribution(
            request,
            source=self.source_name,
            distribution=distribution,
            raw_payload=payload,
            distribution_summary_v2=summary_v2.to_json(),
        )


@dataclass(frozen=True)
class NWSAdapter:
    client: Any
    base_url: str = "https://api.weather.gov"
    points_endpoint: str = "/points/{lat},{lon}"
    source_name: str = "nws"
    std_dev_provider: ForecastStdDevProvider | None = None

    def fetch_forecast(self, request: ForecastRequest) -> ForecastDistribution:
        points_url = build_url(
            self.base_url,
            self.points_endpoint.format(lat=request.latitude, lon=request.longitude),
        )
        points_payload = self.client.get_json(points_url, context={"source": self.source_name, "step": "points"})
        forecast_url = _extract_forecast_url(points_payload)
        forecast_payload = self.client.get_json(forecast_url, context={"source": self.source_name, "step": "forecast"})
        periods = ((forecast_payload.get("properties") or {}).get("periods"))
        if not isinstance(periods, list) or not periods:
            raise ValueError("nws periods missing")
        temperatures = [float(item["temperature"]) for item in periods if isinstance(item, dict) and item.get("temperature") is not None]
        if not temperatures:
            raise ValueError("nws temperature missing")
        metric = request.metric.lower()
        point_value = max(temperatures) if "max" in metric or "high" in metric else min(temperatures)
        raw_std_dev = _resolve_std_dev(request, self.std_dev_provider)
        raw_distribution = build_normal_distribution(point_value, raw_std_dev)
        summary_v2 = resolve_distribution_summary_v2(request, self.std_dev_provider, raw_distribution=raw_distribution)
        distribution = _apply_calibration_correction_layer(summary_v2)

        raw_payload = {"points": points_payload, "forecast": forecast_payload}
        return _build_distribution(
            request,
            source=self.source_name,
            distribution=distribution,
            raw_payload=raw_payload,
            distribution_summary_v2=summary_v2.to_json(),
        )


def _build_distribution(
    request: ForecastRequest,
    *,
    source: str,
    distribution: dict[int, float],
    raw_payload: dict[str, Any],
    distribution_summary_v2: dict[str, Any] | None,
) -> ForecastDistribution:
    return ForecastDistribution(
        market_id=request.market_id,
        condition_id=request.condition_id,
        station_id=request.station_id,
        source=source,
        model_run=request.model_run,
        forecast_target_time=request.forecast_target_time,
        observation_date=request.observation_date,
        metric=request.metric,
        latitude=request.latitude,
        longitude=request.longitude,
        timezone=request.timezone,
        spec_version=request.spec_version,
        temperature_distribution=distribution,
        distribution_summary_v2=distribution_summary_v2,
        source_trace=[source],
        raw_payload=raw_payload,
        from_cache=False,
        fallback_used=False,
        cache_key="",
    )


def _normal_quantiles(mean: float, std_dev: float) -> dict[str, float]:
    normalized = max(float(std_dev), 0.0001)
    return {
        "p10": round(mean - (1.28155 * normalized), 6),
        "p50": round(mean, 6),
        "p90": round(mean + (1.28155 * normalized), 6),
        "p95": round(mean + (1.645 * normalized), 6),
    }


def _bias_quality_status_from_profile(mean_bias: float, sample_count: int) -> str:
    if sample_count < 10:
        return "sparse"
    normalized = abs(float(mean_bias))
    if normalized <= 0.75:
        return "healthy"
    if normalized <= 1.5:
        return "watch"
    return "degraded"


def _threshold_quality_status_from_profile(profile_json: dict[str, dict[str, float | int | str]] | None) -> str:
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


def _openmeteo_variable_for_metric(metric: str) -> str:
    lower = metric.lower()
    if "min" in lower or "low" in lower:
        return "temperature_2m_min"
    return "temperature_2m_max"


def _extract_forecast_url(payload: dict[str, Any]) -> str:
    properties = payload.get("properties")
    if not isinstance(properties, dict):
        raise ValueError("nws points properties missing")
    url = properties.get("forecast")
    if not isinstance(url, str) or not url.strip():
        raise ValueError("nws forecast url missing")
    return url
