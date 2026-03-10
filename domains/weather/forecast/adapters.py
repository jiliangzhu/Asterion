from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from asterion_core.clients.shared import build_url
from asterion_core.contracts import ForecastRequest

from .service import ForecastDistribution


@dataclass(frozen=True)
class OpenMeteoAdapter:
    client: Any
    base_url: str = "https://api.open-meteo.com"
    forecast_endpoint: str = "/v1/forecast"
    source_name: str = "openmeteo"

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
        distribution = {int(round(point_value)): 1.0}
        return _build_distribution(request, source=self.source_name, distribution=distribution, raw_payload=payload)


@dataclass(frozen=True)
class NWSAdapter:
    client: Any
    base_url: str = "https://api.weather.gov"
    points_endpoint: str = "/points/{lat},{lon}"
    source_name: str = "nws"

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
        distribution = {int(round(point_value)): 1.0}
        raw_payload = {"points": points_payload, "forecast": forecast_payload}
        return _build_distribution(request, source=self.source_name, distribution=distribution, raw_payload=raw_payload)


def _build_distribution(
    request: ForecastRequest,
    *,
    source: str,
    distribution: dict[int, float],
    raw_payload: dict[str, Any],
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
        source_trace=[source],
        raw_payload=raw_payload,
        from_cache=False,
        fallback_used=False,
        cache_key="",
    )


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
