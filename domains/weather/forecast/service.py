from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Protocol

from asterion_core.contracts import ForecastRequest, ResolutionSpec, build_forecast_cache_key


@dataclass(frozen=True)
class ForecastDistribution:
    market_id: str
    condition_id: str
    station_id: str
    source: str
    model_run: str
    forecast_target_time: datetime
    observation_date: date
    metric: str
    latitude: float
    longitude: float
    timezone: str
    spec_version: str
    temperature_distribution: dict[int, float]
    distribution_summary_v2: dict[str, Any] | None
    source_trace: list[str]
    raw_payload: dict[str, Any]
    from_cache: bool
    fallback_used: bool
    cache_key: str

    @property
    def confidence(self) -> float:
        if not self.temperature_distribution:
            return 0.0
        return max(self.temperature_distribution.values())


class ForecastAdapter(Protocol):
    source_name: str

    def fetch_forecast(self, request: ForecastRequest) -> ForecastDistribution:
        ...


class ForecastCache(Protocol):
    def get(self, cache_key: str) -> ForecastDistribution | None:
        ...

    def put(self, cache_key: str, distribution: ForecastDistribution) -> None:
        ...


class AdapterRouter:
    def __init__(self, adapters: list[ForecastAdapter]) -> None:
        self._adapters = {normalize_forecast_source_name(adapter.source_name): adapter for adapter in adapters}

    def fetch(self, request: ForecastRequest, *, fallback_sources: list[str]) -> ForecastDistribution:
        primary = normalize_forecast_source_name(request.source)
        seen: set[str] = set()
        sources: list[str] = []
        for raw in [primary, *fallback_sources]:
            source = normalize_forecast_source_name(raw)
            if source in seen:
                continue
            seen.add(source)
            sources.append(source)
        errors: list[str] = []
        attempted: list[str] = []
        for source in sources:
            adapter = self._adapters.get(source)
            if adapter is None:
                errors.append(f"missing_adapter:{source}")
                continue
            attempted.append(source)
            routed_request = request if source == primary else ForecastRequest(
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
            )
            try:
                distribution = adapter.fetch_forecast(routed_request)
                return ForecastDistribution(
                    market_id=distribution.market_id,
                    condition_id=distribution.condition_id,
                    station_id=distribution.station_id,
                    source=distribution.source,
                    model_run=distribution.model_run,
                    forecast_target_time=distribution.forecast_target_time,
                    observation_date=distribution.observation_date,
                    metric=distribution.metric,
                    latitude=distribution.latitude,
                    longitude=distribution.longitude,
                    timezone=distribution.timezone,
                    spec_version=distribution.spec_version,
                    temperature_distribution=dict(distribution.temperature_distribution),
                    distribution_summary_v2=None if distribution.distribution_summary_v2 is None else dict(distribution.distribution_summary_v2),
                    source_trace=attempted,
                    raw_payload=dict(distribution.raw_payload),
                    from_cache=distribution.from_cache,
                    fallback_used=source != primary,
                    cache_key=distribution.cache_key,
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{source}:{exc}")
        raise RuntimeError("forecast_fetch_failed:" + ";".join(errors))


def build_forecast_request(
    resolution_spec: ResolutionSpec,
    *,
    source: str,
    model_run: str,
    forecast_target_time: datetime,
) -> ForecastRequest:
    return ForecastRequest(
        market_id=resolution_spec.market_id,
        condition_id=resolution_spec.condition_id,
        station_id=resolution_spec.station_id,
        source=source,
        model_run=model_run,
        forecast_target_time=forecast_target_time,
        observation_date=resolution_spec.observation_date,
        metric=resolution_spec.metric,
        latitude=resolution_spec.latitude,
        longitude=resolution_spec.longitude,
        timezone=resolution_spec.timezone,
        spec_version=resolution_spec.spec_version,
    )


class ForecastService:
    def __init__(self, *, adapter_router: AdapterRouter, cache: ForecastCache) -> None:
        self._adapter_router = adapter_router
        self._cache = cache

    def get_forecast(
        self,
        resolution_spec: ResolutionSpec,
        *,
        source: str,
        model_run: str,
        forecast_target_time: datetime,
    ) -> ForecastDistribution:
        request = build_forecast_request(
            resolution_spec,
            source=normalize_forecast_source_name(source),
            model_run=model_run,
            forecast_target_time=forecast_target_time,
        )
        cache_key = build_forecast_cache_key(
            market_id=request.market_id,
            station_id=request.station_id,
            spec_version=request.spec_version,
            source=request.source,
            model_run=request.model_run,
            forecast_target_time=request.forecast_target_time,
        )
        cached = self._cache.get(cache_key)
        if cached is not None:
            return ForecastDistribution(
                market_id=cached.market_id,
                condition_id=cached.condition_id,
                station_id=cached.station_id,
                source=cached.source,
                model_run=cached.model_run,
                forecast_target_time=cached.forecast_target_time,
                observation_date=cached.observation_date,
                metric=cached.metric,
                latitude=cached.latitude,
                longitude=cached.longitude,
                timezone=cached.timezone,
                spec_version=cached.spec_version,
                temperature_distribution=dict(cached.temperature_distribution),
                distribution_summary_v2=None if cached.distribution_summary_v2 is None else dict(cached.distribution_summary_v2),
                source_trace=list(cached.source_trace),
                raw_payload=dict(cached.raw_payload),
                from_cache=True,
                fallback_used=cached.fallback_used,
                cache_key=cache_key,
            )

        distribution = self._adapter_router.fetch(
            request,
            fallback_sources=list(resolution_spec.fallback_sources),
        )
        normalized = ForecastDistribution(
            market_id=distribution.market_id,
            condition_id=distribution.condition_id,
            station_id=distribution.station_id,
            source=distribution.source,
            model_run=distribution.model_run,
            forecast_target_time=distribution.forecast_target_time,
            observation_date=distribution.observation_date,
            metric=distribution.metric,
            latitude=distribution.latitude,
            longitude=distribution.longitude,
            timezone=distribution.timezone,
            spec_version=distribution.spec_version,
            temperature_distribution=_normalize_distribution(distribution.temperature_distribution),
            distribution_summary_v2=None if distribution.distribution_summary_v2 is None else dict(distribution.distribution_summary_v2),
            source_trace=list(distribution.source_trace or [distribution.source]),
            raw_payload=dict(distribution.raw_payload),
            from_cache=False,
            fallback_used=distribution.fallback_used,
            cache_key=cache_key,
        )
        self._cache.put(cache_key, normalized)
        return normalized


def _normalize_distribution(distribution: dict[int, float]) -> dict[int, float]:
    if not distribution:
        raise ValueError("temperature_distribution is required")
    total = sum(max(0.0, float(value)) for value in distribution.values())
    if total <= 0:
        raise ValueError("temperature_distribution total must be positive")
    return {int(k): float(max(0.0, v)) / total for k, v in distribution.items()}


def normalize_forecast_source_name(value: str) -> str:
    text = str(value).strip().lower().replace("_", "-")
    if text in {"openmeteo", "open-meteo"}:
        return "openmeteo"
    return text
