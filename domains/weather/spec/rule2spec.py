from __future__ import annotations

import json
import re
from datetime import UTC, date, datetime
from typing import Any

from asterion_core.contracts import (
    ResolutionSpec,
    Rule2SpecDraft,
    StationMetadata,
    WeatherMarket,
    WeatherMarketSpecRecord,
    stable_object_id,
)
from asterion_core.storage.os_queue import enqueue_upsert_rows_v1
from asterion_core.storage.utils import safe_json_dumps
from asterion_core.storage.write_queue import WriteQueueConfig

from .station_mapper import StationMapper


WEATHER_MARKET_SPEC_COLUMNS = [
    "market_id",
    "condition_id",
    "location_name",
    "station_id",
    "latitude",
    "longitude",
    "timezone",
    "observation_date",
    "observation_window_local",
    "metric",
    "unit",
    "bucket_min_value",
    "bucket_max_value",
    "authoritative_source",
    "fallback_sources",
    "rounding_rule",
    "inclusive_bounds",
    "spec_version",
    "parse_confidence",
    "risk_flags_json",
    "created_at",
    "updated_at",
]

WEATHER_MARKET_SELECT_SQL = """
SELECT
    market_id,
    condition_id,
    event_id,
    slug,
    title,
    description,
    rules,
    status,
    active,
    closed,
    archived,
    accepting_orders,
    enable_order_book,
    tags_json,
    outcomes_json,
    token_ids_json,
    close_time,
    end_date,
    raw_market_json
FROM weather.weather_markets
"""

_TEMP_RANGE_TITLE_RE = re.compile(
    r"(?i)will the (?P<metric>high(?:est)?|low(?:est)?) temperature in (?P<location>.+?)"
    r"(?:\s+on\s+(?P<date_before>[A-Za-z]+ \d{1,2}(?:st|nd|rd|th)?(?:, \d{4})?))?"
    r"\s+be "
    r"(?:between\s+)?(?P<min>-?\d+(?:\.\d+)?)\s*(?:-|to)\s*(?P<max>-?\d+(?:\.\d+)?)\s*°?\s*(?P<unit>[FC])"
    r"(?:\s+on\s+(?P<date_after>[A-Za-z]+ \d{1,2}(?:st|nd|rd|th)?(?:, \d{4})?))?\??"
)
_TEMP_THRESHOLD_TITLE_RE = re.compile(
    r"(?i)will the (?P<metric>high(?:est)?|low(?:est)?) temperature in (?P<location>.+?) be "
    r"(?P<threshold>-?\d+(?:\.\d+)?)\s*°?\s*(?P<unit>[FC])\s*or\s*(?P<direction>higher|lower)"
    r"(?:\s+on\s+(?P<date>[A-Za-z]+ \d{1,2}(?:st|nd|rd|th)?, \d{4}))?\??"
)


def load_weather_markets_for_rule2spec(con, *, active_only: bool = True, limit: int | None = None) -> list[WeatherMarket]:
    sql = WEATHER_MARKET_SELECT_SQL
    params: list[Any] = []
    if active_only:
        sql += " WHERE active = ?"
        params.append(True)
    sql += " ORDER BY updated_at DESC NULLS LAST, market_id"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))

    rows = con.execute(sql, params).fetchall()
    markets: list[WeatherMarket] = []
    for row in rows:
        markets.append(
            WeatherMarket(
                market_id=row[0],
                condition_id=row[1],
                event_id=row[2],
                slug=row[3],
                title=row[4],
                description=row[5],
                rules=row[6],
                status=row[7],
                active=bool(row[8]),
                closed=bool(row[9]),
                archived=bool(row[10]),
                accepting_orders=_optional_bool(row[11]),
                enable_order_book=_optional_bool(row[12]),
                tags=_json_list(row[13]),
                outcomes=_json_list(row[14]),
                token_ids=_json_list(row[15]),
                close_time=_to_datetime(row[16]),
                end_date=_to_datetime(row[17]),
                raw_market=_json_dict(row[18]),
            )
        )
    return markets


def parse_rule2spec_draft(market: WeatherMarket) -> Rule2SpecDraft:
    title_match = _TEMP_RANGE_TITLE_RE.search(market.title)
    risk_flags: list[str] = []
    parse_confidence = 1.0

    if title_match is not None:
        metric_name = _normalize_metric_name(title_match.group("metric"))
        location_name = title_match.group("location").strip()
        observation_date = _parse_range_date(
            title_match.group("date_before") or title_match.group("date_after"),
            market.end_date,
            market.close_time,
        )
        bucket_min_value = float(title_match.group("min"))
        bucket_max_value = float(title_match.group("max"))
        unit = "fahrenheit" if title_match.group("unit").upper() == "F" else "celsius"
    else:
        threshold_match = _TEMP_THRESHOLD_TITLE_RE.search(market.title)
        if threshold_match is None:
            raise ValueError(f"unsupported_weather_market_title:{market.title}")
        metric_name = _normalize_metric_name(threshold_match.group("metric"))
        location_name = threshold_match.group("location").strip()
        observation_date = _parse_threshold_date(
            threshold_match.group("date"),
            market.end_date,
            market.close_time,
        )
        unit = "fahrenheit" if threshold_match.group("unit").upper() == "F" else "celsius"
        threshold = float(threshold_match.group("threshold"))
        if threshold_match.group("direction").lower() == "higher":
            bucket_min_value = threshold
            bucket_max_value = _default_upper_bound(unit)
        else:
            bucket_min_value = _default_lower_bound(unit)
            bucket_max_value = threshold
        risk_flags.append("threshold_market_template")
        parse_confidence -= 0.05

    metric = "temperature_max" if metric_name == "high" else "temperature_min"
    observation_window_local = "daily_max" if metric_name == "high" else "daily_min"

    authoritative_source = _extract_authoritative_source(market.rules or "")
    if authoritative_source == "unknown":
        risk_flags.append("missing_authoritative_source")
        parse_confidence -= 0.15

    rounding_rule = _extract_rounding_rule(market.rules or "")
    if rounding_rule == "identity":
        parse_confidence -= 0.05

    if bucket_max_value < bucket_min_value:
        raise ValueError("bucket_max_value must be >= bucket_min_value")
    if not market.token_ids:
        risk_flags.append("missing_token_ids")
        parse_confidence -= 0.10

    fallback_sources = _fallback_sources_for(authoritative_source)
    return Rule2SpecDraft(
        market_id=market.market_id,
        condition_id=market.condition_id,
        location_name=location_name,
        observation_date=observation_date,
        observation_window_local=observation_window_local,
        metric=metric,
        unit=unit,
        bucket_min_value=bucket_min_value,
        bucket_max_value=bucket_max_value,
        authoritative_source=authoritative_source,
        fallback_sources=fallback_sources,
        rounding_rule=rounding_rule,
        inclusive_bounds=True,
        parse_confidence=max(0.0, min(1.0, parse_confidence)),
        risk_flags=risk_flags,
    )


def build_rule2spec_review_payload(draft: Rule2SpecDraft) -> dict[str, Any]:
    return {
        "market_id": draft.market_id,
        "condition_id": draft.condition_id,
        "location_name": draft.location_name,
        "observation_date": draft.observation_date.isoformat(),
        "observation_window_local": draft.observation_window_local,
        "metric": draft.metric,
        "unit": draft.unit,
        "bucket_min_value": draft.bucket_min_value,
        "bucket_max_value": draft.bucket_max_value,
        "authoritative_source": draft.authoritative_source,
        "fallback_sources": list(draft.fallback_sources),
        "rounding_rule": draft.rounding_rule,
        "inclusive_bounds": draft.inclusive_bounds,
        "parse_confidence": draft.parse_confidence,
        "risk_flags": list(draft.risk_flags),
    }


def build_resolution_spec_from_draft(
    draft: Rule2SpecDraft,
    *,
    station_metadata: StationMetadata,
    spec_version: str | None = None,
) -> ResolutionSpec:
    version = spec_version or build_spec_version(draft, station_metadata=station_metadata)
    return ResolutionSpec(
        market_id=draft.market_id,
        condition_id=draft.condition_id,
        location_name=draft.location_name,
        station_id=station_metadata.station_id,
        latitude=station_metadata.latitude,
        longitude=station_metadata.longitude,
        timezone=station_metadata.timezone,
        observation_date=draft.observation_date,
        observation_window_local=draft.observation_window_local,
        metric=draft.metric,
        unit=draft.unit,
        authoritative_source=draft.authoritative_source,
        fallback_sources=list(draft.fallback_sources),
        rounding_rule=draft.rounding_rule,
        inclusive_bounds=draft.inclusive_bounds,
        spec_version=version,
    )


def build_resolution_spec_via_station_mapper(
    draft: Rule2SpecDraft,
    *,
    mapper: StationMapper,
    con,
) -> ResolutionSpec:
    station_metadata = mapper.resolve_from_spec_inputs(
        con,
        market_id=draft.market_id,
        location_name=draft.location_name,
        authoritative_source=draft.authoritative_source,
    )
    return build_resolution_spec_from_draft(draft, station_metadata=station_metadata)


def build_weather_market_spec_record(
    draft: Rule2SpecDraft,
    *,
    station_metadata: StationMetadata,
    spec_version: str | None = None,
) -> WeatherMarketSpecRecord:
    resolution_spec = build_resolution_spec_from_draft(
        draft,
        station_metadata=station_metadata,
        spec_version=spec_version,
    )
    return WeatherMarketSpecRecord(
        market_id=resolution_spec.market_id,
        condition_id=resolution_spec.condition_id,
        location_name=resolution_spec.location_name,
        station_id=resolution_spec.station_id,
        latitude=resolution_spec.latitude,
        longitude=resolution_spec.longitude,
        timezone=resolution_spec.timezone,
        observation_date=resolution_spec.observation_date,
        observation_window_local=resolution_spec.observation_window_local,
        metric=resolution_spec.metric,
        unit=resolution_spec.unit,
        bucket_min_value=draft.bucket_min_value,
        bucket_max_value=draft.bucket_max_value,
        authoritative_source=resolution_spec.authoritative_source,
        fallback_sources=list(resolution_spec.fallback_sources),
        rounding_rule=resolution_spec.rounding_rule,
        inclusive_bounds=resolution_spec.inclusive_bounds,
        spec_version=resolution_spec.spec_version,
        parse_confidence=draft.parse_confidence,
        risk_flags=list(draft.risk_flags),
    )


def build_weather_market_spec_record_via_station_mapper(
    draft: Rule2SpecDraft,
    *,
    mapper: StationMapper,
    con,
) -> WeatherMarketSpecRecord:
    station_metadata = mapper.resolve_from_spec_inputs(
        con,
        market_id=draft.market_id,
        location_name=draft.location_name,
        authoritative_source=draft.authoritative_source,
    )
    return build_weather_market_spec_record(draft, station_metadata=station_metadata)


def enqueue_weather_market_spec_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    specs: list[WeatherMarketSpecRecord],
    run_id: str | None = None,
    observed_at: datetime | None = None,
) -> str | None:
    if not specs:
        return None
    now = (observed_at or datetime.now(UTC).replace(tzinfo=None)).replace(microsecond=0)
    rows = [weather_market_spec_to_row(spec, observed_at=now) for spec in specs]
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="weather.weather_market_specs",
        pk_cols=["market_id"],
        columns=list(WEATHER_MARKET_SPEC_COLUMNS),
        rows=rows,
        run_id=run_id,
    )


def weather_market_spec_to_row(spec: WeatherMarketSpecRecord, *, observed_at: datetime) -> list[Any]:
    ts = observed_at.isoformat(sep=" ", timespec="seconds")
    return [
        spec.market_id,
        spec.condition_id,
        spec.location_name,
        spec.station_id,
        spec.latitude,
        spec.longitude,
        spec.timezone,
        spec.observation_date.isoformat(),
        spec.observation_window_local,
        spec.metric,
        spec.unit,
        spec.bucket_min_value,
        spec.bucket_max_value,
        spec.authoritative_source,
        safe_json_dumps(spec.fallback_sources),
        spec.rounding_rule,
        spec.inclusive_bounds,
        spec.spec_version,
        spec.parse_confidence,
        safe_json_dumps(spec.risk_flags),
        ts,
        ts,
    ]


def build_spec_version(draft: Rule2SpecDraft, *, station_metadata: StationMetadata) -> str:
    payload = build_rule2spec_review_payload(draft)
    payload.update(
        {
            "station_id": station_metadata.station_id,
            "latitude": station_metadata.latitude,
            "longitude": station_metadata.longitude,
            "timezone": station_metadata.timezone,
        }
    )
    return stable_object_id("spec", payload)


def _extract_authoritative_source(rules: str) -> str:
    lower = rules.lower()
    if "weather.com" in lower:
        return "weather.com"
    if "national weather service" in lower or "weather.gov" in lower or "nws" in lower:
        return "nws"
    if "open-meteo" in lower:
        return "open-meteo"
    return "unknown"


def _fallback_sources_for(authoritative_source: str) -> list[str]:
    if authoritative_source == "weather.com":
        return ["nws", "open-meteo"]
    if authoritative_source == "nws":
        return ["open-meteo"]
    if authoritative_source == "open-meteo":
        return ["nws"]
    return ["nws", "open-meteo"]


def _parse_threshold_date(raw_date: str | None, end_date: datetime | None, close_time: datetime | None) -> date:
    if raw_date:
        normalized = re.sub(r"(\d)(st|nd|rd|th)", r"\1", raw_date)
        return datetime.strptime(normalized, "%B %d, %Y").date()
    if end_date is not None:
        return end_date.date()
    if close_time is not None:
        return close_time.date()
    raise ValueError("threshold weather market is missing a parseable observation date")


def _parse_range_date(raw_date: str | None, end_date: datetime | None, close_time: datetime | None) -> date:
    if raw_date:
        normalized = re.sub(r"(\d)(st|nd|rd|th)", r"\1", raw_date)
        try:
            return datetime.strptime(normalized, "%B %d, %Y").date()
        except ValueError:
            pass
        month_day = re.fullmatch(r"([A-Za-z]+)\s+(\d{1,2})", normalized)
        if month_day:
            source = end_date or close_time
            if source is None:
                raise ValueError("range weather market date missing year and no end_date/close_time available")
            month_name, day = month_day.groups()
            return datetime.strptime(f"{month_name} {int(day):02d}, {source.year}", "%B %d, %Y").date()
        raise ValueError(f"unsupported range weather market date: {raw_date}")
    if end_date is not None:
        return end_date.date()
    if close_time is not None:
        return close_time.date()
    raise ValueError("range weather market is missing a parseable observation date")


def _normalize_metric_name(raw_metric: str) -> str:
    lowered = raw_metric.lower()
    return "high" if lowered.startswith("high") else "low"


def _default_upper_bound(unit: str) -> float:
    return 200.0 if unit == "fahrenheit" else 100.0


def _default_lower_bound(unit: str) -> float:
    return -100.0 if unit == "fahrenheit" else -80.0


def _extract_rounding_rule(rules: str) -> str:
    lower = rules.lower()
    if "nearest whole degree" in lower or "nearest degree" in lower:
        return "round_half_away_from_zero"
    if "rounded" in lower:
        return "round_half_up"
    return "identity"


def _optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    return bool(value)


def _json_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return [item.strip() for item in text.split(",") if item.strip()]
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
    return [str(value)]


def _json_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def _to_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
