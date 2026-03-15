from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from asterion_core.contracts import StationMetadata
from asterion_core.contracts import stable_object_id
from asterion_core.storage.os_queue import enqueue_upsert_rows_v1
from asterion_core.storage.utils import safe_json_dumps
from asterion_core.storage.write_queue import WriteQueueConfig


WEATHER_STATION_MAP_COLUMNS = [
    "map_id",
    "market_id",
    "location_name",
    "location_key",
    "station_id",
    "station_name",
    "latitude",
    "longitude",
    "timezone",
    "source",
    "authoritative_source",
    "is_override",
    "mapping_method",
    "mapping_confidence",
    "override_reason",
    "metadata_json",
    "created_at",
    "updated_at",
]


@dataclass(frozen=True)
class StationMappingRecord:
    map_id: str
    market_id: str | None
    location_name: str
    location_key: str
    station_id: str
    station_name: str | None
    latitude: float
    longitude: float
    timezone: str
    source: str
    authoritative_source: str | None
    is_override: bool
    mapping_method: str
    mapping_confidence: float
    override_reason: str | None
    metadata: dict[str, Any]

    def to_station_metadata(self) -> StationMetadata:
        return StationMetadata(
            station_id=self.station_id,
            location_name=self.location_name,
            latitude=self.latitude,
            longitude=self.longitude,
            timezone=self.timezone,
            source=self.source,
        )


class StationMapper:
    def resolve_from_spec_inputs(
        self,
        con,
        *,
        market_id: str,
        location_name: str,
        authoritative_source: str,
    ) -> StationMetadata:
        record = self.resolve_record_from_spec_inputs(
            con,
            market_id=market_id,
            location_name=location_name,
            authoritative_source=authoritative_source,
        )
        return record.to_station_metadata()

    def resolve_record_from_spec_inputs(
        self,
        con,
        *,
        market_id: str,
        location_name: str,
        authoritative_source: str,
    ) -> StationMappingRecord:
        record = self._load_preferred_mapping(
            con,
            market_id=market_id,
            location_name=location_name,
            authoritative_source=authoritative_source,
        )
        if record is None:
            raise LookupError(f"station mapping not found for market_id={market_id} location_name={location_name!r}")
        return record

    def get_station_metadata(self, con, *, station_id: str) -> StationMetadata:
        row = con.execute(
            """
            SELECT
                map_id,
                market_id,
                location_name,
                location_key,
                station_id,
                station_name,
                latitude,
                longitude,
                timezone,
                source,
                authoritative_source,
                is_override,
                mapping_method,
                mapping_confidence,
                override_reason,
                metadata_json
            FROM weather.weather_station_map
            WHERE station_id = ?
            ORDER BY is_override DESC, updated_at DESC, created_at DESC
            LIMIT 1
            """,
            [station_id],
        ).fetchone()
        if row is None:
            raise LookupError(f"station metadata not found for station_id={station_id}")
        return _row_to_station_mapping(row).to_station_metadata()

    def _load_preferred_mapping(
        self,
        con,
        *,
        market_id: str,
        location_name: str,
        authoritative_source: str,
    ) -> StationMappingRecord | None:
        location_key = normalize_location_key(location_name)
        params = [market_id, location_key, authoritative_source, location_key]
        row = con.execute(
            """
            SELECT
                map_id,
                market_id,
                location_name,
                location_key,
                station_id,
                station_name,
                latitude,
                longitude,
                timezone,
                source,
                authoritative_source,
                is_override,
                mapping_method,
                mapping_confidence,
                override_reason,
                metadata_json
            FROM weather.weather_station_map
            WHERE
                (market_id = ?)
                OR (market_id IS NULL AND location_key = ? AND (authoritative_source = ? OR authoritative_source IS NULL))
                OR (market_id IS NULL AND location_key = ?)
            ORDER BY
                CASE WHEN market_id = ? THEN 0 ELSE 1 END,
                is_override DESC,
                updated_at DESC,
                created_at DESC
            LIMIT 1
            """,
            [*params, market_id],
        ).fetchone()
        if row is None:
            return None
        return _row_to_station_mapping(row)


def normalize_location_key(location_name: str) -> str:
    return " ".join(location_name.strip().lower().replace(",", " ").split())


def build_station_mapping_record(
    *,
    location_name: str,
    station_id: str,
    latitude: float,
    longitude: float,
    timezone: str,
    source: str,
    market_id: str | None = None,
    station_name: str | None = None,
    authoritative_source: str | None = None,
    is_override: bool = False,
    mapping_method: str | None = None,
    mapping_confidence: float = 1.0,
    override_reason: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> StationMappingRecord:
    payload = {
        "authoritative_source": authoritative_source,
        "location_key": normalize_location_key(location_name),
        "location_name": location_name,
        "market_id": market_id,
        "source": source,
        "station_id": station_id,
        "timezone": timezone,
    }
    return StationMappingRecord(
        map_id=stable_object_id("stmap", payload),
        market_id=market_id,
        location_name=location_name,
        location_key=normalize_location_key(location_name),
        station_id=station_id,
        station_name=station_name,
        latitude=float(latitude),
        longitude=float(longitude),
        timezone=timezone,
        source=source,
        authoritative_source=authoritative_source,
        is_override=bool(is_override),
        mapping_method=mapping_method or ("market_override" if market_id or is_override else "location_default"),
        mapping_confidence=max(0.0, min(1.0, float(mapping_confidence))),
        override_reason=override_reason,
        metadata=dict(metadata or {}),
    )


def enqueue_station_mapping_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    mappings: list[StationMappingRecord],
    run_id: str | None = None,
    observed_at: datetime | None = None,
) -> str | None:
    if not mappings:
        return None
    now = (observed_at or datetime.now(UTC).replace(tzinfo=None)).replace(microsecond=0)
    rows = [station_mapping_to_row(item, observed_at=now) for item in mappings]
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="weather.weather_station_map",
        pk_cols=["map_id"],
        columns=list(WEATHER_STATION_MAP_COLUMNS),
        rows=rows,
        run_id=run_id,
    )


def station_mapping_to_row(mapping: StationMappingRecord, *, observed_at: datetime) -> list[Any]:
    ts = observed_at.isoformat(sep=" ", timespec="seconds")
    return [
        mapping.map_id,
        mapping.market_id,
        mapping.location_name,
        mapping.location_key,
        mapping.station_id,
        mapping.station_name,
        mapping.latitude,
        mapping.longitude,
        mapping.timezone,
        mapping.source,
        mapping.authoritative_source,
        mapping.is_override,
        mapping.mapping_method,
        mapping.mapping_confidence,
        mapping.override_reason,
        safe_json_dumps(mapping.metadata),
        ts,
        ts,
    ]


def _row_to_station_mapping(row: Any) -> StationMappingRecord:
    metadata_json = row[15]
    if isinstance(metadata_json, str):
        try:
            metadata = json.loads(metadata_json)
        except json.JSONDecodeError:
            metadata = {}
    else:
        metadata = {}
    return StationMappingRecord(
        map_id=row[0],
        market_id=row[1],
        location_name=row[2],
        location_key=row[3],
        station_id=row[4],
        station_name=row[5],
        latitude=float(row[6]),
        longitude=float(row[7]),
        timezone=row[8],
        source=row[9],
        authoritative_source=row[10],
        is_override=bool(row[11]),
        mapping_method=str(row[12] or ("market_override" if row[1] or row[11] else "location_default")),
        mapping_confidence=float(row[13] if row[13] is not None else 1.0),
        override_reason=str(row[14]) if row[14] is not None else None,
        metadata=metadata,
    )
