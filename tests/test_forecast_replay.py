from __future__ import annotations

import importlib.util
import json
import os
import tempfile
import unittest
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import patch

from asterion_core.contracts import (
    ForecastReplayRequest,
    ForecastReplayResult,
    ForecastRunRecord,
    WatchOnlySnapshotRecord,
    WeatherFairValueRecord,
    WeatherMarket,
    WeatherMarketSpecRecord,
    build_forecast_cache_key,
)
from asterion_core.storage.db_migrate import MigrationConfig, apply_migrations
from asterion_core.storage.write_queue import WriteQueueConfig
from asterion_core.storage.writerd import process_one
from domains.weather.forecast import (
    AdapterRouter,
    ForecastService,
    InMemoryForecastCache,
    OpenMeteoAdapter,
    build_forecast_replay_diff_records,
    build_forecast_replay_record,
    build_forecast_replay_request,
    build_forecast_run_record,
    enqueue_forecast_replay_diff_upserts,
    enqueue_forecast_replay_upserts,
    enqueue_forecast_run_upserts,
    load_original_pricing_outputs,
    load_replay_inputs,
    recompute_pricing_outputs,
    run_forecast_replay,
)
from domains.weather.pricing import (
    build_binary_fair_values,
    build_watch_only_snapshot,
    enqueue_fair_value_upserts,
    enqueue_watch_only_snapshot_upserts,
)
from domains.weather.scout import run_weather_market_discovery
from domains.weather.spec import (
    StationMapper,
    build_station_mapping_record,
    build_weather_market_spec_record_via_station_mapper,
    enqueue_station_mapping_upserts,
    enqueue_weather_market_spec_upserts,
    load_weather_markets_for_rule2spec,
    parse_rule2spec_draft,
)
from domains.weather.forecast.adapters import build_normal_distribution


HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


class _RoutingClient:
    def __init__(self, routes: dict[str, object]) -> None:
        self.routes = routes

    def get_json(self, url: str, *, context: dict) -> object:
        for pattern, payload in self.routes.items():
            if pattern in url:
                if isinstance(payload, Exception):
                    raise payload
                return payload
        raise AssertionError(f"unexpected url: {url}")


class _GammaClient:
    def __init__(self, pages: list[list[dict]]) -> None:
        self._pages = pages

    def get_json(self, url: str, *, context: dict) -> dict:
        page = int(context["page"])
        return {"markets": self._pages[page] if page < len(self._pages) else []}


def _raw_weather_market() -> dict:
    return {
        "id": "mkt_weather_1",
        "conditionId": "cond_weather_1",
        "question": "Will the high temperature in New York City on March 8, 2026 be 50-59°F?",
        "description": "Template weather market",
        "rules": "Resolve to Yes if the observed high temperature is within range.",
        "slug": "nyc-high-temp-mar-8",
        "active": True,
        "closed": False,
        "archived": False,
        "acceptingOrders": True,
        "enableOrderBook": True,
        "tags": ["Weather", "Temperature"],
        "outcomes": "[\"Yes\", \"No\"]",
        "clobTokenIds": "[\"tok_yes\", \"tok_no\"]",
        "closeTime": "2026-03-08T23:59:59Z",
        "endDate": "2026-03-08T23:59:59Z",
        "createdAt": "2026-03-01T00:00:00Z",
        "event": {"id": "evt_weather_1", "category": "Weather"},
    }


def _weather_market() -> WeatherMarket:
    return WeatherMarket(
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        event_id="evt_weather_1",
        slug="nyc-high-temp-mar-8",
        title="NYC high temp",
        description="Template weather market",
        rules="Resolve to Yes if the observed high temperature is within range.",
        status="active",
        active=True,
        closed=False,
        archived=False,
        accepting_orders=True,
        enable_order_book=True,
        tags=["Weather", "Temperature"],
        outcomes=["Yes", "No"],
        token_ids=["tok_yes", "tok_no"],
        close_time=datetime(2026, 3, 8, 23, 59, 59),
        end_date=datetime(2026, 3, 8, 23, 59, 59),
        raw_market={"id": "mkt_weather_1"},
    )


def _weather_spec() -> WeatherMarketSpecRecord:
    return WeatherMarketSpecRecord(
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        location_name="New York City",
        station_id="KNYC",
        latitude=40.7128,
        longitude=-74.0060,
        timezone="America/New_York",
        observation_date=date(2026, 3, 8),
        observation_window_local="daily_max",
        metric="temperature_max",
        unit="fahrenheit",
        bucket_min_value=50.0,
        bucket_max_value=59.0,
        authoritative_source="weather.com",
        fallback_sources=["nws", "open-meteo"],
        rounding_rule="identity",
        inclusive_bounds=True,
        spec_version="spec_abc123",
        parse_confidence=0.95,
        risk_flags=[],
    )


def _forecast_run(*, run_id: str, confidence: float = 1.0, temperature: int = 55) -> ForecastRunRecord:
    distribution = build_normal_distribution(float(temperature), 3.0)
    return ForecastRunRecord(
        run_id=run_id,
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        station_id="KNYC",
        source="openmeteo",
        model_run="2026-03-07T12:00Z",
        forecast_target_time=datetime(2026, 3, 7, 12, 0, tzinfo=UTC),
        observation_date=date(2026, 3, 8),
        metric="temperature_max",
        latitude=40.7128,
        longitude=-74.0060,
        timezone="America/New_York",
        spec_version="spec_abc123",
        cache_key="mkt_weather_1|KNYC|spec_abc123|openmeteo|2026-03-07T12:00Z|2026-03-07T12:00:00+00:00",
        source_trace=["openmeteo"],
        fallback_used=False,
        from_cache=False,
        confidence=confidence,
        forecast_payload={"temperature_distribution": distribution},
        raw_payload={"daily": {"temperature_2m_max": [float(temperature)]}},
    )


def _replay_request() -> ForecastReplayRequest:
    return ForecastReplayRequest(
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        station_id="KNYC",
        source="openmeteo",
        model_run="2026-03-07T12:00Z",
        forecast_target_time=datetime(2026, 3, 7, 12, 0, tzinfo=UTC),
        spec_version="spec_abc123",
        replay_reason="unit_test",
    )


def _result_from_run(run: ForecastRunRecord) -> ForecastReplayResult:
    market = _weather_market()
    spec = _weather_spec()
    fair_values = build_binary_fair_values(market=market, spec=spec, forecast_run=run)
    snapshots = [
        build_watch_only_snapshot(fair_value=fair_values[0], reference_price=0.55, threshold_bps=500, agent_review_status="passed"),
        build_watch_only_snapshot(fair_value=fair_values[1], reference_price=0.45, threshold_bps=500, agent_review_status="passed"),
    ]
    return ForecastReplayResult(
        replay_id="freplay_unit",
        request=_replay_request(),
        forecast_run=run,
        fair_values=fair_values,
        watch_only_snapshots=snapshots,
    )


class ForecastReplayUnitTest(unittest.TestCase):
    def test_forecast_replay_recomputes_fair_values_and_snapshots(self) -> None:
        fair_values, snapshots = recompute_pricing_outputs(
            market=_weather_market(),
            spec=_weather_spec(),
            forecast_run=_forecast_run(run_id="frun_replayed"),
            original_snapshots=[
                WatchOnlySnapshotRecord(
                    snapshot_id="snap_yes",
                    fair_value_id="fv_yes",
                    run_id="frun_orig",
                    market_id="mkt_weather_1",
                    condition_id="cond_weather_1",
                    token_id="tok_yes",
                    outcome="YES",
                    reference_price=0.55,
                    fair_value=1.0,
                    edge_bps=4500,
                    threshold_bps=500,
                    decision="TAKE",
                    side="BUY",
                    rationale="unit",
                    pricing_context={"agent_review_status": "passed", "live_prereq_status": "shadow_aligned"},
                ),
                WatchOnlySnapshotRecord(
                    snapshot_id="snap_no",
                    fair_value_id="fv_no",
                    run_id="frun_orig",
                    market_id="mkt_weather_1",
                    condition_id="cond_weather_1",
                    token_id="tok_no",
                    outcome="NO",
                    reference_price=0.45,
                    fair_value=0.0,
                    edge_bps=-4500,
                    threshold_bps=500,
                    decision="TAKE",
                    side="SELL",
                    rationale="unit",
                    pricing_context={"agent_review_status": "passed", "live_prereq_status": "shadow_aligned"},
                ),
            ],
        )
        self.assertEqual(len(fair_values), 2)
        self.assertEqual(len(snapshots), 2)
        self.assertEqual({item.outcome for item in fair_values}, {"YES", "NO"})
        self.assertEqual({item.side for item in snapshots}, {"BUY", "SELL"})

    def test_forecast_replay_diff_marks_match_for_identical_outputs(self) -> None:
        original = _forecast_run(run_id="frun_orig")
        replayed = _forecast_run(run_id="frun_replayed")
        replayed = ForecastRunRecord(**{**replayed.__dict__, "run_id": "frun_replayed"})
        result = _result_from_run(replayed)
        original_result = _result_from_run(original)

        diffs = build_forecast_replay_diff_records(
            replay_result=result,
            original_run=original,
            original_fair_values=original_result.fair_values,
            original_watch_only_snapshots=original_result.watch_only_snapshots,
        )

        statuses = {item.entity_type: item.status for item in diffs if item.entity_type == "forecast_run"}
        self.assertEqual(statuses["forecast_run"], "MATCH")
        self.assertEqual([item.status for item in diffs if item.entity_type == "fair_value"], ["MATCH", "MATCH"])
        self.assertEqual([item.status for item in diffs if item.entity_type == "watch_only_snapshot"], ["MATCH", "MATCH"])

    def test_forecast_replay_diff_marks_different_when_distribution_changes(self) -> None:
        original = _forecast_run(run_id="frun_orig", confidence=1.0, temperature=55)
        replayed = _forecast_run(run_id="frun_replayed", confidence=0.9, temperature=58)
        result = _result_from_run(replayed)
        original_result = _result_from_run(original)

        diffs = build_forecast_replay_diff_records(
            replay_result=result,
            original_run=original,
            original_fair_values=original_result.fair_values,
            original_watch_only_snapshots=original_result.watch_only_snapshots,
        )

        self.assertEqual(next(item for item in diffs if item.entity_type == "forecast_run").status, "DIFFERENT")
        self.assertIn("temperature_distribution", next(item for item in diffs if item.entity_type == "forecast_run").diff_summary_json["changed_fields"])


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required for forecast replay tests")
class ForecastReplayDuckDBTest(unittest.TestCase):
    def test_build_forecast_replay_request_uses_canonical_replay_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path, queue_path = _bootstrap_weather_state(tmpdir)
            request = _build_request_from_db(db_path)
            self.assertEqual(request.source, "openmeteo")
            self.assertEqual(
                request.replay_key,
                build_forecast_cache_key(
                    market_id="mkt_weather_1",
                    station_id="KNYC",
                    spec_version=request.spec_version,
                    source="openmeteo",
                    model_run="2026-03-07T12:00Z",
                    forecast_target_time=datetime(2026, 3, 7, 12, 0, tzinfo=UTC),
                ),
            )

    def test_forecast_replay_recomputes_same_distribution_from_persisted_spec(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path, queue_path = _bootstrap_weather_state(tmpdir)
            request = _build_request_from_db(db_path)
            replay_result, _, _ = _run_replay(db_path, queue_path, request, temperature=55.0)
            self.assertEqual(
                replay_result.forecast_run.forecast_payload["temperature_distribution"],
                build_normal_distribution(55.0, 3.0),
            )
            self.assertEqual(len(replay_result.fair_values), 2)
            self.assertEqual(len(replay_result.watch_only_snapshots), 2)

    def test_replay_persistence_writes_replays_and_diffs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path, queue_path = _bootstrap_weather_state(tmpdir)
            request = _build_request_from_db(db_path)
            replay_result, replay_record, diff_records = _run_replay(db_path, queue_path, request, temperature=55.0)

            self.assertEqual(replay_record.replayed_run_id, replay_result.forecast_run.run_id)
            self.assertTrue(diff_records)

            import duckdb

            con = duckdb.connect(db_path, read_only=True)
            try:
                replay_row = con.execute(
                    """
                    SELECT replay_id, replay_key, replay_reason, original_run_id, replayed_run_id
                    FROM weather.weather_forecast_replays
                    WHERE replay_id = ?
                    """,
                    [replay_record.replay_id],
                ).fetchone()
                diff_rows = con.execute(
                    """
                    SELECT entity_type, status, diff_summary_json
                    FROM weather.weather_forecast_replay_diffs
                    WHERE replay_id = ?
                    ORDER BY entity_type, entity_key
                    """,
                    [replay_record.replay_id],
                ).fetchall()
            finally:
                con.close()

            self.assertEqual(replay_row[0], replay_record.replay_id)
            self.assertEqual(replay_row[1], request.replay_key)
            self.assertEqual(replay_row[2], "cold_path_replay")
            self.assertEqual(replay_row[4], replay_result.forecast_run.run_id)
            self.assertTrue(any(row[0] == "forecast_run" and row[1] == "MATCH" for row in diff_rows))
            self.assertTrue(all(json.loads(row[2]) for row in diff_rows))


def _bootstrap_weather_state(tmpdir: str) -> tuple[str, str]:
    db_path = str(Path(tmpdir) / "asterion.duckdb")
    queue_path = str(Path(tmpdir) / "write_queue.sqlite")
    migrations_dir = str(Path(__file__).resolve().parents[1] / "sql" / "migrations")

    with patch.dict(
        os.environ,
        {
            "ASTERION_STRICT_SINGLE_WRITER": "1",
            "ASTERION_DB_ROLE": "writer",
            "WRITERD": "1",
        },
        clear=False,
    ):
        apply_migrations(MigrationConfig(db_path=db_path, migrations_dir=migrations_dir))

    allow_tables = ",".join(
        [
            "weather.weather_markets",
            "weather.weather_station_map",
            "weather.weather_market_specs",
            "weather.weather_forecast_runs",
            "weather.weather_forecast_replays",
            "weather.weather_forecast_replay_diffs",
            "weather.weather_fair_values",
            "weather.weather_watch_only_snapshots",
        ]
    )

    gamma_client = _GammaClient([[_raw_weather_market()]])
    queue_cfg = WriteQueueConfig(path=queue_path)
    discovery = run_weather_market_discovery(
        base_url="https://gamma.example",
        markets_endpoint="/markets",
        page_limit=100,
        max_pages=1,
        sleep_s=0.0,
        active_only=True,
        closed=False,
        archived=False,
        client=gamma_client,
        queue_cfg=queue_cfg,
        run_id="run_market_discovery",
    )
    assert discovery.discovered_count == 1

    station_mapping = build_station_mapping_record(
        market_id="mkt_weather_1",
        location_name="New York City",
        station_id="KNYC",
        station_name="Central Park",
        latitude=40.7128,
        longitude=-74.0060,
        timezone="America/New_York",
        source="operator_override",
        authoritative_source="weather.com",
        is_override=True,
        metadata={"kind": "manual"},
    )
    enqueue_station_mapping_upserts(queue_cfg, mappings=[station_mapping], run_id="run_station_map")
    _drain_queue(db_path=db_path, queue_path=queue_path, allow_tables=allow_tables)

    from asterion_core.storage.database import DuckDBConfig, connect_duckdb

    with patch.dict(
        os.environ,
        {
            "ASTERION_STRICT_SINGLE_WRITER": "1",
            "ASTERION_DB_ROLE": "reader",
            "WRITERD": "0",
        },
        clear=False,
    ):
        con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
        try:
            weather_market = load_weather_markets_for_rule2spec(con, active_only=True, limit=10)[0]
            draft = parse_rule2spec_draft(weather_market)
            mapper = StationMapper()
            spec_record = build_weather_market_spec_record_via_station_mapper(draft, mapper=mapper, con=con)
        finally:
            con.close()

    enqueue_weather_market_spec_upserts(queue_cfg, specs=[spec_record], run_id="run_spec")
    _drain_queue(db_path=db_path, queue_path=queue_path, allow_tables=allow_tables)

    service = ForecastService(
        adapter_router=AdapterRouter([OpenMeteoAdapter(client=_RoutingClient({"api.open-meteo.com": {"daily": {"temperature_2m_max": [55.0]}}}))]),
        cache=InMemoryForecastCache(),
    )
    distribution = service.get_forecast(
        _resolution_spec_from_spec_record(spec_record),
        source="openmeteo",
        model_run="2026-03-07T12:00Z",
        forecast_target_time=datetime(2026, 3, 7, 12, 0, tzinfo=UTC),
    )
    run_record = build_forecast_run_record(distribution)
    fair_values = build_binary_fair_values(
        market=_weather_market(),
        spec=_weather_spec(),
        forecast_run=run_record,
    )
    snapshots = [
        build_watch_only_snapshot(fair_value=fair_values[0], reference_price=0.55, threshold_bps=500, agent_review_status="passed"),
        build_watch_only_snapshot(fair_value=fair_values[1], reference_price=0.45, threshold_bps=500, agent_review_status="passed"),
    ]

    enqueue_forecast_run_upserts(queue_cfg, forecast_runs=[run_record], run_id="run_forecast")
    enqueue_fair_value_upserts(queue_cfg, fair_values=fair_values, run_id="run_fair_values")
    enqueue_watch_only_snapshot_upserts(queue_cfg, snapshots=snapshots, run_id="run_watch_only")
    _drain_queue(db_path=db_path, queue_path=queue_path, allow_tables=allow_tables)

    return db_path, queue_path
def _resolution_spec_from_spec_record(spec_record: WeatherMarketSpecRecord):
    from asterion_core.contracts import ResolutionSpec

    return ResolutionSpec(
        market_id=spec_record.market_id,
        condition_id=spec_record.condition_id,
        location_name=spec_record.location_name,
        station_id=spec_record.station_id,
        latitude=spec_record.latitude,
        longitude=spec_record.longitude,
        timezone=spec_record.timezone,
        observation_date=spec_record.observation_date,
        observation_window_local=spec_record.observation_window_local,
        metric=spec_record.metric,
        unit=spec_record.unit,
        authoritative_source=spec_record.authoritative_source,
        fallback_sources=list(spec_record.fallback_sources),
        rounding_rule=spec_record.rounding_rule,
        inclusive_bounds=spec_record.inclusive_bounds,
        spec_version=spec_record.spec_version,
    )


def _build_request_from_db(db_path: str) -> ForecastReplayRequest:
    import duckdb

    con = duckdb.connect(db_path, read_only=True)
    try:
        spec_version = con.execute(
            "SELECT spec_version FROM weather.weather_market_specs WHERE market_id = ?",
            ["mkt_weather_1"],
        ).fetchone()[0]
        return build_forecast_replay_request(
            con,
            market_id="mkt_weather_1",
            station_id="KNYC",
            source="open-meteo",
            model_run="2026-03-07T12:00Z",
            forecast_target_time=datetime(2026, 3, 7, 12, 0, tzinfo=UTC),
            spec_version=spec_version,
            replay_reason="cold_path_replay",
        )
    finally:
        con.close()


def _run_replay(db_path: str, queue_path: str, request: ForecastReplayRequest, *, temperature: float):
    allow_tables = ",".join(
        [
            "weather.weather_forecast_runs",
            "weather.weather_forecast_replays",
            "weather.weather_forecast_replay_diffs",
            "weather.weather_fair_values",
            "weather.weather_watch_only_snapshots",
        ]
    )

    import duckdb

    con = duckdb.connect(db_path, read_only=True)
    try:
        replay_result = run_forecast_replay(
            con,
            adapter_router=AdapterRouter([OpenMeteoAdapter(client=_RoutingClient({"api.open-meteo.com": {"daily": {"temperature_2m_max": [temperature]}}}))]),
            cache=InMemoryForecastCache(),
            market_id=request.market_id,
            station_id=request.station_id,
            source=request.source,
            model_run=request.model_run,
            forecast_target_time=request.forecast_target_time,
            spec_version=request.spec_version,
            replay_reason=request.replay_reason,
        )
        _, original_run, _ = load_replay_inputs(con, request)
        original_fair_values, original_snapshots = load_original_pricing_outputs(con, run_id=original_run.run_id)
    finally:
        con.close()

    replay_record = build_forecast_replay_record(replay_result, original_run_id=original_run.run_id)
    diff_records = build_forecast_replay_diff_records(
        replay_result=replay_result,
        original_run=original_run,
        original_fair_values=original_fair_values,
        original_watch_only_snapshots=original_snapshots,
    )

    queue_cfg = WriteQueueConfig(path=queue_path)
    enqueue_forecast_run_upserts(queue_cfg, forecast_runs=[replay_result.forecast_run], run_id="run_forecast_replay")
    enqueue_fair_value_upserts(queue_cfg, fair_values=replay_result.fair_values, run_id="run_forecast_replay")
    enqueue_watch_only_snapshot_upserts(queue_cfg, snapshots=replay_result.watch_only_snapshots, run_id="run_forecast_replay")
    enqueue_forecast_replay_upserts(queue_cfg, replays=[replay_record], run_id="run_forecast_replay")
    enqueue_forecast_replay_diff_upserts(queue_cfg, diffs=diff_records, run_id="run_forecast_replay")
    _drain_queue(db_path=db_path, queue_path=queue_path, allow_tables=allow_tables)
    return replay_result, replay_record, diff_records


def _drain_queue(*, db_path: str, queue_path: str, allow_tables: str) -> None:
    with patch.dict(
        os.environ,
        {
            "ASTERION_DB_PATH": db_path,
            "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables,
        },
        clear=False,
    ):
        while process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False):
            pass


if __name__ == "__main__":
    unittest.main()
