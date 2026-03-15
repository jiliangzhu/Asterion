#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from agents.common import enqueue_agent_artifact_upserts
from agents.common.client import build_agent_client_from_env
from agents.weather.rule2spec_agent import Rule2SpecAgentRequest, run_rule2spec_agent_review
from asterion_core.contracts import StationMetadata
from asterion_core.storage.database import DuckDBConfig, connect_duckdb
from asterion_core.storage.db_migrate import MigrationConfig, apply_migrations
from asterion_core.storage.write_queue import WriteQueueConfig
from asterion_core.storage.writerd import process_one
from dagster_asterion.handlers import run_weather_forecast_refresh, run_weather_spec_sync
from dagster_asterion.resources import HttpJsonClient
from domains.weather.forecast import AdapterRouter, ForecastService, InMemoryForecastCache, NWSAdapter, OpenMeteoAdapter
from domains.weather.pricing import (
    build_binary_fair_values,
    build_watch_only_snapshot,
    enqueue_fair_value_upserts,
    enqueue_watch_only_snapshot_upserts,
    load_forecast_run,
    load_weather_market,
    load_weather_market_spec,
)
from domains.weather.scout import enqueue_weather_market_upserts, normalize_weather_market, run_weather_market_discovery
from domains.weather.spec import (
    StationMapper,
    build_weather_market_spec_record_via_station_mapper,
    build_station_mapping_record,
    enqueue_weather_market_spec_upserts,
    enqueue_station_mapping_upserts,
    load_weather_markets_for_rule2spec,
    normalize_location_key,
    parse_rule2spec_draft,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "data" / "dev" / "real_weather_chain"
TARGET_THRESHOLD_BPS = 300
TARGET_SOURCE_REQUESTED = "weather.com"
DEFAULT_RECENT_WITHIN_DAYS = 14
DEFAULT_HORIZON_SEQUENCE = (14, 30, 60, 90)
WEATHER_MARKET_DISCOVERY_BASE_URL = "https://gamma-api.polymarket.com"
WEATHER_MARKET_DISCOVERY_ENDPOINT = "/events"
WEATHER_MARKET_DISCOVERY_TAG_SLUG = "weather"
WEATHER_MARKET_DISCOVERY_PAGE_LIMIT = 100
WEATHER_MARKET_DISCOVERY_MAX_PAGES = 5
WEATHER_PAGE_URL = "https://polymarket.com/markets/weather"
WEATHER_STATION_CATALOG_PATH = ROOT / "config" / "weather_station_smoke_catalog.json"

FROZEN_TARGET_MARKET_ID = "239139"
FROZEN_TARGET_CONDITION_ID = "0xa9f2f1f8c5d7b0b9e8c7d6a5f4e3d2c1b0a99887"
FROZEN_TARGET_QUESTION = "Will the high temperature in New York's Central Park be 60°F or higher on November 2, 2021?"

FROZEN_REAL_GAMMA_MARKET = {
    "id": FROZEN_TARGET_MARKET_ID,
    "conditionId": FROZEN_TARGET_CONDITION_ID,
    "question": FROZEN_TARGET_QUESTION,
    "description": "Historical real Gamma weather market snapshot used for deterministic smoke validation.",
    "rules": "Resolve using weather.com official station high temperature for New York's Central Park on November 2, 2021.",
    "slug": "central-park-high-temp-60f-or-higher-nov-2-2021",
    "active": False,
    "closed": True,
    "archived": False,
    "acceptingOrders": False,
    "enableOrderBook": True,
    "tags": ["Weather", "Temperature"],
    "outcomes": "[\"Yes\", \"No\"]",
    "clobTokenIds": "[\"tok_yes_threshold\", \"tok_no_threshold\"]",
    "outcomePrices": "[\"0.0000001761935909832323804205989381462587\", \"0.9999998238064090167676195794010619\"]",
    "closeTime": "2021-11-02T23:59:59Z",
    "endDate": "2021-11-02T23:59:59Z",
    "createdAt": "2021-10-25T00:00:00Z",
    "event": {"id": "evt_gamma_239139", "category": "Weather"},
    "tokens": [
        {"token_id": "tok_yes_threshold", "outcome": "Yes"},
        {"token_id": "tok_no_threshold", "outcome": "No"},
    ],
}


class FrozenGammaClient:
    def get_json(self, url: str, *, context: dict[str, Any]) -> dict[str, Any]:
        del url
        if int(context.get("page", 0)) == 0:
            return {"markets": [dict(FROZEN_REAL_GAMMA_MARKET)]}
        return {"markets": []}


class CurlJsonClient:
    def __init__(self, *, timeout_seconds: float = 20.0, retries: int = 3) -> None:
        self._timeout_seconds = float(timeout_seconds)
        self._retries = int(retries)

    def get_json(self, url: str, *, context: dict[str, Any]) -> dict[str, Any]:
        del context
        return json.loads(self.get_text(url))

    def get_text(self, url: str) -> str:
        last_error = "curl transport failure"
        for attempt in range(1, self._retries + 1):
            proc = subprocess.run(
                [
                    "curl",
                    "-L",
                    "--compressed",
                    "--max-time",
                    str(int(self._timeout_seconds)),
                    "-A",
                    "asterion-cold-path/0.1",
                    url,
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            if proc.returncode == 0:
                return proc.stdout
            stderr = (proc.stderr or proc.stdout or "").strip()
            last_error = f"curl exit {proc.returncode}: {stderr or 'unknown transport error'}"
            if attempt == self._retries:
                break
        raise RuntimeError(f"transport_error:{last_error}")


def _json_dump(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _stage_status(*, success_count: int, total_count: int) -> str:
    if total_count <= 0:
        return "skipped"
    if success_count <= 0:
        return "degraded"
    if success_count < total_count:
        return "degraded"
    return "ok"


def _first_non_empty(values: list[str | None]) -> str | None:
    for value in values:
        if value:
            return value
    return None


@contextlib.contextmanager
def patched_env(updates: dict[str, str | None]):
    old = {key: os.environ.get(key) for key in updates}
    try:
        for key, value in updates.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        yield
    finally:
        for key, value in old.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


@contextlib.contextmanager
def reader_connection(db_path: Path):
    with patched_env(
        {
            "ASTERION_STRICT_SINGLE_WRITER": "1",
            "ASTERION_DB_ROLE": "reader",
            "WRITERD": "0",
        }
    ):
        con = connect_duckdb(DuckDBConfig(db_path=str(db_path), ddl_path=None))
        try:
            yield con
        finally:
            con.close()


def drain_queue(*, queue_path: Path, db_path: Path, allow_tables: list[str]) -> None:
    with patched_env(
        {
            "ASTERION_DB_PATH": str(db_path),
            "ASTERION_WRITERD_ALLOWED_TABLES": ",".join(allow_tables),
        }
    ):
        while process_one(queue_path=str(queue_path), db_path=str(db_path), ddl_path=None, apply_schema=False):
            pass


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    db_path = output_dir / "real_weather_chain.duckdb"
    queue_path = output_dir / "real_weather_chain_write_queue.sqlite"
    report_path = output_dir / "real_weather_chain_report.json"

    if args.force_rebuild:
        for path in (db_path, queue_path):
            if path.exists():
                path.unlink()

    apply_schema(db_path)
    queue_cfg = WriteQueueConfig(path=str(queue_path))
    allow_tables = [
        "weather.weather_markets",
        "weather.weather_station_map",
        "weather.weather_market_specs",
        "weather.weather_forecast_runs",
        "weather.weather_fair_values",
        "weather.weather_watch_only_snapshots",
        "agent.invocations",
        "agent.outputs",
        "agent.reviews",
        "agent.evaluations",
    ]

    discovery, target_markets, report_mode, report_note = discover_target_markets(args=args)
    primary_market = target_markets[0]
    target_market_ids = [market.market_id for market in target_markets]
    enqueue_weather_market_upserts(queue_cfg, markets=target_markets, run_id="run_market_discovery")
    drain_queue(queue_path=queue_path, db_path=db_path, allow_tables=allow_tables)

    station_mappings = [
        build_station_mapping_record(
            market_id=market.market_id,
            location_name=station_override["location_name"],
            station_id=station_override["station_id"],
            station_name=station_override["station_name"],
            latitude=station_override["latitude"],
            longitude=station_override["longitude"],
            timezone=station_override["timezone"],
            source="operator_override",
            authoritative_source="weather.com",
            is_override=True,
            metadata={"reason": "real_weather_chain_smoke"},
        )
        for market in target_markets
        for station_override in [build_station_mapping_for_market(market)]
    ]
    enqueue_station_mapping_upserts(queue_cfg, mappings=station_mappings, run_id="run_station_map")
    drain_queue(queue_path=queue_path, db_path=db_path, allow_tables=allow_tables)

    with reader_connection(db_path) as con:
        available_markets = load_weather_markets_for_rule2spec(con, active_only=False, limit=max(len(target_markets) * 4, 100))
        selected_markets_for_specs = [
            market
            for market in available_markets
            if market.market_id in set(target_market_ids)
        ]
        selected_markets_for_specs.sort(key=lambda market: target_market_ids.index(market.market_id))
        if len(selected_markets_for_specs) != len(target_markets):
            missing_market_ids = sorted(set(target_market_ids) - {market.market_id for market in selected_markets_for_specs})
            raise RuntimeError(f"failed to load discovered markets for spec sync: {missing_market_ids}")
        spec_records = [
            build_weather_market_spec_record_via_station_mapper(
                parse_rule2spec_draft(market),
                mapper=StationMapper(),
                con=con,
            )
            for market in selected_markets_for_specs
        ]
    enqueue_weather_market_spec_upserts(
        queue_cfg,
        specs=spec_records,
        run_id="run_spec_sync",
    )
    spec_result = {
        "item_count": len(spec_records),
        "market_count": len(selected_markets_for_specs),
    }
    drain_queue(queue_path=queue_path, db_path=db_path, allow_tables=allow_tables)

    agent_report = run_agent_validations(
        db_path=db_path,
        queue_cfg=queue_cfg,
        allow_tables=allow_tables,
        market_ids=target_market_ids,
        skip_agent=args.skip_agent,
    )

    per_market_specs: list[dict[str, Any]] = []
    per_market_forecasts: list[dict[str, Any]] = []
    per_market_pricing: list[dict[str, Any]] = []
    per_market_signals: list[dict[str, Any]] = []
    with reader_connection(db_path) as con:
        for target_market in target_markets:
            spec = load_weather_market_spec(con, market_id=target_market.market_id)
            agent_market_report = agent_report["markets"].get(target_market.market_id, {})
            per_market_specs.append(
                {
                    "market_id": target_market.market_id,
                    "question": target_market.title,
                    "location_name": spec.location_name,
                    "station_id": spec.station_id,
                    "bucket_min_value": spec.bucket_min_value,
                    "bucket_max_value": spec.bucket_max_value,
                    "metric": spec.metric,
                    "authoritative_source": spec.authoritative_source,
                    "rule2spec_status": agent_market_report.get("rule2spec_status"),
                    "rule2spec_verdict": agent_market_report.get("rule2spec_verdict"),
                    "rule2spec_summary": agent_market_report.get("rule2spec_summary"),
                    "data_qa_status": agent_market_report.get("data_qa_status"),
                    "data_qa_verdict": agent_market_report.get("data_qa_verdict"),
                    "data_qa_summary": agent_market_report.get("data_qa_summary"),
                    "resolution_status": agent_market_report.get("resolution_status"),
                    "resolution_verdict": agent_market_report.get("resolution_verdict"),
                    "resolution_summary": agent_market_report.get("resolution_summary"),
                }
            )
    forecast_service = ForecastService(
        adapter_router=AdapterRouter(
            [
                NWSAdapter(client=HttpJsonClient(timeout_seconds=10.0)),
                OpenMeteoAdapter(client=HttpJsonClient(timeout_seconds=10.0)),
            ]
        ),
        cache=InMemoryForecastCache(),
    )
    model_run = datetime.now(UTC).strftime("%Y-%m-%dT%H:%MZ")
    forecast_target_time = datetime.now(UTC)
    forecast_success_market_ids: list[str] = []
    forecast_error_by_market: dict[str, str] = {}
    fair_values = []
    snapshots = []

    for target_market in target_markets:
        try:
            with reader_connection(db_path) as con:
                forecast_result = run_weather_forecast_refresh(
                    con,
                    queue_cfg,
                    forecast_service=forecast_service,
                    source=TARGET_SOURCE_REQUESTED,
                    model_run=model_run,
                    forecast_target_time=forecast_target_time,
                    market_ids=[target_market.market_id],
                    run_id=f"run_forecast_refresh_{target_market.market_id}",
                )
            drain_queue(queue_path=queue_path, db_path=db_path, allow_tables=allow_tables)
            with reader_connection(db_path) as con:
                market = load_weather_market(con, market_id=target_market.market_id)
                spec = load_weather_market_spec(con, market_id=target_market.market_id)
                forecast_run_id = con.execute(
                    """
                    SELECT run_id
                    FROM weather.weather_forecast_runs
                    WHERE market_id = ?
                    ORDER BY created_at DESC, run_id DESC
                    LIMIT 1
                    """,
                    [target_market.market_id],
                ).fetchone()[0]
                forecast_run = load_forecast_run(con, run_id=forecast_run_id)
            current_fair_values = build_binary_fair_values(market=market, spec=spec, forecast_run=forecast_run)
            market_prices = extract_market_prices(market.raw_market)
            current_snapshots = [
                build_watch_only_snapshot(
                    fair_value=item,
                    reference_price=market_prices[item.outcome],
                    threshold_bps=TARGET_THRESHOLD_BPS,
                    pricing_context={
                        "forecast_run_id": forecast_run.run_id,
                        "source_requested": TARGET_SOURCE_REQUESTED,
                        "source_used": forecast_run.source,
                        "source_trace": forecast_run.source_trace,
                    },
                )
                for item in current_fair_values
            ]
            forecast_success_market_ids.append(target_market.market_id)
            fair_values.extend(current_fair_values)
            snapshots.extend(current_snapshots)
            per_market_forecasts.append(
                {
                    "market_id": target_market.market_id,
                    "question": target_market.title,
                    "status": "ok",
                    "forecast_run_id": forecast_run.run_id,
                    "source_used": forecast_run.source,
                    "source_trace": forecast_run.source_trace,
                    "forecast_item_count": forecast_result.item_count,
                }
            )
            per_market_pricing.append(
                {
                    "market_id": target_market.market_id,
                    "question": target_market.title,
                    "status": "ok",
                    "market_prices": market_prices,
                    "fair_values": [
                        {
                            "token_id": item.token_id,
                            "outcome": item.outcome,
                            "fair_value": item.fair_value,
                            "confidence": item.confidence,
                        }
                        for item in sorted(current_fair_values, key=lambda row: row.outcome)
                    ],
                }
            )
            per_market_signals.append(
                {
                    "market_id": target_market.market_id,
                    "question": target_market.title,
                    "status": "ok",
                    "signals": [
                        {
                            "token_id": item.token_id,
                            "outcome": item.outcome,
                            "reference_price": item.reference_price,
                            "fair_value": item.fair_value,
                            "edge_bps": item.edge_bps,
                            "threshold_bps": item.threshold_bps,
                            "decision": item.decision,
                            "side": item.side,
                            "rationale": item.rationale,
                        }
                        for item in sorted(current_snapshots, key=lambda row: row.outcome)
                    ],
                }
            )
        except Exception as exc:  # noqa: BLE001
            forecast_error_by_market[target_market.market_id] = str(exc)

    if fair_values:
        enqueue_fair_value_upserts(queue_cfg, fair_values=fair_values, run_id="run_fair_values")
    if snapshots:
        enqueue_watch_only_snapshot_upserts(queue_cfg, snapshots=snapshots, run_id="run_watch_only")
    if fair_values or snapshots:
        drain_queue(queue_path=queue_path, db_path=db_path, allow_tables=allow_tables)

    counts = collect_counts(db_path)
    forecast_status = _stage_status(success_count=len(forecast_success_market_ids), total_count=len(target_markets))
    chain_status = "ok" if forecast_status == "ok" else "degraded"
    forecast_note = None
    if forecast_error_by_market:
        failed_market_count = len(forecast_error_by_market)
        forecast_note = (
            f"{failed_market_count}/{len(target_markets)} 个市场 forecast 拉取失败；"
            "其余成功市场已继续生成 pricing/opportunity。"
            if forecast_success_market_ids
            else f"所有 {len(target_markets)} 个市场的 forecast 拉取都失败；当前仅保留 discovery/spec/agent 结果。"
        )
    report = {
        "timestamp": datetime.now(UTC).isoformat(),
        "chain_status": chain_status,
        "report_scope": ["市场发现", "规则解析", "预测服务", "定价引擎", "机会发现"],
        "note": forecast_note,
        "market_discovery": {
            "status": "ok",
            "input_mode": report_mode,
            "market_source": discovery["market_source"],
            "selected_horizon_days": discovery["selected_horizon_days"],
            "question": primary_market.title,
            "market_id": primary_market.market_id,
            "condition_id": primary_market.condition_id,
            "close_time": (primary_market.close_time or primary_market.end_date).isoformat() if (primary_market.close_time or primary_market.end_date) else None,
            "tags": primary_market.tags,
            "outcomes": primary_market.outcomes,
            "token_ids": primary_market.token_ids,
            "discovered_count": discovery["discovered_count"],
            "selected_market_count": len(target_markets),
            "selected_market_ids": target_market_ids,
            "selected_markets": [
                {
                    "market_id": market.market_id,
                    "condition_id": market.condition_id,
                    "question": market.title,
                    "close_time": (market.close_time or market.end_date).isoformat() if (market.close_time or market.end_date) else None,
                    "accepting_orders": market.accepting_orders,
                    "location_name": next((item["location_name"] for item in per_market_specs if item["market_id"] == market.market_id), None),
                    "station_id": next((item["station_id"] for item in per_market_specs if item["market_id"] == market.market_id), None),
                    "rule2spec_status": next((item["rule2spec_status"] for item in per_market_specs if item["market_id"] == market.market_id), None),
                    "rule2spec_verdict": next((item["rule2spec_verdict"] for item in per_market_specs if item["market_id"] == market.market_id), None),
                    "rule2spec_summary": next((item["rule2spec_summary"] for item in per_market_specs if item["market_id"] == market.market_id), None),
                    "data_qa_status": next((item["data_qa_status"] for item in per_market_specs if item["market_id"] == market.market_id), None),
                    "data_qa_verdict": next((item["data_qa_verdict"] for item in per_market_specs if item["market_id"] == market.market_id), None),
                    "data_qa_summary": next((item["data_qa_summary"] for item in per_market_specs if item["market_id"] == market.market_id), None),
                    "resolution_status": next((item["resolution_status"] for item in per_market_specs if item["market_id"] == market.market_id), None),
                    "resolution_verdict": next((item["resolution_verdict"] for item in per_market_specs if item["market_id"] == market.market_id), None),
                    "resolution_summary": next((item["resolution_summary"] for item in per_market_specs if item["market_id"] == market.market_id), None),
                    "forecast_status": "ok" if market.market_id in forecast_success_market_ids else "failure",
                    "forecast_summary": forecast_error_by_market.get(market.market_id),
                }
                for market in target_markets
            ],
            "note": report_note,
        },
        "rule_parse": {
            "status": "ok",
            "location_name": per_market_specs[0]["location_name"],
            "bucket_min_value": per_market_specs[0]["bucket_min_value"],
            "bucket_max_value": per_market_specs[0]["bucket_max_value"],
            "station_id": per_market_specs[0]["station_id"],
            "spec_sync_item_count": spec_result["item_count"],
            "spec_sync_market_count": spec_result["market_count"],
            "selected_specs": per_market_specs,
            "agent_invocation_status": agent_report["primary_rule2spec_status"],
            "agent_verdict": agent_report["primary_rule2spec_verdict"],
            "agent_confidence": agent_report["primary_rule2spec_confidence"],
            "agent_summary": agent_report["primary_rule2spec_summary"],
            "agent_direct_validation_only": False,
        },
        "forecast_service": {
            "status": forecast_status,
            "source_requested": TARGET_SOURCE_REQUESTED,
            "source_used": _first_non_empty([item.get("source_used") for item in per_market_forecasts]),
            "source_trace": next((item.get("source_trace") for item in per_market_forecasts if item.get("source_trace")), []),
            "source_note": "当前代码会先请求 weather.com 语义，再按 spec fallback 到 NWS/OpenMeteo adapters。",
            "forecast_item_count": sum(int(item.get("forecast_item_count") or 0) for item in per_market_forecasts),
            "market_count": len(per_market_forecasts),
            "failed_market_count": len(forecast_error_by_market),
            "error_by_market": forecast_error_by_market,
            "note": forecast_note,
            "markets": per_market_forecasts,
        },
        "pricing_engine": {
            "status": _stage_status(success_count=len(per_market_pricing), total_count=len(target_markets)),
            "market_count": len(per_market_pricing),
            "market_prices": per_market_pricing[0]["market_prices"] if per_market_pricing else {},
            "fair_values": per_market_pricing[0]["fair_values"] if per_market_pricing else [],
            "markets": per_market_pricing,
        },
        "opportunity_discovery": {
            "status": _stage_status(success_count=len(per_market_signals), total_count=len(target_markets)),
            "signal_count": sum(len(item["signals"]) for item in per_market_signals),
            "signals": per_market_signals[0]["signals"] if per_market_signals else [],
            "markets": per_market_signals,
        },
        "db_counts": counts,
        "agent_summary": {
            "enabled": agent_report["enabled"],
            "rule2spec_run_count": agent_report["rule2spec_run_count"],
            "data_qa_run_count": agent_report["data_qa_run_count"],
            "resolution_run_count": agent_report["resolution_run_count"],
            "success_count": agent_report["success_count"],
            "failure_count": agent_report["failure_count"],
            "subjects": agent_report["subjects"],
        },
        "artifacts": {
            "duckdb_path": str(db_path),
            "report_path": str(report_path),
        },
    }
    report_path.write_text(_json_dump(report) + "\n", encoding="utf-8")
    print(_json_dump(report))
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the market-discovery -> spec -> forecast -> pricing -> opportunity smoke chain.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--force-rebuild", action="store_true")
    parser.add_argument("--recent-within-days", type=int, default=DEFAULT_RECENT_WITHIN_DAYS)
    parser.add_argument(
        "--use-frozen-market",
        action="store_true",
        help="Use the historical frozen Gamma weather market snapshot. Disabled by default because the smoke now prefers open recent live markets.",
    )
    parser.add_argument(
        "--with-agent",
        action="store_true",
        help="Backward-compatible no-op; agent validation is enabled by default.",
    )
    parser.add_argument(
        "--skip-agent",
        action="store_true",
        help="显式跳过 agent 链路，仅用于 debug/fallback。",
    )
    return parser.parse_args()


def apply_schema(db_path: Path) -> None:
    migrations_dir = ROOT / "sql" / "migrations"
    with patched_env(
        {
            "ASTERION_STRICT_SINGLE_WRITER": "1",
            "ASTERION_DB_ROLE": "writer",
            "WRITERD": "1",
        }
    ):
        apply_migrations(MigrationConfig(db_path=str(db_path), migrations_dir=str(migrations_dir)))


def run_agent_validations(
    *,
    db_path: Path,
    queue_cfg: WriteQueueConfig,
    allow_tables: list[str],
    market_ids: list[str],
    skip_agent: bool,
) -> dict[str, Any]:
    default_market_state = {
        "rule2spec_status": "not_run",
        "rule2spec_verdict": None,
        "rule2spec_confidence": None,
        "rule2spec_summary": "rule2spec 未运行",
        "data_qa_status": "not_run",
        "data_qa_verdict": None,
        "data_qa_summary": "no canonical forecast replay inputs in smoke chain",
        "resolution_status": "not_run",
        "resolution_verdict": None,
        "resolution_summary": "no canonical resolution inputs in smoke chain",
    }
    market_results = {market_id: dict(default_market_state) for market_id in market_ids}
    if skip_agent:
        return {
            "enabled": False,
            "primary_rule2spec_status": "skipped",
            "primary_rule2spec_verdict": None,
            "primary_rule2spec_confidence": None,
            "primary_rule2spec_summary": "agent validation skipped via --skip-agent",
            "rule2spec_run_count": 0,
            "data_qa_run_count": 0,
            "resolution_run_count": 0,
            "success_count": 0,
            "failure_count": 0,
            "subjects": [],
            "markets": {
                market_id: {
                    **state,
                    "rule2spec_status": "skipped",
                    "rule2spec_summary": "agent validation skipped via --skip-agent",
                }
                for market_id, state in market_results.items()
            },
        }
    try:
        client = build_agent_client_from_env()
        with reader_connection(db_path) as con:
            markets = [
                item
                for item in load_weather_markets_for_rule2spec(con, active_only=False, limit=max(len(market_ids) * 4, 100))
                if item.market_id in set(market_ids)
            ]
            markets.sort(key=lambda item: market_ids.index(item.market_id))
            requests: list[tuple[str, Rule2SpecAgentRequest]] = []
            for market in markets:
                draft = parse_rule2spec_draft(market)
                spec = load_weather_market_spec(con, market_id=market.market_id)
                override = build_station_mapping_for_market(market)
                station = StationMetadata(
                    station_id=override["station_id"],
                    location_name=override["location_name"],
                    latitude=override["latitude"],
                    longitude=override["longitude"],
                    timezone=override["timezone"],
                    source="operator_override",
                )
                requests.append(
                    (
                        market.market_id,
                        Rule2SpecAgentRequest(
                            market=market,
                            draft=draft,
                            current_spec=spec,
                            station_metadata=station,
                            station_override_summary={
                                "has_override": True,
                                "mapping_count": 1,
                                "station_ids": [override["station_id"]],
                                "sources": ["operator_override"],
                                "metadata_samples": [{"reason": "real_weather_chain_smoke"}],
                            },
                        ),
                    )
                )
        artifacts = []
        success_count = 0
        failure_count = 0
        for market_id, request in requests:
            try:
                artifact = run_rule2spec_agent_review(client, request, force_rerun=True)
                enqueue_agent_artifact_upserts(queue_cfg, artifacts=[artifact], run_id="run_weather_smoke_agents")
                drain_queue(queue_path=queue_cfg.path, db_path=db_path, allow_tables=allow_tables)
                artifacts.append(artifact)
                result = market_results.setdefault(market_id, dict(default_market_state))
                result["rule2spec_status"] = artifact.invocation.status.value
                if artifact.output is None:
                    result["rule2spec_summary"] = artifact.invocation.error_message
                    failure_count += 1
                    continue
                payload = artifact.output.structured_output_json
                result["rule2spec_verdict"] = payload.get("verdict")
                result["rule2spec_confidence"] = payload.get("confidence")
                result["rule2spec_summary"] = payload.get("summary")
                if artifact.invocation.status.value == "success":
                    success_count += 1
                else:
                    failure_count += 1
            except Exception as exc:  # noqa: BLE001
                result = market_results.setdefault(market_id, dict(default_market_state))
                result["rule2spec_status"] = "failure"
                result["rule2spec_summary"] = str(exc)
                failure_count += 1
    except Exception as exc:  # noqa: BLE001
        message = str(exc)
        return {
            "enabled": True,
            "primary_rule2spec_status": "failure",
            "primary_rule2spec_verdict": None,
            "primary_rule2spec_confidence": None,
            "primary_rule2spec_summary": message,
            "rule2spec_run_count": 0,
            "data_qa_run_count": 0,
            "resolution_run_count": 0,
            "success_count": 0,
            "failure_count": len(market_ids),
            "subjects": market_ids,
            "markets": {
                market_id: {
                    **state,
                    "rule2spec_status": "failure",
                    "rule2spec_summary": message,
                }
                for market_id, state in market_results.items()
            },
        }

    primary_market_id = market_ids[0]
    primary = market_results[primary_market_id]
    return {
        "enabled": True,
        "primary_rule2spec_status": primary.get("rule2spec_status"),
        "primary_rule2spec_verdict": primary.get("rule2spec_verdict"),
        "primary_rule2spec_confidence": primary.get("rule2spec_confidence"),
        "primary_rule2spec_summary": primary.get("rule2spec_summary"),
        "rule2spec_run_count": len(artifacts),
        "data_qa_run_count": 0,
        "resolution_run_count": 0,
        "success_count": success_count,
        "failure_count": failure_count,
        "subjects": list(market_ids),
        "markets": market_results,
    }


def extract_market_prices(raw_market: dict[str, Any]) -> dict[str, float]:
    raw_prices = raw_market.get("outcomePrices") or raw_market.get("outcome_prices")
    if isinstance(raw_prices, str):
        raw_prices = json.loads(raw_prices)
    if not isinstance(raw_prices, list) or len(raw_prices) != 2:
        raise ValueError("outcomePrices must contain exactly 2 entries")
    return {"YES": float(raw_prices[0]), "NO": float(raw_prices[1])}


def collect_counts(db_path: Path) -> dict[str, int]:
    with reader_connection(db_path) as con:
        return {
            "weather_markets": con.execute("SELECT COUNT(*) FROM weather.weather_markets").fetchone()[0],
            "weather_market_specs": con.execute("SELECT COUNT(*) FROM weather.weather_market_specs").fetchone()[0],
            "weather_forecast_runs": con.execute("SELECT COUNT(*) FROM weather.weather_forecast_runs").fetchone()[0],
            "weather_fair_values": con.execute("SELECT COUNT(*) FROM weather.weather_fair_values").fetchone()[0],
            "weather_watch_only_snapshots": con.execute("SELECT COUNT(*) FROM weather.weather_watch_only_snapshots").fetchone()[0],
        }


def discover_target_markets(*, args: argparse.Namespace):
    if args.use_frozen_market:
        market = normalize_weather_market(dict(FROZEN_REAL_GAMMA_MARKET))
        if market is None:
            raise RuntimeError("failed to normalize frozen market sample")
        return (
            {
                "market_source": "frozen",
                "selected_horizon_days": None,
                "discovered_count": 1,
                "selected_count": 1,
            },
            [market],
            "frozen_real_gamma_snapshot",
            "显式启用了历史冻结样本，仅用于 deterministic fallback；默认模式已改为只抓取开盘且近期的真实市场。",
        )

    horizons = build_horizon_sequence(int(args.recent_within_days))
    client = CurlJsonClient(timeout_seconds=20.0)
    saw_successful_scan = False
    transport_errors: list[str] = []

    for horizon in horizons:
        api_result = discover_target_markets_via_api(client=client, horizon_days=horizon)
        if api_result["status"] == "ok":
            saw_successful_scan = True
            if api_result["selected_markets"]:
                return (
                    {
                        "market_source": "gamma_events_api",
                        "selected_horizon_days": horizon,
                        "discovered_count": api_result["discovered_count"],
                        "selected_count": len(api_result["selected_markets"]),
                    },
                    api_result["selected_markets"],
                    "live_weather_market_auto_horizon",
                    f"通过 Gamma events weather feed 命中当前开盘且最近的天气市场，共 {len(api_result['selected_markets'])} 个，最终 horizon={horizon} 天。",
                )
        elif api_result["status"] == "transport_error":
            transport_errors.append(str(api_result["error"]))

        fallback_result = discover_target_markets_via_weather_page(client=client, horizon_days=horizon)
        if fallback_result["status"] == "ok":
            saw_successful_scan = True
            if fallback_result["selected_markets"]:
                return (
                    {
                        "market_source": "polymarket_weather_page",
                        "selected_horizon_days": horizon,
                        "discovered_count": fallback_result["discovered_count"],
                        "selected_count": len(fallback_result["selected_markets"]),
                    },
                    fallback_result["selected_markets"],
                    "live_weather_market_auto_horizon",
                    f"Gamma events 未命中可映射市场，回退官网天气页成功命中开盘近期市场，共 {len(fallback_result['selected_markets'])} 个，最终 horizon={horizon} 天。",
                )
        elif fallback_result["status"] == "transport_error":
            transport_errors.append(str(fallback_result["error"]))

    if saw_successful_scan:
        raise RuntimeError(
            f"no open recent weather markets found within {horizons[-1]} days; checked open markets only and did not fall back to frozen history"
        )
    if transport_errors:
        raise RuntimeError(f"transport_error:{'; '.join(transport_errors)}")
    raise RuntimeError("transport_error:weather market discovery failed without a classified result")


def build_station_mapping_for_market(market) -> dict[str, Any]:
    draft = parse_rule2spec_draft(market)
    location_key = normalize_location_key(draft.location_name)
    override = load_weather_station_catalog().get(location_key)
    if override is None:
        raise LookupError(f"no station override configured for location={draft.location_name!r}")
    return {
        "location_name": draft.location_name,
        **override,
    }


def build_horizon_sequence(base_days: int) -> list[int]:
    horizons = [int(base_days), *DEFAULT_HORIZON_SEQUENCE]
    ordered: list[int] = []
    for item in horizons:
        if item <= 0 or item in ordered:
            continue
        ordered.append(item)
    return ordered


def load_weather_station_catalog() -> dict[str, dict[str, Any]]:
    payload = json.loads(WEATHER_STATION_CATALOG_PATH.read_text(encoding="utf-8"))
    stations = payload.get("stations")
    if not isinstance(stations, list) or not stations:
        raise ValueError("weather station smoke catalog must contain a non-empty stations list")
    catalog: dict[str, dict[str, Any]] = {}
    for item in stations:
        if not isinstance(item, dict):
            continue
        station_id = str(item.get("station_id") or "").strip()
        station_name = str(item.get("station_name") or "").strip()
        timezone_name = str(item.get("timezone") or "").strip()
        latitude = item.get("latitude")
        longitude = item.get("longitude")
        location_keys = item.get("location_keys") or []
        if not station_id or not station_name or not timezone_name or latitude is None or longitude is None:
            raise ValueError("weather station smoke catalog entries must define station_id/station_name/latitude/longitude/timezone")
        for raw_key in location_keys:
            normalized = normalize_location_key(str(raw_key))
            if not normalized:
                continue
            catalog[normalized] = {
                "station_id": station_id,
                "station_name": station_name,
                "latitude": float(latitude),
                "longitude": float(longitude),
                "timezone": timezone_name,
            }
    if not catalog:
        raise ValueError("weather station smoke catalog produced no location keys")
    return catalog


def discover_target_markets_via_api(*, client: CurlJsonClient, horizon_days: int) -> dict[str, Any]:
    try:
        result = run_weather_market_discovery(
            base_url=WEATHER_MARKET_DISCOVERY_BASE_URL,
            markets_endpoint=WEATHER_MARKET_DISCOVERY_ENDPOINT,
            page_limit=WEATHER_MARKET_DISCOVERY_PAGE_LIMIT,
            max_pages=WEATHER_MARKET_DISCOVERY_MAX_PAGES,
            sleep_s=0.0,
            active_only=True,
            closed=False,
            archived=False,
            tag_slug=WEATHER_MARKET_DISCOVERY_TAG_SLUG,
            recent_within_days=int(horizon_days),
            client=client,
        )
    except Exception as exc:  # noqa: BLE001
        return {"status": "transport_error", "error": str(exc), "selected_markets": [], "discovered_count": 0}
    selected = select_supported_markets(result.discovered_markets)
    return {
        "status": "ok",
        "selected_markets": selected,
        "discovered_count": result.discovered_count,
    }


def discover_target_markets_via_weather_page(*, client: CurlJsonClient, horizon_days: int) -> dict[str, Any]:
    try:
        weather_page_html = client.get_text(WEATHER_PAGE_URL)
        event_urls = extract_weather_event_urls_from_page(weather_page_html)
    except Exception as exc:  # noqa: BLE001
        return {"status": "transport_error", "error": str(exc), "selected_markets": [], "discovered_count": 0}

    discovered: list[Any] = []
    last_errors: list[str] = []
    for relative_url in event_urls[:40]:
        event_url = urljoin("https://polymarket.com", relative_url)
        try:
            event_html = client.get_text(event_url)
            event_title = extract_weather_event_title_from_page(event_html)
            event_slug = relative_url.rsplit("/", 1)[-1]
            event_id = extract_weather_event_id_from_page(event_html)
            event_markets = extract_markets_from_event_page(event_html)
        except Exception as exc:  # noqa: BLE001
            last_errors.append(f"{relative_url}: {exc}")
            continue
        for raw_market in event_markets:
            normalized = normalize_weather_market(
                {
                    **raw_market,
                    "event": {
                        "id": event_id,
                        "title": event_title,
                        "slug": event_slug,
                        "category": "Weather",
                        "subcategory": "Temperature",
                        "tags": ["Weather"],
                    },
                }
            )
            if normalized is None:
                continue
            if not normalized.active or normalized.closed or normalized.archived:
                continue
            if normalized.accepting_orders is False:
                continue
            if not is_market_within_horizon(normalized, horizon_days=horizon_days):
                continue
            discovered.append(normalized)
    if not discovered and last_errors:
        return {"status": "transport_error", "error": "; ".join(last_errors), "selected_markets": [], "discovered_count": 0}
    selected = select_supported_markets(discovered)
    return {
        "status": "ok",
        "selected_markets": selected,
        "discovered_count": len(discovered),
    }


def select_supported_markets(markets: list[Any]) -> list[Any]:
    ranked = sorted(
        markets,
        key=lambda item: (
            0 if item.accepting_orders else 1,
            item.close_time or item.end_date or datetime.max.replace(tzinfo=UTC),
            item.market_id,
        ),
    )
    selected: list[Any] = []
    seen_market_ids: set[str] = set()
    for candidate in ranked:
        try:
            build_station_mapping_for_market(candidate)
            if candidate.market_id not in seen_market_ids:
                selected.append(candidate)
                seen_market_ids.add(candidate.market_id)
        except Exception:
            continue
    return selected


def is_market_within_horizon(market, *, horizon_days: int) -> bool:
    target = market.close_time or market.end_date
    if target is None:
        return False
    now = datetime.now(UTC)
    if target < now:
        return False
    return target <= (now + timedelta(days=int(horizon_days)))


def extract_weather_event_urls_from_page(html: str) -> list[str]:
    urls = sorted(set(re.findall(r"/event/[a-z0-9\\-]+", html)))
    return [url for url in urls if _looks_like_weather_event_slug(url.rsplit("/", 1)[-1])]


def extract_weather_event_title_from_page(html: str) -> str:
    match = re.search(r'"title":"([^"]+)"', html)
    if match:
        return bytes(match.group(1), "utf-8").decode("unicode_escape")
    raise ValueError("could not extract weather event title from page")


def extract_weather_event_id_from_page(html: str) -> str | None:
    match = re.search(r'"id":"([0-9]+)"', html)
    return match.group(1) if match else None


def extract_markets_from_event_page(html: str) -> list[dict[str, Any]]:
    start = html.find('"markets":[')
    if start < 0:
        return []
    bracket_start = html.find("[", start)
    if bracket_start < 0:
        return []
    depth = 0
    in_string = False
    escape = False
    for index in range(bracket_start, len(html)):
        ch = html[index]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
            continue
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                payload = html[bracket_start : index + 1]
                return json.loads(payload)
    raise ValueError("could not extract markets array from event page")


def _looks_like_weather_event_slug(slug: str) -> bool:
    lowered = slug.lower()
    return (
        lowered.startswith("highest-temperature-in-")
        or lowered.startswith("lowest-temperature-in-")
        or lowered.startswith("will-the-high-temperature-in-")
        or lowered.startswith("will-the-low-temperature-in-")
    )


if __name__ == "__main__":
    raise SystemExit(main())
