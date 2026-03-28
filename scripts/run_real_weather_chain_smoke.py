#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import json
import math
import os
import re
import subprocess
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from statistics import pstdev
from typing import Any
from urllib.parse import urljoin

import duckdb

from asterion_core.clients.http_retry import RetryHttpClient
from asterion_core.clients.shared import build_url
from asterion_core.contracts import ForecastRunRecord, StationMetadata, WeatherMarketSpecRecord, stable_object_id
from asterion_core.execution import ChainAccountCapabilityState, WalletRegistryEntry
from asterion_core.ui import (
    default_ui_db_replica_path,
    default_ui_lite_db_path,
    default_ui_lite_meta_path,
    default_ui_replica_meta_path,
)
from asterion_core.storage.database import DuckDBConfig, connect_duckdb
from asterion_core.storage.db_migrate import MigrationConfig, apply_migrations
from asterion_core.storage.write_queue import WriteQueueConfig
from asterion_core.storage.writerd import process_one
from dagster_asterion.handlers import (
    SettlementVerificationInput,
    run_operator_surface_refresh,
    run_weather_allocation_preview_refresh_job,
    run_weather_capability_refresh_job,
    run_weather_execution_priors_refresh_job,
    run_weather_forecast_calibration_profiles_v2_refresh_job,
    run_weather_forecast_refresh,
    run_weather_opportunity_triage_review_job,
    run_weather_paper_execution_job,
    run_weather_resolution_reconciliation,
    run_weather_resolution_review_job,
    run_weather_spec_sync,
    run_weather_ranking_retrospective_refresh_job,
)
from dagster_asterion.resources import (
    AsterionColdPathSettings,
    CapabilityRefreshRuntimeResource,
    HttpJsonClient,
    WatcherRpcPoolResource,
)
from domains.weather.forecast import (
    AdapterRouter,
    build_forecast_calibration_sample,
    build_normal_distribution,
    calibration_profile_age_hours,
    calibration_profile_freshness_status,
    DuckDBForecastStdDevProvider,
    ForecastService,
    InMemoryForecastCache,
    NWSAdapter,
    OpenMeteoAdapter,
    enqueue_forecast_calibration_sample_upserts,
    enqueue_forecast_run_upserts,
    enqueue_source_health_snapshot_upserts,
)
from domains.weather.forecast.calibration import threshold_probability_profile_for_probability
from domains.weather.opportunity import build_source_health_snapshot, load_execution_prior_summary
from domains.weather.opportunity.execution_priors import _hours_to_close_bucket, _market_age_bucket
from domains.weather.pricing import (
    build_forecast_calibration_pricing_context,
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
    build_weather_market_spec_record,
    build_weather_market_spec_record_via_station_mapper,
    build_station_mapping_record,
    enqueue_weather_market_spec_upserts,
    enqueue_station_mapping_upserts,
    load_weather_markets_for_rule2spec,
    normalize_location_key,
    parse_rule2spec_draft,
    validate_rule2spec_draft,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "data" / "dev" / "real_weather_chain"
DEFAULT_CANONICAL_DB_PATH = ROOT / "data" / "asterion.duckdb"
TARGET_THRESHOLD_BPS = 300
DEFAULT_FORECAST_SOURCE_REQUESTED = "openmeteo"
DEFAULT_RECENT_WITHIN_DAYS = 14
DEFAULT_HORIZON_SEQUENCE = (14, 30, 60, 90)
DEFAULT_MARKET_LIMIT = 24
DEFAULT_TRIAGE_LIMIT = 12
WATCHER_BACKFILL_RECENT_BLOCK_SPAN = 1000
WEATHER_MARKET_DISCOVERY_BASE_URL = "https://gamma-api.polymarket.com"
WEATHER_MARKET_DISCOVERY_ENDPOINT = "/events"
WEATHER_MARKET_DISCOVERY_TAG_SLUG = "weather"
WEATHER_MARKET_DISCOVERY_PAGE_LIMIT = 100
WEATHER_MARKET_DISCOVERY_MAX_PAGES = 5
CALIBRATION_BOOTSTRAP_MARKET_LIMIT = 12
SYNTHETIC_CALIBRATION_BOOTSTRAP_DAYS = 21
WEATHER_PAGE_URL = "https://polymarket.com/markets/weather"
WEATHER_STATION_CATALOG_PATH = ROOT / "config" / "weather_station_smoke_catalog.json"

FROZEN_TARGET_MARKET_ID = "239139"
FROZEN_TARGET_CONDITION_ID = "0xa9f2f1f8c5d7b0b9e8c7d6a5f4e3d2c1b0a99887"
FROZEN_TARGET_QUESTION = "Will the high temperature in New York's Central Park be 60°F or higher on November 2, 2021?"
DEFAULT_WALLET_ID = "wallet_weather_1"
DEFAULT_PAPER_ALLOCATION_POLICY_ID = "policy_weather_primary_paper"
DEFAULT_PAPER_CAPITAL_POLICY_ID = "cap_weather_primary_paper"
DEFAULT_PAPER_LIMIT_ID = "limit_weather_primary_station"
DEFAULT_PAPER_CASH_QUANTITY = 100.0
DEFAULT_STRATEGY_REGISTRATIONS = [
    {
        "strategy_id": "weather_primary",
        "strategy_version": "v1",
        "priority": 1,
        "route_action": "FAK",
        "size": "10",
        "min_edge_bps": 500,
    }
]
FULL_ALLOW_TABLES = [
    "weather.weather_markets",
    "weather.weather_station_map",
    "weather.weather_market_specs",
    "weather.weather_forecast_runs",
    "weather.weather_fair_values",
    "weather.weather_watch_only_snapshots",
    "weather.forecast_calibration_profiles_v2",
    "weather.forecast_calibration_samples",
    "weather.weather_execution_priors",
    "weather.source_health_snapshots",
    "capability.market_capabilities",
    "capability.account_trading_capabilities",
    "capability.execution_contexts",
    "resolution.uma_proposals",
    "resolution.proposal_state_transitions",
    "resolution.processed_uma_events",
    "resolution.block_watermarks",
    "resolution.watcher_continuity_checks",
    "resolution.watcher_continuity_gaps",
    "resolution.settlement_verifications",
    "resolution.proposal_evidence_links",
    "resolution.redeem_readiness_suggestions",
    "resolution.operator_review_decisions",
    "runtime.strategy_runs",
    "runtime.trade_tickets",
    "runtime.gate_decisions",
    "runtime.journal_events",
    "runtime.external_order_observations",
    "runtime.external_fill_observations",
    "runtime.external_balance_observations",
    "runtime.capital_allocation_runs",
    "runtime.allocation_decisions",
    "runtime.position_limit_checks",
    "runtime.execution_feedback_materializations",
    "runtime.execution_intelligence_runs",
    "runtime.execution_intelligence_summaries",
    "runtime.ranking_retrospective_runs",
    "runtime.ranking_retrospective_rows",
    "runtime.calibration_profile_materializations",
    "runtime.operator_surface_refresh_runs",
    "trading.orders",
    "trading.fills",
    "trading.order_state_transitions",
    "trading.reservations",
    "trading.inventory_positions",
    "trading.exposure_snapshots",
    "trading.reconciliation_results",
    "agent.invocations",
    "agent.outputs",
    "agent.reviews",
    "agent.evaluations",
    "agent.operator_review_decisions",
]

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
        try:
            import httpx
        except ModuleNotFoundError:  # pragma: no cover - dependency is present in canonical env
            self._httpx_client = None
        else:
            self._httpx_client = httpx.Client(timeout=self._timeout_seconds)

    def get_json(self, url: str, *, context: dict[str, Any]) -> dict[str, Any]:
        del context
        if self._httpx_client is not None:
            try:
                response = self._httpx_client.get(url, headers={"User-Agent": "asterion-cold-path/0.1"})
                response.raise_for_status()
                return response.json()
            except Exception:  # noqa: BLE001
                pass
        return json.loads(self.get_text(url))

    def get_text(self, url: str) -> str:
        if self._httpx_client is not None:
            try:
                response = self._httpx_client.get(url, headers={"User-Agent": "asterion-cold-path/0.1"})
                response.raise_for_status()
                return response.text
            except Exception:  # noqa: BLE001
                pass
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


class InsecureHttpJsonClient:
    def __init__(self, *, timeout_seconds: float = 20.0) -> None:
        import httpx

        self._client = httpx.Client(timeout=timeout_seconds, verify=False)

    def get_json(self, url: str, *, context: dict[str, Any]) -> dict[str, Any]:
        del context
        response = self._client.get(url, headers={"User-Agent": "asterion-cold-path/0.1"})
        response.raise_for_status()
        return response.json()


class InsecureCurlJsonClient:
    def __init__(self, *, timeout_seconds: float = 20.0, retries: int = 3) -> None:
        self._timeout_seconds = float(timeout_seconds)
        self._retries = int(retries)

    def get_json(self, url: str, *, context: dict[str, Any]) -> dict[str, Any]:
        del context
        last_error = "insecure curl transport failure"
        for attempt in range(1, self._retries + 1):
            proc = subprocess.run(
                [
                    "curl",
                    "-k",
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
                return json.loads(proc.stdout)
            stderr = (proc.stderr or proc.stdout or "").strip()
            last_error = f"insecure curl exit {proc.returncode}: {stderr or 'unknown transport error'}"
            if attempt == self._retries:
                break
        raise RuntimeError(f"transport_error:{last_error}")


class _PaperDefaultChainAccountCapabilityReader:
    def read_account_state(self, wallet_entry: WalletRegistryEntry) -> ChainAccountCapabilityState:
        return ChainAccountCapabilityState(
            approved_targets=list(wallet_entry.allowance_targets),
            can_trade=bool(wallet_entry.enabled),
            restricted_reason=None if bool(wallet_entry.enabled) else "wallet_disabled",
        )


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
            "ASTERION_DB_READ_ONLY": "1",
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


@contextlib.contextmanager
def writer_connection(db_path: Path):
    with patched_env(
        {
            "ASTERION_STRICT_SINGLE_WRITER": "1",
            "ASTERION_DB_ROLE": "writer",
            "WRITERD": "1",
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


def _resolve_db_path(raw_db_path: str | None) -> Path:
    if raw_db_path:
        return Path(raw_db_path)
    env_path = os.getenv("ASTERION_DB_PATH", "").strip()
    if env_path:
        return Path(env_path)
    return DEFAULT_CANONICAL_DB_PATH


def _resolve_queue_path(db_path: Path) -> Path:
    return db_path.parent / f"{db_path.stem}_write_queue.sqlite"


def _count_rows(con, table_name: str) -> int:
    try:
        row = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    except Exception:  # noqa: BLE001
        return 0
    return int(row[0]) if row and row[0] is not None else 0


def _load_latest_snapshot_ids(
    db_path: Path,
    *,
    limit: int = 10,
) -> list[str]:
    if not db_path.exists():
        return []
    with reader_connection(db_path) as con:
        try:
            rows = con.execute(
                """
                WITH latest AS (
                    SELECT * EXCLUDE (rn)
                    FROM (
                        SELECT
                            snapshot_id,
                            market_id,
                            outcome,
                            decision,
                            side,
                            created_at,
                            ROW_NUMBER() OVER (
                                PARTITION BY market_id, outcome
                                ORDER BY created_at DESC, snapshot_id DESC
                            ) AS rn
                        FROM weather.weather_watch_only_snapshots
                    )
                    WHERE rn = 1
                )
                SELECT snapshot_id
                FROM latest
                WHERE decision <> 'NO_TRADE'
                  AND side IN ('BUY', 'SELL')
                ORDER BY created_at DESC, snapshot_id DESC
                LIMIT ?
                """,
                [int(limit)],
            ).fetchall()
        except Exception:  # noqa: BLE001
            return []
    return [str(row[0]) for row in rows if row and row[0] is not None]


def _table_count_map(db_path: Path, table_names: list[str]) -> dict[str, int]:
    if not db_path.exists():
        return {table_name: 0 for table_name in table_names}
    with reader_connection(db_path) as con:
        return {table_name: _count_rows(con, table_name) for table_name in table_names}


def _bootstrap_local_paper_operator_state(db_path: Path) -> dict[str, Any]:
    seeded = {
        "allocation_policy_seeded": False,
        "capital_policy_seeded": False,
        "position_limit_seeded": False,
        "cash_inventory_seeded": False,
    }
    now = datetime.now(UTC).replace(tzinfo=None)
    with writer_connection(db_path) as con:
        allocation_policy_count = con.execute(
            """
            SELECT COUNT(*)
            FROM trading.allocation_policies
            WHERE wallet_id = ?
              AND status IN ('active', 'enabled')
            """,
            [DEFAULT_WALLET_ID],
        ).fetchone()
        if int(allocation_policy_count[0] or 0) <= 0:
            con.execute(
                """
                INSERT INTO trading.allocation_policies VALUES (
                    ?, ?, ?, 'active', 'alloc_v1_local_paper', 100.0, 100.0, 1.0, 1.0, ?, ?
                )
                """,
                [
                    DEFAULT_PAPER_ALLOCATION_POLICY_ID,
                    DEFAULT_WALLET_ID,
                    DEFAULT_STRATEGY_REGISTRATIONS[0]["strategy_id"],
                    now,
                    now,
                ],
            )
            seeded["allocation_policy_seeded"] = True

        capital_policy_count = con.execute(
            """
            SELECT COUNT(*)
            FROM trading.capital_budget_policies
            WHERE wallet_id = ?
              AND status IN ('active', 'enabled')
            """,
            [DEFAULT_WALLET_ID],
        ).fetchone()
        if int(capital_policy_count[0] or 0) <= 0:
            con.execute(
                """
                INSERT INTO trading.capital_budget_policies VALUES (
                    ?, ?, ?, 'warm', 'review_required', 'active', 'cap_v1_local_paper',
                    20.0, 10.0, 4, 2, 1.0, ?, ?
                )
                """,
                [
                    DEFAULT_PAPER_CAPITAL_POLICY_ID,
                    DEFAULT_WALLET_ID,
                    DEFAULT_STRATEGY_REGISTRATIONS[0]["strategy_id"],
                    now,
                    now,
                ],
            )
            seeded["capital_policy_seeded"] = True

        position_limit_count = con.execute(
            """
            SELECT COUNT(*)
            FROM trading.position_limit_policies
            WHERE wallet_id = ?
              AND policy_id = ?
              AND status IN ('active', 'enabled')
            """,
            [DEFAULT_WALLET_ID, DEFAULT_PAPER_ALLOCATION_POLICY_ID],
        ).fetchone()
        if int(position_limit_count[0] or 0) <= 0:
            con.execute(
                """
                INSERT INTO trading.position_limit_policies VALUES (
                    ?, ?, ?, 'station', 'default_weather_station', 25.0, NULL, 'active', ?, ?
                )
                """,
                [
                    DEFAULT_PAPER_LIMIT_ID,
                    DEFAULT_PAPER_ALLOCATION_POLICY_ID,
                    DEFAULT_WALLET_ID,
                    now,
                    now,
                ],
            )
            seeded["position_limit_seeded"] = True

        cash_balance = con.execute(
            """
            SELECT quantity
            FROM trading.inventory_positions
            WHERE wallet_id = ?
              AND asset_type = 'usdc_e'
              AND token_id = 'usdc_e'
              AND market_id = 'cash'
              AND outcome = 'cash'
              AND balance_type = 'available'
            """,
            [DEFAULT_WALLET_ID],
        ).fetchone()
        if cash_balance is None or float(cash_balance[0] or 0.0) <= 0.0:
            con.execute(
                """
                DELETE FROM trading.inventory_positions
                WHERE wallet_id = ?
                  AND asset_type = 'usdc_e'
                  AND token_id = 'usdc_e'
                  AND market_id = 'cash'
                  AND outcome = 'cash'
                  AND balance_type = 'available'
                """,
                [DEFAULT_WALLET_ID],
            )
            con.execute(
                """
                INSERT INTO trading.inventory_positions VALUES (
                    ?, 'usdc_e', 'usdc_e', 'cash', 'cash', 'available', ?, ?, 1, ?
                )
                """,
                [
                    DEFAULT_WALLET_ID,
                    DEFAULT_PAPER_CASH_QUANTITY,
                    "0x1111111111111111111111111111111111111111",
                    now,
                ],
            )
            seeded["cash_inventory_seeded"] = True
    return seeded


def _resolve_runtime_forecast_source(db_path: Path) -> str:
    explicit = str(os.getenv("ASTERION_REAL_CHAIN_FORECAST_SOURCE") or "").strip()
    if explicit:
        return explicit
    if not db_path.exists():
        return DEFAULT_FORECAST_SOURCE_REQUESTED
    with contextlib.suppress(Exception), reader_connection(db_path) as con:
        rows = con.execute(
            """
            SELECT DISTINCT lower(source)
            FROM weather.forecast_calibration_profiles_v2
            WHERE source IS NOT NULL
            """
        ).fetchall()
        sources = {str(row[0]).strip().lower() for row in rows if row and row[0] is not None}
        if len(sources) == 1:
            return next(iter(sources))
        if "openmeteo" in sources:
            return "openmeteo"
    return DEFAULT_FORECAST_SOURCE_REQUESTED


def _table_exists(con, table_name: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
    except Exception:  # noqa: BLE001
        return False
    return True


def _scalar_count(con, sql: str, params: list[Any] | None = None) -> int:
    row = con.execute(sql, params or []).fetchone()
    return int(row[0] or 0) if row is not None else 0


def _stable_reason_codes(*values: Any) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if isinstance(value, list):
            candidates = value
        elif value is None:
            candidates = []
        else:
            candidates = [value]
        for candidate in candidates:
            normalized = str(candidate or "").strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            ordered.append(normalized)
    return ordered


def _bias_quality_status_from_profile(*, mean_bias: float, sample_count: int) -> str:
    if int(sample_count) < 10:
        return "sparse"
    absolute_bias = abs(float(mean_bias))
    if absolute_bias <= 0.75:
        return "healthy"
    if absolute_bias <= 1.5:
        return "watch"
    return "degraded"


def _build_active_market_calibration_context(
    *,
    db_path: Path,
    forecast_run: ForecastRunRecord,
    outcome: str,
    fair_value: float,
) -> dict[str, Any]:
    base_context = build_forecast_calibration_pricing_context(
        forecast_run=forecast_run,
        outcome=outcome,
        fair_value=fair_value,
    )
    if (
        str(base_context.get("calibration_health_status") or "").strip().lower() not in {"", "lookup_missing"}
        and int(base_context.get("sample_count") or 0) > 0
    ):
        return base_context

    provider = DuckDBForecastStdDevProvider(db_path)
    summary_json = forecast_run.forecast_payload.get("distribution_summary_v2")
    regime_bucket = None
    if isinstance(summary_json, dict):
        regime_bucket = summary_json.get("regime_bucket")
    profile = provider.resolve_profile_v2(
        station_id=forecast_run.station_id,
        source=forecast_run.source,
        observation_date=forecast_run.observation_date,
        forecast_target_time=forecast_run.forecast_target_time,
        metric=forecast_run.metric,
        regime_bucket=str(regime_bucket or "unknown"),
    )
    if profile is None:
        return base_context

    normalized_outcome = str(outcome or "").strip().upper()
    yes_probability = float(fair_value) if normalized_outcome == "YES" else max(0.0, min(1.0, 1.0 - float(fair_value)))
    threshold_profile = threshold_probability_profile_for_probability(
        profile.threshold_probability_profile_json,
        yes_probability,
    )
    threshold_quality = (
        threshold_profile.quality_status
        if threshold_profile is not None
        else base_context.get("threshold_probability_quality_status")
        or "lookup_missing"
    )
    reason_codes = _stable_reason_codes(
        [code for code in list(base_context.get("calibration_reason_codes") or []) if code != "calibration_v2_lookup_missing"],
        "active_market_profile_override",
    )
    override_context = dict(base_context)
    override_context.update(
        {
            "calibration_v2_mode": "profile_v2",
            "calibration_health_status": profile.calibration_health_status,
            "sample_count": int(profile.sample_count),
            "bias_quality_status": _bias_quality_status_from_profile(
                mean_bias=float(profile.mean_bias),
                sample_count=int(profile.sample_count),
            ),
            "regime_bucket": profile.regime_bucket,
            "regime_stability_score": float(profile.regime_stability_score),
            "profile_materialized_at": profile.materialized_at.isoformat(),
            "profile_window_end": profile.window_end.isoformat(),
            "calibration_freshness_status": calibration_profile_freshness_status(profile.materialized_at),
            "profile_age_hours": calibration_profile_age_hours(profile.materialized_at),
            "threshold_probability_summary_json": profile.threshold_probability_profile_json,
            "threshold_probability_quality_status": threshold_quality,
            "calibration_reason_codes": reason_codes,
        }
    )
    if threshold_profile is not None:
        override_context.update(
            {
                "threshold_probability_bucket": threshold_profile.threshold_bucket,
                "threshold_probability_reliability_gap": threshold_profile.reliability_gap,
                "threshold_probability_sample_count": int(threshold_profile.sample_count),
            }
        )
    return override_context


def _build_market_outputs(
    *,
    db_path: Path,
    target_markets: list[Any],
    forecast_success_market_ids: set[str],
    forecast_error_by_market: dict[str, str],
    per_market_forecasts: list[dict[str, Any]],
    forecast_source_requested: str,
    forecast_target_time: datetime,
) -> dict[str, Any]:
    fair_values: list[Any] = []
    snapshots: list[Any] = []
    source_health_snapshots: list[Any] = []
    per_market_pricing: list[dict[str, Any]] = []
    per_market_signals: list[dict[str, Any]] = []

    observed_at = datetime.now(UTC).replace(tzinfo=None, microsecond=0)
    default_strategy_id = str((DEFAULT_STRATEGY_REGISTRATIONS[0] or {}).get("strategy_id") or "")

    with reader_connection(db_path) as con:
        for target_market in target_markets:
            if target_market.market_id not in forecast_success_market_ids:
                continue
            try:
                market = load_weather_market(con, market_id=target_market.market_id)
                spec = load_weather_market_spec(con, market_id=target_market.market_id)
                station_mapping = StationMapper().resolve_record_from_spec_inputs(
                    con,
                    market_id=target_market.market_id,
                    location_name=spec.location_name,
                    authoritative_source=spec.authoritative_source,
                )
                market_timestamps_row = con.execute(
                    "SELECT created_at, updated_at FROM weather.weather_markets WHERE market_id = ?",
                    [target_market.market_id],
                ).fetchone()
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
                forecast_created_at_row = con.execute(
                    "SELECT created_at FROM weather.weather_forecast_runs WHERE run_id = ?",
                    [forecast_run_id],
                ).fetchone()
                source_health_snapshot = build_source_health_snapshot(
                    market_id=target_market.market_id,
                    station_id=spec.station_id,
                    source=forecast_run.source,
                    market_updated_at=market_timestamps_row[1] if market_timestamps_row else None,
                    forecast_created_at=forecast_created_at_row[0] if forecast_created_at_row else forecast_target_time,
                    snapshot_created_at=observed_at,
                )
                current_fair_values = build_binary_fair_values(market=market, spec=spec, forecast_run=forecast_run)
                market_prices = extract_market_prices(market.raw_market)
                current_snapshots = []
                market_age_bucket = _market_age_bucket(
                    ticket_created_at=observed_at,
                    market_created_at=market_timestamps_row[0] if market_timestamps_row else None,
                )
                hours_to_close_bucket = _hours_to_close_bucket(
                    ticket_created_at=observed_at,
                    market_close_time=market.close_time,
                )
                for item in current_fair_values:
                    pricing_context = {
                        "forecast_run_id": forecast_run.run_id,
                        "mapping_confidence": station_mapping.mapping_confidence,
                        "mapping_method": station_mapping.mapping_method,
                        "market_quality_reason_codes": list(source_health_snapshot.degraded_reason_codes),
                        "price_staleness_ms": source_health_snapshot.price_staleness_ms,
                        "source_requested": forecast_source_requested,
                        "source_freshness_status": source_health_snapshot.source_freshness_status,
                        "source_used": forecast_run.source,
                        "source_trace": forecast_run.source_trace,
                        **_build_active_market_calibration_context(
                            db_path=db_path,
                            forecast_run=forecast_run,
                            outcome=item.outcome,
                            fair_value=item.fair_value,
                        ),
                    }
                    execution_prior_summary = load_execution_prior_summary(
                        con,
                        market_id=item.market_id,
                        strategy_id=default_strategy_id or None,
                        wallet_id=DEFAULT_WALLET_ID,
                        station_id=spec.station_id,
                        metric=forecast_run.metric,
                        side="BUY" if item.fair_value >= market_prices[item.outcome] else "SELL",
                        forecast_target_time=forecast_run.forecast_target_time,
                        observation_date=forecast_run.observation_date,
                        depth_proxy=0.85 if market.enable_order_book and 0.10 <= market_prices[item.outcome] <= 0.90 else 0.55,
                        spread_bps=None,
                        market_age_bucket=market_age_bucket,
                        hours_to_close_bucket=hours_to_close_bucket,
                        calibration_quality_bucket=str(pricing_context.get("calibration_health_status") or ""),
                        source_freshness_bucket=source_health_snapshot.source_freshness_status,
                    )
                    current_snapshots.append(
                        build_watch_only_snapshot(
                            fair_value=item,
                            reference_price=market_prices[item.outcome],
                            threshold_bps=TARGET_THRESHOLD_BPS,
                            accepting_orders=bool(market.accepting_orders),
                            enable_order_book=market.enable_order_book,
                            agent_review_status="passed",
                            execution_prior_summary=execution_prior_summary,
                            pricing_context=pricing_context,
                        )
                    )

                fair_values.extend(current_fair_values)
                snapshots.extend(current_snapshots)
                source_health_snapshots.append(source_health_snapshot)
                for item in per_market_forecasts:
                    if item["market_id"] == target_market.market_id:
                        item["forecast_run_id"] = forecast_run.run_id
                        item["source_used"] = forecast_run.source
                        item["source_trace"] = forecast_run.source_trace
                        break
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
                                "model_fair_value": (item.pricing_context or {}).get("model_fair_value"),
                                "execution_adjusted_fair_value": (item.pricing_context or {}).get("execution_adjusted_fair_value", item.fair_value),
                                "edge_bps_model": (item.pricing_context or {}).get("edge_bps_model"),
                                "edge_bps": item.edge_bps,
                                "edge_bps_executable": (item.pricing_context or {}).get("edge_bps_executable", item.edge_bps),
                                "fees_bps": (item.pricing_context or {}).get("fees_bps"),
                                "slippage_bps": (item.pricing_context or {}).get("slippage_bps"),
                                "fill_probability": (item.pricing_context or {}).get("fill_probability"),
                                "depth_proxy": (item.pricing_context or {}).get("depth_proxy"),
                                "liquidity_penalty_bps": (item.pricing_context or {}).get("liquidity_penalty_bps"),
                                "mapping_confidence": (item.pricing_context or {}).get("mapping_confidence"),
                                "source_freshness_status": (item.pricing_context or {}).get("source_freshness_status"),
                                "price_staleness_ms": (item.pricing_context or {}).get("price_staleness_ms"),
                                "market_quality_status": (item.pricing_context or {}).get("market_quality_status"),
                                "market_quality_reason_codes": (item.pricing_context or {}).get("market_quality_reason_codes"),
                                "expected_value_score": (item.pricing_context or {}).get("expected_value_score"),
                                "expected_pnl_score": (item.pricing_context or {}).get("expected_pnl_score"),
                                "ranking_score": (item.pricing_context or {}).get("ranking_score"),
                                "actionability_status": (item.pricing_context or {}).get("actionability_status"),
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

    return {
        "fair_values": fair_values,
        "snapshots": snapshots,
        "source_health_snapshots": source_health_snapshots,
        "per_market_pricing": per_market_pricing,
        "per_market_signals": per_market_signals,
        "per_market_forecasts": per_market_forecasts,
    }


def _agent_counts_by_type(db_path: Path, *, agent_type: str) -> dict[str, int]:
    if not db_path.exists():
        return {"invocation_count": 0, "output_count": 0, "evaluation_count": 0}
    with reader_connection(db_path) as con:
        if not _table_exists(con, "agent.invocations"):
            return {"invocation_count": 0, "output_count": 0, "evaluation_count": 0}
        invocation_count = _scalar_count(
            con,
            "SELECT COUNT(*) FROM agent.invocations WHERE agent_type = ?",
            [agent_type],
        )
        output_count = 0
        evaluation_count = 0
        if _table_exists(con, "agent.outputs"):
            output_count = _scalar_count(
                con,
                """
                SELECT COUNT(*)
                FROM agent.outputs AS outputs
                INNER JOIN agent.invocations AS invocations
                    ON invocations.invocation_id = outputs.invocation_id
                WHERE invocations.agent_type = ?
                """,
                [agent_type],
            )
        if _table_exists(con, "agent.evaluations"):
            evaluation_count = _scalar_count(
                con,
                """
                SELECT COUNT(*)
                FROM agent.evaluations AS evaluations
                INNER JOIN agent.invocations AS invocations
                    ON invocations.invocation_id = evaluations.invocation_id
                WHERE invocations.agent_type = ?
                """,
                [agent_type],
            )
        return {
            "invocation_count": invocation_count,
            "output_count": output_count,
            "evaluation_count": evaluation_count,
        }


def _agent_invocation_breakdown_by_type(db_path: Path, *, agent_type: str) -> dict[str, int]:
    if not db_path.exists():
        return {
            "success_count": 0,
            "failure_count": 0,
            "timeout_count": 0,
            "parse_error_count": 0,
            "rate_limited_count": 0,
            "provider_forbidden_count": 0,
        }
    with reader_connection(db_path) as con:
        if not _table_exists(con, "agent.invocations"):
            return {
                "success_count": 0,
                "failure_count": 0,
                "timeout_count": 0,
                "parse_error_count": 0,
                "rate_limited_count": 0,
                "provider_forbidden_count": 0,
            }
        row = con.execute(
            """
            SELECT
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success_count,
                SUM(CASE WHEN status = 'failure' THEN 1 ELSE 0 END) AS failure_count,
                SUM(CASE WHEN status = 'timeout' THEN 1 ELSE 0 END) AS timeout_count,
                SUM(CASE WHEN status = 'parse_error' THEN 1 ELSE 0 END) AS parse_error_count,
                SUM(
                    CASE
                        WHEN lower(coalesce(error_message, '')) LIKE '%429%'
                          OR lower(coalesce(error_message, '')) LIKE '%too many requests%'
                        THEN 1 ELSE 0
                    END
                ) AS rate_limited_count,
                SUM(
                    CASE
                        WHEN lower(coalesce(error_message, '')) LIKE '%401%'
                          OR lower(coalesce(error_message, '')) LIKE '%unauthorized%'
                          OR lower(coalesce(error_message, '')) LIKE '%403%'
                          OR lower(coalesce(error_message, '')) LIKE '%forbidden%'
                        THEN 1 ELSE 0
                    END
                ) AS provider_forbidden_count
            FROM agent.invocations
            WHERE agent_type = ?
            """,
            [agent_type],
        ).fetchone()
    return {
        "success_count": int(row[0] or 0),
        "failure_count": int(row[1] or 0),
        "timeout_count": int(row[2] or 0),
        "parse_error_count": int(row[3] or 0),
        "rate_limited_count": int(row[4] or 0),
        "provider_forbidden_count": int(row[5] or 0),
    }


def _triage_operator_decision_counts(db_path: Path) -> dict[str, int]:
    if not db_path.exists():
        return {"accepted": 0, "ignored": 0, "deferred": 0}
    with reader_connection(db_path) as con:
        if not _table_exists(con, "agent.operator_review_decisions"):
            return {"accepted": 0, "ignored": 0, "deferred": 0}
        rows = con.execute(
            """
            SELECT decision_status, COUNT(*) AS decision_count
            FROM agent.operator_review_decisions
            WHERE agent_type = 'opportunity_triage'
              AND subject_type = 'weather_market'
            GROUP BY 1
            """
        ).fetchall()
    counts = {"accepted": 0, "ignored": 0, "deferred": 0}
    for decision_status, decision_count in rows:
        key = str(decision_status or "").strip().lower()
        if key in counts:
            counts[key] = int(decision_count or 0)
    return counts


def _latest_triage_quality_counts(
    db_path: Path,
    *,
    subject_ids: list[str] | None,
) -> dict[str, int]:
    if not db_path.exists() or not subject_ids:
        return {
            "latest_triage_non_fallback_output_count": 0,
            "latest_triage_medium_or_high_confidence_count": 0,
            "latest_triage_operator_review_count": 0,
        }
    with reader_connection(db_path) as con:
        if not _table_exists(con, "agent.invocations"):
            return {
                "latest_triage_non_fallback_output_count": 0,
                "latest_triage_medium_or_high_confidence_count": 0,
                "latest_triage_operator_review_count": 0,
            }
        placeholders = ",".join(["?"] * len(subject_ids))
        latest_output_rows = []
        if _table_exists(con, "agent.outputs"):
            latest_output_rows = con.execute(
                f"""
                WITH ranked AS (
                    SELECT
                        inv.subject_id,
                        inv.model_provider,
                        outputs.structured_output_json,
                        ROW_NUMBER() OVER (
                            PARTITION BY inv.subject_id
                            ORDER BY coalesce(inv.ended_at, inv.started_at) DESC, inv.invocation_id DESC
                        ) AS row_num
                    FROM agent.invocations AS inv
                    INNER JOIN agent.outputs AS outputs
                        ON outputs.invocation_id = inv.invocation_id
                    WHERE inv.agent_type = 'opportunity_triage'
                      AND inv.subject_id IN ({placeholders})
                )
                SELECT subject_id, model_provider, structured_output_json
                FROM ranked
                WHERE row_num = 1
                """,
                list(subject_ids),
            ).fetchall()
        operator_review_count = 0
        if _table_exists(con, "agent.operator_review_decisions"):
            operator_review_count = _scalar_count(
                con,
                f"""
                SELECT COUNT(*)
                FROM agent.operator_review_decisions
                WHERE agent_type = 'opportunity_triage'
                  AND subject_type = 'weather_market'
                  AND subject_id IN ({placeholders})
                """,
                list(subject_ids),
            )
    non_fallback_output_count = 0
    medium_or_high_confidence_count = 0
    for _subject_id, model_provider, structured_output_json in latest_output_rows:
        provider = str(model_provider or "").strip().lower()
        if provider and provider != "deterministic_fallback":
            non_fallback_output_count += 1
        payload = {}
        if isinstance(structured_output_json, str):
            with contextlib.suppress(json.JSONDecodeError):
                payload = json.loads(structured_output_json)
        elif isinstance(structured_output_json, dict):
            payload = structured_output_json
        confidence_band = str((payload or {}).get("confidence_band") or "").strip().lower()
        if confidence_band in {"medium", "high"}:
            medium_or_high_confidence_count += 1
    return {
        "latest_triage_non_fallback_output_count": non_fallback_output_count,
        "latest_triage_medium_or_high_confidence_count": medium_or_high_confidence_count,
        "latest_triage_operator_review_count": int(operator_review_count),
    }


def _collect_ui_row_counts() -> dict[str, int]:
    counts: dict[str, int] = {}
    for table_name, db_path in [
        ("ui.market_opportunity_summary", default_ui_lite_db_path()),
        ("ui.action_queue_summary", default_ui_lite_db_path()),
        ("ui.market_microstructure_summary", default_ui_lite_db_path()),
        ("ui.predicted_vs_realized_summary", default_ui_lite_db_path()),
        ("ui.execution_science_summary", default_ui_lite_db_path()),
        ("ui.watch_only_vs_executed_summary", default_ui_lite_db_path()),
        ("ui.opportunity_triage_summary", default_ui_lite_db_path()),
        ("ui.proposal_resolution_summary", default_ui_lite_db_path()),
    ]:
        db_path = Path(db_path)
        if not db_path.exists():
            counts[table_name] = 0
            continue
        with reader_connection(db_path) as con:
            counts[table_name] = _count_rows(con, table_name)
    return counts


def _load_ui_market_opportunity_metrics() -> dict[str, int]:
    lite_db_path = Path(default_ui_lite_db_path())
    if not lite_db_path.exists():
        return {
            "deployable_snapshot_count": 0,
            "execution_intelligence_covered_snapshot_count": 0,
            "active_market_prior_hit_count": 0,
        }
    with reader_connection(lite_db_path) as con:
        deployable_snapshot_count = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM ui.market_opportunity_summary
                WHERE UPPER(COALESCE(best_decision, 'NO_TRADE')) <> 'NO_TRADE'
                """
            ).fetchone()[0]
        )
        execution_intelligence_covered_snapshot_count = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM ui.market_opportunity_summary
                WHERE UPPER(COALESCE(best_decision, 'NO_TRADE')) <> 'NO_TRADE'
                  AND (
                    execution_intelligence_summary_id IS NOT NULL
                    OR COALESCE(execution_intelligence_score, 0.0) > 0.0
                  )
                """
            ).fetchone()[0]
        )
        active_market_prior_hit_count = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM ui.market_opportunity_summary
                WHERE UPPER(COALESCE(best_decision, 'NO_TRADE')) <> 'NO_TRADE'
                  AND (
                    execution_prior_key IS NOT NULL
                    OR LOWER(COALESCE(feedback_status, 'heuristic_only')) <> 'heuristic_only'
                  )
                """
            ).fetchone()[0]
        )
    return {
        "deployable_snapshot_count": deployable_snapshot_count,
        "execution_intelligence_covered_snapshot_count": execution_intelligence_covered_snapshot_count,
        "active_market_prior_hit_count": active_market_prior_hit_count,
    }


def _openmeteo_archive_variable(metric: str) -> str:
    lower = str(metric or "").lower()
    if "min" in lower or "low" in lower:
        return "temperature_2m_min"
    return "temperature_2m_max"


def _fetch_observed_value_from_archive(
    client: RetryHttpClient,
    *,
    latitude: float,
    longitude: float,
    observation_date: date,
    timezone_name: str,
    metric: str,
) -> float:
    variable = _openmeteo_archive_variable(metric)
    url = build_url(
        "https://archive-api.open-meteo.com",
        "/v1/archive",
        {
            "latitude": latitude,
            "longitude": longitude,
            "timezone": timezone_name,
            "start_date": observation_date.isoformat(),
            "end_date": observation_date.isoformat(),
            "daily": variable,
        },
    )
    payload = client.get_json(
        url,
        context={
            "source": "openmeteo_archive",
            "metric": metric,
            "observation_date": observation_date.isoformat(),
        },
    )
    daily = payload.get("daily")
    if not isinstance(daily, dict):
        raise ValueError("open-meteo archive daily payload missing")
    values = daily.get(variable)
    if not isinstance(values, list) or not values:
        raise ValueError(f"open-meteo archive variable missing:{variable}")
    return float(values[0])


def _fetch_historical_forecast_value(
    client: RetryHttpClient,
    *,
    latitude: float,
    longitude: float,
    observation_date: date,
    timezone_name: str,
    metric: str,
) -> float:
    variable = _openmeteo_archive_variable(metric)
    url = build_url(
        "https://historical-forecast-api.open-meteo.com",
        "/v1/forecast",
        {
            "latitude": latitude,
            "longitude": longitude,
            "timezone": timezone_name,
            "start_date": observation_date.isoformat(),
            "end_date": observation_date.isoformat(),
            "daily": variable,
        },
    )
    payload = client.get_json(
        url,
        context={
            "source": "openmeteo_historical_forecast",
            "metric": metric,
            "observation_date": observation_date.isoformat(),
        },
    )
    daily = payload.get("daily")
    if not isinstance(daily, dict):
        raise ValueError("open-meteo historical forecast daily payload missing")
    values = daily.get(variable)
    if not isinstance(values, list) or not values:
        raise ValueError(f"open-meteo historical forecast variable missing:{variable}")
    return float(values[0])


def _observed_value_hits_bucket(spec: WeatherMarketSpecRecord, observed_value: float) -> bool:
    lower_ok = (
        float(observed_value) >= float(spec.bucket_min_value)
        if spec.inclusive_bounds
        else float(observed_value) > float(spec.bucket_min_value)
    )
    upper_ok = (
        float(observed_value) <= float(spec.bucket_max_value)
        if spec.inclusive_bounds
        else float(observed_value) < float(spec.bucket_max_value)
    )
    return bool(lower_ok and upper_ok)


def _expected_outcome_from_observed_value(spec: WeatherMarketSpecRecord, observed_value: float) -> str:
    return "YES" if _observed_value_hits_bucket(spec, observed_value) else "NO"


def _load_latest_resolution_proposals_by_market(con) -> dict[str, dict[str, Any]]:
    if not _table_exists(con, "resolution.uma_proposals"):
        return {}
    rows = con.execute(
        """
        SELECT
            market_id,
            proposal_id,
            proposed_outcome,
            proposal_timestamp,
            proposal_block_number
        FROM resolution.uma_proposals
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY market_id
            ORDER BY proposal_timestamp DESC, proposal_block_number DESC, proposal_id DESC
        ) = 1
        """
    ).fetchall()
    return {
        str(row[0]): {
            "proposal_id": str(row[1]),
            "proposed_outcome": str(row[2]) if row[2] is not None else None,
        }
        for row in rows
        if row and row[0] is not None and row[1] is not None
    }


def _load_unresolved_filled_market_ids(con) -> list[str]:
    required_tables = [
        "runtime.trade_tickets",
        "runtime.submit_attempts",
        "trading.fills",
        "weather.weather_market_specs",
    ]
    if any(not _table_exists(con, table_name) for table_name in required_tables):
        return []
    rows = con.execute(
        """
        WITH filled_markets AS (
            SELECT
                tickets.market_id,
                SUM(fills.size) AS filled_quantity
            FROM runtime.trade_tickets AS tickets
            INNER JOIN runtime.submit_attempts AS submit_attempts
                ON submit_attempts.ticket_id = tickets.ticket_id
               AND submit_attempts.attempt_kind = 'submit_order'
               AND submit_attempts.order_id IS NOT NULL
            INNER JOIN trading.fills AS fills
                ON fills.order_id = submit_attempts.order_id
            GROUP BY tickets.market_id
        )
        SELECT filled_markets.market_id
        FROM filled_markets
        INNER JOIN weather.weather_market_specs AS specs
            ON specs.market_id = filled_markets.market_id
        WHERE filled_markets.filled_quantity > 0
          AND NOT EXISTS (
              SELECT 1
              FROM resolution.settlement_verifications AS verifications
              WHERE verifications.market_id = filled_markets.market_id
          )
        ORDER BY specs.observation_date ASC, filled_markets.market_id ASC
        """
    ).fetchall()
    return [str(row[0]) for row in rows if row and row[0] is not None]


def _resolve_watcher_rpc_urls(settings: AsterionColdPathSettings) -> list[str]:
    urls = [str(item).strip() for item in list(settings.watcher_rpc_urls or []) if str(item).strip()]
    if urls:
        return urls
    fallback_urls: list[str] = []
    for index in range(1, 6):
        value = str(os.getenv(f"ASTERION_POLYGON_RPC_URL_{index}") or "").strip()
        if value:
            fallback_urls.append(value)
    single_fallback = str(os.getenv("ASTERION_POLYGON_RPC_URL") or "").strip()
    if single_fallback:
        fallback_urls.append(single_fallback)
    deduped: list[str] = []
    seen: set[str] = set()
    for url in fallback_urls:
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped


def _run_watcher_backfill_step(
    *,
    db_path: Path,
    queue_cfg: WriteQueueConfig,
    allow_tables: list[str],
    settings: AsterionColdPathSettings,
) -> dict[str, Any]:
    with reader_connection(db_path) as con:
        unresolved_filled_market_ids = _load_unresolved_filled_market_ids(con)
    if not unresolved_filled_market_ids:
        return {
            "status": "idle_no_subjects",
            "watch_scope": "filled_unresolved_markets",
            "watch_scope_market_count": 0,
            "watch_scope_market_ids": [],
            "watcher_chain_id": int(settings.watcher_chain_id),
            "rpc_endpoint_count": 0,
        }
    watcher_rpc_urls = _resolve_watcher_rpc_urls(settings)
    if not watcher_rpc_urls:
        return {
            "status": "not_configured",
            "watcher_chain_id": int(settings.watcher_chain_id),
            "rpc_endpoint_count": 0,
            "reason": "missing_watcher_rpc_urls",
            "watch_scope": "filled_unresolved_markets",
            "watch_scope_market_count": len(unresolved_filled_market_ids),
            "watch_scope_market_ids": unresolved_filled_market_ids[:20],
        }
    try:
        WatcherRpcPoolResource(settings=settings).build_rpc_pool()
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "not_configured",
            "watcher_chain_id": int(settings.watcher_chain_id),
            "rpc_endpoint_count": len(watcher_rpc_urls),
            "reason": str(exc),
            "watch_scope": "filled_unresolved_markets",
            "watch_scope_market_count": len(unresolved_filled_market_ids),
            "watch_scope_market_ids": unresolved_filled_market_ids[:20],
        }
    try:
        with reader_connection(db_path) as con:
            result = run_weather_watcher_backfill_job(
                con,
                queue_cfg,
                rpc_pool=WatcherRpcPoolResource(settings=settings).build_rpc_pool(),
                chain_id=int(settings.watcher_chain_id),
                replay_reason="real_weather_chain_smoke",
                max_block_span=WATCHER_BACKFILL_RECENT_BLOCK_SPAN,
            )
        drain_queue(queue_path=_resolve_queue_path(db_path), db_path=db_path, allow_tables=allow_tables)
        return {
            "status": "ok",
            "watch_scope": "filled_unresolved_markets",
            "watch_scope_market_count": len(unresolved_filled_market_ids),
            "watch_scope_market_ids": unresolved_filled_market_ids[:20],
            **result.metadata,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "failed",
            "watcher_chain_id": int(settings.watcher_chain_id),
            "rpc_endpoint_count": len(watcher_rpc_urls),
            "error": str(exc),
            "watch_scope": "filled_unresolved_markets",
            "watch_scope_market_count": len(unresolved_filled_market_ids),
            "watch_scope_market_ids": unresolved_filled_market_ids[:20],
        }


def _build_realtime_settlement_verification_inputs(
    *,
    db_path: Path,
) -> tuple[list[SettlementVerificationInput], dict[str, Any]]:
    archive_client = RetryHttpClient(HttpJsonClient(timeout_seconds=20.0), max_retries=2, initial_delay=0.5)
    verification_inputs: list[SettlementVerificationInput] = []
    awaiting_uma_proposal_market_ids: list[str] = []
    awaiting_observation_market_ids: list[str] = []
    verification_market_ids: list[str] = []
    errors: list[str] = []
    today = datetime.now(UTC).date()

    with reader_connection(db_path) as con:
        unresolved_market_ids = _load_unresolved_filled_market_ids(con)
        proposal_by_market = _load_latest_resolution_proposals_by_market(con)
        for market_id in unresolved_market_ids:
            proposal = proposal_by_market.get(market_id)
            if proposal is None:
                awaiting_uma_proposal_market_ids.append(market_id)
                continue
            try:
                spec = load_weather_market_spec(con, market_id=market_id)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{market_id}:failed_to_load_spec:{exc}")
                awaiting_observation_market_ids.append(market_id)
                continue
            if spec.observation_date >= today:
                awaiting_observation_market_ids.append(market_id)
                continue
            try:
                observed_value = _fetch_observed_value_from_archive(
                    archive_client,
                    latitude=float(spec.latitude),
                    longitude=float(spec.longitude),
                    observation_date=spec.observation_date,
                    timezone_name=str(spec.timezone),
                    metric=str(spec.metric),
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{market_id}:observed_value:{exc}")
                awaiting_observation_market_ids.append(market_id)
                continue
            verification_inputs.append(
                SettlementVerificationInput(
                    proposal_id=str(proposal["proposal_id"]),
                    expected_outcome=_expected_outcome_from_observed_value(spec, observed_value),
                    confidence=0.99,
                    sources_checked=["openmeteo_archive"],
                    evidence_payload={
                        "observed_value": float(observed_value),
                        "observation_date": spec.observation_date.isoformat(),
                        "station_id": spec.station_id,
                        "metric": spec.metric,
                        "bucket_min_value": spec.bucket_min_value,
                        "bucket_max_value": spec.bucket_max_value,
                        "inclusive_bounds": bool(spec.inclusive_bounds),
                        "authoritative_source": spec.authoritative_source,
                    },
                )
            )
            verification_market_ids.append(market_id)

    status = "idle_no_subjects"
    if verification_inputs:
        status = "ready"
    elif awaiting_uma_proposal_market_ids:
        status = "awaiting_uma_proposal"
    elif awaiting_observation_market_ids:
        status = "awaiting_observation"
    return verification_inputs, {
        "status": status,
        "unresolved_filled_market_count": len(unresolved_market_ids) if 'unresolved_market_ids' in locals() else 0,
        "verification_candidate_count": len(verification_inputs),
        "verification_market_count": len(verification_market_ids),
        "verification_market_ids": verification_market_ids,
        "awaiting_uma_proposal_market_count": len(awaiting_uma_proposal_market_ids),
        "awaiting_uma_proposal_market_ids": awaiting_uma_proposal_market_ids,
        "awaiting_observation_market_count": len(awaiting_observation_market_ids),
        "awaiting_observation_market_ids": awaiting_observation_market_ids,
        "error_count": len(errors),
        "errors": errors[:10],
    }


def _load_predicted_vs_realized_metrics() -> dict[str, int]:
    lite_db_path = Path(default_ui_lite_db_path())
    if not lite_db_path.exists():
        return {
            "pending_resolution_ticket_count": 0,
            "resolved_ticket_count": 0,
            "realized_pnl_row_count": 0,
        }
    try:
        with duckdb.connect(str(lite_db_path), read_only=True) as con:
            if not _table_exists(con, "ui.predicted_vs_realized_summary"):
                return {
                    "pending_resolution_ticket_count": 0,
                    "resolved_ticket_count": 0,
                    "realized_pnl_row_count": 0,
                }
            row = con.execute(
                """
                SELECT
                    SUM(CASE WHEN evaluation_status = 'pending_resolution' THEN 1 ELSE 0 END) AS pending_resolution_ticket_count,
                    SUM(CASE WHEN evaluation_status = 'resolved' THEN 1 ELSE 0 END) AS resolved_ticket_count,
                    SUM(CASE WHEN realized_pnl IS NOT NULL THEN 1 ELSE 0 END) AS realized_pnl_row_count
                FROM ui.predicted_vs_realized_summary
                """
            ).fetchone()
    except Exception:
        return {
            "pending_resolution_ticket_count": 0,
            "resolved_ticket_count": 0,
            "realized_pnl_row_count": 0,
        }
    return {
        "pending_resolution_ticket_count": int(row[0] or 0),
        "resolved_ticket_count": int(row[1] or 0),
        "realized_pnl_row_count": int(row[2] or 0),
    }


def _load_latest_feedback_materialization_summary(db_path: Path) -> dict[str, Any]:
    if not db_path.exists():
        return {"latest_feedback_writeback_status": "not_run", "latest_feedback_materialization_count": 0}
    with reader_connection(db_path) as con:
        count = _count_rows(con, "runtime.execution_feedback_materializations")
        if count <= 0:
            return {"latest_feedback_writeback_status": "not_run", "latest_feedback_materialization_count": 0}
        row = con.execute(
            """
            SELECT status
            FROM runtime.execution_feedback_materializations
            ORDER BY materialized_at DESC, materialization_id DESC
            LIMIT 1
            """
        ).fetchone()
    return {
        "latest_feedback_writeback_status": str(row[0]) if row and row[0] is not None else "not_run",
        "latest_feedback_materialization_count": int(count),
    }


def _fetch_archive_daily_series(
    client: RetryHttpClient,
    *,
    base_url: str,
    endpoint: str,
    latitude: float,
    longitude: float,
    start_date: date,
    end_date: date,
    timezone_name: str,
    metric: str,
    context_source: str,
) -> dict[date, float]:
    variable = _openmeteo_archive_variable(metric)
    url = build_url(
        base_url,
        endpoint,
        {
            "latitude": latitude,
            "longitude": longitude,
            "timezone": timezone_name,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "daily": variable,
        },
    )
    payload = client.get_json(
        url,
        context={
            "source": context_source,
            "metric": metric,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
    )
    daily = payload.get("daily")
    if not isinstance(daily, dict):
        raise ValueError(f"{context_source} daily payload missing")
    times = daily.get("time")
    values = daily.get(variable)
    if not isinstance(times, list) or not isinstance(values, list) or len(times) != len(values):
        raise ValueError(f"{context_source} daily series missing:{variable}")
    out: dict[date, float] = {}
    for raw_day, raw_value in zip(times, values, strict=True):
        if raw_value is None:
            continue
        out[date.fromisoformat(str(raw_day))] = float(raw_value)
    return out


def _fetch_observed_series_from_archive(
    client: RetryHttpClient,
    *,
    latitude: float,
    longitude: float,
    start_date: date,
    end_date: date,
    timezone_name: str,
    metric: str,
) -> dict[date, float]:
    return _fetch_archive_daily_series(
        client,
        base_url="https://archive-api.open-meteo.com",
        endpoint="/v1/archive",
        latitude=latitude,
        longitude=longitude,
        start_date=start_date,
        end_date=end_date,
        timezone_name=timezone_name,
        metric=metric,
        context_source="openmeteo_archive_series",
    )


def _fetch_historical_forecast_series(
    client: RetryHttpClient,
    *,
    latitude: float,
    longitude: float,
    start_date: date,
    end_date: date,
    timezone_name: str,
    metric: str,
) -> dict[date, float]:
    return _fetch_archive_daily_series(
        client,
        base_url="https://historical-forecast-api.open-meteo.com",
        endpoint="/v1/forecast",
        latitude=latitude,
        longitude=longitude,
        start_date=start_date,
        end_date=end_date,
        timezone_name=timezone_name,
        metric=metric,
        context_source="openmeteo_historical_forecast_series",
    )


def _discover_supported_closed_weather_markets_for_calibration(*, lookback_days: int, limit: int = 200) -> list[Any]:
    errors: list[str] = []
    result = None
    for client in (
        RetryHttpClient(HttpJsonClient(timeout_seconds=20.0), max_retries=3, initial_delay=0.5),
        CurlJsonClient(timeout_seconds=20.0, retries=3),
        RetryHttpClient(InsecureHttpJsonClient(timeout_seconds=20.0), max_retries=2, initial_delay=0.5),
        InsecureCurlJsonClient(timeout_seconds=20.0, retries=2),
    ):
        try:
            result = run_weather_market_discovery(
                base_url=WEATHER_MARKET_DISCOVERY_BASE_URL,
                markets_endpoint=WEATHER_MARKET_DISCOVERY_ENDPOINT,
                page_limit=WEATHER_MARKET_DISCOVERY_PAGE_LIMIT,
                max_pages=WEATHER_MARKET_DISCOVERY_MAX_PAGES,
                sleep_s=0.0,
                active_only=False,
                closed=None,
                archived=None,
                tag_slug=WEATHER_MARKET_DISCOVERY_TAG_SLUG,
                recent_within_days=None,
                client=client,
            )
            break
        except Exception as exc:  # noqa: BLE001
            errors.append(str(exc))
    if result is None:
        raise RuntimeError("; ".join(errors) if errors else "historical market discovery failed")
    cutoff_date = datetime.now(UTC).date() - timedelta(days=max(1, int(lookback_days)))
    supported: list[Any] = []
    for market in select_supported_markets(result.discovered_markets):
        target = market.close_time or market.end_date
        if not market.closed or target is None or target.date() < cutoff_date:
            continue
        supported.append(market)
        if len(supported) >= int(limit):
            break
    return supported


def _load_frozen_calibration_market() -> Any | None:
    market = normalize_weather_market(dict(FROZEN_REAL_GAMMA_MARKET))
    if market is None or market.closed is not True:
        return None
    return market


def _bootstrap_distribution_from_point_value(
    point_value: float,
    *,
    forecast_std_dev: float | None = None,
) -> dict[int, float]:
    if forecast_std_dev is not None and float(forecast_std_dev) > 0.0:
        return build_normal_distribution(float(point_value), max(0.5, float(forecast_std_dev)))
    low = math.floor(float(point_value))
    high = math.ceil(float(point_value))
    if low == high:
        return {int(low): 1.0}
    upper_weight = float(point_value) - float(low)
    lower_weight = 1.0 - upper_weight
    return {
        int(low): lower_weight,
        int(high): upper_weight,
    }


def _bootstrap_forecast_target_time(observation_date: date) -> datetime:
    return datetime.combine(observation_date - timedelta(days=1), datetime.min.time()).replace(hour=12)


def _build_historical_bootstrap_forecast_run(
    *,
    market,
    station_mapping: dict[str, Any],
    metric: str,
    observation_date: date,
    forecast_point_value: float,
    forecast_std_dev: float | None = None,
) -> ForecastRunRecord:
    forecast_target_time = _bootstrap_forecast_target_time(observation_date)
    distribution = _bootstrap_distribution_from_point_value(
        forecast_point_value,
        forecast_std_dev=forecast_std_dev,
    )
    payload = {
        "market_id": str(market.market_id),
        "station_id": str(station_mapping["station_id"]),
        "source": "openmeteo",
        "model_run": "historical_forecast_bootstrap_v1",
        "forecast_target_time": forecast_target_time.isoformat(),
        "observation_date": observation_date.isoformat(),
        "metric": metric,
        "bootstrap_mode": "historical_forecast_api_daily",
    }
    return ForecastRunRecord(
        run_id=stable_object_id("frun", payload),
        market_id=str(market.market_id),
        condition_id=str(market.condition_id),
        station_id=str(station_mapping["station_id"]),
        source="openmeteo",
        model_run="historical_forecast_bootstrap_v1",
        forecast_target_time=forecast_target_time,
        observation_date=observation_date,
        metric=metric,
        latitude=float(station_mapping["latitude"]),
        longitude=float(station_mapping["longitude"]),
        timezone=str(station_mapping["timezone"]),
        spec_version="historical_bootstrap_v1",
        cache_key=stable_object_id("fcache", payload),
        source_trace=["openmeteo_historical_forecast"],
        fallback_used=False,
        from_cache=False,
        confidence=0.85,
        forecast_payload={
            "temperature_distribution": distribution,
            "distribution_summary_v2": None,
        },
        raw_payload={
            "bootstrap_mode": "historical_forecast_api_daily",
            "historical_forecast_point_value": float(forecast_point_value),
        },
    )


def _build_synthetic_calibration_spec(
    template: WeatherMarketSpecRecord,
    *,
    observation_date: date,
) -> WeatherMarketSpecRecord:
    payload = {
        "station_id": template.station_id,
        "metric": template.metric,
        "observation_date": observation_date.isoformat(),
        "bucket_min_value": template.bucket_min_value,
        "bucket_max_value": template.bucket_max_value,
        "inclusive_bounds": template.inclusive_bounds,
    }
    synthetic_market_id = stable_object_id("calib_bootstrap_mkt", payload)
    return WeatherMarketSpecRecord(
        market_id=synthetic_market_id,
        condition_id=stable_object_id("cond", {"market_id": synthetic_market_id}),
        location_name=template.location_name,
        station_id=template.station_id,
        latitude=template.latitude,
        longitude=template.longitude,
        timezone=template.timezone,
        observation_date=observation_date,
        observation_window_local=template.observation_window_local,
        metric=template.metric,
        unit=template.unit,
        bucket_min_value=template.bucket_min_value,
        bucket_max_value=template.bucket_max_value,
        authoritative_source=template.authoritative_source,
        fallback_sources=list(template.fallback_sources),
        rounding_rule=template.rounding_rule,
        inclusive_bounds=template.inclusive_bounds,
        spec_version="synthetic_calibration_bootstrap_v1",
        parse_confidence=float(template.parse_confidence),
        risk_flags=list(template.risk_flags),
    )


def _build_synthetic_bootstrap_forecast_run(
    spec: WeatherMarketSpecRecord,
    *,
    forecast_point_value: float,
    forecast_std_dev: float | None = None,
) -> ForecastRunRecord:
    forecast_target_time = _bootstrap_forecast_target_time(spec.observation_date)
    distribution = _bootstrap_distribution_from_point_value(
        forecast_point_value,
        forecast_std_dev=forecast_std_dev,
    )
    payload = {
        "market_id": str(spec.market_id),
        "station_id": str(spec.station_id),
        "source": "openmeteo",
        "model_run": "synthetic_calibration_bootstrap_v3",
        "forecast_target_time": forecast_target_time.isoformat(),
        "observation_date": spec.observation_date.isoformat(),
        "metric": spec.metric,
    }
    return ForecastRunRecord(
        run_id=stable_object_id("frun", payload),
        market_id=str(spec.market_id),
        condition_id=str(spec.condition_id),
        station_id=str(spec.station_id),
        source="openmeteo",
        model_run="synthetic_calibration_bootstrap_v3",
        forecast_target_time=forecast_target_time,
        observation_date=spec.observation_date,
        metric=spec.metric,
        latitude=float(spec.latitude),
        longitude=float(spec.longitude),
        timezone=str(spec.timezone),
        spec_version=str(spec.spec_version),
        cache_key=stable_object_id("fcache", payload),
        source_trace=["openmeteo_historical_forecast"],
        fallback_used=False,
        from_cache=False,
        confidence=0.8,
        forecast_payload={
            "temperature_distribution": distribution,
            "distribution_summary_v2": None,
        },
        raw_payload={
            "bootstrap_mode": "synthetic_active_template_series_v3",
            "historical_forecast_point_value": float(forecast_point_value),
            "historical_forecast_std_dev": None if forecast_std_dev is None else float(forecast_std_dev),
        },
    )


def _build_station_metadata_for_market(market) -> StationMetadata:
    draft = parse_rule2spec_draft(market)
    station_mapping = build_station_mapping_for_market(market)
    return StationMetadata(
        station_id=str(station_mapping["station_id"]),
        location_name=str(draft.location_name),
        latitude=float(station_mapping["latitude"]),
        longitude=float(station_mapping["longitude"]),
        timezone=str(station_mapping["timezone"]),
        source="weather_station_smoke_catalog",
    )


def _load_active_calibration_coverage_gaps(
    db_path: Path,
    *,
    source: str,
) -> set[tuple[str, str]]:
    if not db_path.exists():
        return set()
    with reader_connection(db_path) as con:
        try:
            active_pairs = {
                (str(row[0]), str(row[1]))
                for row in con.execute(
                    """
                    SELECT DISTINCT spec.station_id, spec.metric
                    FROM weather.weather_market_specs spec
                    JOIN weather.weather_markets m USING (market_id)
                    WHERE lower(coalesce(m.status, '')) IN ('active', 'open')
                    """
                ).fetchall()
                if row and row[0] is not None and row[1] is not None
            }
            covered_pairs = {
                (str(row[0]), str(row[1]))
                for row in con.execute(
                    """
                    SELECT DISTINCT station_id, metric
                    FROM weather.forecast_calibration_profiles_v2
                    WHERE lower(coalesce(source, '')) = lower(?)
                    """,
                    [str(source)],
                ).fetchall()
                if row and row[0] is not None and row[1] is not None
            }
        except Exception:  # noqa: BLE001
            return set()
    return active_pairs - covered_pairs


def _load_active_calibration_quality_gaps(
    db_path: Path,
    *,
    source: str,
) -> set[tuple[str, str]]:
    if not db_path.exists():
        return set()
    with reader_connection(db_path) as con:
        try:
            active_pairs = {
                (str(row[0]), str(row[1]))
                for row in con.execute(
                    """
                    SELECT DISTINCT spec.station_id, spec.metric
                    FROM weather.weather_market_specs spec
                    JOIN weather.weather_markets m USING (market_id)
                    WHERE lower(coalesce(m.status, '')) IN ('active', 'open')
                    """
                ).fetchall()
                if row and row[0] is not None and row[1] is not None
            }
            latest_profiles = con.execute(
                """
                WITH ranked AS (
                    SELECT
                        station_id,
                        metric,
                        calibration_health_status,
                        ROW_NUMBER() OVER (
                            PARTITION BY station_id, metric
                            ORDER BY materialized_at DESC, profile_key DESC
                        ) AS rn
                    FROM weather.forecast_calibration_profiles_v2
                    WHERE lower(coalesce(source, '')) = lower(?)
                )
                SELECT station_id, metric, calibration_health_status
                FROM ranked
                WHERE rn = 1
                """,
                [str(source)],
            ).fetchall()
        except Exception:  # noqa: BLE001
            return set()
    healthy_pairs = {
        (str(row[0]), str(row[1]))
        for row in latest_profiles
        if row
        and row[0] is not None
        and row[1] is not None
        and str(row[2] or "").strip().lower() in {"healthy", "watch"}
    }
    return active_pairs - healthy_pairs


def _load_active_template_specs_for_coverage(
    db_path: Path,
    *,
    coverage_gaps: set[tuple[str, str]],
) -> dict[tuple[str, str], list[WeatherMarketSpecRecord]]:
    if not coverage_gaps or not db_path.exists():
        return {}
    templates: dict[tuple[str, str], list[WeatherMarketSpecRecord]] = {}
    with reader_connection(db_path) as con:
        rows = con.execute(
            """
            SELECT spec.market_id
            FROM weather.weather_market_specs spec
            JOIN weather.weather_markets m USING (market_id)
            WHERE lower(coalesce(m.status, '')) IN ('active', 'open')
            ORDER BY spec.observation_date ASC, spec.bucket_min_value ASC NULLS LAST, spec.market_id ASC
            """
        ).fetchall()
        for row in rows:
            market_id = str(row[0])
            spec = load_weather_market_spec(con, market_id=market_id)
            key = (str(spec.station_id), str(spec.metric))
            if key not in coverage_gaps:
                continue
            bucket = templates.setdefault(key, [])
            if len(bucket) >= 10:
                continue
            bucket.append(spec)
    return templates


def _series_residual_std_dev(
    *,
    forecast_series: dict[date, float],
    observed_series: dict[date, float],
) -> float | None:
    residuals = [
        float(observed_series[day]) - float(forecast_series[day])
        for day in sorted(set(forecast_series).intersection(observed_series))
        if forecast_series.get(day) is not None and observed_series.get(day) is not None
    ]
    if not residuals:
        return None
    if len(residuals) == 1:
        return max(1.0, abs(float(residuals[0])))
    abs_residuals = sorted(abs(float(value)) for value in residuals)
    p90_index = min(len(abs_residuals) - 1, max(0, math.ceil(len(abs_residuals) * 0.9) - 1))
    p90_abs_residual = abs_residuals[p90_index]
    robust_scale = max(
        float(pstdev(residuals)),
        float(p90_abs_residual) / 1.28155 if p90_abs_residual > 0 else 0.0,
    )
    return max(1.0, min(6.0, robust_scale * 1.25))


def _purge_synthetic_bootstrap_artifacts(
    *,
    db_path: Path,
    station_metric_pairs: set[tuple[str, str]],
) -> dict[str, Any]:
    if not station_metric_pairs or not db_path.exists():
        return {
            "deleted_sample_ids": set(),
            "deleted_sample_count": 0,
            "deleted_forecast_run_count": 0,
        }

    pair_list = sorted({(str(station_id), str(metric)) for station_id, metric in station_metric_pairs})
    pair_predicate = " OR ".join(["(r.station_id = ? AND r.metric = ?)"] * len(pair_list))
    params = [item for pair in pair_list for item in pair]
    deleted_sample_ids: set[str] = set()

    with writer_connection(db_path) as con:
        sample_rows = con.execute(
            f"""
            SELECT DISTINCT s.sample_id
            FROM weather.forecast_calibration_samples s
            WHERE EXISTS (
                SELECT 1
                FROM weather.weather_forecast_runs r
                WHERE r.market_id = s.market_id
                  AND r.station_id = s.station_id
                  AND r.source = s.source
                  AND r.forecast_target_time = s.forecast_target_time
                  AND r.metric = s.metric
                  AND r.model_run LIKE 'synthetic_calibration_bootstrap_%'
                  AND ({pair_predicate})
            )
            """,
            params,
        ).fetchall()
        deleted_sample_ids = {str(row[0]) for row in sample_rows if row and row[0] is not None}
        if deleted_sample_ids:
            sample_placeholders = ",".join(["?"] * len(deleted_sample_ids))
            con.execute(
                f"DELETE FROM weather.forecast_calibration_samples WHERE sample_id IN ({sample_placeholders})",
                list(sorted(deleted_sample_ids)),
            )

        forecast_run_rows = con.execute(
            f"""
            SELECT DISTINCT r.run_id
            FROM weather.weather_forecast_runs r
            WHERE r.model_run LIKE 'synthetic_calibration_bootstrap_%'
              AND ({pair_predicate})
            """,
            params,
        ).fetchall()
        deleted_run_ids = [str(row[0]) for row in forecast_run_rows if row and row[0] is not None]
        if deleted_run_ids:
            run_placeholders = ",".join(["?"] * len(deleted_run_ids))
            con.execute(
                f"DELETE FROM weather.weather_forecast_runs WHERE run_id IN ({run_placeholders})",
                deleted_run_ids,
            )

    return {
        "deleted_sample_ids": deleted_sample_ids,
        "deleted_sample_count": len(deleted_sample_ids),
        "deleted_forecast_run_count": len(deleted_run_ids),
    }


def _bootstrap_calibration_samples_from_archive(
    *,
    db_path: Path,
    queue_cfg: WriteQueueConfig,
    allow_tables: list[str],
    lookback_days: int = 365,
    force_rebuild: bool = False,
) -> dict[str, Any]:
    if not db_path.exists():
        return {"status": "failed", "error": f"canonical db missing: {db_path}"}
    window_start = datetime.now(UTC).date() - timedelta(days=max(1, int(lookback_days)))
    archive_client = RetryHttpClient(HttpJsonClient(timeout_seconds=10.0), max_retries=3, initial_delay=0.5)
    observation_cache: dict[tuple[float, float, str, date, str], float] = {}
    forecast_cache: dict[tuple[float, float, str, date, str], float] = {}
    sample_count = 0
    matured_forecast_count = 0
    matured_missing_sample_count = 0
    historical_supported_market_count = 0
    historical_forecast_run_count = 0
    historical_spec_count = 0
    synthetic_spec_count = 0
    synthetic_forecast_run_count = 0
    synthetic_sample_count = 0
    synthetic_missing_date_count = 0
    used_frozen_market_fallback = False
    skipped_existing = 0
    purged_synthetic_sample_count = 0
    purged_synthetic_forecast_run_count = 0
    errors: list[str] = []
    samples = []
    forecast_runs: list[ForecastRunRecord] = []
    bootstrap_specs = []
    with reader_connection(db_path) as con:
        try:
            forecast_rows = con.execute(
                """
                SELECT
                    r.run_id,
                    r.market_id,
                    r.station_id,
                    r.source,
                    r.forecast_target_time,
                    r.latitude,
                    r.longitude,
                    r.timezone,
                    r.observation_date,
                    r.metric
                FROM weather.weather_forecast_runs r
                WHERE r.observation_date < ?
                  AND r.observation_date >= ?
                  AND NOT EXISTS (
                      SELECT 1
                      FROM weather.forecast_calibration_samples s
                      WHERE s.market_id = r.market_id
                        AND s.station_id = r.station_id
                        AND lower(coalesce(s.source, '')) = lower(coalesce(r.source, ''))
                        AND s.metric = r.metric
                        AND s.forecast_target_time = r.forecast_target_time
                  )
                ORDER BY r.observation_date DESC, r.forecast_target_time DESC, r.run_id DESC
                """,
                [datetime.now(UTC).date(), window_start],
            ).fetchall()
        except Exception as exc:  # noqa: BLE001
            return {"status": "failed", "error": str(exc)}
        existing_sample_ids = {
            str(row[0])
            for row in con.execute("SELECT sample_id FROM weather.forecast_calibration_samples").fetchall()
        } if _count_rows(con, "weather.forecast_calibration_samples") > 0 else set()
        profile_count = _count_rows(con, "weather.forecast_calibration_profiles_v2") if _table_exists(con, "weather.forecast_calibration_profiles_v2") else 0
        existing_spec_market_ids = {
            str(row[0])
            for row in con.execute("SELECT market_id FROM weather.weather_market_specs").fetchall()
        } if _count_rows(con, "weather.weather_market_specs") > 0 else set()
        sample_market_ids_missing_specs = {
            str(row[0])
            for row in con.execute("SELECT DISTINCT market_id FROM weather.forecast_calibration_samples").fetchall()
            if row and row[0] is not None and str(row[0]) not in existing_spec_market_ids
        }
        active_coverage_gaps = _load_active_calibration_coverage_gaps(
            db_path,
            source="openmeteo",
        )
        active_quality_gaps = _load_active_calibration_quality_gaps(
            db_path,
            source="openmeteo",
        )
        for row in forecast_rows:
            matured_forecast_count += 1
            run_id = str(row[0])
            matured_missing_sample_count += 1
            try:
                forecast_run = load_forecast_run(con, run_id=run_id)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{run_id}: {exc}")
                continue
            cache_key = (
                float(row[5]),
                float(row[6]),
                str(row[7]),
                row[8],
                str(row[9]),
            )
            try:
                observed_value = observation_cache.get(cache_key)
                if observed_value is None:
                    observed_value = _fetch_observed_value_from_archive(
                        archive_client,
                        latitude=float(row[5]),
                        longitude=float(row[6]),
                        observation_date=row[8],
                        timezone_name=str(row[7]),
                        metric=str(row[9]),
                    )
                    observation_cache[cache_key] = observed_value
                sample = build_forecast_calibration_sample(
                    forecast_run=forecast_run,
                    observed_value=observed_value,
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{run_id}: {exc}")
                continue
            if sample.sample_id in existing_sample_ids:
                skipped_existing += 1
                continue
            samples.append(sample)
            existing_sample_ids.add(sample.sample_id)

        should_bootstrap_history = bool(
            profile_count <= 0
            or sample_market_ids_missing_specs
            or active_coverage_gaps
        )
        if should_bootstrap_history:
            historical_limit = max(
                1,
                min(
                    CALIBRATION_BOOTSTRAP_MARKET_LIMIT,
                    len(active_coverage_gaps) if active_coverage_gaps else 1,
                ),
            )
            try:
                historical_markets = _discover_supported_closed_weather_markets_for_calibration(
                    lookback_days=lookback_days,
                    limit=historical_limit,
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"historical_market_discovery: {exc}")
                historical_markets = []
            historical_markets = list(historical_markets[:historical_limit])
            if not historical_markets:
                frozen_market = _load_frozen_calibration_market()
                if frozen_market is not None:
                    historical_markets = [frozen_market]
                    used_frozen_market_fallback = True
            historical_supported_market_count = len(historical_markets)
            remaining_gap_pairs = set(active_coverage_gaps)
            for market in historical_markets:
                if active_coverage_gaps and not remaining_gap_pairs:
                    break
                try:
                    draft = parse_rule2spec_draft(market)
                    station_mapping = build_station_mapping_for_market(market)
                    station_id = str(station_mapping["station_id"])
                    metric = str(draft.metric)
                    market_id = str(market.market_id)
                    needs_spec = market_id in sample_market_ids_missing_specs
                    needs_coverage = (station_id, metric) in active_coverage_gaps
                    if not needs_spec and not needs_coverage and profile_count > 0:
                        continue
                    if market_id not in existing_spec_market_ids:
                        bootstrap_specs.append(
                            build_weather_market_spec_record(
                                draft,
                                station_metadata=_build_station_metadata_for_market(market),
                                spec_version="historical_bootstrap_v1",
                            )
                        )
                        existing_spec_market_ids.add(market_id)
                        historical_spec_count += 1
                    observation_date = draft.observation_date
                    cache_key = (
                        float(station_mapping["latitude"]),
                        float(station_mapping["longitude"]),
                        str(station_mapping["timezone"]),
                        observation_date,
                        str(draft.metric),
                    )
                    forecast_point_value = forecast_cache.get(cache_key)
                    if forecast_point_value is None:
                        forecast_point_value = _fetch_historical_forecast_value(
                            archive_client,
                            latitude=float(station_mapping["latitude"]),
                            longitude=float(station_mapping["longitude"]),
                            observation_date=observation_date,
                            timezone_name=str(station_mapping["timezone"]),
                            metric=str(draft.metric),
                        )
                        forecast_cache[cache_key] = forecast_point_value
                    observed_value = observation_cache.get(cache_key)
                    if observed_value is None:
                        observed_value = _fetch_observed_value_from_archive(
                            archive_client,
                            latitude=float(station_mapping["latitude"]),
                            longitude=float(station_mapping["longitude"]),
                            observation_date=observation_date,
                            timezone_name=str(station_mapping["timezone"]),
                            metric=str(draft.metric),
                        )
                        observation_cache[cache_key] = observed_value
                    forecast_run = _build_historical_bootstrap_forecast_run(
                        market=market,
                        station_mapping=station_mapping,
                        metric=str(draft.metric),
                        observation_date=observation_date,
                        forecast_point_value=float(forecast_point_value),
                        forecast_std_dev=1.5,
                    )
                    sample = build_forecast_calibration_sample(
                        forecast_run=forecast_run,
                        observed_value=float(observed_value),
                    )
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"historical:{getattr(market, 'market_id', 'unknown')}: {exc}")
                    continue
                if sample.sample_id in existing_sample_ids:
                    skipped_existing += 1
                    continue
                forecast_runs.append(forecast_run)
                samples.append(sample)
                existing_sample_ids.add(sample.sample_id)
                remaining_gap_pairs.discard((station_id, metric))

    active_bootstrap_pairs = set(active_coverage_gaps)
    if active_bootstrap_pairs:
        if force_rebuild:
            purge_result = _purge_synthetic_bootstrap_artifacts(
                db_path=db_path,
                station_metric_pairs=active_bootstrap_pairs,
            )
            purged_synthetic_sample_count = int(purge_result["deleted_sample_count"])
            purged_synthetic_forecast_run_count = int(purge_result["deleted_forecast_run_count"])
            existing_sample_ids.difference_update(set(purge_result["deleted_sample_ids"]))
        template_specs = _load_active_template_specs_for_coverage(
            db_path,
            coverage_gaps=active_bootstrap_pairs,
        )
        for (station_id, metric), templates in template_specs.items():
            if not templates:
                continue
            template = templates[0]
            anchor_date = max(item.observation_date for item in templates) - timedelta(days=365)
            start_date = anchor_date - timedelta(days=SYNTHETIC_CALIBRATION_BOOTSTRAP_DAYS - 1)
            try:
                forecast_series = _fetch_historical_forecast_series(
                    archive_client,
                    latitude=float(template.latitude),
                    longitude=float(template.longitude),
                    start_date=start_date,
                    end_date=anchor_date,
                    timezone_name=str(template.timezone),
                    metric=str(metric),
                )
                observed_series = _fetch_observed_series_from_archive(
                    archive_client,
                    latitude=float(template.latitude),
                    longitude=float(template.longitude),
                    start_date=start_date,
                    end_date=anchor_date,
                    timezone_name=str(template.timezone),
                    metric=str(metric),
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"synthetic:{station_id}:{metric}: {exc}")
                continue
            series_std_dev = _series_residual_std_dev(
                forecast_series=forecast_series,
                observed_series=observed_series,
            )
            bootstrap_dates = sorted(set(forecast_series).intersection(observed_series))
            missing_dates_for_pair: set[date] = set()
            for bootstrap_date in bootstrap_dates:
                forecast_point_value = forecast_series.get(bootstrap_date)
                observed_value = observed_series.get(bootstrap_date)
                if forecast_point_value is None or observed_value is None:
                    continue
                for template_spec in templates:
                    synthetic_spec = _build_synthetic_calibration_spec(
                        template_spec,
                        observation_date=bootstrap_date,
                    )
                    if synthetic_spec.market_id not in existing_spec_market_ids:
                        bootstrap_specs.append(synthetic_spec)
                        existing_spec_market_ids.add(synthetic_spec.market_id)
                        synthetic_spec_count += 1
                    forecast_run = _build_synthetic_bootstrap_forecast_run(
                        synthetic_spec,
                        forecast_point_value=float(forecast_point_value),
                        forecast_std_dev=series_std_dev,
                    )
                    sample = build_forecast_calibration_sample(
                        forecast_run=forecast_run,
                        observed_value=float(observed_value),
                    )
                    if sample.sample_id in existing_sample_ids:
                        skipped_existing += 1
                        continue
                    forecast_runs.append(forecast_run)
                    samples.append(sample)
                    existing_sample_ids.add(sample.sample_id)
                    synthetic_forecast_run_count += 1
                    synthetic_sample_count += 1
                    missing_dates_for_pair.add(bootstrap_date)
            synthetic_missing_date_count += len(missing_dates_for_pair)

    if bootstrap_specs:
        enqueue_weather_market_spec_upserts(
            queue_cfg,
            specs=bootstrap_specs,
            run_id="run_real_weather_chain_historical_spec_bootstrap",
        )
        drain_queue(queue_path=_resolve_queue_path(db_path), db_path=db_path, allow_tables=allow_tables)

    if forecast_runs:
        enqueue_forecast_run_upserts(
            queue_cfg,
            forecast_runs=forecast_runs,
            run_id="run_real_weather_chain_historical_forecast_bootstrap",
        )
        drain_queue(queue_path=_resolve_queue_path(db_path), db_path=db_path, allow_tables=allow_tables)
        historical_forecast_run_count = len(forecast_runs)

    if samples:
        enqueue_forecast_calibration_sample_upserts(
            queue_cfg,
            samples=samples,
            run_id="run_real_weather_chain_calibration_bootstrap",
        )
        drain_queue(queue_path=_resolve_queue_path(db_path), db_path=db_path, allow_tables=allow_tables)
        sample_count = len(samples)

    if matured_forecast_count <= 0 and historical_supported_market_count <= 0:
        status = "idle_no_matured_forecasts"
    elif sample_count > 0 and errors:
        status = "degraded"
    elif sample_count > 0:
        status = "ok"
    elif errors:
        status = "failed"
    else:
        status = "skipped_existing_samples"
    return {
        "status": status,
        "bootstrap_mode": "full_rebuild" if force_rebuild else "incremental",
        "matured_forecast_count": matured_forecast_count,
        "matured_missing_sample_count": matured_missing_sample_count,
        "historical_supported_market_count": historical_supported_market_count,
        "historical_gap_pair_count": len(active_coverage_gaps),
        "historical_forecast_run_count": historical_forecast_run_count,
        "historical_spec_count": historical_spec_count,
        "synthetic_spec_count": synthetic_spec_count,
        "synthetic_forecast_run_count": synthetic_forecast_run_count,
        "synthetic_sample_count": synthetic_sample_count,
        "synthetic_missing_date_count": synthetic_missing_date_count,
        "missing_sample_spec_market_count": len(sample_market_ids_missing_specs),
        "active_calibration_coverage_gap_count": len(active_coverage_gaps),
        "active_calibration_quality_gap_count": len(active_quality_gaps),
        "used_frozen_market_fallback": used_frozen_market_fallback,
        "bootstrap_subject_limit": CALIBRATION_BOOTSTRAP_MARKET_LIMIT,
        "calibration_sample_count": sample_count,
        "skipped_existing_sample_count": skipped_existing,
        "synthetic_rebuild_mode": (
            "forced_rebuild"
            if force_rebuild and active_bootstrap_pairs
            else "incremental_only"
            if active_bootstrap_pairs
            else "none"
        ),
        "purged_synthetic_sample_count": purged_synthetic_sample_count,
        "purged_synthetic_forecast_run_count": purged_synthetic_forecast_run_count,
        "error_count": len(errors),
        "errors": errors[:10],
    }


def _run_prepricing_jobs(
    *,
    db_path: Path,
    queue_cfg: WriteQueueConfig,
    allow_tables: list[str],
    force_rebuild: bool = False,
) -> dict[str, Any]:
    settings = AsterionColdPathSettings.from_env()
    capability_runtime = CapabilityRefreshRuntimeResource(settings=settings)
    metadata: dict[str, Any] = {
        "watcher_backfill": {"status": "not_run"},
        "capability_refresh": {"status": "not_run"},
        "allocation_preview": {"status": "not_run"},
        "paper_execution": {"status": "not_run"},
        "operator_surface_refresh": {"status": "not_run"},
        "opportunity_triage": {"status": "not_run"},
        "resolution_review": {"status": "not_run"},
        "resolution_reconciliation": {"status": "not_run"},
        "calibration_bootstrap": {"status": "not_run"},
        "calibration_refresh": {"status": "not_run"},
        "selected_snapshot_ids": [],
    }

    try:
        with reader_connection(db_path) as con:
            result = run_weather_capability_refresh_job(
                con,
                queue_cfg,
                clob_client=capability_runtime.build_clob_client(),
                wallet_registry_path=capability_runtime.resolve_wallet_registry_path(),
                chain_reader=_PaperDefaultChainAccountCapabilityReader(),
                observed_at=datetime.now(UTC),
            )
        drain_queue(queue_path=_resolve_queue_path(db_path), db_path=db_path, allow_tables=allow_tables)
        metadata["capability_refresh"] = {"status": "ok", **result.metadata}
    except Exception as exc:  # noqa: BLE001
        metadata["capability_refresh"] = {"status": "failed", "error": str(exc)}
        return metadata

    metadata["watcher_backfill"] = _run_watcher_backfill_step(
        db_path=db_path,
        queue_cfg=queue_cfg,
        allow_tables=allow_tables,
        settings=settings,
    )
    verification_inputs, verification_metadata = _build_realtime_settlement_verification_inputs(db_path=db_path)
    try:
        with reader_connection(db_path) as con:
            reconciliation_result = run_weather_resolution_reconciliation(
                con,
                queue_cfg,
                verification_inputs=verification_inputs,
            )
        drain_queue(queue_path=_resolve_queue_path(db_path), db_path=db_path, allow_tables=allow_tables)
        if int(reconciliation_result.metadata.get("verification_count") or 0) > 0:
            metadata["resolution_reconciliation"] = {
                "status": "ok",
                **verification_metadata,
                **reconciliation_result.metadata,
            }
        else:
            metadata["resolution_reconciliation"] = {
                "status": verification_metadata.get("status") or "idle_no_subjects",
                **verification_metadata,
                **reconciliation_result.metadata,
            }
    except Exception as exc:  # noqa: BLE001
        metadata["resolution_reconciliation"] = {"status": "failed", "error": str(exc)}

    metadata["calibration_bootstrap"] = _bootstrap_calibration_samples_from_archive(
        db_path=db_path,
        queue_cfg=queue_cfg,
        allow_tables=allow_tables,
        force_rebuild=force_rebuild,
    )

    try:
        with reader_connection(db_path) as con:
            calibration_result = run_weather_forecast_calibration_profiles_v2_refresh_job(
                con,
                queue_cfg,
                as_of=datetime.now(UTC),
            )
        drain_queue(queue_path=_resolve_queue_path(db_path), db_path=db_path, allow_tables=allow_tables)
        metadata["calibration_refresh"] = {"status": "ok", **calibration_result.metadata}
    except Exception as exc:  # noqa: BLE001
        metadata["calibration_refresh"] = {"status": "failed", "error": str(exc)}

    return metadata


def _run_postpricing_jobs(
    *,
    db_path: Path,
    queue_cfg: WriteQueueConfig,
    allow_tables: list[str],
    triage_limit: int | None = None,
) -> dict[str, Any]:
    bootstrap_state = _bootstrap_local_paper_operator_state(db_path)
    metadata: dict[str, Any] = {
        "paper_bootstrap": {"status": "ok", **bootstrap_state},
        "allocation_preview": {"status": "not_run"},
        "paper_execution": {"status": "not_run"},
        "execution_priors_refresh": {"status": "not_run"},
        "ranking_retrospective_refresh": {"status": "not_run"},
        "operator_surface_refresh": {"status": "not_run"},
        "opportunity_triage": {"status": "not_run"},
        "resolution_review": {"status": "not_run"},
        "selected_snapshot_ids": [],
    }
    selected_snapshot_ids = _load_latest_snapshot_ids(db_path)
    metadata["selected_snapshot_ids"] = selected_snapshot_ids
    if not selected_snapshot_ids:
        metadata["allocation_preview"] = {
            "status": "skipped",
            "reason": "no_deployable_snapshots",
        }
        metadata["paper_execution"] = {
            "status": "skipped",
            "reason": "no_deployable_snapshots",
        }
    else:
        params_json = {
            "wallet_id": DEFAULT_WALLET_ID,
            "strategy_registrations": list(DEFAULT_STRATEGY_REGISTRATIONS),
            "snapshot_ids": list(selected_snapshot_ids),
        }
        try:
            with writer_connection(db_path) as con:
                preview_result = run_weather_allocation_preview_refresh_job(
                    con,
                    queue_cfg,
                    params_json=params_json,
                    observed_at=datetime.now(UTC),
                )
            drain_queue(queue_path=_resolve_queue_path(db_path), db_path=db_path, allow_tables=allow_tables)
            metadata["allocation_preview"] = {"status": "ok", **preview_result.metadata}
        except Exception as exc:  # noqa: BLE001
            metadata["allocation_preview"] = {"status": "failed", "error": str(exc)}

        try:
            with writer_connection(db_path) as con:
                paper_result = run_weather_paper_execution_job(
                    con,
                    queue_cfg,
                    params_json=params_json,
                    observed_at=datetime.now(UTC),
                )
            drain_queue(queue_path=_resolve_queue_path(db_path), db_path=db_path, allow_tables=allow_tables)
            metadata["paper_execution"] = {"status": "ok", **paper_result.metadata}
        except Exception as exc:  # noqa: BLE001
            metadata["paper_execution"] = {"status": "failed", "error": str(exc)}

    try:
        with writer_connection(db_path) as con:
            priors_result = run_weather_execution_priors_refresh_job(
                con,
                queue_cfg,
                as_of=datetime.now(UTC),
            )
        drain_queue(queue_path=_resolve_queue_path(db_path), db_path=db_path, allow_tables=allow_tables)
        metadata["execution_priors_refresh"] = {
            "status": priors_result.metadata.get("feedback_writeback_status") or "ok",
            **priors_result.metadata,
        }
    except Exception as exc:  # noqa: BLE001
        metadata["execution_priors_refresh"] = {"status": "failed", "error": str(exc)}

    try:
        with writer_connection(db_path) as con:
            retrospective_result = run_weather_ranking_retrospective_refresh_job(
                con,
                queue_cfg,
                as_of=datetime.now(UTC),
            )
        drain_queue(queue_path=_resolve_queue_path(db_path), db_path=db_path, allow_tables=allow_tables)
        metadata["ranking_retrospective_refresh"] = {"status": "ok", **retrospective_result.metadata}
    except Exception as exc:  # noqa: BLE001
        metadata["ranking_retrospective_refresh"] = {"status": "failed", "error": str(exc)}

    try:
        with writer_connection(db_path) as con:
            refresh_metadata = run_operator_surface_refresh(
                con,
                job_name="real_weather_chain_smoke",
                trigger_mode="manual",
                ui_replica_db_path=str(default_ui_db_replica_path()),
                ui_replica_meta_path=str(default_ui_replica_meta_path(replica_db_path=default_ui_db_replica_path())),
                ui_lite_db_path=str(default_ui_lite_db_path()),
                ui_lite_meta_path=str(default_ui_lite_meta_path(lite_db_path=default_ui_lite_db_path())),
                readiness_report_json_path=os.getenv("ASTERION_READINESS_REPORT_JSON_PATH", "data/ui/asterion_readiness_p4.json"),
                readiness_evidence_json_path=os.getenv("ASTERION_READINESS_EVIDENCE_JSON_PATH", "data/ui/asterion_readiness_evidence_p4.json"),
            )
        metadata["operator_surface_refresh"] = {"status": "ok", **refresh_metadata}
    except Exception as exc:  # noqa: BLE001
        metadata["operator_surface_refresh"] = {"status": "failed", "error": str(exc)}

    try:
        with writer_connection(db_path) as con:
            triage_result = run_weather_opportunity_triage_review_job(
                con,
                queue_cfg,
                market_ids=None,
                limit=max(1, int(triage_limit)) if triage_limit is not None and int(triage_limit) > 0 else None,
                force_rerun=False,
            )
        drain_queue(queue_path=_resolve_queue_path(db_path), db_path=db_path, allow_tables=allow_tables)
        if triage_result.item_count <= 0:
            metadata["opportunity_triage"] = {
                "status": "idle_no_subjects",
                "item_count": triage_result.item_count,
                "requested_limit": max(1, int(triage_limit)) if triage_limit is not None and int(triage_limit) > 0 else None,
                **triage_result.metadata,
            }
        else:
            metadata["opportunity_triage"] = {
                "status": "ok",
                "item_count": triage_result.item_count,
                "requested_limit": max(1, int(triage_limit)) if triage_limit is not None and int(triage_limit) > 0 else None,
                **triage_result.metadata,
            }
    except Exception as exc:  # noqa: BLE001
        metadata["opportunity_triage"] = {"status": "failed", "error": str(exc)}

    try:
        with writer_connection(db_path) as con:
            resolution_result = run_weather_resolution_review_job(
                con,
                queue_cfg,
                proposal_ids=None,
                force_rerun=False,
            )
        drain_queue(queue_path=_resolve_queue_path(db_path), db_path=db_path, allow_tables=allow_tables)
        if resolution_result.item_count <= 0:
            metadata["resolution_review"] = {
                "status": "idle_no_subjects",
                "item_count": resolution_result.item_count,
                **resolution_result.metadata,
            }
        else:
            metadata["resolution_review"] = {
                "status": "ok",
                "item_count": resolution_result.item_count,
                **resolution_result.metadata,
            }
    except Exception as exc:  # noqa: BLE001
        metadata["resolution_review"] = {"status": "failed", "error": str(exc)}

    return metadata


def _merge_runtime_chain(*sections: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for section in sections:
        merged.update(section)
    return merged


def _truth_source_split_brain_status(*, canonical_db_path: Path) -> bool:
    smoke_db_path = ROOT / "data" / "dev" / "real_weather_chain" / "real_weather_chain.duckdb"
    if smoke_db_path == canonical_db_path or not smoke_db_path.exists():
        return False
    canonical_counts = _table_count_map(
        canonical_db_path,
        ["weather.weather_forecast_runs", "weather.weather_watch_only_snapshots", "ui.market_opportunity_summary"],
    )
    smoke_counts = _table_count_map(
        smoke_db_path,
        ["weather.weather_forecast_runs", "weather.weather_watch_only_snapshots"],
    )
    return (
        int(smoke_counts.get("weather.weather_forecast_runs", 0)) > 0
        and int(canonical_counts.get("weather.weather_forecast_runs", 0)) == 0
    ) or (
        int(smoke_counts.get("weather.weather_watch_only_snapshots", 0)) > 0
        and int(canonical_counts.get("weather.weather_watch_only_snapshots", 0)) == 0
    )


def _calibration_gate_breakdown(snapshots: list[Any]) -> dict[str, int]:
    breakdown: dict[str, int] = {}
    for snapshot in snapshots:
        pricing_context = getattr(snapshot, "pricing_context", {}) or {}
        status = str(pricing_context.get("calibration_gate_status") or pricing_context.get("calibration_health_status") or "unknown")
        breakdown[status] = breakdown.get(status, 0) + 1
    return dict(sorted(breakdown.items()))


def _prior_lookup_breakdown(snapshots: list[Any]) -> dict[str, int]:
    breakdown: dict[str, int] = {}
    for snapshot in snapshots:
        pricing_context = getattr(snapshot, "pricing_context", {}) or {}
        mode = str(pricing_context.get("prior_lookup_mode") or "").strip()
        if not mode and str(pricing_context.get("execution_prior_key") or "").strip():
            mode = "empirical_primary"
        if not mode:
            mode = "missing"
        breakdown[mode] = breakdown.get(mode, 0) + 1
    return dict(sorted(breakdown.items()))


def _deployable_snapshot_count(snapshots: list[Any]) -> int:
    return sum(
        1
        for snapshot in snapshots
        if str(getattr(snapshot, "decision", "")).strip().upper() != "NO_TRADE"
        and str(getattr(snapshot, "side", "")).strip().upper() in {"BUY", "SELL"}
    )


def _execution_intelligence_covered_snapshot_count(snapshots: list[Any]) -> int:
    covered = 0
    for snapshot in snapshots:
        if str(getattr(snapshot, "decision", "")).strip().upper() == "NO_TRADE":
            continue
        if str(getattr(snapshot, "side", "")).strip().upper() not in {"BUY", "SELL"}:
            continue
        pricing_context = getattr(snapshot, "pricing_context", {}) or {}
        if str(pricing_context.get("execution_intelligence_summary_id") or "").strip():
            covered += 1
            continue
        score = pricing_context.get("execution_intelligence_score")
        if score is not None and float(score or 0.0) > 0.0:
            covered += 1
            continue
        reason_codes = pricing_context.get("microstructure_reason_codes") or pricing_context.get("execution_intelligence_reason_codes")
        if isinstance(reason_codes, list) and reason_codes:
            covered += 1
    return covered


def _active_market_prior_hit_count(snapshots: list[Any]) -> int:
    return len(
        {
            str(getattr(snapshot, "market_id", "")).strip()
            for snapshot in snapshots
            if str(((getattr(snapshot, "pricing_context", {}) or {}).get("execution_prior_key") or "")).strip()
        }
    )


def _build_agent_pipeline(
    *,
    db_path: Path,
    ui_counts: dict[str, int],
    runtime_chain: dict[str, Any],
    resolution_counts_override: dict[str, int] | None = None,
) -> dict[str, Any]:
    triage_counts = _agent_counts_by_type(db_path, agent_type="opportunity_triage")
    triage_breakdown = _agent_invocation_breakdown_by_type(db_path, agent_type="opportunity_triage")
    resolution_counts = resolution_counts_override or _agent_counts_by_type(db_path, agent_type="resolution")
    triage_decision_counts = _triage_operator_decision_counts(db_path)
    latest_triage = runtime_chain.get("opportunity_triage") or {}
    latest_resolution = runtime_chain.get("resolution_review") or {}
    latest_quality_counts = _latest_triage_quality_counts(
        db_path,
        subject_ids=list(latest_triage.get("subject_ids") or []),
    )
    agent_running_status = (
        "failed"
        if str(latest_triage.get("status") or "") == "failed"
        else "idle_no_subjects"
        if str(latest_triage.get("status") or "") == "idle_no_subjects"
        else "ok"
        if int(triage_counts["invocation_count"]) > 0
        and str(latest_triage.get("status") or "") not in {"failed", "not_run", ""}
        else "idle"
    )
    agent_value_status = (
        "useful"
        if any(
            int(latest_quality_counts[key]) > 0
            for key in [
                "latest_triage_non_fallback_output_count",
                "latest_triage_medium_or_high_confidence_count",
                "latest_triage_operator_review_count",
            ]
        )
        else "fallback_only"
        if int(latest_triage.get("output_count") or 0) > 0
        else "not_running"
    )
    return {
        "triage_invocation_count": triage_counts["invocation_count"],
        "triage_output_count": triage_counts["output_count"],
        "triage_evaluation_count": triage_counts["evaluation_count"],
        "triage_success_count": triage_breakdown["success_count"],
        "triage_failure_count": triage_breakdown["failure_count"],
        "triage_timeout_count": triage_breakdown["timeout_count"],
        "triage_parse_error_count": triage_breakdown["parse_error_count"],
        "triage_rate_limited_count": triage_breakdown["rate_limited_count"],
        "triage_provider_forbidden_count": triage_breakdown["provider_forbidden_count"],
        "latest_triage_item_count": int(latest_triage.get("item_count") or 0),
        "latest_triage_output_count": int(latest_triage.get("output_count") or 0),
        "latest_triage_subject_count": len(list(latest_triage.get("subject_ids") or [])),
        "latest_triage_requested_limit": latest_triage.get("requested_limit"),
        "latest_triage_status": latest_triage.get("status"),
        **latest_quality_counts,
        "resolution_invocation_count": resolution_counts["invocation_count"],
        "resolution_output_count": resolution_counts["output_count"],
        "resolution_evaluation_count": resolution_counts["evaluation_count"],
        "latest_resolution_item_count": int(latest_resolution.get("item_count") or 0),
        "latest_resolution_output_count": int(latest_resolution.get("output_count") or 0),
        "latest_resolution_status": latest_resolution.get("status"),
        "resolution_proposal_count": _table_count_map(db_path, ["resolution.uma_proposals"]).get("resolution.uma_proposals", 0),
        "resolution_verification_count": _table_count_map(db_path, ["resolution.settlement_verifications"]).get(
            "resolution.settlement_verifications", 0
        ),
        "triage_row_count": int(ui_counts.get("ui.opportunity_triage_summary", 0)),
        "resolution_row_count": int(ui_counts.get("ui.proposal_resolution_summary", 0)),
        "triage_status": runtime_chain["opportunity_triage"].get("status"),
        "resolution_status": runtime_chain["resolution_review"].get("status"),
        "triage_accepted_count": triage_decision_counts["accepted"],
        "triage_ignored_count": triage_decision_counts["ignored"],
        "triage_deferred_count": triage_decision_counts["deferred"],
        "agent_running_status": agent_running_status,
        "agent_value_status": agent_value_status,
    }


def _build_roi_status(
    *,
    source_split_brain: bool,
    db_path: Path,
    ui_counts: dict[str, int],
    signal_pipeline: dict[str, Any],
    execution_pipeline: dict[str, Any],
    agent_pipeline: dict[str, Any],
    settlement_feedback_pipeline: dict[str, Any] | None = None,
) -> dict[str, Any]:
    settlement_feedback_pipeline = dict(settlement_feedback_pipeline or {})
    has_empirical_feedback = (
        int(execution_pipeline.get("predicted_vs_realized_rows", 0)) > 0
        and int(execution_pipeline.get("fills", 0)) > 0
    )
    useful_agent_output = any(
        int(agent_pipeline.get(key, 0)) > 0
        for key in [
            "latest_triage_non_fallback_output_count",
            "latest_triage_medium_or_high_confidence_count",
            "latest_triage_operator_review_count",
        ]
    )
    execution_closed = all(
        [
            signal_pipeline["calibration_profile_count"] > 0,
            execution_pipeline["strategy_runs"] > 0,
            execution_pipeline["trade_tickets"] > 0,
            execution_pipeline["allocation_decisions"] > 0,
            execution_pipeline["paper_orders"] > 0,
            execution_pipeline["fills"] > 0,
            int(ui_counts.get("ui.market_opportunity_summary", 0)) > 0,
            int(ui_counts.get("ui.action_queue_summary", 0)) > 0,
            int(execution_pipeline.get("predicted_vs_realized_rows", 0)) > 0,
        ]
    )
    deployable_snapshot_count = int(execution_pipeline.get("deployable_snapshot_count", 0))
    execution_intelligence_covered_snapshot_count = int(execution_pipeline.get("execution_intelligence_covered_snapshot_count", 0))
    intelligence_closed = (
        int(execution_pipeline.get("execution_intelligence_summaries", 0)) > 0
        and int(ui_counts.get("ui.market_microstructure_summary", 0)) > 0
        and (deployable_snapshot_count <= 0 or execution_intelligence_covered_snapshot_count >= deployable_snapshot_count)
        and useful_agent_output
    )
    if intelligence_closed:
        intelligence_closure_status = "closed"
    elif (
        int(agent_pipeline.get("triage_invocation_count", 0)) > 0
        or str(agent_pipeline.get("latest_triage_status") or "") not in {"not_run", "idle_no_subjects", ""}
    ):
        intelligence_closure_status = "degraded"
    else:
        intelligence_closure_status = "not_running"

    if execution_closed:
        execution_closure_status = "closed"
    elif (
        signal_pipeline["calibration_profile_count"] > 0
        and execution_pipeline["strategy_runs"] > 0
        and execution_pipeline["trade_tickets"] > 0
    ):
        execution_closure_status = "partial"
    else:
        execution_closure_status = "open"

    resolved_ticket_count = int(settlement_feedback_pipeline.get("resolved_ticket_count", 0))
    pending_resolution_ticket_count = int(settlement_feedback_pipeline.get("pending_resolution_ticket_count", 0))
    latest_feedback_writeback_status = str(settlement_feedback_pipeline.get("latest_feedback_writeback_status") or "not_run")
    if resolved_ticket_count > 0 and latest_feedback_writeback_status == "ok":
        settlement_feedback_closure_status = "closed"
    elif pending_resolution_ticket_count > 0 or int(execution_pipeline.get("fills", 0)) > 0:
        settlement_feedback_closure_status = "waiting_for_resolution"
    else:
        settlement_feedback_closure_status = "open"

    return {
        "canonical_db_unique": not source_split_brain,
        "canonical_db_path": str(db_path),
        "ui_replica_db_path": str(default_ui_db_replica_path()),
        "ui_lite_db_path": str(default_ui_lite_db_path()),
        "path_closed": execution_closed,
        "execution_closure_status": execution_closure_status,
        "intelligence_closure_status": intelligence_closure_status,
        "settlement_feedback_closure_status": settlement_feedback_closure_status,
        "has_deployable_signals": signal_pipeline["non_no_trade_snapshot_count"] > 0,
        "has_empirical_feedback": has_empirical_feedback,
        "agents_have_useful_output": useful_agent_output,
    }


def _build_forecast_service(*, db_path: Path) -> ForecastService:
    forecast_http_client = RetryHttpClient(HttpJsonClient(timeout_seconds=10.0), max_retries=3, initial_delay=0.5)
    std_dev_provider = DuckDBForecastStdDevProvider(db_path)
    return ForecastService(
        adapter_router=AdapterRouter(
            [
                NWSAdapter(client=forecast_http_client, std_dev_provider=std_dev_provider),
                OpenMeteoAdapter(client=forecast_http_client, std_dev_provider=std_dev_provider),
            ]
        ),
        cache=InMemoryForecastCache(),
    )


def _refresh_forecasts_for_markets(
    *,
    db_path: Path,
    queue_cfg: WriteQueueConfig,
    allow_tables: list[str],
    forecast_service: ForecastService,
    source_requested: str,
    model_run: str,
    forecast_target_time: datetime,
    target_markets: list[Any],
) -> tuple[list[str], dict[str, str], list[dict[str, Any]]]:
    success_market_ids: list[str] = []
    error_by_market: dict[str, str] = {}
    per_market_forecasts: list[dict[str, Any]] = []
    for target_market in target_markets:
        try:
            with reader_connection(db_path) as con:
                forecast_result = run_weather_forecast_refresh(
                    con,
                    queue_cfg,
                    forecast_service=forecast_service,
                    source=source_requested,
                    model_run=model_run,
                    forecast_target_time=forecast_target_time,
                    market_ids=[target_market.market_id],
                    run_id=f"run_forecast_refresh_{target_market.market_id}",
                )
            drain_queue(queue_path=_resolve_queue_path(db_path), db_path=db_path, allow_tables=allow_tables)
            success_market_ids.append(target_market.market_id)
            per_market_forecasts.append(
                {
                    "market_id": target_market.market_id,
                    "question": target_market.title,
                    "status": "ok",
                    "forecast_item_count": forecast_result.item_count,
                }
            )
        except Exception as exc:  # noqa: BLE001
            error_by_market[target_market.market_id] = str(exc)
    return success_market_ids, error_by_market, per_market_forecasts


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    db_path = _resolve_db_path(args.db_path)
    queue_path = _resolve_queue_path(db_path)
    report_path = output_dir / "real_weather_chain_report.json"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    queue_path.parent.mkdir(parents=True, exist_ok=True)
    os.environ["ASTERION_DB_PATH"] = str(db_path)

    if args.force_rebuild:
        for path in (db_path, queue_path):
            if path.exists():
                path.unlink()

    apply_schema(db_path)
    queue_cfg = WriteQueueConfig(path=str(queue_path))
    allow_tables = list(FULL_ALLOW_TABLES)

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
            mapping_method="market_override",
            mapping_confidence=1.0,
            override_reason="real_weather_chain_smoke",
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
                    "mapping_confidence": getattr(StationMapper().resolve_record_from_spec_inputs(
                        con,
                        market_id=target_market.market_id,
                        location_name=spec.location_name,
                        authoritative_source=spec.authoritative_source,
                    ), "mapping_confidence", 1.0),
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
    forecast_source_requested = _resolve_runtime_forecast_source(db_path)
    model_run = datetime.now(UTC).strftime("%Y-%m-%dT%H:%MZ")
    forecast_target_time = datetime.now(UTC)
    forecast_service = _build_forecast_service(db_path=db_path)
    forecast_success_market_ids, forecast_error_by_market, per_market_forecasts = _refresh_forecasts_for_markets(
        db_path=db_path,
        queue_cfg=queue_cfg,
        allow_tables=allow_tables,
        forecast_service=forecast_service,
        source_requested=forecast_source_requested,
        model_run=model_run,
        forecast_target_time=forecast_target_time,
        target_markets=target_markets,
    )
    pre_pricing_runtime = _run_prepricing_jobs(
        db_path=db_path,
        queue_cfg=queue_cfg,
        allow_tables=allow_tables,
        force_rebuild=bool(args.force_rebuild),
    )
    if pre_pricing_runtime.get("calibration_refresh", {}).get("status") == "ok":
        forecast_service = _build_forecast_service(db_path=db_path)
        (
            forecast_success_market_ids,
            forecast_error_by_market,
            per_market_forecasts,
        ) = _refresh_forecasts_for_markets(
            db_path=db_path,
            queue_cfg=queue_cfg,
            allow_tables=allow_tables,
            forecast_service=forecast_service,
            source_requested=forecast_source_requested,
            model_run=model_run,
            forecast_target_time=forecast_target_time,
            target_markets=target_markets,
        )
    market_outputs = _build_market_outputs(
        db_path=db_path,
        target_markets=target_markets,
        forecast_success_market_ids=forecast_success_market_ids,
        forecast_error_by_market=forecast_error_by_market,
        per_market_forecasts=per_market_forecasts,
        forecast_source_requested=forecast_source_requested,
        forecast_target_time=forecast_target_time,
    )
    fair_values = market_outputs["fair_values"]
    snapshots = market_outputs["snapshots"]
    source_health_snapshots = market_outputs["source_health_snapshots"]
    per_market_pricing = market_outputs["per_market_pricing"]
    per_market_signals = market_outputs["per_market_signals"]
    per_market_forecasts = market_outputs["per_market_forecasts"]

    if fair_values:
        enqueue_fair_value_upserts(queue_cfg, fair_values=fair_values, run_id="run_fair_values")
    if snapshots:
        enqueue_watch_only_snapshot_upserts(queue_cfg, snapshots=snapshots, run_id="run_watch_only")
    if source_health_snapshots:
        enqueue_source_health_snapshot_upserts(queue_cfg, snapshots=source_health_snapshots, run_id="run_source_health")
    if fair_values or snapshots or source_health_snapshots:
        drain_queue(queue_path=queue_path, db_path=db_path, allow_tables=allow_tables)

    post_pricing_runtime = _run_postpricing_jobs(
        db_path=db_path,
        queue_cfg=queue_cfg,
        allow_tables=allow_tables,
        triage_limit=max(1, int(args.triage_limit)) if int(args.triage_limit) > 0 else None,
    )
    refresh_priors = post_pricing_runtime.get("execution_priors_refresh") or {}
    should_refresh_market_outputs = (
        int(refresh_priors.get("prior_count") or 0) > 0
        or _table_count_map(db_path, ["runtime.trade_tickets"]).get("runtime.trade_tickets", 0) > 0
    )
    if should_refresh_market_outputs:
        market_outputs = _build_market_outputs(
            db_path=db_path,
            target_markets=target_markets,
            forecast_success_market_ids=forecast_success_market_ids,
            forecast_error_by_market=forecast_error_by_market,
            per_market_forecasts=per_market_forecasts,
            forecast_source_requested=forecast_source_requested,
            forecast_target_time=forecast_target_time,
        )
        fair_values = market_outputs["fair_values"]
        snapshots = market_outputs["snapshots"]
        source_health_snapshots = market_outputs["source_health_snapshots"]
        per_market_pricing = market_outputs["per_market_pricing"]
        per_market_signals = market_outputs["per_market_signals"]
        per_market_forecasts = market_outputs["per_market_forecasts"]
        if fair_values:
            enqueue_fair_value_upserts(queue_cfg, fair_values=fair_values, run_id="run_fair_values_refresh")
        if snapshots:
            enqueue_watch_only_snapshot_upserts(queue_cfg, snapshots=snapshots, run_id="run_watch_only_refresh")
        if source_health_snapshots:
            enqueue_source_health_snapshot_upserts(queue_cfg, snapshots=source_health_snapshots, run_id="run_source_health_refresh")
        if fair_values or snapshots or source_health_snapshots:
            drain_queue(queue_path=queue_path, db_path=db_path, allow_tables=allow_tables)
        try:
            with writer_connection(db_path) as con:
                refresh_metadata = run_operator_surface_refresh(
                    con,
                    job_name="real_weather_chain_smoke_post_priors",
                    trigger_mode="manual",
                    ui_replica_db_path=str(default_ui_db_replica_path()),
                    ui_replica_meta_path=str(default_ui_replica_meta_path(replica_db_path=default_ui_db_replica_path())),
                    ui_lite_db_path=str(default_ui_lite_db_path()),
                    ui_lite_meta_path=str(default_ui_lite_meta_path(lite_db_path=default_ui_lite_db_path())),
                    readiness_report_json_path=os.getenv("ASTERION_READINESS_REPORT_JSON_PATH", "data/ui/asterion_readiness_p4.json"),
                    readiness_evidence_json_path=os.getenv("ASTERION_READINESS_EVIDENCE_JSON_PATH", "data/ui/asterion_readiness_evidence_p4.json"),
                )
            post_pricing_runtime["operator_surface_refresh"] = {"status": "ok", **refresh_metadata}
        except Exception as exc:  # noqa: BLE001
            post_pricing_runtime["operator_surface_refresh"] = {"status": "failed", "error": str(exc)}
        try:
            with writer_connection(db_path) as con:
                triage_result = run_weather_opportunity_triage_review_job(
                    con,
                    queue_cfg,
                    market_ids=None,
                    limit=max(1, int(args.triage_limit)) if int(args.triage_limit) > 0 else None,
                    force_rerun=False,
                )
            drain_queue(queue_path=_resolve_queue_path(db_path), db_path=db_path, allow_tables=allow_tables)
            post_pricing_runtime["opportunity_triage"] = {
                "status": "ok" if triage_result.item_count > 0 else "idle_no_subjects",
                "item_count": triage_result.item_count,
                "requested_limit": max(1, int(args.triage_limit)) if int(args.triage_limit) > 0 else None,
                **triage_result.metadata,
            }
        except Exception as exc:  # noqa: BLE001
            post_pricing_runtime["opportunity_triage"] = {"status": "failed", "error": str(exc)}
    runtime_chain = _merge_runtime_chain(pre_pricing_runtime, post_pricing_runtime)
    counts = collect_counts(db_path)
    ui_counts = _collect_ui_row_counts()
    predicted_vs_realized_metrics = _load_predicted_vs_realized_metrics()
    feedback_materialization_summary = _load_latest_feedback_materialization_summary(db_path)
    forecast_status = _stage_status(success_count=len(forecast_success_market_ids), total_count=len(target_markets))
    chain_status = "ok" if forecast_status == "ok" else "degraded"
    if runtime_chain["paper_execution"].get("status") in {"failed"} or runtime_chain["operator_surface_refresh"].get("status") in {"failed"}:
        chain_status = "degraded"
    forecast_note = None
    if forecast_error_by_market:
        failed_market_count = len(forecast_error_by_market)
        forecast_note = (
            f"{failed_market_count}/{len(target_markets)} 个市场 forecast 拉取失败；"
            "其余成功市场已继续生成 pricing/opportunity。"
            if forecast_success_market_ids
            else f"所有 {len(target_markets)} 个市场的 forecast 拉取都失败；当前仅保留 discovery/spec/agent 结果。"
        )
    source_split_brain = _truth_source_split_brain_status(canonical_db_path=db_path)
    ui_market_metrics = _load_ui_market_opportunity_metrics()
    active_market_prior_hit_count = max(
        _active_market_prior_hit_count(snapshots),
        int(ui_market_metrics.get("active_market_prior_hit_count", 0)),
    )
    deployable_snapshot_count = max(
        _deployable_snapshot_count(snapshots),
        int(ui_market_metrics.get("deployable_snapshot_count", 0)),
    )
    execution_intelligence_covered_snapshot_count = max(
        _execution_intelligence_covered_snapshot_count(snapshots),
        int(ui_market_metrics.get("execution_intelligence_covered_snapshot_count", 0)),
    )
    signal_pipeline = {
        "discovered_markets": int(counts.get("weather.weather_markets", 0)),
        "forecast_runs": int(counts.get("weather.weather_forecast_runs", 0)),
        "fair_values": int(counts.get("weather.weather_fair_values", 0)),
        "watch_only_snapshots": int(counts.get("weather.weather_watch_only_snapshots", 0)),
        "non_no_trade_snapshot_count": len(runtime_chain.get("selected_snapshot_ids") or []),
        "calibration_sample_count": int(counts.get("weather.forecast_calibration_samples", 0)),
        "calibration_profile_count": int(counts.get("weather.forecast_calibration_profiles_v2", 0)),
        "calibration_materialization_count": int(counts.get("runtime.calibration_profile_materializations", 0)),
        "calibration_gate_breakdown": _calibration_gate_breakdown(snapshots),
        "prior_lookup_breakdown": _prior_lookup_breakdown(snapshots),
        "active_market_profile_hit_count": sum(
            1
            for snapshot in snapshots
            if str(((getattr(snapshot, "pricing_context", {}) or {}).get("calibration_health_status") or "")).strip().lower()
            not in {"", "lookup_missing"}
            and int(((getattr(snapshot, "pricing_context", {}) or {}).get("sample_count") or 0)) > 0
        ),
        "active_market_prior_hit_count": active_market_prior_hit_count,
        "calibration_status": (
            "ok"
            if int(counts.get("weather.forecast_calibration_profiles_v2", 0)) > 0
            else "calibration_missing"
        ),
    }
    execution_pipeline = {
        "strategy_runs": int(counts.get("runtime.strategy_runs", 0)),
        "trade_tickets": int(counts.get("runtime.trade_tickets", 0)),
        "allocation_decisions": int(counts.get("runtime.allocation_decisions", 0)),
        "execution_intelligence_runs": int(counts.get("runtime.execution_intelligence_runs", 0)),
        "execution_intelligence_summaries": int(counts.get("runtime.execution_intelligence_summaries", 0)),
        "deployable_snapshot_count": deployable_snapshot_count,
        "execution_intelligence_covered_snapshot_count": execution_intelligence_covered_snapshot_count,
        "paper_orders": int(counts.get("trading.orders", 0)),
        "fills": int(counts.get("trading.fills", 0)),
        "predicted_vs_realized_rows": int(ui_counts.get("ui.predicted_vs_realized_summary", 0)),
        "execution_science_rows": int(ui_counts.get("ui.execution_science_summary", 0)),
        "watch_only_vs_executed_rows": int(ui_counts.get("ui.watch_only_vs_executed_summary", 0)),
    }
    settlement_feedback_pipeline = {
        **predicted_vs_realized_metrics,
        **feedback_materialization_summary,
        "latest_resolution_market_count": int(
            (runtime_chain.get("resolution_reconciliation") or {}).get("verification_market_count")
            or (runtime_chain.get("resolution_reconciliation") or {}).get("verification_count")
            or 0
        ),
    }
    agent_pipeline = _build_agent_pipeline(
        db_path=db_path,
        ui_counts=ui_counts,
        runtime_chain=runtime_chain,
    )
    roi_status = _build_roi_status(
        source_split_brain=source_split_brain,
        db_path=db_path,
        ui_counts=ui_counts,
        signal_pipeline=signal_pipeline,
        execution_pipeline=execution_pipeline,
        agent_pipeline=agent_pipeline,
        settlement_feedback_pipeline=settlement_feedback_pipeline,
    )
    report = {
        "timestamp": datetime.now(UTC).isoformat(),
        "chain_status": chain_status,
        "report_scope": ["市场发现", "规则解析", "预测服务", "定价引擎", "机会发现", "execution", "agents", "roi"],
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
                    "mapping_confidence": next((item.get("mapping_confidence") for item in per_market_specs if item["market_id"] == market.market_id), None),
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
            "source_requested": forecast_source_requested,
            "source_used": _first_non_empty([item.get("source_used") for item in per_market_forecasts]),
            "source_trace": next((item.get("source_trace") for item in per_market_forecasts if item.get("source_trace")), []),
            "source_note": (
                "recovery 路径会优先选择与 calibration profiles 对齐的 forecast source；"
                "未命中时默认请求 openmeteo。"
            ),
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
        "truth_source": {
            "canonical_db_path": str(db_path),
            "ui_replica_db_path": str(default_ui_db_replica_path()),
            "ui_lite_db_path": str(default_ui_lite_db_path()),
            "report_path": str(report_path),
            "source_split_brain": source_split_brain,
        },
        "signal_pipeline": signal_pipeline,
        "execution_pipeline": execution_pipeline,
        "settlement_feedback_pipeline": settlement_feedback_pipeline,
        "agent_pipeline": agent_pipeline,
        "runtime_chain": runtime_chain,
        "roi_status": roi_status,
        "db_counts": counts,
        "ui_counts": ui_counts,
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
            "queue_path": str(queue_path),
        },
    }
    report_path.write_text(_json_dump(report) + "\n", encoding="utf-8")
    print(_json_dump(report))
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the market-discovery -> spec -> forecast -> pricing -> opportunity smoke chain.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--force-rebuild", action="store_true")
    parser.add_argument("--recent-within-days", type=int, default=DEFAULT_RECENT_WITHIN_DAYS)
    parser.add_argument("--market-limit", type=int, default=DEFAULT_MARKET_LIMIT)
    parser.add_argument(
        "--triage-limit",
        type=int,
        default=DEFAULT_TRIAGE_LIMIT,
        help="限制 smoke 本轮 triage subject 数量，避免外部模型限流拖慢整轮链路。",
    )
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
        with reader_connection(db_path) as con:
            markets = [
                item
                for item in load_weather_markets_for_rule2spec(con, active_only=False, limit=max(len(market_ids) * 4, 100))
                if item.market_id in set(market_ids)
            ]
            markets.sort(key=lambda item: market_ids.index(item.market_id))
        success_count = 0
        failure_count = 0
        validation_count = 0
        for market in markets:
            market_id = market.market_id
            try:
                with reader_connection(db_path) as con:
                    draft = parse_rule2spec_draft(market)
                    spec = load_weather_market_spec(con, market_id=market_id)
                override = build_station_mapping_for_market(market)
                station = StationMetadata(
                    station_id=override["station_id"],
                    location_name=override["location_name"],
                    latitude=override["latitude"],
                    longitude=override["longitude"],
                    timezone=override["timezone"],
                    source="operator_override",
                )
                validation = validate_rule2spec_draft(draft, current_spec=spec, station_metadata=station)
                result = market_results.setdefault(market_id, dict(default_market_state))
                result["rule2spec_status"] = "success"
                result["rule2spec_verdict"] = validation.verdict
                result["rule2spec_confidence"] = draft.parse_confidence
                result["rule2spec_summary"] = validation.summary
                validation_count += 1
                if validation.verdict == "pass":
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
        "rule2spec_run_count": validation_count,
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
    return _table_count_map(
        db_path,
        [
            "weather.weather_markets",
            "weather.weather_market_specs",
            "weather.weather_forecast_runs",
            "weather.weather_fair_values",
            "weather.weather_watch_only_snapshots",
            "weather.forecast_calibration_samples",
            "weather.forecast_calibration_profiles_v2",
            "runtime.calibration_profile_materializations",
            "runtime.strategy_runs",
            "runtime.trade_tickets",
            "runtime.gate_decisions",
            "runtime.allocation_decisions",
            "runtime.execution_intelligence_runs",
            "runtime.execution_intelligence_summaries",
            "runtime.execution_feedback_materializations",
            "trading.orders",
            "trading.fills",
            "agent.invocations",
            "agent.outputs",
            "agent.evaluations",
            "resolution.uma_proposals",
            "resolution.settlement_verifications",
        ],
    )


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
    db_path = _resolve_db_path(args.db_path)
    client = CurlJsonClient(timeout_seconds=20.0)
    saw_successful_scan = False
    transport_errors: list[str] = []

    for horizon in horizons:
        reusable_local_markets = _load_reusable_active_markets_from_canonical(
            db_path=db_path,
            horizon_days=horizon,
        )
        if reusable_local_markets:
            return (
                {
                    "market_source": "canonical_active_reuse",
                    "selected_horizon_days": horizon,
                    "discovered_count": len(reusable_local_markets),
                    "selected_count": len(reusable_local_markets),
                },
                reusable_local_markets[: max(1, int(args.market_limit))],
                "canonical_active_market_reuse",
                f"本地 canonical DB 中已存在 {len(reusable_local_markets)} 个仍在开盘且具备可复用 execution/prior 历史的天气市场；"
                f"本轮 recovery 优先复用其中前 {min(len(reusable_local_markets), max(1, int(args.market_limit)))} 个，最终 horizon={horizon} 天。",
            )
        api_result = discover_target_markets_via_api(client=client, horizon_days=horizon, db_path=db_path)
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
                    api_result["selected_markets"][: max(1, int(args.market_limit))],
                    "live_weather_market_auto_horizon",
                    f"通过 Gamma events weather feed 命中当前开盘且最近的天气市场，共 {len(api_result['selected_markets'])} 个；"
                    f"本轮 recovery 取前 {min(len(api_result['selected_markets']), max(1, int(args.market_limit)))} 个，最终 horizon={horizon} 天。",
                )
        elif api_result["status"] == "transport_error":
            transport_errors.append(str(api_result["error"]))

        fallback_result = discover_target_markets_via_weather_page(client=client, horizon_days=horizon, db_path=db_path)
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
                    fallback_result["selected_markets"][: max(1, int(args.market_limit))],
                    "live_weather_market_auto_horizon",
                    f"Gamma events 未命中可映射市场，回退官网天气页成功命中开盘近期市场，共 {len(fallback_result['selected_markets'])} 个；"
                    f"本轮 recovery 取前 {min(len(fallback_result['selected_markets']), max(1, int(args.market_limit)))} 个，最终 horizon={horizon} 天。",
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


def discover_target_markets_via_api(*, client: CurlJsonClient, horizon_days: int, db_path: Path | None = None) -> dict[str, Any]:
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
    selected = select_supported_markets(result.discovered_markets, db_path=db_path)
    return {
        "status": "ok",
        "selected_markets": selected,
        "discovered_count": result.discovered_count,
    }


def discover_target_markets_via_weather_page(*, client: CurlJsonClient, horizon_days: int, db_path: Path | None = None) -> dict[str, Any]:
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
    selected = select_supported_markets(discovered, db_path=db_path)
    return {
        "status": "ok",
        "selected_markets": selected,
        "discovered_count": len(discovered),
    }


def select_supported_markets(markets: list[Any], *, db_path: Path | None = None) -> list[Any]:
    history_reuse_scores = _load_market_history_reuse_scores(markets, db_path=db_path)
    ranked = sorted(
        markets,
        key=lambda item: (
            -int(history_reuse_scores.get(str(item.market_id), {}).get("has_reuse_history", 0)),
            -int(history_reuse_scores.get(str(item.market_id), {}).get("exact_market_ticket_count", 0)),
            -int(history_reuse_scores.get(str(item.market_id), {}).get("station_metric_ticket_count", 0)),
            -int(history_reuse_scores.get(str(item.market_id), {}).get("station_metric_prior_count", 0)),
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


def _load_reusable_active_markets_from_canonical(*, db_path: Path, horizon_days: int) -> list[Any]:
    if not Path(db_path).exists():
        return []
    horizon_end = date.today() + timedelta(days=max(1, int(horizon_days)))
    with reader_connection(Path(db_path)) as con:
        if not _table_exists(con, "weather.weather_markets") or not _table_exists(con, "weather.weather_market_specs"):
            return []
        rows = con.execute(
            """
            SELECT markets.raw_market_json
            FROM weather.weather_markets AS markets
            INNER JOIN weather.weather_market_specs AS specs
                ON specs.market_id = markets.market_id
            WHERE markets.active = TRUE
              AND markets.closed = FALSE
              AND markets.archived = FALSE
              AND markets.accepting_orders = TRUE
              AND specs.observation_date <= ?
            ORDER BY specs.observation_date ASC, markets.close_time ASC, markets.market_id ASC
            """,
            [horizon_end],
        ).fetchall()
    normalized_markets: list[Any] = []
    for (raw_market_json,) in rows:
        payload = raw_market_json
        if isinstance(payload, str):
            with contextlib.suppress(json.JSONDecodeError):
                payload = json.loads(payload)
        if not isinstance(payload, dict):
            continue
        normalized = normalize_weather_market(payload)
        if normalized is None:
            continue
        if not is_market_within_horizon(normalized, horizon_days=horizon_days):
            continue
        normalized_markets.append(normalized)
    if not normalized_markets:
        return []
    selected = select_supported_markets(normalized_markets, db_path=db_path)
    history_reuse_scores = _load_market_history_reuse_scores(selected, db_path=db_path)
    reusable_selected = [
        market
        for market in selected
        if int(history_reuse_scores.get(str(market.market_id), {}).get("has_reuse_history", 0)) > 0
    ]
    return reusable_selected


def _load_market_history_reuse_scores(markets: list[Any], *, db_path: Path | None) -> dict[str, dict[str, int]]:
    if not markets or db_path is None or not Path(db_path).exists():
        return {}
    market_ids = [str(item.market_id) for item in markets if getattr(item, "market_id", None)]
    if not market_ids:
        return {}
    mapping_by_market: dict[str, tuple[str, str]] = {}
    for item in markets:
        try:
            draft = parse_rule2spec_draft(item)
            mapping = build_station_mapping_for_market(item)
        except Exception:
            continue
        mapping_by_market[str(item.market_id)] = (
            str(mapping.get("station_id") or ""),
            str(draft.metric or ""),
        )
    if not mapping_by_market:
        return {}

    placeholders = ",".join(["?"] * len(market_ids))
    with reader_connection(Path(db_path)) as con:
        exact_market_ticket_counts = {
            str(market_id): int(count)
            for market_id, count in con.execute(
                f"""
                SELECT market_id, COUNT(*)
                FROM runtime.trade_tickets
                WHERE market_id IN ({placeholders})
                GROUP BY market_id
                """,
                market_ids,
            ).fetchall()
        }
        station_metric_prior_counts = {
            (str(station_id), str(metric)): int(count)
            for station_id, metric, count in con.execute(
                """
                SELECT station_id, metric, COUNT(*)
                FROM weather.weather_execution_priors
                WHERE station_id IS NOT NULL AND metric IS NOT NULL
                GROUP BY station_id, metric
                """
            ).fetchall()
        }
        station_metric_ticket_counts = {
            (str(station_id), str(metric)): int(count)
            for station_id, metric, count in con.execute(
                """
                SELECT spec.station_id, spec.metric, COUNT(*)
                FROM runtime.trade_tickets ticket
                JOIN weather.weather_market_specs spec ON spec.market_id = ticket.market_id
                WHERE spec.station_id IS NOT NULL AND spec.metric IS NOT NULL
                GROUP BY spec.station_id, spec.metric
                """
            ).fetchall()
        }

    scores: dict[str, dict[str, int]] = {}
    for market_id in market_ids:
        station_id, metric = mapping_by_market.get(market_id, ("", ""))
        prior_count = int(station_metric_prior_counts.get((station_id, metric), 0))
        ticket_count = int(station_metric_ticket_counts.get((station_id, metric), 0))
        exact_count = int(exact_market_ticket_counts.get(market_id, 0))
        scores[market_id] = {
            "exact_market_ticket_count": exact_count,
            "station_metric_ticket_count": ticket_count,
            "station_metric_prior_count": prior_count,
            "has_reuse_history": 1 if (exact_count > 0 or ticket_count > 0 or prior_count > 0) else 0,
        }
    return scores


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
