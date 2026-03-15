from __future__ import annotations

import importlib.util
import json
import os
import tempfile
import unittest
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from asterion_core.contracts import ResolutionSpec
from asterion_core.storage.database import DuckDBConfig, connect_duckdb
from asterion_core.storage.db_migrate import MigrationConfig, apply_migrations
from asterion_core.storage.write_queue import WriteQueueConfig
from asterion_core.storage.writerd import process_one
from dagster_asterion.handlers import (
    run_weather_capability_refresh_job,
    run_weather_forecast_refresh,
    run_weather_market_discovery_job,
    run_weather_paper_execution_job,
    run_weather_wallet_state_refresh_job,
    run_weather_spec_sync,
)
from domains.weather.forecast import (
    AdapterRouter,
    ForecastService,
    InMemoryForecastCache,
    OpenMeteoAdapter,
    build_forecast_run_record,
    enqueue_forecast_run_upserts,
)
from domains.weather.forecast.adapters import build_normal_distribution
from domains.weather.pricing import (
    build_binary_fair_values,
    build_watch_only_snapshot,
    enqueue_fair_value_upserts,
    enqueue_watch_only_snapshot_upserts,
    load_forecast_run,
    load_weather_market,
    load_weather_market_spec,
    probability_in_bucket,
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


class _ClobClient:
    def fetch_book_summary(self, token_id: str) -> dict:
        if token_id == "tok_yes":
            return {"tick_size": "0.01", "min_order_size": "1", "neg_risk": False}
        if token_id == "tok_no":
            return {"tick_size": "0.01", "min_order_size": "1", "neg_risk": False}
        raise AssertionError(f"unexpected token_id: {token_id}")

    def fetch_fee_rate(self, token_id: str) -> dict:
        if token_id in {"tok_yes", "tok_no"}:
            return {"fee_rate_bps": 30}
        raise AssertionError(f"unexpected token_id: {token_id}")


class _ChainReader:
    def read_account_state(self, wallet_entry) -> object:
        return type(
            "ChainStateStub",
            (),
            {
                "approved_targets": list(wallet_entry.allowance_targets),
                "can_trade": True,
                "restricted_reason": None,
            },
        )()


class _WalletStateReader:
    def read_native_balance(self, funder: str, *, decimals: int) -> object:
        return type(
            "ObservationReadStub",
            (),
            {
                "observed_quantity": Decimal("1.5"),
                "block_number": 123,
                "raw_observation_json": {"method": "eth_getBalance", "funder": funder, "decimals": decimals},
                "source": "polygon_rpc",
            },
        )()

    def read_erc20_balance(self, funder: str, token_address: str, *, decimals: int) -> object:
        return type(
            "ObservationReadStub",
            (),
            {
                "observed_quantity": Decimal("25"),
                "block_number": 123,
                "raw_observation_json": {
                    "method": "erc20.balanceOf",
                    "funder": funder,
                    "token_address": token_address,
                    "decimals": decimals,
                },
                "source": "polygon_rpc",
            },
        )()

    def read_erc20_allowance(self, funder: str, spender: str, token_address: str, *, decimals: int) -> object:
        return type(
            "ObservationReadStub",
            (),
            {
                "observed_quantity": Decimal("100"),
                "block_number": 123,
                "raw_observation_json": {
                    "method": "erc20.allowance",
                    "funder": funder,
                    "spender": spender,
                    "token_address": token_address,
                    "decimals": decimals,
                },
                "source": "polygon_rpc",
            },
        )()


def _raw_weather_market() -> dict:
    observation_date = _observation_date()
    date_label = f"{observation_date.strftime('%B')} {observation_date.day}, {observation_date.year}"
    close_time = datetime.combine(observation_date, time(23, 59, 59), tzinfo=timezone.utc)
    created_at = close_time - timedelta(days=7)
    return {
        "id": "mkt_weather_1",
        "conditionId": "cond_weather_1",
        "question": f"Will the high temperature in New York City on {date_label} be 50-59°F?",
        "description": "Template weather market",
        "rules": "Resolve to Yes if the observed high temperature is within range.",
        "slug": f"nyc-high-temp-{observation_date.isoformat()}",
        "active": True,
        "closed": False,
        "archived": False,
        "acceptingOrders": True,
        "enableOrderBook": True,
        "tags": ["Weather", "Temperature"],
        "outcomes": "[\"Yes\", \"No\"]",
        "clobTokenIds": "[\"tok_yes\", \"tok_no\"]",
        "closeTime": close_time.isoformat().replace("+00:00", "Z"),
        "endDate": close_time.isoformat().replace("+00:00", "Z"),
        "createdAt": created_at.isoformat().replace("+00:00", "Z"),
        "event": {"id": "evt_weather_1", "category": "Weather"},
    }


def _resolution_spec() -> ResolutionSpec:
    observation_date = _observation_date()
    return ResolutionSpec(
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        location_name="New York City",
        station_id="KNYC",
        latitude=40.7128,
        longitude=-74.0060,
        timezone="America/New_York",
        observation_date=observation_date,
        observation_window_local="daily_max",
        metric="temperature_max",
        unit="fahrenheit",
        authoritative_source="weather.com",
        fallback_sources=["nws", "open-meteo"],
        rounding_rule="identity",
        inclusive_bounds=True,
        spec_version="spec_abc123",
    )


def _observation_date() -> date:
    return (datetime.now(timezone.utc) + timedelta(days=3)).date()


def _forecast_target_time() -> datetime:
    observation_date = _observation_date()
    return datetime.combine(observation_date - timedelta(days=1), time(12, 0), tzinfo=timezone.utc)


def _model_run() -> str:
    return _forecast_target_time().strftime("%Y-%m-%dT%H:%MZ")


class WeatherPricingUnitTest(unittest.TestCase):
    def test_probability_in_bucket_and_snapshot_decision(self) -> None:
        probability = probability_in_bucket(
            {49: 0.2, 50: 0.3, 55: 0.4, 60: 0.1},
            bucket_min=50.0,
            bucket_max=59.0,
            inclusive_bounds=True,
        )
        self.assertAlmostEqual(probability, 0.7)

    def test_snapshot_uses_fair_value_edge(self) -> None:
        from asterion_core.contracts import WeatherFairValueRecord

        fair_value = WeatherFairValueRecord(
            fair_value_id="fv1",
            run_id="run1",
            market_id="m1",
            condition_id="c1",
            token_id="tok_yes",
            outcome="YES",
            fair_value=0.70,
            confidence=0.9,
        )
        snapshot = build_watch_only_snapshot(
            fair_value=fair_value,
            reference_price=0.55,
            threshold_bps=500,
            pricing_context={"source": "unit_test"},
        )
        self.assertEqual(snapshot.decision, "TAKE")
        self.assertEqual(snapshot.side, "BUY")
        self.assertGreater(snapshot.edge_bps, 0)


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required for weather pricing tests")
class WeatherPricingDuckDBTest(unittest.TestCase):
    def test_forecast_run_drives_fair_values_and_watch_only_snapshots(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
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
            self.assertEqual(discovery.discovered_count, 1)

            station_mapping = build_station_mapping_record(
                market_id="mkt_weather_1",
                location_name="New York City",
                station_id="KNYC",
                station_name="Central Park",
                latitude=40.7128,
                longitude=-74.0060,
                timezone="America/New_York",
                source="operator_override",
                authoritative_source="unknown",
                is_override=True,
                metadata={"kind": "manual"},
            )
            enqueue_station_mapping_upserts(queue_cfg, mappings=[station_mapping], run_id="run_station_map")

            with patch.dict(
                os.environ,
                {"ASTERION_DB_PATH": db_path, "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables},
                clear=False,
            ):
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))

                reader_env = {
                    "ASTERION_STRICT_SINGLE_WRITER": "1",
                    "ASTERION_DB_ROLE": "reader",
                    "WRITERD": "0",
                }
                from asterion_core.storage.database import DuckDBConfig, connect_duckdb

                with patch.dict(os.environ, reader_env, clear=False):
                    con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                    try:
                        weather_market = load_weather_markets_for_rule2spec(con, active_only=True, limit=10)[0]
                        draft = parse_rule2spec_draft(weather_market)
                        mapper = StationMapper()
                        spec_record = build_weather_market_spec_record_via_station_mapper(draft, mapper=mapper, con=con)
                    finally:
                        con.close()

            enqueue_weather_market_spec_upserts(queue_cfg, specs=[spec_record], run_id="run_spec")

            with patch.dict(
                os.environ,
                {"ASTERION_DB_PATH": db_path, "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables},
                clear=False,
            ):
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))

            forecast_client = _RoutingClient(
                {
                    "api.open-meteo.com": {
                        "daily": {
                            "temperature_2m_max": [55.0],
                        }
                    }
                }
            )
            service = ForecastService(
                adapter_router=AdapterRouter([OpenMeteoAdapter(client=forecast_client)]),
                cache=InMemoryForecastCache(),
            )
            distribution = service.get_forecast(
                _resolution_spec(),
                source="openmeteo",
                model_run=_model_run(),
                forecast_target_time=_forecast_target_time(),
            )
            forecast_run = build_forecast_run_record(distribution)
            enqueue_forecast_run_upserts(queue_cfg, forecast_runs=[forecast_run], run_id="run_forecast")

            with patch.dict(
                os.environ,
                {"ASTERION_DB_PATH": db_path, "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables},
                clear=False,
            ):
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))

                from asterion_core.storage.database import DuckDBConfig, connect_duckdb

                reader_env = {
                    "ASTERION_STRICT_SINGLE_WRITER": "1",
                    "ASTERION_DB_ROLE": "reader",
                    "WRITERD": "0",
                }
                with patch.dict(os.environ, reader_env, clear=False):
                    con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                    try:
                        loaded_market = load_weather_market(con, market_id="mkt_weather_1")
                        loaded_spec = load_weather_market_spec(con, market_id="mkt_weather_1")
                        loaded_run = load_forecast_run(con, run_id=forecast_run.run_id)
                    finally:
                        con.close()

            fair_values = build_binary_fair_values(
                market=loaded_market,
                spec=loaded_spec,
                forecast_run=loaded_run,
            )
            yes_value = next(item for item in fair_values if item.outcome == "YES")
            no_value = next(item for item in fair_values if item.outcome == "NO")
            expected_yes = probability_in_bucket(
                build_normal_distribution(55.0, 3.0),
                bucket_min=50.0,
                bucket_max=59.0,
                inclusive_bounds=True,
            )
            self.assertAlmostEqual(yes_value.fair_value, expected_yes)
            self.assertAlmostEqual(no_value.fair_value, 1.0 - expected_yes)

            snapshots = [
                build_watch_only_snapshot(
                    fair_value=yes_value,
                    reference_price=0.72,
                    threshold_bps=300,
                    pricing_context={"forecast_run_id": loaded_run.run_id, "source_trace": loaded_run.source_trace},
                ),
                build_watch_only_snapshot(
                    fair_value=no_value,
                    reference_price=0.20,
                    threshold_bps=300,
                    pricing_context={"forecast_run_id": loaded_run.run_id, "source_trace": loaded_run.source_trace},
                ),
            ]

            enqueue_fair_value_upserts(queue_cfg, fair_values=fair_values, run_id="run_fair_values")
            enqueue_watch_only_snapshot_upserts(queue_cfg, snapshots=snapshots, run_id="run_watch_snapshots")

            with patch.dict(
                os.environ,
                {"ASTERION_DB_PATH": db_path, "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables},
                clear=False,
            ):
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))

            import duckdb

            con = duckdb.connect(db_path, read_only=True)
            try:
                fv_rows = con.execute(
                    """
                    SELECT outcome, fair_value, run_id
                    FROM weather.weather_fair_values
                    WHERE market_id = 'mkt_weather_1'
                    ORDER BY outcome
                    """
                ).fetchall()
                snap_rows = con.execute(
                    """
                    SELECT outcome, decision, side, pricing_context_json
                    FROM weather.weather_watch_only_snapshots
                    WHERE market_id = 'mkt_weather_1'
                    ORDER BY outcome
                    """
                ).fetchall()
            finally:
                con.close()

            self.assertEqual(
                fv_rows,
                [
                    ("NO", 1.0 - expected_yes, forecast_run.run_id),
                    ("YES", expected_yes, forecast_run.run_id),
                ],
            )
            self.assertEqual(snap_rows[0][0], "NO")
            self.assertEqual(snap_rows[0][1], "TAKE")
            self.assertEqual(snap_rows[0][2], "SELL")
            self.assertEqual(json.loads(snap_rows[0][3])["forecast_run_id"], forecast_run.run_id)
            self.assertEqual(snap_rows[1][0], "YES")
            self.assertEqual(snap_rows[1][1], "TAKE")
            self.assertEqual(snap_rows[1][2], "BUY")

    def test_real_ingress_chain_can_feed_weather_paper_execution(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
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
                    "weather.weather_fair_values",
                    "weather.weather_watch_only_snapshots",
                    "capability.market_capabilities",
                    "capability.account_trading_capabilities",
                    "capability.execution_contexts",
                    "runtime.strategy_runs",
                    "runtime.trade_tickets",
                    "runtime.gate_decisions",
                    "runtime.external_balance_observations",
                    "runtime.journal_events",
                    "trading.orders",
                    "trading.order_state_transitions",
                    "trading.reservations",
                    "trading.fills",
                    "trading.inventory_positions",
                    "trading.exposure_snapshots",
                    "trading.reconciliation_results",
                ]
            )
            queue_cfg = WriteQueueConfig(path=queue_path)

            gamma_client = _GammaClient([[_raw_weather_market()]])
            with patch.dict(
                os.environ,
                {
                    "ASTERION_DB_PATH": db_path,
                    "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables,
                },
                clear=False,
            ):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    discovery_result = run_weather_market_discovery_job(
                        con,
                        queue_cfg,
                        client=gamma_client,
                        base_url="https://gamma.example",
                        markets_endpoint="/markets",
                        page_limit=100,
                        max_pages=1,
                        sleep_s=0.0,
                        active_only=True,
                        closed=False,
                        archived=False,
                        run_id="run_market_discovery",
                    )
                finally:
                    con.close()
                self.assertEqual(discovery_result.item_count, 1)
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))

            station_mapping = build_station_mapping_record(
                market_id="mkt_weather_1",
                location_name="New York City",
                station_id="KNYC",
                station_name="Central Park",
                latitude=40.7128,
                longitude=-74.0060,
                timezone="America/New_York",
                source="operator_override",
                authoritative_source="unknown",
                is_override=True,
                metadata={"kind": "manual"},
            )
            enqueue_station_mapping_upserts(queue_cfg, mappings=[station_mapping], run_id="run_station_map")

            with patch.dict(
                os.environ,
                {
                    "ASTERION_DB_PATH": db_path,
                    "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables,
                    "ASTERION_STRICT_SINGLE_WRITER": "1",
                    "ASTERION_DB_ROLE": "reader",
                    "WRITERD": "0",
                },
                clear=False,
            ):
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    spec_result = run_weather_spec_sync(con, queue_cfg, run_id="run_spec_sync")
                finally:
                    con.close()
                self.assertEqual(spec_result.item_count, 1)

            with patch.dict(
                os.environ,
                {
                    "ASTERION_DB_PATH": db_path,
                    "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables,
                },
                clear=False,
            ):
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))

            forecast_client = _RoutingClient(
                {
                    "api.open-meteo.com": {
                        "daily": {
                            "temperature_2m_max": [55.0],
                        }
                    }
                }
            )
            service = ForecastService(
                adapter_router=AdapterRouter([OpenMeteoAdapter(client=forecast_client)]),
                cache=InMemoryForecastCache(),
            )

            with patch.dict(
                os.environ,
                {
                    "ASTERION_DB_PATH": db_path,
                    "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables,
                    "ASTERION_STRICT_SINGLE_WRITER": "1",
                    "ASTERION_DB_ROLE": "reader",
                    "WRITERD": "0",
                },
                clear=False,
            ):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    forecast_result = run_weather_forecast_refresh(
                        con,
                        queue_cfg,
                        forecast_service=service,
                        source="openmeteo",
                        model_run=_model_run(),
                        forecast_target_time=_forecast_target_time(),
                        run_id="run_forecast_refresh",
                    )
                finally:
                    con.close()
                self.assertEqual(forecast_result.item_count, 1)

            with patch.dict(
                os.environ,
                {
                    "ASTERION_DB_PATH": db_path,
                    "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables,
                },
                clear=False,
            ):
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))

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
                    loaded_market = load_weather_market(con, market_id="mkt_weather_1")
                    loaded_spec = load_weather_market_spec(con, market_id="mkt_weather_1")
                    forecast_run_id = con.execute(
                        "SELECT run_id FROM weather.weather_forecast_runs WHERE market_id = ? ORDER BY created_at DESC, run_id DESC LIMIT 1",
                        ["mkt_weather_1"],
                    ).fetchone()[0]
                    loaded_run = load_forecast_run(con, run_id=forecast_run_id)
                finally:
                    con.close()

            fair_values = build_binary_fair_values(
                market=loaded_market,
                spec=loaded_spec,
                forecast_run=loaded_run,
            )
            snapshots = [
                build_watch_only_snapshot(
                    fair_value=next(item for item in fair_values if item.outcome == "YES"),
                    reference_price=0.55,
                    threshold_bps=300,
                    pricing_context={"forecast_run_id": loaded_run.run_id},
                )
            ]
            enqueue_fair_value_upserts(queue_cfg, fair_values=fair_values, run_id="run_fair_values")
            enqueue_watch_only_snapshot_upserts(queue_cfg, snapshots=snapshots, run_id="run_watch_snapshots")

            with patch.dict(
                os.environ,
                {
                    "ASTERION_DB_PATH": db_path,
                    "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables,
                },
                clear=False,
            ):
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))

            wallet_registry_path = Path(tmpdir) / "wallet_registry.json"
            wallet_registry_path.write_text(
                json.dumps(
                    {
                        "wallets": [
                            {
                                "wallet_id": "wallet_weather_1",
                                "wallet_type": "eoa",
                                "signature_type": 1,
                                "funder": "0x1111111111111111111111111111111111111111",
                                "can_use_relayer": True,
                                "allowance_targets": ["0x2222222222222222222222222222222222222222"],
                                "enabled": True,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            chain_registry_path = Path(tmpdir) / "chain_registry.polygon.json"
            chain_registry_path.write_text(
                json.dumps(
                    {
                        "chain_id": 137,
                        "native_gas": {"asset_type": "native_gas", "symbol": "POL", "decimals": 18},
                        "usdc_e": {
                            "asset_type": "usdc_e",
                            "token_id": "usdc_e",
                            "contract_address": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
                            "decimals": 6,
                        },
                        "allowance_targets": {"relayer": "0x2222222222222222222222222222222222222222"},
                    }
                ),
                encoding="utf-8",
            )

            with patch.dict(
                os.environ,
                {
                    "ASTERION_STRICT_SINGLE_WRITER": "1",
                    "ASTERION_DB_ROLE": "writer",
                    "WRITERD": "1",
                },
                clear=False,
            ):
                writer_con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    writer_con.execute(
                        "INSERT INTO trading.inventory_positions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        [
                            "wallet_weather_1",
                            "usdc_e",
                            "usdc_e",
                            "cash",
                            "cash",
                            "available",
                            100.0,
                            "0x1111111111111111111111111111111111111111",
                            1,
                            datetime(2026, 3, 10, 10, 0),
                        ],
                    )
                finally:
                    writer_con.close()

            with patch.dict(
                os.environ,
                {
                    "ASTERION_DB_PATH": db_path,
                    "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables,
                    "ASTERION_STRICT_SINGLE_WRITER": "1",
                    "ASTERION_DB_ROLE": "reader",
                    "WRITERD": "0",
                },
                clear=False,
            ):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    refresh_result = run_weather_capability_refresh_job(
                        con,
                        queue_cfg,
                        clob_client=_ClobClient(),
                        wallet_registry_path=str(wallet_registry_path),
                        chain_reader=_ChainReader(),
                        run_id="run_capability_refresh",
                        observed_at=datetime(2026, 3, 10, 10, 5, tzinfo=timezone.utc),
                    )
                finally:
                    con.close()
                self.assertEqual(refresh_result.metadata["market_capability_count"], 2)
                self.assertEqual(refresh_result.metadata["account_capability_count"], 1)

            with patch.dict(
                os.environ,
                {
                    "ASTERION_DB_PATH": db_path,
                    "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables,
                },
                clear=False,
            ):
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))

            with patch.dict(
                os.environ,
                {
                    "ASTERION_DB_PATH": db_path,
                    "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables,
                    "ASTERION_STRICT_SINGLE_WRITER": "1",
                    "ASTERION_DB_ROLE": "reader",
                    "WRITERD": "0",
                },
                clear=False,
            ):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    wallet_state_result = run_weather_wallet_state_refresh_job(
                        con,
                        queue_cfg,
                        chain_registry_path=str(chain_registry_path),
                        wallet_state_reader=_WalletStateReader(),
                        run_id="run_wallet_state_refresh",
                        observed_at=datetime(2026, 3, 10, 10, 5, tzinfo=timezone.utc),
                    )
                finally:
                    con.close()
                self.assertEqual(wallet_state_result.metadata["wallet_count"], 1)
                self.assertEqual(wallet_state_result.metadata["observation_count"], 3)

            with patch.dict(
                os.environ,
                {
                    "ASTERION_DB_PATH": db_path,
                    "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables,
                },
                clear=False,
            ):
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))

            with patch.dict(
                os.environ,
                {
                    "ASTERION_DB_PATH": db_path,
                    "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables,
                    "ASTERION_STRICT_SINGLE_WRITER": "1",
                    "ASTERION_DB_ROLE": "reader",
                    "WRITERD": "0",
                },
                clear=False,
            ):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    paper_result = run_weather_paper_execution_job(
                        con,
                        queue_cfg,
                        params_json={
                            "wallet_id": "wallet_weather_1",
                            "strategy_registrations": [
                                {
                                    "strategy_id": "weather_primary",
                                    "strategy_version": "v1",
                                    "priority": 1,
                                    "route_action": "FAK",
                                    "size": "10",
                                    "min_edge_bps": 100,
                                }
                            ],
                            "snapshot_ids": [snapshots[0].snapshot_id],
                        },
                        observed_at=datetime(2026, 3, 10, 10, 6, tzinfo=timezone.utc),
                    )
                finally:
                    con.close()
                self.assertEqual(paper_result.metadata["ticket_count"], 1)

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

            import duckdb

            con = duckdb.connect(db_path, read_only=True)
            try:
                counts = con.execute(
                    """
                    SELECT
                        (SELECT COUNT(*) FROM weather.weather_markets),
                        (SELECT COUNT(*) FROM weather.weather_market_specs),
                        (SELECT COUNT(*) FROM weather.weather_forecast_runs),
                        (SELECT COUNT(*) FROM weather.weather_watch_only_snapshots),
                        (SELECT COUNT(*) FROM capability.market_capabilities),
                        (SELECT COUNT(*) FROM capability.account_trading_capabilities),
                        (SELECT COUNT(*) FROM runtime.external_balance_observations),
                        (SELECT COUNT(*) FROM runtime.strategy_runs),
                        (SELECT COUNT(*) FROM trading.orders)
                    """
                ).fetchone()
                capability_row = con.execute(
                    """
                    SELECT tick_size, fee_rate_bps, data_sources
                    FROM capability.market_capabilities
                    WHERE token_id = 'tok_yes'
                    """
                ).fetchone()
            finally:
                con.close()

            self.assertEqual(counts[0], 1)
            self.assertEqual(counts[1], 1)
            self.assertEqual(counts[2], 1)
            self.assertEqual(counts[3], 1)
            self.assertEqual(counts[4], 2)
            self.assertEqual(counts[5], 1)
            self.assertEqual(counts[6], 3)
            self.assertEqual(counts[7], 1)
            self.assertEqual(counts[8], 1)
            self.assertEqual(capability_row[0], Decimal("0.01000000"))
            self.assertEqual(capability_row[1], 30)
            self.assertEqual(json.loads(capability_row[2]), ["gamma", "clob_public"])


if __name__ == "__main__":
    unittest.main()
