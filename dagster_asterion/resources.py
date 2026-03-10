from __future__ import annotations

import importlib.util
import os
from dataclasses import dataclass
from typing import Any

from asterion_core.storage.database import DuckDBConfig, connect_duckdb
from asterion_core.storage.write_queue import WriteQueueConfig, default_write_queue_path
from domains.weather.forecast import AdapterRouter, ForecastService, InMemoryForecastCache, NWSAdapter, OpenMeteoAdapter
from domains.weather.resolution import BackfillRpcClient, FallbackRpcPool, RpcEndpointConfig


DAGSTER_AVAILABLE = importlib.util.find_spec("dagster") is not None

if DAGSTER_AVAILABLE:  # pragma: no cover - optional dependency
    from dagster import ConfigurableResource


@dataclass(frozen=True)
class AsterionColdPathSettings:
    db_path: str
    ddl_path: str | None
    write_queue_path: str
    forecast_primary_source: str
    forecast_fallback_sources: list[str]
    watcher_chain_id: int
    watcher_rpc_urls: list[str]

    @classmethod
    def from_env(cls) -> "AsterionColdPathSettings":
        fallback = [
            item.strip()
            for item in os.getenv("ASTERION_FORECAST_FALLBACK_SOURCES", "nws,openmeteo").split(",")
            if item.strip()
        ]
        watcher_urls = [
            item.strip()
            for item in os.getenv("ASTERION_WATCHER_RPC_URLS", "").split(",")
            if item.strip()
        ]
        return cls(
            db_path=os.getenv("ASTERION_DB_PATH", "data/asterion.duckdb"),
            ddl_path=os.getenv("ASTERION_DB_DDL_PATH") or None,
            write_queue_path=os.getenv("ASTERION_WRITE_QUEUE", default_write_queue_path()),
            forecast_primary_source=os.getenv("ASTERION_FORECAST_PRIMARY_SOURCE", "openmeteo"),
            forecast_fallback_sources=fallback,
            watcher_chain_id=int(os.getenv("ASTERION_WATCHER_CHAIN_ID", "137")),
            watcher_rpc_urls=watcher_urls,
        )


class HttpJsonClient:
    def __init__(self, *, timeout_seconds: float = 10.0) -> None:
        try:
            import httpx
        except ModuleNotFoundError as exc:  # pragma: no cover
            raise RuntimeError("Missing dependency: httpx. Install with: pip install httpx") from exc
        self._client = httpx.Client(timeout=timeout_seconds)

    def get_json(self, url: str, *, context: dict[str, Any]) -> Any:
        response = self._client.get(url, headers={"User-Agent": "asterion-cold-path/0.1"})
        response.raise_for_status()
        return response.json()


@dataclass(frozen=True)
class DuckDBResource:
    settings: AsterionColdPathSettings

    def get_config(self) -> DuckDBConfig:
        return DuckDBConfig(db_path=self.settings.db_path, ddl_path=self.settings.ddl_path)

    def get_connection(self):
        return connect_duckdb(self.get_config())


@dataclass(frozen=True)
class WriteQueueResource:
    settings: AsterionColdPathSettings

    def get_config(self) -> WriteQueueConfig:
        return WriteQueueConfig(path=self.settings.write_queue_path)


@dataclass(frozen=True)
class ForecastRuntimeResource:
    settings: AsterionColdPathSettings

    def build_adapter_router(self, *, client: Any | None = None, adapters: list[Any] | None = None) -> AdapterRouter:
        if adapters is not None:
            return AdapterRouter(list(adapters))
        http_client = client or HttpJsonClient()
        return AdapterRouter(
            [
                OpenMeteoAdapter(client=http_client),
                NWSAdapter(client=http_client),
            ]
        )

    def build_cache(self) -> InMemoryForecastCache:
        return InMemoryForecastCache()

    def build_service(
        self,
        *,
        client: Any | None = None,
        adapters: list[Any] | None = None,
        cache: InMemoryForecastCache | None = None,
    ) -> ForecastService:
        return ForecastService(
            adapter_router=self.build_adapter_router(client=client, adapters=adapters),
            cache=cache or self.build_cache(),
        )


@dataclass(frozen=True)
class WatcherRpcPoolResource:
    settings: AsterionColdPathSettings

    def build_rpc_pool(
        self,
        *,
        clients: list[tuple[RpcEndpointConfig, BackfillRpcClient]] | None = None,
    ) -> FallbackRpcPool:
        if clients is None:
            raise ValueError("WatcherRpcPoolResource requires injected RPC clients in the P2 orchestration shell")
        return FallbackRpcPool(list(clients))


if DAGSTER_AVAILABLE:  # pragma: no cover - optional dependency
    class DagsterDuckDBResource(ConfigurableResource):
        db_path: str = "data/asterion.duckdb"
        ddl_path: str | None = None

        def build_runtime(self) -> DuckDBResource:
            settings = AsterionColdPathSettings.from_env()
            settings = AsterionColdPathSettings(
                db_path=self.db_path,
                ddl_path=self.ddl_path,
                write_queue_path=settings.write_queue_path,
                forecast_primary_source=settings.forecast_primary_source,
                forecast_fallback_sources=list(settings.forecast_fallback_sources),
                watcher_chain_id=settings.watcher_chain_id,
                watcher_rpc_urls=list(settings.watcher_rpc_urls),
            )
            return DuckDBResource(settings=settings)


    class DagsterWriteQueueResource(ConfigurableResource):
        write_queue_path: str = default_write_queue_path()

        def build_runtime(self) -> WriteQueueResource:
            settings = AsterionColdPathSettings.from_env()
            settings = AsterionColdPathSettings(
                db_path=settings.db_path,
                ddl_path=settings.ddl_path,
                write_queue_path=self.write_queue_path,
                forecast_primary_source=settings.forecast_primary_source,
                forecast_fallback_sources=list(settings.forecast_fallback_sources),
                watcher_chain_id=settings.watcher_chain_id,
                watcher_rpc_urls=list(settings.watcher_rpc_urls),
            )
            return WriteQueueResource(settings=settings)


    class DagsterForecastRuntimeResource(ConfigurableResource):
        forecast_primary_source: str = "openmeteo"

        def build_runtime(self) -> ForecastRuntimeResource:
            settings = AsterionColdPathSettings.from_env()
            settings = AsterionColdPathSettings(
                db_path=settings.db_path,
                ddl_path=settings.ddl_path,
                write_queue_path=settings.write_queue_path,
                forecast_primary_source=self.forecast_primary_source,
                forecast_fallback_sources=list(settings.forecast_fallback_sources),
                watcher_chain_id=settings.watcher_chain_id,
                watcher_rpc_urls=list(settings.watcher_rpc_urls),
            )
            return ForecastRuntimeResource(settings=settings)


    class DagsterWatcherRpcPoolResource(ConfigurableResource):
        watcher_chain_id: int = 137

        def build_runtime(self) -> WatcherRpcPoolResource:
            settings = AsterionColdPathSettings.from_env()
            settings = AsterionColdPathSettings(
                db_path=settings.db_path,
                ddl_path=settings.ddl_path,
                write_queue_path=settings.write_queue_path,
                forecast_primary_source=settings.forecast_primary_source,
                forecast_fallback_sources=list(settings.forecast_fallback_sources),
                watcher_chain_id=int(self.watcher_chain_id),
                watcher_rpc_urls=list(settings.watcher_rpc_urls),
            )
            return WatcherRpcPoolResource(settings=settings)


def build_runtime_resources(settings: AsterionColdPathSettings | None = None) -> dict[str, Any]:
    active = settings or AsterionColdPathSettings.from_env()
    return {
        "cold_path_settings": active,
        "duckdb": DuckDBResource(settings=active),
        "write_queue": WriteQueueResource(settings=active),
        "forecast_runtime": ForecastRuntimeResource(settings=active),
        "watcher_rpc_pool": WatcherRpcPoolResource(settings=active),
    }


def build_dagster_resource_defs(settings: AsterionColdPathSettings | None = None) -> dict[str, Any]:
    active = settings or AsterionColdPathSettings.from_env()
    if not DAGSTER_AVAILABLE:
        return build_runtime_resources(active)
    return {
        "cold_path_settings": active,
        "duckdb": DagsterDuckDBResource(db_path=active.db_path, ddl_path=active.ddl_path),
        "write_queue": DagsterWriteQueueResource(write_queue_path=active.write_queue_path),
        "forecast_runtime": DagsterForecastRuntimeResource(forecast_primary_source=active.forecast_primary_source),
        "watcher_rpc_pool": DagsterWatcherRpcPoolResource(watcher_chain_id=active.watcher_chain_id),
    }
