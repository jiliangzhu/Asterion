from __future__ import annotations

import importlib.util
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from asterion_core.blockchain import (
    ChainTxServiceShell,
    DisabledChainTxBackend,
    PolygonChainTxReader,
    PolygonWalletStateReader,
    ShadowBroadcastBackend,
)
from asterion_core.clients import ClobPublicClient
from asterion_core.execution import (
    DisabledSubmitterBackend,
    SafeDefaultChainAccountCapabilityReader,
    ShadowSubmitterBackend,
    SubmitterServiceShell,
)
from asterion_core.signer import (
    DeterministicOfficialOrderSigningBackend,
    DeterministicTransactionSignerBackend,
    DisabledSignerBackend,
    PyClobClientOrderSigningBackend,
    SignerServiceShell,
)
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
    gamma_base_url: str
    gamma_markets_endpoint: str
    gamma_page_limit: int
    gamma_max_pages: int
    gamma_sleep_s: float
    gamma_active_only: bool
    gamma_closed: bool | None
    gamma_archived: bool | None
    clob_base_url: str
    clob_book_endpoint: str
    clob_fee_rate_endpoint: str
    wallet_registry_path: str
    chain_registry_path: str
    capability_chain_id: int
    capability_rpc_urls: list[str]
    signer_backend_kind: str
    signer_rpc_url: str | None
    submitter_backend_kind: str
    submitter_api_base_url: str | None
    chain_tx_backend_kind: str
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
        capability_rpc_urls = [
            item.strip()
            for item in os.getenv("ASTERION_CAPABILITY_RPC_URLS", "").split(",")
            if item.strip()
        ]
        return cls(
            db_path=os.getenv("ASTERION_DB_PATH", "data/asterion.duckdb"),
            ddl_path=os.getenv("ASTERION_DB_DDL_PATH") or None,
            write_queue_path=os.getenv("ASTERION_WRITE_QUEUE", default_write_queue_path()),
            gamma_base_url=os.getenv("ASTERION_GAMMA_BASE_URL", "https://gamma-api.polymarket.com"),
            gamma_markets_endpoint=os.getenv("ASTERION_GAMMA_MARKETS_ENDPOINT", "/markets"),
            gamma_page_limit=int(os.getenv("ASTERION_GAMMA_PAGE_LIMIT", "100")),
            gamma_max_pages=int(os.getenv("ASTERION_GAMMA_MAX_PAGES", "5")),
            gamma_sleep_s=float(os.getenv("ASTERION_GAMMA_SLEEP_S", "0.0")),
            gamma_active_only=_env_bool(os.getenv("ASTERION_GAMMA_ACTIVE_ONLY"), default=True),
            gamma_closed=_env_optional_bool(os.getenv("ASTERION_GAMMA_CLOSED")),
            gamma_archived=_env_optional_bool(os.getenv("ASTERION_GAMMA_ARCHIVED")),
            clob_base_url=os.getenv("ASTERION_CLOB_BASE_URL", "https://clob.polymarket.com"),
            clob_book_endpoint=os.getenv("ASTERION_CLOB_BOOK_ENDPOINT", "/book"),
            clob_fee_rate_endpoint=os.getenv("ASTERION_CLOB_FEE_RATE_ENDPOINT", "/fee-rate"),
            wallet_registry_path=os.getenv("ASTERION_WALLET_REGISTRY_PATH", "config/wallet_registry.json"),
            chain_registry_path=os.getenv("ASTERION_CHAIN_REGISTRY_PATH", "config/chain_registry.polygon.json"),
            capability_chain_id=int(os.getenv("ASTERION_CAPABILITY_CHAIN_ID", "137")),
            capability_rpc_urls=capability_rpc_urls,
            signer_backend_kind=os.getenv("ASTERION_SIGNER_BACKEND_KIND", "disabled"),
            signer_rpc_url=os.getenv("ASTERION_SIGNER_RPC_URL") or None,
            submitter_backend_kind=os.getenv("ASTERION_SUBMITTER_BACKEND_KIND", "disabled"),
            submitter_api_base_url=os.getenv("ASTERION_SUBMITTER_API_BASE_URL") or None,
            chain_tx_backend_kind=os.getenv("ASTERION_CHAIN_TX_BACKEND_KIND", "disabled"),
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
class GammaDiscoveryRuntimeResource:
    settings: AsterionColdPathSettings

    def build_client(self, *, client: Any | None = None) -> Any:
        return client or HttpJsonClient()

    def build_config(self) -> dict[str, Any]:
        return {
            "base_url": self.settings.gamma_base_url,
            "markets_endpoint": self.settings.gamma_markets_endpoint,
            "page_limit": self.settings.gamma_page_limit,
            "max_pages": self.settings.gamma_max_pages,
            "sleep_s": self.settings.gamma_sleep_s,
            "active_only": self.settings.gamma_active_only,
            "closed": self.settings.gamma_closed,
            "archived": self.settings.gamma_archived,
        }


@dataclass(frozen=True)
class CapabilityRefreshRuntimeResource:
    settings: AsterionColdPathSettings

    def build_clob_client(self, *, client: Any | None = None) -> ClobPublicClient:
        return ClobPublicClient(
            client=client or HttpJsonClient(),
            base_url=self.settings.clob_base_url,
            book_endpoint=self.settings.clob_book_endpoint,
            fee_rate_endpoint=self.settings.clob_fee_rate_endpoint,
        )

    def build_chain_reader(self, *, reader: Any | None = None) -> Any:
        return reader or SafeDefaultChainAccountCapabilityReader()

    def resolve_wallet_registry_path(self) -> str:
        return str(Path(self.settings.wallet_registry_path))


@dataclass(frozen=True)
class WalletStateObservationRuntimeResource:
    settings: AsterionColdPathSettings

    def resolve_chain_registry_path(self) -> str:
        return str(Path(self.settings.chain_registry_path))

    def build_wallet_state_reader(self, *, reader: Any | None = None) -> Any:
        return reader or PolygonWalletStateReader(
            chain_id=self.settings.capability_chain_id,
            rpc_urls=list(self.settings.capability_rpc_urls),
        )


@dataclass(frozen=True)
class SignerRuntimeResource:
    settings: AsterionColdPathSettings

    def build_signer_service(self, *, service: Any | None = None) -> SignerServiceShell:
        if service is not None:
            return service
        if self.settings.signer_backend_kind == "disabled":
            return SignerServiceShell(DisabledSignerBackend())
        if self.settings.signer_backend_kind == "official_stub":
            return SignerServiceShell(DeterministicOfficialOrderSigningBackend())
        if self.settings.signer_backend_kind == "tx_stub":
            return SignerServiceShell(DeterministicTransactionSignerBackend())
        if self.settings.signer_backend_kind == "py_clob_client":
            return SignerServiceShell(PyClobClientOrderSigningBackend())
        raise ValueError(f"unsupported signer_backend_kind={self.settings.signer_backend_kind!r}")


@dataclass(frozen=True)
class SubmitterRuntimeResource:
    settings: AsterionColdPathSettings

    def build_submitter_service(self, *, service: Any | None = None) -> SubmitterServiceShell:
        if service is not None:
            return service
        if self.settings.submitter_backend_kind == "disabled":
            return SubmitterServiceShell(DisabledSubmitterBackend())
        if self.settings.submitter_backend_kind == "shadow_stub":
            return SubmitterServiceShell(ShadowSubmitterBackend())
        raise ValueError(f"unsupported submitter_backend_kind={self.settings.submitter_backend_kind!r}")


@dataclass(frozen=True)
class ChainTxRuntimeResource:
    settings: AsterionColdPathSettings

    def resolve_chain_registry_path(self) -> str:
        return str(Path(self.settings.chain_registry_path))

    def build_chain_tx_reader(self, *, reader: Any | None = None) -> Any:
        return reader or PolygonChainTxReader(
            chain_id=self.settings.capability_chain_id,
            rpc_urls=list(self.settings.capability_rpc_urls),
        )

    def build_chain_tx_service(self, *, service: Any | None = None) -> ChainTxServiceShell:
        if service is not None:
            return service
        if self.settings.chain_tx_backend_kind == "disabled":
            return ChainTxServiceShell(DisabledChainTxBackend())
        if self.settings.chain_tx_backend_kind == "shadow_stub":
            return ChainTxServiceShell(ShadowBroadcastBackend())
        raise ValueError(f"unsupported chain_tx_backend_kind={self.settings.chain_tx_backend_kind!r}")


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
                gamma_base_url=settings.gamma_base_url,
                gamma_markets_endpoint=settings.gamma_markets_endpoint,
                gamma_page_limit=settings.gamma_page_limit,
                gamma_max_pages=settings.gamma_max_pages,
                gamma_sleep_s=settings.gamma_sleep_s,
                gamma_active_only=settings.gamma_active_only,
                gamma_closed=settings.gamma_closed,
                gamma_archived=settings.gamma_archived,
                clob_base_url=settings.clob_base_url,
                clob_book_endpoint=settings.clob_book_endpoint,
                clob_fee_rate_endpoint=settings.clob_fee_rate_endpoint,
                wallet_registry_path=settings.wallet_registry_path,
                chain_registry_path=settings.chain_registry_path,
                capability_chain_id=settings.capability_chain_id,
                capability_rpc_urls=list(settings.capability_rpc_urls),
                signer_backend_kind=settings.signer_backend_kind,
                signer_rpc_url=settings.signer_rpc_url,
                submitter_backend_kind=settings.submitter_backend_kind,
                submitter_api_base_url=settings.submitter_api_base_url,
                chain_tx_backend_kind=settings.chain_tx_backend_kind,
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
                gamma_base_url=settings.gamma_base_url,
                gamma_markets_endpoint=settings.gamma_markets_endpoint,
                gamma_page_limit=settings.gamma_page_limit,
                gamma_max_pages=settings.gamma_max_pages,
                gamma_sleep_s=settings.gamma_sleep_s,
                gamma_active_only=settings.gamma_active_only,
                gamma_closed=settings.gamma_closed,
                gamma_archived=settings.gamma_archived,
                clob_base_url=settings.clob_base_url,
                clob_book_endpoint=settings.clob_book_endpoint,
                clob_fee_rate_endpoint=settings.clob_fee_rate_endpoint,
                wallet_registry_path=settings.wallet_registry_path,
                chain_registry_path=settings.chain_registry_path,
                capability_chain_id=settings.capability_chain_id,
                capability_rpc_urls=list(settings.capability_rpc_urls),
                signer_backend_kind=settings.signer_backend_kind,
                signer_rpc_url=settings.signer_rpc_url,
                submitter_backend_kind=settings.submitter_backend_kind,
                submitter_api_base_url=settings.submitter_api_base_url,
                chain_tx_backend_kind=settings.chain_tx_backend_kind,
                forecast_primary_source=settings.forecast_primary_source,
                forecast_fallback_sources=list(settings.forecast_fallback_sources),
                watcher_chain_id=settings.watcher_chain_id,
                watcher_rpc_urls=list(settings.watcher_rpc_urls),
            )
            return WriteQueueResource(settings=settings)


    class DagsterGammaDiscoveryRuntimeResource(ConfigurableResource):
        gamma_base_url: str = "https://gamma-api.polymarket.com"
        gamma_markets_endpoint: str = "/markets"
        gamma_page_limit: int = 100
        gamma_max_pages: int = 5
        gamma_sleep_s: float = 0.0
        gamma_active_only: bool = True
        gamma_closed: bool | None = False
        gamma_archived: bool | None = False

        def build_runtime(self) -> GammaDiscoveryRuntimeResource:
            settings = AsterionColdPathSettings.from_env()
            settings = AsterionColdPathSettings(
                db_path=settings.db_path,
                ddl_path=settings.ddl_path,
                write_queue_path=settings.write_queue_path,
                gamma_base_url=self.gamma_base_url,
                gamma_markets_endpoint=self.gamma_markets_endpoint,
                gamma_page_limit=int(self.gamma_page_limit),
                gamma_max_pages=int(self.gamma_max_pages),
                gamma_sleep_s=float(self.gamma_sleep_s),
                gamma_active_only=bool(self.gamma_active_only),
                gamma_closed=self.gamma_closed,
                gamma_archived=self.gamma_archived,
                clob_base_url=settings.clob_base_url,
                clob_book_endpoint=settings.clob_book_endpoint,
                clob_fee_rate_endpoint=settings.clob_fee_rate_endpoint,
                wallet_registry_path=settings.wallet_registry_path,
                chain_registry_path=settings.chain_registry_path,
                capability_chain_id=settings.capability_chain_id,
                capability_rpc_urls=list(settings.capability_rpc_urls),
                signer_backend_kind=settings.signer_backend_kind,
                signer_rpc_url=settings.signer_rpc_url,
                submitter_backend_kind=settings.submitter_backend_kind,
                submitter_api_base_url=settings.submitter_api_base_url,
                chain_tx_backend_kind=settings.chain_tx_backend_kind,
                forecast_primary_source=settings.forecast_primary_source,
                forecast_fallback_sources=list(settings.forecast_fallback_sources),
                watcher_chain_id=settings.watcher_chain_id,
                watcher_rpc_urls=list(settings.watcher_rpc_urls),
            )
            return GammaDiscoveryRuntimeResource(settings=settings)


    class DagsterCapabilityRefreshRuntimeResource(ConfigurableResource):
        clob_base_url: str = "https://clob.polymarket.com"
        clob_book_endpoint: str = "/book"
        clob_fee_rate_endpoint: str = "/fee-rate"
        wallet_registry_path: str = "config/wallet_registry.json"
        capability_chain_id: int = 137
        capability_rpc_urls: list[str] = []

        def build_runtime(self) -> CapabilityRefreshRuntimeResource:
            settings = AsterionColdPathSettings.from_env()
            settings = AsterionColdPathSettings(
                db_path=settings.db_path,
                ddl_path=settings.ddl_path,
                write_queue_path=settings.write_queue_path,
                gamma_base_url=settings.gamma_base_url,
                gamma_markets_endpoint=settings.gamma_markets_endpoint,
                gamma_page_limit=settings.gamma_page_limit,
                gamma_max_pages=settings.gamma_max_pages,
                gamma_sleep_s=settings.gamma_sleep_s,
                gamma_active_only=settings.gamma_active_only,
                gamma_closed=settings.gamma_closed,
                gamma_archived=settings.gamma_archived,
                clob_base_url=self.clob_base_url,
                clob_book_endpoint=self.clob_book_endpoint,
                clob_fee_rate_endpoint=self.clob_fee_rate_endpoint,
                wallet_registry_path=self.wallet_registry_path,
                chain_registry_path=settings.chain_registry_path,
                capability_chain_id=int(self.capability_chain_id),
                capability_rpc_urls=list(self.capability_rpc_urls),
                signer_backend_kind=settings.signer_backend_kind,
                signer_rpc_url=settings.signer_rpc_url,
                submitter_backend_kind=settings.submitter_backend_kind,
                submitter_api_base_url=settings.submitter_api_base_url,
                chain_tx_backend_kind=settings.chain_tx_backend_kind,
                forecast_primary_source=settings.forecast_primary_source,
                forecast_fallback_sources=list(settings.forecast_fallback_sources),
                watcher_chain_id=settings.watcher_chain_id,
                watcher_rpc_urls=list(settings.watcher_rpc_urls),
            )
            return CapabilityRefreshRuntimeResource(settings=settings)


    class DagsterWalletStateObservationRuntimeResource(ConfigurableResource):
        chain_registry_path: str = "config/chain_registry.polygon.json"
        capability_chain_id: int = 137
        capability_rpc_urls: list[str] = []

        def build_runtime(self) -> WalletStateObservationRuntimeResource:
            settings = AsterionColdPathSettings.from_env()
            settings = AsterionColdPathSettings(
                db_path=settings.db_path,
                ddl_path=settings.ddl_path,
                write_queue_path=settings.write_queue_path,
                gamma_base_url=settings.gamma_base_url,
                gamma_markets_endpoint=settings.gamma_markets_endpoint,
                gamma_page_limit=settings.gamma_page_limit,
                gamma_max_pages=settings.gamma_max_pages,
                gamma_sleep_s=settings.gamma_sleep_s,
                gamma_active_only=settings.gamma_active_only,
                gamma_closed=settings.gamma_closed,
                gamma_archived=settings.gamma_archived,
                clob_base_url=settings.clob_base_url,
                clob_book_endpoint=settings.clob_book_endpoint,
                clob_fee_rate_endpoint=settings.clob_fee_rate_endpoint,
                wallet_registry_path=settings.wallet_registry_path,
                chain_registry_path=self.chain_registry_path,
                capability_chain_id=int(self.capability_chain_id),
                capability_rpc_urls=list(self.capability_rpc_urls),
                signer_backend_kind=settings.signer_backend_kind,
                signer_rpc_url=settings.signer_rpc_url,
                submitter_backend_kind=settings.submitter_backend_kind,
                submitter_api_base_url=settings.submitter_api_base_url,
                chain_tx_backend_kind=settings.chain_tx_backend_kind,
                forecast_primary_source=settings.forecast_primary_source,
                forecast_fallback_sources=list(settings.forecast_fallback_sources),
                watcher_chain_id=settings.watcher_chain_id,
                watcher_rpc_urls=list(settings.watcher_rpc_urls),
            )
            return WalletStateObservationRuntimeResource(settings=settings)


    class DagsterForecastRuntimeResource(ConfigurableResource):
        forecast_primary_source: str = "openmeteo"

        def build_runtime(self) -> ForecastRuntimeResource:
            settings = AsterionColdPathSettings.from_env()
            settings = AsterionColdPathSettings(
                db_path=settings.db_path,
                ddl_path=settings.ddl_path,
                write_queue_path=settings.write_queue_path,
                gamma_base_url=settings.gamma_base_url,
                gamma_markets_endpoint=settings.gamma_markets_endpoint,
                gamma_page_limit=settings.gamma_page_limit,
                gamma_max_pages=settings.gamma_max_pages,
                gamma_sleep_s=settings.gamma_sleep_s,
                gamma_active_only=settings.gamma_active_only,
                gamma_closed=settings.gamma_closed,
                gamma_archived=settings.gamma_archived,
                clob_base_url=settings.clob_base_url,
                clob_book_endpoint=settings.clob_book_endpoint,
                clob_fee_rate_endpoint=settings.clob_fee_rate_endpoint,
                wallet_registry_path=settings.wallet_registry_path,
                chain_registry_path=settings.chain_registry_path,
                capability_chain_id=settings.capability_chain_id,
                capability_rpc_urls=list(settings.capability_rpc_urls),
                signer_backend_kind=settings.signer_backend_kind,
                signer_rpc_url=settings.signer_rpc_url,
                submitter_backend_kind=settings.submitter_backend_kind,
                submitter_api_base_url=settings.submitter_api_base_url,
                chain_tx_backend_kind=settings.chain_tx_backend_kind,
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
                gamma_base_url=settings.gamma_base_url,
                gamma_markets_endpoint=settings.gamma_markets_endpoint,
                gamma_page_limit=settings.gamma_page_limit,
                gamma_max_pages=settings.gamma_max_pages,
                gamma_sleep_s=settings.gamma_sleep_s,
                gamma_active_only=settings.gamma_active_only,
                gamma_closed=settings.gamma_closed,
                gamma_archived=settings.gamma_archived,
                clob_base_url=settings.clob_base_url,
                clob_book_endpoint=settings.clob_book_endpoint,
                clob_fee_rate_endpoint=settings.clob_fee_rate_endpoint,
                wallet_registry_path=settings.wallet_registry_path,
                chain_registry_path=settings.chain_registry_path,
                capability_chain_id=settings.capability_chain_id,
                capability_rpc_urls=list(settings.capability_rpc_urls),
                signer_backend_kind=settings.signer_backend_kind,
                signer_rpc_url=settings.signer_rpc_url,
                submitter_backend_kind=settings.submitter_backend_kind,
                submitter_api_base_url=settings.submitter_api_base_url,
                chain_tx_backend_kind=settings.chain_tx_backend_kind,
                forecast_primary_source=settings.forecast_primary_source,
                forecast_fallback_sources=list(settings.forecast_fallback_sources),
                watcher_chain_id=int(self.watcher_chain_id),
                watcher_rpc_urls=list(settings.watcher_rpc_urls),
            )
            return WatcherRpcPoolResource(settings=settings)


    class DagsterSignerRuntimeResource(ConfigurableResource):
        signer_backend_kind: str = "disabled"
        signer_rpc_url: str | None = None

        def build_runtime(self) -> SignerRuntimeResource:
            settings = AsterionColdPathSettings.from_env()
            settings = AsterionColdPathSettings(
                db_path=settings.db_path,
                ddl_path=settings.ddl_path,
                write_queue_path=settings.write_queue_path,
                gamma_base_url=settings.gamma_base_url,
                gamma_markets_endpoint=settings.gamma_markets_endpoint,
                gamma_page_limit=settings.gamma_page_limit,
                gamma_max_pages=settings.gamma_max_pages,
                gamma_sleep_s=settings.gamma_sleep_s,
                gamma_active_only=settings.gamma_active_only,
                gamma_closed=settings.gamma_closed,
                gamma_archived=settings.gamma_archived,
                clob_base_url=settings.clob_base_url,
                clob_book_endpoint=settings.clob_book_endpoint,
                clob_fee_rate_endpoint=settings.clob_fee_rate_endpoint,
                wallet_registry_path=settings.wallet_registry_path,
                chain_registry_path=settings.chain_registry_path,
                capability_chain_id=settings.capability_chain_id,
                capability_rpc_urls=list(settings.capability_rpc_urls),
                signer_backend_kind=self.signer_backend_kind,
                signer_rpc_url=self.signer_rpc_url,
                submitter_backend_kind=settings.submitter_backend_kind,
                submitter_api_base_url=settings.submitter_api_base_url,
                chain_tx_backend_kind=settings.chain_tx_backend_kind,
                forecast_primary_source=settings.forecast_primary_source,
                forecast_fallback_sources=list(settings.forecast_fallback_sources),
                watcher_chain_id=settings.watcher_chain_id,
                watcher_rpc_urls=list(settings.watcher_rpc_urls),
            )
            return SignerRuntimeResource(settings=settings)


    class DagsterSubmitterRuntimeResource(ConfigurableResource):
        submitter_backend_kind: str = "disabled"
        submitter_api_base_url: str | None = None

        def build_runtime(self) -> SubmitterRuntimeResource:
            settings = AsterionColdPathSettings.from_env()
            settings = AsterionColdPathSettings(
                db_path=settings.db_path,
                ddl_path=settings.ddl_path,
                write_queue_path=settings.write_queue_path,
                gamma_base_url=settings.gamma_base_url,
                gamma_markets_endpoint=settings.gamma_markets_endpoint,
                gamma_page_limit=settings.gamma_page_limit,
                gamma_max_pages=settings.gamma_max_pages,
                gamma_sleep_s=settings.gamma_sleep_s,
                gamma_active_only=settings.gamma_active_only,
                gamma_closed=settings.gamma_closed,
                gamma_archived=settings.gamma_archived,
                clob_base_url=settings.clob_base_url,
                clob_book_endpoint=settings.clob_book_endpoint,
                clob_fee_rate_endpoint=settings.clob_fee_rate_endpoint,
                wallet_registry_path=settings.wallet_registry_path,
                chain_registry_path=settings.chain_registry_path,
                capability_chain_id=settings.capability_chain_id,
                capability_rpc_urls=list(settings.capability_rpc_urls),
                signer_backend_kind=settings.signer_backend_kind,
                signer_rpc_url=settings.signer_rpc_url,
                submitter_backend_kind=self.submitter_backend_kind,
                submitter_api_base_url=self.submitter_api_base_url,
                chain_tx_backend_kind=settings.chain_tx_backend_kind,
                forecast_primary_source=settings.forecast_primary_source,
                forecast_fallback_sources=list(settings.forecast_fallback_sources),
                watcher_chain_id=settings.watcher_chain_id,
                watcher_rpc_urls=list(settings.watcher_rpc_urls),
            )
            return SubmitterRuntimeResource(settings=settings)


    class DagsterChainTxRuntimeResource(ConfigurableResource):
        chain_tx_backend_kind: str = "disabled"
        chain_registry_path: str = "config/chain_registry.polygon.json"
        capability_chain_id: int = 137
        capability_rpc_urls: list[str] = []

        def build_runtime(self) -> ChainTxRuntimeResource:
            settings = AsterionColdPathSettings.from_env()
            settings = AsterionColdPathSettings(
                db_path=settings.db_path,
                ddl_path=settings.ddl_path,
                write_queue_path=settings.write_queue_path,
                gamma_base_url=settings.gamma_base_url,
                gamma_markets_endpoint=settings.gamma_markets_endpoint,
                gamma_page_limit=settings.gamma_page_limit,
                gamma_max_pages=settings.gamma_max_pages,
                gamma_sleep_s=settings.gamma_sleep_s,
                gamma_active_only=settings.gamma_active_only,
                gamma_closed=settings.gamma_closed,
                gamma_archived=settings.gamma_archived,
                clob_base_url=settings.clob_base_url,
                clob_book_endpoint=settings.clob_book_endpoint,
                clob_fee_rate_endpoint=settings.clob_fee_rate_endpoint,
                wallet_registry_path=settings.wallet_registry_path,
                chain_registry_path=self.chain_registry_path,
                capability_chain_id=int(self.capability_chain_id),
                capability_rpc_urls=list(self.capability_rpc_urls),
                signer_backend_kind=settings.signer_backend_kind,
                signer_rpc_url=settings.signer_rpc_url,
                submitter_backend_kind=settings.submitter_backend_kind,
                submitter_api_base_url=settings.submitter_api_base_url,
                chain_tx_backend_kind=self.chain_tx_backend_kind,
                forecast_primary_source=settings.forecast_primary_source,
                forecast_fallback_sources=list(settings.forecast_fallback_sources),
                watcher_chain_id=settings.watcher_chain_id,
                watcher_rpc_urls=list(settings.watcher_rpc_urls),
            )
            return ChainTxRuntimeResource(settings=settings)


def build_runtime_resources(settings: AsterionColdPathSettings | None = None) -> dict[str, Any]:
    active = settings or AsterionColdPathSettings.from_env()
    return {
        "cold_path_settings": active,
        "duckdb": DuckDBResource(settings=active),
        "write_queue": WriteQueueResource(settings=active),
        "gamma_discovery_runtime": GammaDiscoveryRuntimeResource(settings=active),
        "capability_refresh_runtime": CapabilityRefreshRuntimeResource(settings=active),
        "wallet_state_observation_runtime": WalletStateObservationRuntimeResource(settings=active),
        "signer_runtime": SignerRuntimeResource(settings=active),
        "submitter_runtime": SubmitterRuntimeResource(settings=active),
        "chain_tx_runtime": ChainTxRuntimeResource(settings=active),
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
        "gamma_discovery_runtime": DagsterGammaDiscoveryRuntimeResource(
            gamma_base_url=active.gamma_base_url,
            gamma_markets_endpoint=active.gamma_markets_endpoint,
            gamma_page_limit=active.gamma_page_limit,
            gamma_max_pages=active.gamma_max_pages,
            gamma_sleep_s=active.gamma_sleep_s,
            gamma_active_only=active.gamma_active_only,
            gamma_closed=active.gamma_closed,
            gamma_archived=active.gamma_archived,
        ),
        "capability_refresh_runtime": DagsterCapabilityRefreshRuntimeResource(
            clob_base_url=active.clob_base_url,
            clob_book_endpoint=active.clob_book_endpoint,
            clob_fee_rate_endpoint=active.clob_fee_rate_endpoint,
            wallet_registry_path=active.wallet_registry_path,
            capability_chain_id=active.capability_chain_id,
            capability_rpc_urls=list(active.capability_rpc_urls),
        ),
        "wallet_state_observation_runtime": DagsterWalletStateObservationRuntimeResource(
            chain_registry_path=active.chain_registry_path,
            capability_chain_id=active.capability_chain_id,
            capability_rpc_urls=list(active.capability_rpc_urls),
        ),
        "signer_runtime": DagsterSignerRuntimeResource(
            signer_backend_kind=active.signer_backend_kind,
            signer_rpc_url=active.signer_rpc_url,
        ),
        "submitter_runtime": DagsterSubmitterRuntimeResource(
            submitter_backend_kind=active.submitter_backend_kind,
            submitter_api_base_url=active.submitter_api_base_url,
        ),
        "chain_tx_runtime": DagsterChainTxRuntimeResource(
            chain_tx_backend_kind=active.chain_tx_backend_kind,
            chain_registry_path=active.chain_registry_path,
            capability_chain_id=active.capability_chain_id,
            capability_rpc_urls=list(active.capability_rpc_urls),
        ),
        "forecast_runtime": DagsterForecastRuntimeResource(forecast_primary_source=active.forecast_primary_source),
        "watcher_rpc_pool": DagsterWatcherRpcPoolResource(watcher_chain_id=active.watcher_chain_id),
    }


def _env_bool(value: str | None, *, default: bool) -> bool:
    if value is None or not value.strip():
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


def _env_optional_bool(value: str | None) -> bool | None:
    if value is None or not value.strip():
        return None
    normalized = value.strip().lower()
    if normalized in {"none", "null"}:
        return None
    return normalized not in {"0", "false", "no", "off"}
