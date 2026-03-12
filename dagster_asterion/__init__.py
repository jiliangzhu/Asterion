from __future__ import annotations

import importlib.util

from .handlers import (
    ChainTxSmokeRequest,
    ColdPathHandlerResult,
    OrderSigningSmokeRequest,
    PaperExecutionBatchRequest,
    SignerAuditSmokeRequest,
    SubmitterSmokeRequest,
    SettlementVerificationInput,
    run_weather_capability_refresh_job,
    run_weather_chain_tx_smoke_job,
    run_weather_data_qa_review_job,
    run_weather_external_execution_reconciliation_job,
    run_weather_live_prereq_readiness_job,
    run_weather_market_discovery_job,
    run_weather_forecast_refresh,
    run_weather_forecast_replay_job,
    run_weather_order_signing_smoke_job,
    run_weather_paper_execution_job,
    run_weather_signer_audit_smoke_job,
    run_weather_submitter_smoke_job,
    run_weather_wallet_state_refresh_job,
    run_weather_resolution_review_job,
    run_weather_resolution_reconciliation,
    run_weather_rule2spec_review_job,
    run_weather_spec_sync,
    run_weather_watcher_backfill_job,
)
from .job_map import (
    ColdPathJobSpec,
    ColdPathRunRequest,
    ColdPathScheduleSpec,
    build_weather_cold_path_job_map,
    build_weather_cold_path_schedule_map,
    get_job_spec,
    get_schedule_spec,
    list_weather_cold_path_jobs,
    list_weather_cold_path_schedules,
    resolve_default_enabled_schedule_specs,
)
from .jobs import build_job_definitions
from .resources import (
    DAGSTER_AVAILABLE,
    AsterionColdPathSettings,
    CapabilityRefreshRuntimeResource,
    ChainTxRuntimeResource,
    DuckDBResource,
    ForecastRuntimeResource,
    GammaDiscoveryRuntimeResource,
    HttpJsonClient,
    LivePrereqReadinessRuntimeResource,
    SignerRuntimeResource,
    SubmitterRuntimeResource,
    WalletStateObservationRuntimeResource,
    WatcherRpcPoolResource,
    WriteQueueResource,
    build_dagster_resource_defs,
    build_runtime_resources,
)
from .schedules import build_schedule_definitions, list_enabled_schedule_keys


defs = None
if importlib.util.find_spec("dagster") is not None:  # pragma: no cover - optional dependency
    from dagster import Definitions

    def build_definitions(settings: AsterionColdPathSettings | None = None):
        jobs = build_job_definitions()
        schedules = build_schedule_definitions(job_definitions=jobs)
        resources = build_dagster_resource_defs(settings)
        return Definitions(jobs=list(jobs.values()), schedules=schedules, resources=resources)

    defs = build_definitions()
else:
    def build_definitions(settings: AsterionColdPathSettings | None = None):
        return None


__all__ = [
    "AsterionColdPathSettings",
    "CapabilityRefreshRuntimeResource",
    "ChainTxRuntimeResource",
    "ChainTxSmokeRequest",
    "ColdPathHandlerResult",
    "ColdPathJobSpec",
    "ColdPathRunRequest",
    "ColdPathScheduleSpec",
    "DAGSTER_AVAILABLE",
    "DuckDBResource",
    "ForecastRuntimeResource",
    "GammaDiscoveryRuntimeResource",
    "HttpJsonClient",
    "LivePrereqReadinessRuntimeResource",
    "OrderSigningSmokeRequest",
    "PaperExecutionBatchRequest",
    "SignerAuditSmokeRequest",
    "SignerRuntimeResource",
    "SubmitterRuntimeResource",
    "SubmitterSmokeRequest",
    "SettlementVerificationInput",
    "WalletStateObservationRuntimeResource",
    "WatcherRpcPoolResource",
    "WriteQueueResource",
    "build_dagster_resource_defs",
    "build_definitions",
    "build_job_definitions",
    "build_runtime_resources",
    "build_schedule_definitions",
    "build_weather_cold_path_job_map",
    "build_weather_cold_path_schedule_map",
    "get_job_spec",
    "get_schedule_spec",
    "list_enabled_schedule_keys",
    "list_weather_cold_path_jobs",
    "list_weather_cold_path_schedules",
    "resolve_default_enabled_schedule_specs",
    "run_weather_data_qa_review_job",
    "run_weather_capability_refresh_job",
    "run_weather_chain_tx_smoke_job",
    "run_weather_external_execution_reconciliation_job",
    "run_weather_live_prereq_readiness_job",
    "run_weather_market_discovery_job",
    "run_weather_forecast_refresh",
    "run_weather_forecast_replay_job",
    "run_weather_order_signing_smoke_job",
    "run_weather_paper_execution_job",
    "run_weather_signer_audit_smoke_job",
    "run_weather_submitter_smoke_job",
    "run_weather_wallet_state_refresh_job",
    "run_weather_resolution_review_job",
    "run_weather_resolution_reconciliation",
    "run_weather_rule2spec_review_job",
    "run_weather_spec_sync",
    "run_weather_watcher_backfill_job",
    "defs",
]
