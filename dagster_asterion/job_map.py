from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ColdPathJobSpec:
    job_name: str
    description: str
    mode: str
    upstream_jobs: list[str]
    input_tables: list[str]
    output_tables: list[str]
    handler_name: str
    default_schedule_key: str | None = None

    def __post_init__(self) -> None:
        if not self.job_name or not self.description or not self.handler_name:
            raise ValueError("job_name, description, and handler_name are required")
        if self.mode not in {"scheduled", "manual"}:
            raise ValueError("mode must be scheduled or manual")


@dataclass(frozen=True)
class ColdPathScheduleSpec:
    schedule_key: str
    job_name: str
    cron_schedule: str
    execution_timezone: str
    enabled_by_default: bool

    def __post_init__(self) -> None:
        if not self.schedule_key or not self.job_name or not self.cron_schedule or not self.execution_timezone:
            raise ValueError("schedule_key, job_name, cron_schedule, and execution_timezone are required")


@dataclass(frozen=True)
class ColdPathRunRequest:
    job_name: str
    run_reason: str
    partition_key: str | None = None
    params_json: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.job_name or not self.run_reason:
            raise ValueError("job_name and run_reason are required")
        if not isinstance(self.params_json, dict):
            raise ValueError("params_json must be a dictionary")


_JOB_SPECS = [
    ColdPathJobSpec(
        job_name="weather_market_discovery",
        description="Discover canonical weather markets from Gamma ingress and persist weather.weather_markets.",
        mode="scheduled",
        upstream_jobs=[],
        input_tables=[],
        output_tables=["weather.weather_markets"],
        handler_name="run_weather_market_discovery_job",
        default_schedule_key="weather_market_discovery_daily",
    ),
    ColdPathJobSpec(
        job_name="weather_spec_sync",
        description="Parse weather markets into station-first specs via Rule2Spec and StationMapper.",
        mode="scheduled",
        upstream_jobs=["weather_market_discovery"],
        input_tables=["weather.weather_markets", "weather.weather_station_map"],
        output_tables=["weather.weather_market_specs"],
        handler_name="run_weather_spec_sync",
        default_schedule_key="weather_spec_sync_daily",
    ),
    ColdPathJobSpec(
        job_name="weather_capability_refresh",
        description="Refresh canonical weather market/account capabilities from Gamma, CLOB public, wallet registry, and overrides.",
        mode="scheduled",
        upstream_jobs=["weather_market_discovery"],
        input_tables=["weather.weather_markets", "capability.capability_overrides"],
        output_tables=["capability.market_capabilities", "capability.account_trading_capabilities"],
        handler_name="run_weather_capability_refresh_job",
        default_schedule_key="weather_capability_refresh_hourly",
    ),
    ColdPathJobSpec(
        job_name="weather_forecast_refresh",
        description="Refresh forecast runs from canonical weather market specs.",
        mode="scheduled",
        upstream_jobs=["weather_spec_sync"],
        input_tables=["weather.weather_market_specs"],
        output_tables=["weather.weather_forecast_runs"],
        handler_name="run_weather_forecast_refresh",
        default_schedule_key="weather_forecast_refresh_hourly",
    ),
    ColdPathJobSpec(
        job_name="weather_wallet_state_refresh",
        description="Observe canonical external wallet balances and allowances for weather trading wallets.",
        mode="scheduled",
        upstream_jobs=["weather_capability_refresh"],
        input_tables=["capability.account_trading_capabilities"],
        output_tables=["runtime.external_balance_observations", "runtime.journal_events"],
        handler_name="run_weather_wallet_state_refresh_job",
        default_schedule_key="weather_wallet_state_refresh_hourly",
    ),
    ColdPathJobSpec(
        job_name="weather_signer_audit_smoke",
        description="Run the canonical manual signer shell smoke and persist signature audit records without real signing side effects.",
        mode="manual",
        upstream_jobs=["weather_capability_refresh"],
        input_tables=["capability.account_trading_capabilities", "meta.signature_audit_logs"],
        output_tables=["meta.signature_audit_logs", "runtime.journal_events"],
        handler_name="run_weather_signer_audit_smoke_job",
        default_schedule_key=None,
    ),
    ColdPathJobSpec(
        job_name="weather_order_signing_smoke",
        description="Run the canonical manual official-order-compatible signing smoke and persist signer audit plus sign-only submit attempts.",
        mode="manual",
        upstream_jobs=["weather_paper_execution"],
        input_tables=[
            "runtime.trade_tickets",
            "capability.execution_contexts",
            "capability.market_capabilities",
            "capability.account_trading_capabilities",
            "meta.signature_audit_logs",
        ],
        output_tables=["meta.signature_audit_logs", "runtime.submit_attempts", "runtime.journal_events"],
        handler_name="run_weather_order_signing_smoke_job",
        default_schedule_key=None,
    ),
    ColdPathJobSpec(
        job_name="weather_submitter_smoke",
        description="Run the canonical manual submitter smoke over signed official-compatible payloads and persist submit attempts plus external order observations.",
        mode="manual",
        upstream_jobs=["weather_order_signing_smoke"],
        input_tables=[
            "runtime.submit_attempts",
            "runtime.trade_tickets",
            "capability.execution_contexts",
        ],
        output_tables=[
            "runtime.submit_attempts",
            "runtime.external_order_observations",
            "runtime.external_fill_observations",
            "runtime.journal_events",
        ],
        handler_name="run_weather_submitter_smoke_job",
        default_schedule_key=None,
    ),
    ColdPathJobSpec(
        job_name="weather_external_execution_reconciliation",
        description="Reconcile local execution ledger against external submit/order/fill observations and persist external-aware reconciliation results.",
        mode="scheduled",
        upstream_jobs=["weather_submitter_smoke", "weather_wallet_state_refresh"],
        input_tables=[
            "runtime.submit_attempts",
            "runtime.external_order_observations",
            "runtime.external_fill_observations",
            "runtime.external_balance_observations",
            "trading.orders",
            "trading.fills",
        ],
        output_tables=[
            "trading.reconciliation_results",
            "runtime.journal_events",
        ],
        handler_name="run_weather_external_execution_reconciliation_job",
        default_schedule_key="weather_external_execution_reconciliation_hourly",
    ),
    ColdPathJobSpec(
        job_name="weather_chain_tx_smoke",
        description="Run the canonical manual chain transaction scaffold smoke for approve_usdc and persist chain tx attempts plus signer audit and journal events.",
        mode="manual",
        upstream_jobs=["weather_wallet_state_refresh"],
        input_tables=[
            "capability.account_trading_capabilities",
            "runtime.external_balance_observations",
        ],
        output_tables=[
            "runtime.chain_tx_attempts",
            "meta.signature_audit_logs",
            "runtime.journal_events",
        ],
        handler_name="run_weather_chain_tx_smoke_job",
        default_schedule_key=None,
    ),
    ColdPathJobSpec(
        job_name="weather_forecast_replay",
        description="Replay persisted forecast/pricing outputs from canonical replay keys.",
        mode="manual",
        upstream_jobs=["weather_forecast_refresh"],
        input_tables=[
            "weather.weather_market_specs",
            "weather.weather_forecast_runs",
            "weather.weather_fair_values",
            "weather.weather_watch_only_snapshots",
        ],
        output_tables=["weather.weather_forecast_replays", "weather.weather_forecast_replay_diffs"],
        handler_name="run_weather_forecast_replay_job",
        default_schedule_key=None,
    ),
    ColdPathJobSpec(
        job_name="weather_paper_execution",
        description="Run the canonical manual paper execution batch from selected watch-only snapshots.",
        mode="manual",
        upstream_jobs=["weather_forecast_replay", "weather_capability_refresh"],
        input_tables=[
            "weather.weather_watch_only_snapshots",
            "capability.market_capabilities",
            "capability.account_trading_capabilities",
        ],
        output_tables=[
            "runtime.strategy_runs",
            "runtime.trade_tickets",
            "capability.execution_contexts",
        ],
        handler_name="run_weather_paper_execution_job",
        default_schedule_key=None,
    ),
    ColdPathJobSpec(
        job_name="weather_watcher_backfill",
        description="Backfill finalized UMA watcher state using block watermarks and RPC fallback.",
        mode="scheduled",
        upstream_jobs=[],
        input_tables=["resolution.block_watermarks"],
        output_tables=[
            "resolution.uma_proposals",
            "resolution.proposal_state_transitions",
            "resolution.processed_uma_events",
            "resolution.block_watermarks",
            "resolution.watcher_continuity_checks",
            "resolution.watcher_continuity_gaps",
        ],
        handler_name="run_weather_watcher_backfill_job",
        default_schedule_key="weather_watcher_backfill_bihourly",
    ),
    ColdPathJobSpec(
        job_name="weather_resolution_reconciliation",
        description="Persist settlement verification, evidence linkage, and redeem suggestions for watched proposals.",
        mode="scheduled",
        upstream_jobs=["weather_watcher_backfill"],
        input_tables=["resolution.uma_proposals", "resolution.settlement_verifications"],
        output_tables=[
            "resolution.settlement_verifications",
            "resolution.proposal_evidence_links",
            "resolution.redeem_readiness_suggestions",
        ],
        handler_name="run_weather_resolution_reconciliation",
        default_schedule_key="weather_resolution_reconciliation_bihourly",
    ),
    ColdPathJobSpec(
        job_name="weather_rule2spec_review",
        description="Run Rule2Spec Agent review over deterministic station-first spec parsing outputs.",
        mode="manual",
        upstream_jobs=["weather_spec_sync"],
        input_tables=[
            "weather.weather_markets",
            "weather.weather_station_map",
            "weather.weather_market_specs",
        ],
        output_tables=[
            "agent.invocations",
            "agent.outputs",
            "agent.reviews",
            "agent.evaluations",
        ],
        handler_name="run_weather_rule2spec_review_job",
        default_schedule_key=None,
    ),
    ColdPathJobSpec(
        job_name="weather_data_qa_review",
        description="Run Data QA Agent review over forecast replay outputs and pricing provenance.",
        mode="manual",
        upstream_jobs=["weather_forecast_replay"],
        input_tables=[
            "weather.weather_market_specs",
            "weather.weather_forecast_runs",
            "weather.weather_forecast_replays",
            "weather.weather_forecast_replay_diffs",
            "weather.weather_fair_values",
            "weather.weather_watch_only_snapshots",
        ],
        output_tables=[
            "agent.invocations",
            "agent.outputs",
            "agent.reviews",
            "agent.evaluations",
        ],
        handler_name="run_weather_data_qa_review_job",
        default_schedule_key=None,
    ),
    ColdPathJobSpec(
        job_name="weather_resolution_review",
        description="Run Resolution Agent review over settlement verification, evidence linkage, and redeem readiness.",
        mode="manual",
        upstream_jobs=["weather_resolution_reconciliation"],
        input_tables=[
            "resolution.uma_proposals",
            "resolution.settlement_verifications",
            "resolution.proposal_evidence_links",
            "resolution.redeem_readiness_suggestions",
            "resolution.watcher_continuity_checks",
        ],
        output_tables=[
            "agent.invocations",
            "agent.outputs",
            "agent.reviews",
            "agent.evaluations",
        ],
        handler_name="run_weather_resolution_review_job",
        default_schedule_key=None,
    ),
]

_SCHEDULE_SPECS = [
    ColdPathScheduleSpec(
        schedule_key="weather_market_discovery_daily",
        job_name="weather_market_discovery",
        cron_schedule="0 0 * * *",
        execution_timezone="UTC",
        enabled_by_default=True,
    ),
    ColdPathScheduleSpec(
        schedule_key="weather_spec_sync_daily",
        job_name="weather_spec_sync",
        cron_schedule="15 0 * * *",
        execution_timezone="UTC",
        enabled_by_default=True,
    ),
    ColdPathScheduleSpec(
        schedule_key="weather_capability_refresh_hourly",
        job_name="weather_capability_refresh",
        cron_schedule="25 * * * *",
        execution_timezone="UTC",
        enabled_by_default=True,
    ),
    ColdPathScheduleSpec(
        schedule_key="weather_forecast_refresh_hourly",
        job_name="weather_forecast_refresh",
        cron_schedule="10 * * * *",
        execution_timezone="UTC",
        enabled_by_default=True,
    ),
    ColdPathScheduleSpec(
        schedule_key="weather_wallet_state_refresh_hourly",
        job_name="weather_wallet_state_refresh",
        cron_schedule="35 * * * *",
        execution_timezone="UTC",
        enabled_by_default=True,
    ),
    ColdPathScheduleSpec(
        schedule_key="weather_external_execution_reconciliation_hourly",
        job_name="weather_external_execution_reconciliation",
        cron_schedule="45 * * * *",
        execution_timezone="UTC",
        enabled_by_default=True,
    ),
    ColdPathScheduleSpec(
        schedule_key="weather_forecast_replay_manual",
        job_name="weather_forecast_replay",
        cron_schedule="0 0 * * *",
        execution_timezone="UTC",
        enabled_by_default=False,
    ),
    ColdPathScheduleSpec(
        schedule_key="weather_watcher_backfill_bihourly",
        job_name="weather_watcher_backfill",
        cron_schedule="20 */2 * * *",
        execution_timezone="UTC",
        enabled_by_default=True,
    ),
    ColdPathScheduleSpec(
        schedule_key="weather_resolution_reconciliation_bihourly",
        job_name="weather_resolution_reconciliation",
        cron_schedule="35 */2 * * *",
        execution_timezone="UTC",
        enabled_by_default=True,
    ),
]


def build_weather_cold_path_job_map() -> dict[str, ColdPathJobSpec]:
    return {item.job_name: item for item in _JOB_SPECS}


def build_weather_cold_path_schedule_map() -> dict[str, ColdPathScheduleSpec]:
    return {item.schedule_key: item for item in _SCHEDULE_SPECS}


def list_weather_cold_path_jobs() -> list[ColdPathJobSpec]:
    return list(_JOB_SPECS)


def list_weather_cold_path_schedules() -> list[ColdPathScheduleSpec]:
    return list(_SCHEDULE_SPECS)


def get_job_spec(job_name: str) -> ColdPathJobSpec:
    try:
        return build_weather_cold_path_job_map()[job_name]
    except KeyError as exc:
        raise LookupError(f"unknown cold-path job: {job_name}") from exc


def get_schedule_spec(schedule_key: str) -> ColdPathScheduleSpec:
    try:
        return build_weather_cold_path_schedule_map()[schedule_key]
    except KeyError as exc:
        raise LookupError(f"unknown cold-path schedule: {schedule_key}") from exc


def resolve_default_enabled_schedule_specs() -> list[ColdPathScheduleSpec]:
    return [item for item in _SCHEDULE_SPECS if item.enabled_by_default]
