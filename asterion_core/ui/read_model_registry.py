from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class ReadModelCatalogRecord:
    table_name: str
    schema_version: str
    builder_name: str
    primary_key_columns: tuple[str, ...]
    primary_score_column: str | None
    truth_source_description: str
    required_columns: tuple[str, ...]

    def as_row(self, *, updated_at: str) -> dict[str, Any]:
        row = asdict(self)
        row["primary_key_columns"] = list(self.primary_key_columns)
        row["required_columns"] = list(self.required_columns)
        row["updated_at"] = updated_at
        return row


@dataclass(frozen=True)
class TruthSourceCheckRecord:
    check_id: str
    surface_id: str
    table_name: str
    check_status: str
    issues: tuple[str, ...]

    def as_row(self, *, checked_at: str) -> dict[str, Any]:
        return {
            "check_id": self.check_id,
            "surface_id": self.surface_id,
            "table_name": self.table_name,
            "check_status": self.check_status,
            "issues": list(self.issues),
            "checked_at": checked_at,
        }


@dataclass(frozen=True)
class SurfaceTruthCheckSpec:
    surface_id: str
    table_names: tuple[str, ...]
    critical: bool = True


READ_MODEL_SCHEMA_VERSION = "v1"

_READ_MODEL_RECORDS: tuple[ReadModelCatalogRecord, ...] = (
    ReadModelCatalogRecord(
        table_name="ui.market_watch_summary",
        schema_version=READ_MODEL_SCHEMA_VERSION,
        builder_name="opportunity_builder",
        primary_key_columns=("market_id",),
        primary_score_column=None,
        truth_source_description="latest weather watch snapshot read model",
        required_columns=("market_id",),
    ),
    ReadModelCatalogRecord(
        table_name="ui.market_opportunity_summary",
        schema_version=READ_MODEL_SCHEMA_VERSION,
        builder_name="opportunity_builder",
        primary_key_columns=("market_id",),
        primary_score_column="ranking_score",
        truth_source_description="canonical operator opportunity summary",
        required_columns=(
            "market_id",
            "ranking_score",
            "base_ranking_score",
            "deployable_expected_pnl",
            "calibration_gate_status",
            "calibration_impacted_market",
            "recommended_size",
            "allocation_status",
            "source_badge",
            "source_truth_status",
            "primary_score_label",
        ),
    ),
    ReadModelCatalogRecord(
        table_name="ui.proposal_resolution_summary",
        schema_version=READ_MODEL_SCHEMA_VERSION,
        builder_name="ops_review_builder",
        primary_key_columns=("proposal_id",),
        primary_score_column=None,
        truth_source_description="proposal and resolution summary",
        required_columns=(
            "proposal_id",
            "latest_agent_invocation_id",
            "latest_agent_verdict",
            "latest_recommended_operator_action",
            "latest_settlement_risk_score",
            "latest_operator_review_status",
            "latest_operator_action",
            "effective_redeem_status",
        ),
    ),
    ReadModelCatalogRecord(
        table_name="ui.execution_ticket_summary",
        schema_version=READ_MODEL_SCHEMA_VERSION,
        builder_name="execution_builder",
        primary_key_columns=("ticket_id",),
        primary_score_column=None,
        truth_source_description="execution ticket lifecycle summary",
        required_columns=("ticket_id",),
    ),
    ReadModelCatalogRecord(
        table_name="ui.execution_run_summary",
        schema_version=READ_MODEL_SCHEMA_VERSION,
        builder_name="execution_builder",
        primary_key_columns=("run_id",),
        primary_score_column=None,
        truth_source_description="strategy run execution summary",
        required_columns=("run_id",),
    ),
    ReadModelCatalogRecord(
        table_name="ui.execution_exception_summary",
        schema_version=READ_MODEL_SCHEMA_VERSION,
        builder_name="execution_builder",
        primary_key_columns=("ticket_id",),
        primary_score_column=None,
        truth_source_description="execution exception attention queue",
        required_columns=("ticket_id",),
    ),
    ReadModelCatalogRecord(
        table_name="ui.live_prereq_execution_summary",
        schema_version=READ_MODEL_SCHEMA_VERSION,
        builder_name="readiness_builder",
        primary_key_columns=("ticket_id",),
        primary_score_column=None,
        truth_source_description="live-prereq execution readiness summary",
        required_columns=("ticket_id",),
    ),
    ReadModelCatalogRecord(
        table_name="ui.live_prereq_wallet_summary",
        schema_version=READ_MODEL_SCHEMA_VERSION,
        builder_name="readiness_builder",
        primary_key_columns=("wallet_id",),
        primary_score_column=None,
        truth_source_description="live-prereq wallet readiness summary",
        required_columns=("wallet_id",),
    ),
    ReadModelCatalogRecord(
        table_name="ui.paper_run_journal_summary",
        schema_version=READ_MODEL_SCHEMA_VERSION,
        builder_name="ops_review_builder",
        primary_key_columns=("run_id",),
        primary_score_column=None,
        truth_source_description="paper run journal summary",
        required_columns=("run_id",),
    ),
    ReadModelCatalogRecord(
        table_name="ui.daily_ops_summary",
        schema_version=READ_MODEL_SCHEMA_VERSION,
        builder_name="ops_review_builder",
        primary_key_columns=("run_id",),
        primary_score_column=None,
        truth_source_description="daily ops summary",
        required_columns=("run_id",),
    ),
    ReadModelCatalogRecord(
        table_name="ui.daily_review_input",
        schema_version=READ_MODEL_SCHEMA_VERSION,
        builder_name="ops_review_builder",
        primary_key_columns=("item_id",),
        primary_score_column=None,
        truth_source_description="daily review input",
        required_columns=("item_id",),
    ),
    ReadModelCatalogRecord(
        table_name="ui.agent_review_summary",
        schema_version=READ_MODEL_SCHEMA_VERSION,
        builder_name="ops_review_builder",
        primary_key_columns=("agent_type", "subject_type", "subject_id"),
        primary_score_column=None,
        truth_source_description="agent review summary",
        required_columns=("agent_type", "subject_type", "subject_id"),
    ),
    ReadModelCatalogRecord(
        table_name="ui.phase_readiness_summary",
        schema_version=READ_MODEL_SCHEMA_VERSION,
        builder_name="readiness_builder",
        primary_key_columns=("gate_name",),
        primary_score_column=None,
        truth_source_description="phase readiness summary",
        required_columns=("gate_name", "status"),
    ),
    ReadModelCatalogRecord(
        table_name="ui.readiness_evidence_summary",
        schema_version=READ_MODEL_SCHEMA_VERSION,
        builder_name="readiness_builder",
        primary_key_columns=("generated_at",),
        primary_score_column=None,
        truth_source_description="readiness evidence summary",
        required_columns=("generated_at",),
    ),
    ReadModelCatalogRecord(
        table_name="ui.predicted_vs_realized_summary",
        schema_version=READ_MODEL_SCHEMA_VERSION,
        builder_name="execution_builder",
        primary_key_columns=("ticket_id",),
        primary_score_column=None,
        truth_source_description="execution-path predicted vs realized summary",
        required_columns=("ticket_id", "source_badge", "source_truth_status", "primary_score_label"),
    ),
    ReadModelCatalogRecord(
        table_name="ui.watch_only_vs_executed_summary",
        schema_version=READ_MODEL_SCHEMA_VERSION,
        builder_name="execution_builder",
        primary_key_columns=("market_id",),
        primary_score_column=None,
        truth_source_description="watch-only opportunity capture summary",
        required_columns=("market_id", "source_badge", "source_truth_status", "primary_score_label"),
    ),
    ReadModelCatalogRecord(
        table_name="ui.execution_science_summary",
        schema_version=READ_MODEL_SCHEMA_VERSION,
        builder_name="execution_builder",
        primary_key_columns=("cohort_type", "cohort_key"),
        primary_score_column=None,
        truth_source_description="execution science cohort summary",
        required_columns=("cohort_type", "cohort_key", "source_badge", "source_truth_status", "primary_score_label"),
    ),
    ReadModelCatalogRecord(
        table_name="ui.market_research_summary",
        schema_version=READ_MODEL_SCHEMA_VERSION,
        builder_name="execution_builder",
        primary_key_columns=("market_id",),
        primary_score_column=None,
        truth_source_description="market research execution evidence summary",
        required_columns=("market_id",),
    ),
    ReadModelCatalogRecord(
        table_name="ui.calibration_health_summary",
        schema_version=READ_MODEL_SCHEMA_VERSION,
        builder_name="opportunity_builder",
        primary_key_columns=("station_id", "source", "metric", "forecast_horizon_bucket"),
        primary_score_column=None,
        truth_source_description="forecast calibration health summary",
        required_columns=(
            "station_id",
            "source",
            "metric",
            "forecast_horizon_bucket",
            "impacted_market_count",
            "hard_gate_market_count",
            "review_required_market_count",
            "research_only_market_count",
        ),
    ),
    ReadModelCatalogRecord(
        table_name="ui.action_queue_summary",
        schema_version=READ_MODEL_SCHEMA_VERSION,
        builder_name="opportunity_builder",
        primary_key_columns=("queue_item_id",),
        primary_score_column="ranking_score",
        truth_source_description="operator action queue summary",
        required_columns=(
            "queue_item_id",
            "ranking_score",
            "base_ranking_score",
            "deployable_expected_pnl",
            "operator_bucket",
            "calibration_gate_status",
            "capital_policy_id",
            "recommended_size",
            "allocation_status",
            "binding_limit_scope",
            "source_badge",
            "source_truth_status",
            "primary_score_label",
        ),
    ),
    ReadModelCatalogRecord(
        table_name="ui.cohort_history_summary",
        schema_version=READ_MODEL_SCHEMA_VERSION,
        builder_name="execution_builder",
        primary_key_columns=("history_row_id",),
        primary_score_column=None,
        truth_source_description="retrospective cohort history summary",
        required_columns=(
            "history_row_id",
            "market_id",
            "strategy_id",
            "ranking_decile",
            "submitted_capture_ratio",
            "fill_capture_ratio",
            "resolution_capture_ratio",
            "source_badge",
            "source_truth_status",
            "primary_score_label",
        ),
    ),
    ReadModelCatalogRecord(
        table_name="ui.read_model_catalog",
        schema_version=READ_MODEL_SCHEMA_VERSION,
        builder_name="catalog_builder",
        primary_key_columns=("table_name",),
        primary_score_column=None,
        truth_source_description="internal ui read model catalog",
        required_columns=("table_name", "schema_version", "builder_name", "required_columns_json"),
    ),
    ReadModelCatalogRecord(
        table_name="ui.truth_source_checks",
        schema_version=READ_MODEL_SCHEMA_VERSION,
        builder_name="catalog_builder",
        primary_key_columns=("check_id",),
        primary_score_column=None,
        truth_source_description="internal ui truth source check results",
        required_columns=("check_id", "surface_id", "table_name", "check_status"),
    ),
)

_SURFACE_SPECS: tuple[SurfaceTruthCheckSpec, ...] = (
    SurfaceTruthCheckSpec(
        surface_id="home",
        table_names=(
            "ui.market_opportunity_summary",
            "ui.action_queue_summary",
            "ui.watch_only_vs_executed_summary",
            "ui.phase_readiness_summary",
            "ui.readiness_evidence_summary",
        ),
    ),
    SurfaceTruthCheckSpec(
        surface_id="markets",
        table_names=(
            "ui.market_watch_summary",
            "ui.market_opportunity_summary",
            "ui.action_queue_summary",
            "ui.cohort_history_summary",
            "ui.market_research_summary",
            "ui.watch_only_vs_executed_summary",
        ),
    ),
    SurfaceTruthCheckSpec(
        surface_id="execution",
        table_names=(
            "ui.execution_ticket_summary",
            "ui.execution_run_summary",
            "ui.execution_exception_summary",
            "ui.predicted_vs_realized_summary",
            "ui.execution_science_summary",
            "ui.cohort_history_summary",
            "ui.watch_only_vs_executed_summary",
        ),
    ),
    SurfaceTruthCheckSpec(
        surface_id="system",
        table_names=(
            "ui.calibration_health_summary",
            "ui.phase_readiness_summary",
            "ui.readiness_evidence_summary",
            "ui.live_prereq_execution_summary",
            "ui.live_prereq_wallet_summary",
            "ui.proposal_resolution_summary",
        ),
    ),
    SurfaceTruthCheckSpec(
        surface_id="agents",
        table_names=("ui.agent_review_summary", "ui.proposal_resolution_summary"),
    ),
)


def iter_read_model_catalog_records() -> tuple[ReadModelCatalogRecord, ...]:
    return _READ_MODEL_RECORDS


def get_read_model_catalog_record(table_name: str) -> ReadModelCatalogRecord | None:
    for record in _READ_MODEL_RECORDS:
        if record.table_name == table_name:
            return record
    return None


def required_ui_tables() -> tuple[str, ...]:
    return tuple(record.table_name for record in _READ_MODEL_RECORDS)


def builder_registry() -> dict[str, tuple[str, ...]]:
    registry: dict[str, list[str]] = {}
    for record in _READ_MODEL_RECORDS:
        registry.setdefault(record.builder_name, []).append(record.table_name)
    return {name: tuple(values) for name, values in registry.items()}


def builder_names_in_order() -> tuple[str, ...]:
    return (
        "readiness_builder",
        "opportunity_builder",
        "execution_builder",
        "ops_review_builder",
        "catalog_builder",
    )


def surface_truth_check_specs() -> tuple[SurfaceTruthCheckSpec, ...]:
    return _SURFACE_SPECS
