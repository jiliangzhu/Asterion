from __future__ import annotations

import dataclasses
import hashlib
import json
import os
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any

from asterion_core.monitoring.health_monitor_v1 import (
    collect_chain_tx_health,
    collect_external_execution_health,
    collect_queue_health,
    collect_signer_health,
    collect_submitter_health,
)
from asterion_core.storage.logger import get_logger
from asterion_core.storage.write_queue import default_write_queue_path
from asterion_core.storage.utils import safe_json_dumps
from asterion_core.ui import (
    default_ui_db_replica_path,
    default_ui_lite_db_path,
    default_ui_lite_meta_path,
    default_ui_replica_meta_path,
    load_ui_lite_meta,
    load_ui_replica_meta,
    validate_ui_lite_db,
)


log = get_logger(__name__)

DEFAULT_READINESS_REPORT_JSON_PATH = "data/ui/asterion_readiness_p3.json"
DEFAULT_READINESS_REPORT_MARKDOWN_PATH = "data/ui/asterion_readiness_p3.md"
DEFAULT_P4_READINESS_REPORT_JSON_PATH = "data/ui/asterion_readiness_p4.json"
DEFAULT_P4_READINESS_REPORT_MARKDOWN_PATH = "data/ui/asterion_readiness_p4.md"

_COLD_PATH_TABLES = [
    "weather.weather_market_specs",
    "weather.weather_forecast_runs",
    "weather.weather_forecast_replays",
    "weather.weather_forecast_replay_diffs",
    "weather.weather_watch_only_snapshots",
    "resolution.uma_proposals",
    "resolution.block_watermarks",
    "resolution.watcher_continuity_checks",
    "resolution.settlement_verifications",
    "resolution.redeem_readiness_suggestions",
]

_PAPER_EXECUTION_TABLES = [
    "runtime.strategy_runs",
    "runtime.trade_tickets",
    "runtime.gate_decisions",
    "runtime.journal_events",
    "trading.orders",
    "trading.fills",
    "trading.order_state_transitions",
]

_PORTFOLIO_RECONCILIATION_TABLES = [
    "trading.reservations",
    "trading.inventory_positions",
    "trading.exposure_snapshots",
    "trading.reconciliation_results",
]

_AGENT_SURFACE_TABLES = [
    "agent.invocations",
    "agent.outputs",
    "agent.reviews",
    "agent.evaluations",
]

_REQUIRED_AGENT_JOBS = {
    "weather_rule2spec_review",
    "weather_data_qa_review",
    "weather_resolution_review",
}

_OPERATOR_UI_TABLES = [
    "ui.execution_ticket_summary",
    "ui.execution_run_summary",
    "ui.execution_exception_summary",
]

_DAILY_OPS_UI_TABLES = [
    "ui.paper_run_journal_summary",
    "ui.daily_ops_summary",
    "ui.daily_review_input",
]


class ReadinessTarget(str, Enum):
    P3_PAPER_EXECUTION = "p3_paper_execution"
    P4_LIVE_PREREQUISITES = "p4_live_prerequisites"


@dataclasses.dataclass(frozen=True)
class ReadinessGateResult:
    gate_name: str
    passed: bool
    checks: dict[str, bool]
    violations: list[str]
    warnings: list[str]
    metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "gate_name": self.gate_name,
            "passed": self.passed,
            "checks": dict(self.checks),
            "violations": list(self.violations),
            "warnings": list(self.warnings),
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ReadinessGateResult":
        return cls(
            gate_name=str(data["gate_name"]),
            passed=bool(data["passed"]),
            checks={str(key): bool(value) for key, value in dict(data.get("checks", {})).items()},
            violations=[str(item) for item in list(data.get("violations", []))],
            warnings=[str(item) for item in list(data.get("warnings", []))],
            metadata=dict(data.get("metadata", {})),
        )


@dataclasses.dataclass(frozen=True)
class ReadinessReport:
    target: ReadinessTarget
    generated_at: datetime
    all_passed: bool
    go_decision: str
    decision_reason: str
    data_hash: str
    gate_results: list[ReadinessGateResult]

    def to_dict(self) -> dict[str, Any]:
        return {
            "target": self.target.value,
            "generated_at": _iso_utc(self.generated_at),
            "all_passed": self.all_passed,
            "go_decision": self.go_decision,
            "decision_reason": self.decision_reason,
            "data_hash": self.data_hash,
            "gate_results": [item.to_dict() for item in self.gate_results],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ReadinessReport":
        return cls(
            target=ReadinessTarget(str(data["target"])),
            generated_at=_parse_datetime(str(data["generated_at"])),
            all_passed=bool(data["all_passed"]),
            go_decision=str(data["go_decision"]),
            decision_reason=str(data["decision_reason"]),
            data_hash=str(data["data_hash"]),
            gate_results=[ReadinessGateResult.from_dict(item) for item in list(data.get("gate_results", []))],
        )

    def to_markdown(self) -> str:
        title = {
            ReadinessTarget.P3_PAPER_EXECUTION: "# Asterion P3 Readiness Report",
            ReadinessTarget.P4_LIVE_PREREQUISITES: "# Asterion P4 Live Prereq Readiness Report",
        }[self.target]
        lines = [
            title,
            "",
            f"**Target**: `{self.target.value}`",
            f"**Generated**: {self.generated_at.astimezone(UTC).isoformat()}",
            f"**Decision**: `{self.go_decision}`",
            f"**Reason**: {self.decision_reason}",
            f"**Data Hash**: `{self.data_hash}`",
            "",
            "## Gate Results",
            "",
        ]
        for gate in self.gate_results:
            status = "PASS" if gate.passed else "FAIL"
            lines.append(f"### {gate.gate_name}: {status}")
            lines.append("")
            for key, value in gate.checks.items():
                lines.append(f"- {'OK' if value else 'FAIL'} `{key}`")
            if gate.violations:
                lines.append("")
                lines.append("Violations:")
                for item in gate.violations:
                    lines.append(f"- {item}")
            if gate.warnings:
                lines.append("")
                lines.append("Warnings:")
                for item in gate.warnings:
                    lines.append(f"- {item}")
            lines.append("")
        return "\n".join(lines).strip() + "\n"


@dataclasses.dataclass(frozen=True)
class ReadinessConfig:
    db_path: str = "data/asterion.duckdb"
    ui_replica_db_path: str = dataclasses.field(default_factory=default_ui_db_replica_path)
    ui_replica_meta_path: str = dataclasses.field(default_factory=default_ui_replica_meta_path)
    ui_lite_db_path: str = dataclasses.field(default_factory=default_ui_lite_db_path)
    ui_lite_meta_path: str = dataclasses.field(default_factory=default_ui_lite_meta_path)
    readiness_report_json_path: str = DEFAULT_READINESS_REPORT_JSON_PATH
    readiness_report_markdown_path: str = DEFAULT_READINESS_REPORT_MARKDOWN_PATH
    write_queue_path: str = dataclasses.field(default_factory=default_write_queue_path)
    require_agent_surface: bool = True
    require_ui_lite: bool = True


def evaluate_p3_readiness(config: ReadinessConfig) -> ReadinessReport:
    gate_results = [
        _evaluate_cold_path_determinism(config),
        _evaluate_paper_execution_chain(config),
        _evaluate_portfolio_reconciliation(config),
        _evaluate_agent_review_surface(config),
        _evaluate_operator_surface(config),
        _evaluate_daily_ops_surface(config),
    ]
    all_passed = all(item.passed for item in gate_results)
    go_decision = "GO" if all_passed else "NO-GO"
    failed = [item.gate_name for item in gate_results if not item.passed]
    decision_reason = (
        "all readiness gates passed; ready for P4 planning only"
        if all_passed
        else f"failed gates: {', '.join(failed)}; P3 not ready to close"
    )
    generated_at = datetime.now(UTC)
    hash_payload = {
        "target": ReadinessTarget.P3_PAPER_EXECUTION.value,
        "generated_at": _iso_utc(generated_at),
        "all_passed": all_passed,
        "go_decision": go_decision,
        "decision_reason": decision_reason,
        "gate_results": [item.to_dict() for item in gate_results],
    }
    return ReadinessReport(
        target=ReadinessTarget.P3_PAPER_EXECUTION,
        generated_at=generated_at,
        all_passed=all_passed,
        go_decision=go_decision,
        decision_reason=decision_reason,
        data_hash=_stable_hash(hash_payload),
        gate_results=gate_results,
    )


def evaluate_p4_live_prereq_readiness(config: ReadinessConfig) -> ReadinessReport:
    gate_results = [
        _evaluate_live_prereq_operator_surface(config),
        _evaluate_signer_path_health(config),
        _evaluate_submitter_shadow_path(config),
        _evaluate_wallet_state_and_allowance(config),
        _evaluate_external_execution_alignment(config),
        _evaluate_ops_queue_and_chain_tx(config),
    ]
    all_passed = all(item.passed for item in gate_results)
    go_decision = "GO" if all_passed else "NO-GO"
    failed = [item.gate_name for item in gate_results if not item.passed]
    decision_reason = (
        "all readiness gates passed; ready for controlled live rollout decision"
        if all_passed
        else f"failed gates: {', '.join(failed)}; not ready for controlled live rollout decision"
    )
    generated_at = datetime.now(UTC)
    hash_payload = {
        "target": ReadinessTarget.P4_LIVE_PREREQUISITES.value,
        "generated_at": _iso_utc(generated_at),
        "all_passed": all_passed,
        "go_decision": go_decision,
        "decision_reason": decision_reason,
        "gate_results": [item.to_dict() for item in gate_results],
    }
    return ReadinessReport(
        target=ReadinessTarget.P4_LIVE_PREREQUISITES,
        generated_at=generated_at,
        all_passed=all_passed,
        go_decision=go_decision,
        decision_reason=decision_reason,
        data_hash=_stable_hash(hash_payload),
        gate_results=gate_results,
    )


def write_readiness_report(
    report: ReadinessReport,
    *,
    json_path: str,
    markdown_path: str,
) -> None:
    _write_text_atomic(Path(json_path), json.dumps(report.to_dict(), ensure_ascii=True, sort_keys=True, indent=2) + "\n")
    _write_text_atomic(Path(markdown_path), report.to_markdown())


def _evaluate_cold_path_determinism(config: ReadinessConfig) -> ReadinessGateResult:
    return _evaluate_table_gate(
        gate_name="cold_path_determinism",
        db_path=config.db_path,
        tables=_COLD_PATH_TABLES,
        require_nonempty=True,
        extra_check=_latest_continuity_check,
    )


def _evaluate_paper_execution_chain(config: ReadinessConfig) -> ReadinessGateResult:
    return _evaluate_table_gate(
        gate_name="paper_execution_chain",
        db_path=config.db_path,
        tables=_PAPER_EXECUTION_TABLES,
        require_nonempty=True,
    )


def _evaluate_portfolio_reconciliation(config: ReadinessConfig) -> ReadinessGateResult:
    return _evaluate_table_gate(
        gate_name="portfolio_reconciliation",
        db_path=config.db_path,
        tables=_PORTFOLIO_RECONCILIATION_TABLES,
        require_nonempty=True,
        extra_check=_reconciliation_mismatch_check,
    )


def _evaluate_agent_review_surface(config: ReadinessConfig) -> ReadinessGateResult:
    from dagster_asterion.job_map import build_weather_cold_path_job_map

    if not config.require_agent_surface:
        return ReadinessGateResult(
            gate_name="agent_review_surface",
            passed=True,
            checks={"agent_surface.required": False},
            violations=[],
            warnings=["agent review surface disabled by config"],
            metadata={},
        )
    result = _evaluate_table_gate(
        gate_name="agent_review_surface",
        db_path=config.db_path,
        tables=_AGENT_SURFACE_TABLES,
        require_nonempty=False,
    )
    job_map = build_weather_cold_path_job_map()
    checks = dict(result.checks)
    violations = list(result.violations)
    warnings = list(result.warnings)
    metadata = dict(result.metadata)
    for job_name in sorted(_REQUIRED_AGENT_JOBS):
        present = job_name in job_map and job_map[job_name].mode == "manual"
        checks[f"job:{job_name}.manual"] = present
        if not present:
            violations.append(f"missing manual agent review job: {job_name}")
    metadata["required_agent_jobs"] = sorted(_REQUIRED_AGENT_JOBS)
    return ReadinessGateResult(
        gate_name="agent_review_surface",
        passed=not violations,
        checks=checks,
        violations=violations,
        warnings=warnings,
        metadata=metadata,
    )


def _evaluate_operator_surface(config: ReadinessConfig) -> ReadinessGateResult:
    checks: dict[str, bool] = {}
    violations: list[str] = []
    warnings: list[str] = []
    metadata: dict[str, Any] = {}

    replica_meta = load_ui_replica_meta(config.ui_replica_meta_path)
    replica_db_exists = Path(config.ui_replica_db_path).exists()
    checks["ui_replica.db_exists"] = replica_db_exists
    if not replica_db_exists:
        violations.append(f"missing UI replica DB: {config.ui_replica_db_path}")
    replica_ok = bool(replica_meta) and replica_meta.get("last_success_ts_ms") is not None and not replica_meta.get("last_error")
    checks["ui_replica.last_success"] = replica_ok
    if not replica_ok:
        violations.append("UI replica meta does not show a successful refresh")
    metadata["ui_replica_meta"] = replica_meta or {}

    lite_meta = load_ui_lite_meta(config.ui_lite_meta_path)
    lite_db_exists = Path(config.ui_lite_db_path).exists()
    checks["ui_lite.db_exists"] = lite_db_exists
    if config.require_ui_lite and not lite_db_exists:
        violations.append(f"missing UI lite DB: {config.ui_lite_db_path}")
    lite_meta_ok = bool(lite_meta) and lite_meta.get("last_success_ts_ms") is not None and not lite_meta.get("last_error")
    checks["ui_lite.last_success"] = lite_meta_ok
    if config.require_ui_lite and not lite_meta_ok:
        violations.append("UI lite meta does not show a successful build")
    metadata["ui_lite_meta"] = lite_meta or {}

    lite_valid = False
    lite_validation: dict[str, int] = {}
    if lite_db_exists:
        try:
            lite_validation = validate_ui_lite_db(config.ui_lite_db_path)
            lite_valid = True
        except Exception as exc:  # noqa: BLE001
            if config.require_ui_lite:
                violations.append(f"UI lite validation failed: {exc}")
            else:
                warnings.append(f"UI lite validation failed: {exc}")
    checks["ui_lite.validated"] = lite_valid
    metadata["ui_lite_validation"] = lite_validation
    if lite_valid:
        ui_gate = _evaluate_ui_table_gate(
            config.ui_lite_db_path,
            required_tables=_OPERATOR_UI_TABLES,
            nonempty_tables=["ui.execution_ticket_summary", "ui.execution_run_summary"],
        )
        checks.update(ui_gate["checks"])
        violations.extend(ui_gate["violations"])
        warnings.extend(ui_gate["warnings"])
        metadata.update(ui_gate["metadata"])
    return ReadinessGateResult(
        gate_name="operator_surface",
        passed=not violations,
        checks=checks,
        violations=violations,
        warnings=warnings,
        metadata=metadata,
    )


def _evaluate_daily_ops_surface(config: ReadinessConfig) -> ReadinessGateResult:
    checks: dict[str, bool] = {}
    violations: list[str] = []
    warnings: list[str] = []
    metadata: dict[str, Any] = {}

    lite_db_exists = Path(config.ui_lite_db_path).exists()
    checks["ui_lite.db_exists"] = lite_db_exists
    if not lite_db_exists:
        violations.append(f"missing UI lite DB: {config.ui_lite_db_path}")
        return ReadinessGateResult(
            gate_name="daily_ops_surface",
            passed=False,
            checks=checks,
            violations=violations,
            warnings=warnings,
            metadata=metadata,
        )

    ui_gate = _evaluate_ui_table_gate(
        config.ui_lite_db_path,
        required_tables=_DAILY_OPS_UI_TABLES,
        nonempty_tables=list(_DAILY_OPS_UI_TABLES),
    )
    checks.update(ui_gate["checks"])
    violations.extend(ui_gate["violations"])
    warnings.extend(ui_gate["warnings"])
    metadata.update(ui_gate["metadata"])
    return ReadinessGateResult(
        gate_name="daily_ops_surface",
        passed=not violations,
        checks=checks,
        violations=violations,
        warnings=warnings,
        metadata=metadata,
    )


def _evaluate_live_prereq_operator_surface(config: ReadinessConfig) -> ReadinessGateResult:
    checks: dict[str, bool] = {}
    violations: list[str] = []
    metadata: dict[str, Any] = {}
    if not Path(config.ui_lite_db_path).exists():
        return ReadinessGateResult(
            gate_name="live_prereq_operator_surface",
            passed=False,
            checks={"ui_lite.db_exists": False},
            violations=[f"missing UI lite DB: {config.ui_lite_db_path}"],
            warnings=[],
            metadata=metadata,
        )
    ui_gate = _evaluate_ui_table_gate(
        config.ui_lite_db_path,
        required_tables=[
            "ui.live_prereq_wallet_summary",
            "ui.live_prereq_execution_summary",
            "ui.execution_exception_summary",
        ],
        nonempty_tables=[
            "ui.live_prereq_wallet_summary",
            "ui.live_prereq_execution_summary",
        ],
    )
    checks.update(ui_gate["checks"])
    violations.extend(ui_gate["violations"])
    metadata.update(ui_gate["metadata"])
    return ReadinessGateResult(
        gate_name="live_prereq_operator_surface",
        passed=not violations,
        checks=checks,
        violations=violations,
        warnings=list(ui_gate["warnings"]),
        metadata=metadata,
    )


def _evaluate_signer_path_health(config: ReadinessConfig) -> ReadinessGateResult:
    checks: dict[str, bool] = {}
    violations: list[str] = []
    warnings: list[str] = []
    metadata: dict[str, Any] = {}
    con = _connect_duckdb(config.db_path, read_only=True)
    try:
        signer_health = collect_signer_health(con)
    finally:
        con.close()
    metadata["signer_health"] = dataclasses.asdict(signer_health)
    request_count_ok = signer_health.request_count > 0
    rejected_count_ok = signer_health.rejected_count == 0
    checks["signer.request_count_positive"] = request_count_ok
    checks["signer.rejected_count_zero"] = rejected_count_ok
    if not request_count_ok:
        violations.append("signer health has no request activity")
    if not rejected_count_ok:
        violations.append("signer health has rejected requests")
    ui_rows = _query_ui_rows(
        config.ui_lite_db_path,
        """
        SELECT wallet_id
        FROM ui.live_prereq_wallet_summary
        WHERE can_trade
          AND latest_signer_status = 'rejected'
        """,
    )
    wallet_status_ok = len(ui_rows) == 0
    checks["wallet_summary.no_signer_rejects_for_can_trade"] = wallet_status_ok
    metadata["signer_rejected_wallet_ids"] = [str(row[0]) for row in ui_rows]
    if not wallet_status_ok:
        violations.append("signer reject surfaced in live prereq wallet summary")
    return ReadinessGateResult(
        gate_name="signer_path_health",
        passed=not violations,
        checks=checks,
        violations=violations,
        warnings=warnings,
        metadata=metadata,
    )


def _evaluate_submitter_shadow_path(config: ReadinessConfig) -> ReadinessGateResult:
    checks: dict[str, bool] = {}
    violations: list[str] = []
    warnings: list[str] = []
    metadata: dict[str, Any] = {}
    con = _connect_duckdb(config.db_path, read_only=True)
    try:
        submitter_health = collect_submitter_health(con)
    finally:
        con.close()
    metadata["submitter_health"] = dataclasses.asdict(submitter_health)
    sign_only_ok = submitter_health.sign_only_signed_count > 0
    submit_activity_ok = (submitter_health.submit_preview_count + submitter_health.submit_accepted_count + submitter_health.submit_rejected_count) > 0
    checks["submitter.sign_only_signed_present"] = sign_only_ok
    checks["submitter.submit_activity_present"] = submit_activity_ok
    if not sign_only_ok:
        violations.append("missing sign-only signed attempts")
    if not submit_activity_ok:
        violations.append("missing submitter activity")
    ui_rows = _query_ui_rows(
        config.ui_lite_db_path,
        """
        SELECT ticket_id, live_prereq_execution_status
        FROM ui.live_prereq_execution_summary
        WHERE live_prereq_execution_status IN ('sign_rejected', 'submit_rejected')
        """,
    )
    status_ok = len(ui_rows) == 0
    checks["execution_summary.no_sign_or_submit_rejects"] = status_ok
    metadata["submitter_rejected_ticket_ids"] = [str(row[0]) for row in ui_rows]
    if not status_ok:
        violations.append("submitter shadow path has sign_rejected or submit_rejected executions")
    return ReadinessGateResult(
        gate_name="submitter_shadow_path",
        passed=not violations,
        checks=checks,
        violations=violations,
        warnings=warnings,
        metadata=metadata,
    )


def _evaluate_wallet_state_and_allowance(config: ReadinessConfig) -> ReadinessGateResult:
    checks: dict[str, bool] = {}
    violations: list[str] = []
    warnings: list[str] = []
    metadata: dict[str, Any] = {}
    con = _connect_duckdb(config.db_path, read_only=True)
    try:
        observation_count = _table_count(con, "runtime.external_balance_observations")
    finally:
        con.close()
    checks["runtime.external_balance_observations.nonempty"] = observation_count > 0
    metadata["external_balance_observation_count"] = observation_count
    if observation_count <= 0:
        violations.append("missing external balance observations")
    ui_rows = _query_ui_rows(
        config.ui_lite_db_path,
        """
        SELECT wallet_id, wallet_readiness_status
        FROM ui.live_prereq_wallet_summary
        WHERE can_trade
          AND wallet_readiness_status <> 'ready'
        """,
    )
    ready_ok = len(ui_rows) == 0
    checks["wallet_summary.can_trade_wallets_ready"] = ready_ok
    metadata["wallet_readiness_failures"] = [
        {"wallet_id": str(row[0]), "wallet_readiness_status": str(row[1])}
        for row in ui_rows
    ]
    if not ready_ok:
        violations.append("live prereq wallet summary shows non-ready can_trade wallets")
    return ReadinessGateResult(
        gate_name="wallet_state_and_allowance",
        passed=not violations,
        checks=checks,
        violations=violations,
        warnings=warnings,
        metadata=metadata,
    )


def _evaluate_external_execution_alignment(config: ReadinessConfig) -> ReadinessGateResult:
    checks: dict[str, bool] = {}
    violations: list[str] = []
    warnings: list[str] = []
    metadata: dict[str, Any] = {}
    con = _connect_duckdb(config.db_path, read_only=True)
    try:
        external_health = collect_external_execution_health(con)
    finally:
        con.close()
    metadata["external_execution_health"] = dataclasses.asdict(external_health)
    mismatch_ok = external_health.external_reconciliation_mismatch_count == 0
    unverified_ok = external_health.external_reconciliation_unverified_count == 0
    checks["external_execution.mismatch_count_zero"] = mismatch_ok
    checks["external_execution.unverified_count_zero"] = unverified_ok
    if not mismatch_ok:
        violations.append("external reconciliation mismatches present")
    if not unverified_ok:
        violations.append("external reconciliation unverified rows present")
    ui_rows = _query_ui_rows(
        config.ui_lite_db_path,
        """
        SELECT ticket_id, live_prereq_execution_status
        FROM ui.live_prereq_execution_summary
        WHERE live_prereq_execution_status IN ('external_unverified', 'external_mismatch')
        """,
    )
    status_ok = len(ui_rows) == 0
    checks["execution_summary.no_external_mismatch_or_unverified"] = status_ok
    metadata["external_execution_failures"] = [
        {"ticket_id": str(row[0]), "live_prereq_execution_status": str(row[1])}
        for row in ui_rows
    ]
    if not status_ok:
        violations.append("live prereq execution summary shows external mismatch or unverified status")
    return ReadinessGateResult(
        gate_name="external_execution_alignment",
        passed=not violations,
        checks=checks,
        violations=violations,
        warnings=warnings,
        metadata=metadata,
    )


def _evaluate_ops_queue_and_chain_tx(config: ReadinessConfig) -> ReadinessGateResult:
    checks: dict[str, bool] = {}
    violations: list[str] = []
    warnings: list[str] = []
    metadata: dict[str, Any] = {}
    con = _connect_duckdb(config.db_path, read_only=True)
    try:
        chain_tx_health = collect_chain_tx_health(con)
    finally:
        con.close()
    queue_health = collect_queue_health(config.write_queue_path)
    metadata["chain_tx_health"] = dataclasses.asdict(chain_tx_health)
    metadata["queue_health"] = dataclasses.asdict(queue_health)
    approve_present = chain_tx_health.approve_attempt_count > 0
    approve_rejected_ok = chain_tx_health.approve_rejected_count == 0
    latest_approve_ok = chain_tx_health.latest_approve_status != "rejected"
    queue_ok = queue_health.dead_tasks_1h == 0
    checks["chain_tx.approve_attempt_present"] = approve_present
    checks["chain_tx.approve_rejected_count_zero"] = approve_rejected_ok
    checks["chain_tx.latest_approve_not_rejected"] = latest_approve_ok
    checks["queue.dead_tasks_1h_zero"] = queue_ok
    if not approve_present:
        violations.append("missing approve_usdc activity")
    if not approve_rejected_ok:
        violations.append("approve_usdc attempts include rejected rows")
    if not latest_approve_ok:
        violations.append("latest approve_usdc status is rejected")
    if not queue_ok:
        violations.append("writer queue has dead tasks in the last hour")
    return ReadinessGateResult(
        gate_name="ops_queue_and_chain_tx",
        passed=not violations,
        checks=checks,
        violations=violations,
        warnings=warnings,
        metadata=metadata,
    )


def _evaluate_table_gate(
    *,
    gate_name: str,
    db_path: str,
    tables: list[str],
    require_nonempty: bool,
    extra_check=None,
) -> ReadinessGateResult:
    checks: dict[str, bool] = {}
    violations: list[str] = []
    warnings: list[str] = []
    metadata: dict[str, Any] = {"table_row_counts": {}}

    if not Path(db_path).exists():
        return ReadinessGateResult(
            gate_name=gate_name,
            passed=False,
            checks={"db.exists": False},
            violations=[f"missing database: {db_path}"],
            warnings=[],
            metadata=metadata,
        )

    con = _connect_duckdb(db_path, read_only=True)
    try:
        for table in tables:
            exists = _table_exists(con, table)
            checks[f"{table}.exists"] = exists
            if not exists:
                violations.append(f"missing table: {table}")
                continue
            row_count = _table_count(con, table)
            metadata["table_row_counts"][table] = row_count
            if require_nonempty:
                nonempty = row_count > 0
                checks[f"{table}.nonempty"] = nonempty
                if not nonempty:
                    violations.append(f"table is empty: {table}")
        if extra_check is not None:
            extra_result = extra_check(con)
            checks.update(extra_result["checks"])
            violations.extend(extra_result["violations"])
            warnings.extend(extra_result["warnings"])
            metadata.update(extra_result["metadata"])
    finally:
        con.close()
    return ReadinessGateResult(
        gate_name=gate_name,
        passed=not violations,
        checks=checks,
        violations=violations,
        warnings=warnings,
        metadata=metadata,
    )


def _latest_continuity_check(con) -> dict[str, Any]:
    checks: dict[str, bool] = {}
    violations: list[str] = []
    warnings: list[str] = []
    metadata: dict[str, Any] = {}
    row = con.execute(
        """
        SELECT check_id, status, from_block, to_block, created_at
        FROM resolution.watcher_continuity_checks
        ORDER BY created_at DESC, to_block DESC, check_id DESC
        LIMIT 1
        """
    ).fetchone()
    latest_ok = row is not None
    checks["watcher_continuity.latest_present"] = latest_ok
    if row is None:
        violations.append("missing watcher continuity check rows")
        return {"checks": checks, "violations": violations, "warnings": warnings, "metadata": metadata}
    status = str(row[1])
    metadata["latest_continuity"] = {
        "check_id": str(row[0]),
        "status": status,
        "from_block": int(row[2]),
        "to_block": int(row[3]),
        "created_at": str(row[4]),
    }
    status_ok = status != "RPC_INCOMPLETE"
    checks["watcher_continuity.latest_not_rpc_incomplete"] = status_ok
    if not status_ok:
        violations.append("latest continuity status is RPC_INCOMPLETE")
    return {"checks": checks, "violations": violations, "warnings": warnings, "metadata": metadata}


def _reconciliation_mismatch_check(con) -> dict[str, Any]:
    checks: dict[str, bool] = {}
    violations: list[str] = []
    warnings: list[str] = []
    metadata: dict[str, Any] = {}
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM trading.reconciliation_results
        WHERE status <> 'ok'
        """
    ).fetchone()
    mismatch_count = int(row[0]) if row is not None else 0
    checks["trading.reconciliation_results.no_mismatches"] = mismatch_count == 0
    metadata["reconciliation_mismatch_count"] = mismatch_count
    if mismatch_count > 0:
        violations.append(f"reconciliation mismatches present: {mismatch_count}")
    return {"checks": checks, "violations": violations, "warnings": warnings, "metadata": metadata}


def _evaluate_ui_table_gate(
    db_path: str,
    *,
    required_tables: list[str],
    nonempty_tables: list[str],
) -> dict[str, Any]:
    checks: dict[str, bool] = {}
    violations: list[str] = []
    warnings: list[str] = []
    metadata: dict[str, Any] = {"ui_table_row_counts": {}}
    con = _connect_duckdb(db_path, read_only=True)
    try:
        for table in required_tables:
            exists = _table_exists(con, table)
            checks[f"{table}.exists"] = exists
            if not exists:
                violations.append(f"missing UI table: {table}")
                continue
            row_count = _table_count(con, table)
            metadata["ui_table_row_counts"][table] = row_count
            if table in nonempty_tables:
                nonempty = row_count > 0
                checks[f"{table}.nonempty"] = nonempty
                if not nonempty:
                    violations.append(f"UI table is empty: {table}")
    finally:
        con.close()
    return {"checks": checks, "violations": violations, "warnings": warnings, "metadata": metadata}


def _query_ui_rows(db_path: str, query: str, params: list[Any] | None = None) -> list[tuple[Any, ...]]:
    if not Path(db_path).exists():
        return []
    con = _connect_duckdb(db_path, read_only=True)
    try:
        return list(con.execute(query, params or []).fetchall())
    except Exception:  # noqa: BLE001
        return []
    finally:
        con.close()


def _connect_duckdb(db_path: str, *, read_only: bool):
    try:
        import duckdb  # type: ignore
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError("Missing dependency: duckdb. Install with: pip install duckdb") from exc
    return duckdb.connect(db_path, read_only=read_only)


def _table_exists(con, table_name: str) -> bool:
    schema, table = table_name.split(".", 1)
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema, table],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _table_count(con, table_name: str) -> int:
    row = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return int(row[0]) if row is not None else 0


def _stable_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(safe_json_dumps(payload).encode("utf-8")).hexdigest()


def _iso_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    else:
        value = value.astimezone(UTC)
    return value.isoformat()


def _parse_datetime(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _write_text_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    os.replace(tmp, path)


__all__ = [
    "DEFAULT_READINESS_REPORT_JSON_PATH",
    "DEFAULT_READINESS_REPORT_MARKDOWN_PATH",
    "DEFAULT_P4_READINESS_REPORT_JSON_PATH",
    "DEFAULT_P4_READINESS_REPORT_MARKDOWN_PATH",
    "ReadinessConfig",
    "ReadinessGateResult",
    "ReadinessReport",
    "ReadinessTarget",
    "evaluate_p3_readiness",
    "evaluate_p4_live_prereq_readiness",
    "write_readiness_report",
]
