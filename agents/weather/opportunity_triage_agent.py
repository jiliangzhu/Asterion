from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from agents.common import (
    AgentClient,
    AgentExecutionArtifacts,
    AgentFinding,
    AgentInvocationStatus,
    AgentType,
    AgentVerdict,
    build_agent_evaluation_record,
    build_agent_invocation_record,
    build_agent_output_record,
    build_agent_review_record,
)

try:
    import duckdb
except ModuleNotFoundError:  # pragma: no cover - exercised in runtime
    duckdb = None


AGENT_VERSION = "opportunity_triage_agent_v1"
PROMPT_VERSION = "opportunity_triage_prompt_v2"
SUBJECT_TYPE = "weather_market"


@dataclass(frozen=True)
class OpportunityTriageAgentRequest:
    market_id: str
    question: str
    location_name: str
    best_side: str
    ranking_score: float
    recommended_size: float
    allocation_status: str
    operator_bucket: str
    queue_reason_codes: list[str]
    execution_intelligence_score: float
    top_of_book_stability: float
    spread_regime: str
    expected_capture_regime: str
    expected_slippage_regime: str
    microstructure_reason_codes: list[str]
    calibration_gate_status: str
    capital_policy_id: str | None
    surface_delivery_status: str
    surface_fallback_origin: str | None
    source_provenance: dict[str, Any]
    source_freshness: str | None
    is_degraded_source: bool
    why_ranked_json: dict[str, Any]
    pricing_context_json: dict[str, Any]

    def __post_init__(self) -> None:
        for name in ("market_id", "question", "best_side", "allocation_status", "operator_bucket", "calibration_gate_status", "surface_delivery_status"):
            if not str(getattr(self, name) or "").strip():
                raise ValueError(f"{name} is required")
        for name in (
            "ranking_score",
            "recommended_size",
            "execution_intelligence_score",
            "top_of_book_stability",
        ):
            float(getattr(self, name))
        for name in ("queue_reason_codes", "microstructure_reason_codes"):
            if not isinstance(getattr(self, name), list):
                raise ValueError(f"{name} must be a list")
        if not isinstance(self.source_provenance, dict):
            raise ValueError("source_provenance must be a dictionary")
        if not isinstance(self.why_ranked_json, dict):
            raise ValueError("why_ranked_json must be a dictionary")
        if not isinstance(self.pricing_context_json, dict):
            raise ValueError("pricing_context_json must be a dictionary")


@dataclass(frozen=True)
class OpportunityTriageAgentOutput:
    triage_status: str
    priority_band: str
    triage_reason_codes: list[str]
    execution_risk_flags: list[str]
    recommended_operator_action: str
    confidence_band: str
    supporting_evidence_refs: list[str]
    verdict: AgentVerdict
    confidence: float
    summary: str
    human_review_required: bool

    def __post_init__(self) -> None:
        for name in ("triage_status", "priority_band", "recommended_operator_action", "confidence_band", "summary"):
            if not str(getattr(self, name) or "").strip():
                raise ValueError(f"{name} is required")
        for name in ("triage_reason_codes", "execution_risk_flags", "supporting_evidence_refs"):
            if not isinstance(getattr(self, name), list):
                raise ValueError(f"{name} must be a list")
        if not (0.0 <= float(self.confidence) <= 1.0):
            raise ValueError("confidence must be between 0 and 1")


def load_opportunity_triage_agent_requests(
    replica_db_path: Path | str,
    *,
    market_ids: list[str] | None = None,
    limit: int | None = None,
    primary_source: str = "ui_replica",
) -> list[OpportunityTriageAgentRequest]:
    if duckdb is None:
        return []
    path = Path(replica_db_path)
    if not path.exists():
        return []
    con = duckdb.connect(str(path), read_only=True)
    try:
        required_tables = {
            "ui.market_opportunity_summary",
            "ui.action_queue_summary",
            "ui.market_microstructure_summary",
        }
        existing_tables = {
            row[0]
            for row in con.execute(
                """
                SELECT table_schema || '.' || table_name
                FROM information_schema.tables
                WHERE table_schema = 'ui'
                """
            ).fetchall()
        }
        if not required_tables.issubset(existing_tables):
            return []
        opp_columns = _table_columns(con, "ui", "market_opportunity_summary")
        queue_columns = _table_columns(con, "ui", "action_queue_summary")
        micro_columns = _table_columns(con, "ui", "market_microstructure_summary")

        def _qualified(schema_alias: str, column: str, available: set[str]) -> str | None:
            if column in available:
                return f"{schema_alias}.{column}"
            return None

        def _coalesce_expr(exprs: list[str | None], fallback_sql: str) -> str:
            usable = [expr for expr in exprs if expr]
            if not usable:
                return fallback_sql
            return f"COALESCE({', '.join(usable + [fallback_sql])})"

        def _coalesce_text_expr(exprs: list[str | None], fallback_sql: str) -> str:
            usable = [f"CAST({expr} AS VARCHAR)" for expr in exprs if expr]
            if not usable:
                return fallback_sql
            return f"COALESCE({', '.join(usable + [fallback_sql])})"

        def _coalesce_double_expr(exprs: list[str | None], fallback_sql: str) -> str:
            usable = [f"CAST({expr} AS DOUBLE)" for expr in exprs if expr]
            if not usable:
                return fallback_sql
            return f"COALESCE({', '.join(usable + [fallback_sql])})"

        sql = """
            WITH latest_queue AS (
                SELECT *
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY market_id
                            ORDER BY updated_at DESC, market_id DESC
                        ) AS rn
                    FROM ui.action_queue_summary
                )
                WHERE rn = 1
            ),
            latest_micro AS (
                SELECT *
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY market_id
                            ORDER BY materialized_at DESC, summary_id DESC
                        ) AS rn
                    FROM ui.market_microstructure_summary
                )
                WHERE rn = 1
            )
            SELECT
                opp.market_id,
                opp.question,
                COALESCE(opp.location_name, '') AS location_name,
                COALESCE(opp.best_side, '') AS best_side,
                COALESCE(opp.ranking_score, 0.0) AS ranking_score,
                {recommended_size_expr} AS recommended_size,
                {allocation_status_expr} AS allocation_status,
                {operator_bucket_expr} AS operator_bucket,
                {queue_reason_codes_expr} AS queue_reason_codes_json,
                {execution_intelligence_score_expr} AS execution_intelligence_score,
                {top_of_book_stability_expr} AS top_of_book_stability,
                {spread_regime_expr} AS spread_regime,
                {expected_capture_regime_expr} AS expected_capture_regime,
                {expected_slippage_regime_expr} AS expected_slippage_regime,
                {microstructure_reason_codes_expr} AS microstructure_reason_codes_json,
                {calibration_gate_status_expr} AS calibration_gate_status,
                {capital_policy_id_expr} AS capital_policy_id,
                {surface_delivery_status_expr} AS surface_delivery_status,
                {surface_fallback_origin_expr} AS surface_fallback_origin,
                COALESCE(opp.source_freshness_status, 'unknown') AS source_freshness_status,
                COALESCE(opp.source_truth_status, 'unknown') AS source_truth_status,
                COALESCE(opp.is_degraded_source, FALSE) AS is_degraded_source,
                COALESCE(opp.why_ranked_json, '{{}}') AS why_ranked_json,
                COALESCE(opp.pricing_context_json, '{{}}') AS pricing_context_json,
                COALESCE(opp.source_badge, 'ui_lite') AS source_badge,
                {updated_at_expr} AS updated_at
            FROM ui.market_opportunity_summary opp
            LEFT JOIN latest_queue queue ON queue.market_id = opp.market_id
            LEFT JOIN latest_micro micro ON micro.market_id = opp.market_id
            WHERE COALESCE(CAST(opp.best_side AS VARCHAR), '') <> ''
        """.format(
            recommended_size_expr=_coalesce_double_expr(
                [
                    _qualified("queue", "recommended_size", queue_columns),
                    _qualified("opp", "recommended_size", opp_columns),
                ],
                "0.0",
            ),
            allocation_status_expr=_coalesce_text_expr(
                [
                    _qualified("queue", "allocation_status", queue_columns),
                    _qualified("opp", "allocation_status", opp_columns),
                ],
                "'unknown'",
            ),
            operator_bucket_expr=_coalesce_text_expr(
                [_qualified("queue", "operator_bucket", queue_columns)],
                "'review_required'",
            ),
            queue_reason_codes_expr=_coalesce_text_expr(
                [_qualified("queue", "queue_reason_codes_json", queue_columns)],
                "'[]'",
            ),
            execution_intelligence_score_expr=_coalesce_double_expr(
                [
                    _qualified("micro", "execution_intelligence_score", micro_columns),
                    _qualified("opp", "execution_intelligence_score", opp_columns),
                ],
                "0.0",
            ),
            top_of_book_stability_expr=_coalesce_double_expr(
                [
                    _qualified("micro", "top_of_book_stability", micro_columns),
                    _qualified("opp", "top_of_book_stability", opp_columns),
                ],
                "0.0",
            ),
            spread_regime_expr=_coalesce_text_expr(
                [
                    _qualified("micro", "spread_regime", micro_columns),
                    _qualified("opp", "spread_regime", opp_columns),
                ],
                "'unknown'",
            ),
            expected_capture_regime_expr=_coalesce_text_expr(
                [
                    _qualified("micro", "expected_capture_regime", micro_columns),
                    _qualified("opp", "expected_capture_regime", opp_columns),
                ],
                "'low'",
            ),
            expected_slippage_regime_expr=_coalesce_text_expr(
                [
                    _qualified("micro", "expected_slippage_regime", micro_columns),
                    _qualified("opp", "expected_slippage_regime", opp_columns),
                ],
                "'high'",
            ),
            microstructure_reason_codes_expr=_coalesce_text_expr(
                [_qualified("micro", "reason_codes_json", micro_columns)],
                "'[]'",
            ),
            calibration_gate_status_expr=_coalesce_text_expr(
                [
                    _qualified("queue", "calibration_gate_status", queue_columns),
                    _qualified("opp", "calibration_gate_status", opp_columns),
                ],
                "'clear'",
            ),
            capital_policy_id_expr=_coalesce_text_expr(
                [
                    _qualified("queue", "capital_policy_id", queue_columns),
                    _qualified("opp", "capital_policy_id", opp_columns),
                ],
                "NULL",
            ),
            surface_delivery_status_expr=_coalesce_text_expr(
                [
                    _qualified("queue", "surface_delivery_status", queue_columns),
                    _qualified("opp", "surface_delivery_status", opp_columns),
                ],
                "'ok'",
            ),
            surface_fallback_origin_expr=_coalesce_text_expr(
                [
                    _qualified("queue", "surface_fallback_origin", queue_columns),
                    _qualified("opp", "surface_fallback_origin", opp_columns),
                ],
                "NULL",
            ),
            updated_at_expr=_coalesce_text_expr(
                [
                    _qualified("queue", "updated_at", queue_columns),
                    _qualified("opp", "signal_created_at", opp_columns),
                    _qualified("opp", "agent_updated_at", opp_columns),
                    _qualified("opp", "live_updated_at", opp_columns),
                    _qualified("opp", "surface_last_refresh_ts", opp_columns),
                    _qualified("micro", "materialized_at", micro_columns),
                ],
                "NULL",
            ),
        )
        params: list[Any] = []
        if market_ids:
            placeholders = ",".join(["?"] * len(market_ids))
            sql += f" AND opp.market_id IN ({placeholders})"
            params.extend(market_ids)
        sql += " ORDER BY COALESCE(queue.queue_priority, 999) ASC, opp.ranking_score DESC, updated_at DESC, opp.market_id DESC"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(int(limit))
        rows = con.execute(sql, params).fetchall()
        requests: list[OpportunityTriageAgentRequest] = []
        for row in rows:
            source_provenance = {
                "primary_source": primary_source,
                "market_summary_table": "ui.market_opportunity_summary",
                "action_queue_table": "ui.action_queue_summary",
                "microstructure_table": "ui.market_microstructure_summary",
                "source_badge": str(row[24]),
                "source_truth_status": str(row[20]),
                "subject_type": SUBJECT_TYPE,
            }
            try:
                requests.append(
                    OpportunityTriageAgentRequest(
                        market_id=str(row[0]),
                        question=str(row[1]),
                        location_name=str(row[2]),
                        best_side=str(row[3]),
                        ranking_score=float(row[4] or 0.0),
                        recommended_size=float(row[5] or 0.0),
                        allocation_status=str(row[6]),
                        operator_bucket=str(row[7]),
                        queue_reason_codes=_json_list(row[8]),
                        execution_intelligence_score=float(row[9] or 0.0),
                        top_of_book_stability=float(row[10] or 0.0),
                        spread_regime=str(row[11]),
                        expected_capture_regime=str(row[12]),
                        expected_slippage_regime=str(row[13]),
                        microstructure_reason_codes=_json_list(row[14]),
                        calibration_gate_status=str(row[15]),
                        capital_policy_id=str(row[16]) if row[16] is not None else None,
                        surface_delivery_status=str(row[17]),
                        surface_fallback_origin=str(row[18]) if row[18] is not None else None,
                        source_freshness=str(row[19]),
                        source_provenance=source_provenance,
                        is_degraded_source=bool(row[21]),
                        why_ranked_json=_json_dict(row[22]),
                        pricing_context_json=_json_dict(row[23]),
                    )
                )
            except ValueError:
                continue
        return requests
    finally:
        con.close()


def run_opportunity_triage_agent_review(
    client: AgentClient,
    request: OpportunityTriageAgentRequest,
    *,
    force_rerun: bool = False,
    now: datetime | None = None,
) -> AgentExecutionArtifacts:
    started_at = _normalize_ts(now) or datetime.now(UTC)
    payload = build_opportunity_triage_input_payload(request)
    model_payload = build_opportunity_triage_model_payload(request)
    try:
        response = client.invoke(
            system_prompt=_system_prompt(),
            user_prompt=_user_prompt(),
            input_payload_json=model_payload,
            metadata={"agent_type": AgentType.OPPORTUNITY_TRIAGE.value, "subject_type": SUBJECT_TYPE, "subject_id": request.market_id},
        )
        output = parse_opportunity_triage_output(response.structured_output_json, request=request)
        ended_at = datetime.now(UTC)
        invocation = build_agent_invocation_record(
            agent_type=AgentType.OPPORTUNITY_TRIAGE,
            agent_version=AGENT_VERSION,
            prompt_version=PROMPT_VERSION,
            subject_type=SUBJECT_TYPE,
            subject_id=request.market_id,
            input_payload_json=payload,
            model_provider=response.model_provider,
            model_name=response.model_name,
            status=AgentInvocationStatus.SUCCESS,
            started_at=started_at,
            ended_at=ended_at,
            latency_ms=_latency_ms(started_at, ended_at),
            force_rerun=force_rerun,
        )
        output_record = build_agent_output_record(
            invocation_id=invocation.invocation_id,
            verdict=output.verdict,
            confidence=output.confidence,
            summary=output.summary,
            findings=_findings_from_output(request.market_id, output),
            structured_output_json=opportunity_triage_output_to_json(output),
            human_review_required=output.human_review_required,
            created_at=ended_at,
        )
        review_record = build_agent_review_record(
            invocation_id=invocation.invocation_id,
            human_review_required=output.human_review_required,
            review_payload_json={
                "priority_band": output.priority_band,
                "recommended_operator_action": output.recommended_operator_action,
                "triage_status": output.triage_status,
                "subject_id": request.market_id,
            },
            reviewed_at=ended_at,
        )
        evaluation_record = build_agent_evaluation_record(
            invocation_id=invocation.invocation_id,
            confidence=output.confidence,
            human_review_required=output.human_review_required,
            verification_method="operator_outcome_proxy",
            is_verified=False,
            notes="initial triage overlay proxy; replay acceptance still required before default-on advisory",
            score_json=_evaluation_score_json(request, output),
            created_at=ended_at,
        )
        return AgentExecutionArtifacts(
            invocation=invocation,
            output=output_record,
            review=review_record,
            evaluation=evaluation_record,
        )
    except TimeoutError as exc:
        return _failed_artifacts(request=request, payload=payload, started_at=started_at, status=AgentInvocationStatus.TIMEOUT, error_message=str(exc), force_rerun=force_rerun)
    except ValueError as exc:
        return _failed_artifacts(request=request, payload=payload, started_at=started_at, status=AgentInvocationStatus.PARSE_ERROR, error_message=str(exc), force_rerun=force_rerun)
    except Exception as exc:  # noqa: BLE001
        error_message = str(exc)
        if _should_use_deterministic_fallback(error_message):
            return _deterministic_fallback_artifacts(
                request=request,
                payload=payload,
                started_at=started_at,
                error_message=error_message,
                force_rerun=force_rerun,
            )
        return _failed_artifacts(request=request, payload=payload, started_at=started_at, status=AgentInvocationStatus.FAILURE, error_message=str(exc), force_rerun=force_rerun)


def build_failed_opportunity_triage_artifacts(
    request: OpportunityTriageAgentRequest,
    *,
    error_message: str,
    force_rerun: bool = False,
    now: datetime | None = None,
) -> AgentExecutionArtifacts:
    started_at = _normalize_ts(now) or datetime.now(UTC)
    payload = build_opportunity_triage_input_payload(request)
    return _failed_artifacts(
        request=request,
        payload=payload,
        started_at=started_at,
        status=AgentInvocationStatus.FAILURE,
        error_message=error_message,
        force_rerun=force_rerun,
    )


def build_opportunity_triage_input_payload(request: OpportunityTriageAgentRequest) -> dict[str, Any]:
    return {
        "market_id": request.market_id,
        "question": request.question,
        "location_name": request.location_name,
        "best_side": request.best_side,
        "ranking_score": request.ranking_score,
        "recommended_size": request.recommended_size,
        "allocation_status": request.allocation_status,
        "operator_bucket": request.operator_bucket,
        "queue_reason_codes": list(request.queue_reason_codes),
        "execution_intelligence_score": request.execution_intelligence_score,
        "top_of_book_stability": request.top_of_book_stability,
        "spread_regime": request.spread_regime,
        "expected_capture_regime": request.expected_capture_regime,
        "expected_slippage_regime": request.expected_slippage_regime,
        "microstructure_reason_codes": list(request.microstructure_reason_codes),
        "calibration_gate_status": request.calibration_gate_status,
        "capital_policy_id": request.capital_policy_id,
        "surface_delivery_status": request.surface_delivery_status,
        "surface_fallback_origin": request.surface_fallback_origin,
        "source_provenance": dict(request.source_provenance),
        "source_freshness": request.source_freshness,
        "is_degraded_source": request.is_degraded_source,
        "why_ranked_json": dict(request.why_ranked_json),
        "pricing_context_json": dict(request.pricing_context_json),
    }


def build_opportunity_triage_model_payload(request: OpportunityTriageAgentRequest) -> dict[str, Any]:
    why_ranked_json = dict(request.why_ranked_json or {})
    pricing_context_json = dict(request.pricing_context_json or {})
    return {
        "market_id": request.market_id,
        "question": request.question,
        "location_name": request.location_name,
        "best_side": request.best_side,
        "ranking_score": request.ranking_score,
        "recommended_size": request.recommended_size,
        "allocation_status": request.allocation_status,
        "operator_bucket": request.operator_bucket,
        "queue_reason_codes": list(request.queue_reason_codes),
        "execution_intelligence_score": request.execution_intelligence_score,
        "top_of_book_stability": request.top_of_book_stability,
        "spread_regime": request.spread_regime,
        "expected_capture_regime": request.expected_capture_regime,
        "expected_slippage_regime": request.expected_slippage_regime,
        "microstructure_reason_codes": list(request.microstructure_reason_codes),
        "calibration_gate_status": request.calibration_gate_status,
        "capital_policy_id": request.capital_policy_id,
        "surface_delivery_status": request.surface_delivery_status,
        "surface_fallback_origin": request.surface_fallback_origin,
        "source_provenance": {
            "primary_source": request.source_provenance.get("primary_source"),
            "source_badge": request.source_provenance.get("source_badge"),
            "source_truth_status": request.source_provenance.get("source_truth_status"),
            "subject_type": request.source_provenance.get("subject_type"),
        },
        "source_freshness": request.source_freshness,
        "is_degraded_source": request.is_degraded_source,
        "why_ranked": {
            "economics_path": why_ranked_json.get("economics_path"),
            "prior_lookup_mode": why_ranked_json.get("prior_lookup_mode"),
            "prior_quality_status": why_ranked_json.get("prior_quality_status"),
            "empirical_sample_count": why_ranked_json.get("empirical_sample_count"),
            "capital_scaling_reason_codes": why_ranked_json.get("capital_scaling_reason_codes"),
            "ranking_penalty_reasons": pricing_context_json.get("ranking_penalty_reasons"),
        },
        "pricing_context": {
            "actionability_status": pricing_context_json.get("actionability_status"),
            "decision": pricing_context_json.get("decision"),
            "feedback_status": pricing_context_json.get("feedback_status"),
            "calibration_gate_reason_codes": pricing_context_json.get("calibration_gate_reason_codes"),
            "execution_intelligence_score": pricing_context_json.get("execution_intelligence_score"),
            "prior_lookup_mode": pricing_context_json.get("prior_lookup_mode"),
        },
    }


def parse_opportunity_triage_output(
    payload: dict[str, Any],
    *,
    request: OpportunityTriageAgentRequest,
) -> OpportunityTriageAgentOutput:
    triage_status = _required_text(payload.get("triage_status"), "triage_status")
    priority_band = _required_text(payload.get("priority_band"), "priority_band")
    recommended_operator_action = _required_text(payload.get("recommended_operator_action"), "recommended_operator_action")
    confidence_band = _required_text(payload.get("confidence_band"), "confidence_band")
    summary = _required_text(payload.get("summary") or payload.get("triage_summary"), "summary")
    triage_reason_codes = _string_list(payload.get("triage_reason_codes"))
    execution_risk_flags = _string_list(payload.get("execution_risk_flags"))
    supporting_evidence_refs = _string_list(payload.get("supporting_evidence_refs"))
    confidence = float(payload.get("confidence") or _confidence_from_band(confidence_band))
    verdict = _verdict_from_triage_status(request=request, triage_status=triage_status)
    human_review_required = bool(
        payload.get("human_review_required")
        if payload.get("human_review_required") is not None
        else verdict in {AgentVerdict.REVIEW, AgentVerdict.BLOCK}
    )
    return OpportunityTriageAgentOutput(
        triage_status=triage_status,
        priority_band=priority_band,
        triage_reason_codes=triage_reason_codes,
        execution_risk_flags=execution_risk_flags,
        recommended_operator_action=recommended_operator_action,
        confidence_band=confidence_band,
        supporting_evidence_refs=supporting_evidence_refs,
        verdict=verdict,
        confidence=confidence,
        summary=summary,
        human_review_required=human_review_required,
    )


def opportunity_triage_output_to_json(output: OpportunityTriageAgentOutput) -> dict[str, Any]:
    return {
        "triage_status": output.triage_status,
        "priority_band": output.priority_band,
        "triage_reason_codes": list(output.triage_reason_codes),
        "execution_risk_flags": list(output.execution_risk_flags),
        "recommended_operator_action": output.recommended_operator_action,
        "confidence_band": output.confidence_band,
        "supporting_evidence_refs": list(output.supporting_evidence_refs),
        "verdict": output.verdict.value,
        "confidence": output.confidence,
        "summary": output.summary,
        "human_review_required": output.human_review_required,
    }


def _failed_artifacts(
    *,
    request: OpportunityTriageAgentRequest,
    payload: dict[str, Any],
    started_at: datetime,
    status: AgentInvocationStatus,
    error_message: str,
    force_rerun: bool,
) -> AgentExecutionArtifacts:
    ended_at = datetime.now(UTC)
    invocation = build_agent_invocation_record(
        agent_type=AgentType.OPPORTUNITY_TRIAGE,
        agent_version=AGENT_VERSION,
        prompt_version=PROMPT_VERSION,
        subject_type=SUBJECT_TYPE,
        subject_id=request.market_id,
        input_payload_json=payload,
        model_provider="unavailable",
        model_name="unavailable",
        status=status,
        started_at=started_at,
        ended_at=ended_at,
        latency_ms=_latency_ms(started_at, ended_at),
        error_message=error_message,
        force_rerun=force_rerun,
    )
    return AgentExecutionArtifacts(invocation=invocation, output=None, review=None, evaluation=None)


def _deterministic_fallback_artifacts(
    *,
    request: OpportunityTriageAgentRequest,
    payload: dict[str, Any],
    started_at: datetime,
    error_message: str,
    force_rerun: bool,
) -> AgentExecutionArtifacts:
    ended_at = datetime.now(UTC)
    output = _build_deterministic_fallback_output(request=request, error_message=error_message)
    invocation = build_agent_invocation_record(
        agent_type=AgentType.OPPORTUNITY_TRIAGE,
        agent_version=AGENT_VERSION,
        prompt_version=PROMPT_VERSION,
        subject_type=SUBJECT_TYPE,
        subject_id=request.market_id,
        input_payload_json=payload,
        model_provider="deterministic_fallback",
        model_name="local_triage_fallback_v1",
        status=AgentInvocationStatus.SUCCESS,
        started_at=started_at,
        ended_at=ended_at,
        latency_ms=_latency_ms(started_at, ended_at),
        error_message=error_message,
        force_rerun=force_rerun,
    )
    output_record = build_agent_output_record(
        invocation_id=invocation.invocation_id,
        verdict=output.verdict,
        confidence=output.confidence,
        summary=output.summary,
        findings=_findings_from_output(request.market_id, output),
        structured_output_json=opportunity_triage_output_to_json(output),
        human_review_required=output.human_review_required,
        created_at=ended_at,
    )
    review_record = build_agent_review_record(
        invocation_id=invocation.invocation_id,
        human_review_required=output.human_review_required,
        review_payload_json={
            "priority_band": output.priority_band,
            "recommended_operator_action": output.recommended_operator_action,
            "triage_status": output.triage_status,
            "subject_id": request.market_id,
            "fallback_mode": "provider_unavailable",
        },
        reviewed_at=ended_at,
    )
    evaluation_record = build_agent_evaluation_record(
        invocation_id=invocation.invocation_id,
        confidence=output.confidence,
        human_review_required=output.human_review_required,
        verification_method="operator_outcome_proxy",
        is_verified=False,
        notes="deterministic fallback because triage provider was unavailable",
        score_json=_evaluation_score_json(request, output),
        created_at=ended_at,
    )
    return AgentExecutionArtifacts(
        invocation=invocation,
        output=output_record,
        review=review_record,
        evaluation=evaluation_record,
    )


def _build_deterministic_fallback_output(
    *,
    request: OpportunityTriageAgentRequest,
    error_message: str,
) -> OpportunityTriageAgentOutput:
    reason_codes: list[str] = []
    risk_flags = list(request.microstructure_reason_codes)
    lowered = error_message.lower()
    if "401" in lowered or "unauthorized" in lowered:
        reason_codes.append("provider_unauthorized")
    if "403" in lowered or "forbidden" in lowered:
        reason_codes.append("provider_forbidden")
    if "429" in lowered or "rate" in lowered:
        reason_codes.append("provider_rate_limited")
    if request.is_degraded_source or request.surface_delivery_status != "ok":
        reason_codes.append("surface_degraded")
    if request.calibration_gate_status != "clear":
        reason_codes.append(f"calibration:{request.calibration_gate_status}")
    if request.execution_intelligence_score < 0.40:
        risk_flags.append("execution_intelligence_weak")
    if request.top_of_book_stability < 0.45:
        risk_flags.append("book_stability_low")

    if request.operator_bucket in {"blocked", "research_only"} or request.calibration_gate_status == "research_only":
        triage_status = "defer"
        priority_band = "low"
        recommended_operator_action = "defer"
    elif request.ranking_score >= 0.75 and request.allocation_status in {"approved", "resized"}:
        triage_status = "review"
        priority_band = "high"
        recommended_operator_action = "manual_review"
    else:
        triage_status = "review"
        priority_band = "medium"
        recommended_operator_action = "manual_review"

    summary = (
        "外部 triage 服务当前不可用，已退回本地保守分诊。"
        f" ranking={round(float(request.ranking_score), 4)}"
        f" bucket={request.operator_bucket}"
        f" calibration={request.calibration_gate_status}"
        f" delivery={request.surface_delivery_status}"
    )
    supporting_evidence_refs = [
        f"ui.market_opportunity_summary:{request.market_id}",
        f"ui.action_queue_summary:{request.market_id}",
    ]
    if request.execution_intelligence_score > 0.0:
        supporting_evidence_refs.append(f"ui.market_microstructure_summary:{request.market_id}")
    return OpportunityTriageAgentOutput(
        triage_status=triage_status,
        priority_band=priority_band,
        triage_reason_codes=_dedup_text(reason_codes or ["provider_unavailable"]),
        execution_risk_flags=_dedup_text(risk_flags),
        recommended_operator_action=recommended_operator_action,
        confidence_band="low",
        supporting_evidence_refs=_dedup_text(supporting_evidence_refs),
        verdict=_verdict_from_triage_status(request=request, triage_status=triage_status),
        confidence=0.35,
        summary=summary,
        human_review_required=True,
    )


def _evaluation_score_json(request: OpportunityTriageAgentRequest, output: OpportunityTriageAgentOutput) -> dict[str, Any]:
    surface_ok = request.surface_delivery_status == "ok"
    quality_clear = request.calibration_gate_status == "clear"
    priority_precision_proxy = round(min(1.0, max(0.0, output.confidence * max(request.ranking_score, 0.0))), 4)
    queue_cleanliness_delta = round(
        0.2 if output.priority_band.lower() in {"critical", "high"} and surface_ok else (0.05 if surface_ok else -0.1),
        4,
    )
    false_escalation_rate = round(0.0 if surface_ok and quality_clear else 0.25, 4)
    operator_throughput_delta = round(0.15 if not output.human_review_required else 0.05, 4)
    return {
        "queue_cleanliness_delta": queue_cleanliness_delta,
        "priority_precision_proxy": priority_precision_proxy,
        "false_escalation_rate": false_escalation_rate,
        "operator_throughput_delta": operator_throughput_delta,
        "baseline_operator_bucket": request.operator_bucket,
        "overlay_priority_band": output.priority_band,
        "verification_method_hint": "operator_outcome_proxy",
    }


def _findings_from_output(market_id: str, output: OpportunityTriageAgentOutput) -> list[AgentFinding]:
    findings: list[AgentFinding] = []
    for code in output.triage_reason_codes:
        findings.append(
            AgentFinding(
                finding_code=str(code),
                severity="warn",
                entity_type=SUBJECT_TYPE,
                entity_id=market_id,
                field_name="triage_reason_codes",
                summary=str(code),
            )
        )
    for code in output.execution_risk_flags:
        findings.append(
            AgentFinding(
                finding_code=str(code),
                severity="warn",
                entity_type=SUBJECT_TYPE,
                entity_id=market_id,
                field_name="execution_risk_flags",
                summary=str(code),
                suggested_action=output.recommended_operator_action,
            )
        )
    return findings


def _system_prompt() -> str:
    return (
        "You are the Asterion Opportunity Triage Agent. "
        "You are advisory-only. You must not recommend direct autonomous execution. "
        "Read only the provided persisted facts and return a compact JSON object. "
        "Keep all structured enum fields in English, but write the operator-facing summary in Simplified Chinese."
    )


def _user_prompt() -> str:
    return (
        "Assess the weather market opportunity using only the persisted facts. "
        "Return JSON with triage_status, priority_band, triage_reason_codes, "
        "execution_risk_flags, recommended_operator_action, confidence_band, "
        "supporting_evidence_refs, summary, confidence, and human_review_required. "
        "The summary must be concise Simplified Chinese. supporting_evidence_refs must remain unchanged."
    )


def _required_text(value: Any, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{name} is required")
    return text


def _string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if value in {None, ""}:
        return []
    try:
        parsed = json.loads(str(value))
    except Exception:  # noqa: BLE001
        return [str(value)]
    if isinstance(parsed, list):
        return [str(item) for item in parsed if str(item).strip()]
    return []


def _dedup_text(values: list[str]) -> list[str]:
    out: list[str] = []
    for item in values:
        text = str(item or "").strip()
        if not text or text in out:
            continue
        out.append(text)
    return out


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value in {None, ""}:
        return {}
    try:
        payload = json.loads(str(value))
    except Exception:  # noqa: BLE001
        return {}
    return payload if isinstance(payload, dict) else {}


def _table_columns(con, schema: str, table: str) -> set[str]:
    return {
        str(row[0])
        for row in con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ?
            """,
            [schema, table],
        ).fetchall()
    }


def _json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if value in {None, ""}:
        return []
    try:
        payload = json.loads(str(value))
    except Exception:  # noqa: BLE001
        return []
    return [str(item) for item in payload] if isinstance(payload, list) else []


def _normalize_ts(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _latency_ms(started_at: datetime, ended_at: datetime) -> int:
    return max(0, int((ended_at - started_at).total_seconds() * 1000))


def _confidence_from_band(band: str) -> float:
    normalized = band.strip().lower()
    if normalized == "high":
        return 0.85
    if normalized == "medium":
        return 0.65
    if normalized == "low":
        return 0.4
    return 0.5


def _verdict_from_triage_status(*, request: OpportunityTriageAgentRequest, triage_status: str) -> AgentVerdict:
    normalized = triage_status.strip().lower()
    if normalized in {"blocked", "suppress", "do_not_prioritize"}:
        return AgentVerdict.BLOCK
    if request.surface_delivery_status in {"read_error", "missing"}:
        return AgentVerdict.BLOCK
    if normalized in {"prioritized", "review", "escalate", "defer"}:
        return AgentVerdict.REVIEW
    return AgentVerdict.PASS


def _should_use_deterministic_fallback(error_message: str) -> bool:
    lowered = str(error_message or "").lower()
    return any(
        token in lowered
        for token in (
            "401",
            "unauthorized",
            "403",
            "forbidden",
            "429",
            "rate limit",
            "rate-limited",
            "provider unavailable",
        )
    )
