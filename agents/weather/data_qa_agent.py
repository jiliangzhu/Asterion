from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
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
from asterion_core.contracts import (
    ForecastReplayDiffRecord,
    ForecastReplayRecord,
    ForecastRunRecord,
    WatchOnlySnapshotRecord,
    WeatherFairValueRecord,
    WeatherMarketSpecRecord,
)
from domains.weather.forecast import load_original_pricing_outputs
from domains.weather.pricing.engine import load_forecast_run, load_weather_market_spec


AGENT_VERSION = "data_qa_agent_v1"
PROMPT_VERSION = "data_qa_prompt_v1"


@dataclass(frozen=True)
class DataQaAgentRequest:
    spec: WeatherMarketSpecRecord
    replay: ForecastReplayRecord
    diffs: list[ForecastReplayDiffRecord]
    original_run: ForecastRunRecord
    replayed_run: ForecastRunRecord
    replay_fair_values: list[WeatherFairValueRecord]
    replay_watch_only_snapshots: list[WatchOnlySnapshotRecord]


@dataclass(frozen=True)
class DataQaAgentOutput:
    verdict: AgentVerdict
    confidence: float
    summary: str
    station_match_score: float
    timezone_ok: bool
    unit_ok: bool
    pricing_provenance_ok: bool
    fallback_risk: str
    findings: list[AgentFinding]
    recommended_actions: list[str]
    human_review_required: bool

    def __post_init__(self) -> None:
        if not (0.0 <= float(self.confidence) <= 1.0):
            raise ValueError("confidence must be between 0 and 1")
        if not (0.0 <= float(self.station_match_score) <= 1.0):
            raise ValueError("station_match_score must be between 0 and 1")
        if self.fallback_risk not in {"low", "medium", "high"}:
            raise ValueError("fallback_risk must be low, medium, or high")
        if not self.summary:
            raise ValueError("summary is required")


def load_data_qa_agent_requests(
    con,
    *,
    replay_ids: list[str] | None = None,
    limit: int | None = None,
) -> list[DataQaAgentRequest]:
    sql = """
        SELECT
            replay_id,
            market_id,
            condition_id,
            station_id,
            source,
            model_run,
            forecast_target_time,
            spec_version,
            replay_key,
            replay_reason,
            original_run_id,
            replayed_run_id,
            created_at
        FROM weather.weather_forecast_replays
    """
    params: list[Any] = []
    if replay_ids:
        placeholders = ",".join(["?"] * len(replay_ids))
        sql += f" WHERE replay_id IN ({placeholders})"
        params.extend(replay_ids)
    sql += " ORDER BY created_at DESC, replay_id DESC"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    rows = con.execute(sql, params).fetchall()
    requests: list[DataQaAgentRequest] = []
    for row in rows:
        replay = ForecastReplayRecord(
            replay_id=str(row[0]),
            market_id=str(row[1]),
            condition_id=str(row[2]),
            station_id=str(row[3]),
            source=str(row[4]),
            model_run=str(row[5]),
            forecast_target_time=row[6],
            spec_version=str(row[7]),
            replay_key=str(row[8]),
            replay_reason=str(row[9]),
            original_run_id=str(row[10]),
            replayed_run_id=str(row[11]),
            created_at=row[12],
        )
        diffs = _load_replay_diffs(con, replay_id=replay.replay_id)
        replayed_fair_values, replayed_snapshots = load_original_pricing_outputs(con, run_id=replay.replayed_run_id)
        requests.append(
            DataQaAgentRequest(
                spec=load_weather_market_spec(con, market_id=replay.market_id),
                replay=replay,
                diffs=diffs,
                original_run=load_forecast_run(con, run_id=replay.original_run_id),
                replayed_run=load_forecast_run(con, run_id=replay.replayed_run_id),
                replay_fair_values=replayed_fair_values,
                replay_watch_only_snapshots=replayed_snapshots,
            )
        )
    return requests


def run_data_qa_agent_review(
    client: AgentClient,
    request: DataQaAgentRequest,
    *,
    force_rerun: bool = False,
    now: datetime | None = None,
) -> AgentExecutionArtifacts:
    started_at = _normalize_ts(now) or datetime.now(UTC)
    payload = build_data_qa_agent_input_payload(request)
    try:
        response = client.invoke(
            system_prompt=_system_prompt(),
            user_prompt=_user_prompt(),
            input_payload_json=payload,
            metadata={"agent_type": AgentType.DATA_QA.value, "subject_type": "forecast_replay", "subject_id": request.replay.replay_id},
        )
        output = parse_data_qa_agent_output(response.structured_output_json, request=request)
        ended_at = datetime.now(UTC)
        invocation = build_agent_invocation_record(
            agent_type=AgentType.DATA_QA,
            agent_version=AGENT_VERSION,
            prompt_version=PROMPT_VERSION,
            subject_type="forecast_replay",
            subject_id=request.replay.replay_id,
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
            findings=output.findings,
            structured_output_json=data_qa_output_to_json(output),
            human_review_required=output.human_review_required,
            created_at=ended_at,
        )
        review_record = build_agent_review_record(
            invocation_id=invocation.invocation_id,
            human_review_required=output.human_review_required,
            review_payload_json={
                "critical_diff_count": sum(1 for item in request.diffs if item.status != "MATCH"),
                "fallback_risk": output.fallback_risk,
                "subject_id": request.replay.replay_id,
            },
            reviewed_at=ended_at,
        )
        evaluation_record = build_agent_evaluation_record(
            invocation_id=invocation.invocation_id,
            confidence=output.confidence,
            human_review_required=output.human_review_required,
            score_json={
                "critical_diff_count": sum(1 for item in request.diffs if item.status != "MATCH"),
                "fallback_used": request.replayed_run.fallback_used,
                "station_match_score": output.station_match_score,
                "timezone_ok": output.timezone_ok,
                "unit_ok": output.unit_ok,
            },
            created_at=ended_at,
        )
        return AgentExecutionArtifacts(invocation=invocation, output=output_record, review=review_record, evaluation=evaluation_record)
    except TimeoutError as exc:
        return _failed_artifacts(request=request, payload=payload, started_at=started_at, status=AgentInvocationStatus.TIMEOUT, error_message=str(exc), force_rerun=force_rerun)
    except ValueError as exc:
        return _failed_artifacts(request=request, payload=payload, started_at=started_at, status=AgentInvocationStatus.PARSE_ERROR, error_message=str(exc), force_rerun=force_rerun)
    except Exception as exc:  # noqa: BLE001
        return _failed_artifacts(request=request, payload=payload, started_at=started_at, status=AgentInvocationStatus.FAILURE, error_message=str(exc), force_rerun=force_rerun)


def build_data_qa_agent_input_payload(request: DataQaAgentRequest) -> dict[str, Any]:
    return {
        "diffs": [
            {
                "diff_id": item.diff_id,
                "diff_summary_json": dict(item.diff_summary_json),
                "entity_key": item.entity_key,
                "entity_type": item.entity_type,
                "status": item.status,
            }
            for item in request.diffs
        ],
        "original_run": _forecast_run_summary(request.original_run),
        "replay": {
            "market_id": request.replay.market_id,
            "original_run_id": request.replay.original_run_id,
            "replay_id": request.replay.replay_id,
            "replay_key": request.replay.replay_key,
            "replay_reason": request.replay.replay_reason,
            "replayed_run_id": request.replay.replayed_run_id,
            "source": request.replay.source,
            "spec_version": request.replay.spec_version,
            "station_id": request.replay.station_id,
        },
        "replay_fair_values": [_fair_value_summary(item) for item in request.replay_fair_values],
        "replay_watch_only_snapshots": [_snapshot_summary(item) for item in request.replay_watch_only_snapshots],
        "replayed_run": _forecast_run_summary(request.replayed_run),
        "spec": {
            "market_id": request.spec.market_id,
            "metric": request.spec.metric,
            "spec_version": request.spec.spec_version,
            "station_id": request.spec.station_id,
            "timezone": request.spec.timezone,
            "unit": request.spec.unit,
        },
    }


def parse_data_qa_agent_output(payload: dict[str, Any], *, request: DataQaAgentRequest) -> DataQaAgentOutput:
    findings = _parse_findings(payload.get("findings"), default_entity_type="forecast_replay", default_entity_id=request.replay.replay_id)
    verdict = AgentVerdict(str(payload["verdict"]))
    confidence = float(payload["confidence"])
    summary = str(payload["summary"])
    recommended_actions = [str(item) for item in payload.get("recommended_actions", [])]
    human_review_required = bool(
        payload.get(
            "human_review_required",
            verdict is not AgentVerdict.PASS
            or request.replayed_run.fallback_used
            or any(item.status != "MATCH" for item in request.diffs),
        )
    )
    return DataQaAgentOutput(
        verdict=verdict,
        confidence=confidence,
        summary=summary,
        station_match_score=float(payload["station_match_score"]),
        timezone_ok=bool(payload["timezone_ok"]),
        unit_ok=bool(payload["unit_ok"]),
        pricing_provenance_ok=bool(payload["pricing_provenance_ok"]),
        fallback_risk=str(payload["fallback_risk"]).lower(),
        findings=findings,
        recommended_actions=recommended_actions,
        human_review_required=human_review_required,
    )


def data_qa_output_to_json(output: DataQaAgentOutput) -> dict[str, Any]:
    return {
        "confidence": output.confidence,
        "fallback_risk": output.fallback_risk,
        "findings": [finding.__dict__ for finding in output.findings],
        "human_review_required": output.human_review_required,
        "pricing_provenance_ok": output.pricing_provenance_ok,
        "recommended_actions": list(output.recommended_actions),
        "station_match_score": output.station_match_score,
        "summary": output.summary,
        "timezone_ok": output.timezone_ok,
        "unit_ok": output.unit_ok,
        "verdict": output.verdict.value,
    }


def _load_replay_diffs(con, *, replay_id: str) -> list[ForecastReplayDiffRecord]:
    rows = con.execute(
        """
        SELECT
            diff_id,
            replay_id,
            entity_type,
            entity_key,
            original_entity_id,
            replayed_entity_id,
            status,
            diff_summary_json,
            created_at
        FROM weather.weather_forecast_replay_diffs
        WHERE replay_id = ?
        ORDER BY entity_type, entity_key
        """,
        [replay_id],
    ).fetchall()
    return [
        ForecastReplayDiffRecord(
            diff_id=str(row[0]),
            replay_id=str(row[1]),
            entity_type=str(row[2]),
            entity_key=str(row[3]),
            original_entity_id=str(row[4]) if row[4] is not None else None,
            replayed_entity_id=str(row[5]) if row[5] is not None else None,
            status=str(row[6]),
            diff_summary_json=_json_dict(row[7]),
            created_at=row[8],
        )
        for row in rows
    ]


def _system_prompt() -> str:
    return (
        "You review deterministic forecast replay and pricing provenance. "
        "Do not suggest table writes. Return only JSON."
    )


def _user_prompt() -> str:
    return (
        "Review the replay, replay diffs, and pricing provenance. "
        "Return verdict, confidence, summary, station_match_score, timezone_ok, unit_ok, pricing_provenance_ok, "
        "fallback_risk, findings, recommended_actions, and human_review_required."
    )


def _parse_findings(raw: Any, *, default_entity_type: str, default_entity_id: str) -> list[AgentFinding]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ValueError("findings must be a list")
    findings: list[AgentFinding] = []
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("finding entry must be an object")
        findings.append(
            AgentFinding(
                finding_code=str(item["finding_code"]),
                severity=str(item["severity"]).lower(),
                entity_type=str(item.get("entity_type") or default_entity_type),
                entity_id=str(item.get("entity_id") or default_entity_id),
                field_name=str(item["field_name"]) if item.get("field_name") is not None else None,
                summary=str(item["summary"]),
                suggested_action=str(item["suggested_action"]) if item.get("suggested_action") is not None else None,
            )
        )
    return findings


def _forecast_run_summary(run: ForecastRunRecord) -> dict[str, Any]:
    return {
        "cache_key": run.cache_key,
        "confidence": run.confidence,
        "fallback_used": run.fallback_used,
        "forecast_payload": dict(run.forecast_payload),
        "from_cache": run.from_cache,
        "model_run": run.model_run,
        "run_id": run.run_id,
        "source": run.source,
        "source_trace": list(run.source_trace),
        "spec_version": run.spec_version,
        "station_id": run.station_id,
        "timezone": run.timezone,
    }


def _fair_value_summary(record: WeatherFairValueRecord) -> dict[str, Any]:
    return {
        "confidence": record.confidence,
        "fair_value": record.fair_value,
        "outcome": record.outcome,
        "token_id": record.token_id,
    }


def _snapshot_summary(record: WatchOnlySnapshotRecord) -> dict[str, Any]:
    return {
        "decision": record.decision,
        "edge_bps": record.edge_bps,
        "fair_value": record.fair_value,
        "outcome": record.outcome,
        "reference_price": record.reference_price,
        "side": record.side,
        "threshold_bps": record.threshold_bps,
        "token_id": record.token_id,
    }


def _failed_artifacts(
    *,
    request: DataQaAgentRequest,
    payload: dict[str, Any],
    started_at: datetime,
    status: AgentInvocationStatus,
    error_message: str,
    force_rerun: bool,
) -> AgentExecutionArtifacts:
    ended_at = datetime.now(UTC)
    invocation = build_agent_invocation_record(
        agent_type=AgentType.DATA_QA,
        agent_version=AGENT_VERSION,
        prompt_version=PROMPT_VERSION,
        subject_type="forecast_replay",
        subject_id=request.replay.replay_id,
        input_payload_json=payload,
        model_provider="agent_runtime",
        model_name="failed_before_output",
        status=status,
        started_at=started_at,
        ended_at=ended_at,
        latency_ms=_latency_ms(started_at, ended_at),
        error_message=error_message,
        force_rerun=force_rerun,
    )
    return AgentExecutionArtifacts(invocation=invocation, output=None, review=None, evaluation=None)


def _json_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    import json

    decoded = json.loads(str(value))
    if not isinstance(decoded, dict):
        raise ValueError("diff_summary_json must decode to an object")
    return decoded


def _normalize_ts(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(microsecond=0)
    return value.astimezone(UTC).replace(microsecond=0)


def _latency_ms(started_at: datetime, ended_at: datetime) -> int:
    return max(0, int((ended_at - started_at).total_seconds() * 1000))
