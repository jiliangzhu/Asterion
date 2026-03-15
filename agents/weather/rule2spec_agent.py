from __future__ import annotations

import json
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
from asterion_core.contracts import Rule2SpecDraft, StationMetadata, WeatherMarket, WeatherMarketSpecRecord
from domains.weather.pricing.engine import load_weather_market_spec
from domains.weather.spec import StationMapper, load_weather_markets_for_rule2spec, parse_rule2spec_draft


AGENT_VERSION = "rule2spec_agent_v1"
PROMPT_VERSION = "rule2spec_prompt_v1"
ALLOWED_RULE2SPEC_PATCH_FIELDS = {
    "authoritative_source",
    "bucket_max_value",
    "bucket_min_value",
    "inclusive_bounds",
    "location_name",
    "metric",
    "observation_window_local",
    "rounding_rule",
    "station_id",
    "unit",
}


@dataclass(frozen=True)
class Rule2SpecAgentRequest:
    market: WeatherMarket
    draft: Rule2SpecDraft
    current_spec: WeatherMarketSpecRecord | None
    station_metadata: StationMetadata | None
    station_override_summary: dict[str, Any]


@dataclass(frozen=True)
class Rule2SpecAgentOutput:
    verdict: AgentVerdict
    confidence: float
    summary: str
    risk_flags: list[str]
    suggested_patch_json: dict[str, Any]
    findings: list[AgentFinding]
    human_review_required: bool

    def __post_init__(self) -> None:
        if not (0.0 <= float(self.confidence) <= 1.0):
            raise ValueError("confidence must be between 0 and 1")
        if not self.summary:
            raise ValueError("summary is required")
        unknown_keys = set(self.suggested_patch_json) - ALLOWED_RULE2SPEC_PATCH_FIELDS
        if "city" in self.suggested_patch_json or unknown_keys:
            raise ValueError("suggested_patch_json contains unsupported fields")
        if "station_id" in self.suggested_patch_json and not self.suggested_patch_json["station_id"]:
            raise ValueError("station_id patch must be non-empty when provided")


def load_rule2spec_agent_requests(
    con,
    *,
    mapper: StationMapper | None = None,
    market_ids: list[str] | None = None,
    active_only: bool = False,
    limit: int | None = None,
) -> list[Rule2SpecAgentRequest]:
    active_mapper = mapper or StationMapper()
    id_filter = set(market_ids or [])
    requests: list[Rule2SpecAgentRequest] = []
    for market in load_weather_markets_for_rule2spec(con, active_only=active_only, limit=limit):
        if id_filter and market.market_id not in id_filter:
            continue
        draft = parse_rule2spec_draft(market)
        current_spec = _load_optional_weather_market_spec(con, market_id=market.market_id)
        station_metadata = _load_optional_station_metadata(active_mapper, con, draft=draft)
        requests.append(
            Rule2SpecAgentRequest(
                market=market,
                draft=draft,
                current_spec=current_spec,
                station_metadata=station_metadata,
                station_override_summary=_load_station_override_summary(con, market_id=market.market_id),
            )
        )
    return requests


def run_rule2spec_agent_review(
    client: AgentClient,
    request: Rule2SpecAgentRequest,
    *,
    force_rerun: bool = False,
    now: datetime | None = None,
) -> AgentExecutionArtifacts:
    started_at = _normalize_ts(now) or datetime.now(UTC)
    payload = build_rule2spec_agent_input_payload(request)
    try:
        response = client.invoke(
            system_prompt=_system_prompt(),
            user_prompt=_user_prompt(),
            input_payload_json=payload,
            metadata={"agent_type": AgentType.RULE2SPEC.value, "subject_type": "weather_market", "subject_id": request.market.market_id},
        )
        output = parse_rule2spec_agent_output(response.structured_output_json, request=request)
        ended_at = datetime.now(UTC)
        invocation = build_agent_invocation_record(
            agent_type=AgentType.RULE2SPEC,
            agent_version=AGENT_VERSION,
            prompt_version=PROMPT_VERSION,
            subject_type="weather_market",
            subject_id=request.market.market_id,
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
            structured_output_json=rule2spec_output_to_json(output),
            human_review_required=output.human_review_required,
            created_at=ended_at,
        )
        review_record = build_agent_review_record(
            invocation_id=invocation.invocation_id,
            human_review_required=output.human_review_required,
            review_payload_json={
                "risk_flags": list(output.risk_flags),
                "subject_id": request.market.market_id,
            },
            reviewed_at=ended_at,
        )
        evaluation_record = build_agent_evaluation_record(
            invocation_id=invocation.invocation_id,
            confidence=output.confidence,
            human_review_required=output.human_review_required,
            score_json={
                "authoritative_source_match": _score_match(
                    output.suggested_patch_json.get("authoritative_source"),
                    request.current_spec.authoritative_source if request.current_spec is not None else None,
                ),
                "bucket_bounds_match": _score_match(
                    [output.suggested_patch_json.get("bucket_min_value"), output.suggested_patch_json.get("bucket_max_value")],
                    [request.current_spec.bucket_min_value, request.current_spec.bucket_max_value] if request.current_spec is not None else None,
                ),
                "metric_match": _score_match(output.suggested_patch_json.get("metric"), request.draft.metric),
                "station_id_match": _score_match(
                    output.suggested_patch_json.get("station_id"),
                    request.station_metadata.station_id if request.station_metadata is not None else None,
                ),
                "unit_match": _score_match(output.suggested_patch_json.get("unit"), request.draft.unit),
            },
            created_at=ended_at,
        )
        return AgentExecutionArtifacts(
            invocation=invocation,
            output=output_record,
            review=review_record,
            evaluation=evaluation_record,
        )
    except TimeoutError as exc:
        return _failed_artifacts(
            request=request,
            payload=payload,
            started_at=started_at,
            status=AgentInvocationStatus.TIMEOUT,
            error_message=str(exc),
            force_rerun=force_rerun,
        )
    except ValueError as exc:
        return _failed_artifacts(
            request=request,
            payload=payload,
            started_at=started_at,
            status=AgentInvocationStatus.PARSE_ERROR,
            error_message=str(exc),
            force_rerun=force_rerun,
        )
    except Exception as exc:  # noqa: BLE001
        return _failed_artifacts(
            request=request,
            payload=payload,
            started_at=started_at,
            status=AgentInvocationStatus.FAILURE,
            error_message=str(exc),
            force_rerun=force_rerun,
        )


def build_rule2spec_agent_input_payload(request: Rule2SpecAgentRequest) -> dict[str, Any]:
    current_spec = None
    if request.current_spec is not None:
        current_spec = {
            "authoritative_source": request.current_spec.authoritative_source,
            "bucket_max_value": request.current_spec.bucket_max_value,
            "bucket_min_value": request.current_spec.bucket_min_value,
            "inclusive_bounds": request.current_spec.inclusive_bounds,
            "location_name": request.current_spec.location_name,
            "metric": request.current_spec.metric,
            "observation_window_local": request.current_spec.observation_window_local,
            "rounding_rule": request.current_spec.rounding_rule,
            "spec_version": request.current_spec.spec_version,
            "station_id": request.current_spec.station_id,
            "unit": request.current_spec.unit,
        }
    station_metadata = None
    if request.station_metadata is not None:
        station_metadata = {
            "latitude": request.station_metadata.latitude,
            "location_name": request.station_metadata.location_name,
            "longitude": request.station_metadata.longitude,
            "source": request.station_metadata.source,
            "station_id": request.station_metadata.station_id,
            "timezone": request.station_metadata.timezone,
        }
    return {
        "current_spec": current_spec,
        "draft": {
            "authoritative_source": request.draft.authoritative_source,
            "bucket_max_value": request.draft.bucket_max_value,
            "bucket_min_value": request.draft.bucket_min_value,
            "condition_id": request.draft.condition_id,
            "fallback_sources": list(request.draft.fallback_sources),
            "inclusive_bounds": request.draft.inclusive_bounds,
            "location_name": request.draft.location_name,
            "market_id": request.draft.market_id,
            "metric": request.draft.metric,
            "observation_date": request.draft.observation_date.isoformat(),
            "observation_window_local": request.draft.observation_window_local,
            "parse_confidence": request.draft.parse_confidence,
            "risk_flags": list(request.draft.risk_flags),
            "rounding_rule": request.draft.rounding_rule,
            "unit": request.draft.unit,
        },
        "market": {
            "close_time": request.market.close_time.isoformat() if request.market.close_time is not None else None,
            "condition_id": request.market.condition_id,
            "market_id": request.market.market_id,
            "outcomes": list(request.market.outcomes),
            "rules": request.market.rules,
            "title": request.market.title,
            "token_ids": list(request.market.token_ids),
        },
        "station_metadata": station_metadata,
        "station_override_summary": dict(request.station_override_summary),
    }


def parse_rule2spec_agent_output(payload: dict[str, Any], *, request: Rule2SpecAgentRequest) -> Rule2SpecAgentOutput:
    findings = _parse_findings(payload.get("findings"), default_entity_type="weather_market", default_entity_id=request.market.market_id)
    raw_patch = dict(payload.get("suggested_patch_json") or {})
    if "city" in raw_patch:
        raise ValueError("suggested_patch_json contains unsupported fields")
    suggested_patch_json = {key: value for key, value in raw_patch.items() if key in ALLOWED_RULE2SPEC_PATCH_FIELDS}
    raw_risk_flags = payload.get("risk_flags", [])
    if raw_risk_flags is None:
        risk_flags = []
    elif isinstance(raw_risk_flags, list):
        risk_flags = [str(item) for item in raw_risk_flags]
    else:
        risk_flags = [str(raw_risk_flags)]
    verdict = _normalize_verdict(payload["verdict"])
    summary = str(payload["summary"])
    confidence = float(payload["confidence"])
    human_review_required = bool(
        payload.get(
            "human_review_required",
            verdict is not AgentVerdict.PASS
            or bool(risk_flags)
            or request.station_metadata is None
            or "missing_station_mapping" in risk_flags,
        )
    )
    return Rule2SpecAgentOutput(
        verdict=verdict,
        confidence=confidence,
        summary=summary,
        risk_flags=risk_flags,
        suggested_patch_json=suggested_patch_json,
        findings=findings,
        human_review_required=human_review_required,
    )


def _normalize_verdict(raw: Any) -> AgentVerdict:
    text = str(raw).strip().lower()
    if text in {"pass", "review", "block"}:
        return AgentVerdict(text)
    if text in {"accepted", "approved"}:
        return AgentVerdict.PASS
    if text in {
        "accept_with_patches",
        "accept-with-patches",
        "accepted_with_changes",
        "accepted-with-changes",
        "acceptable_with_changes",
        "acceptable-with-changes",
        "needs_changes",
        "needs_patch",
        "needs_review",
    }:
        return AgentVerdict.REVIEW
    if text in {"reject", "rejected"}:
        return AgentVerdict.BLOCK
    raise ValueError(f"unsupported verdict: {raw}")


def rule2spec_output_to_json(output: Rule2SpecAgentOutput) -> dict[str, Any]:
    return {
        "confidence": output.confidence,
        "findings": [finding.__dict__ for finding in output.findings],
        "human_review_required": output.human_review_required,
        "risk_flags": list(output.risk_flags),
        "suggested_patch_json": dict(output.suggested_patch_json),
        "summary": output.summary,
        "verdict": output.verdict.value,
    }


def _system_prompt() -> str:
    return (
        "You review deterministic weather market parsing results. "
        "Stay station-first. Never output city-first fields. "
        "Return only one JSON object. Do not use markdown. Do not use code fences. "
        "If a field is unknown, return an empty list, empty object, or null-compatible value instead of prose."
    )


def _user_prompt() -> str:
    return (
        "Review the deterministic Rule2Spec draft against current station mapping and current spec. "
        "Return a single JSON object with exactly these keys: "
        "verdict, confidence, summary, risk_flags, suggested_patch_json, findings, human_review_required. "
        "Allowed verdict values: pass, review, block. "
        "risk_flags must be a JSON array of strings. "
        "suggested_patch_json must only contain: authoritative_source, bucket_max_value, bucket_min_value, inclusive_bounds, "
        "location_name, metric, observation_window_local, rounding_rule, station_id, unit. "
        "findings must be a JSON array. Each finding object should prefer: finding_code, severity(info|warn|error), entity_type, entity_id, field_name, summary, suggested_action. "
        "If there are no findings, return []. If no patch is needed, return {}."
    )


def _parse_findings(raw: Any, *, default_entity_type: str, default_entity_id: str) -> list[AgentFinding]:
    findings: list[AgentFinding] = []
    if raw is None:
        return findings
    if not isinstance(raw, list):
        raise ValueError("findings must be a list")
    for item in raw:
        if isinstance(item, str):
            findings.append(
                AgentFinding(
                    finding_code="model_finding",
                    severity="info",
                    entity_type=default_entity_type,
                    entity_id=default_entity_id,
                    field_name=None,
                    summary=item,
                    suggested_action=None,
                )
            )
            continue
        if not isinstance(item, dict):
            raise ValueError("finding entry must be an object")
        finding_code = str(
            item.get("finding_code")
            or item.get("code")
            or item.get("type")
            or item.get("category")
            or "model_finding"
        )
        severity = str(item.get("severity") or item.get("level") or "info").lower()
        if severity == "warning":
            severity = "warn"
        field_name = item.get("field_name")
        if field_name is None:
            field_name = item.get("field") or item.get("path")
        summary = item.get("summary")
        if summary is None:
            summary = item.get("message") or item.get("detail") or item.get("description") or item.get("finding")
        if summary is None:
            summary = json.dumps(item, ensure_ascii=False, sort_keys=True)
        suggested_action = item.get("suggested_action")
        if suggested_action is None:
            suggested_action = item.get("action") or item.get("recommendation")
        findings.append(
            AgentFinding(
                finding_code=finding_code,
                severity=severity,
                entity_type=str(item.get("entity_type") or default_entity_type),
                entity_id=str(item.get("entity_id") or default_entity_id),
                field_name=str(field_name) if field_name is not None else None,
                summary=str(summary),
                suggested_action=str(suggested_action) if suggested_action is not None else None,
            )
        )
    return findings


def _load_optional_weather_market_spec(con, *, market_id: str) -> WeatherMarketSpecRecord | None:
    try:
        return load_weather_market_spec(con, market_id=market_id)
    except LookupError:
        return None


def _load_optional_station_metadata(mapper: StationMapper, con, *, draft: Rule2SpecDraft) -> StationMetadata | None:
    try:
        return mapper.resolve_from_spec_inputs(
            con,
            market_id=draft.market_id,
            location_name=draft.location_name,
            authoritative_source=draft.authoritative_source,
        )
    except LookupError:
        return None


def _load_station_override_summary(con, *, market_id: str) -> dict[str, Any]:
    rows = con.execute(
        """
        SELECT
            station_id,
            source,
            is_override,
            metadata_json
        FROM weather.weather_station_map
        WHERE market_id = ?
        ORDER BY is_override DESC, updated_at DESC, created_at DESC
        """,
        [market_id],
    ).fetchall()
    return {
        "has_override": any(bool(row[2]) for row in rows),
        "mapping_count": len(rows),
        "station_ids": [str(row[0]) for row in rows],
        "sources": sorted({str(row[1]) for row in rows}),
        "metadata_samples": [json.loads(str(row[3])) if row[3] else {} for row in rows[:2]],
    }


def _failed_artifacts(
    *,
    request: Rule2SpecAgentRequest,
    payload: dict[str, Any],
    started_at: datetime,
    status: AgentInvocationStatus,
    error_message: str,
    force_rerun: bool,
) -> AgentExecutionArtifacts:
    ended_at = datetime.now(UTC)
    invocation = build_agent_invocation_record(
        agent_type=AgentType.RULE2SPEC,
        agent_version=AGENT_VERSION,
        prompt_version=PROMPT_VERSION,
        subject_type="weather_market",
        subject_id=request.market.market_id,
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


def _normalize_ts(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(microsecond=0)
    return value.astimezone(UTC).replace(microsecond=0)


def _latency_ms(started_at: datetime, ended_at: datetime) -> int:
    return max(0, int((ended_at - started_at).total_seconds() * 1000))


def _score_match(proposed: Any, actual: Any) -> float:
    return 1.0 if proposed == actual else 0.0
