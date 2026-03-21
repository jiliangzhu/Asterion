from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from asterion_core.contracts import (
    ForecastReplayDiffRecord,
    ForecastReplayRecord,
    ForecastRunRecord,
    ResolutionOperatorDecisionStatus,
    ResolutionOperatorReviewDecisionRecord,
    StationMetadata,
    WeatherFairValueRecord,
    WeatherMarket,
    WeatherMarketSpecRecord,
    WatchOnlySnapshotRecord,
    stable_object_id,
)
from domains.weather.opportunity import build_weather_opportunity_assessment, derive_opportunity_side
from domains.weather.forecast import validate_replay_quality
from domains.weather.spec import parse_rule2spec_draft, validate_rule2spec_draft
from ui.surface_truth import (
    annotate_frame_with_source_truth,
    build_opportunity_row_source_badge,
    load_boundary_sidebar_summary,
    load_primary_score_descriptor,
)

try:
    import duckdb
except ModuleNotFoundError:  # pragma: no cover - exercised by runtime environments
    duckdb = None


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
UI_DIR = DATA_DIR / "ui"
REAL_WEATHER_CHAIN_DIR = DATA_DIR / "dev" / "real_weather_chain"

DEFAULT_UI_LITE_DB_PATH = UI_DIR / "asterion_ui_lite.duckdb"
DEFAULT_UI_REPLICA_DB_PATH = UI_DIR / "asterion_ui.duckdb"
DEFAULT_P4_READINESS_REPORT_PATH = UI_DIR / "asterion_readiness_p4.json"
DEFAULT_P4_READINESS_REPORT_MD_PATH = UI_DIR / "asterion_readiness_p4.md"
DEFAULT_P4_READINESS_EVIDENCE_PATH = UI_DIR / "asterion_readiness_evidence_p4.json"
DEFAULT_CONTROLLED_LIVE_CAPABILITY_MANIFEST_PATH = DATA_DIR / "meta" / "controlled_live_capability_manifest.json"
DEFAULT_REAL_WEATHER_CHAIN_REPORT_PATH = REAL_WEATHER_CHAIN_DIR / "real_weather_chain_report.json"
DEFAULT_REAL_WEATHER_CHAIN_DB_PATH = REAL_WEATHER_CHAIN_DIR / "real_weather_chain.duckdb"
DEFAULT_CANONICAL_DB_PATH = DATA_DIR / "asterion.duckdb"

UI_TABLES = {
    "market_watch_summary": "ui.market_watch_summary",
    "market_opportunity_summary": "ui.market_opportunity_summary",
    "execution_ticket_summary": "ui.execution_ticket_summary",
    "execution_run_summary": "ui.execution_run_summary",
    "execution_exception_summary": "ui.execution_exception_summary",
    "live_prereq_execution_summary": "ui.live_prereq_execution_summary",
    "live_prereq_wallet_summary": "ui.live_prereq_wallet_summary",
    "paper_run_journal_summary": "ui.paper_run_journal_summary",
    "daily_ops_summary": "ui.daily_ops_summary",
    "phase_readiness_summary": "ui.phase_readiness_summary",
    "readiness_evidence_summary": "ui.readiness_evidence_summary",
    "agent_review_summary": "ui.agent_review_summary",
    "predicted_vs_realized_summary": "ui.predicted_vs_realized_summary",
    "watch_only_vs_executed_summary": "ui.watch_only_vs_executed_summary",
    "execution_science_summary": "ui.execution_science_summary",
    "market_research_summary": "ui.market_research_summary",
    "calibration_health_summary": "ui.calibration_health_summary",
    "action_queue_summary": "ui.action_queue_summary",
    "cohort_history_summary": "ui.cohort_history_summary",
    "proposal_resolution_summary": "ui.proposal_resolution_summary",
    "read_model_catalog": "ui.read_model_catalog",
    "truth_source_checks": "ui.truth_source_checks",
}


def _safe_read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None
    return payload if isinstance(payload, dict) else None


def _read_json_result(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"payload": None, "exists": False, "error": None}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        return {"payload": None, "exists": True, "error": str(exc)}
    if not isinstance(payload, dict):
        return {"payload": None, "exists": True, "error": "json payload is not an object"}
    return {"payload": payload, "exists": True, "error": None}


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


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value in {None, ""}:
        return []
    try:
        payload = json.loads(str(value))
    except Exception:  # noqa: BLE001
        return []
    return payload if isinstance(payload, list) else []


def _derive_calibration_gate_overlay(
    *,
    calibration_freshness_status: Any = None,
    calibration_health_status: Any = None,
    threshold_probability_quality: Any = None,
    sample_count: Any = None,
) -> dict[str, Any]:
    freshness = _ensure_text(calibration_freshness_status)
    health = _ensure_text(calibration_health_status)
    threshold_quality = _ensure_text(threshold_probability_quality)
    sample_count_value = int(_coerce_float(sample_count) or 0)
    if not any([freshness, health, threshold_quality]) and sample_count in {None, ""}:
        return {
            "calibration_gate_status": "clear",
            "calibration_gate_reason_codes": [],
            "calibration_impacted_market": False,
        }
    assessment = build_weather_opportunity_assessment(
        market_id="gate_overlay_probe",
        token_id="gate_overlay_probe:YES",
        outcome="YES",
        reference_price=0.5,
        model_fair_value=0.5,
        accepting_orders=True,
        enable_order_book=True,
        threshold_bps=0,
        agent_review_status="passed",
        live_prereq_status="shadow_aligned",
        source_context={
            "calibration_freshness_status": freshness or None,
            "calibration_health_status": health or None,
            "threshold_probability_quality": threshold_quality or None,
            "sample_count": sample_count_value,
        },
    )
    return {
        "calibration_gate_status": assessment.calibration_gate_status,
        "calibration_gate_reason_codes": list(assessment.calibration_gate_reason_codes),
        "calibration_impacted_market": bool(assessment.calibration_impacted_market),
    }


def _apply_p8_overlay_defaults(row: dict[str, Any]) -> dict[str, Any]:
    if not row.get("calibration_gate_status"):
        gate_payload = _derive_calibration_gate_overlay(
            calibration_freshness_status=row.get("calibration_freshness_status"),
            calibration_health_status=row.get("calibration_health_status"),
            threshold_probability_quality=row.get("threshold_probability_quality"),
            sample_count=row.get("sample_count"),
        )
        row["calibration_gate_status"] = gate_payload["calibration_gate_status"]
        row["calibration_gate_reason_codes"] = gate_payload["calibration_gate_reason_codes"]
        row["calibration_impacted_market"] = gate_payload["calibration_impacted_market"]
    else:
        row["calibration_gate_reason_codes"] = _json_list(row.get("calibration_gate_reason_codes"))
        row["calibration_impacted_market"] = bool(row.get("calibration_impacted_market"))
    row.setdefault("capital_policy_id", None)
    row.setdefault("capital_policy_version", None)
    row.setdefault("capital_scaling_reason_codes", [])
    row.setdefault("preview_binding_limit_scope", None)
    row.setdefault("preview_binding_limit_key", None)
    return row


def _iso_now() -> datetime:
    return datetime.now(UTC)


def _normalize_timestamp(value: Any) -> datetime | None:
    if value in {None, ""}:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _sql_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    normalized = value.astimezone(UTC).replace(tzinfo=None)
    return normalized.isoformat(sep=" ", timespec="seconds")


def _json_array_text(values: list[Any]) -> str:
    return json.dumps(values, ensure_ascii=True, sort_keys=True)


def _build_weather_market_from_row(row: dict[str, Any]) -> WeatherMarket:
    return WeatherMarket(
        market_id=str(row["market_id"]),
        condition_id=str(row["condition_id"]),
        event_id=str(row["event_id"]) if row.get("event_id") is not None else None,
        slug=str(row["slug"]) if row.get("slug") is not None else None,
        title=str(row["title"]),
        description=str(row["description"]) if row.get("description") is not None else None,
        rules=str(row["rules"]) if row.get("rules") is not None else None,
        status=str(row["status"]),
        active=_normalize_bool(row.get("active")),
        closed=_normalize_bool(row.get("closed")),
        archived=_normalize_bool(row.get("archived")),
        accepting_orders=row.get("accepting_orders"),
        enable_order_book=row.get("enable_order_book"),
        tags=[str(item) for item in _json_list(row.get("tags_json"))],
        outcomes=[str(item) for item in _json_list(row.get("outcomes_json"))],
        token_ids=[str(item) for item in _json_list(row.get("token_ids_json"))],
        close_time=_normalize_timestamp(row.get("close_time")),
        end_date=_normalize_timestamp(row.get("end_date")),
        raw_market=_json_dict(row.get("raw_market_json")),
    )


def _build_station_metadata_from_row(row: dict[str, Any] | None) -> StationMetadata | None:
    if row is None or row.get("station_id") in {None, ""}:
        return None
    return StationMetadata(
        station_id=str(row["station_id"]),
        location_name=str(row.get("location_name") or ""),
        latitude=float(row.get("latitude") or 0.0),
        longitude=float(row.get("longitude") or 0.0),
        timezone=str(row.get("timezone") or ""),
        source=str(row.get("source") or "unknown"),
    )


def _build_weather_spec_from_row(row: dict[str, Any] | None) -> WeatherMarketSpecRecord | None:
    if row is None or row.get("market_id") in {None, ""}:
        return None
    observation_date = row.get("observation_date")
    if isinstance(observation_date, datetime):
        observation_date = observation_date.date()
    return WeatherMarketSpecRecord(
        market_id=str(row["market_id"]),
        condition_id=str(row["condition_id"]),
        location_name=str(row["location_name"]),
        station_id=str(row["station_id"]),
        latitude=float(row["latitude"]),
        longitude=float(row["longitude"]),
        timezone=str(row["timezone"]),
        observation_date=observation_date,
        observation_window_local=str(row["observation_window_local"]),
        metric=str(row["metric"]),
        unit=str(row["unit"]),
        bucket_min_value=_coerce_float(row.get("bucket_min_value")),
        bucket_max_value=_coerce_float(row.get("bucket_max_value")),
        authoritative_source=str(row["authoritative_source"]),
        fallback_sources=[str(item) for item in _json_list(row.get("fallback_sources_json"))],
        rounding_rule=str(row["rounding_rule"]),
        inclusive_bounds=_normalize_bool(row.get("inclusive_bounds")),
        spec_version=str(row["spec_version"]),
        parse_confidence=float(row["parse_confidence"]),
        risk_flags=[str(item) for item in _json_list(row.get("risk_flags_json"))],
    )


def _build_forecast_run_from_row(row: dict[str, Any] | None) -> ForecastRunRecord | None:
    if row is None or row.get("run_id") in {None, ""}:
        return None
    observation_date = row.get("observation_date")
    if isinstance(observation_date, datetime):
        observation_date = observation_date.date()
    return ForecastRunRecord(
        run_id=str(row["run_id"]),
        market_id=str(row["market_id"]),
        condition_id=str(row["condition_id"]),
        station_id=str(row["station_id"]),
        source=str(row["source"]),
        model_run=str(row["model_run"]),
        forecast_target_time=_normalize_timestamp(row.get("forecast_target_time")) or _iso_now(),
        observation_date=observation_date,
        metric=str(row["metric"]),
        latitude=float(row["latitude"]),
        longitude=float(row["longitude"]),
        timezone=str(row["timezone"]),
        spec_version=str(row["spec_version"]),
        cache_key=str(row["cache_key"]),
        source_trace=[str(item) for item in _json_list(row.get("source_trace_json"))],
        fallback_used=_normalize_bool(row.get("fallback_used")),
        from_cache=_normalize_bool(row.get("from_cache")),
        confidence=float(row["confidence"]),
        forecast_payload=_json_dict(row.get("forecast_payload_json")),
        raw_payload=_json_dict(row.get("raw_payload_json")),
    )


def _build_replay_from_row(row: dict[str, Any] | None) -> ForecastReplayRecord | None:
    if row is None or row.get("replay_id") in {None, ""}:
        return None
    return ForecastReplayRecord(
        replay_id=str(row["replay_id"]),
        market_id=str(row["market_id"]),
        condition_id=str(row["condition_id"]),
        station_id=str(row["station_id"]),
        source=str(row["source"]),
        model_run=str(row["model_run"]),
        forecast_target_time=_normalize_timestamp(row.get("forecast_target_time")) or _iso_now(),
        spec_version=str(row["spec_version"]),
        replay_key=str(row["replay_key"]),
        replay_reason=str(row["replay_reason"]),
        original_run_id=str(row["original_run_id"]),
        replayed_run_id=str(row["replayed_run_id"]),
        created_at=_normalize_timestamp(row.get("created_at")) or _iso_now(),
    )


def _build_replay_diff_from_row(row: dict[str, Any]) -> ForecastReplayDiffRecord:
    return ForecastReplayDiffRecord(
        diff_id=str(row["diff_id"]),
        replay_id=str(row["replay_id"]),
        entity_type=str(row["entity_type"]),
        entity_key=str(row["entity_key"]),
        original_entity_id=str(row["original_entity_id"]) if row.get("original_entity_id") is not None else None,
        replayed_entity_id=str(row["replayed_entity_id"]) if row.get("replayed_entity_id") is not None else None,
        status=str(row["status"]),
        diff_summary_json=_json_dict(row.get("diff_summary_json")),
        created_at=_normalize_timestamp(row.get("created_at")) or _iso_now(),
    )


def _build_fair_value_from_row(row: dict[str, Any]) -> WeatherFairValueRecord:
    return WeatherFairValueRecord(
        fair_value_id=str(row["fair_value_id"]),
        run_id=str(row["run_id"]),
        market_id=str(row["market_id"]),
        condition_id=str(row["condition_id"]),
        token_id=str(row["token_id"]),
        outcome=str(row["outcome"]),
        fair_value=float(row["fair_value"]),
        confidence=float(row["confidence"]),
    )


def _build_snapshot_from_row(row: dict[str, Any]) -> WatchOnlySnapshotRecord:
    return WatchOnlySnapshotRecord(
        snapshot_id=str(row["snapshot_id"]),
        fair_value_id=str(row["fair_value_id"]),
        run_id=str(row["run_id"]),
        market_id=str(row["market_id"]),
        condition_id=str(row["condition_id"]),
        token_id=str(row["token_id"]),
        outcome=str(row["outcome"]),
        reference_price=float(row["reference_price"]),
        fair_value=float(row["fair_value"]),
        edge_bps=int(row["edge_bps"]),
        threshold_bps=int(row["threshold_bps"]),
        decision=str(row["decision"]),
        side=str(row["side"]),
        rationale=str(row["rationale"]),
        pricing_context=_json_dict(row.get("pricing_context_json")),
    )


def _resolve_real_weather_smoke_report_path() -> Path:
    path = os.getenv("ASTERION_REAL_WEATHER_CHAIN_REPORT_PATH", "").strip()
    return Path(path) if path else DEFAULT_REAL_WEATHER_CHAIN_REPORT_PATH


def load_real_weather_smoke_report() -> dict[str, Any] | None:
    return _safe_read_json(_resolve_real_weather_smoke_report_path())


def _resolve_ui_lite_db_path() -> Path:
    path = os.getenv("ASTERION_UI_LITE_DB_PATH", "").strip()
    return Path(path) if path else DEFAULT_UI_LITE_DB_PATH


def _resolve_ui_replica_db_path() -> Path:
    path = os.getenv("ASTERION_UI_DB_REPLICA_PATH", "").strip()
    return Path(path) if path else DEFAULT_UI_REPLICA_DB_PATH


def _resolve_canonical_db_path() -> Path:
    path = os.getenv("ASTERION_DB_PATH", "").strip()
    return Path(path) if path else DEFAULT_CANONICAL_DB_PATH


def _resolve_real_weather_chain_db_path() -> Path:
    path = os.getenv("ASTERION_REAL_WEATHER_CHAIN_DB_PATH", "").strip()
    return Path(path) if path else DEFAULT_REAL_WEATHER_CHAIN_DB_PATH


def _resolve_readiness_report_path() -> Path:
    path = os.getenv("ASTERION_READINESS_REPORT_JSON_PATH", "").strip()
    return Path(path) if path else DEFAULT_P4_READINESS_REPORT_PATH


def _resolve_readiness_markdown_path() -> Path:
    path = os.getenv("ASTERION_READINESS_REPORT_MARKDOWN_PATH", "").strip()
    return Path(path) if path else DEFAULT_P4_READINESS_REPORT_MD_PATH


def _resolve_readiness_evidence_path() -> Path:
    path = os.getenv("ASTERION_READINESS_EVIDENCE_JSON_PATH", "").strip()
    return Path(path) if path else DEFAULT_P4_READINESS_EVIDENCE_PATH


def _resolve_controlled_live_capability_manifest_path() -> Path:
    path = os.getenv("ASTERION_CONTROLLED_LIVE_CAPABILITY_MANIFEST_PATH", "").strip()
    return Path(path) if path else DEFAULT_CONTROLLED_LIVE_CAPABILITY_MANIFEST_PATH


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame()


def _read_ui_table(db_path: Path, table: str) -> pd.DataFrame:
    return _read_ui_table_result(db_path, table)["frame"]


def _read_ui_table_result(db_path: Path, table: str) -> dict[str, Any]:
    if duckdb is None or not db_path.exists():
        return {"frame": _empty_df(), "error": None}
    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except Exception as exc:  # noqa: BLE001
        return {"frame": _empty_df(), "error": str(exc)}
    try:
        return {"frame": con.execute(f"SELECT * FROM {table}").df(), "error": None}
    except Exception as exc:  # noqa: BLE001
        return {"frame": _empty_df(), "error": str(exc)}
    finally:
        con.close()


def _read_agent_review_from_runtime(db_path: Path) -> pd.DataFrame:
    return _read_agent_review_from_runtime_result(db_path)["frame"]


def _read_agent_review_from_runtime_result(db_path: Path) -> dict[str, Any]:
    if duckdb is None or not db_path.exists():
        return {"frame": _empty_df(), "error": None}
    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except Exception as exc:  # noqa: BLE001
        return {"frame": _empty_df(), "error": str(exc)}
    try:
        frame = con.execute(
            """
            WITH latest_invocation AS (
                SELECT
                    invocation_id,
                    agent_type,
                    subject_type,
                    subject_id,
                    status,
                    model_provider,
                    model_name,
                    started_at,
                    ended_at
                FROM (
                    SELECT
                        invocation_id,
                        agent_type,
                        subject_type,
                        subject_id,
                        status,
                        model_provider,
                        model_name,
                        started_at,
                        ended_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY agent_type, subject_type, subject_id
                            ORDER BY COALESCE(ended_at, started_at) DESC, invocation_id DESC
                        ) AS rn
                    FROM agent.invocations
                )
                WHERE rn = 1
            ),
            latest_output AS (
                SELECT
                    invocation_id,
                    output_id,
                    verdict,
                    confidence,
                    summary,
                    human_review_required,
                    created_at
                FROM (
                    SELECT
                        invocation_id,
                        output_id,
                        verdict,
                        confidence,
                        summary,
                        human_review_required,
                        created_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY invocation_id
                            ORDER BY created_at DESC, output_id DESC
                        ) AS rn
                    FROM agent.outputs
                )
                WHERE rn = 1
            ),
            latest_review AS (
                SELECT
                    invocation_id,
                    review_id,
                    review_status,
                    reviewer_id,
                    reviewed_at
                FROM (
                    SELECT
                        invocation_id,
                        review_id,
                        review_status,
                        reviewer_id,
                        reviewed_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY invocation_id
                            ORDER BY reviewed_at DESC, review_id DESC
                        ) AS rn
                    FROM agent.reviews
                )
                WHERE rn = 1
            ),
            latest_evaluation AS (
                SELECT
                    invocation_id,
                    evaluation_id,
                    verification_method,
                    is_verified,
                    created_at
                FROM (
                    SELECT
                        invocation_id,
                        evaluation_id,
                        verification_method,
                        is_verified,
                        created_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY invocation_id
                            ORDER BY created_at DESC, evaluation_id DESC
                        ) AS rn
                    FROM agent.evaluations
                )
                WHERE rn = 1
            )
            SELECT
                inv.agent_type,
                inv.subject_type,
                inv.subject_id,
                inv.invocation_id,
                inv.status AS invocation_status,
                inv.model_provider,
                inv.model_name,
                output.output_id,
                output.verdict,
                output.confidence,
                output.summary,
                output.human_review_required,
                review.review_id,
                review.review_status,
                review.reviewer_id,
                evaluation.evaluation_id,
                evaluation.verification_method,
                evaluation.is_verified,
                COALESCE(output.created_at, review.reviewed_at, evaluation.created_at, inv.ended_at, inv.started_at) AS updated_at
            FROM latest_invocation inv
            LEFT JOIN latest_output output ON output.invocation_id = inv.invocation_id
            LEFT JOIN latest_review review ON review.invocation_id = inv.invocation_id
            LEFT JOIN latest_evaluation evaluation ON evaluation.invocation_id = inv.invocation_id
            """
        ).df()
        return {"frame": frame, "error": None}
    except Exception as exc:  # noqa: BLE001
        return {"frame": _empty_df(), "error": str(exc)}
    finally:
        con.close()


def _load_market_validation_overlays(db_path: Path) -> dict[str, dict[str, Any]]:
    if duckdb is None or not db_path.exists():
        return {}
    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except Exception:  # noqa: BLE001
        return {}
    try:
        markets = con.execute(
            """
            SELECT
                market_id,
                condition_id,
                event_id,
                slug,
                title,
                description,
                rules,
                status,
                active,
                closed,
                archived,
                accepting_orders,
                enable_order_book,
                tags_json,
                outcomes_json,
                token_ids_json,
                close_time,
                end_date,
                raw_market_json
            FROM weather.weather_markets
            """
        ).df()
        specs = con.execute(
            """
            SELECT *
            FROM (
                SELECT
                    market_id,
                    condition_id,
                    location_name,
                    station_id,
                    latitude,
                    longitude,
                    timezone,
                    observation_date,
                    observation_window_local,
                    metric,
                    unit,
                    bucket_min_value,
                    bucket_max_value,
                    authoritative_source,
                    fallback_sources_json,
                    rounding_rule,
                    inclusive_bounds,
                    spec_version,
                    parse_confidence,
                    risk_flags_json,
                    updated_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY market_id
                        ORDER BY updated_at DESC, spec_version DESC
                    ) AS rn
                FROM weather.weather_market_specs
            )
            WHERE rn = 1
            """
        ).df()
        mappings = con.execute(
            """
            SELECT *
            FROM (
                SELECT
                    market_id,
                    location_name,
                    station_id,
                    latitude,
                    longitude,
                    timezone,
                    source,
                    mapping_confidence,
                    updated_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY market_id
                        ORDER BY updated_at DESC, map_id DESC
                    ) AS rn
                FROM weather.weather_station_map
                WHERE market_id IS NOT NULL
            )
            WHERE rn = 1
            """
        ).df()
        replays = con.execute(
            """
            SELECT *
            FROM (
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
                    created_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY market_id
                        ORDER BY created_at DESC, replay_id DESC
                    ) AS rn
                FROM weather.weather_forecast_replays
            )
            WHERE rn = 1
            """
        ).df()
        replay_diffs = con.execute("SELECT * FROM weather.weather_forecast_replay_diffs").df()
        forecast_runs = con.execute(
            """
            SELECT
                run_id,
                market_id,
                condition_id,
                station_id,
                source,
                model_run,
                forecast_target_time,
                observation_date,
                metric,
                latitude,
                longitude,
                timezone,
                spec_version,
                cache_key,
                source_trace_json,
                fallback_used,
                from_cache,
                confidence,
                forecast_payload_json,
                raw_payload_json
            FROM weather.weather_forecast_runs
            """
        ).df()
        fair_values = con.execute(
            """
            SELECT fair_value_id, run_id, market_id, condition_id, token_id, outcome, fair_value, confidence
            FROM weather.weather_fair_values
            """
        ).df()
        snapshots = con.execute(
            """
            SELECT
                snapshot_id,
                fair_value_id,
                run_id,
                market_id,
                condition_id,
                token_id,
                outcome,
                reference_price,
                fair_value,
                edge_bps,
                threshold_bps,
                decision,
                side,
                rationale,
                pricing_context_json
            FROM weather.weather_watch_only_snapshots
            """
        ).df()
    except Exception:  # noqa: BLE001
        con.close()
        return {}
    finally:
        try:
            con.close()
        except Exception:  # noqa: BLE001
            pass

    spec_by_market = {str(row["market_id"]): row.to_dict() for _, row in specs.iterrows()}
    mapping_by_market = {str(row["market_id"]): row.to_dict() for _, row in mappings.iterrows()}
    replay_by_market = {str(row["market_id"]): row.to_dict() for _, row in replays.iterrows()}
    run_by_id = {str(row["run_id"]): row.to_dict() for _, row in forecast_runs.iterrows()}
    diffs_by_replay: dict[str, list[ForecastReplayDiffRecord]] = {}
    for _, row in replay_diffs.iterrows():
        record = _build_replay_diff_from_row(row.to_dict())
        diffs_by_replay.setdefault(record.replay_id, []).append(record)
    fair_values_by_run: dict[str, list[WeatherFairValueRecord]] = {}
    for _, row in fair_values.iterrows():
        record = _build_fair_value_from_row(row.to_dict())
        fair_values_by_run.setdefault(record.run_id, []).append(record)
    snapshots_by_run: dict[str, list[WatchOnlySnapshotRecord]] = {}
    for _, row in snapshots.iterrows():
        record = _build_snapshot_from_row(row.to_dict())
        snapshots_by_run.setdefault(record.run_id, []).append(record)

    overlays: dict[str, dict[str, Any]] = {}
    for _, market_row in markets.iterrows():
        market_payload = market_row.to_dict()
        market_id = str(market_payload["market_id"])
        spec_payload = spec_by_market.get(market_id)
        mapping_payload = mapping_by_market.get(market_id)
        try:
            rule2spec_result = validate_rule2spec_draft(
                parse_rule2spec_draft(_build_weather_market_from_row(market_payload)),
                current_spec=_build_weather_spec_from_row(spec_payload),
                station_metadata=_build_station_metadata_from_row(mapping_payload),
            )
        except Exception:  # noqa: BLE001
            rule2spec_result = None
        replay_payload = replay_by_market.get(market_id)
        replay_result = None
        if replay_payload is not None:
            try:
                replay_record = _build_replay_from_row(replay_payload)
                replayed_run = _build_forecast_run_from_row(run_by_id.get(replay_record.replayed_run_id))
                original_run = _build_forecast_run_from_row(run_by_id.get(replay_record.original_run_id))
                replay_result = validate_replay_quality(
                    replay_record,
                    spec=_build_weather_spec_from_row(spec_payload),
                    original_run=original_run,
                    replayed_run=replayed_run,
                    replay_diffs=diffs_by_replay.get(replay_record.replay_id, []),
                    fair_values=fair_values_by_run.get(replay_record.replayed_run_id, []),
                    watch_snapshots=snapshots_by_run.get(replay_record.replayed_run_id, []),
                )
            except Exception:  # noqa: BLE001
                replay_result = None
        overlays[market_id] = {
            "rule2spec_status": (
                "success"
                if rule2spec_result is not None and rule2spec_result.verdict == "pass"
                else "review_required"
                if rule2spec_result is not None and rule2spec_result.verdict == "review"
                else "blocked"
                if rule2spec_result is None
                else "blocked"
            ),
            "rule2spec_verdict": rule2spec_result.verdict if rule2spec_result is not None else "block",
            "rule2spec_summary": rule2spec_result.summary if rule2spec_result is not None else "deterministic rule2spec validation failed to evaluate",
            "data_qa_status": (
                "not_run"
                if replay_result is None
                else "success" if replay_result.verdict == "pass" else "review_required" if replay_result.verdict == "review" else "blocked"
            ),
            "data_qa_verdict": replay_result.verdict if replay_result is not None else None,
            "data_qa_summary": replay_result.summary if replay_result is not None else "no replay validation inputs available",
        }
    return overlays


def _derive_market_review_status(rule2spec_verdict: Any, data_qa_verdict: Any) -> str:
    verdicts = {str(item) for item in [rule2spec_verdict, data_qa_verdict] if item}
    if "block" in verdicts:
        return "review_required"
    if "review" in verdicts:
        return "review_required"
    if "pass" in verdicts:
        return "passed"
    return "no_agent_signal"


def load_market_validation_overlays() -> dict[str, dict[str, Any]]:
    overlays = _load_market_validation_overlays(_resolve_canonical_db_path())
    if overlays:
        return overlays
    return _load_market_validation_overlays(_resolve_real_weather_chain_db_path())


def _read_weather_market_rows_from_runtime(db_path: Path) -> pd.DataFrame:
    return _read_weather_market_rows_from_runtime_result(db_path)["frame"]


def _read_weather_market_rows_from_runtime_result(db_path: Path) -> dict[str, Any]:
    if duckdb is None or not db_path.exists():
        return {"frame": _empty_df(), "error": None}
    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except Exception as exc:  # noqa: BLE001
        return {"frame": _empty_df(), "error": str(exc)}
    try:
        frame = con.execute(
            """
            WITH latest_invocation AS (
                SELECT
                    invocation_id,
                    subject_id,
                    status
                FROM (
                    SELECT
                        invocation_id,
                        subject_id,
                        status,
                        started_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY subject_id
                            ORDER BY started_at DESC, invocation_id DESC
                        ) AS rn
                    FROM agent.invocations
                    WHERE agent_type = 'rule2spec'
                      AND subject_type = 'weather_market'
                )
                WHERE rn = 1
            ),
            latest_output AS (
                SELECT
                    invocation_id,
                    verdict,
                    summary
                FROM (
                    SELECT
                        invocation_id,
                        verdict,
                        summary,
                        created_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY invocation_id
                            ORDER BY created_at DESC, invocation_id DESC
                        ) AS rn
                    FROM agent.outputs
                )
                WHERE rn = 1
            ),
            latest_mapping AS (
                SELECT
                    market_id,
                    mapping_confidence,
                    mapping_method
                FROM (
                    SELECT
                        market_id,
                        mapping_confidence,
                        mapping_method,
                        updated_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY market_id
                            ORDER BY updated_at DESC, map_id DESC
                        ) AS rn
                    FROM weather.weather_station_map
                    WHERE market_id IS NOT NULL
                )
                WHERE rn = 1
            ),
            latest_health AS (
                SELECT
                    market_id,
                    source_freshness_status,
                    price_staleness_ms
                FROM (
                    SELECT
                        market_id,
                        source_freshness_status,
                        price_staleness_ms,
                        created_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY market_id
                            ORDER BY created_at DESC, snapshot_id DESC
                        ) AS rn
                    FROM weather.source_health_snapshots
                )
                WHERE rn = 1
            )
            SELECT
                m.market_id,
                m.title AS question,
                s.location_name,
                s.station_id,
                COALESCE(m.close_time, m.end_date) AS market_close_time,
                m.accepting_orders,
                CAST(NULL AS VARCHAR) AS best_side,
                CAST(NULL AS DOUBLE) AS market_price,
                CAST(NULL AS DOUBLE) AS fair_value,
                CAST(NULL AS DOUBLE) AS edge_bps,
                CAST(COALESCE(mp.mapping_confidence, 1.0) AS DOUBLE) AS mapping_confidence,
                COALESCE(health.source_freshness_status, 'missing') AS source_freshness_status,
                CAST(COALESCE(health.price_staleness_ms, 0) AS BIGINT) AS price_staleness_ms,
                CASE
                    WHEN COALESCE(m.accepting_orders, FALSE) = FALSE THEN 'blocked'
                    WHEN COALESCE(health.source_freshness_status, 'missing') IN ('missing', 'degraded') THEN 'review_required'
                    WHEN COALESCE(mp.mapping_confidence, 1.0) < 0.75 THEN 'review_required'
                    ELSE 'pass'
                END AS market_quality_status,
                CAST(CASE WHEN COALESCE(m.accepting_orders, FALSE) THEN 55.0 ELSE 25.0 END AS DOUBLE) AS liquidity_proxy,
                CAST(CASE
                    WHEN o.verdict = 'pass' THEN 85.0
                    WHEN o.verdict = 'review' THEN 60.0
                    WHEN i.status = 'failure' THEN 35.0
                    ELSE 50.0
                END AS DOUBLE) AS confidence_proxy,
                CASE
                    WHEN i.status = 'failure' THEN 'agent_failure'
                    WHEN o.verdict = 'review' THEN 'review_required'
                    WHEN i.status = 'success' THEN 'passed'
                    ELSE 'no_agent_signal'
                END AS agent_review_status,
                'not_started' AS live_prereq_status,
                'runtime_only' AS opportunity_bucket,
                CAST(CASE
                    WHEN COALESCE(m.accepting_orders, FALSE) THEN 32.0
                    ELSE 12.0
                END AS DOUBLE) AS opportunity_score,
                CASE
                    WHEN COALESCE(m.accepting_orders, FALSE) AND i.status = 'success' THEN 'review_required'
                    WHEN COALESCE(m.accepting_orders, FALSE) THEN 'review_required'
                    ELSE 'blocked'
                END AS actionability_status,
                i.status AS rule2spec_status,
                o.verdict AS rule2spec_verdict,
                o.summary AS rule2spec_summary,
                'not_run' AS data_qa_status,
                CAST(NULL AS VARCHAR) AS data_qa_verdict,
                'no canonical forecast replay inputs in smoke chain' AS data_qa_summary,
                'not_run' AS resolution_status,
                CAST(NULL AS VARCHAR) AS resolution_verdict,
                'no canonical resolution inputs in smoke chain' AS resolution_summary,
                s.authoritative_source,
                s.metric,
                s.bucket_min_value,
                s.bucket_max_value,
                s.observation_window_local,
                CAST(NULL AS VARCHAR) AS latest_run_source
            FROM weather.weather_markets AS m
            LEFT JOIN weather.weather_market_specs AS s
                ON s.market_id = m.market_id
            LEFT JOIN latest_mapping AS mp
                ON mp.market_id = m.market_id
            LEFT JOIN latest_health AS health
                ON health.market_id = m.market_id
            LEFT JOIN latest_invocation AS i
                ON i.subject_id = m.market_id
            LEFT JOIN latest_output AS o
                ON o.invocation_id = i.invocation_id
            WHERE COALESCE(m.active, FALSE) = TRUE
              AND COALESCE(m.closed, FALSE) = FALSE
              AND COALESCE(m.archived, FALSE) = FALSE
            ORDER BY COALESCE(m.close_time, m.end_date) ASC, m.market_id ASC
            """
        ).df()
        overlays = _load_market_validation_overlays(db_path)
        if not frame.empty:
            patched_rows: list[dict[str, Any]] = []
            for _, row in frame.iterrows():
                item = row.to_dict()
                overlay = overlays.get(str(item.get("market_id"))) or {}
                item.update(overlay)
                item["agent_review_status"] = _derive_market_review_status(
                    item.get("rule2spec_verdict"),
                    item.get("data_qa_verdict"),
                )
                patched_rows.append(_apply_p8_overlay_defaults(item))
            frame = pd.DataFrame(patched_rows)
        return {"frame": frame, "error": None}
    except Exception as exc:  # noqa: BLE001
        return {"frame": _empty_df(), "error": str(exc)}
    finally:
        con.close()


def _agent_rows_from_smoke_report(report: dict[str, Any] | None) -> pd.DataFrame:
    if not report:
        return _empty_df()
    discovery = report.get("market_discovery") or {}
    selected_markets = discovery.get("selected_markets") or []
    rows: list[dict[str, Any]] = []
    for item in selected_markets:
        market_id = item.get("market_id")
        for agent_type, status_key, verdict_key, summary_key in [
            ("rule2spec", "rule2spec_status", "rule2spec_verdict", "rule2spec_summary"),
            ("data_qa", "data_qa_status", "data_qa_verdict", "data_qa_summary"),
            ("resolution", "resolution_status", "resolution_verdict", "resolution_summary"),
        ]:
            status = item.get(status_key)
            verdict = item.get(verdict_key)
            summary = item.get(summary_key)
            if not any(value is not None for value in (status, verdict, summary)):
                continue
            rows.append(
                {
                    "agent_type": agent_type,
                    "subject_type": "weather_market",
                    "subject_id": market_id,
                    "invocation_status": status,
                    "verdict": verdict,
                    "confidence": None,
                    "summary": summary,
                    "human_review_required": None,
                    "updated_at": report.get("timestamp"),
                }
            )
    return pd.DataFrame(rows)


def _sort_desc(frame: pd.DataFrame, *columns: str) -> pd.DataFrame:
    keys = [column for column in columns if column in frame.columns]
    if not keys:
        return frame
    return frame.sort_values(by=keys, ascending=[False] * len(keys), kind="stable")


_ACTIONABILITY_ORDER = {
    "actionable": 0,
    "review_required": 1,
    "blocked": 2,
    "no_trade": 3,
}


def _ensure_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _coerce_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return False


def _derive_agent_review_status(item: dict[str, Any]) -> str:
    statuses = [item.get("rule2spec_status"), item.get("data_qa_status"), item.get("resolution_status")]
    if any(status in {"failure", "blocked"} for status in statuses):
        return "agent_failure"
    if any(status == "review_required" for status in statuses):
        return "review_required"
    verdicts = [item.get("rule2spec_verdict"), item.get("data_qa_verdict"), item.get("resolution_verdict")]
    if any(verdict in {"review", "block"} for verdict in verdicts):
        return "review_required"
    if any(status == "success" for status in statuses):
        return "passed"
    return "no_agent_signal"


def _sort_market_opportunities(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    sortable = frame.copy()
    actionability_values = (
        sortable["actionability_status"]
        if "actionability_status" in sortable.columns
        else pd.Series(index=sortable.index, dtype="object")
    )
    sortable["_actionability_rank"] = actionability_values.map(_ACTIONABILITY_ORDER).fillna(9)
    if "ranking_score" in sortable.columns:
        ranking_values = sortable["ranking_score"]
    else:
        ranking_values = sortable.get("opportunity_score", pd.Series(index=sortable.index, dtype="float64"))
    sortable["_opportunity_score_value"] = pd.to_numeric(ranking_values, errors="coerce").fillna(-1.0)
    edge_values = sortable["edge_bps"] if "edge_bps" in sortable.columns else pd.Series(index=sortable.index, dtype="float64")
    market_close_values = (
        sortable["market_close_time"]
        if "market_close_time" in sortable.columns
        else pd.Series(index=sortable.index, dtype="object")
    )
    sortable["_edge_bps_value"] = pd.to_numeric(edge_values, errors="coerce").fillna(-999999.0)
    sortable["_market_close_time_value"] = market_close_values.fillna("")
    sortable = sortable.sort_values(
        by=["_actionability_rank", "_opportunity_score_value", "_edge_bps_value", "_market_close_time_value"],
        ascending=[True, False, False, True],
        kind="stable",
    )
    return sortable.drop(columns=["_actionability_rank", "_opportunity_score_value", "_edge_bps_value", "_market_close_time_value"], errors="ignore")


def _build_opportunity_row(
    *,
    market_id: str,
    question: Any,
    location_name: Any,
    station_id: Any,
    market_close_time: Any,
    accepting_orders: bool,
    enable_order_book: bool | None,
    token_id: str,
    outcome: str,
    reference_price: float,
    model_fair_value: float,
    threshold_bps: int,
    agent_review_status: str,
    live_prereq_status: str,
    confidence_score: float | None,
    latest_run_source: Any,
    latest_forecast_target_time: Any,
    signal_created_at: Any,
    mapping_confidence: float | None = None,
    source_freshness_status: str = "missing",
    price_staleness_ms: int = 0,
    spread_bps: int | None = None,
    calibration_health_status: str = "lookup_missing",
    calibration_bias_quality: str = "lookup_missing",
    threshold_probability_quality: str = "lookup_missing",
    sample_count: int = 0,
    calibration_multiplier: float | None = None,
    calibration_reason_codes: list[str] | None = None,
    recommended_size: float | None = None,
    allocation_status: str | None = None,
    budget_impact: dict[str, Any] | None = None,
    allocation_decision_id: str | None = None,
    policy_id: str | None = None,
    policy_version: str | None = None,
    capital_policy_id: str | None = None,
    capital_policy_version: str | None = None,
    capital_scaling_reason_codes: list[str] | None = None,
) -> dict[str, Any]:
    assessment = build_weather_opportunity_assessment(
        market_id=market_id,
        token_id=token_id,
        outcome=outcome,
        reference_price=reference_price,
        model_fair_value=model_fair_value,
        accepting_orders=accepting_orders,
        enable_order_book=enable_order_book,
        threshold_bps=threshold_bps,
        agent_review_status=agent_review_status,
        live_prereq_status=live_prereq_status,
        confidence_score=confidence_score,
        mapping_confidence=mapping_confidence or 1.0,
        price_staleness_ms=price_staleness_ms,
        source_freshness_status=source_freshness_status,
        spread_bps=spread_bps,
        calibration_health_status=calibration_health_status,
        calibration_bias_quality=calibration_bias_quality,
        threshold_probability_quality=threshold_probability_quality,
        sample_count=sample_count,
        calibration_multiplier=calibration_multiplier,
        calibration_reason_codes=calibration_reason_codes,
        recommended_size=recommended_size,
        allocation_status=allocation_status,
        budget_impact=budget_impact,
        allocation_decision_id=allocation_decision_id,
        policy_id=policy_id,
        policy_version=policy_version,
        capital_policy_id=capital_policy_id,
        capital_policy_version=capital_policy_version,
        capital_scaling_reason_codes=capital_scaling_reason_codes,
        source_context={
            "calibration_health_status": calibration_health_status,
            "calibration_bias_quality": calibration_bias_quality,
            "threshold_probability_quality": threshold_probability_quality,
            "sample_count": sample_count,
            "calibration_multiplier": calibration_multiplier,
            "calibration_reason_codes": calibration_reason_codes,
            "latest_run_source": latest_run_source,
            "latest_forecast_target_time": latest_forecast_target_time,
            "mapping_confidence": mapping_confidence,
            "price_staleness_ms": price_staleness_ms,
            "signal_created_at": signal_created_at,
            "source_freshness_status": source_freshness_status,
            "spread_bps": spread_bps,
        },
    )
    best_side = derive_opportunity_side(assessment.edge_bps_executable)
    edge_magnitude = abs(int(assessment.edge_bps_executable))
    return {
        "market_id": market_id,
        "question": question,
        "location_name": location_name,
        "station_id": station_id,
        "market_close_time": market_close_time,
        "accepting_orders": accepting_orders,
        "best_side": best_side,
        "best_outcome": outcome,
        "best_decision": "TAKE" if best_side else "NO_TRADE",
        "market_price": assessment.reference_price,
        "fair_value": assessment.execution_adjusted_fair_value,
        "edge_bps": assessment.edge_bps_executable,
        "model_fair_value": assessment.model_fair_value,
        "execution_adjusted_fair_value": assessment.execution_adjusted_fair_value,
        "edge_bps_model": assessment.edge_bps_model,
        "edge_bps_executable": assessment.edge_bps_executable,
        "fees_bps": assessment.fees_bps,
        "slippage_bps": assessment.slippage_bps,
        "fill_probability": assessment.fill_probability,
        "depth_proxy": assessment.depth_proxy,
        "calibration_health_status": assessment.calibration_health_status,
        "calibration_bias_quality": assessment.calibration_bias_quality,
        "threshold_probability_quality": assessment.threshold_probability_quality,
        "sample_count": assessment.sample_count,
        "uncertainty_multiplier": assessment.uncertainty_multiplier,
        "uncertainty_penalty_bps": assessment.uncertainty_penalty_bps,
        "ranking_penalty_reasons": assessment.ranking_penalty_reasons,
        "mapping_confidence": assessment.assessment_context_json.get("mapping_confidence"),
        "source_freshness_status": assessment.assessment_context_json.get("source_freshness_status"),
        "price_staleness_ms": assessment.assessment_context_json.get("price_staleness_ms"),
        "market_quality_status": assessment.assessment_context_json.get("market_quality_status"),
        "calibration_gate_status": assessment.calibration_gate_status,
        "calibration_gate_reason_codes": assessment.calibration_gate_reason_codes,
        "calibration_impacted_market": assessment.calibration_impacted_market,
        "liquidity_proxy": assessment.depth_proxy * 100.0,
        "liquidity_penalty_bps": assessment.liquidity_penalty_bps,
        "confidence_score": assessment.confidence_score,
        "confidence_proxy": assessment.confidence_score,
        "ops_readiness_score": assessment.ops_readiness_score,
        "expected_value_score": assessment.expected_value_score,
        "expected_pnl_score": assessment.expected_pnl_score,
        "expected_dollar_pnl": assessment.expected_dollar_pnl,
        "capture_probability": assessment.capture_probability,
        "risk_penalty": assessment.risk_penalty,
        "capital_efficiency": assessment.capital_efficiency,
        "feedback_penalty": assessment.feedback_penalty,
        "feedback_status": assessment.feedback_status,
        "cohort_prior_version": assessment.cohort_prior_version,
        "base_ranking_score": assessment.base_ranking_score,
        "deployable_expected_pnl": assessment.deployable_expected_pnl,
        "deployable_notional": assessment.deployable_notional,
        "max_deployable_size": assessment.max_deployable_size,
        "capital_scarcity_penalty": assessment.capital_scarcity_penalty,
        "concentration_penalty": assessment.concentration_penalty,
        "pre_budget_deployable_size": assessment.pre_budget_deployable_size,
        "pre_budget_deployable_notional": assessment.pre_budget_deployable_notional,
        "pre_budget_deployable_expected_pnl": assessment.pre_budget_deployable_expected_pnl,
        "preview_binding_limit_scope": assessment.assessment_context_json.get("preview_binding_limit_scope"),
        "preview_binding_limit_key": assessment.assessment_context_json.get("preview_binding_limit_key"),
        "requested_size": assessment.assessment_context_json.get("requested_size"),
        "requested_notional": assessment.assessment_context_json.get("requested_notional"),
        "rerank_position": assessment.rerank_position,
        "rerank_reason_codes": assessment.rerank_reason_codes,
        "recommended_size": assessment.recommended_size,
        "allocation_status": assessment.allocation_status,
        "budget_impact": assessment.budget_impact,
        "capital_policy_id": assessment.capital_policy_id,
        "capital_policy_version": assessment.capital_policy_version,
        "capital_scaling_reason_codes": assessment.capital_scaling_reason_codes,
        "regime_bucket": assessment.regime_bucket,
        "allocation_decision_id": allocation_decision_id,
        "ranking_score": assessment.ranking_score,
        "execution_prior_key": assessment.execution_prior_key,
        "why_ranked_json": assessment.why_ranked_json,
        "agent_review_status": agent_review_status,
        "live_prereq_status": live_prereq_status,
        "opportunity_bucket": "high_edge" if edge_magnitude >= 1500 else "medium_edge" if edge_magnitude >= 750 else "low_edge" if edge_magnitude > 0 else "negative_edge",
        "opportunity_score": assessment.ranking_score,
        "actionability_status": assessment.actionability_status,
        "latest_run_source": latest_run_source,
        "latest_forecast_target_time": latest_forecast_target_time,
        "threshold_bps": threshold_bps,
        "signal_created_at": signal_created_at,
        "source_badge": "fallback",
        "source_truth_status": "fallback",
        "is_degraded_source": True,
        "primary_score_label": "ranking_score",
    }


def _derive_market_opportunities_from_report(report: dict[str, Any] | None) -> pd.DataFrame:
    if not report:
        return _empty_df()
    discovery = report.get("market_discovery") or {}
    selected_markets = discovery.get("selected_markets") or []
    pricing_by_market = {
        item.get("market_id"): item for item in (report.get("pricing_engine") or {}).get("markets") or []
    }
    signal_by_market = {
        item.get("market_id"): item for item in (report.get("opportunity_discovery") or {}).get("markets") or []
    }
    rows: list[dict[str, Any]] = []
    for item in selected_markets:
        market_id = item.get("market_id")
        pricing = pricing_by_market.get(market_id) or {}
        signals = (signal_by_market.get(market_id) or {}).get("signals") or []
        market_prices = pricing.get("market_prices") or {}
        fair_value_map = {
            fair_row.get("outcome"): _coerce_float(fair_row.get("fair_value"))
            for fair_row in (pricing.get("fair_values") or [])
        }
        best_signal = None
        if signals:
            best_signal = sorted(
                signals,
                key=lambda signal: (
                    0 if _ensure_text(signal.get("decision")) == "TAKE" else 1,
                    -float(signal.get("ranking_score") or 0.0),
                    -float(signal.get("edge_bps") or 0.0),
                ),
            )[0]
        if best_signal is not None:
            signal_outcome = best_signal.get("outcome")
            if not best_signal.get("token_id") and signal_outcome:
                best_signal["token_id"] = f"{market_id}:{signal_outcome}"
            signal_market_price = _coerce_float(best_signal.get("reference_price"))
            signal_fair_value = _coerce_float(best_signal.get("model_fair_value")) or _coerce_float(best_signal.get("fair_value"))
            if signal_market_price is None and signal_outcome in market_prices:
                best_signal["reference_price"] = market_prices.get(signal_outcome)
            if signal_fair_value is None and signal_outcome in fair_value_map:
                best_signal["model_fair_value"] = fair_value_map.get(signal_outcome)
            signal_market_price = _coerce_float(best_signal.get("reference_price"))
            signal_fair_value = _coerce_float(best_signal.get("model_fair_value")) or _coerce_float(best_signal.get("fair_value"))
            if best_signal.get("edge_bps") is None and signal_market_price is not None and signal_fair_value is not None:
                best_signal["edge_bps"] = round((signal_fair_value - signal_market_price) * 10000.0, 2)
            if not best_signal.get("side") and _coerce_float(best_signal.get("edge_bps")) not in {None, 0.0}:
                best_signal["side"] = "BUY" if float(best_signal["edge_bps"]) > 0 else None
        if best_signal is None:
            fair_values = pricing.get("fair_values") or []
            derived_signals: list[dict[str, Any]] = []
            for fair_row in fair_values:
                outcome = fair_row.get("outcome")
                fair_value = _coerce_float(fair_row.get("fair_value"))
                market_price = _coerce_float(market_prices.get(outcome))
                if fair_value is None or market_price is None:
                    continue
                edge_bps = round((fair_value - market_price) * 10000.0, 2)
                derived_signals.append(
                    {
                        "token_id": f"{market_id}:{outcome}",
                        "outcome": outcome,
                        "fair_value": fair_value,
                        "reference_price": market_price,
                        "edge_bps": edge_bps,
                        "side": "BUY" if edge_bps > 0 else None,
                        "decision": "TAKE" if edge_bps > 0 else "NO_TRADE",
                    }
                )
            if derived_signals:
                best_signal = sorted(
                    derived_signals,
                    key=lambda signal: (
                        0 if _ensure_text(signal.get("decision")) == "TAKE" else 1,
                        -float(signal.get("ranking_score") or 0.0),
                        -float(signal.get("edge_bps") or 0.0),
                    ),
                )[0]
        market_price = _coerce_float((best_signal or {}).get("reference_price"))
        fair_value = _coerce_float((best_signal or {}).get("model_fair_value")) or _coerce_float((best_signal or {}).get("fair_value"))
        token_id = _ensure_text((best_signal or {}).get("token_id"))
        outcome = _ensure_text((best_signal or {}).get("outcome")) or "YES"
        agent_review_status = _derive_agent_review_status(item)
        live_prereq_status = "not_started"
        accepting_orders = _normalize_bool(item.get("accepting_orders"))
        if market_price is None or fair_value is None or not token_id:
            rows.append(
                _apply_p8_overlay_defaults(
                    {
                        "market_id": market_id,
                        "question": item.get("question"),
                        "location_name": item.get("location_name"),
                        "station_id": item.get("station_id"),
                        "market_close_time": item.get("close_time"),
                        "accepting_orders": accepting_orders,
                        "best_side": None,
                        "best_outcome": outcome,
                        "best_decision": "NO_TRADE",
                        "market_price": market_price,
                        "fair_value": None,
                        "edge_bps": None,
                        "model_fair_value": None,
                        "execution_adjusted_fair_value": None,
                        "edge_bps_model": None,
                        "edge_bps_executable": None,
                        "fees_bps": None,
                        "slippage_bps": None,
                        "fill_probability": None,
                        "depth_proxy": None,
                        "calibration_freshness_status": _ensure_text((best_signal or {}).get("calibration_freshness_status")) or None,
                        "calibration_health_status": _ensure_text((best_signal or {}).get("calibration_health_status")) or None,
                        "threshold_probability_quality": _ensure_text((best_signal or {}).get("threshold_probability_quality")) or None,
                        "sample_count": int(_coerce_float((best_signal or {}).get("sample_count")) or 0) if (best_signal or {}).get("sample_count") is not None else None,
                        "uncertainty_multiplier": 0.0,
                        "uncertainty_penalty_bps": 0,
                        "ranking_penalty_reasons": (best_signal or {}).get("calibration_reason_codes")
                        if isinstance((best_signal or {}).get("calibration_reason_codes"), list)
                        else [],
                        "mapping_confidence": _coerce_float((best_signal or {}).get("mapping_confidence")) or 1.0,
                        "source_freshness_status": _ensure_text((best_signal or {}).get("source_freshness_status")) or "missing",
                        "price_staleness_ms": int(_coerce_float((best_signal or {}).get("price_staleness_ms")) or 0),
                        "market_quality_status": _ensure_text((best_signal or {}).get("market_quality_status")) or "review_required",
                        "liquidity_proxy": 25.0 if not accepting_orders else 55.0,
                        "liquidity_penalty_bps": None,
                        "confidence_score": 85.0 if agent_review_status == "passed" else 60.0 if agent_review_status == "review_required" else 35.0 if agent_review_status == "agent_failure" else 50.0,
                        "confidence_proxy": 85.0 if agent_review_status == "passed" else 60.0 if agent_review_status == "review_required" else 35.0 if agent_review_status == "agent_failure" else 50.0,
                        "ops_readiness_score": 0.0,
                        "expected_value_score": 0.0,
                        "expected_pnl_score": 0.0,
                        "expected_dollar_pnl": 0.0,
                        "capture_probability": 0.0,
                        "risk_penalty": 0.0,
                        "capital_efficiency": 0.0,
                        "feedback_penalty": 0.0,
                        "feedback_status": "heuristic_only",
                        "cohort_prior_version": None,
                        "ranking_score": 0.0,
                        "execution_prior_key": None,
                        "why_ranked_json": {},
                        "agent_review_status": agent_review_status,
                        "live_prereq_status": live_prereq_status,
                        "opportunity_bucket": "negative_edge",
                        "opportunity_score": 0.0,
                        "actionability_status": "review_required" if agent_review_status != "passed" else "no_trade",
                        "latest_run_source": (report.get("forecast_service") or {}).get("source_used"),
                        "latest_forecast_target_time": None,
                        "threshold_bps": int(_coerce_float((best_signal or {}).get("threshold_bps")) or 0),
                        "signal_created_at": report.get("timestamp"),
                    }
                )
            )
            continue
        confidence_score = 85.0 if agent_review_status == "passed" else 60.0 if agent_review_status == "review_required" else 35.0 if agent_review_status == "agent_failure" else 50.0
        rows.append(
            _build_opportunity_row(
                market_id=str(market_id),
                question=item.get("question"),
                location_name=item.get("location_name"),
                station_id=item.get("station_id"),
                market_close_time=item.get("close_time"),
                accepting_orders=accepting_orders,
                enable_order_book=_normalize_bool(item.get("enable_order_book")) if item.get("enable_order_book") is not None else None,
                token_id=token_id,
                outcome=outcome,
                reference_price=market_price,
                model_fair_value=fair_value,
                threshold_bps=int(_coerce_float((best_signal or {}).get("threshold_bps")) or 0),
                agent_review_status=agent_review_status,
                live_prereq_status=live_prereq_status,
                confidence_score=confidence_score,
                latest_run_source=(report.get("forecast_service") or {}).get("source_used"),
                latest_forecast_target_time=None,
                signal_created_at=report.get("timestamp"),
                mapping_confidence=_coerce_float((best_signal or {}).get("mapping_confidence")) or 1.0,
                source_freshness_status=_ensure_text((best_signal or {}).get("source_freshness_status")) or "missing",
                price_staleness_ms=int(_coerce_float((best_signal or {}).get("price_staleness_ms")) or 0),
                spread_bps=int(_coerce_float((best_signal or {}).get("spread_bps")) or 0) or None,
                calibration_health_status=_ensure_text((best_signal or {}).get("calibration_health_status")) or "lookup_missing",
                sample_count=int(_coerce_float((best_signal or {}).get("sample_count")) or 0),
                calibration_multiplier=_coerce_float((best_signal or {}).get("calibration_multiplier")),
                calibration_reason_codes=(best_signal or {}).get("calibration_reason_codes")
                if isinstance((best_signal or {}).get("calibration_reason_codes"), list)
                else None,
            )
        )
    return _sort_market_opportunities(pd.DataFrame(rows))


def load_ui_lite_snapshot() -> dict[str, Any]:
    db_path = _resolve_ui_lite_db_path()
    table_results = {name: _read_ui_table_result(db_path, table) for name, table in UI_TABLES.items()}
    tables = {name: result["frame"] for name, result in table_results.items()}
    table_errors = {name: result["error"] for name, result in table_results.items() if result["error"]}
    return {
        "db_path": str(db_path),
        "exists": db_path.exists(),
        "tables": tables,
        "table_row_counts": {name: int(len(frame.index)) for name, frame in tables.items()},
        "table_errors": table_errors,
        "read_error": next(iter(table_errors.values()), None),
    }


def load_readiness_summary() -> dict[str, Any]:
    snapshot = load_ui_lite_snapshot()
    frame = snapshot["tables"]["phase_readiness_summary"]
    report_path = _resolve_readiness_report_path()
    report_result = _read_json_result(report_path)
    report = report_result["payload"]
    manifest_path = _resolve_controlled_live_capability_manifest_path()
    manifest_result = _read_json_result(manifest_path)
    manifest = manifest_result["payload"]

    target = None
    go_decision = None
    decision_reason = None
    updated_at = None
    capability_boundary_summary = None
    capability_manifest_status = None
    if report:
        target = report.get("target")
        go_decision = report.get("go_decision")
        decision_reason = report.get("decision_reason")
        updated_at = report.get("evaluated_at") or report.get("generated_at")
        capability_boundary_summary = report.get("capability_boundary_summary")
        capability_manifest_status = report.get("capability_manifest_status")
    if manifest and capability_boundary_summary is None:
        capability_boundary_summary = {
            "manual_only": manifest.get("controlled_live_mode") == "manual_only",
            "default_off": bool(manifest.get("default_off")),
            "approve_usdc_only": manifest.get("allowed_tx_kinds") == ["approve_usdc"],
            "shadow_submitter_only": manifest.get("submitter_capability") == "shadow_only",
            "constrained_real_submit_enabled": manifest.get("submitter_capability") == "constrained_real_submit",
            "manifest_status": manifest.get("manifest_status"),
        }
    if manifest and not capability_manifest_status:
        capability_manifest_status = manifest.get("manifest_status")

    failed_gate_names: list[str] = []
    if not frame.empty:
        gate_name_column = "gate_name" if "gate_name" in frame.columns else None
        status_column = "status" if "status" in frame.columns else None
        if gate_name_column and status_column:
            failed_gate_names = [
                str(row[gate_name_column])
                for _, row in frame.iterrows()
                if str(row[status_column]).upper() not in {"PASS", "OK", "GO"}
            ]

    return {
        "report": report,
        "report_path": str(report_path),
        "report_exists": report_path.exists(),
        "report_markdown_path": str(_resolve_readiness_markdown_path()),
        "phase_table": frame,
        "target": target,
        "go_decision": go_decision,
        "decision_reason": decision_reason,
        "updated_at": updated_at,
        "capability_boundary_summary": capability_boundary_summary or {},
        "capability_manifest_path": str(manifest_path),
        "capability_manifest_exists": manifest_path.exists(),
        "capability_manifest_status": capability_manifest_status or (manifest or {}).get("manifest_status"),
        "failed_gate_names": failed_gate_names,
        "source": "ui_lite+json" if (snapshot["exists"] or report_path.exists()) else "missing",
        "read_error": snapshot.get("read_error") or report_result["error"] or manifest_result["error"],
    }


def load_readiness_evidence_bundle() -> dict[str, Any]:
    snapshot = load_ui_lite_snapshot()
    frame = snapshot["tables"]["readiness_evidence_summary"]
    evidence_path = _resolve_readiness_evidence_path()
    evidence_result = _read_json_result(evidence_path)
    payload = evidence_result["payload"] or {}
    if not frame.empty:
        row = frame.iloc[0].to_dict()
        return {
            "source": "ui_lite",
            "exists": True,
            "path": str(evidence_path),
            "generated_at": row.get("generated_at"),
            "go_decision": row.get("go_decision"),
            "decision_reason": row.get("decision_reason"),
            "capability_manifest_status": row.get("capability_manifest_status"),
            "capability_boundary_summary": _json_dict(row.get("capability_boundary_summary_json")),
            "dependency_statuses": _json_dict(row.get("dependency_statuses_json")),
            "artifact_freshness": _json_dict(row.get("artifact_freshness_json")),
            "latest_verification_summary": _json_dict(row.get("latest_verification_summary_json")),
            "stale_dependencies": _json_list(row.get("stale_dependencies_json")),
            "blockers": _json_list(row.get("blockers_json")),
            "warnings": _json_list(row.get("warnings_json")),
            "evidence_paths": _json_dict(row.get("evidence_paths_json")),
            "frame": frame,
            "read_error": snapshot.get("read_error") or evidence_result["error"],
        }
    return {
        "source": "json" if evidence_result["exists"] else "missing",
        "exists": bool(evidence_result["exists"] and payload),
        "path": str(evidence_path),
        "generated_at": payload.get("generated_at"),
        "go_decision": payload.get("go_decision"),
        "decision_reason": payload.get("decision_reason"),
        "capability_manifest_status": payload.get("capability_manifest_status"),
        "capability_boundary_summary": dict(payload.get("capability_boundary_summary") or {}),
        "dependency_statuses": dict(payload.get("dependency_statuses") or {}),
        "artifact_freshness": dict(payload.get("artifact_freshness") or {}),
        "latest_verification_summary": dict(payload.get("latest_verification_summary") or {}),
        "stale_dependencies": list(payload.get("stale_dependencies") or []),
        "blockers": list(payload.get("blockers") or []),
        "warnings": list(payload.get("warnings") or []),
        "evidence_paths": dict(payload.get("evidence_paths") or {}),
        "frame": frame,
        "read_error": snapshot.get("read_error") or evidence_result["error"],
    }


def load_predicted_vs_realized_data() -> dict[str, Any]:
    snapshot = load_ui_lite_snapshot()
    frame = annotate_frame_with_source_truth(
        _sort_desc(snapshot["tables"]["predicted_vs_realized_summary"], "latest_fill_at", "latest_resolution_at"),
        source_origin="ui_lite",
        derived=True,
        freshness_column="forecast_freshness",
    )
    return {
        "source": "ui_lite" if not frame.empty else ("ui_lite" if snapshot["exists"] else "missing"),
        "frame": frame,
        "read_error": snapshot.get("read_error"),
    }


def load_execution_console_data() -> dict[str, pd.DataFrame]:
    from ui.loaders.execution_loader import load_execution_console_data as load_execution_console_data_impl

    return load_execution_console_data_impl()


def load_wallet_readiness_data() -> pd.DataFrame:
    snapshot = load_ui_lite_snapshot()
    return _sort_desc(snapshot["tables"]["live_prereq_wallet_summary"], "latest_allowance_observed_at", "latest_chain_tx_created_at")


def load_market_watch_data() -> dict[str, Any]:
    snapshot = load_ui_lite_snapshot()
    return {
        "market_watch": _sort_desc(snapshot["tables"]["market_watch_summary"], "snapshot_created_at", "forecast_created_at"),
        "weather_smoke_report": load_real_weather_smoke_report(),
    }


def load_market_opportunity_data() -> dict[str, Any]:
    snapshot = load_ui_lite_snapshot()
    frame = annotate_frame_with_source_truth(
        _sort_market_opportunities(snapshot["tables"]["market_opportunity_summary"]),
        source_origin="ui_lite",
        derived=False,
        freshness_column="source_freshness_status",
    )
    if not frame.empty:
        return {"source": "ui_lite", "frame": frame, "read_error": snapshot.get("read_error")}
    report = load_real_weather_smoke_report()
    report_frame = annotate_frame_with_source_truth(
        _derive_market_opportunities_from_report(report),
        source_origin="smoke_report",
        derived=False,
        freshness_column="source_freshness_status",
    )
    if not report_frame.empty:
        return {"source": "smoke_report", "frame": report_frame, "read_error": snapshot.get("read_error")}
    if report:
        chain_status = _ensure_text(report.get("chain_status"))
        refresh_state = _ensure_text(report.get("refresh_state"))
        if chain_status not in {"initializing", "unknown"} and refresh_state != "initializing":
            return {"source": "smoke_report", "frame": report_frame, "read_error": snapshot.get("read_error")}
    runtime_result = _read_weather_market_rows_from_runtime_result(_resolve_real_weather_chain_db_path())
    runtime_frame = annotate_frame_with_source_truth(
        _sort_market_opportunities(runtime_result["frame"]),
        source_origin="weather_smoke_db",
        derived=False,
        freshness_column="source_freshness_status",
    )
    return {"source": "weather_smoke_db", "frame": runtime_frame, "read_error": snapshot.get("read_error") or runtime_result["error"]}


def _read_proposal_resolution_summary_result(db_path: Path) -> dict[str, Any]:
    if duckdb is None or not db_path.exists():
        return {"frame": _empty_df(), "error": None}
    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except Exception as exc:  # noqa: BLE001
        return {"frame": _empty_df(), "error": str(exc)}
    try:
        frame = con.execute(
            """
            WITH latest_verification AS (
                SELECT *
                FROM (
                    SELECT
                        proposal_id,
                        verification_id,
                        expected_outcome,
                        is_correct,
                        confidence,
                        evidence_package,
                        created_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY proposal_id
                            ORDER BY created_at DESC, verification_id DESC
                        ) AS rn
                    FROM resolution.settlement_verifications
                )
                WHERE rn = 1
            ),
            latest_redeem AS (
                SELECT *
                FROM (
                    SELECT
                        proposal_id,
                        suggestion_id,
                        decision,
                        reason,
                        human_review_required,
                        created_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY proposal_id
                            ORDER BY created_at DESC, suggestion_id DESC
                        ) AS rn
                    FROM resolution.redeem_readiness_suggestions
                )
                WHERE rn = 1
            ),
            latest_continuity AS (
                SELECT *
                FROM (
                    SELECT
                        check_id,
                        status,
                        from_block,
                        to_block,
                        created_at,
                        ROW_NUMBER() OVER (
                            ORDER BY created_at DESC, to_block DESC, check_id DESC
                        ) AS rn
                    FROM resolution.watcher_continuity_checks
                )
                WHERE rn = 1
            ),
            latest_invocation AS (
                SELECT *
                FROM (
                    SELECT
                        invocation_id,
                        subject_id,
                        status,
                        ended_at,
                        started_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY subject_id
                            ORDER BY COALESCE(ended_at, started_at) DESC, invocation_id DESC
                        ) AS rn
                    FROM agent.invocations
                    WHERE agent_type = 'resolution'
                      AND subject_type = 'uma_proposal'
                )
                WHERE rn = 1
            ),
            latest_output AS (
                SELECT *
                FROM (
                    SELECT
                        invocation_id,
                        verdict,
                        confidence,
                        summary,
                        human_review_required,
                        structured_output_json,
                        created_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY invocation_id
                            ORDER BY created_at DESC, output_id DESC
                        ) AS rn
                    FROM agent.outputs
                )
                WHERE rn = 1
            ),
            latest_agent_review AS (
                SELECT *
                FROM (
                    SELECT
                        invocation_id,
                        review_payload_json,
                        reviewed_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY invocation_id
                            ORDER BY reviewed_at DESC, review_id DESC
                        ) AS rn
                    FROM agent.reviews
                )
                WHERE rn = 1
            ),
            latest_operator_review AS (
                SELECT *
                FROM (
                    SELECT
                        proposal_id,
                        review_decision_id,
                        invocation_id,
                        suggestion_id,
                        decision_status,
                        operator_action,
                        reason,
                        actor,
                        created_at,
                        updated_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY proposal_id
                            ORDER BY updated_at DESC, review_decision_id DESC
                        ) AS rn
                    FROM resolution.operator_review_decisions
                )
                WHERE rn = 1
            )
            SELECT
                p.proposal_id,
                p.market_id,
                p.condition_id,
                p.status AS proposal_status,
                p.proposed_outcome,
                v.verification_id,
                v.expected_outcome,
                v.is_correct,
                v.confidence AS verification_confidence,
                json_extract_string(try_cast(v.evidence_package AS JSON), '$.evidence_package_id') AS evidence_package_id,
                redeem.suggestion_id,
                redeem.decision AS redeem_decision,
                redeem.reason AS redeem_reason,
                redeem.human_review_required,
                continuity.check_id AS latest_continuity_check_id,
                continuity.status AS latest_continuity_status,
                continuity.from_block AS latest_continuity_from_block,
                continuity.to_block AS latest_continuity_to_block,
                inv.invocation_id AS latest_agent_invocation_id,
                inv.status AS latest_agent_invocation_status,
                out.verdict AS latest_agent_verdict,
                out.confidence AS latest_agent_confidence,
                out.summary AS latest_agent_summary,
                json_extract_string(try_cast(rev.review_payload_json AS JSON), '$.recommended_operator_action') AS latest_recommended_operator_action,
                TRY_CAST(json_extract_string(try_cast(rev.review_payload_json AS JSON), '$.settlement_risk_score') AS DOUBLE) AS latest_settlement_risk_score,
                operator_review.decision_status AS latest_operator_review_status,
                operator_review.operator_action AS latest_operator_action,
                operator_review.reason AS latest_operator_review_reason,
                operator_review.actor AS latest_operator_review_actor,
                operator_review.updated_at AS latest_operator_review_updated_at,
                CASE
                    WHEN operator_review.decision_status = 'accepted' AND operator_review.operator_action = 'ready_for_redeem_review' THEN 'ready_for_redeem_review'
                    WHEN operator_review.decision_status = 'accepted' AND operator_review.operator_action IN ('hold_redeem', 'manual_review', 'consider_dispute') THEN 'blocked_by_operator_review'
                    WHEN operator_review.decision_status = 'deferred' THEN 'pending_operator_review'
                    WHEN operator_review.decision_status = 'rejected' THEN redeem.decision
                    WHEN json_extract_string(try_cast(rev.review_payload_json AS JSON), '$.recommended_operator_action') IN ('hold_redeem', 'manual_review', 'consider_dispute') THEN 'pending_operator_review'
                    ELSE redeem.decision
                END AS effective_redeem_status
            FROM resolution.uma_proposals p
            LEFT JOIN latest_verification v ON v.proposal_id = p.proposal_id
            LEFT JOIN latest_redeem redeem ON redeem.proposal_id = p.proposal_id
            LEFT JOIN latest_continuity continuity ON TRUE
            LEFT JOIN latest_invocation inv ON inv.subject_id = p.proposal_id
            LEFT JOIN latest_output out ON out.invocation_id = inv.invocation_id
            LEFT JOIN latest_agent_review rev ON rev.invocation_id = inv.invocation_id
            LEFT JOIN latest_operator_review operator_review ON operator_review.proposal_id = p.proposal_id
            ORDER BY p.proposal_block_number DESC, p.proposal_id DESC
            """
        ).df()
        return {"frame": frame, "error": None}
    except Exception as exc:  # noqa: BLE001
        return {"frame": _empty_df(), "error": str(exc)}
    finally:
        con.close()


def load_resolution_review_data() -> dict[str, Any]:
    snapshot = load_ui_lite_snapshot()
    frame = _sort_desc(snapshot["tables"]["proposal_resolution_summary"], "latest_operator_review_updated_at", "latest_agent_invocation_id")
    if not frame.empty:
        return {"source": "ui_lite", "frame": frame, "read_error": snapshot.get("read_error")}
    runtime_result = _read_proposal_resolution_summary_result(_resolve_canonical_db_path())
    runtime_frame = _sort_desc(runtime_result["frame"], "latest_operator_review_updated_at", "latest_agent_invocation_id")
    return {"source": "runtime_db", "frame": runtime_frame, "read_error": snapshot.get("read_error") or runtime_result["error"]}


def write_resolution_operator_review_decision(
    *,
    proposal_id: str,
    invocation_id: str,
    suggestion_id: str,
    decision_status: str,
    operator_action: str,
    actor: str,
    reason: str | None = None,
) -> ResolutionOperatorReviewDecisionRecord:
    status = ResolutionOperatorDecisionStatus(decision_status)
    timestamp = _iso_now()
    record = ResolutionOperatorReviewDecisionRecord(
        review_decision_id=stable_object_id(
            "resolution_operator_review",
            {
                "proposal_id": proposal_id,
                "invocation_id": invocation_id,
                "suggestion_id": suggestion_id,
                "decision_status": status.value,
                "operator_action": operator_action,
                "actor": actor,
                "ts": timestamp.isoformat(),
            },
        ),
        proposal_id=proposal_id,
        invocation_id=invocation_id,
        suggestion_id=suggestion_id,
        decision_status=status,
        operator_action=operator_action,
        reason=reason,
        actor=actor,
        created_at=timestamp,
        updated_at=timestamp,
    )
    if duckdb is None:
        raise RuntimeError("duckdb is not installed")
    db_path = _resolve_canonical_db_path()
    con = duckdb.connect(str(db_path), read_only=False)
    try:
        con.execute(
            """
            INSERT INTO resolution.operator_review_decisions (
                review_decision_id,
                proposal_id,
                invocation_id,
                suggestion_id,
                decision_status,
                operator_action,
                reason,
                actor,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                record.review_decision_id,
                record.proposal_id,
                record.invocation_id,
                record.suggestion_id,
                record.decision_status.value,
                record.operator_action,
                record.reason,
                record.actor,
                _sql_timestamp(record.created_at),
                _sql_timestamp(record.updated_at),
            ],
        )
    finally:
        con.close()
    return record


def load_agent_review_data() -> dict[str, Any]:
    snapshot = load_ui_lite_snapshot()
    frame = _sort_desc(snapshot["tables"]["agent_review_summary"], "updated_at")
    if not frame.empty and "agent_type" in frame.columns:
        frame = frame[frame["agent_type"] == "resolution"]
    if not frame.empty:
        return {"source": "ui_lite", "frame": frame, "read_error": snapshot.get("read_error")}

    runtime_result = _read_agent_review_from_runtime_result(_resolve_canonical_db_path())
    runtime_frame = _sort_desc(runtime_result["frame"], "updated_at")
    if not runtime_frame.empty and "agent_type" in runtime_frame.columns:
        runtime_frame = runtime_frame[runtime_frame["agent_type"] == "resolution"]
    if not runtime_frame.empty:
        return {"source": "runtime_db", "frame": runtime_frame, "read_error": snapshot.get("read_error") or runtime_result["error"]}

    smoke_runtime_result = _read_agent_review_from_runtime_result(_resolve_real_weather_chain_db_path())
    smoke_runtime_frame = _sort_desc(smoke_runtime_result["frame"], "updated_at")
    if not smoke_runtime_frame.empty and "agent_type" in smoke_runtime_frame.columns:
        smoke_runtime_frame = smoke_runtime_frame[smoke_runtime_frame["agent_type"] == "resolution"]
    if not smoke_runtime_frame.empty:
        return {
            "source": "weather_smoke_db",
            "frame": smoke_runtime_frame,
            "read_error": snapshot.get("read_error") or runtime_result["error"] or smoke_runtime_result["error"],
        }

    smoke_frame = _sort_desc(_agent_rows_from_smoke_report(load_real_weather_smoke_report()), "updated_at")
    return {
        "source": "smoke_report",
        "frame": smoke_frame,
        "read_error": snapshot.get("read_error") or runtime_result["error"] or smoke_runtime_result["error"],
    }


def load_market_chain_analysis_data() -> dict[str, Any]:
    from ui.loaders.markets_loader import load_market_chain_analysis_data as load_market_chain_analysis_data_impl

    return load_market_chain_analysis_data_impl()


def load_agent_runtime_status() -> dict[str, Any]:
    provider = os.getenv("ASTERION_AGENT_PROVIDER", "").strip() or "openai_compatible"
    model = (
        os.getenv("ASTERION_OPENAI_COMPATIBLE_MODEL", "").strip()
        or os.getenv("ASTERION_AGENT_MODEL", "").strip()
        or os.getenv("QWEN_MODEL", "").strip()
        or "unconfigured"
    )
    return {
        "provider": provider,
        "model": model,
        "configured": model != "unconfigured",
        "agents": [
            {
                "agent_name": "Rule2Spec Validation",
                "file": str(ROOT / "domains" / "weather" / "spec" / "rule2spec_validation.py"),
                "role": "deterministic spec/station validation",
            },
            {
                "agent_name": "Replay Validation",
                "file": str(ROOT / "domains" / "weather" / "forecast" / "replay_validation.py"),
                "role": "deterministic replay/provenance validation",
            },
            {
                "agent_name": "Resolution Agent",
                "file": str(ROOT / "agents" / "weather" / "resolution_agent.py"),
                "role": "结算监控与争议分析",
            },
        ],
    }


def load_system_runtime_status() -> dict[str, Any]:
    readiness = load_readiness_summary()
    evidence = load_readiness_evidence_bundle()
    snapshot = load_ui_lite_snapshot()
    report_result = _read_json_result(_resolve_real_weather_smoke_report_path())
    report = report_result["payload"]
    opportunity_payload = load_market_opportunity_data()
    opportunities = opportunity_payload["frame"]
    agent_payload = load_agent_review_data()
    agent_data = agent_payload["frame"]
    resolution_payload = load_resolution_review_data()
    resolution_data = resolution_payload["frame"]
    calibration_health = _sort_desc(snapshot["tables"]["calibration_health_summary"], "materialized_at", "sample_count")
    latest_calibration = calibration_health.iloc[0].to_dict() if not calibration_health.empty else {}
    calibration_station_rollup = (
        calibration_health.groupby("station_id", dropna=False)[
            [
                "impacted_market_count",
                "hard_gate_market_count",
                "review_required_market_count",
                "research_only_market_count",
            ]
        ].max()
        if not calibration_health.empty
        and {"station_id", "impacted_market_count", "hard_gate_market_count", "review_required_market_count", "research_only_market_count"}.issubset(calibration_health.columns)
        else pd.DataFrame()
    )
    return {
        "ui_lite_db_path": snapshot["db_path"],
        "ui_lite_exists": snapshot["exists"],
        "ui_replica_db_path": str(_resolve_ui_replica_db_path()),
        "ui_replica_exists": _resolve_ui_replica_db_path().exists(),
        "readiness_report_path": readiness["report_path"],
        "readiness_report_exists": readiness["report_exists"],
        "readiness_report_markdown_path": readiness["report_markdown_path"],
        "readiness_report_markdown_exists": Path(readiness["report_markdown_path"]).exists(),
        "capability_manifest_path": readiness["capability_manifest_path"],
        "capability_manifest_exists": readiness["capability_manifest_exists"],
        "capability_manifest_status": readiness.get("capability_manifest_status"),
        "capability_boundary_summary": readiness.get("capability_boundary_summary") or {},
        "readiness_evidence_path": evidence.get("path"),
        "readiness_evidence_exists": bool(evidence.get("exists")),
        "readiness_evidence_generated_at": evidence.get("generated_at"),
        "readiness_evidence_blockers": evidence.get("blockers") or [],
        "readiness_evidence_warnings": evidence.get("warnings") or [],
        "readiness_evidence_stale_dependencies": evidence.get("stale_dependencies") or [],
        "readiness_evidence_read_error": evidence.get("read_error"),
        "weather_smoke_report_path": str(_resolve_real_weather_smoke_report_path()),
        "weather_smoke_report_exists": _resolve_real_weather_smoke_report_path().exists(),
        "weather_smoke_status": (report or {}).get("chain_status"),
        "weather_smoke_report_error": report_result["error"],
        "table_row_counts": snapshot["table_row_counts"],
        "ui_lite_read_error": snapshot.get("read_error"),
        "opportunity_row_count": int(len(opportunities.index)),
        "actionable_market_count": int((opportunities["actionability_status"] == "actionable").sum()) if "actionability_status" in opportunities.columns else 0,
        "agent_row_count": int(len(agent_data.index)),
        "agent_read_error": agent_payload.get("read_error"),
        "resolution_review_read_error": resolution_payload.get("read_error"),
        "opportunity_read_error": opportunity_payload.get("read_error"),
        "pending_operator_review_count": int((resolution_data["effective_redeem_status"] == "pending_operator_review").sum()) if "effective_redeem_status" in resolution_data.columns else 0,
        "blocked_by_operator_review_count": int((resolution_data["effective_redeem_status"] == "blocked_by_operator_review").sum()) if "effective_redeem_status" in resolution_data.columns else 0,
        "ready_for_redeem_review_count": int((resolution_data["effective_redeem_status"] == "ready_for_redeem_review").sum()) if "effective_redeem_status" in resolution_data.columns else 0,
        "latest_calibration_materialized_at": latest_calibration.get("materialized_at"),
        "latest_calibration_window_end": latest_calibration.get("window_end"),
        "latest_calibration_freshness_status": latest_calibration.get("calibration_freshness_status"),
        "latest_calibration_profile_age_hours": latest_calibration.get("profile_age_hours"),
        "calibration_impacted_market_count": int(calibration_station_rollup["impacted_market_count"].sum()) if not calibration_station_rollup.empty else int(_coerce_float(latest_calibration.get("impacted_market_count")) or 0),
        "calibration_hard_gate_market_count": int(calibration_station_rollup["hard_gate_market_count"].sum()) if not calibration_station_rollup.empty else int(_coerce_float(latest_calibration.get("hard_gate_market_count")) or 0),
        "calibration_review_required_market_count": int(calibration_station_rollup["review_required_market_count"].sum()) if not calibration_station_rollup.empty else int(_coerce_float(latest_calibration.get("review_required_market_count")) or 0),
        "calibration_research_only_market_count": int(calibration_station_rollup["research_only_market_count"].sum()) if not calibration_station_rollup.empty else int(_coerce_float(latest_calibration.get("research_only_market_count")) or 0),
    }


def load_boundary_sidebar_truth() -> dict[str, Any]:
    summary = load_boundary_sidebar_summary()
    readiness_surface = load_operator_surface_status()["readiness"]
    return {
        **summary.as_dict(),
        "status": readiness_surface["status"],
        "label": readiness_surface["label"],
        "detail": readiness_surface["detail"],
        "source": readiness_surface["source"],
        "updated_at": readiness_surface["updated_at"],
    }


def _surface_status(status: str, label: str, detail: str, source: str, updated_at: Any) -> dict[str, Any]:
    return {
        "status": status,
        "label": label,
        "detail": detail,
        "source": source,
        "updated_at": updated_at,
    }


def _status_rank(status: str) -> int:
    return {
        "read_error": 4,
        "degraded_source": 3,
        "refresh_in_progress": 2,
        "no_data": 1,
        "ok": 0,
    }.get(status, 0)


def load_operator_surface_status() -> dict[str, dict[str, Any]]:
    readiness = load_readiness_summary()
    evidence = load_readiness_evidence_bundle()
    execution = load_execution_console_data()
    market_payload = load_market_chain_analysis_data()
    agent_payload = load_agent_review_data()
    system_status = load_system_runtime_status()

    readiness_source = readiness.get("source") or "missing"
    if readiness.get("read_error"):
        readiness_surface = _surface_status(
            "read_error",
            "Readiness 读取失败",
            str(readiness.get("read_error")),
            readiness_source,
            readiness.get("updated_at"),
        )
    elif readiness.get("capability_manifest_status") not in {None, "valid"}:
        readiness_surface = _surface_status(
            "degraded_source",
            "Readiness 边界清单未就绪",
            f"capability manifest status={readiness.get('capability_manifest_status') or 'missing'}",
            readiness_source,
            readiness.get("updated_at"),
        )
    elif not readiness.get("report_exists") and readiness["phase_table"].empty:
        readiness_surface = _surface_status(
            "no_data",
            "Readiness 暂无数据",
            "尚未生成 readiness report 或 ui.phase_readiness_summary。",
            readiness_source,
            readiness.get("updated_at"),
        )
    elif evidence.get("read_error"):
        readiness_surface = _surface_status(
            "read_error",
            "Readiness Evidence 读取失败",
            str(evidence.get("read_error")),
            readiness_source,
            evidence.get("generated_at"),
        )
    elif not evidence.get("exists"):
        readiness_surface = _surface_status(
            "degraded_source",
            "Readiness Evidence 缺失",
            "当前只有 readiness report，尚未生成 evidence bundle。",
            readiness_source,
            readiness.get("updated_at"),
        )
    elif evidence.get("blockers"):
        readiness_surface = _surface_status(
            "degraded_source",
            "Readiness Evidence 存在阻断项",
            " / ".join(str(item) for item in evidence.get("blockers") or []),
            readiness_source,
            evidence.get("generated_at"),
        )
    else:
        readiness_surface = _surface_status(
            "ok",
            "Readiness 就绪",
            readiness.get("decision_reason") or "readiness report 可读。",
            readiness_source,
            readiness.get("updated_at"),
        )

    report = market_payload["weather_smoke_report"] or {}
    chain_status = report.get("chain_status")
    refresh_state = report.get("refresh_state")
    market_rows = market_payload["market_rows"]
    market_source = market_payload.get("market_opportunity_source") or "missing"
    market_read_error = load_market_opportunity_data().get("read_error") or system_status.get("weather_smoke_report_error")
    if market_read_error and not market_rows:
        market_surface = _surface_status(
            "read_error",
            "Market 链路读取失败",
            str(market_read_error),
            market_source,
            report.get("timestamp"),
        )
    elif refresh_state == "initializing" or chain_status == "initializing":
        market_surface = _surface_status(
            "refresh_in_progress",
            "Market 链路刷新中",
            report.get("refresh_note") or "正在生成最新一轮市场链报告。",
            market_source,
            report.get("timestamp"),
        )
    elif chain_status in {"transport_error", "degraded"} or (market_source in {"smoke_report", "weather_smoke_db"} and market_rows):
        market_surface = _surface_status(
            "degraded_source",
            "Market 链路处于降级数据源",
            report.get("note") or ((report.get("forecast_service") or {}).get("note")) or "当前使用 fallback source 或部分上游降级。",
            market_source,
            report.get("timestamp"),
        )
    elif (chain_status == "no_open_recent_markets") or not market_rows:
        market_surface = _surface_status(
            "no_data",
            "Market 链路暂无数据",
            report.get("note") or "当前没有命中的开盘近期天气市场。",
            market_source,
            report.get("timestamp"),
        )
    else:
        market_surface = _surface_status(
            "ok",
            "Market 链路正常",
            "市场链路已生成可用读面。",
            market_source,
            report.get("timestamp"),
        )

    agent_frame = agent_payload["frame"]
    agent_source = agent_payload.get("source") or "missing"
    if agent_payload.get("read_error") and agent_frame.empty:
        agent_surface = _surface_status(
            "read_error",
            "Agent 工作读取失败",
            str(agent_payload.get("read_error")),
            agent_source,
            None,
        )
    elif agent_frame.empty:
        agent_surface = _surface_status(
            "no_data",
            "Resolution Review 暂无数据",
            "当前没有可见的 resolution review rows。",
            agent_source,
            None,
        )
    elif agent_source in {"smoke_report", "weather_smoke_db"}:
        agent_surface = _surface_status(
            "degraded_source",
            "Resolution Review 来自降级数据源",
            "当前 resolution review 通过 runtime fallback 暴露，尚未进入 UI lite 主读面。",
            agent_source,
            agent_frame.iloc[0].get("updated_at") if not agent_frame.empty else None,
        )
    else:
        agent_surface = _surface_status(
            "ok",
            "Resolution Review 正常",
            "resolution review rows 可正常读取。",
            agent_source,
            agent_frame.iloc[0].get("updated_at") if not agent_frame.empty else None,
        )

    execution_frames = [execution["tickets"], execution["live_prereq"], execution["exceptions"], load_wallet_readiness_data()]
    execution_rows = sum(len(frame.index) for frame in execution_frames)
    if system_status.get("ui_lite_read_error") and execution_rows == 0:
        execution_surface = _surface_status(
            "read_error",
            "Execution / Live-Prereq 读取失败",
            str(system_status.get("ui_lite_read_error")),
            "ui_lite",
            None,
        )
    elif execution_rows == 0 and not system_status.get("ui_lite_exists"):
        execution_surface = _surface_status(
            "no_data",
            "Execution / Live-Prereq 暂无数据",
            "当前没有 execution/live-prereq 读面数据。",
            "ui_lite",
            None,
        )
    else:
        execution_surface = _surface_status(
            "ok",
            "Execution / Live-Prereq 正常",
            "execution/live-prereq 读面可读。",
            "ui_lite",
            None,
        )

    surfaces = {
        "readiness": readiness_surface,
        "market_chain": market_surface,
        "agent_review": agent_surface,
        "execution": execution_surface,
    }
    worst_name, worst_surface = max(surfaces.items(), key=lambda item: _status_rank(item[1]["status"]))
    return {
        **surfaces,
        "overall": {
            "surface": worst_name,
            **worst_surface,
        },
    }


def build_ops_console_overview() -> dict[str, Any]:
    from ui.loaders.home_loader import build_ops_console_overview as build_ops_console_overview_impl

    return build_ops_console_overview_impl()


def load_home_decision_snapshot() -> dict[str, Any]:
    from ui.loaders.home_loader import load_home_decision_snapshot as load_home_decision_snapshot_impl

    return load_home_decision_snapshot_impl()
