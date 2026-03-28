from __future__ import annotations

import dataclasses
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from asterion_core.contracts import (
    ForecastReplayDiffRecord,
    ForecastReplayRecord,
    ForecastRunRecord,
    StationMetadata,
    WeatherFairValueRecord,
    WeatherMarket,
    WeatherMarketSpecRecord,
    WatchOnlySnapshotRecord,
)
from asterion_core.ui.builders import build_execution_tables, build_opportunity_tables
from asterion_core.ui.builders.catalog_builder import build_catalog_tables
from asterion_core.ui.read_model_registry import get_read_model_catalog_record, required_ui_tables
from asterion_core.ui.surface_truth_shared import annotate_frame_with_source_truth, ensure_primary_score_fields
from asterion_core.storage.logger import get_logger
from domains.weather.forecast import validate_replay_quality
from domains.weather.opportunity import (
    build_execution_science_cohort_summaries,
    build_weather_opportunity_assessment,
    derive_opportunity_side,
)
from domains.weather.opportunity.resolved_execution_projection import build_resolved_execution_projection
from domains.weather.spec import parse_rule2spec_draft, validate_rule2spec_draft


log = get_logger(__name__)

DEFAULT_UI_LITE_DB_PATH = "data/ui/asterion_ui_lite.duckdb"
DEFAULT_UI_DB_REPLICA_SOURCE_PATH = "data/ui/asterion_ui.duckdb"
DEFAULT_READINESS_REPORT_JSON_PATH = "data/ui/asterion_readiness_p3.json"
DEFAULT_READINESS_EVIDENCE_JSON_PATH = "data/ui/asterion_readiness_evidence_p4.json"

_REQUIRED_UI_TABLES = list(required_ui_tables())


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


def default_ui_lite_db_path() -> str:
    return os.getenv("ASTERION_UI_LITE_DB_PATH", DEFAULT_UI_LITE_DB_PATH)


def default_ui_lite_meta_path(*, lite_db_path: str | None = None) -> str:
    env_path = os.getenv("ASTERION_UI_LITE_META_PATH", "").strip()
    if env_path:
        return env_path
    db_path = Path(lite_db_path or default_ui_lite_db_path())
    return str(db_path.with_name(f"{db_path.stem}.meta.json"))


def default_readiness_report_json_path() -> str:
    return os.getenv("ASTERION_READINESS_P3_JSON_PATH", DEFAULT_READINESS_REPORT_JSON_PATH)


def load_ui_lite_meta(meta_path: str) -> dict[str, Any] | None:
    path = Path(meta_path)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None


@dataclasses.dataclass(frozen=True)
class UiLiteBuildResult:
    ok: bool
    ts_ms: int
    elapsed_ms: int
    error: str | None
    src_db_path: str
    dst_db_path: str
    meta_path: str
    readiness_report_json_path: str | None
    table_row_counts: dict[str, int]


def build_ui_lite_db_once(
    *,
    src_db_path: str | None = None,
    dst_db_path: str | None = None,
    meta_path: str | None = None,
    readiness_report_json_path: str | None = None,
    readiness_evidence_json_path: str | None = None,
    refresh_interval_s: float | None = None,
) -> UiLiteBuildResult:
    started_ms = int(time.time() * 1000)
    src = Path(src_db_path or os.getenv("ASTERION_UI_DB_REPLICA_PATH", DEFAULT_UI_DB_REPLICA_SOURCE_PATH))
    dst = Path(dst_db_path or default_ui_lite_db_path())
    meta = Path(meta_path or default_ui_lite_meta_path(lite_db_path=str(dst)))
    report_path = readiness_report_json_path or default_readiness_report_json_path()
    evidence_path = readiness_evidence_json_path or os.getenv(
        "ASTERION_READINESS_EVIDENCE_JSON_PATH",
        DEFAULT_READINESS_EVIDENCE_JSON_PATH,
    )
    prev = load_ui_lite_meta(str(meta)) or {}

    def _emit(ok: bool, error: str | None, table_row_counts: dict[str, int] | None = None) -> UiLiteBuildResult:
        now_ms = int(time.time() * 1000)
        src_stat = _safe_stat(src)
        dst_stat = _safe_stat(dst)
        payload = {
            "source_db_path": str(src),
            "lite_db_path": str(dst),
            "readiness_report_json_path": str(report_path),
            "readiness_evidence_json_path": str(evidence_path),
            "last_attempt_ts_ms": now_ms,
            "last_success_ts_ms": now_ms if ok else prev.get("last_success_ts_ms"),
            "consecutive_failures": 0 if ok else int(prev.get("consecutive_failures", 0) or 0) + 1,
            "last_error": None if ok else str(error),
            "source_size_bytes": src_stat.get("size_bytes"),
            "source_mtime_ms": src_stat.get("mtime_ms"),
            "lite_size_bytes": dst_stat.get("size_bytes"),
            "lite_mtime_ms": dst_stat.get("mtime_ms"),
            "table_row_counts": table_row_counts or {},
            "refresh_interval_s": float(refresh_interval_s) if refresh_interval_s is not None else prev.get("refresh_interval_s"),
        }
        _write_json_atomic(meta, payload)
        return UiLiteBuildResult(
            ok=ok,
            ts_ms=now_ms,
            elapsed_ms=max(0, now_ms - started_ms),
            error=error,
            src_db_path=str(src),
            dst_db_path=str(dst),
            meta_path=str(meta),
            readiness_report_json_path=str(report_path),
            table_row_counts=table_row_counts or {},
        )

    snapshot: Path | None = None
    tmp_db = Path(str(dst) + ".tmp")
    try:
        if not src.exists():
            raise FileNotFoundError(f"UI replica DB not found: {src}")
        dst.parent.mkdir(parents=True, exist_ok=True)
        snapshot = _create_source_snapshot(src)
        if tmp_db.exists():
            tmp_db.unlink()
        table_row_counts = _build_ui_lite_contract(
            tmp_db_path=tmp_db,
            src_snapshot_path=snapshot,
            readiness_report_json_path=Path(report_path),
            readiness_evidence_json_path=Path(evidence_path),
        )
        validate_ui_lite_db(str(tmp_db))
        os.replace(tmp_db, dst)
        return _emit(True, None, table_row_counts)
    except Exception as exc:  # noqa: BLE001
        if tmp_db.exists():
            tmp_db.unlink()
        return _emit(False, str(exc), {})
    finally:
        if snapshot is not None and snapshot.exists():
            snapshot.unlink()


def run_ui_lite_db_loop(
    *,
    src_db_path: str | None = None,
    dst_db_path: str | None = None,
    meta_path: str | None = None,
    readiness_report_json_path: str | None = None,
    readiness_evidence_json_path: str | None = None,
    interval_s: float = 30.0,
) -> None:
    while True:
        result = build_ui_lite_db_once(
            src_db_path=src_db_path,
            dst_db_path=dst_db_path,
            meta_path=meta_path,
            readiness_report_json_path=readiness_report_json_path,
            readiness_evidence_json_path=readiness_evidence_json_path,
            refresh_interval_s=interval_s,
        )
        if result.ok:
            log.info("ui lite build ok src=%s dst=%s elapsed_ms=%s", result.src_db_path, result.dst_db_path, result.elapsed_ms)
        else:
            log.warning(
                "ui lite build failed src=%s dst=%s elapsed_ms=%s err=%s",
                result.src_db_path,
                result.dst_db_path,
                result.elapsed_ms,
                result.error,
            )
        time.sleep(max(1.0, float(interval_s)))


def validate_ui_lite_db(db_path: str) -> dict[str, int]:
    path = Path(db_path)
    if not path.exists():
        raise FileNotFoundError(f"UI lite DB not found: {db_path}")
    con = _connect_duckdb(str(path), read_only=True)
    try:
        row_counts: dict[str, int] = {}
        for table in _REQUIRED_UI_TABLES:
            if not _table_exists(con, table):
                raise RuntimeError(f"missing required UI lite table: {table}")
            row = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            row_counts[table] = int(row[0]) if row is not None else 0
        catalog = con.execute("SELECT table_name, required_columns_json FROM ui.read_model_catalog").fetchall()
        for table_name, required_columns_json in catalog:
            actual_columns = _table_columns(con, str(table_name))
            try:
                required_columns = json.loads(str(required_columns_json or "[]"))
            except Exception:  # noqa: BLE001
                required_columns = []
            missing = [column for column in required_columns if column not in actual_columns]
            if missing:
                raise RuntimeError(f"ui lite table {table_name} missing required columns: {','.join(missing)}")
            record = get_read_model_catalog_record(str(table_name))
            if record is not None and record.primary_score_column == "ranking_score":
                if "ranking_score" not in actual_columns or "primary_score_label" not in actual_columns:
                    raise RuntimeError(f"ui lite primary score contract incomplete for {table_name}")
        failed_checks = con.execute(
            "SELECT surface_id, table_name FROM ui.truth_source_checks WHERE check_status = 'fail'"
        ).fetchall()
        if failed_checks:
            rendered = ", ".join(f"{surface}:{table}" for surface, table in failed_checks)
            raise RuntimeError(f"ui truth source checks failed: {rendered}")
        return row_counts
    finally:
        con.close()


def _build_ui_lite_contract(
    *,
    tmp_db_path: Path,
    src_snapshot_path: Path,
    readiness_report_json_path: Path,
    readiness_evidence_json_path: Path,
) -> dict[str, int]:
    con = _connect_duckdb(str(tmp_db_path), read_only=False)
    table_row_counts: dict[str, int] = {}
    try:
        snapshot_sql = str(src_snapshot_path).replace("'", "''")
        con.execute(f"ATTACH '{snapshot_sql}' AS src (READ_ONLY)")
        con.execute("CREATE SCHEMA IF NOT EXISTS ui")
        _create_table_from_src(
            con,
            target="ui.market_watch_summary",
            sql_body="""
            WITH latest_run AS (
                SELECT market_id, run_id, source, model_run, forecast_target_time, created_at
                FROM (
                    SELECT
                        market_id,
                        run_id,
                        source,
                        model_run,
                        forecast_target_time,
                        created_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY market_id
                            ORDER BY created_at DESC, run_id DESC
                        ) AS rn
                    FROM src.weather.weather_forecast_runs
                )
                WHERE rn = 1
            ),
            latest_snapshot AS (
                SELECT
                    market_id,
                    snapshot_id,
                    token_id,
                    outcome,
                    decision,
                    side,
                    edge_bps,
                    threshold_bps,
                    reference_price,
                    fair_value,
                    created_at
                FROM (
                    SELECT
                        market_id,
                        snapshot_id,
                        token_id,
                        outcome,
                        decision,
                        side,
                        edge_bps,
                        threshold_bps,
                        reference_price,
                        fair_value,
                        created_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY market_id
                            ORDER BY created_at DESC, snapshot_id DESC
                        ) AS rn
                    FROM src.weather.weather_watch_only_snapshots
                )
                WHERE rn = 1
            )
            SELECT
                m.market_id,
                m.condition_id,
                m.title,
                m.status,
                m.active,
                m.closed,
                spec.location_name,
                spec.station_id,
                spec.spec_version,
                run.run_id AS latest_run_id,
                run.source AS latest_run_source,
                run.model_run AS latest_model_run,
                run.forecast_target_time AS latest_forecast_target_time,
                snap.snapshot_id AS latest_snapshot_id,
                snap.token_id AS latest_token_id,
                snap.outcome AS latest_outcome,
                snap.decision AS latest_decision,
                snap.side AS latest_side,
                snap.edge_bps,
                snap.threshold_bps,
                snap.reference_price,
                snap.fair_value,
                snap.created_at AS latest_snapshot_created_at
            FROM src.weather.weather_markets m
            LEFT JOIN src.weather.weather_market_specs spec ON spec.market_id = m.market_id
            LEFT JOIN latest_run run ON run.market_id = m.market_id
            LEFT JOIN latest_snapshot snap ON snap.market_id = m.market_id
            """,
            table_row_counts=table_row_counts,
        )
        _create_table_from_src(
            con,
            target="ui.proposal_resolution_summary",
            sql_body="""
            WITH latest_verification AS (
                SELECT
                    proposal_id,
                    verification_id,
                    expected_outcome,
                    is_correct,
                    confidence,
                    evidence_package,
                    created_at
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
                    FROM src.resolution.settlement_verifications
                )
                WHERE rn = 1
            ),
            latest_redeem AS (
                SELECT
                    proposal_id,
                    suggestion_id,
                    decision,
                    reason,
                    human_review_required,
                    created_at
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
                    FROM src.resolution.redeem_readiness_suggestions
                )
                WHERE rn = 1
            ),
            latest_continuity AS (
                SELECT
                    check_id,
                    status,
                    from_block,
                    to_block,
                    created_at
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
                    FROM src.resolution.watcher_continuity_checks
                )
                WHERE rn = 1
            ),
            latest_invocation AS (
                SELECT
                    invocation_id,
                    subject_id,
                    status,
                    ended_at,
                    started_at
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
                    FROM src.agent.invocations
                    WHERE agent_type = 'resolution'
                      AND subject_type = 'uma_proposal'
                )
                WHERE rn = 1
            ),
            latest_output AS (
                SELECT
                    invocation_id,
                    verdict,
                    confidence,
                    summary,
                    human_review_required,
                    structured_output_json,
                    created_at
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
                    FROM src.agent.outputs
                )
                WHERE rn = 1
            ),
            latest_agent_review AS (
                SELECT
                    invocation_id,
                    review_payload_json,
                    reviewed_at
                FROM (
                    SELECT
                        invocation_id,
                        review_payload_json,
                        reviewed_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY invocation_id
                            ORDER BY reviewed_at DESC, review_id DESC
                        ) AS rn
                    FROM src.agent.reviews
                )
                WHERE rn = 1
            ),
            latest_operator_review AS (
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
                    updated_at
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
                    FROM src.resolution.operator_review_decisions
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
                link.evidence_package_id,
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
                output.verdict AS latest_agent_verdict,
                output.confidence AS latest_agent_confidence,
                output.summary AS latest_agent_summary,
                json_extract_string(try_cast(review.review_payload_json AS JSON), '$.recommended_operator_action') AS latest_recommended_operator_action,
                TRY_CAST(json_extract_string(try_cast(review.review_payload_json AS JSON), '$.settlement_risk_score') AS DOUBLE) AS latest_settlement_risk_score,
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
                    WHEN json_extract_string(try_cast(review.review_payload_json AS JSON), '$.recommended_operator_action') IN ('hold_redeem', 'manual_review', 'consider_dispute') THEN 'pending_operator_review'
                    ELSE redeem.decision
                END AS effective_redeem_status
            FROM src.resolution.uma_proposals p
            LEFT JOIN latest_verification v ON v.proposal_id = p.proposal_id
            LEFT JOIN src.resolution.proposal_evidence_links link ON link.proposal_id = p.proposal_id
            LEFT JOIN latest_redeem redeem ON redeem.proposal_id = p.proposal_id
            LEFT JOIN latest_continuity continuity ON TRUE
            LEFT JOIN latest_invocation inv ON inv.subject_id = p.proposal_id
            LEFT JOIN latest_output output ON output.invocation_id = inv.invocation_id
            LEFT JOIN latest_agent_review review ON review.invocation_id = inv.invocation_id
            LEFT JOIN latest_operator_review operator_review ON operator_review.proposal_id = p.proposal_id
            """,
            table_row_counts=table_row_counts,
        )
        _create_table_from_src(
            con,
            target="ui.execution_ticket_summary",
            sql_body="""
            WITH latest_gate AS (
                SELECT
                    ticket_id,
                    gate_id,
                    allowed,
                    reason,
                    reason_codes_json,
                    created_at
                FROM (
                    SELECT
                        ticket_id,
                        gate_id,
                        allowed,
                        reason,
                        reason_codes_json,
                        created_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY ticket_id
                            ORDER BY created_at DESC, gate_id DESC
                        ) AS rn
                    FROM src.runtime.gate_decisions
                )
                WHERE rn = 1
            ),
            latest_order_event_by_ticket AS (
                SELECT
                    ticket_id,
                    request_id,
                    order_id
                FROM (
                    SELECT
                        json_extract_string(try_cast(payload_json AS JSON), '$.ticket_id') AS ticket_id,
                        json_extract_string(try_cast(payload_json AS JSON), '$.request_id') AS request_id,
                        entity_id AS order_id,
                        created_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY json_extract_string(try_cast(payload_json AS JSON), '$.ticket_id')
                            ORDER BY created_at DESC, entity_id DESC
                        ) AS rn
                    FROM src.runtime.journal_events
                    WHERE event_type = 'order.created'
                      AND json_extract_string(try_cast(payload_json AS JSON), '$.ticket_id') IS NOT NULL
                )
                WHERE rn = 1
            ),
            latest_order_event_by_request AS (
                SELECT
                    ticket_id,
                    request_id,
                    order_id
                FROM (
                    SELECT
                        json_extract_string(try_cast(payload_json AS JSON), '$.ticket_id') AS ticket_id,
                        json_extract_string(try_cast(payload_json AS JSON), '$.request_id') AS request_id,
                        entity_id AS order_id,
                        created_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY json_extract_string(try_cast(payload_json AS JSON), '$.request_id')
                            ORDER BY created_at DESC, entity_id DESC
                        ) AS rn
                    FROM src.runtime.journal_events
                    WHERE event_type = 'order.created'
                      AND json_extract_string(try_cast(payload_json AS JSON), '$.ticket_id') IS NULL
                      AND json_extract_string(try_cast(payload_json AS JSON), '$.request_id') IS NOT NULL
                )
                WHERE rn = 1
            ),
            latest_reservation_event_by_ticket AS (
                SELECT
                    ticket_id,
                    request_id,
                    reservation_id
                FROM (
                    SELECT
                        json_extract_string(try_cast(payload_json AS JSON), '$.ticket_id') AS ticket_id,
                        json_extract_string(try_cast(payload_json AS JSON), '$.request_id') AS request_id,
                        entity_id AS reservation_id,
                        created_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY json_extract_string(try_cast(payload_json AS JSON), '$.ticket_id')
                            ORDER BY created_at DESC, entity_id DESC
                        ) AS rn
                    FROM src.runtime.journal_events
                    WHERE event_type = 'reservation.created'
                      AND json_extract_string(try_cast(payload_json AS JSON), '$.ticket_id') IS NOT NULL
                )
                WHERE rn = 1
            ),
            latest_reservation_event_by_request AS (
                SELECT
                    ticket_id,
                    request_id,
                    reservation_id
                FROM (
                    SELECT
                        json_extract_string(try_cast(payload_json AS JSON), '$.ticket_id') AS ticket_id,
                        json_extract_string(try_cast(payload_json AS JSON), '$.request_id') AS request_id,
                        entity_id AS reservation_id,
                        created_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY json_extract_string(try_cast(payload_json AS JSON), '$.request_id')
                            ORDER BY created_at DESC, entity_id DESC
                        ) AS rn
                    FROM src.runtime.journal_events
                    WHERE event_type = 'reservation.created'
                      AND json_extract_string(try_cast(payload_json AS JSON), '$.ticket_id') IS NULL
                      AND json_extract_string(try_cast(payload_json AS JSON), '$.request_id') IS NOT NULL
                )
                WHERE rn = 1
            ),
            latest_sign_attempt AS (
                SELECT
                    ticket_id,
                    attempt_id,
                    status,
                    created_at
                FROM (
                    SELECT
                        ticket_id,
                        attempt_id,
                        status,
                        created_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY ticket_id
                            ORDER BY created_at DESC, attempt_id DESC
                        ) AS rn
                    FROM src.runtime.submit_attempts
                    WHERE attempt_kind = 'sign_order'
                      AND attempt_mode = 'sign_only'
                )
                WHERE rn = 1
            ),
            latest_submit_attempt AS (
                SELECT
                    ticket_id,
                    attempt_id,
                    attempt_mode,
                    status,
                    created_at
                FROM (
                    SELECT
                        ticket_id,
                        attempt_id,
                        attempt_mode,
                        status,
                        created_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY ticket_id
                            ORDER BY created_at DESC, attempt_id DESC
                        ) AS rn
                    FROM src.runtime.submit_attempts
                    WHERE attempt_kind = 'submit_order'
                )
                WHERE rn = 1
            ),
            latest_external_order_observation AS (
                SELECT
                    ticket_id,
                    observation_id,
                    external_status,
                    observed_at
                FROM (
                    SELECT
                        ticket_id,
                        observation_id,
                        external_status,
                        observed_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY ticket_id
                            ORDER BY observed_at DESC, observation_id DESC
                        ) AS rn
                    FROM src.runtime.external_order_observations
                )
                WHERE rn = 1
            ),
            external_fill_agg AS (
                SELECT
                    ticket_id,
                    COUNT(*) AS external_fill_count,
                    SUM(size) AS external_filled_size,
                    MAX(observed_at) AS external_last_fill_at
                FROM src.runtime.external_fill_observations
                GROUP BY ticket_id
            ),
            fill_agg AS (
                SELECT
                    order_id,
                    COUNT(*) AS fill_count,
                    SUM(size) AS filled_size,
                    SUM(price * size) AS filled_notional,
                    CASE
                        WHEN SUM(size) > 0 THEN SUM(price * size) / SUM(size)
                        ELSE NULL
                    END AS avg_fill_price,
                    MAX(filled_at) AS last_fill_at
                FROM src.trading.fills
                GROUP BY order_id
            ),
            latest_transition AS (
                SELECT
                    order_id,
                    from_status,
                    to_status,
                    reason,
                    timestamp
                FROM (
                    SELECT
                        order_id,
                        from_status,
                        to_status,
                        reason,
                        timestamp,
                        ROW_NUMBER() OVER (
                            PARTITION BY order_id
                            ORDER BY timestamp DESC, transition_id DESC
                        ) AS rn
                    FROM src.trading.order_state_transitions
                )
                WHERE rn = 1
            ),
            latest_transition_by_status AS (
                SELECT
                    order_id,
                    from_status,
                    to_status,
                    reason,
                    timestamp
                FROM (
                    SELECT
                        order_id,
                        from_status,
                        to_status,
                        reason,
                        timestamp,
                        ROW_NUMBER() OVER (
                            PARTITION BY order_id, to_status
                            ORDER BY timestamp DESC, transition_id DESC
                        ) AS rn
                    FROM src.trading.order_state_transitions
                )
                WHERE rn = 1
            ),
            latest_paper_reconciliation AS (
                SELECT
                    order_id,
                    reconciliation_id,
                    status,
                    discrepancy
                FROM (
                    SELECT
                        order_id,
                        reconciliation_id,
                        status,
                        discrepancy,
                        created_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY order_id
                            ORDER BY created_at DESC, reconciliation_id DESC
                        ) AS rn
                    FROM src.trading.reconciliation_results
                    WHERE COALESCE(reconciliation_scope, 'paper_local') = 'paper_local'
                )
                WHERE rn = 1
            ),
            latest_external_reconciliation AS (
                SELECT
                    order_id,
                    reconciliation_id,
                    status,
                    discrepancy,
                    reconciliation_scope,
                    source_system
                FROM (
                    SELECT
                        order_id,
                        reconciliation_id,
                        status,
                        discrepancy,
                        reconciliation_scope,
                        source_system,
                        created_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY order_id
                            ORDER BY created_at DESC, reconciliation_id DESC
                        ) AS rn
                    FROM src.trading.reconciliation_results
                    WHERE reconciliation_scope = 'external_execution'
                )
                WHERE rn = 1
            ),
            latest_journal_by_ticket AS (
                SELECT
                    ticket_id,
                    request_id,
                    event_id,
                    event_type,
                    created_at
                FROM (
                    SELECT
                        CASE
                            WHEN event_type = 'trade_ticket.created' THEN entity_id
                            ELSE json_extract_string(try_cast(payload_json AS JSON), '$.ticket_id')
                        END AS ticket_id,
                        json_extract_string(try_cast(payload_json AS JSON), '$.request_id') AS request_id,
                        event_id,
                        event_type,
                        created_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY CASE
                                WHEN event_type = 'trade_ticket.created' THEN entity_id
                                ELSE json_extract_string(try_cast(payload_json AS JSON), '$.ticket_id')
                            END
                            ORDER BY created_at DESC, event_id DESC
                        ) AS rn
                    FROM src.runtime.journal_events
                    WHERE (
                        event_type = 'trade_ticket.created'
                        OR json_extract_string(try_cast(payload_json AS JSON), '$.ticket_id') IS NOT NULL
                    )
                )
                WHERE rn = 1
            ),
            latest_journal_by_request AS (
                SELECT
                    ticket_id,
                    request_id,
                    event_id,
                    event_type,
                    created_at
                FROM (
                    SELECT
                        CASE
                            WHEN event_type = 'trade_ticket.created' THEN entity_id
                            ELSE json_extract_string(try_cast(payload_json AS JSON), '$.ticket_id')
                        END AS ticket_id,
                        json_extract_string(try_cast(payload_json AS JSON), '$.request_id') AS request_id,
                        event_id,
                        event_type,
                        created_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY json_extract_string(try_cast(payload_json AS JSON), '$.request_id')
                            ORDER BY created_at DESC, event_id DESC
                        ) AS rn
                    FROM src.runtime.journal_events
                    WHERE json_extract_string(try_cast(payload_json AS JSON), '$.request_id') IS NOT NULL
                      AND CASE
                            WHEN event_type = 'trade_ticket.created' THEN entity_id
                            ELSE json_extract_string(try_cast(payload_json AS JSON), '$.ticket_id')
                          END IS NULL
                )
                WHERE rn = 1
            )
            SELECT
                ticket.ticket_id,
                ticket.run_id,
                ticket.strategy_id,
                ticket.strategy_version,
                ticket.market_id,
                ticket.token_id,
                ticket.outcome,
                ticket.side,
                ticket.route_action,
                ticket.size,
                ticket.reference_price,
                ticket.fair_value,
                ticket.wallet_id,
                ticket.request_id,
                ticket.execution_context_id,
                gate.gate_id,
                gate.allowed AS gate_allowed,
                gate.reason AS gate_reason,
                gate.reason_codes_json,
                ord.order_id,
                ord.status AS order_status,
                ord.reservation_id,
                fill.fill_count,
                fill.filled_size,
                fill.filled_notional,
                fill.avg_fill_price,
                fill.last_fill_at,
                reservation.status AS reservation_status,
                COALESCE(transition_status.from_status, transition.from_status) AS latest_transition_from_status,
                COALESCE(transition_status.to_status, transition.to_status, ord.status) AS latest_transition_to_status,
                COALESCE(transition_status.reason, transition.reason) AS latest_transition_reason,
                COALESCE(transition_status.timestamp, transition.timestamp) AS latest_transition_at,
                paper_reconciliation.reconciliation_id,
                paper_reconciliation.status AS reconciliation_status,
                paper_reconciliation.discrepancy AS reconciliation_discrepancy,
                external_reconciliation.reconciliation_id AS external_reconciliation_id,
                external_reconciliation.status AS external_reconciliation_status,
                external_reconciliation.discrepancy AS external_reconciliation_discrepancy,
                external_reconciliation.reconciliation_scope AS external_reconciliation_scope,
                external_reconciliation.source_system AS external_reconciliation_source_system,
                sign_attempt.attempt_id AS latest_sign_attempt_id,
                sign_attempt.status AS latest_sign_attempt_status,
                sign_attempt.created_at AS latest_sign_attempt_created_at,
                submit_attempt.attempt_id AS latest_submit_attempt_id,
                submit_attempt.attempt_mode AS latest_submit_mode,
                submit_attempt.status AS latest_submit_status,
                submit_attempt.created_at AS latest_submit_created_at,
                external_order.observation_id AS external_order_observation_id,
                external_order.external_status AS external_order_status,
                external_order.observed_at AS external_order_observed_at,
                COALESCE(external_fill.external_fill_count, 0) AS external_fill_count,
                external_fill.external_filled_size,
                external_fill.external_last_fill_at,
                CASE
                    WHEN paper_reconciliation.status IS NOT NULL AND paper_reconciliation.status <> 'ok' THEN 'reconciliation_mismatch'
                    WHEN gate.allowed = FALSE THEN 'rejected_by_gate'
                    WHEN ord.status = 'filled' THEN 'filled'
                    WHEN ord.status = 'partial_filled' THEN 'partial_filled'
                    WHEN ord.status = 'cancelled' THEN 'cancelled'
                    WHEN ord.status = 'posted' THEN 'posted_resting'
                    ELSE 'pending_gate'
                END AS execution_result,
                CASE
                    WHEN paper_reconciliation.status IS NOT NULL AND paper_reconciliation.status <> 'ok' THEN TRUE
                    WHEN gate.allowed = FALSE THEN TRUE
                    WHEN ord.status IN ('cancelled', 'rejected') THEN TRUE
                    ELSE FALSE
                END AS operator_attention_required,
                CASE
                    WHEN sign_attempt.attempt_id IS NULL THEN 'not_signed'
                    WHEN sign_attempt.status = 'rejected' THEN 'sign_rejected'
                    WHEN submit_attempt.attempt_id IS NULL THEN 'signed_not_submitted'
                    WHEN submit_attempt.attempt_mode = 'dry_run' AND submit_attempt.status = 'previewed' THEN 'preview_only'
                    WHEN submit_attempt.status = 'rejected' OR external_order.external_status = 'rejected' THEN 'submit_rejected'
                    WHEN external_reconciliation.status = 'external_state_unverified' THEN 'external_unverified'
                    WHEN external_reconciliation.status IN ('external_order_mismatch', 'external_fill_mismatch') THEN 'external_mismatch'
                    WHEN submit_attempt.status = 'accepted' AND external_reconciliation.status = 'ok' THEN 'shadow_aligned'
                    ELSE 'signed_not_submitted'
                END AS live_prereq_execution_status,
                CASE
                    WHEN sign_attempt.status = 'rejected' THEN TRUE
                    WHEN submit_attempt.status = 'rejected' OR external_order.external_status = 'rejected' THEN TRUE
                    WHEN external_reconciliation.status = 'external_state_unverified' THEN TRUE
                    WHEN external_reconciliation.status IN ('external_order_mismatch', 'external_fill_mismatch') THEN TRUE
                    ELSE FALSE
                END AS live_prereq_attention_required,
                'quote_based' AS paper_fill_mode,
                COALESCE(journal_ticket.event_id, journal_request.event_id) AS latest_journal_event_id,
                COALESCE(journal_ticket.event_type, journal_request.event_type) AS latest_journal_event_type,
                COALESCE(journal_ticket.created_at, journal_request.created_at) AS latest_journal_created_at
            FROM src.runtime.trade_tickets ticket
            LEFT JOIN latest_gate gate ON gate.ticket_id = ticket.ticket_id
            LEFT JOIN latest_order_event_by_ticket order_event_ticket ON order_event_ticket.ticket_id = ticket.ticket_id
            LEFT JOIN latest_order_event_by_request order_event_request
                ON order_event_request.request_id = ticket.request_id
               AND order_event_ticket.order_id IS NULL
            LEFT JOIN src.trading.orders ord
                ON ord.order_id = COALESCE(order_event_ticket.order_id, order_event_request.order_id)
            LEFT JOIN fill_agg fill ON fill.order_id = ord.order_id
            LEFT JOIN latest_reservation_event_by_ticket reservation_event_ticket
                ON reservation_event_ticket.ticket_id = ticket.ticket_id
            LEFT JOIN latest_reservation_event_by_request reservation_event_request
                ON reservation_event_request.request_id = ticket.request_id
               AND reservation_event_ticket.reservation_id IS NULL
            LEFT JOIN src.trading.reservations reservation
                ON reservation.reservation_id = COALESCE(reservation_event_ticket.reservation_id, reservation_event_request.reservation_id)
            LEFT JOIN latest_transition transition ON transition.order_id = ord.order_id
            LEFT JOIN latest_transition_by_status transition_status
                ON transition_status.order_id = ord.order_id
               AND transition_status.to_status = ord.status
            LEFT JOIN latest_paper_reconciliation paper_reconciliation ON paper_reconciliation.order_id = ord.order_id
            LEFT JOIN latest_external_reconciliation external_reconciliation ON external_reconciliation.order_id = ord.order_id
            LEFT JOIN latest_sign_attempt sign_attempt ON sign_attempt.ticket_id = ticket.ticket_id
            LEFT JOIN latest_submit_attempt submit_attempt ON submit_attempt.ticket_id = ticket.ticket_id
            LEFT JOIN latest_external_order_observation external_order ON external_order.ticket_id = ticket.ticket_id
            LEFT JOIN external_fill_agg external_fill ON external_fill.ticket_id = ticket.ticket_id
            LEFT JOIN latest_journal_by_ticket journal_ticket ON journal_ticket.ticket_id = ticket.ticket_id
            LEFT JOIN latest_journal_by_request journal_request
                ON journal_request.request_id = ticket.request_id
               AND journal_ticket.event_id IS NULL
            """,
            table_row_counts=table_row_counts,
        )
        _create_table_from_src(
            con,
            target="ui.execution_run_summary",
            sql_body="""
            SELECT
                run_id,
                MAX(wallet_id) AS wallet_id,
                COUNT(DISTINCT strategy_id) AS strategy_count,
                COUNT(*) AS ticket_count,
                SUM(CASE WHEN gate_allowed THEN 1 ELSE 0 END) AS gate_allowed_count,
                SUM(CASE WHEN gate_allowed = FALSE THEN 1 ELSE 0 END) AS gate_rejected_count,
                SUM(CASE WHEN order_status = 'posted' THEN 1 ELSE 0 END) AS posted_count,
                SUM(CASE WHEN order_status = 'filled' THEN 1 ELSE 0 END) AS filled_count,
                SUM(CASE WHEN order_status = 'partial_filled' THEN 1 ELSE 0 END) AS partial_filled_count,
                SUM(CASE WHEN order_status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_count,
                SUM(CASE WHEN reconciliation_status = 'ok' THEN 1 ELSE 0 END) AS reconciliation_ok_count,
                SUM(CASE WHEN reconciliation_status IS NOT NULL AND reconciliation_status <> 'ok' THEN 1 ELSE 0 END) AS reconciliation_mismatch_count,
                SUM(CASE WHEN latest_sign_attempt_id IS NOT NULL THEN 1 ELSE 0 END) AS sign_requested_count,
                SUM(CASE WHEN latest_sign_attempt_status = 'rejected' THEN 1 ELSE 0 END) AS sign_rejected_count,
                SUM(CASE WHEN latest_submit_mode = 'dry_run' AND latest_submit_status = 'previewed' THEN 1 ELSE 0 END) AS submit_preview_count,
                SUM(CASE WHEN latest_submit_status = 'accepted' THEN 1 ELSE 0 END) AS submit_accepted_count,
                SUM(CASE WHEN latest_submit_status = 'rejected' THEN 1 ELSE 0 END) AS submit_rejected_count,
                SUM(CASE WHEN external_reconciliation_status = 'ok' THEN 1 ELSE 0 END) AS external_reconciliation_ok_count,
                SUM(CASE WHEN external_reconciliation_status IN ('external_order_mismatch', 'external_fill_mismatch') THEN 1 ELSE 0 END) AS external_reconciliation_mismatch_count,
                SUM(CASE WHEN external_reconciliation_status = 'external_state_unverified' THEN 1 ELSE 0 END) AS external_reconciliation_unverified_count,
                SUM(CASE WHEN live_prereq_attention_required THEN 1 ELSE 0 END) AS live_prereq_attention_required_count,
                SUM(CASE WHEN operator_attention_required THEN 1 ELSE 0 END) AS attention_required_count,
                MAX(latest_journal_created_at) AS latest_event_at
            FROM ui.execution_ticket_summary
            GROUP BY run_id
            """,
            table_row_counts=table_row_counts,
        )
        _create_table_from_src(
            con,
            target="ui.execution_exception_summary",
            sql_body="""
            SELECT
                ticket_id,
                run_id,
                request_id,
                wallet_id,
                strategy_id,
                strategy_version,
                market_id,
                execution_context_id,
                order_id,
                reservation_id,
                execution_result,
                gate_reason,
                reconciliation_status,
                reconciliation_discrepancy,
                latest_sign_attempt_status,
                latest_submit_status,
                latest_submit_mode,
                external_order_status,
                external_reconciliation_status,
                live_prereq_execution_status,
                live_prereq_attention_required,
                latest_transition_to_status,
                latest_transition_reason,
                latest_journal_event_type,
                operator_attention_required
            FROM ui.execution_ticket_summary
            WHERE operator_attention_required OR live_prereq_attention_required
            """,
            table_row_counts=table_row_counts,
        )
        _create_table_from_src(
            con,
            target="ui.live_prereq_execution_summary",
            sql_body="""
            SELECT
                run_id,
                ticket_id,
                request_id,
                wallet_id,
                strategy_id,
                strategy_version,
                market_id,
                order_id,
                execution_context_id,
                latest_sign_attempt_id,
                latest_sign_attempt_status,
                latest_sign_attempt_created_at,
                latest_submit_attempt_id,
                latest_submit_mode,
                latest_submit_status,
                latest_submit_created_at,
                external_order_status,
                external_order_observed_at,
                external_fill_count,
                external_filled_size,
                external_last_fill_at,
                external_reconciliation_status,
                external_reconciliation_discrepancy,
                live_prereq_execution_status,
                live_prereq_attention_required
            FROM ui.execution_ticket_summary
            WHERE latest_sign_attempt_id IS NOT NULL
               OR latest_submit_attempt_id IS NOT NULL
               OR external_order_observation_id IS NOT NULL
               OR external_reconciliation_id IS NOT NULL
            """,
            table_row_counts=table_row_counts,
        )
        _create_table_from_src(
            con,
            target="ui.live_prereq_wallet_summary",
            sql_body="""
            WITH wallet_base AS (
                SELECT
                    wallet_id,
                    funder,
                    signature_type,
                    can_trade,
                    restricted_reason,
                    can_use_relayer,
                    CASE
                        WHEN allowance_targets IS NULL OR allowance_targets = '[]' THEN 0
                        ELSE json_array_length(try_cast(allowance_targets AS JSON))
                    END AS configured_allowance_target_count
                FROM src.capability.account_trading_capabilities
            ),
            latest_native_gas AS (
                SELECT wallet_id, observed_quantity, observed_at
                FROM (
                    SELECT
                        wallet_id,
                        observed_quantity,
                        observed_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY wallet_id
                            ORDER BY observed_at DESC, observation_id DESC
                        ) AS rn
                    FROM src.runtime.external_balance_observations
                    WHERE observation_kind = 'wallet_balance'
                      AND asset_type = 'native_gas'
                )
                WHERE rn = 1
            ),
            latest_usdc_balance AS (
                SELECT wallet_id, observed_quantity, observed_at
                FROM (
                    SELECT
                        wallet_id,
                        observed_quantity,
                        observed_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY wallet_id
                            ORDER BY observed_at DESC, observation_id DESC
                        ) AS rn
                    FROM src.runtime.external_balance_observations
                    WHERE observation_kind = 'wallet_balance'
                      AND token_id = 'usdc_e'
                )
                WHERE rn = 1
            ),
            allowance_agg AS (
                SELECT
                    wallet_id,
                    COUNT(DISTINCT allowance_target) AS observed_allowance_target_count,
                    COUNT(DISTINCT CASE WHEN observed_quantity > 0 THEN allowance_target END) AS approved_allowance_target_count,
                    MAX(observed_at) AS latest_allowance_observed_at
                FROM src.runtime.external_balance_observations
                WHERE observation_kind = 'token_allowance'
                  AND token_id = 'usdc_e'
                GROUP BY wallet_id
            ),
            latest_signer AS (
                SELECT funder, status, error, created_at
                FROM (
                    SELECT
                        funder,
                        status,
                        error,
                        COALESCE(created_at, timestamp) AS created_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY funder
                            ORDER BY COALESCE(created_at, timestamp) DESC, log_id DESC
                        ) AS rn
                    FROM src.meta.signature_audit_logs
                    WHERE funder IS NOT NULL
                )
                WHERE rn = 1
            ),
            latest_chain_tx AS (
                SELECT wallet_id, attempt_id, tx_kind, tx_mode, status, error, created_at
                FROM (
                    SELECT
                        wallet_id,
                        attempt_id,
                        tx_kind,
                        tx_mode,
                        status,
                        error,
                        created_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY wallet_id
                            ORDER BY created_at DESC, attempt_id DESC
                        ) AS rn
                    FROM src.runtime.chain_tx_attempts
                )
                WHERE rn = 1
            )
            SELECT
                wallet.wallet_id,
                wallet.funder,
                wallet.signature_type,
                wallet.can_trade,
                wallet.restricted_reason,
                wallet.can_use_relayer,
                wallet.configured_allowance_target_count,
                native_gas.observed_quantity AS latest_native_gas_quantity,
                native_gas.observed_at AS latest_native_gas_observed_at,
                usdc_balance.observed_quantity AS latest_usdc_balance_quantity,
                usdc_balance.observed_at AS latest_usdc_balance_observed_at,
                COALESCE(allowance.observed_allowance_target_count, 0) AS observed_allowance_target_count,
                COALESCE(allowance.approved_allowance_target_count, 0) AS approved_allowance_target_count,
                allowance.latest_allowance_observed_at,
                signer.status AS latest_signer_status,
                signer.error AS latest_signer_error,
                signer.created_at AS latest_signer_created_at,
                chain_tx.attempt_id AS latest_chain_tx_attempt_id,
                chain_tx.tx_kind AS latest_chain_tx_kind,
                chain_tx.tx_mode AS latest_chain_tx_mode,
                chain_tx.status AS latest_chain_tx_status,
                chain_tx.error AS latest_chain_tx_error,
                chain_tx.created_at AS latest_chain_tx_created_at,
                CASE
                    WHEN wallet.can_trade = FALSE THEN 'capability_blocked'
                    WHEN native_gas.observed_at IS NULL OR usdc_balance.observed_at IS NULL THEN 'missing_wallet_state_observation'
                    WHEN wallet.configured_allowance_target_count > COALESCE(allowance.observed_allowance_target_count, 0) THEN 'allowance_unverified'
                    WHEN wallet.configured_allowance_target_count > 0 AND COALESCE(allowance.approved_allowance_target_count, 0) = 0 THEN 'allowance_action_required'
                    WHEN chain_tx.tx_kind = 'approve_usdc' AND chain_tx.status = 'rejected' THEN 'approve_action_required'
                    ELSE 'ready'
                END AS wallet_readiness_status,
                printf(
                    '[%s%s%s%s%s%s]',
                    CASE WHEN wallet.can_trade = FALSE THEN '"capability_blocked"' ELSE '' END,
                    CASE WHEN native_gas.observed_at IS NULL THEN CASE WHEN wallet.can_trade = FALSE THEN ',"missing_native_gas_observation"' ELSE '"missing_native_gas_observation"' END ELSE '' END,
                    CASE WHEN usdc_balance.observed_at IS NULL THEN CASE WHEN wallet.can_trade = FALSE OR native_gas.observed_at IS NULL THEN ',"missing_usdc_balance_observation"' ELSE '"missing_usdc_balance_observation"' END ELSE '' END,
                    CASE WHEN wallet.configured_allowance_target_count > COALESCE(allowance.observed_allowance_target_count, 0) THEN CASE WHEN wallet.can_trade = FALSE OR native_gas.observed_at IS NULL OR usdc_balance.observed_at IS NULL THEN ',"allowance_target_unobserved"' ELSE '"allowance_target_unobserved"' END ELSE '' END,
                    CASE WHEN wallet.configured_allowance_target_count > 0 AND COALESCE(allowance.approved_allowance_target_count, 0) = 0 THEN CASE WHEN wallet.can_trade = FALSE OR native_gas.observed_at IS NULL OR usdc_balance.observed_at IS NULL OR wallet.configured_allowance_target_count > COALESCE(allowance.observed_allowance_target_count, 0) THEN ',"allowance_not_approved"' ELSE '"allowance_not_approved"' END ELSE '' END,
                    CASE WHEN chain_tx.tx_kind = 'approve_usdc' AND chain_tx.status = 'rejected' THEN CASE WHEN wallet.can_trade = FALSE OR native_gas.observed_at IS NULL OR usdc_balance.observed_at IS NULL OR wallet.configured_allowance_target_count > COALESCE(allowance.observed_allowance_target_count, 0) OR (wallet.configured_allowance_target_count > 0 AND COALESCE(allowance.approved_allowance_target_count, 0) = 0) THEN ',"latest_approve_usdc_rejected"' ELSE '"latest_approve_usdc_rejected"' END ELSE '' END
                ) AS wallet_readiness_blockers_json,
                CASE
                    WHEN wallet.can_trade = FALSE THEN TRUE
                    WHEN native_gas.observed_at IS NULL OR usdc_balance.observed_at IS NULL THEN TRUE
                    WHEN wallet.configured_allowance_target_count > COALESCE(allowance.observed_allowance_target_count, 0) THEN TRUE
                    WHEN wallet.configured_allowance_target_count > 0 AND COALESCE(allowance.approved_allowance_target_count, 0) = 0 THEN TRUE
                    WHEN chain_tx.tx_kind = 'approve_usdc' AND chain_tx.status = 'rejected' THEN TRUE
                    WHEN signer.status = 'rejected' THEN TRUE
                    ELSE FALSE
                END AS attention_required
            FROM wallet_base wallet
            LEFT JOIN latest_native_gas native_gas ON native_gas.wallet_id = wallet.wallet_id
            LEFT JOIN latest_usdc_balance usdc_balance ON usdc_balance.wallet_id = wallet.wallet_id
            LEFT JOIN allowance_agg allowance ON allowance.wallet_id = wallet.wallet_id
            LEFT JOIN latest_signer signer ON signer.funder = wallet.funder
            LEFT JOIN latest_chain_tx chain_tx ON chain_tx.wallet_id = wallet.wallet_id
            """,
            table_row_counts=table_row_counts,
        )
        _create_table_from_src(
            con,
            target="ui.paper_run_journal_summary",
            sql_body="""
            WITH journal_agg AS (
                SELECT
                    run_id,
                    COUNT(*) AS event_count,
                    SUM(CASE WHEN entity_type = 'order' THEN 1 ELSE 0 END) AS order_event_count,
                    SUM(CASE WHEN entity_type = 'fill' THEN 1 ELSE 0 END) AS fill_event_count,
                    SUM(CASE WHEN event_type = 'reconciliation.mismatch' THEN 1 ELSE 0 END) AS mismatch_event_count,
                    MAX(created_at) AS latest_event_at
                FROM src.runtime.journal_events
                WHERE run_id IS NOT NULL
                GROUP BY run_id
            )
            SELECT
                run_summary.run_id,
                run_summary.wallet_id,
                COALESCE(journal.event_count, 0) AS event_count,
                run_summary.ticket_count,
                SUM(CASE WHEN ticket.order_id IS NOT NULL THEN 1 ELSE 0 END) AS order_count,
                SUM(CASE WHEN COALESCE(ticket.fill_count, 0) > 0 THEN 1 ELSE 0 END) AS fill_ticket_count,
                COALESCE(journal.order_event_count, 0) AS order_event_count,
                COALESCE(journal.fill_event_count, 0) AS fill_event_count,
                COALESCE(journal.mismatch_event_count, 0) AS mismatch_event_count,
                journal.latest_event_at
            FROM ui.execution_run_summary run_summary
            LEFT JOIN ui.execution_ticket_summary ticket ON ticket.run_id = run_summary.run_id
            LEFT JOIN journal_agg journal ON journal.run_id = run_summary.run_id
            GROUP BY
                run_summary.run_id,
                run_summary.wallet_id,
                journal.event_count,
                run_summary.ticket_count,
                journal.order_event_count,
                journal.fill_event_count,
                journal.mismatch_event_count,
                journal.latest_event_at
            """,
            table_row_counts=table_row_counts,
        )
        _create_table_from_src(
            con,
            target="ui.agent_review_summary",
            sql_body="""
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
                    FROM src.agent.invocations
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
                    FROM src.agent.outputs
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
                    FROM src.agent.reviews
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
                    FROM src.agent.evaluations
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
            """,
            table_row_counts=table_row_counts,
        )
        build_opportunity_tables(
            con,
            table_row_counts=table_row_counts,
            create_market_watch_summary=lambda: None,
            create_market_opportunity_summary=lambda: _create_market_opportunity_summary(
                con,
                table_row_counts=table_row_counts,
            ),
            create_calibration_health_summary=lambda: _create_calibration_health_summary(
                con,
                table_row_counts=table_row_counts,
            ),
        )
        _create_phase_readiness_summary(
            con,
            report_path=readiness_report_json_path,
            table_row_counts=table_row_counts,
        )
        _create_readiness_evidence_summary(
            con,
            evidence_path=readiness_evidence_json_path,
            table_row_counts=table_row_counts,
        )
        build_execution_tables(
            con,
            table_row_counts=table_row_counts,
            create_execution_ticket_summary=lambda: None,
            create_execution_run_summary=lambda: None,
            create_execution_exception_summary=lambda: None,
            create_predicted_vs_realized_summary=lambda: _create_predicted_vs_realized_summary(
                con,
                table_row_counts=table_row_counts,
            ),
            create_watch_only_vs_executed_summary=lambda: _create_watch_only_vs_executed_summary(
                con,
                table_row_counts=table_row_counts,
            ),
            create_execution_science_summary=lambda: _create_execution_science_summary(
                con,
                table_row_counts=table_row_counts,
            ),
            create_market_research_summary=lambda: _create_market_research_summary(
                con,
                table_row_counts=table_row_counts,
            ),
            create_market_microstructure_summary=lambda: _create_market_microstructure_summary(
                con,
                table_row_counts=table_row_counts,
            ),
        )
        _create_table_from_src(
            con,
            target="ui.daily_ops_summary",
            sql_body="""
            WITH latest_readiness AS (
                SELECT
                    MAX(generated_at) AS generated_at
                FROM ui.phase_readiness_summary
            ),
            readiness_summary AS (
                SELECT
                    MAX(go_decision) AS go_decision,
                    MAX(decision_reason) AS decision_reason,
                    MAX(generated_at) AS readiness_generated_at
                FROM ui.phase_readiness_summary
                WHERE generated_at = (SELECT generated_at FROM latest_readiness)
            )
            SELECT
                run_summary.run_id,
                run_summary.wallet_id,
                readiness.go_decision,
                readiness.decision_reason,
                run_summary.ticket_count,
                run_summary.filled_count,
                run_summary.partial_filled_count,
                run_summary.cancelled_count,
                run_summary.gate_rejected_count AS rejected_count,
                run_summary.reconciliation_mismatch_count,
                run_summary.attention_required_count,
                run_summary.latest_event_at,
                readiness.readiness_generated_at
            FROM ui.execution_run_summary run_summary
            LEFT JOIN readiness_summary readiness ON TRUE
            """,
            table_row_counts=table_row_counts,
        )
        _create_table_from_src(
            con,
            target="ui.daily_review_input",
            sql_body="""
            SELECT
                ticket.ticket_id AS item_id,
                ticket.run_id,
                ticket.ticket_id,
                ticket.request_id,
                ticket.wallet_id,
                ticket.strategy_id,
                ticket.strategy_version,
                ticket.market_id,
                ticket.order_id,
                ticket.execution_result,
                ticket.reconciliation_status,
                ticket.reconciliation_discrepancy,
                ticket.latest_transition_to_status,
                ticket.latest_transition_reason,
                ticket.latest_journal_event_type,
                ticket.operator_attention_required,
                ops.go_decision,
                printf(
                    '{"run_id":"%s","ticket_id":"%s","strategy_id":"%s","execution_result":"%s","reconciliation_status":%s,"latest_transition_to_status":%s,"operator_attention_required":%s}',
                    ticket.run_id,
                    ticket.ticket_id,
                    ticket.strategy_id,
                    ticket.execution_result,
                    CASE
                        WHEN ticket.reconciliation_status IS NULL THEN 'null'
                        ELSE printf('"%s"', ticket.reconciliation_status)
                    END,
                    CASE
                        WHEN ticket.latest_transition_to_status IS NULL THEN 'null'
                        ELSE printf('"%s"', ticket.latest_transition_to_status)
                    END,
                    CASE
                        WHEN ticket.operator_attention_required THEN 'true'
                        ELSE 'false'
                    END
                ) AS summary_json
            FROM ui.execution_ticket_summary ticket
            LEFT JOIN ui.daily_ops_summary ops ON ops.run_id = ticket.run_id
            """,
            table_row_counts=table_row_counts,
        )
        _create_opportunity_triage_summary(con, table_row_counts=table_row_counts)
        build_catalog_tables(con, table_row_counts=table_row_counts)
        _create_surface_delivery_summary(con, table_row_counts=table_row_counts)
        _create_system_runtime_summary(con, table_row_counts=table_row_counts)
        build_catalog_tables(con, table_row_counts=table_row_counts)
        _create_surface_delivery_summary(con, table_row_counts=table_row_counts)
        _create_system_runtime_summary(con, table_row_counts=table_row_counts)
        return table_row_counts
    finally:
        con.close()


def _create_phase_readiness_summary(con, *, report_path: Path, table_row_counts: dict[str, int]) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE ui.phase_readiness_summary (
            target TEXT,
            gate_name TEXT,
            status TEXT,
            passed BOOLEAN,
            all_passed BOOLEAN,
            go_decision TEXT,
            decision_reason TEXT,
            generated_at TIMESTAMP,
            checks_json TEXT,
            violations_json TEXT,
            warnings_json TEXT,
            metadata_json TEXT
        )
        """
    )
    if not report_path.exists():
        table_row_counts["ui.phase_readiness_summary"] = 0
        return
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    gate_results = list(payload.get("gate_results", []))
    rows = []
    for gate in gate_results:
        rows.append(
            [
                str(payload.get("target", "")),
                str(gate.get("gate_name", "")),
                "pass" if bool(gate.get("passed", False)) else "fail",
                bool(gate.get("passed", False)),
                bool(payload.get("all_passed", False)),
                str(payload.get("go_decision", "")),
                str(payload.get("decision_reason", "")),
                _coerce_ts(payload.get("generated_at")),
                json.dumps(gate.get("checks", {}), ensure_ascii=True, sort_keys=True),
                json.dumps(gate.get("violations", []), ensure_ascii=True, sort_keys=True),
                json.dumps(gate.get("warnings", []), ensure_ascii=True, sort_keys=True),
                json.dumps(gate.get("metadata", {}), ensure_ascii=True, sort_keys=True),
            ]
        )
    if rows:
        con.executemany(
            """
            INSERT INTO ui.phase_readiness_summary (
                target, gate_name, status, passed, all_passed, go_decision, decision_reason,
                generated_at, checks_json, violations_json, warnings_json, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    row = con.execute("SELECT COUNT(*) FROM ui.phase_readiness_summary").fetchone()
    table_row_counts["ui.phase_readiness_summary"] = int(row[0]) if row is not None else 0


def _create_readiness_evidence_summary(con, *, evidence_path: Path, table_row_counts: dict[str, int]) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE ui.readiness_evidence_summary (
            generated_at TIMESTAMP,
            go_decision TEXT,
            decision_reason TEXT,
            capability_manifest_status TEXT,
            capability_boundary_summary_json TEXT,
            dependency_statuses_json TEXT,
            artifact_freshness_json TEXT,
            latest_verification_summary_json TEXT,
            stale_dependencies_json TEXT,
            blockers_json TEXT,
            warnings_json TEXT,
            evidence_paths_json TEXT
        )
        """
    )
    if not evidence_path.exists():
        table_row_counts["ui.readiness_evidence_summary"] = 0
        return
    payload = json.loads(evidence_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        table_row_counts["ui.readiness_evidence_summary"] = 0
        return
    con.execute(
        """
        INSERT INTO ui.readiness_evidence_summary (
            generated_at,
            go_decision,
            decision_reason,
            capability_manifest_status,
            capability_boundary_summary_json,
            dependency_statuses_json,
            artifact_freshness_json,
            latest_verification_summary_json,
            stale_dependencies_json,
            blockers_json,
            warnings_json,
            evidence_paths_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            _coerce_ts(payload.get("generated_at")),
            str(payload.get("go_decision") or ""),
            str(payload.get("decision_reason") or ""),
            str(payload.get("capability_manifest_status") or ""),
            json.dumps(payload.get("capability_boundary_summary") or {}, ensure_ascii=True, sort_keys=True),
            json.dumps(payload.get("dependency_statuses") or {}, ensure_ascii=True, sort_keys=True),
            json.dumps(payload.get("artifact_freshness") or {}, ensure_ascii=True, sort_keys=True),
            json.dumps(payload.get("latest_verification_summary") or {}, ensure_ascii=True, sort_keys=True),
            json.dumps(payload.get("stale_dependencies") or [], ensure_ascii=True, sort_keys=True),
            json.dumps(payload.get("blockers") or [], ensure_ascii=True, sort_keys=True),
            json.dumps(payload.get("warnings") or [], ensure_ascii=True, sort_keys=True),
            json.dumps(payload.get("evidence_paths") or {}, ensure_ascii=True, sort_keys=True),
        ],
    )
    row = con.execute("SELECT COUNT(*) FROM ui.readiness_evidence_summary").fetchone()
    table_row_counts["ui.readiness_evidence_summary"] = int(row[0]) if row is not None else 0


def _empty_predicted_vs_realized_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "ticket_id",
            "run_id",
            "wallet_id",
            "strategy_id",
            "market_id",
            "order_id",
            "outcome",
            "predicted_edge_bps",
            "expected_fill_price",
            "realized_fill_price",
            "filled_quantity",
            "realized_notional",
            "realized_pnl",
            "resolution_value",
            "forecast_freshness",
            "source_disagreement",
            "post_trade_error",
            "evaluation_status",
            "latest_fill_at",
            "latest_resolution_at",
            "execution_lifecycle_stage",
            "fill_ratio",
            "adverse_fill_slippage_bps",
            "resolution_lag_hours",
            "miss_reason_bucket",
            "distortion_reason_codes_json",
            "source_badge",
            "source_truth_status",
            "is_degraded_source",
            "primary_score_label",
        ]
    )


def _build_execution_path_frame(con) -> pd.DataFrame:
    base = con.execute(
        """
        WITH fill_fee_agg AS (
            SELECT
                order_id,
                SUM(fee) AS total_fee
            FROM src.trading.fills
            GROUP BY order_id
        ),
        latest_resolution AS (
            SELECT
                market_id,
                expected_outcome,
                created_at,
                ROW_NUMBER() OVER (
                    PARTITION BY market_id
                    ORDER BY created_at DESC, verification_id DESC
                ) AS rn
            FROM src.resolution.settlement_verifications
        ),
        latest_replay_diff AS (
            SELECT
                replay.market_id,
                diff.diff_summary_json,
                ROW_NUMBER() OVER (
                    PARTITION BY replay.market_id
                    ORDER BY diff.created_at DESC, diff.diff_id DESC
                ) AS rn
            FROM src.weather.weather_forecast_replay_diffs diff
            JOIN src.weather.weather_forecast_replays replay
              ON replay.replay_id = diff.replay_id
        )
        SELECT
            ticket.ticket_id,
            ticket.run_id,
            ticket.wallet_id,
            ticket.strategy_id,
            ticket.market_id,
            exec.order_id,
            ticket.outcome,
            ticket.side,
            ticket.size,
            ticket.reference_price AS ticket_reference_price,
            ticket.edge_bps AS ticket_edge_bps,
            ticket.provenance_json,
            ticket.watch_snapshot_id,
            exec.gate_allowed,
            exec.order_status,
            exec.execution_result,
            exec.latest_submit_status,
            exec.external_order_status,
            exec.live_prereq_execution_status,
            exec.latest_sign_attempt_id,
            exec.latest_submit_attempt_id,
            exec.filled_size,
            exec.filled_notional,
            exec.avg_fill_price,
            exec.last_fill_at,
            fee.total_fee,
            resolution.expected_outcome,
            resolution.created_at AS latest_resolution_at,
            replay.diff_summary_json
        FROM src.runtime.trade_tickets ticket
        LEFT JOIN ui.execution_ticket_summary exec ON exec.ticket_id = ticket.ticket_id
        LEFT JOIN fill_fee_agg fee ON fee.order_id = exec.order_id
        LEFT JOIN latest_resolution resolution
          ON resolution.market_id = ticket.market_id
         AND resolution.rn = 1
        LEFT JOIN latest_replay_diff replay
          ON replay.market_id = ticket.market_id
         AND replay.rn = 1
        """
    ).df()
    if base.empty:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for _, item in base.iterrows():
        provenance = _json_object(item.get("provenance_json"))
        pricing_context = _json_object(provenance.get("pricing_context"))
        predicted_edge_bps = _coerce_float(pricing_context.get("edge_bps_executable"))
        if predicted_edge_bps is None:
            predicted_edge_bps = _coerce_float(item.get("ticket_edge_bps")) or 0.0
        expected_fill_price = _coerce_float(pricing_context.get("reference_price"))
        if expected_fill_price is None:
            expected_fill_price = _coerce_float(item.get("ticket_reference_price"))
        realized_fill_price = _coerce_float(item.get("avg_fill_price"))
        filled_quantity = _coerce_float(item.get("filled_size")) or 0.0
        realized_notional = _coerce_float(item.get("filled_notional")) or 0.0
        total_fee = _coerce_float(item.get("total_fee")) or 0.0
        ticket_size = _coerce_float(item.get("size")) or 0.0
        fill_ratio = (filled_quantity / ticket_size) if ticket_size > 0 else 0.0
        projection = build_resolved_execution_projection(
            outcome=item.get("outcome"),
            side=item.get("side"),
            expected_outcome=item.get("expected_outcome"),
            filled_quantity=filled_quantity,
            ticket_size=ticket_size,
            expected_fill_price=expected_fill_price,
            realized_fill_price=realized_fill_price,
            total_fee=total_fee,
            predicted_edge_bps=predicted_edge_bps,
            execution_result=item.get("execution_result"),
            order_status=item.get("order_status"),
            latest_submit_status=item.get("latest_submit_status"),
            live_prereq_execution_status=item.get("live_prereq_execution_status"),
            external_order_status=item.get("external_order_status"),
            gate_allowed=item.get("gate_allowed"),
            latest_sign_attempt_id=item.get("latest_sign_attempt_id"),
            latest_submit_attempt_id=item.get("latest_submit_attempt_id"),
            latest_fill_at=item.get("last_fill_at"),
            latest_resolution_at=item.get("latest_resolution_at"),
        )
        latest_fill_at = item.get("last_fill_at")
        latest_resolution_at = projection.latest_resolution_at
        source_disagreement = _source_disagreement(item.get("diff_summary_json"))
        distortion_reasons = _distortion_reason_codes(
            stage=projection.execution_lifecycle_stage,
            source_disagreement=source_disagreement,
            realized_pnl=projection.realized_pnl,
            adverse_fill_slippage_bps=projection.adverse_fill_slippage_bps,
        )
        rows.append(
            {
                "ticket_id": item.get("ticket_id"),
                "run_id": item.get("run_id"),
                "wallet_id": item.get("wallet_id"),
                "strategy_id": item.get("strategy_id"),
                "market_id": item.get("market_id"),
                "order_id": item.get("order_id"),
                "watch_snapshot_id": item.get("watch_snapshot_id"),
                "outcome": item.get("outcome"),
                "side": item.get("side"),
                "ticket_size": ticket_size,
                "predicted_edge_bps": predicted_edge_bps,
                "expected_fill_price": expected_fill_price,
                "realized_fill_price": realized_fill_price,
                "filled_quantity": filled_quantity,
                "realized_notional": realized_notional,
                "realized_pnl": projection.realized_pnl,
                "resolution_value": projection.resolution_value,
                "forecast_freshness": str(pricing_context.get("source_freshness_status") or "unavailable"),
                "source_disagreement": source_disagreement,
                "post_trade_error": projection.post_trade_error,
                "evaluation_status": projection.evaluation_status,
                "latest_fill_at": latest_fill_at,
                "latest_resolution_at": latest_resolution_at,
                "execution_lifecycle_stage": projection.execution_lifecycle_stage,
                "fill_ratio": projection.fill_ratio,
                "adverse_fill_slippage_bps": projection.adverse_fill_slippage_bps,
                "resolution_lag_hours": projection.resolution_lag_hours,
                "miss_reason_bucket": _miss_reason_bucket_for_stage(projection.execution_lifecycle_stage),
                "distortion_reason_codes_json": _json_array_text(distortion_reasons),
            }
        )
    frame = pd.DataFrame(rows)
    for column in [
        "ticket_id",
        "run_id",
        "wallet_id",
        "strategy_id",
        "market_id",
        "order_id",
        "watch_snapshot_id",
        "outcome",
        "side",
        "forecast_freshness",
        "source_disagreement",
        "evaluation_status",
        "execution_lifecycle_stage",
        "miss_reason_bucket",
        "distortion_reason_codes_json",
    ]:
        if column in frame.columns:
            frame[column] = frame[column].astype("string")
    return frame


def _load_latest_execution_feedback_priors(con) -> pd.DataFrame:
    if not _table_exists(con, "src.weather.weather_execution_priors"):
        return pd.DataFrame(
            columns=[
                "cohort_type",
                "cohort_key",
                "feedback_status",
                "feedback_penalty",
                "cohort_prior_version",
            ]
        )
    return con.execute(
        """
        SELECT
            cohort_type,
            cohort_key,
            feedback_status,
            feedback_penalty,
            cohort_prior_version
        FROM (
            SELECT
                cohort_type,
                cohort_key,
                feedback_status,
                feedback_penalty,
                cohort_prior_version,
                ROW_NUMBER() OVER (
                    PARTITION BY cohort_type, cohort_key
                    ORDER BY materialized_at DESC, feedback_penalty DESC, sample_count DESC, prior_key DESC
                ) AS rn
            FROM src.weather.weather_execution_priors
        )
        WHERE rn = 1
        """
    ).df()


def _merge_feedback_prior_fields(
    frame: pd.DataFrame,
    *,
    feedback_priors: pd.DataFrame,
    cohort_type: str,
    key_column: str,
) -> pd.DataFrame:
    out = frame.copy()
    if out.empty:
        if "feedback_status" not in out.columns:
            out["feedback_status"] = pd.Series(dtype="string")
        if "feedback_penalty" not in out.columns:
            out["feedback_penalty"] = pd.Series(dtype="float64")
        if "cohort_prior_version" not in out.columns:
            out["cohort_prior_version"] = pd.Series(dtype="string")
        return out
    if feedback_priors.empty or key_column not in out.columns:
        out["feedback_status"] = out.get("feedback_status", pd.Series(index=out.index, dtype="string")).fillna("heuristic_only")
        out["feedback_penalty"] = pd.to_numeric(
            out.get("feedback_penalty", pd.Series(index=out.index, dtype="float64")),
            errors="coerce",
        ).fillna(0.0)
        out["cohort_prior_version"] = out.get("cohort_prior_version", pd.Series(index=out.index, dtype="string"))
        return out
    prior_frame = feedback_priors[feedback_priors["cohort_type"] == cohort_type].copy()
    if prior_frame.empty:
        out["feedback_status"] = "heuristic_only"
        out["feedback_penalty"] = 0.0
        out["cohort_prior_version"] = None
        return out
    prior_frame["cohort_key"] = prior_frame["cohort_key"].astype("string")
    out[key_column] = out[key_column].astype("string")
    merged = out.merge(
        prior_frame[["cohort_key", "feedback_status", "feedback_penalty", "cohort_prior_version"]],
        how="left",
        left_on=key_column,
        right_on="cohort_key",
    )
    if "cohort_key_y" in merged.columns:
        merged = merged.drop(columns=["cohort_key_y"])
    if "cohort_key_x" in merged.columns and key_column != "cohort_key":
        merged = merged.rename(columns={"cohort_key_x": key_column})
    merged["feedback_status"] = merged["feedback_status"].fillna("heuristic_only").astype("string")
    merged["feedback_penalty"] = pd.to_numeric(merged["feedback_penalty"], errors="coerce").fillna(0.0)
    if "cohort_prior_version" in merged.columns:
        merged["cohort_prior_version"] = merged["cohort_prior_version"].astype("string")
    return merged


def _merge_feedback_prior_fields_by_cohort(frame: pd.DataFrame, *, feedback_priors: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if out.empty:
        if "feedback_status" not in out.columns:
            out["feedback_status"] = pd.Series(dtype="string")
        if "feedback_penalty" not in out.columns:
            out["feedback_penalty"] = pd.Series(dtype="float64")
        if "cohort_prior_version" not in out.columns:
            out["cohort_prior_version"] = pd.Series(dtype="string")
        return out
    if feedback_priors.empty:
        out["feedback_status"] = "heuristic_only"
        out["feedback_penalty"] = 0.0
        out["cohort_prior_version"] = None
        return out
    base = out.copy()
    base["cohort_type"] = base["cohort_type"].astype("string")
    base["cohort_key"] = base["cohort_key"].astype("string")
    priors = feedback_priors.copy()
    priors["cohort_type"] = priors["cohort_type"].astype("string")
    priors["cohort_key"] = priors["cohort_key"].astype("string")
    merged = base.merge(
        priors[["cohort_type", "cohort_key", "feedback_status", "feedback_penalty", "cohort_prior_version"]],
        how="left",
        on=["cohort_type", "cohort_key"],
    )
    merged["feedback_status"] = merged["feedback_status"].fillna("heuristic_only").astype("string")
    merged["feedback_penalty"] = pd.to_numeric(merged["feedback_penalty"], errors="coerce").fillna(0.0)
    merged["cohort_prior_version"] = merged["cohort_prior_version"].astype("string")
    return merged


def _create_predicted_vs_realized_summary(con, *, table_row_counts: dict[str, int]) -> None:
    con.execute("DROP TABLE IF EXISTS ui.predicted_vs_realized_summary")
    frame = _build_execution_path_frame(con)
    if frame.empty:
        con.register("predicted_vs_realized_df", _empty_predicted_vs_realized_frame())
    else:
        frame = annotate_frame_with_source_truth(
            frame,
            source_origin="ui_lite",
            derived=True,
            freshness_column="forecast_freshness",
        )
        con.register(
            "predicted_vs_realized_df",
            frame[
                [
                    "ticket_id",
                    "run_id",
                    "wallet_id",
                    "strategy_id",
                    "market_id",
                    "order_id",
                    "outcome",
                    "predicted_edge_bps",
                    "expected_fill_price",
                    "realized_fill_price",
                    "filled_quantity",
                    "realized_notional",
                    "realized_pnl",
                    "resolution_value",
                    "forecast_freshness",
                    "source_disagreement",
                    "post_trade_error",
                    "evaluation_status",
                    "latest_fill_at",
                    "latest_resolution_at",
                    "execution_lifecycle_stage",
                    "fill_ratio",
                    "adverse_fill_slippage_bps",
                    "resolution_lag_hours",
                    "miss_reason_bucket",
                    "distortion_reason_codes_json",
                    "source_badge",
                    "source_truth_status",
                    "is_degraded_source",
                    "primary_score_label",
                ]
            ],
        )
    con.execute("CREATE OR REPLACE TABLE ui.predicted_vs_realized_summary AS SELECT * FROM predicted_vs_realized_df")
    row = con.execute("SELECT COUNT(*) FROM ui.predicted_vs_realized_summary").fetchone()
    table_row_counts["ui.predicted_vs_realized_summary"] = int(row[0]) if row is not None else 0
    con.unregister("predicted_vs_realized_df")


def _create_watch_only_vs_executed_summary(con, *, table_row_counts: dict[str, int]) -> None:
    if not _table_exists(con, "src.weather.weather_watch_only_snapshots"):
        con.execute(
            """
            CREATE OR REPLACE TABLE ui.watch_only_vs_executed_summary (
                market_id TEXT,
                opportunity_count BIGINT,
                submitted_ticket_count BIGINT,
                filled_ticket_count BIGINT,
                resolved_ticket_count BIGINT,
                executed_ticket_count BIGINT,
                avg_model_edge_bps DOUBLE,
                avg_executable_edge_bps DOUBLE,
                avg_realized_pnl DOUBLE,
                submission_capture_ratio DOUBLE,
                fill_capture_ratio DOUBLE,
                resolution_capture_ratio DOUBLE,
                execution_capture_ratio DOUBLE,
                dominant_lifecycle_stage TEXT,
                miss_reason_bucket TEXT,
                distortion_reason_bucket TEXT,
                feedback_status TEXT,
                feedback_penalty DOUBLE,
                cohort_prior_version TEXT,
                source_badge TEXT,
                source_truth_status TEXT,
                is_degraded_source BOOLEAN,
                primary_score_label TEXT
            )
            """
        )
        table_row_counts["ui.watch_only_vs_executed_summary"] = 0
        return
    opportunity = con.execute(
        """
        SELECT
            snapshot_id,
            market_id,
            edge_bps AS executable_edge_bps,
            pricing_context_json
        FROM src.weather.weather_watch_only_snapshots
        WHERE decision = 'TAKE'
        """
    ).df()
    if opportunity.empty:
        con.execute(
            """
            CREATE OR REPLACE TABLE ui.watch_only_vs_executed_summary (
                market_id TEXT,
                opportunity_count BIGINT,
                submitted_ticket_count BIGINT,
                filled_ticket_count BIGINT,
                resolved_ticket_count BIGINT,
                executed_ticket_count BIGINT,
                avg_model_edge_bps DOUBLE,
                avg_executable_edge_bps DOUBLE,
                avg_realized_pnl DOUBLE,
                submission_capture_ratio DOUBLE,
                fill_capture_ratio DOUBLE,
                resolution_capture_ratio DOUBLE,
                execution_capture_ratio DOUBLE,
                dominant_lifecycle_stage TEXT,
                miss_reason_bucket TEXT,
                distortion_reason_bucket TEXT,
                feedback_status TEXT,
                feedback_penalty DOUBLE,
                cohort_prior_version TEXT,
                source_badge TEXT,
                source_truth_status TEXT,
                is_degraded_source BOOLEAN,
                primary_score_label TEXT
            )
            """
        )
        table_row_counts["ui.watch_only_vs_executed_summary"] = 0
        return

    execution_frame = _build_execution_path_frame(con)
    snapshot_rows: list[dict[str, Any]] = []
    submitted_stages = {"submitted_ack", "working_unfilled", "partially_filled", "filled_unresolved", "resolved", "cancelled", "submit_rejected"}
    for _, item in opportunity.iterrows():
        pricing_context = _json_object(item.get("pricing_context_json"))
        snapshot_id = item.get("snapshot_id")
        linked = execution_frame[execution_frame["watch_snapshot_id"] == snapshot_id] if (not execution_frame.empty and "watch_snapshot_id" in execution_frame.columns) else execution_frame.iloc[0:0]
        linked = linked.sort_values(
            by=["execution_lifecycle_stage", "latest_resolution_at", "latest_fill_at"],
            ascending=[True, False, False],
            key=lambda series: series.map(_lifecycle_sort_key) if series.name == "execution_lifecycle_stage" else series,
            kind="stable",
        ) if not linked.empty else linked
        best_stage = str(linked.iloc[0]["execution_lifecycle_stage"]) if not linked.empty else "ticket_created"
        best_miss_reason = str(linked.iloc[0]["miss_reason_bucket"]) if not linked.empty else "not_submitted"
        snapshot_rows.append(
            {
                "snapshot_id": snapshot_id,
                "market_id": item.get("market_id"),
                "model_edge_bps": _coerce_float(pricing_context.get("edge_bps_model")) or _coerce_float(item.get("executable_edge_bps")) or 0.0,
                "executable_edge_bps": _coerce_float(item.get("executable_edge_bps")) or 0.0,
                "stage": best_stage,
                "miss_reason_bucket": best_miss_reason,
                "has_submission": bool(not linked.empty and linked["execution_lifecycle_stage"].isin(submitted_stages).any()),
                "has_fill": bool(not linked.empty and (pd.to_numeric(linked["filled_quantity"], errors="coerce").fillna(0) > 0).any()),
                "has_resolution": bool(not linked.empty and (linked["evaluation_status"] == "resolved").any()),
                "resolved_source_disagreement": "different"
                if (not linked.empty and ((linked["evaluation_status"] == "resolved") & (linked["source_disagreement"] == "different")).any())
                else "match",
            }
        )
    snapshot_frame = pd.DataFrame(snapshot_rows)
    rows: list[dict[str, Any]] = []
    for market_id, market_opportunities in snapshot_frame.groupby("market_id", dropna=False):
        market_key = str(market_id)
        market_execution = execution_frame[execution_frame["market_id"] == market_id] if (not execution_frame.empty and "market_id" in execution_frame.columns) else execution_frame.iloc[0:0]
        opportunity_count = int(len(market_opportunities.index))
        submitted_ticket_count = int(
            market_execution[
                market_execution["execution_lifecycle_stage"].isin(list(submitted_stages))
            ]["ticket_id"].nunique()
        ) if not market_execution.empty else 0
        filled_ticket_count = int(
            market_execution[
                pd.to_numeric(market_execution["filled_quantity"], errors="coerce").fillna(0) > 0
            ]["ticket_id"].nunique()
        ) if not market_execution.empty else 0
        resolved_ticket_count = int(
            market_execution[
                market_execution["evaluation_status"] == "resolved"
            ]["ticket_id"].nunique()
        ) if not market_execution.empty else 0
        dominant_lifecycle_stage = _dominant_bucket(
            [str(value) for value in market_opportunities["stage"].dropna().tolist()],
            priority=_LIFECYCLE_PRIORITY,
            default="ticket_created",
        )
        uncaptured = [
            str(value)
            for value in market_opportunities["miss_reason_bucket"].dropna().tolist()
            if str(value) != "captured_resolved"
        ]
        if not uncaptured:
            miss_reason_bucket = "captured"
        else:
            miss_reason_bucket = _dominant_bucket(
                uncaptured,
                priority={**_MARKET_MISS_PRIORITY, "not_submitted": 3},
                default="not_submitted",
            )
        submission_capture_ratio = float(submitted_ticket_count / opportunity_count) if opportunity_count > 0 else 0.0
        fill_capture_ratio = float(filled_ticket_count / opportunity_count) if opportunity_count > 0 else 0.0
        resolution_capture_ratio = float(resolved_ticket_count / opportunity_count) if opportunity_count > 0 else 0.0
        resolved_disagreements = market_execution[
            market_execution["evaluation_status"] == "resolved"
        ]["source_disagreement"].dropna().astype(str).tolist() if not market_execution.empty else []
        dominant_resolved_disagreement = _dominant_bucket(
            resolved_disagreements,
            priority={"different": 0, "match": 1, "unavailable": 2},
            default="match",
        )
        if dominant_lifecycle_stage in {"working_unfilled", "partially_filled", "cancelled", "submit_rejected"}:
            distortion_reason_bucket = "execution_distortion"
        elif opportunity_count > 0 and submission_capture_ratio == 0.0:
            distortion_reason_bucket = "ranking_distortion"
        elif resolved_ticket_count > 0 and dominant_resolved_disagreement == "different":
            distortion_reason_bucket = "forecast_distortion"
        else:
            distortion_reason_bucket = "none"
        rows.append(
            {
                "market_id": market_key,
                "opportunity_count": opportunity_count,
                "submitted_ticket_count": submitted_ticket_count,
                "filled_ticket_count": filled_ticket_count,
                "resolved_ticket_count": resolved_ticket_count,
                "executed_ticket_count": filled_ticket_count,
                "avg_model_edge_bps": float(pd.to_numeric(market_opportunities["model_edge_bps"], errors="coerce").dropna().mean()) if "model_edge_bps" in market_opportunities.columns else None,
                "avg_executable_edge_bps": float(pd.to_numeric(market_opportunities["executable_edge_bps"], errors="coerce").dropna().mean()) if "executable_edge_bps" in market_opportunities.columns else None,
                "avg_realized_pnl": float(pd.to_numeric(market_execution["realized_pnl"], errors="coerce").dropna().mean()) if ("realized_pnl" in market_execution.columns and not market_execution.empty) else None,
                "submission_capture_ratio": submission_capture_ratio,
                "fill_capture_ratio": fill_capture_ratio,
                "resolution_capture_ratio": resolution_capture_ratio,
                "execution_capture_ratio": fill_capture_ratio,
                "dominant_lifecycle_stage": dominant_lifecycle_stage,
                "miss_reason_bucket": miss_reason_bucket,
                "distortion_reason_bucket": distortion_reason_bucket,
            }
        )
    feedback_priors = _load_latest_execution_feedback_priors(con)
    frame = _merge_feedback_prior_fields(
        pd.DataFrame(rows),
        feedback_priors=feedback_priors,
        cohort_type="market",
        key_column="market_id",
    )
    frame = annotate_frame_with_source_truth(
        frame,
        source_origin="ui_lite",
        derived=True,
    )
    if "market_id" in frame.columns:
        frame["market_id"] = frame["market_id"].astype("string")
    con.register("watch_only_vs_executed_df", frame)
    con.execute("CREATE OR REPLACE TABLE ui.watch_only_vs_executed_summary AS SELECT * FROM watch_only_vs_executed_df")
    row = con.execute("SELECT COUNT(*) FROM ui.watch_only_vs_executed_summary").fetchone()
    table_row_counts["ui.watch_only_vs_executed_summary"] = int(row[0]) if row is not None else 0
    con.unregister("watch_only_vs_executed_df")


def _create_execution_science_summary(con, *, table_row_counts: dict[str, int]) -> None:
    frame = _build_execution_path_frame(con)
    if frame.empty:
        con.execute(
            """
            CREATE OR REPLACE TABLE ui.execution_science_summary (
                cohort_type TEXT,
                cohort_key TEXT,
                ticket_count BIGINT,
                submitted_ack_count BIGINT,
                filled_ticket_count BIGINT,
                resolved_ticket_count BIGINT,
                partial_fill_count BIGINT,
                cancelled_count BIGINT,
                rejected_count BIGINT,
                working_unfilled_count BIGINT,
                avg_predicted_edge_bps DOUBLE,
                avg_realized_pnl DOUBLE,
                avg_post_trade_error DOUBLE,
                avg_adverse_fill_slippage_bps DOUBLE,
                submission_capture_ratio DOUBLE,
                fill_capture_ratio DOUBLE,
                resolution_capture_ratio DOUBLE,
                dominant_miss_reason_bucket TEXT,
                dominant_distortion_reason_bucket TEXT,
                feedback_status TEXT,
                feedback_penalty DOUBLE,
                cohort_prior_version TEXT,
                source_badge TEXT,
                source_truth_status TEXT,
                is_degraded_source BOOLEAN,
                primary_score_label TEXT
            )
            """
        )
        table_row_counts["ui.execution_science_summary"] = 0
        return

    summaries = build_execution_science_cohort_summaries(frame)
    feedback_priors = _load_latest_execution_feedback_priors(con)
    rows: list[dict[str, Any]] = []
    for summary in summaries:
        cohort_frame = frame[frame[f"{summary.cohort_type}_id" if summary.cohort_type != "market" else "market_id"] == summary.cohort_key]
        rows.append(
            {
                "cohort_type": summary.cohort_type,
                "cohort_key": summary.cohort_key,
                "ticket_count": summary.ticket_count,
                "submitted_ack_count": summary.submitted_ack_count,
                "filled_ticket_count": summary.filled_ticket_count,
                "resolved_ticket_count": summary.resolved_ticket_count,
                "partial_fill_count": summary.partial_fill_count,
                "cancelled_count": summary.cancelled_count,
                "rejected_count": summary.rejected_count,
                "working_unfilled_count": summary.working_unfilled_count,
                "avg_predicted_edge_bps": float(pd.to_numeric(cohort_frame["predicted_edge_bps"], errors="coerce").dropna().mean()) if ("predicted_edge_bps" in cohort_frame.columns and not cohort_frame.empty) else None,
                "avg_realized_pnl": float(pd.to_numeric(cohort_frame["realized_pnl"], errors="coerce").dropna().mean()) if ("realized_pnl" in cohort_frame.columns and not cohort_frame.empty) else None,
                "avg_post_trade_error": float(pd.to_numeric(cohort_frame["post_trade_error"], errors="coerce").dropna().mean()) if ("post_trade_error" in cohort_frame.columns and not cohort_frame.empty) else None,
                "avg_adverse_fill_slippage_bps": float(pd.to_numeric(cohort_frame["adverse_fill_slippage_bps"], errors="coerce").dropna().mean()) if ("adverse_fill_slippage_bps" in cohort_frame.columns and not cohort_frame.empty) else None,
                "submission_capture_ratio": float(summary.submitted_ack_count / summary.ticket_count) if summary.ticket_count > 0 else 0.0,
                "fill_capture_ratio": float(summary.filled_ticket_count / summary.ticket_count) if summary.ticket_count > 0 else 0.0,
                "resolution_capture_ratio": float(summary.resolved_ticket_count / summary.ticket_count) if summary.ticket_count > 0 else 0.0,
                "dominant_miss_reason_bucket": summary.dominant_miss_reason_bucket,
                "dominant_distortion_reason_bucket": summary.dominant_distortion_reason_bucket,
            }
        )
    science = _merge_feedback_prior_fields_by_cohort(
        pd.DataFrame(
            rows,
            columns=[
                "cohort_type",
                "cohort_key",
                "ticket_count",
                "submitted_ack_count",
                "filled_ticket_count",
                "resolved_ticket_count",
                "partial_fill_count",
                "cancelled_count",
                "rejected_count",
                "working_unfilled_count",
                "avg_predicted_edge_bps",
                "avg_realized_pnl",
                "avg_post_trade_error",
                "avg_adverse_fill_slippage_bps",
                "submission_capture_ratio",
                "fill_capture_ratio",
                "resolution_capture_ratio",
                "dominant_miss_reason_bucket",
                "dominant_distortion_reason_bucket",
            ],
        ),
        feedback_priors=feedback_priors,
    )
    science = annotate_frame_with_source_truth(
        science,
        source_origin="ui_lite",
        derived=True,
    )
    for column in ["cohort_type", "cohort_key", "dominant_miss_reason_bucket", "dominant_distortion_reason_bucket"]:
        if column in science.columns:
            science[column] = science[column].astype("string")
    con.register("execution_science_df", science)
    con.execute("CREATE OR REPLACE TABLE ui.execution_science_summary AS SELECT * FROM execution_science_df")
    row = con.execute("SELECT COUNT(*) FROM ui.execution_science_summary").fetchone()
    table_row_counts["ui.execution_science_summary"] = int(row[0]) if row is not None else 0
    con.unregister("execution_science_df")


def _create_market_research_summary(con, *, table_row_counts: dict[str, int]) -> None:
    opportunity = con.execute(
        """
        SELECT
            market_id,
            market_price AS latest_reference_price,
            model_fair_value AS latest_model_fair_value,
            COALESCE(execution_adjusted_fair_value, fair_value) AS latest_execution_adjusted_fair_value,
            mapping_confidence AS latest_mapping_confidence,
            source_freshness_status AS latest_source_freshness_status,
            market_quality_status AS latest_market_quality_status
        FROM ui.market_opportunity_summary
        """
    ).df()
    watch_only = con.execute(
        """
        SELECT
            market_id,
            submission_capture_ratio,
            fill_capture_ratio,
            resolution_capture_ratio,
            dominant_lifecycle_stage,
            miss_reason_bucket,
            distortion_reason_bucket
        FROM ui.watch_only_vs_executed_summary
        """
    ).df()
    executed = con.execute(
        """
        SELECT
            market_id,
            COUNT(*) FILTER (WHERE evaluation_status = 'resolved') AS resolved_trade_count,
            COUNT(*) FILTER (WHERE evaluation_status = 'pending_resolution') AS filled_unresolved_count,
            COUNT(*) FILTER (
                WHERE execution_lifecycle_stage IN ('submitted_ack', 'working_unfilled', 'submit_rejected', 'cancelled', 'partially_filled')
            ) AS submitted_only_count,
            AVG(post_trade_error) AS avg_post_trade_error
        FROM ui.predicted_vs_realized_summary
        GROUP BY market_id
        """
    ).df()
    for frame in [opportunity, watch_only, executed]:
        if "market_id" in frame.columns:
            frame["market_id"] = frame["market_id"].astype("string")
    base = opportunity.merge(watch_only, on="market_id", how="left").merge(executed, on="market_id", how="left")
    rows: list[dict[str, Any]] = []
    for _, item in base.iterrows():
        resolved_trade_count = _coerce_int(item.get("resolved_trade_count")) or 0
        filled_unresolved_count = _coerce_int(item.get("filled_unresolved_count")) or 0
        submitted_only_count = _coerce_int(item.get("submitted_only_count")) or 0
        if resolved_trade_count > 0:
            executed_evidence_status = "resolved"
        elif filled_unresolved_count > 0:
            executed_evidence_status = "filled_unresolved"
        elif submitted_only_count > 0:
            executed_evidence_status = "submitted_only"
        else:
            executed_evidence_status = "watch_only"
        rows.append(
            {
                "market_id": item.get("market_id"),
                "latest_reference_price": _coerce_float(item.get("latest_reference_price")),
                "latest_model_fair_value": _coerce_float(item.get("latest_model_fair_value")),
                "latest_execution_adjusted_fair_value": _coerce_float(item.get("latest_execution_adjusted_fair_value")),
                "latest_mapping_confidence": _coerce_float(item.get("latest_mapping_confidence")),
                "latest_source_freshness_status": item.get("latest_source_freshness_status"),
                "latest_market_quality_status": item.get("latest_market_quality_status"),
                "executed_evidence_status": executed_evidence_status,
                "resolved_trade_count": resolved_trade_count,
                "avg_post_trade_error": _coerce_float(item.get("avg_post_trade_error")),
                "submission_capture_ratio": _coerce_float(item.get("submission_capture_ratio")) or 0.0,
                "fill_capture_ratio": _coerce_float(item.get("fill_capture_ratio")) or 0.0,
                "resolution_capture_ratio": _coerce_float(item.get("resolution_capture_ratio")) or 0.0,
                "latest_execution_lifecycle_stage": item.get("dominant_lifecycle_stage"),
                "dominant_miss_reason_bucket": item.get("miss_reason_bucket"),
                "dominant_distortion_reason_bucket": item.get("distortion_reason_bucket"),
            }
        )
    frame = pd.DataFrame(
        rows,
        columns=[
            "market_id",
            "latest_reference_price",
            "latest_model_fair_value",
            "latest_execution_adjusted_fair_value",
            "latest_mapping_confidence",
            "latest_source_freshness_status",
            "latest_market_quality_status",
            "executed_evidence_status",
            "resolved_trade_count",
            "avg_post_trade_error",
            "submission_capture_ratio",
            "fill_capture_ratio",
            "resolution_capture_ratio",
            "latest_execution_lifecycle_stage",
            "dominant_miss_reason_bucket",
            "dominant_distortion_reason_bucket",
        ],
    )
    if "market_id" in frame.columns:
        frame["market_id"] = frame["market_id"].astype("string")
    con.register("market_research_df", frame)
    con.execute("CREATE OR REPLACE TABLE ui.market_research_summary AS SELECT * FROM market_research_df")
    row = con.execute("SELECT COUNT(*) FROM ui.market_research_summary").fetchone()
    table_row_counts["ui.market_research_summary"] = int(row[0]) if row is not None else 0
    con.unregister("market_research_df")


def _create_market_microstructure_summary(con, *, table_row_counts: dict[str, int]) -> None:
    if not _table_exists(con, "src.runtime.execution_intelligence_summaries"):
        con.execute(
            """
            CREATE OR REPLACE TABLE ui.market_microstructure_summary (
                summary_id TEXT,
                run_id TEXT,
                market_id TEXT,
                side TEXT,
                quote_imbalance_score DOUBLE,
                top_of_book_stability DOUBLE,
                book_update_intensity DOUBLE,
                spread_regime TEXT,
                visible_size_shock_flag BOOLEAN,
                book_pressure_side TEXT,
                expected_capture_regime TEXT,
                expected_slippage_regime TEXT,
                execution_intelligence_score DOUBLE,
                reason_codes_json TEXT,
                source_window_start TIMESTAMP,
                source_window_end TIMESTAMP,
                materialized_at TIMESTAMP,
                source_badge TEXT,
                source_truth_status TEXT,
                is_degraded_source BOOLEAN,
                primary_score_label TEXT
            )
            """
        )
        table_row_counts["ui.market_microstructure_summary"] = 0
        return

    frame = con.execute(
        """
        SELECT
            summary_id,
            run_id,
            market_id,
            side,
            quote_imbalance_score,
            top_of_book_stability,
            book_update_intensity,
            spread_regime,
            visible_size_shock_flag,
            book_pressure_side,
            expected_capture_regime,
            expected_slippage_regime,
            execution_intelligence_score,
            reason_codes_json,
            source_window_start,
            source_window_end,
            materialized_at
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY market_id, side
                    ORDER BY materialized_at DESC, execution_intelligence_score DESC, summary_id DESC
                ) AS rn
            FROM src.runtime.execution_intelligence_summaries
        )
        WHERE rn = 1
        """
    ).df()
    if frame.empty:
        con.execute(
            """
            CREATE OR REPLACE TABLE ui.market_microstructure_summary (
                summary_id TEXT,
                run_id TEXT,
                market_id TEXT,
                side TEXT,
                quote_imbalance_score DOUBLE,
                top_of_book_stability DOUBLE,
                book_update_intensity DOUBLE,
                spread_regime TEXT,
                visible_size_shock_flag BOOLEAN,
                book_pressure_side TEXT,
                expected_capture_regime TEXT,
                expected_slippage_regime TEXT,
                execution_intelligence_score DOUBLE,
                reason_codes_json TEXT,
                source_window_start TIMESTAMP,
                source_window_end TIMESTAMP,
                materialized_at TIMESTAMP,
                source_badge TEXT,
                source_truth_status TEXT,
                is_degraded_source BOOLEAN,
                primary_score_label TEXT
            )
            """
        )
        table_row_counts["ui.market_microstructure_summary"] = 0
        return
    frame = annotate_frame_with_source_truth(
        frame,
        source_origin="ui_lite",
        derived=False,
    )
    con.register("market_microstructure_summary_df", frame)
    con.execute("CREATE OR REPLACE TABLE ui.market_microstructure_summary AS SELECT * FROM market_microstructure_summary_df")
    row = con.execute("SELECT COUNT(*) FROM ui.market_microstructure_summary").fetchone()
    table_row_counts["ui.market_microstructure_summary"] = int(row[0]) if row is not None else 0
    con.unregister("market_microstructure_summary_df")


def _create_calibration_health_summary(con, *, table_row_counts: dict[str, int]) -> None:
    if not _table_exists(con, "src.weather.forecast_calibration_profiles_v2"):
        con.execute(
            """
            CREATE OR REPLACE TABLE ui.calibration_health_summary (
                station_id TEXT,
                source TEXT,
                metric TEXT,
                forecast_horizon_bucket TEXT,
                season_bucket TEXT,
                regime_bucket TEXT,
                sample_count BIGINT,
                mean_abs_residual DOUBLE,
                p90_abs_residual DOUBLE,
                calibration_health_status TEXT,
                threshold_profile_present BOOLEAN,
                window_end TIMESTAMP,
                materialized_at TIMESTAMP,
                calibration_freshness_status TEXT,
                profile_age_hours DOUBLE,
                impacted_market_count BIGINT,
                hard_gate_market_count BIGINT,
                review_required_market_count BIGINT,
                research_only_market_count BIGINT
            )
            """
        )
        table_row_counts["ui.calibration_health_summary"] = 0
        return
    base = con.execute(
        """
        SELECT
            station_id,
            source,
            metric,
            forecast_horizon_bucket,
            season_bucket,
            regime_bucket,
            sample_count,
            mean_abs_residual,
            p90_abs_residual,
            calibration_health_status,
            threshold_probability_profile_json,
            window_end,
            materialized_at
        FROM src.weather.forecast_calibration_profiles_v2
        """
    ).df()
    if base.empty:
        con.execute(
            """
            CREATE OR REPLACE TABLE ui.calibration_health_summary (
                station_id TEXT,
                source TEXT,
                metric TEXT,
                forecast_horizon_bucket TEXT,
                season_bucket TEXT,
                regime_bucket TEXT,
                sample_count BIGINT,
                mean_abs_residual DOUBLE,
                p90_abs_residual DOUBLE,
                calibration_health_status TEXT,
                threshold_profile_present BOOLEAN,
                window_end TIMESTAMP,
                materialized_at TIMESTAMP,
                calibration_freshness_status TEXT,
                profile_age_hours DOUBLE,
                impacted_market_count BIGINT,
                hard_gate_market_count BIGINT,
                review_required_market_count BIGINT,
                research_only_market_count BIGINT
            )
            """
        )
        table_row_counts["ui.calibration_health_summary"] = 0
        return
    gate_counts_by_station: dict[str, dict[str, int]] = {}
    if _table_exists(con, "ui.market_opportunity_summary"):
        counts_frame = con.execute(
            """
            SELECT
                station_id,
                SUM(CASE WHEN COALESCE(calibration_impacted_market, FALSE) THEN 1 ELSE 0 END) AS impacted_market_count,
                SUM(CASE WHEN calibration_gate_status IN ('review_required', 'research_only', 'blocked') THEN 1 ELSE 0 END) AS hard_gate_market_count,
                SUM(CASE WHEN calibration_gate_status = 'review_required' THEN 1 ELSE 0 END) AS review_required_market_count,
                SUM(CASE WHEN calibration_gate_status = 'research_only' THEN 1 ELSE 0 END) AS research_only_market_count
            FROM ui.market_opportunity_summary
            GROUP BY station_id
            """
        ).df()
        gate_counts_by_station = {
            str(item["station_id"]): {
                "impacted_market_count": int(_coerce_float(item.get("impacted_market_count")) or 0),
                "hard_gate_market_count": int(_coerce_float(item.get("hard_gate_market_count")) or 0),
                "review_required_market_count": int(_coerce_float(item.get("review_required_market_count")) or 0),
                "research_only_market_count": int(_coerce_float(item.get("research_only_market_count")) or 0),
            }
            for item in counts_frame.to_dict(orient="records")
            if item.get("station_id") not in {None, ""}
        }
    rows: list[dict[str, Any]] = []
    now = pd.Timestamp.utcnow().tz_localize(None)
    for item in base.to_dict(orient="records"):
        materialized_at = pd.to_datetime(item.get("materialized_at"), errors="coerce")
        profile_age_hours = None
        freshness_status = "degraded_or_missing"
        if pd.notna(materialized_at):
            profile_age_hours = max(0.0, float((now - materialized_at.to_pydatetime()).total_seconds()) / 3600.0)
            if profile_age_hours <= 36.0:
                freshness_status = "fresh"
            elif profile_age_hours <= 96.0:
                freshness_status = "stale"
        gate_counts = gate_counts_by_station.get(str(item.get("station_id") or ""), {})
        rows.append(
            {
                "station_id": item.get("station_id"),
                "source": item.get("source"),
                "metric": item.get("metric"),
                "forecast_horizon_bucket": item.get("forecast_horizon_bucket"),
                "season_bucket": item.get("season_bucket"),
                "regime_bucket": item.get("regime_bucket"),
                "sample_count": int(_coerce_float(item.get("sample_count")) or 0),
                "mean_abs_residual": _coerce_float(item.get("mean_abs_residual")),
                "p90_abs_residual": _coerce_float(item.get("p90_abs_residual")),
                "calibration_health_status": item.get("calibration_health_status"),
                "threshold_profile_present": bool(item.get("threshold_probability_profile_json")),
                "window_end": item.get("window_end"),
                "materialized_at": item.get("materialized_at"),
                "calibration_freshness_status": freshness_status,
                "profile_age_hours": profile_age_hours,
                "impacted_market_count": int(gate_counts.get("impacted_market_count", 0)),
                "hard_gate_market_count": int(gate_counts.get("hard_gate_market_count", 0)),
                "review_required_market_count": int(gate_counts.get("review_required_market_count", 0)),
                "research_only_market_count": int(gate_counts.get("research_only_market_count", 0)),
            }
        )
    frame = pd.DataFrame(rows)
    con.register("calibration_health_df", frame)
    con.execute("CREATE OR REPLACE TABLE ui.calibration_health_summary AS SELECT * FROM calibration_health_df")
    row = con.execute("SELECT COUNT(*) FROM ui.calibration_health_summary").fetchone()
    table_row_counts["ui.calibration_health_summary"] = int(row[0]) if row is not None else 0
    con.unregister("calibration_health_df")


def _create_market_opportunity_summary(con, *, table_row_counts: dict[str, int]) -> None:
    frame = con.execute(
        """
        WITH latest_signal AS (
            SELECT
                market_id,
                snapshot_id,
                token_id,
                outcome,
                decision,
                side,
                edge_bps,
                threshold_bps,
                reference_price,
                fair_value,
                pricing_context_json,
                created_at
            FROM (
                SELECT
                    market_id,
                    snapshot_id,
                    token_id,
                    outcome,
                    decision,
                    side,
                    edge_bps,
                    threshold_bps,
                    reference_price,
                    fair_value,
                    pricing_context_json,
                    created_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY market_id
                        ORDER BY
                            CASE WHEN decision = 'TAKE' THEN 0 ELSE 1 END,
                            COALESCE(
                                TRY_CAST(
                                    json_extract_string(try_cast(pricing_context_json AS JSON), '$.ranking_score') AS DOUBLE
                                ),
                                -1.0
                            ) DESC,
                            ABS(COALESCE(edge_bps, 0)) DESC,
                            created_at DESC,
                            snapshot_id DESC
                    ) AS rn
                FROM src.weather.weather_watch_only_snapshots
            )
            WHERE rn = 1
        ),
        agent_rollup AS (
            SELECT
                subject_id AS market_id,
                MAX(CASE WHEN invocation_status = 'failure' THEN 1 ELSE 0 END) AS has_failure,
                MAX(CASE WHEN COALESCE(human_review_required, FALSE) THEN 1 ELSE 0 END) AS has_review_required,
                MAX(CASE WHEN invocation_status = 'success' THEN 1 ELSE 0 END) AS has_success,
                MAX(COALESCE(CAST(confidence AS DOUBLE), 0.0)) AS max_confidence,
                MAX(updated_at) AS updated_at
            FROM ui.agent_review_summary
            WHERE subject_type = 'weather_market'
            GROUP BY subject_id
        ),
        live_rollup AS (
            SELECT
                market_id,
                MAX(CASE WHEN COALESCE(live_prereq_attention_required, FALSE) THEN 1 ELSE 0 END) AS has_attention,
                MAX(CASE WHEN live_prereq_execution_status = 'shadow_aligned' THEN 1 ELSE 0 END) AS has_shadow_aligned,
                MAX(CASE WHEN live_prereq_execution_status IS NOT NULL THEN 1 ELSE 0 END) AS has_any_live,
                MAX(COALESCE(latest_submit_created_at, latest_sign_attempt_created_at)) AS updated_at
            FROM ui.live_prereq_execution_summary
            GROUP BY market_id
        )
        SELECT
            market.market_id,
            market.title AS question,
            watch.location_name,
            watch.station_id,
            COALESCE(market.close_time, market.end_date) AS market_close_time,
            market.accepting_orders,
            market.enable_order_book,
            signal.snapshot_id,
            signal.token_id,
            signal.outcome,
            signal.decision,
            signal.side,
            signal.edge_bps,
            signal.threshold_bps,
            signal.reference_price,
            signal.fair_value,
            signal.pricing_context_json,
            signal.created_at AS signal_created_at,
            watch.latest_run_source,
            watch.latest_forecast_target_time,
            CASE
                WHEN COALESCE(agent.has_failure, 0) = 1 THEN 'agent_failure'
                WHEN COALESCE(agent.has_review_required, 0) = 1 THEN 'review_required'
                WHEN COALESCE(agent.has_success, 0) = 1 THEN 'passed'
                ELSE 'no_agent_signal'
            END AS agent_review_status,
            CASE
                WHEN COALESCE(live.has_attention, 0) = 1 THEN 'attention_required'
                WHEN COALESCE(live.has_shadow_aligned, 0) = 1 THEN 'shadow_aligned'
                WHEN COALESCE(live.has_any_live, 0) = 1 THEN 'in_progress'
                ELSE 'not_started'
            END AS live_prereq_status,
            COALESCE(agent.max_confidence, 0.0) AS agent_confidence,
            agent.updated_at AS agent_updated_at,
            live.updated_at AS live_updated_at
        FROM src.weather.weather_markets market
        LEFT JOIN ui.market_watch_summary watch ON watch.market_id = market.market_id
        LEFT JOIN latest_signal signal ON signal.market_id = market.market_id
        LEFT JOIN agent_rollup agent ON agent.market_id = market.market_id
        LEFT JOIN live_rollup live ON live.market_id = market.market_id
        WHERE market.active = TRUE
          AND COALESCE(market.closed, FALSE) = FALSE
          AND COALESCE(market.archived, FALSE) = FALSE
        """
    ).df()
    allocation_by_market: dict[str, dict[str, Any]] = {}
    if _table_exists(con, "src.runtime.allocation_decisions"):
        allocation_frame = con.execute(
            """
            SELECT * FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY market_id
                        ORDER BY created_at DESC, allocation_decision_id DESC
                    ) AS rn
                FROM src.runtime.allocation_decisions
            )
            WHERE rn = 1
            """
        ).df()
        allocation_by_market = {
            str(row["market_id"]): row.to_dict()
            for _, row in allocation_frame.iterrows()
            if row.get("market_id") is not None
        }
    execution_intelligence_by_market_side: dict[tuple[str, str], dict[str, Any]] = {}
    if _table_exists(con, "src.runtime.execution_intelligence_summaries"):
        execution_intelligence_frame = con.execute(
            """
            SELECT * FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY market_id, side
                        ORDER BY materialized_at DESC, execution_intelligence_score DESC, summary_id DESC
                    ) AS rn
                FROM src.runtime.execution_intelligence_summaries
            )
            WHERE rn = 1
            """
        ).df()
        execution_intelligence_by_market_side = {
            (str(item["market_id"]), str(item["side"]).upper()): item.to_dict()
            for _, item in execution_intelligence_frame.iterrows()
            if item.get("market_id") is not None and item.get("side") is not None
        }
    rows: list[dict[str, Any]] = []
    for _, row in frame.iterrows():
        allocation_payload = _json_object(allocation_by_market.get(str(row.get("market_id"))))
        budget_impact = _json_object(allocation_payload.get("budget_impact_json")) if allocation_payload else {}
        preview_budget = _json_object(budget_impact.get("preview"))
        pricing_context = _json_object(row.get("pricing_context_json"))
        execution_intelligence_payload = execution_intelligence_by_market_side.get(
            (str(row.get("market_id")), str(row.get("side") or "").upper())
        ) or execution_intelligence_by_market_side.get((str(row.get("market_id")), "BUY")) or {}
        model_fair_value = _coerce_float(pricing_context.get("model_fair_value")) or _coerce_float(row.get("fair_value")) or 0.0
        market_price = _coerce_float(row.get("reference_price")) or 0.0
        token_id = str(row.get("token_id") or "")
        outcome = str(row.get("outcome") or "")
        confidence_score = _coerce_float(pricing_context.get("confidence_score"))
        if confidence_score is None:
            confidence_score = max(float(_coerce_float(row.get("agent_confidence")) or 0.0) * 100.0, 50.0 if market_price else 0.0)
        threshold_bps = int(_coerce_float(pricing_context.get("threshold_bps")) or _coerce_float(row.get("threshold_bps")) or 0)
        mapping_confidence = _coerce_float(pricing_context.get("mapping_confidence"))
        if mapping_confidence is None:
            mapping_confidence = 1.0
        source_freshness_status = str(pricing_context.get("source_freshness_status") or "missing")
        price_staleness_ms = int(_coerce_float(pricing_context.get("price_staleness_ms")) or 0)
        market_quality_status = str(pricing_context.get("market_quality_status") or "review_required")
        calibration_health_status = str(pricing_context.get("calibration_health_status") or "lookup_missing")
        calibration_bias_quality = str(
            pricing_context.get("calibration_bias_quality")
            or pricing_context.get("bias_quality_status")
            or "lookup_missing"
        )
        threshold_probability_quality = str(
            pricing_context.get("threshold_probability_quality")
            or pricing_context.get("threshold_probability_quality_status")
            or "lookup_missing"
        )
        sample_count = int(_coerce_float(pricing_context.get("sample_count")) or 0)
        calibration_multiplier = _coerce_float(pricing_context.get("calibration_multiplier"))
        calibration_reason_codes = pricing_context.get("calibration_reason_codes")
        if not isinstance(calibration_reason_codes, list):
            calibration_reason_codes = None
        calibration_freshness_status = str(pricing_context.get("calibration_freshness_status") or "fresh")
        calibration_profile_materialized_at = pricing_context.get("profile_materialized_at")
        calibration_profile_window_end = pricing_context.get("profile_window_end")
        calibration_profile_age_hours = _coerce_float(pricing_context.get("profile_age_hours"))
        calibration_gate_status, calibration_gate_reason_codes = _derive_calibration_gate(
            calibration_freshness_status=calibration_freshness_status,
            calibration_health_status=calibration_health_status,
            threshold_probability_quality=threshold_probability_quality,
            sample_count=sample_count,
        )
        calibration_impacted_market = calibration_gate_status != "clear"
        if not token_id or not outcome or market_price <= 0.0:
            actionability_status = (
                "blocked"
                if (not bool(row.get("accepting_orders")) or str(row.get("live_prereq_status") or "") == "attention_required")
                else "review_required"
                if (
                    calibration_gate_status == "review_required"
                    or str(row.get("agent_review_status") or "") != "passed"
                )
                else "no_trade"
            )
            if calibration_gate_status == "research_only":
                actionability_status = "no_trade"
            rows.append(
                {
                    "market_id": row.get("market_id"),
                    "question": row.get("question"),
                    "location_name": row.get("location_name"),
                    "station_id": row.get("station_id"),
                    "market_close_time": row.get("market_close_time"),
                    "accepting_orders": bool(row.get("accepting_orders")),
                    "best_side": None,
                    "best_outcome": outcome or None,
                    "best_decision": "NO_TRADE",
                    "market_price": None,
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
                    "calibration_health_status": calibration_health_status,
                    "calibration_bias_quality": calibration_bias_quality,
                    "threshold_probability_quality": threshold_probability_quality,
                    "sample_count": sample_count,
                    "uncertainty_multiplier": 0.0,
                    "uncertainty_penalty_bps": 0,
                    "ranking_penalty_reasons": calibration_reason_codes or [],
                    "mapping_confidence": mapping_confidence,
                    "source_freshness_status": source_freshness_status,
                    "price_staleness_ms": price_staleness_ms,
                    "calibration_freshness_status": calibration_freshness_status,
                    "calibration_profile_materialized_at": calibration_profile_materialized_at,
                    "calibration_profile_window_end": calibration_profile_window_end,
                    "calibration_profile_age_hours": calibration_profile_age_hours,
                    "calibration_gate_status": calibration_gate_status,
                    "calibration_gate_reason_codes": calibration_gate_reason_codes,
                    "calibration_impacted_market": calibration_impacted_market,
                    "market_quality_status": market_quality_status,
                    "liquidity_proxy": 25.0 if not bool(row.get("accepting_orders")) else 55.0,
                    "liquidity_penalty_bps": None,
                    "confidence_score": confidence_score,
                    "confidence_proxy": confidence_score,
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
                    "base_ranking_score": _coerce_float(allocation_payload.get("base_ranking_score")) if allocation_payload else None,
                    "deployable_expected_pnl": _coerce_float(allocation_payload.get("deployable_expected_pnl")) if allocation_payload else None,
                    "deployable_notional": _coerce_float(allocation_payload.get("deployable_notional")) if allocation_payload else None,
                    "max_deployable_size": _coerce_float(allocation_payload.get("max_deployable_size")) if allocation_payload else None,
                    "capital_scarcity_penalty": _coerce_float(allocation_payload.get("capital_scarcity_penalty")) if allocation_payload else None,
                    "concentration_penalty": _coerce_float(allocation_payload.get("concentration_penalty")) if allocation_payload else None,
                    "pre_budget_deployable_size": _coerce_float(allocation_payload.get("pre_budget_deployable_size")) if allocation_payload and allocation_payload.get("pre_budget_deployable_size") is not None else _coerce_float(preview_budget.get("pre_budget_deployable_size")),
                    "pre_budget_deployable_notional": _coerce_float(allocation_payload.get("pre_budget_deployable_notional")) if allocation_payload and allocation_payload.get("pre_budget_deployable_notional") is not None else _coerce_float(preview_budget.get("pre_budget_deployable_notional")),
                    "pre_budget_deployable_expected_pnl": _coerce_float(allocation_payload.get("pre_budget_deployable_expected_pnl")) if allocation_payload and allocation_payload.get("pre_budget_deployable_expected_pnl") is not None else _coerce_float(preview_budget.get("pre_budget_deployable_expected_pnl")),
                    "preview_binding_limit_scope": preview_budget.get("preview_binding_limit_scope"),
                    "preview_binding_limit_key": preview_budget.get("preview_binding_limit_key"),
                    "rerank_position": int(_coerce_float(allocation_payload.get("rerank_position")) or _coerce_float(budget_impact.get("rerank_position")) or 0) or None,
                    "rerank_reason_codes": _json_array_of_text(allocation_payload.get("rerank_reason_codes_json")) if allocation_payload and allocation_payload.get("rerank_reason_codes_json") is not None else _json_array_of_text(budget_impact.get("rerank_reason_codes")),
                    "requested_size": _coerce_float(allocation_payload.get("requested_size")) if allocation_payload else _coerce_float(preview_budget.get("requested_size")),
                    "requested_notional": _coerce_float(allocation_payload.get("requested_notional")) if allocation_payload else _coerce_float(preview_budget.get("requested_notional")),
                    "recommended_size": _coerce_float(allocation_payload.get("recommended_size")) if allocation_payload else None,
                    "allocation_status": allocation_payload.get("allocation_status") if allocation_payload else None,
                    "budget_impact": budget_impact,
                    "binding_limit_scope": allocation_payload.get("binding_limit_scope") if allocation_payload else None,
                    "binding_limit_key": allocation_payload.get("binding_limit_key") if allocation_payload else None,
                    "capital_policy_id": allocation_payload.get("capital_policy_id") if allocation_payload else None,
                    "capital_policy_version": allocation_payload.get("capital_policy_version") if allocation_payload else None,
                    "capital_scaling_reason_codes": _json_array_of_text(allocation_payload.get("capital_scaling_reason_codes_json")) if allocation_payload and allocation_payload.get("capital_scaling_reason_codes_json") is not None else [],
                    "execution_intelligence_summary_id": execution_intelligence_payload.get("summary_id"),
                    "quote_imbalance_score": _coerce_float(execution_intelligence_payload.get("quote_imbalance_score")),
                    "top_of_book_stability": _coerce_float(execution_intelligence_payload.get("top_of_book_stability")),
                    "book_update_intensity": _coerce_float(execution_intelligence_payload.get("book_update_intensity")),
                    "spread_regime": execution_intelligence_payload.get("spread_regime"),
                    "visible_size_shock_flag": bool(execution_intelligence_payload.get("visible_size_shock_flag")),
                    "book_pressure_side": execution_intelligence_payload.get("book_pressure_side"),
                    "expected_capture_regime": execution_intelligence_payload.get("expected_capture_regime"),
                    "expected_slippage_regime": execution_intelligence_payload.get("expected_slippage_regime"),
                    "execution_intelligence_score": _coerce_float(execution_intelligence_payload.get("execution_intelligence_score")),
                    "execution_intelligence_reason_codes": _json_array_of_text(execution_intelligence_payload.get("reason_codes_json")),
                    "regime_bucket": pricing_context.get("regime_bucket"),
                    "allocation_decision_id": allocation_payload.get("allocation_decision_id") if allocation_payload else None,
                    "ranking_score": _coerce_float(allocation_payload.get("ranking_score")) if allocation_payload else 0.0,
                    "execution_prior_key": None,
                    "why_ranked_json": json.dumps({}, ensure_ascii=True, sort_keys=True),
                    "agent_review_status": row.get("agent_review_status"),
                    "live_prereq_status": row.get("live_prereq_status"),
                    "opportunity_bucket": "negative_edge",
                    "opportunity_score": 0.0,
                    "actionability_status": actionability_status,
                    "latest_run_source": row.get("latest_run_source"),
                    "latest_forecast_target_time": row.get("latest_forecast_target_time"),
                    "threshold_bps": threshold_bps,
                    "signal_created_at": row.get("signal_created_at"),
                    "agent_updated_at": row.get("agent_updated_at"),
                    "live_updated_at": row.get("live_updated_at"),
                }
            )
            continue
        assessment = build_weather_opportunity_assessment(
            market_id=str(row.get("market_id")),
            token_id=token_id,
            outcome=outcome,
            reference_price=market_price,
            model_fair_value=model_fair_value,
            accepting_orders=bool(row.get("accepting_orders")),
            enable_order_book=bool(row.get("enable_order_book")) if row.get("enable_order_book") is not None else None,
            threshold_bps=threshold_bps,
            fees_bps=int(_coerce_float(pricing_context.get("fees_bps")) or 0),
            agent_review_status=str(row.get("agent_review_status") or "no_agent_signal"),
            live_prereq_status=str(row.get("live_prereq_status") or "not_started"),
            confidence_score=confidence_score,
            mapping_confidence=mapping_confidence,
            price_staleness_ms=price_staleness_ms,
            source_freshness_status=source_freshness_status,
            spread_bps=int(_coerce_float(pricing_context.get("spread_bps")) or 0) or None,
            calibration_health_status=calibration_health_status,
            calibration_bias_quality=calibration_bias_quality,
            threshold_probability_quality=threshold_probability_quality,
            sample_count=sample_count,
            calibration_multiplier=calibration_multiplier,
            calibration_reason_codes=calibration_reason_codes,
            forecast_distribution_summary_v2=pricing_context.get("distribution_summary_v2")
            if isinstance(pricing_context.get("distribution_summary_v2"), dict)
            else None,
            recommended_size=_coerce_float(allocation_payload.get("recommended_size")) if allocation_payload else None,
            allocation_status=str(allocation_payload.get("allocation_status")) if allocation_payload else None,
            budget_impact=budget_impact if allocation_payload else None,
            allocation_decision_id=str(allocation_payload.get("allocation_decision_id")) if allocation_payload else None,
            policy_id=str(allocation_payload.get("policy_id")) if allocation_payload and allocation_payload.get("policy_id") is not None else None,
            policy_version=str(allocation_payload.get("policy_version")) if allocation_payload and allocation_payload.get("policy_version") is not None else None,
            base_ranking_score=_coerce_float(allocation_payload.get("base_ranking_score")) if allocation_payload else None,
            deployable_expected_pnl=_coerce_float(allocation_payload.get("deployable_expected_pnl")) if allocation_payload else None,
            deployable_notional=_coerce_float(allocation_payload.get("deployable_notional")) if allocation_payload else None,
            max_deployable_size=_coerce_float(allocation_payload.get("max_deployable_size")) if allocation_payload else None,
            capital_scarcity_penalty=_coerce_float(allocation_payload.get("capital_scarcity_penalty")) if allocation_payload else None,
            concentration_penalty=_coerce_float(allocation_payload.get("concentration_penalty")) if allocation_payload else None,
            pre_budget_deployable_size=_coerce_float(allocation_payload.get("pre_budget_deployable_size")) if allocation_payload and allocation_payload.get("pre_budget_deployable_size") is not None else _coerce_float(preview_budget.get("pre_budget_deployable_size")),
            pre_budget_deployable_notional=_coerce_float(allocation_payload.get("pre_budget_deployable_notional")) if allocation_payload and allocation_payload.get("pre_budget_deployable_notional") is not None else _coerce_float(preview_budget.get("pre_budget_deployable_notional")),
            pre_budget_deployable_expected_pnl=_coerce_float(allocation_payload.get("pre_budget_deployable_expected_pnl")) if allocation_payload and allocation_payload.get("pre_budget_deployable_expected_pnl") is not None else _coerce_float(preview_budget.get("pre_budget_deployable_expected_pnl")),
            rerank_position=int(_coerce_float(allocation_payload.get("rerank_position")) or _coerce_float(budget_impact.get("rerank_position")) or 0) or None,
            rerank_reason_codes=_json_array_of_text(allocation_payload.get("rerank_reason_codes_json")) if allocation_payload and allocation_payload.get("rerank_reason_codes_json") is not None else _json_array_of_text(budget_impact.get("rerank_reason_codes")),
            deployable_ranking_score=_coerce_float(allocation_payload.get("ranking_score")) if allocation_payload else None,
            capital_policy_id=str(allocation_payload.get("capital_policy_id")) if allocation_payload and allocation_payload.get("capital_policy_id") is not None else None,
            capital_policy_version=str(allocation_payload.get("capital_policy_version")) if allocation_payload and allocation_payload.get("capital_policy_version") is not None else None,
            capital_scaling_reason_codes=_json_array_of_text(allocation_payload.get("capital_scaling_reason_codes_json")) if allocation_payload and allocation_payload.get("capital_scaling_reason_codes_json") is not None else [],
            source_context={
                **pricing_context,
                "calibration_health_status": calibration_health_status,
                "calibration_bias_quality": calibration_bias_quality,
                "threshold_probability_quality": threshold_probability_quality,
                "sample_count": sample_count,
                "calibration_multiplier": calibration_multiplier,
                "calibration_reason_codes": calibration_reason_codes,
                "forecast_target_time": str(row.get("latest_forecast_target_time") or ""),
                "latest_run_source": row.get("latest_run_source"),
                "mapping_method": pricing_context.get("mapping_method"),
                "market_quality_reason_codes": pricing_context.get("market_quality_reason_codes"),
                "price_staleness_ms": price_staleness_ms,
                "signal_created_at": str(row.get("signal_created_at") or ""),
                "snapshot_id": row.get("snapshot_id"),
                "source_freshness_status": source_freshness_status,
                "binding_limit_scope": allocation_payload.get("binding_limit_scope") if allocation_payload else None,
                "binding_limit_key": allocation_payload.get("binding_limit_key") if allocation_payload else None,
                "pre_budget_deployable_size": _coerce_float(allocation_payload.get("pre_budget_deployable_size")) if allocation_payload and allocation_payload.get("pre_budget_deployable_size") is not None else _coerce_float(preview_budget.get("pre_budget_deployable_size")),
                "pre_budget_deployable_notional": _coerce_float(allocation_payload.get("pre_budget_deployable_notional")) if allocation_payload and allocation_payload.get("pre_budget_deployable_notional") is not None else _coerce_float(preview_budget.get("pre_budget_deployable_notional")),
                "pre_budget_deployable_expected_pnl": _coerce_float(allocation_payload.get("pre_budget_deployable_expected_pnl")) if allocation_payload and allocation_payload.get("pre_budget_deployable_expected_pnl") is not None else _coerce_float(preview_budget.get("pre_budget_deployable_expected_pnl")),
                "preview_binding_limit_scope": preview_budget.get("preview_binding_limit_scope"),
                "preview_binding_limit_key": preview_budget.get("preview_binding_limit_key"),
                "requested_size": _coerce_float(allocation_payload.get("requested_size")) if allocation_payload else _coerce_float(preview_budget.get("requested_size")),
                "requested_notional": _coerce_float(allocation_payload.get("requested_notional")) if allocation_payload else _coerce_float(preview_budget.get("requested_notional")),
                "rerank_position": int(_coerce_float(allocation_payload.get("rerank_position")) or _coerce_float(budget_impact.get("rerank_position")) or 0) or None,
                "rerank_reason_codes": _json_array_of_text(allocation_payload.get("rerank_reason_codes_json")) if allocation_payload and allocation_payload.get("rerank_reason_codes_json") is not None else _json_array_of_text(budget_impact.get("rerank_reason_codes")),
                "capital_policy_id": allocation_payload.get("capital_policy_id") if allocation_payload else None,
                "capital_policy_version": allocation_payload.get("capital_policy_version") if allocation_payload else None,
                "capital_scaling_reason_codes": _json_array_of_text(allocation_payload.get("capital_scaling_reason_codes_json")) if allocation_payload and allocation_payload.get("capital_scaling_reason_codes_json") is not None else [],
                "execution_intelligence_summary_id": execution_intelligence_payload.get("summary_id"),
                "execution_intelligence_run_id": execution_intelligence_payload.get("run_id"),
                "execution_intelligence_market_id": execution_intelligence_payload.get("market_id"),
                "execution_intelligence_side": execution_intelligence_payload.get("side"),
                "execution_intelligence_quote_imbalance_score": _coerce_float(execution_intelligence_payload.get("quote_imbalance_score")),
                "execution_intelligence_top_of_book_stability": _coerce_float(execution_intelligence_payload.get("top_of_book_stability")),
                "execution_intelligence_book_update_intensity": _coerce_float(execution_intelligence_payload.get("book_update_intensity")),
                "execution_intelligence_spread_regime": execution_intelligence_payload.get("spread_regime"),
                "execution_intelligence_visible_size_shock_flag": bool(execution_intelligence_payload.get("visible_size_shock_flag")),
                "execution_intelligence_book_pressure_side": execution_intelligence_payload.get("book_pressure_side"),
                "execution_intelligence_expected_capture_regime": execution_intelligence_payload.get("expected_capture_regime"),
                "execution_intelligence_expected_slippage_regime": execution_intelligence_payload.get("expected_slippage_regime"),
                "execution_intelligence_score": _coerce_float(execution_intelligence_payload.get("execution_intelligence_score")),
                "execution_intelligence_reason_codes": _json_array_of_text(execution_intelligence_payload.get("reason_codes_json")),
                "execution_intelligence_source_window_start": execution_intelligence_payload.get("source_window_start"),
                "execution_intelligence_source_window_end": execution_intelligence_payload.get("source_window_end"),
                "execution_intelligence_materialized_at": execution_intelligence_payload.get("materialized_at"),
            },
        )
        best_side = derive_opportunity_side(assessment.edge_bps_executable)
        rows.append(
            {
                "market_id": row.get("market_id"),
                "question": row.get("question"),
                "location_name": row.get("location_name"),
                "station_id": row.get("station_id"),
                "market_close_time": row.get("market_close_time"),
                "accepting_orders": bool(row.get("accepting_orders")),
                "best_side": best_side,
                "best_outcome": row.get("outcome"),
                "best_decision": row.get("decision"),
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
                "calibration_freshness_status": assessment.assessment_context_json.get("calibration_freshness_status"),
                "calibration_profile_materialized_at": assessment.assessment_context_json.get("profile_materialized_at"),
                "calibration_profile_window_end": assessment.assessment_context_json.get("profile_window_end"),
                "calibration_profile_age_hours": assessment.assessment_context_json.get("profile_age_hours"),
                "calibration_gate_status": assessment.calibration_gate_status,
                "calibration_gate_reason_codes": assessment.calibration_gate_reason_codes,
                "calibration_impacted_market": assessment.calibration_impacted_market,
                "market_quality_status": assessment.assessment_context_json.get("market_quality_status"),
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
                "rerank_position": assessment.rerank_position,
                "rerank_reason_codes": assessment.rerank_reason_codes,
                "requested_size": assessment.assessment_context_json.get("requested_size"),
                "requested_notional": assessment.assessment_context_json.get("requested_notional"),
                "recommended_size": assessment.recommended_size,
                "allocation_status": assessment.allocation_status,
                "budget_impact": assessment.budget_impact,
                "binding_limit_scope": allocation_payload.get("binding_limit_scope") if allocation_payload else None,
                "binding_limit_key": allocation_payload.get("binding_limit_key") if allocation_payload else None,
                "capital_policy_id": assessment.capital_policy_id,
                "capital_policy_version": assessment.capital_policy_version,
                "capital_scaling_reason_codes": assessment.capital_scaling_reason_codes,
                "execution_intelligence_summary_id": assessment.assessment_context_json.get("execution_intelligence_summary_id"),
                "quote_imbalance_score": assessment.assessment_context_json.get("execution_intelligence_quote_imbalance_score"),
                "top_of_book_stability": assessment.assessment_context_json.get("execution_intelligence_top_of_book_stability"),
                "book_update_intensity": assessment.assessment_context_json.get("execution_intelligence_book_update_intensity"),
                "spread_regime": assessment.assessment_context_json.get("execution_intelligence_spread_regime"),
                "visible_size_shock_flag": assessment.assessment_context_json.get("execution_intelligence_visible_size_shock_flag"),
                "book_pressure_side": assessment.assessment_context_json.get("execution_intelligence_book_pressure_side"),
                "expected_capture_regime": assessment.assessment_context_json.get("execution_intelligence_expected_capture_regime"),
                "expected_slippage_regime": assessment.assessment_context_json.get("execution_intelligence_expected_slippage_regime"),
                "execution_intelligence_score": assessment.assessment_context_json.get("execution_intelligence_score"),
                "execution_intelligence_reason_codes": assessment.assessment_context_json.get("execution_intelligence_reason_codes"),
                "regime_bucket": assessment.regime_bucket,
                "allocation_decision_id": allocation_payload.get("allocation_decision_id") if allocation_payload else None,
                "ranking_score": assessment.ranking_score,
                "execution_prior_key": assessment.execution_prior_key,
                "pricing_context_json": json.dumps(_json_ready(assessment.assessment_context_json), ensure_ascii=True, sort_keys=True),
                "why_ranked_json": json.dumps(_json_ready(assessment.why_ranked_json), ensure_ascii=True, sort_keys=True),
                "agent_review_status": row.get("agent_review_status"),
                "live_prereq_status": row.get("live_prereq_status"),
                "opportunity_bucket": _opportunity_bucket(assessment.edge_bps_executable),
                "opportunity_score": assessment.ranking_score,
                "actionability_status": assessment.actionability_status,
                "latest_run_source": row.get("latest_run_source"),
                "latest_forecast_target_time": row.get("latest_forecast_target_time"),
                "threshold_bps": threshold_bps,
                "signal_created_at": row.get("signal_created_at"),
                "agent_updated_at": row.get("agent_updated_at"),
                "live_updated_at": row.get("live_updated_at"),
            }
        )
    result = pd.DataFrame(rows, columns=[
        "market_id",
        "question",
        "location_name",
        "station_id",
        "market_close_time",
        "accepting_orders",
        "best_side",
        "best_outcome",
        "best_decision",
        "market_price",
        "fair_value",
        "edge_bps",
        "model_fair_value",
        "execution_adjusted_fair_value",
        "edge_bps_model",
        "edge_bps_executable",
        "fees_bps",
        "slippage_bps",
        "fill_probability",
        "depth_proxy",
        "calibration_health_status",
        "sample_count",
        "uncertainty_multiplier",
        "uncertainty_penalty_bps",
        "ranking_penalty_reasons",
        "mapping_confidence",
        "source_freshness_status",
        "price_staleness_ms",
        "calibration_freshness_status",
        "calibration_profile_materialized_at",
        "calibration_profile_window_end",
        "calibration_profile_age_hours",
        "calibration_gate_status",
        "calibration_gate_reason_codes",
        "calibration_impacted_market",
        "market_quality_status",
        "liquidity_proxy",
        "liquidity_penalty_bps",
        "confidence_score",
        "confidence_proxy",
        "ops_readiness_score",
        "expected_value_score",
        "expected_pnl_score",
        "expected_dollar_pnl",
        "capture_probability",
        "risk_penalty",
        "capital_efficiency",
        "feedback_penalty",
        "feedback_status",
        "cohort_prior_version",
        "base_ranking_score",
        "deployable_expected_pnl",
        "deployable_notional",
        "max_deployable_size",
        "capital_scarcity_penalty",
        "concentration_penalty",
        "pre_budget_deployable_size",
        "pre_budget_deployable_notional",
        "pre_budget_deployable_expected_pnl",
        "preview_binding_limit_scope",
        "preview_binding_limit_key",
        "rerank_position",
        "rerank_reason_codes",
        "requested_size",
        "requested_notional",
        "recommended_size",
        "allocation_status",
        "budget_impact",
        "binding_limit_scope",
        "binding_limit_key",
        "capital_policy_id",
        "capital_policy_version",
        "capital_scaling_reason_codes",
        "execution_intelligence_summary_id",
        "quote_imbalance_score",
        "top_of_book_stability",
        "book_update_intensity",
        "spread_regime",
        "visible_size_shock_flag",
        "book_pressure_side",
        "expected_capture_regime",
        "expected_slippage_regime",
        "execution_intelligence_score",
        "execution_intelligence_reason_codes",
        "regime_bucket",
        "allocation_decision_id",
        "ranking_score",
        "execution_prior_key",
        "pricing_context_json",
        "why_ranked_json",
        "agent_review_status",
        "live_prereq_status",
        "opportunity_bucket",
        "opportunity_score",
        "actionability_status",
        "latest_run_source",
        "latest_forecast_target_time",
        "threshold_bps",
        "signal_created_at",
        "agent_updated_at",
        "live_updated_at",
    ])
    result = ensure_primary_score_fields(result)
    result = annotate_frame_with_source_truth(
        result,
        source_origin="ui_lite",
        derived=False,
        freshness_column="source_freshness_status",
    )
    refresh_columns = [column for column in ["signal_created_at", "agent_updated_at", "live_updated_at", "calibration_profile_materialized_at"] if column in result.columns]
    if refresh_columns:
        result["surface_last_refresh_ts"] = result[refresh_columns].bfill(axis=1).iloc[:, 0]
    else:
        result["surface_last_refresh_ts"] = None
    result["surface_delivery_status"] = "ok"
    result["surface_fallback_origin"] = None
    result["surface_delivery_reason_codes_json"] = "[]"
    con.register("market_opportunity_summary_df", result)
    con.execute("CREATE OR REPLACE TABLE ui.market_opportunity_summary AS SELECT * FROM market_opportunity_summary_df")
    row = con.execute("SELECT COUNT(*) FROM ui.market_opportunity_summary").fetchone()
    table_row_counts["ui.market_opportunity_summary"] = int(row[0]) if row is not None else 0
    con.unregister("market_opportunity_summary_df")


def _create_surface_delivery_summary(con, *, table_row_counts: dict[str, int]) -> None:
    check_rows = con.execute(
        """
        SELECT surface_id, table_name, check_status, issues_json, checked_at
        FROM ui.truth_source_checks
        """
    ).fetchall()
    checks_by_surface: dict[str, list[dict[str, Any]]] = {}
    for surface_id, table_name, check_status, issues_json, checked_at in check_rows:
        if str(table_name) in {"ui.surface_delivery_summary", "ui.system_runtime_summary"}:
            continue
        checks_by_surface.setdefault(str(surface_id), []).append(
            {
                "table_name": str(table_name),
                "check_status": str(check_status),
                "issues": _json_list(issues_json),
                "checked_at": checked_at,
            }
        )
    primary_tables = {
        "home": "ui.action_queue_summary",
        "markets": "ui.market_opportunity_summary",
        "execution": "ui.execution_science_summary",
        "system": "ui.system_runtime_summary",
        "agents": "ui.proposal_resolution_summary",
    }
    rows: list[dict[str, Any]] = []
    for surface_id, checks in checks_by_surface.items():
        primary_table = primary_tables.get(surface_id) or (checks[0]["table_name"] if checks else None)
        primary_source = "ui_lite"
        fallback_origin = None
        truth_check_status = "ok"
        truth_check_issue_count = 0
        degraded_reasons: list[str] = []
        last_refresh_ts = None
        row_count = 0
        delivery_status = "ok"
        if checks:
            if any(item["check_status"] == "fail" for item in checks):
                truth_check_status = "fail"
                delivery_status = "read_error"
            elif any(item["check_status"] == "warn" for item in checks):
                truth_check_status = "warn"
            truth_check_issue_count = sum(len(item["issues"]) for item in checks)
            degraded_reasons = [issue for item in checks for issue in item["issues"]]
            last_refresh_ts = max((item["checked_at"] for item in checks if item["checked_at"] is not None), default=None)
        if primary_table and _table_exists(con, primary_table):
            row_count = int(con.execute(f"SELECT COUNT(*) FROM {primary_table}").fetchone()[0])
            columns = {str(row[1]) for row in con.execute(f"PRAGMA table_info('{primary_table}')").fetchall()}
            if "surface_delivery_status" in columns and row_count > 0:
                statuses = {
                    str(row[0])
                    for row in con.execute(
                        f"SELECT DISTINCT surface_delivery_status FROM {primary_table} WHERE surface_delivery_status IS NOT NULL"
                    ).fetchall()
                }
                if "read_error" in statuses or "missing" in statuses:
                    delivery_status = "read_error" if "read_error" in statuses else "missing"
                elif "degraded_source" in statuses:
                    delivery_status = "degraded_source"
                elif "stale" in statuses:
                    delivery_status = "stale"
            if "surface_fallback_origin" in columns and row_count > 0:
                fallback_row = con.execute(
                    f"""
                    SELECT surface_fallback_origin
                    FROM {primary_table}
                    WHERE surface_fallback_origin IS NOT NULL AND surface_fallback_origin <> ''
                    ORDER BY surface_last_refresh_ts DESC NULLS LAST
                    LIMIT 1
                    """
                ).fetchone()
                fallback_origin = str(fallback_row[0]) if fallback_row is not None else None
            if "surface_last_refresh_ts" in columns and row_count > 0:
                refresh_row = con.execute(f"SELECT MAX(surface_last_refresh_ts) FROM {primary_table}").fetchone()
                if refresh_row is not None:
                    last_refresh_ts = refresh_row[0] or last_refresh_ts
            if "source_truth_status" in columns and row_count > 0 and delivery_status == "ok":
                truth_statuses = {
                    str(row[0])
                    for row in con.execute(
                        f"SELECT DISTINCT source_truth_status FROM {primary_table} WHERE source_truth_status IS NOT NULL"
                    ).fetchall()
                }
                if {"fallback", "degraded", "stale"} & truth_statuses:
                    delivery_status = "degraded_source"
                    if fallback_origin is None:
                        fallback_origin = "runtime_db"
        elif primary_table:
            delivery_status = "missing"
            degraded_reasons.append(f"table_missing:{primary_table}")
        rows.append(
            {
                "surface_id": surface_id,
                "primary_table": primary_table,
                "delivery_status": delivery_status,
                "primary_source": primary_source,
                "fallback_origin": fallback_origin,
                "truth_check_status": truth_check_status,
                "truth_check_issue_count": truth_check_issue_count,
                "row_count": row_count,
                "last_refresh_ts": last_refresh_ts,
                "degraded_reason_codes_json": json.dumps(degraded_reasons, ensure_ascii=True, sort_keys=True),
                "primary_score_label": "surface_delivery_status",
            }
        )
    frame = pd.DataFrame(rows)
    con.register("surface_delivery_df", frame)
    con.execute("CREATE OR REPLACE TABLE ui.surface_delivery_summary AS SELECT * FROM surface_delivery_df")
    table_row_counts["ui.surface_delivery_summary"] = int(len(frame.index))
    con.unregister("surface_delivery_df")


def _create_system_runtime_summary(con, *, table_row_counts: dict[str, int]) -> None:
    latest_refresh = None
    if _table_exists(con, "src.runtime.operator_surface_refresh_runs"):
        latest_refresh = con.execute(
            """
            SELECT
                refresh_run_id,
                CASE
                    WHEN COALESCE(error, '') <> '' OR NOT ui_lite_ok OR NOT ui_replica_ok THEN 'read_error'
                    WHEN read_error_surface_count > 0 THEN 'read_error'
                    WHEN degraded_surface_count > 0 THEN 'degraded_source'
                    ELSE 'ok'
                END AS refresh_status,
                ui_replica_ok,
                ui_lite_ok,
                degraded_surface_count,
                read_error_surface_count,
                refreshed_at
            FROM src.runtime.operator_surface_refresh_runs
            ORDER BY refreshed_at DESC, refresh_run_id DESC
            LIMIT 1
            """
        ).fetchone()
    delivery = con.execute(
        """
        SELECT
            COALESCE(SUM(CASE WHEN delivery_status = 'degraded_source' THEN 1 ELSE 0 END), 0) AS degraded_surface_count,
            COALESCE(SUM(CASE WHEN delivery_status = 'read_error' THEN 1 ELSE 0 END), 0) AS read_error_surface_count
        FROM ui.surface_delivery_summary
        """
    ).fetchone()
    readiness = con.execute(
        """
        SELECT
            COALESCE(MAX(go_decision), 'UNKNOWN') AS readiness_status
        FROM ui.phase_readiness_summary
        """
    ).fetchone()
    weather_chain = con.execute(
        """
        SELECT
            CASE
                WHEN COUNT(*) = 0 THEN 'missing'
                WHEN SUM(CASE WHEN source_truth_status IN ('fallback', 'degraded', 'stale') THEN 1 ELSE 0 END) > 0 THEN 'degraded'
                ELSE 'ok'
            END AS weather_chain_status
        FROM ui.market_opportunity_summary
        """
    ).fetchone()
    calibration = con.execute(
        """
        SELECT
            COALESCE(SUM(hard_gate_market_count), 0) AS calibration_hard_gate_market_count
        FROM ui.calibration_health_summary
        """
    ).fetchone()
    calibration_counts = (
        con.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM src.weather.forecast_calibration_samples) AS calibration_sample_count,
                (SELECT COUNT(*) FROM src.weather.forecast_calibration_profiles_v2) AS calibration_profile_count,
                (SELECT COUNT(*) FROM src.runtime.calibration_profile_materializations) AS calibration_materialization_count
            """
        ).fetchone()
        if _table_exists(con, "src.weather.forecast_calibration_samples")
        and _table_exists(con, "src.weather.forecast_calibration_profiles_v2")
        and _table_exists(con, "src.runtime.calibration_profile_materializations")
        else (0, 0, 0)
    )
    resolution = con.execute(
        """
        SELECT
            COALESCE(SUM(CASE WHEN effective_redeem_status = 'pending_operator_review' THEN 1 ELSE 0 END), 0) AS pending_operator_review_count
        FROM ui.proposal_resolution_summary
        """
    ).fetchone()
    resolution_runtime = (
        con.execute(
            """
            SELECT
                COALESCE(MAX(status), 'not_run') AS resolution_latest_run_status,
                COUNT(DISTINCT subject_id) AS resolution_subject_count
            FROM src.agent.invocations
            WHERE agent_type = 'resolution'
              AND subject_type = 'uma_proposal'
            """
        ).fetchone()
        if _table_exists(con, "src.agent.invocations")
        else ("not_run", 0)
    )
    triage = con.execute(
        """
        SELECT
            latest_agent_invocation_id,
            latest_agent_status,
            latest_evaluation_method,
            advisory_gate_status,
            updated_at
        FROM ui.opportunity_triage_summary
        ORDER BY updated_at DESC, latest_agent_invocation_id DESC
        LIMIT 1
        """
    ).fetchone() if _table_exists(con, "ui.opportunity_triage_summary") else None
    triage_rollup = con.execute(
        """
        SELECT
            COUNT(*) AS triage_subject_count,
            COALESCE(SUM(CASE WHEN effective_triage_status = 'review' THEN 1 ELSE 0 END), 0) AS pending_review_count,
            COALESCE(SUM(CASE WHEN effective_triage_status = 'accepted' THEN 1 ELSE 0 END), 0) AS accepted_count,
            COALESCE(SUM(CASE WHEN effective_triage_status = 'deferred' THEN 1 ELSE 0 END), 0) AS deferred_count,
            COALESCE(SUM(CASE WHEN effective_triage_status IN ('agent_timeout', 'agent_parse_error', 'agent_failed') THEN 1 ELSE 0 END), 0) AS failed_count
        FROM ui.opportunity_triage_summary
        """
    ).fetchone() if _table_exists(con, "ui.opportunity_triage_summary") else None
    execution_counts = (
        con.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM src.runtime.strategy_runs) AS strategy_run_count,
                (SELECT COUNT(*) FROM src.runtime.trade_tickets) AS trade_ticket_count,
                (SELECT COUNT(*) FROM src.runtime.allocation_decisions) AS allocation_decision_count,
                (SELECT COUNT(*) FROM src.trading.orders) AS paper_order_count,
                (SELECT COUNT(*) FROM src.trading.fills) AS fill_count
            """
        ).fetchone()
        if _table_exists(con, "src.runtime.strategy_runs")
        and _table_exists(con, "src.runtime.trade_tickets")
        and _table_exists(con, "src.runtime.allocation_decisions")
        and _table_exists(con, "src.trading.orders")
        and _table_exists(con, "src.trading.fills")
        else (0, 0, 0, 0, 0)
    )
    row = {
        "generated_at": datetime.now(UTC).replace(tzinfo=None),
        "latest_surface_refresh_run_id": latest_refresh[0] if latest_refresh else None,
        "latest_surface_refresh_status": latest_refresh[1] if latest_refresh else ("read_error" if int(delivery[1] or 0) > 0 else "ok"),
        "ui_replica_status": "ok" if (latest_refresh[2] if latest_refresh else True) else "read_error",
        "ui_lite_status": "ok" if (latest_refresh[3] if latest_refresh else True) else "read_error",
        "readiness_status": readiness[0] if readiness else "UNKNOWN",
        "weather_chain_status": weather_chain[0] if weather_chain else "missing",
        "degraded_surface_count": int(delivery[0] or 0),
        "read_error_surface_count": int(delivery[1] or 0),
        "calibration_hard_gate_market_count": int(calibration[0] or 0),
        "calibration_sample_count": int(calibration_counts[0] or 0),
        "calibration_profile_count": int(calibration_counts[1] or 0),
        "calibration_materialization_count": int(calibration_counts[2] or 0),
        "pending_operator_review_count": int(resolution[0] or 0),
        "resolution_latest_run_status": resolution_runtime[0] if resolution_runtime else "not_run",
        "resolution_subject_count": int(resolution_runtime[1] or 0) if resolution_runtime else 0,
        "triage_latest_run_id": triage[0] if triage else None,
        "triage_latest_run_status": triage[1] if triage else None,
        "triage_latest_evaluation_method": triage[2] if triage else None,
        "triage_advisory_gate_status": triage[3] if triage else "experimental",
        "triage_last_evaluated_at": triage[4] if triage else None,
        "triage_subject_count": int(triage_rollup[0] or 0) if triage_rollup else 0,
        "triage_pending_review_count": int(triage_rollup[1] or 0) if triage_rollup else 0,
        "triage_accepted_count": int(triage_rollup[2] or 0) if triage_rollup else 0,
        "triage_deferred_count": int(triage_rollup[3] or 0) if triage_rollup else 0,
        "triage_failed_count": int(triage_rollup[4] or 0) if triage_rollup else 0,
        "strategy_run_count": int(execution_counts[0] or 0),
        "trade_ticket_count": int(execution_counts[1] or 0),
        "allocation_decision_count": int(execution_counts[2] or 0),
        "paper_order_count": int(execution_counts[3] or 0),
        "fill_count": int(execution_counts[4] or 0),
    }
    frame = pd.DataFrame([row])
    con.register("system_runtime_df", frame)
    con.execute("CREATE OR REPLACE TABLE ui.system_runtime_summary AS SELECT * FROM system_runtime_df")
    table_row_counts["ui.system_runtime_summary"] = 1
    con.unregister("system_runtime_df")


def _create_opportunity_triage_summary(con, *, table_row_counts: dict[str, int]) -> None:
    base_frame = (
        con.execute(
            """
            SELECT
                market_id,
                source_badge,
                source_truth_status,
                primary_score_label
            FROM ui.market_opportunity_summary
            """
        ).df()
        if _table_exists(con, "ui.market_opportunity_summary")
        else pd.DataFrame(columns=["market_id", "source_badge", "source_truth_status", "primary_score_label"])
    )
    invocations = (
        con.execute(
            """
            SELECT
                invocation_id,
                subject_id AS market_id,
                status AS latest_agent_status,
                started_at,
                ended_at
            FROM src.agent.invocations
            WHERE agent_type = 'opportunity_triage'
              AND subject_type = 'weather_market'
            """
        ).df()
        if _table_exists(con, "src.agent.invocations")
        else pd.DataFrame(columns=["invocation_id", "market_id", "latest_agent_status", "started_at", "ended_at"])
    )
    outputs = (
        con.execute(
            """
            SELECT
                invocation_id,
                structured_output_json,
                created_at
            FROM src.agent.outputs
            """
        ).df()
        if _table_exists(con, "src.agent.outputs")
        else pd.DataFrame(columns=["invocation_id", "structured_output_json", "created_at"])
    )
    reviews = (
        con.execute(
            """
            SELECT
                invocation_id,
                review_status,
                reviewed_at
            FROM src.agent.reviews
            """
        ).df()
        if _table_exists(con, "src.agent.reviews")
        else pd.DataFrame(columns=["invocation_id", "review_status", "reviewed_at"])
    )
    evaluations = (
        con.execute(
            """
            SELECT
                evaluation_id,
                invocation_id,
                verification_method,
                score_json,
                is_verified,
                created_at
            FROM src.agent.evaluations
            """
        ).df()
        if _table_exists(con, "src.agent.evaluations")
        else pd.DataFrame(columns=["evaluation_id", "invocation_id", "verification_method", "score_json", "is_verified", "created_at"])
    )
    decisions = (
        con.execute(
            """
            SELECT
                invocation_id,
                subject_id AS market_id,
                decision_status,
                operator_action,
                updated_at
            FROM src.agent.operator_review_decisions
            WHERE agent_type = 'opportunity_triage'
              AND subject_type = 'weather_market'
            """
        ).df()
        if _table_exists(con, "src.agent.operator_review_decisions")
        else pd.DataFrame(columns=["invocation_id", "market_id", "decision_status", "operator_action", "updated_at"])
    )

    if not invocations.empty:
        invocations["sort_ts"] = invocations["ended_at"].fillna(invocations["started_at"])
        invocations = (
            invocations.sort_values(by=["sort_ts", "invocation_id"], ascending=[False, False], na_position="last")
            .drop_duplicates(subset=["market_id"], keep="first")
            .drop(columns=["sort_ts"])
        )
    if not outputs.empty:
        outputs = outputs.sort_values(by=["created_at", "invocation_id"], ascending=[False, False], na_position="last").drop_duplicates(
            subset=["invocation_id"], keep="first"
        )
    if not reviews.empty:
        reviews = reviews.sort_values(by=["reviewed_at", "invocation_id"], ascending=[False, False], na_position="last").drop_duplicates(
            subset=["invocation_id"], keep="first"
        )
    if not evaluations.empty:
        evaluations = evaluations.sort_values(by=["created_at", "evaluation_id"], ascending=[False, False], na_position="last")
    if not decisions.empty:
        decisions = decisions.sort_values(by=["updated_at", "invocation_id"], ascending=[False, False], na_position="last").drop_duplicates(
            subset=["market_id"], keep="first"
        )

    extra_market_ids = []
    if not invocations.empty:
        extra_market_ids = [market_id for market_id in invocations["market_id"].astype(str).tolist() if market_id not in set(base_frame.get("market_id", pd.Series(dtype=str)).astype(str).tolist())]
    if extra_market_ids:
        base_frame = pd.concat(
            [
                base_frame,
                pd.DataFrame(
                    [
                        {
                            "market_id": market_id,
                            "source_badge": "ui_replica",
                            "source_truth_status": "derived",
                            "primary_score_label": "ranking_score",
                        }
                        for market_id in extra_market_ids
                    ]
                ),
            ],
            ignore_index=True,
        )

    frame = base_frame.copy()
    if not invocations.empty:
        frame = frame.merge(invocations[["market_id", "invocation_id", "latest_agent_status", "started_at", "ended_at"]], on="market_id", how="left")
    else:
        frame["invocation_id"] = None
        frame["latest_agent_status"] = None
        frame["started_at"] = None
        frame["ended_at"] = None
    if not outputs.empty:
        frame = frame.merge(outputs[["invocation_id", "structured_output_json", "created_at"]], on="invocation_id", how="left")
    else:
        frame["structured_output_json"] = None
        frame["created_at"] = None
    if not reviews.empty:
        frame = frame.merge(reviews[["invocation_id", "review_status", "reviewed_at"]], on="invocation_id", how="left")
    else:
        frame["review_status"] = None
        frame["reviewed_at"] = None
    latest_evaluations = evaluations.drop_duplicates(subset=["invocation_id"], keep="first") if not evaluations.empty else evaluations
    replay_evaluations = (
        evaluations[evaluations["verification_method"] == "replay_backtest"].drop_duplicates(subset=["invocation_id"], keep="first")
        if not evaluations.empty and "verification_method" in evaluations.columns
        else evaluations.iloc[0:0]
    )
    if not latest_evaluations.empty:
        frame = frame.merge(
            latest_evaluations[["invocation_id", "verification_method", "score_json", "is_verified", "created_at"]],
            on="invocation_id",
            how="left",
            suffixes=("", "_evaluation"),
        )
    else:
        frame["verification_method"] = None
        frame["score_json"] = None
        frame["is_verified"] = None
        frame["created_at_evaluation"] = None
    if not replay_evaluations.empty:
        frame = frame.merge(
            replay_evaluations[["invocation_id", "verification_method", "score_json", "is_verified", "created_at"]].rename(
                columns={
                    "verification_method": "replay_verification_method",
                    "score_json": "replay_score_json",
                    "is_verified": "replay_is_verified",
                    "created_at": "replay_created_at",
                }
            ),
            on="invocation_id",
            how="left",
        )
    else:
        frame["replay_verification_method"] = None
        frame["replay_score_json"] = None
        frame["replay_is_verified"] = None
        frame["replay_created_at"] = None
    if not decisions.empty:
        frame = frame.merge(decisions[["market_id", "decision_status", "operator_action", "updated_at"]], on="market_id", how="left", suffixes=("", "_decision"))
    else:
        frame["decision_status"] = None
        frame["operator_action"] = None
        frame["updated_at"] = None

    def _output_value(payload: Any, key: str) -> Any:
        return _json_object(payload).get(key)

    if frame.empty:
        result = pd.DataFrame(
            columns=[
                "market_id",
                "latest_agent_invocation_id",
                "latest_agent_status",
                "latest_triage_status",
                "priority_band",
                "recommended_operator_action",
                "confidence_band",
                "triage_reason_codes_json",
                "execution_risk_flags_json",
                "supporting_evidence_refs_json",
                "latest_operator_review_status",
                "latest_operator_action",
                "effective_triage_status",
                "advisory_gate_status",
                "advisory_gate_reason_codes_json",
                "latest_evaluation_method",
                "latest_evaluation_verified",
                "updated_at",
                "source_badge",
                "source_truth_status",
                "primary_score_label",
            ]
        )
    else:
        triage_status = frame["structured_output_json"].apply(lambda value: _output_value(value, "triage_status"))
        priority_band = frame["structured_output_json"].apply(lambda value: _output_value(value, "priority_band"))
        recommended_operator_action = frame["structured_output_json"].apply(lambda value: _output_value(value, "recommended_operator_action"))
        confidence_band = frame["structured_output_json"].apply(lambda value: _output_value(value, "confidence_band"))
        triage_reason_codes = frame["structured_output_json"].apply(lambda value: _json_list(_output_value(value, "triage_reason_codes")))
        execution_risk_flags = frame["structured_output_json"].apply(lambda value: _json_list(_output_value(value, "execution_risk_flags")))
        supporting_evidence_refs = frame["structured_output_json"].apply(lambda value: _json_list(_output_value(value, "supporting_evidence_refs")))

        def _effective_status(row: pd.Series) -> str:
            if str(row.get("decision_status") or "").strip():
                return str(row.get("decision_status"))
            if str(row.get("latest_triage_status") or "").strip():
                return str(row.get("latest_triage_status"))
            latest_agent_status = str(row.get("latest_agent_status") or "")
            if latest_agent_status == "timeout":
                return "agent_timeout"
            if latest_agent_status == "parse_error":
                return "agent_parse_error"
            if latest_agent_status == "failure":
                return "agent_failed"
            return "no_triage"

        def _advisory_gate_payload(row: pd.Series) -> tuple[str, str, str | None, bool | None]:
            replay_score = _json_object(row.get("replay_score_json"))
            gate_reasons: list[str] = []
            replay_verified = _coerce_bool(row.get("replay_is_verified"))
            replay_method = str(row.get("replay_verification_method") or "")
            if replay_method != "replay_backtest":
                gate_reasons.append("missing_replay_backtest")
            if replay_verified is not True:
                gate_reasons.append("replay_not_verified")
            if float(replay_score.get("queue_cleanliness_delta") or 0.0) < 0.0:
                gate_reasons.append("queue_cleanliness_below_threshold")
            if float(replay_score.get("priority_precision_proxy") or 0.0) < 0.5:
                gate_reasons.append("priority_precision_below_threshold")
            if float(replay_score.get("false_escalation_rate") or 1.0) > 0.2:
                gate_reasons.append("false_escalation_above_threshold")
            if float(replay_score.get("operator_throughput_delta") or 0.0) < 0.05:
                gate_reasons.append("throughput_delta_below_threshold")
            gate_status = "enabled" if not gate_reasons else "experimental"
            latest_method = replay_method or str(row.get("verification_method") or "")
            latest_verified = replay_verified if replay_method else _coerce_bool(row.get("is_verified"))
            return (
                gate_status,
                json.dumps(gate_reasons, ensure_ascii=True, sort_keys=True),
                latest_method or None,
                latest_verified,
            )

        result = pd.DataFrame(
            {
                "market_id": frame["market_id"],
                "latest_agent_invocation_id": frame["invocation_id"],
                "latest_agent_status": frame["latest_agent_status"],
                "latest_triage_status": triage_status,
                "priority_band": priority_band,
                "recommended_operator_action": recommended_operator_action,
                "confidence_band": confidence_band,
                "triage_reason_codes_json": triage_reason_codes.apply(lambda value: json.dumps(_json_ready(value), ensure_ascii=True, sort_keys=True)),
                "execution_risk_flags_json": execution_risk_flags.apply(lambda value: json.dumps(_json_ready(value), ensure_ascii=True, sort_keys=True)),
                "supporting_evidence_refs_json": supporting_evidence_refs.apply(lambda value: json.dumps(_json_ready(value), ensure_ascii=True, sort_keys=True)),
                "latest_operator_review_status": frame["decision_status"],
                "latest_operator_action": frame["operator_action"],
                "source_badge": frame["source_badge"].fillna("ui_lite"),
                "source_truth_status": frame["source_truth_status"].fillna("ok"),
                "primary_score_label": frame["primary_score_label"].fillna("ranking_score"),
            }
        )
        result["effective_triage_status"] = result.apply(_effective_status, axis=1)
        advisory_gate_values = frame.apply(_advisory_gate_payload, axis=1)
        result["advisory_gate_status"] = advisory_gate_values.apply(lambda value: value[0])
        result["advisory_gate_reason_codes_json"] = advisory_gate_values.apply(lambda value: value[1])
        result["latest_evaluation_method"] = advisory_gate_values.apply(lambda value: value[2])
        result["latest_evaluation_verified"] = advisory_gate_values.apply(lambda value: value[3])
        result["updated_at"] = frame.apply(
            lambda row: next(
                (
                    value
                    for value in [
                        row.get("updated_at"),
                        row.get("reviewed_at"),
                        row.get("created_at_evaluation"),
                        row.get("created_at"),
                        row.get("ended_at"),
                        row.get("started_at"),
                    ]
                    if not _is_missing_scalar(value)
                ),
                None,
            ),
            axis=1,
        )

    con.register("opportunity_triage_summary_df", result)
    con.execute("CREATE OR REPLACE TABLE ui.opportunity_triage_summary AS SELECT * FROM opportunity_triage_summary_df")
    row = con.execute("SELECT COUNT(*) FROM ui.opportunity_triage_summary").fetchone()
    table_row_counts["ui.opportunity_triage_summary"] = int(row[0]) if row is not None else 0
    con.unregister("opportunity_triage_summary_df")


def _create_table_from_src(con, *, target: str, sql_body: str, table_row_counts: dict[str, int]) -> None:
    con.execute(f"CREATE OR REPLACE TABLE {target} AS {sql_body}")
    row = con.execute(f"SELECT COUNT(*) FROM {target}").fetchone()
    table_row_counts[target] = int(row[0]) if row is not None else 0


def _derive_calibration_gate(
    *,
    calibration_freshness_status: str,
    calibration_health_status: str,
    threshold_probability_quality: str,
    sample_count: int,
) -> tuple[str, list[str]]:
    freshness = str(calibration_freshness_status or "fresh")
    health = str(calibration_health_status or "lookup_missing")
    threshold = str(threshold_probability_quality or "lookup_missing")
    reasons: list[str] = []
    if freshness == "stale":
        reasons.append("calibration_freshness_stale")
    elif freshness == "degraded_or_missing":
        reasons.append("calibration_freshness_degraded_or_missing")
    if health == "degraded":
        reasons.append("calibration_health_degraded")
    elif health in {"insufficient_samples", "limited_samples", "sparse"}:
        reasons.append("calibration_health_sparse")
    elif health == "lookup_missing":
        reasons.append("calibration_health_lookup_missing")
    if threshold == "degraded":
        reasons.append("threshold_probability_quality_degraded")
    elif threshold == "sparse":
        reasons.append("threshold_probability_quality_sparse")
    elif threshold == "lookup_missing":
        reasons.append("threshold_probability_quality_lookup_missing")
    if int(sample_count) < 5:
        reasons.append("calibration_sample_count_low")
    if freshness == "degraded_or_missing" and (
        health in {"degraded", "insufficient_samples", "limited_samples", "sparse", "lookup_missing"}
        or threshold in {"sparse", "lookup_missing"}
        or int(sample_count) < 5
    ):
        return "research_only", reasons
    if freshness in {"stale", "degraded_or_missing"}:
        return "review_required", reasons
    if health in {"degraded", "insufficient_samples", "limited_samples", "sparse", "lookup_missing"}:
        return "review_required", reasons
    if threshold in {"degraded", "sparse", "lookup_missing"}:
        return "review_required", reasons
    return "clear", []


def _connect_duckdb(db_path: str, *, read_only: bool):
    try:
        import duckdb  # type: ignore
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError("Missing dependency: duckdb. Install with: pip install duckdb") from exc
    return duckdb.connect(db_path, read_only=read_only)


def _is_missing_scalar(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, (list, dict, tuple, set)):
        return False
    if isinstance(value, str):
        return value == ""
    try:
        missing = pd.isna(value)
        if isinstance(missing, (list, dict, tuple, set)):
            return False
        if hasattr(missing, "shape"):
            return False
        return bool(missing)
    except Exception:  # noqa: BLE001
        return False


def _coerce_ts(value: Any) -> str | None:
    if _is_missing_scalar(value):
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        from datetime import datetime

        return str(datetime.fromisoformat(text).replace(tzinfo=None))
    except Exception:  # noqa: BLE001
        return text


def _coerce_float(value: Any) -> float | None:
    if _is_missing_scalar(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    coerced = _coerce_float(value)
    if coerced is None:
        return None
    try:
        return int(coerced)
    except (TypeError, ValueError):
        return None


def _coerce_bool(value: Any) -> bool | None:
    if _is_missing_scalar(value):
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes"}:
        return True
    if text in {"false", "0", "no"}:
        return False
    return None


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if _is_missing_scalar(value):
        return {}
    try:
        payload = json.loads(str(value))
    except Exception:  # noqa: BLE001
        return {}
    return payload if isinstance(payload, dict) else {}


def _json_array_of_text(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if item not in {None, ""}]
    if _is_missing_scalar(value):
        return []
    try:
        payload = json.loads(str(value))
    except Exception:  # noqa: BLE001
        return []
    if not isinstance(payload, list):
        return []
    return [str(item) for item in payload if item not in {None, ""}]


def _source_disagreement(value: Any) -> str:
    payload = _json_object(value)
    changed_fields = payload.get("changed_fields")
    if not isinstance(changed_fields, list):
        return "unavailable"
    normalized = {str(item) for item in changed_fields}
    if {"temperature_distribution", "pricing_context"} & normalized:
        return "different"
    return "match"


def _resolution_value(expected_outcome: Any) -> float | None:
    text = str(expected_outcome or "").strip().upper()
    if not text:
        return None
    return 1.0 if text == "YES" else 0.0 if text == "NO" else None


_LIFECYCLE_PRIORITY = {
    "resolved": 0,
    "filled_unresolved": 1,
    "partially_filled": 2,
    "cancelled": 3,
    "working_unfilled": 4,
    "submitted_ack": 5,
    "submit_rejected": 6,
    "sign_rejected": 7,
    "gate_rejected": 8,
    "signed_not_submitted": 9,
    "ticket_created": 10,
}

_MARKET_MISS_PRIORITY = {
    "gate_rejected": 0,
    "sign_rejected": 1,
    "submit_rejected": 2,
    "working_unfilled": 3,
    "cancelled": 4,
    "partial_fill": 5,
    "captured_unresolved": 6,
    "captured": 7,
}


def _json_array_text(values: list[str]) -> str:
    return json.dumps(values, ensure_ascii=True, sort_keys=True)


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if _is_missing_scalar(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _normalize_submit_mode(value: Any) -> str:
    return str(value or "").strip().lower()


def _normalize_stage(value: Any) -> str:
    return str(value or "").strip()


def _lifecycle_sort_key(stage: Any) -> int:
    return _LIFECYCLE_PRIORITY.get(_normalize_stage(stage), 999)


def _evaluation_status_for_ticket(*, filled_quantity: float, resolution_value: float | None) -> str:
    if resolution_value is not None and filled_quantity > 0:
        return "resolved"
    if filled_quantity > 0:
        return "pending_resolution"
    return "pending_fill"


def _execution_lifecycle_stage(
    *,
    evaluation_status: str,
    filled_quantity: float,
    fill_ratio: float,
    execution_result: Any,
    order_status: Any,
    latest_submit_status: Any,
    live_prereq_execution_status: Any,
    external_order_status: Any,
    gate_allowed: Any,
    latest_sign_attempt_id: Any,
    latest_submit_attempt_id: Any,
) -> str:
    execution_result_text = str(execution_result or "").strip()
    order_status_text = str(order_status or "").strip()
    live_status_text = str(live_prereq_execution_status or "").strip()
    submit_status_text = str(latest_submit_status or "").strip()
    external_order_text = str(external_order_status or "").strip()
    gate_allowed_bool = bool(gate_allowed) if gate_allowed is not None else None

    if evaluation_status == "resolved":
        return "resolved"
    if execution_result_text == "partial_filled" or (0.0 < fill_ratio < 1.0):
        return "partially_filled"
    if filled_quantity > 0 and evaluation_status == "pending_resolution":
        return "filled_unresolved"
    if execution_result_text == "cancelled" or order_status_text == "cancelled":
        return "cancelled"
    if order_status_text == "posted" and filled_quantity <= 0:
        return "working_unfilled"
    if submit_status_text == "accepted":
        return "submitted_ack"
    if live_status_text == "submit_rejected" or external_order_text == "rejected":
        return "submit_rejected"
    if live_status_text == "sign_rejected":
        return "sign_rejected"
    if gate_allowed_bool is False:
        return "gate_rejected"
    if latest_sign_attempt_id and not latest_submit_attempt_id:
        return "signed_not_submitted"
    return "ticket_created"


def _miss_reason_bucket_for_stage(stage: str) -> str:
    if stage == "resolved":
        return "captured_resolved"
    if stage == "filled_unresolved":
        return "captured_unresolved"
    if stage == "partially_filled":
        return "partial_fill"
    if stage == "cancelled":
        return "cancelled"
    if stage == "working_unfilled":
        return "working_unfilled"
    if stage == "submit_rejected":
        return "submit_rejected"
    if stage == "sign_rejected":
        return "sign_rejected"
    if stage == "gate_rejected":
        return "gate_rejected"
    return "not_submitted"


def _distortion_reason_codes(
    *,
    stage: str,
    source_disagreement: str,
    realized_pnl: float | None,
    adverse_fill_slippage_bps: float | None,
) -> list[str]:
    reasons: list[str] = []
    if source_disagreement == "different":
        reasons.append("forecast_source_disagreement")
    if realized_pnl is not None and realized_pnl < 0:
        reasons.append("forecast_realized_pnl_negative")
    if (adverse_fill_slippage_bps or 0.0) > 0:
        reasons.append("execution_adverse_fill")
    if stage == "partially_filled":
        reasons.append("execution_partial_fill")
    if stage == "cancelled":
        reasons.append("execution_cancelled")
    if stage in {"working_unfilled", "submitted_ack"}:
        reasons.append("execution_unfilled")
    return reasons


def _dominant_bucket(values: list[str], *, priority: dict[str, int], default: str) -> str:
    if not values:
        return default
    counts = pd.Series(values, dtype="string").value_counts(dropna=True)
    if counts.empty:
        return default
    best_count = int(counts.max())
    candidates = [str(index) for index, value in counts.items() if int(value) == best_count]
    candidates.sort(key=lambda item: priority.get(item, 999))
    return candidates[0] if candidates else default


def _opportunity_bucket(edge_bps: int) -> str:
    magnitude = abs(int(edge_bps))
    if magnitude >= 1500:
        return "high_edge"
    if magnitude >= 750:
        return "medium_edge"
    if magnitude > 0:
        return "low_edge"
    return "negative_edge"


def _table_exists(con, table_name: str) -> bool:
    parts = table_name.split(".")
    if len(parts) == 2:
        schema, table = parts
    elif len(parts) == 3:
        _, schema, table = parts
    else:
        raise ValueError(f"Unsupported table name: {table_name}")
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema, table],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _table_columns(con, table_name: str) -> set[str]:
    return {str(row[1]) for row in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()}


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(_json_ready(payload), ensure_ascii=True, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


def _safe_stat(path: Path) -> dict[str, int | None]:
    if not path.exists():
        return {"size_bytes": None, "mtime_ms": None}
    st = path.stat()
    return {"size_bytes": int(st.st_size), "mtime_ms": int(st.st_mtime * 1000)}


def _clone_or_copy(src: Path, dst: Path, *, timeout_s: int = 30) -> None:
    if dst.exists():
        dst.unlink()
    try:
        if sys.platform == "darwin":
            subprocess.run(["cp", "-c", str(src), str(dst)], check=True, capture_output=True, timeout=timeout_s)
            return
        if sys.platform.startswith("linux"):
            subprocess.run(["cp", "--reflink=auto", str(src), str(dst)], check=True, capture_output=True, timeout=timeout_s)
            return
    except Exception:
        pass
    shutil.copy2(src, dst)


def _create_source_snapshot(src: Path) -> Path:
    snapshot = src.with_name(f".{src.name}.ui_lite_snapshot_{os.getpid()}_{int(time.time() * 1000)}")
    _clone_or_copy(src, snapshot, timeout_s=45)
    return snapshot


__all__ = [
    "DEFAULT_READINESS_EVIDENCE_JSON_PATH",
    "DEFAULT_READINESS_REPORT_JSON_PATH",
    "DEFAULT_UI_DB_REPLICA_SOURCE_PATH",
    "DEFAULT_UI_LITE_DB_PATH",
    "UiLiteBuildResult",
    "build_ui_lite_db_once",
    "default_readiness_report_json_path",
    "default_ui_lite_db_path",
    "default_ui_lite_meta_path",
    "load_ui_lite_meta",
    "run_ui_lite_db_loop",
    "validate_ui_lite_db",
]
