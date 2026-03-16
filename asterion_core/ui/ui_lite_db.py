from __future__ import annotations

import dataclasses
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd

from asterion_core.storage.logger import get_logger
from domains.weather.opportunity import build_weather_opportunity_assessment, derive_opportunity_side


log = get_logger(__name__)

DEFAULT_UI_LITE_DB_PATH = "data/ui/asterion_ui_lite.duckdb"
DEFAULT_UI_DB_REPLICA_SOURCE_PATH = "data/ui/asterion_ui.duckdb"
DEFAULT_READINESS_REPORT_JSON_PATH = "data/ui/asterion_readiness_p3.json"
DEFAULT_READINESS_EVIDENCE_JSON_PATH = "data/ui/asterion_readiness_evidence_p4.json"

_REQUIRED_UI_TABLES = [
    "ui.market_watch_summary",
    "ui.market_opportunity_summary",
    "ui.proposal_resolution_summary",
    "ui.execution_ticket_summary",
    "ui.execution_run_summary",
    "ui.execution_exception_summary",
    "ui.live_prereq_execution_summary",
    "ui.live_prereq_wallet_summary",
    "ui.paper_run_journal_summary",
    "ui.daily_ops_summary",
    "ui.daily_review_input",
    "ui.agent_review_summary",
    "ui.phase_readiness_summary",
    "ui.readiness_evidence_summary",
    "ui.predicted_vs_realized_summary",
    "ui.watch_only_vs_executed_summary",
    "ui.execution_science_summary",
    "ui.market_research_summary",
    "ui.calibration_health_summary",
]


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
                continuity.to_block AS latest_continuity_to_block
            FROM src.resolution.uma_proposals p
            LEFT JOIN latest_verification v ON v.proposal_id = p.proposal_id
            LEFT JOIN src.resolution.proposal_evidence_links link ON link.proposal_id = p.proposal_id
            LEFT JOIN latest_redeem redeem ON redeem.proposal_id = p.proposal_id
            LEFT JOIN latest_continuity continuity ON TRUE
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
        _create_market_opportunity_summary(con, table_row_counts=table_row_counts)
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
        _create_predicted_vs_realized_summary(
            con,
            table_row_counts=table_row_counts,
        )
        _create_watch_only_vs_executed_summary(
            con,
            table_row_counts=table_row_counts,
        )
        _create_execution_science_summary(
            con,
            table_row_counts=table_row_counts,
        )
        _create_market_research_summary(
            con,
            table_row_counts=table_row_counts,
        )
        _create_calibration_health_summary(
            con,
            table_row_counts=table_row_counts,
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
        return table_row_counts
    finally:
        con.close()


def _create_phase_readiness_summary(con, *, report_path: Path, table_row_counts: dict[str, int]) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE ui.phase_readiness_summary (
            target TEXT,
            gate_name TEXT,
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
                target, gate_name, passed, all_passed, go_decision, decision_reason,
                generated_at, checks_json, violations_json, warnings_json, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        resolution_value = _resolution_value(item.get("expected_outcome"))
        evaluation_status = _evaluation_status_for_ticket(
            filled_quantity=filled_quantity,
            resolution_value=resolution_value,
        )
        stage = _execution_lifecycle_stage(
            evaluation_status=evaluation_status,
            filled_quantity=filled_quantity,
            fill_ratio=fill_ratio,
            execution_result=item.get("execution_result"),
            order_status=item.get("order_status"),
            latest_submit_status=item.get("latest_submit_status"),
            live_prereq_execution_status=item.get("live_prereq_execution_status"),
            external_order_status=item.get("external_order_status"),
            gate_allowed=item.get("gate_allowed"),
            latest_sign_attempt_id=item.get("latest_sign_attempt_id"),
            latest_submit_attempt_id=item.get("latest_submit_attempt_id"),
        )
        side = str(item.get("side") or "").strip().upper()
        adverse_fill_slippage_bps = None
        if expected_fill_price is not None and realized_fill_price is not None:
            if side == "SELL":
                adverse_fill_slippage_bps = max((expected_fill_price - realized_fill_price) * 10000.0, 0.0)
            else:
                adverse_fill_slippage_bps = max((realized_fill_price - expected_fill_price) * 10000.0, 0.0)
        realized_pnl = None
        post_trade_error = None
        if resolution_value is not None and realized_fill_price is not None and filled_quantity > 0:
            contract_value = resolution_value if str(item.get("outcome") or "").upper() == "YES" else 1.0 - resolution_value
            if side == "SELL":
                realized_pnl = (realized_fill_price - contract_value) * filled_quantity - total_fee
            else:
                realized_pnl = (contract_value - realized_fill_price) * filled_quantity - total_fee
            implied_pnl = (float(predicted_edge_bps) / 10000.0) * filled_quantity
            post_trade_error = realized_pnl - implied_pnl
        latest_fill_at = item.get("last_fill_at")
        latest_resolution_at = item.get("latest_resolution_at")
        resolution_lag_hours = None
        if latest_fill_at is not None and latest_resolution_at is not None:
            fill_ts = pd.to_datetime(latest_fill_at, errors="coerce")
            resolution_ts = pd.to_datetime(latest_resolution_at, errors="coerce")
            if pd.notna(fill_ts) and pd.notna(resolution_ts):
                resolution_lag_hours = float((resolution_ts - fill_ts).total_seconds() / 3600.0)
        source_disagreement = _source_disagreement(item.get("diff_summary_json"))
        distortion_reasons = _distortion_reason_codes(
            stage=stage,
            source_disagreement=source_disagreement,
            realized_pnl=realized_pnl,
            adverse_fill_slippage_bps=adverse_fill_slippage_bps,
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
                "realized_pnl": realized_pnl,
                "resolution_value": resolution_value,
                "forecast_freshness": str(pricing_context.get("source_freshness_status") or "unavailable"),
                "source_disagreement": source_disagreement,
                "post_trade_error": post_trade_error,
                "evaluation_status": evaluation_status,
                "latest_fill_at": latest_fill_at,
                "latest_resolution_at": latest_resolution_at,
                "execution_lifecycle_stage": stage,
                "fill_ratio": fill_ratio,
                "adverse_fill_slippage_bps": adverse_fill_slippage_bps,
                "resolution_lag_hours": resolution_lag_hours,
                "miss_reason_bucket": _miss_reason_bucket_for_stage(stage),
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


def _create_predicted_vs_realized_summary(con, *, table_row_counts: dict[str, int]) -> None:
    con.execute("DROP TABLE IF EXISTS ui.predicted_vs_realized_summary")
    frame = _build_execution_path_frame(con)
    if frame.empty:
        con.register("predicted_vs_realized_df", _empty_predicted_vs_realized_frame())
    else:
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
                distortion_reason_bucket TEXT
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
                distortion_reason_bucket TEXT
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
    frame = pd.DataFrame(rows)
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
                dominant_distortion_reason_bucket TEXT
            )
            """
        )
        table_row_counts["ui.execution_science_summary"] = 0
        return

    rows: list[dict[str, Any]] = []
    submitted_ack_stages = {"submitted_ack", "working_unfilled", "partially_filled", "filled_unresolved", "resolved", "cancelled"}
    rejected_stages = {"submit_rejected", "sign_rejected", "gate_rejected"}
    for cohort_type, key_column in [("market", "market_id"), ("strategy", "strategy_id"), ("wallet", "wallet_id")]:
        grouped = frame.groupby(key_column, dropna=False)
        for cohort_key, cohort_frame in grouped:
            ticket_count = int(len(cohort_frame.index))
            submitted_ack_count = int(cohort_frame["execution_lifecycle_stage"].isin(list(submitted_ack_stages)).sum())
            filled_ticket_count = int((pd.to_numeric(cohort_frame["filled_quantity"], errors="coerce").fillna(0) > 0).sum())
            resolved_ticket_count = int((cohort_frame["evaluation_status"] == "resolved").sum())
            partial_fill_count = int((cohort_frame["execution_lifecycle_stage"] == "partially_filled").sum())
            cancelled_count = int((cohort_frame["execution_lifecycle_stage"] == "cancelled").sum())
            rejected_count = int(cohort_frame["execution_lifecycle_stage"].isin(list(rejected_stages)).sum())
            working_unfilled_count = int((cohort_frame["execution_lifecycle_stage"] == "working_unfilled").sum())
            distortion_buckets: list[str] = []
            for _, ticket_row in cohort_frame.iterrows():
                stage = str(ticket_row.get("execution_lifecycle_stage") or "")
                values = json.loads(str(ticket_row.get("distortion_reason_codes_json") or "[]"))
                if stage in {"working_unfilled", "partially_filled", "cancelled", "submit_rejected"}:
                    distortion_buckets.append("execution_distortion")
                elif any(str(item).startswith("execution_") for item in values):
                    distortion_buckets.append("execution_distortion")
                elif any(str(item).startswith("forecast_") for item in values):
                    distortion_buckets.append("forecast_distortion")
                else:
                    distortion_buckets.append("none")
            rows.append(
                {
                    "cohort_type": cohort_type,
                    "cohort_key": str(cohort_key),
                    "ticket_count": ticket_count,
                    "submitted_ack_count": submitted_ack_count,
                    "filled_ticket_count": filled_ticket_count,
                    "resolved_ticket_count": resolved_ticket_count,
                    "partial_fill_count": partial_fill_count,
                    "cancelled_count": cancelled_count,
                    "rejected_count": rejected_count,
                    "working_unfilled_count": working_unfilled_count,
                    "avg_predicted_edge_bps": float(pd.to_numeric(cohort_frame["predicted_edge_bps"], errors="coerce").dropna().mean()) if "predicted_edge_bps" in cohort_frame.columns else None,
                    "avg_realized_pnl": float(pd.to_numeric(cohort_frame["realized_pnl"], errors="coerce").dropna().mean()) if "realized_pnl" in cohort_frame.columns else None,
                    "avg_post_trade_error": float(pd.to_numeric(cohort_frame["post_trade_error"], errors="coerce").dropna().mean()) if "post_trade_error" in cohort_frame.columns else None,
                    "avg_adverse_fill_slippage_bps": float(pd.to_numeric(cohort_frame["adverse_fill_slippage_bps"], errors="coerce").dropna().mean()) if "adverse_fill_slippage_bps" in cohort_frame.columns else None,
                    "submission_capture_ratio": float(submitted_ack_count / ticket_count) if ticket_count > 0 else 0.0,
                    "fill_capture_ratio": float(filled_ticket_count / ticket_count) if ticket_count > 0 else 0.0,
                    "resolution_capture_ratio": float(resolved_ticket_count / ticket_count) if ticket_count > 0 else 0.0,
                    "dominant_miss_reason_bucket": _dominant_bucket(
                        [str(value) for value in cohort_frame["miss_reason_bucket"].dropna().tolist()],
                        priority={**_MARKET_MISS_PRIORITY, "captured_resolved": 8, "not_submitted": 3},
                        default="not_submitted",
                    ),
                    "dominant_distortion_reason_bucket": _dominant_bucket(
                        distortion_buckets,
                        priority={"execution_distortion": 0, "forecast_distortion": 1, "ranking_distortion": 2, "none": 3},
                        default="none",
                    ),
                }
            )
    science = pd.DataFrame(
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
        resolved_trade_count = int(item.get("resolved_trade_count") or 0)
        filled_unresolved_count = int(item.get("filled_unresolved_count") or 0)
        submitted_only_count = int(item.get("submitted_only_count") or 0)
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


def _create_calibration_health_summary(con, *, table_row_counts: dict[str, int]) -> None:
    if not _table_exists(con, "src.weather.forecast_calibration_samples"):
        con.execute(
            """
            CREATE OR REPLACE TABLE ui.calibration_health_summary (
                station_id TEXT,
                source TEXT,
                forecast_horizon_bucket TEXT,
                season_bucket TEXT,
                sample_count BIGINT,
                mean_abs_residual DOUBLE,
                p90_abs_residual DOUBLE,
                calibration_health_status TEXT
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
            forecast_horizon_bucket,
            season_bucket,
            ABS(residual) AS abs_residual
        FROM src.weather.forecast_calibration_samples
        """
    ).df()
    if base.empty:
        con.execute(
            """
            CREATE OR REPLACE TABLE ui.calibration_health_summary (
                station_id TEXT,
                source TEXT,
                forecast_horizon_bucket TEXT,
                season_bucket TEXT,
                sample_count BIGINT,
                mean_abs_residual DOUBLE,
                p90_abs_residual DOUBLE,
                calibration_health_status TEXT
            )
            """
        )
        table_row_counts["ui.calibration_health_summary"] = 0
        return
    rows: list[dict[str, Any]] = []
    grouped = base.groupby(["station_id", "source", "forecast_horizon_bucket", "season_bucket"], dropna=False)
    for (station_id, source, horizon, season), frame in grouped:
        series = pd.to_numeric(frame["abs_residual"], errors="coerce").dropna()
        sample_count = int(len(series.index))
        mean_abs_residual = float(series.mean()) if sample_count else None
        p90_abs_residual = float(series.quantile(0.9)) if sample_count else None
        if sample_count < 5:
            status = "insufficient_samples"
        elif (mean_abs_residual or 0.0) <= 1.5:
            status = "healthy"
        elif (mean_abs_residual or 0.0) <= 3.0:
            status = "watch"
        else:
            status = "degraded"
        rows.append(
            {
                "station_id": station_id,
                "source": source,
                "forecast_horizon_bucket": horizon,
                "season_bucket": season,
                "sample_count": sample_count,
                "mean_abs_residual": mean_abs_residual,
                "p90_abs_residual": p90_abs_residual,
                "calibration_health_status": status,
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
    rows: list[dict[str, Any]] = []
    for _, row in frame.iterrows():
        pricing_context = _json_object(row.get("pricing_context_json"))
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
        sample_count = int(_coerce_float(pricing_context.get("sample_count")) or 0)
        calibration_multiplier = _coerce_float(pricing_context.get("calibration_multiplier"))
        calibration_reason_codes = pricing_context.get("calibration_reason_codes")
        if not isinstance(calibration_reason_codes, list):
            calibration_reason_codes = None
        if not token_id or not outcome or market_price <= 0.0:
            actionability_status = (
                "blocked"
                if (not bool(row.get("accepting_orders")) or str(row.get("live_prereq_status") or "") == "attention_required")
                else "review_required"
                if str(row.get("agent_review_status") or "") != "passed"
                else "no_trade"
            )
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
                    "sample_count": sample_count,
                    "uncertainty_multiplier": 0.0,
                    "uncertainty_penalty_bps": 0,
                    "ranking_penalty_reasons": calibration_reason_codes or [],
                    "mapping_confidence": mapping_confidence,
                    "source_freshness_status": source_freshness_status,
                    "price_staleness_ms": price_staleness_ms,
                    "market_quality_status": market_quality_status,
                    "liquidity_proxy": 25.0 if not bool(row.get("accepting_orders")) else 55.0,
                    "liquidity_penalty_bps": None,
                    "confidence_score": confidence_score,
                    "confidence_proxy": confidence_score,
                    "ops_readiness_score": 0.0,
                    "expected_value_score": 0.0,
                    "expected_pnl_score": 0.0,
                    "ranking_score": 0.0,
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
            sample_count=sample_count,
            calibration_multiplier=calibration_multiplier,
            calibration_reason_codes=calibration_reason_codes,
            source_context={
                "calibration_health_status": calibration_health_status,
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
                "sample_count": assessment.sample_count,
                "uncertainty_multiplier": assessment.uncertainty_multiplier,
                "uncertainty_penalty_bps": assessment.uncertainty_penalty_bps,
                "ranking_penalty_reasons": assessment.ranking_penalty_reasons,
                "mapping_confidence": assessment.assessment_context_json.get("mapping_confidence"),
                "source_freshness_status": assessment.assessment_context_json.get("source_freshness_status"),
                "price_staleness_ms": assessment.assessment_context_json.get("price_staleness_ms"),
                "market_quality_status": assessment.assessment_context_json.get("market_quality_status"),
                "liquidity_proxy": assessment.depth_proxy * 100.0,
                "liquidity_penalty_bps": assessment.liquidity_penalty_bps,
                "confidence_score": assessment.confidence_score,
                "confidence_proxy": assessment.confidence_score,
                "ops_readiness_score": assessment.ops_readiness_score,
                "expected_value_score": assessment.expected_value_score,
                "expected_pnl_score": assessment.expected_pnl_score,
                "ranking_score": assessment.ranking_score,
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
        "market_quality_status",
        "liquidity_proxy",
        "liquidity_penalty_bps",
        "confidence_score",
        "confidence_proxy",
        "ops_readiness_score",
        "expected_value_score",
        "expected_pnl_score",
        "ranking_score",
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
    con.register("market_opportunity_summary_df", result)
    con.execute("CREATE OR REPLACE TABLE ui.market_opportunity_summary AS SELECT * FROM market_opportunity_summary_df")
    row = con.execute("SELECT COUNT(*) FROM ui.market_opportunity_summary").fetchone()
    table_row_counts["ui.market_opportunity_summary"] = int(row[0]) if row is not None else 0
    con.unregister("market_opportunity_summary_df")


def _create_table_from_src(con, *, target: str, sql_body: str, table_row_counts: dict[str, int]) -> None:
    con.execute(f"CREATE OR REPLACE TABLE {target} AS {sql_body}")
    row = con.execute(f"SELECT COUNT(*) FROM {target}").fetchone()
    table_row_counts[target] = int(row[0]) if row is not None else 0


def _connect_duckdb(db_path: str, *, read_only: bool):
    try:
        import duckdb  # type: ignore
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError("Missing dependency: duckdb. Install with: pip install duckdb") from exc
    return duckdb.connect(db_path, read_only=read_only)


def _coerce_ts(value: Any) -> str | None:
    if value is None:
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
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value in {None, ""}:
        return {}
    try:
        payload = json.loads(str(value))
    except Exception:  # noqa: BLE001
        return {}
    return payload if isinstance(payload, dict) else {}


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


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=True, sort_keys=True), encoding="utf-8")
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
