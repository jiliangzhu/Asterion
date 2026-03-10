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

from asterion_core.storage.logger import get_logger


log = get_logger(__name__)

DEFAULT_UI_LITE_DB_PATH = "data/ui/asterion_ui_lite.duckdb"
DEFAULT_UI_DB_REPLICA_SOURCE_PATH = "data/ui/asterion_ui.duckdb"
DEFAULT_READINESS_REPORT_JSON_PATH = "data/ui/asterion_readiness_p3.json"

_REQUIRED_UI_TABLES = [
    "ui.market_watch_summary",
    "ui.proposal_resolution_summary",
    "ui.execution_ticket_summary",
    "ui.agent_review_summary",
    "ui.phase_readiness_summary",
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
    refresh_interval_s: float | None = None,
) -> UiLiteBuildResult:
    started_ms = int(time.time() * 1000)
    src = Path(src_db_path or os.getenv("ASTERION_UI_DB_REPLICA_PATH", DEFAULT_UI_DB_REPLICA_SOURCE_PATH))
    dst = Path(dst_db_path or default_ui_lite_db_path())
    meta = Path(meta_path or default_ui_lite_meta_path(lite_db_path=str(dst)))
    report_path = readiness_report_json_path or default_readiness_report_json_path()
    prev = load_ui_lite_meta(str(meta)) or {}

    def _emit(ok: bool, error: str | None, table_row_counts: dict[str, int] | None = None) -> UiLiteBuildResult:
        now_ms = int(time.time() * 1000)
        src_stat = _safe_stat(src)
        dst_stat = _safe_stat(dst)
        payload = {
            "source_db_path": str(src),
            "lite_db_path": str(dst),
            "readiness_report_json_path": str(report_path),
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
    interval_s: float = 30.0,
) -> None:
    while True:
        result = build_ui_lite_db_once(
            src_db_path=src_db_path,
            dst_db_path=dst_db_path,
            meta_path=meta_path,
            readiness_report_json_path=readiness_report_json_path,
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
                ticket.market_id,
                ticket.token_id,
                ticket.outcome,
                ticket.side,
                ticket.route_action,
                ticket.size,
                ticket.reference_price,
                ticket.fair_value,
                gate.gate_id,
                gate.allowed AS gate_allowed,
                gate.reason AS gate_reason,
                gate.reason_codes_json,
                ord.order_id,
                ord.status AS order_status,
                ord.reservation_id,
                reservation.status AS reservation_status,
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
            LEFT JOIN latest_reservation_event_by_ticket reservation_event_ticket
                ON reservation_event_ticket.ticket_id = ticket.ticket_id
            LEFT JOIN latest_reservation_event_by_request reservation_event_request
                ON reservation_event_request.request_id = ticket.request_id
               AND reservation_event_ticket.reservation_id IS NULL
            LEFT JOIN src.trading.reservations reservation
                ON reservation.reservation_id = COALESCE(reservation_event_ticket.reservation_id, reservation_event_request.reservation_id)
            LEFT JOIN latest_journal_by_ticket journal_ticket ON journal_ticket.ticket_id = ticket.ticket_id
            LEFT JOIN latest_journal_by_request journal_request
                ON journal_request.request_id = ticket.request_id
               AND journal_ticket.event_id IS NULL
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
        _create_phase_readiness_summary(
            con,
            report_path=readiness_report_json_path,
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
