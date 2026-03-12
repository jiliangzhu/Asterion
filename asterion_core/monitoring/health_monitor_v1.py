from __future__ import annotations

import json
import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class WSHealthSnapshot:
    connected: bool
    last_message_ts_ms: int
    delay_ms: int
    delay_p50_ms: float
    delay_p95_ms: float
    messages_per_min: float
    reconnect_count_1h: int


@dataclass(frozen=True)
class QuoteHealthSnapshot:
    active_markets: int
    stale_markets: int
    avg_quote_age_ms: float
    max_quote_age_ms: int
    quote_update_rate_per_min: float


@dataclass(frozen=True)
class QueueHealthSnapshot:
    pending_tasks: int
    backlog_p95_ms: float
    write_rate_per_min: float
    error_rate_per_min: float
    dead_tasks_1h: int


@dataclass(frozen=True)
class DegradeStatus:
    active: bool
    reason: str | None = None
    since_ts_ms: int | None = None
    watch_only: bool = False


@dataclass(frozen=True)
class SystemHealthSnapshot:
    timestamp_ms: int
    ws_health: WSHealthSnapshot
    quote_health: QuoteHealthSnapshot
    queue_health: QueueHealthSnapshot
    degrade_status: DegradeStatus
    realtime_latency_p50_ms: float
    realtime_latency_p95_ms: float
    tickets_per_min: float


@dataclass(frozen=True)
class SignerHealthSnapshot:
    request_count: int
    rejected_count: int
    latest_status: str | None
    latest_created_at: str | None


@dataclass(frozen=True)
class SubmitterHealthSnapshot:
    sign_only_signed_count: int
    submit_preview_count: int
    submit_accepted_count: int
    submit_rejected_count: int
    latest_submit_created_at: str | None


@dataclass(frozen=True)
class ChainTxHealthSnapshot:
    approve_attempt_count: int
    approve_rejected_count: int
    latest_approve_status: str | None
    latest_approve_created_at: str | None


@dataclass(frozen=True)
class ExternalExecutionHealthSnapshot:
    external_order_observation_count: int
    external_fill_observation_count: int
    external_reconciliation_ok_count: int
    external_reconciliation_mismatch_count: int
    external_reconciliation_unverified_count: int
    latest_observed_at: str | None


@dataclass(frozen=True)
class LivePrereqHealthSnapshot:
    timestamp_ms: int
    queue_health: QueueHealthSnapshot
    signer_health: SignerHealthSnapshot
    submitter_health: SubmitterHealthSnapshot
    chain_tx_health: ChainTxHealthSnapshot
    external_execution_health: ExternalExecutionHealthSnapshot


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = int((len(ordered) - 1) * p / 100.0)
    return ordered[min(idx, len(ordered) - 1)]


def _get_quote_map(state_store: Any) -> dict[str, Any]:
    for attr in ("latest_quote_by_market_token", "latest_quote_by_market_asset"):
        value = getattr(state_store, attr, None)
        if isinstance(value, dict):
            return value
    return {}


def collect_ws_health(state_store: Any, window_minutes: int = 10) -> WSHealthSnapshot:
    now_ms = int(time.time() * 1000)
    delay_samples = list(getattr(state_store, "_ws_delay_samples_ms", []))

    if delay_samples:
        float_samples = [float(x) for x in delay_samples]
        delay_p50 = _percentile(float_samples, 50.0)
        delay_p95 = _percentile(float_samples, 95.0)
        current_delay = int(delay_samples[-1])
    else:
        delay_p50 = 0.0
        delay_p95 = 0.0
        current_delay = 0

    quote_count = len(_get_quote_map(state_store))
    reconnect_count = int(getattr(state_store, "reconnect_count_1h", 0) or 0)
    return WSHealthSnapshot(
        connected=current_delay < 10_000,
        last_message_ts_ms=now_ms - current_delay,
        delay_ms=current_delay,
        delay_p50_ms=delay_p50,
        delay_p95_ms=delay_p95,
        messages_per_min=float(quote_count) / max(1, window_minutes),
        reconnect_count_1h=reconnect_count,
    )


def collect_quote_health(state_store: Any, stale_threshold_ms: int = 5_000) -> QuoteHealthSnapshot:
    now_ms = int(time.time() * 1000)
    quotes = _get_quote_map(state_store)
    if not quotes:
        return QuoteHealthSnapshot(0, 0, 0.0, 0, 0.0)

    ages_ms: list[int] = []
    stale_count = 0
    max_age = 0
    for quote in quotes.values():
        last_updated_ms = getattr(quote, "last_updated_ms", None)
        if last_updated_ms is None:
            last_updated_ms = getattr(quote, "last_received_at_ms", now_ms)
        age_ms = now_ms - int(last_updated_ms)
        ages_ms.append(age_ms)
        max_age = max(max_age, age_ms)
        if age_ms > stale_threshold_ms:
            stale_count += 1

    active_markets = len(quotes)
    avg_age = sum(ages_ms) / len(ages_ms)
    return QuoteHealthSnapshot(
        active_markets=active_markets,
        stale_markets=stale_count,
        avg_quote_age_ms=avg_age,
        max_quote_age_ms=max_age,
        quote_update_rate_per_min=float(active_markets),
    )


def collect_queue_health(queue_path: str, window_minutes: int = 10) -> QueueHealthSnapshot:
    now_ms = int(time.time() * 1000)
    window_start_ms = now_ms - window_minutes * 60 * 1000

    pending_tasks = 0
    backlog_p95 = 0.0
    write_rate = 0.0
    error_rate = 0.0
    dead_tasks = 0

    if os.path.exists(queue_path):
        try:
            conn = sqlite3.connect(queue_path)
            cursor = conn.cursor()

            cursor.execute("SELECT COUNT(*) FROM write_queue_tasks WHERE status = 'PENDING'")
            row = cursor.fetchone()
            pending_tasks = int(row[0]) if row else 0

            cursor.execute(
                """
                SELECT COUNT(*) FROM write_queue_tasks
                WHERE status = 'SUCCEEDED' AND ended_ts_ms >= ?
                """,
                (window_start_ms,),
            )
            row = cursor.fetchone()
            write_rate = float(int(row[0]) if row else 0) / max(1, window_minutes)

            cursor.execute(
                """
                SELECT COUNT(*) FROM write_queue_tasks
                WHERE status IN ('FAILED', 'DEAD') AND ended_ts_ms >= ?
                """,
                (window_start_ms,),
            )
            row = cursor.fetchone()
            error_rate = float(int(row[0]) if row else 0) / max(1, window_minutes)

            one_hour_ago = now_ms - 60 * 60 * 1000
            cursor.execute(
                """
                SELECT COUNT(*) FROM write_queue_tasks
                WHERE status = 'DEAD' AND ended_ts_ms >= ?
                """,
                (one_hour_ago,),
            )
            row = cursor.fetchone()
            dead_tasks = int(row[0]) if row else 0

            cursor.execute(
                """
                SELECT created_ts_ms FROM write_queue_tasks
                WHERE status = 'PENDING' ORDER BY created_ts_ms
                """
            )
            rows = cursor.fetchall()
            if rows:
                ages_ms = sorted(now_ms - int(row[0]) for row in rows)
                backlog_p95 = ages_ms[min(int(len(ages_ms) * 0.95), len(ages_ms) - 1)]

            conn.close()
        except Exception:
            pass

    return QueueHealthSnapshot(
        pending_tasks=pending_tasks,
        backlog_p95_ms=backlog_p95,
        write_rate_per_min=write_rate,
        error_rate_per_min=error_rate,
        dead_tasks_1h=dead_tasks,
    )


def collect_degrade_status(watch_only_flag_file: str) -> DegradeStatus:
    if not os.path.exists(watch_only_flag_file):
        return DegradeStatus(active=False)

    try:
        with open(watch_only_flag_file, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        return DegradeStatus(
            active=True,
            reason=data.get("reason"),
            since_ts_ms=data.get("since_ts_ms"),
            watch_only=bool(data.get("watch_only", True)),
        )
    except (json.JSONDecodeError, OSError, TypeError):
        return DegradeStatus(active=False)


def collect_system_health(
    state_store: Any,
    queue_path: str,
    watch_only_flag_file: str,
    db_path: str,
) -> SystemHealthSnapshot:
    del db_path
    now_ms = int(time.time() * 1000)

    return SystemHealthSnapshot(
        timestamp_ms=now_ms,
        ws_health=collect_ws_health(state_store),
        quote_health=collect_quote_health(state_store),
        queue_health=collect_queue_health(queue_path),
        degrade_status=collect_degrade_status(watch_only_flag_file),
        realtime_latency_p50_ms=0.0,
        realtime_latency_p95_ms=0.0,
        tickets_per_min=0.0,
    )


def collect_signer_health(con) -> SignerHealthSnapshot:
    if not _duckdb_table_exists(con, "meta.signature_audit_logs"):
        return SignerHealthSnapshot(0, 0, None, None)
    counts = con.execute(
        """
        SELECT
            COUNT(*) AS request_count,
            COUNT(*) FILTER (WHERE status = 'rejected') AS rejected_count
        FROM meta.signature_audit_logs
        """
    ).fetchone()
    latest = con.execute(
        """
        SELECT status, COALESCE(CAST(created_at AS TEXT), CAST(timestamp AS TEXT))
        FROM meta.signature_audit_logs
        ORDER BY COALESCE(created_at, timestamp) DESC, log_id DESC
        LIMIT 1
        """
    ).fetchone()
    return SignerHealthSnapshot(
        request_count=int(counts[0]) if counts else 0,
        rejected_count=int(counts[1]) if counts else 0,
        latest_status=str(latest[0]) if latest and latest[0] is not None else None,
        latest_created_at=str(latest[1]) if latest and latest[1] is not None else None,
    )


def collect_submitter_health(con) -> SubmitterHealthSnapshot:
    if not _duckdb_table_exists(con, "runtime.submit_attempts"):
        return SubmitterHealthSnapshot(0, 0, 0, 0, None)
    counts = con.execute(
        """
        SELECT
            COUNT(*) FILTER (
                WHERE attempt_kind = 'sign_order'
                  AND attempt_mode = 'sign_only'
                  AND status = 'signed'
            ) AS sign_only_signed_count,
            COUNT(*) FILTER (
                WHERE attempt_kind = 'submit_order'
                  AND attempt_mode = 'dry_run'
                  AND status = 'previewed'
            ) AS submit_preview_count,
            COUNT(*) FILTER (
                WHERE attempt_kind = 'submit_order'
                  AND attempt_mode = 'shadow_submit'
                  AND status = 'accepted'
            ) AS submit_accepted_count,
            COUNT(*) FILTER (
                WHERE attempt_kind = 'submit_order'
                  AND status = 'rejected'
            ) AS submit_rejected_count
        FROM runtime.submit_attempts
        """
    ).fetchone()
    latest = con.execute(
        """
        SELECT created_at
        FROM runtime.submit_attempts
        WHERE attempt_kind = 'submit_order'
        ORDER BY created_at DESC, attempt_id DESC
        LIMIT 1
        """
    ).fetchone()
    return SubmitterHealthSnapshot(
        sign_only_signed_count=int(counts[0]) if counts else 0,
        submit_preview_count=int(counts[1]) if counts else 0,
        submit_accepted_count=int(counts[2]) if counts else 0,
        submit_rejected_count=int(counts[3]) if counts else 0,
        latest_submit_created_at=str(latest[0]) if latest and latest[0] is not None else None,
    )


def collect_chain_tx_health(con) -> ChainTxHealthSnapshot:
    if not _duckdb_table_exists(con, "runtime.chain_tx_attempts"):
        return ChainTxHealthSnapshot(0, 0, None, None)
    counts = con.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE tx_kind = 'approve_usdc') AS approve_attempt_count,
            COUNT(*) FILTER (WHERE tx_kind = 'approve_usdc' AND status = 'rejected') AS approve_rejected_count
        FROM runtime.chain_tx_attempts
        """
    ).fetchone()
    latest = con.execute(
        """
        SELECT status, created_at
        FROM runtime.chain_tx_attempts
        WHERE tx_kind = 'approve_usdc'
        ORDER BY created_at DESC, attempt_id DESC
        LIMIT 1
        """
    ).fetchone()
    return ChainTxHealthSnapshot(
        approve_attempt_count=int(counts[0]) if counts else 0,
        approve_rejected_count=int(counts[1]) if counts else 0,
        latest_approve_status=str(latest[0]) if latest and latest[0] is not None else None,
        latest_approve_created_at=str(latest[1]) if latest and latest[1] is not None else None,
    )


def collect_external_execution_health(con) -> ExternalExecutionHealthSnapshot:
    order_count = _duckdb_table_count(con, "runtime.external_order_observations")
    fill_count = _duckdb_table_count(con, "runtime.external_fill_observations")
    if _duckdb_table_exists(con, "trading.reconciliation_results"):
        counts = con.execute(
            """
            SELECT
                COUNT(*) FILTER (
                    WHERE reconciliation_scope = 'external_execution' AND status = 'ok'
                ) AS ok_count,
                COUNT(*) FILTER (
                    WHERE reconciliation_scope = 'external_execution' AND status IN ('external_order_mismatch', 'external_fill_mismatch')
                ) AS mismatch_count,
                COUNT(*) FILTER (
                    WHERE reconciliation_scope = 'external_execution' AND status = 'external_state_unverified'
                ) AS unverified_count
            FROM trading.reconciliation_results
            """
        ).fetchone()
    else:
        counts = (0, 0, 0)
    latest = None
    if _duckdb_table_exists(con, "runtime.external_fill_observations"):
        latest = con.execute(
            """
            SELECT observed_at
            FROM runtime.external_fill_observations
            ORDER BY observed_at DESC, observation_id DESC
            LIMIT 1
            """
        ).fetchone()
    if latest is None and _duckdb_table_exists(con, "runtime.external_order_observations"):
        latest = con.execute(
            """
            SELECT observed_at
            FROM runtime.external_order_observations
            ORDER BY observed_at DESC, observation_id DESC
            LIMIT 1
            """
        ).fetchone()
    return ExternalExecutionHealthSnapshot(
        external_order_observation_count=order_count,
        external_fill_observation_count=fill_count,
        external_reconciliation_ok_count=int(counts[0]) if counts else 0,
        external_reconciliation_mismatch_count=int(counts[1]) if counts else 0,
        external_reconciliation_unverified_count=int(counts[2]) if counts else 0,
        latest_observed_at=str(latest[0]) if latest and latest[0] is not None else None,
    )


def collect_live_prereq_health(con, *, queue_path: str) -> LivePrereqHealthSnapshot:
    return LivePrereqHealthSnapshot(
        timestamp_ms=int(time.time() * 1000),
        queue_health=collect_queue_health(queue_path),
        signer_health=collect_signer_health(con),
        submitter_health=collect_submitter_health(con),
        chain_tx_health=collect_chain_tx_health(con),
        external_execution_health=collect_external_execution_health(con),
    )


def _duckdb_table_exists(con, table_name: str) -> bool:
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


def _duckdb_table_count(con, table_name: str) -> int:
    if not _duckdb_table_exists(con, table_name):
        return 0
    row = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return int(row[0]) if row is not None else 0
