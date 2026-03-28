from __future__ import annotations

import json
import math
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd

from asterion_core.contracts import ExecutionIntelligenceSummary, stable_object_id
from asterion_core.storage.utils import safe_json_dumps


RUNTIME_EXECUTION_INTELLIGENCE_RUN_COLUMNS = [
    "run_id",
    "job_name",
    "window_start",
    "window_end",
    "input_ticket_count",
    "summary_count",
    "materialized_at",
]

RUNTIME_EXECUTION_INTELLIGENCE_SUMMARY_COLUMNS = [
    "summary_id",
    "run_id",
    "market_id",
    "side",
    "quote_imbalance_score",
    "top_of_book_stability",
    "book_update_intensity",
    "spread_regime",
    "visible_size_shock_flag",
    "book_pressure_side",
    "expected_capture_regime",
    "expected_slippage_regime",
    "execution_intelligence_score",
    "reason_codes_json",
    "source_window_start",
    "source_window_end",
    "materialized_at",
]


def build_execution_intelligence_summary_from_context(source_context: dict[str, Any] | None) -> ExecutionIntelligenceSummary | None:
    if not isinstance(source_context, dict):
        return None
    summary_id = source_context.get("execution_intelligence_summary_id")
    run_id = source_context.get("execution_intelligence_run_id")
    market_id = source_context.get("execution_intelligence_market_id")
    side = source_context.get("execution_intelligence_side")
    if not all([summary_id, run_id, market_id, side]):
        return None
    return ExecutionIntelligenceSummary(
        summary_id=str(summary_id),
        run_id=str(run_id),
        market_id=str(market_id),
        side=str(side),
        quote_imbalance_score=float(source_context.get("execution_intelligence_quote_imbalance_score") or 0.0),
        top_of_book_stability=float(source_context.get("execution_intelligence_top_of_book_stability") or 0.0),
        book_update_intensity=float(source_context.get("execution_intelligence_book_update_intensity") or 0.0),
        spread_regime=str(source_context.get("execution_intelligence_spread_regime") or "unknown"),
        visible_size_shock_flag=bool(source_context.get("execution_intelligence_visible_size_shock_flag")),
        book_pressure_side=str(source_context.get("execution_intelligence_book_pressure_side") or "neutral"),
        expected_capture_regime=str(source_context.get("execution_intelligence_expected_capture_regime") or "low"),
        expected_slippage_regime=str(source_context.get("execution_intelligence_expected_slippage_regime") or "high"),
        execution_intelligence_score=float(source_context.get("execution_intelligence_score") or 0.0),
        reason_codes=_json_list(source_context.get("execution_intelligence_reason_codes")),
        source_window_start=source_context.get("execution_intelligence_source_window_start"),
        source_window_end=source_context.get("execution_intelligence_source_window_end"),
        materialized_at=source_context.get("execution_intelligence_materialized_at"),
    )


def execution_intelligence_context_fields(summary: ExecutionIntelligenceSummary | None) -> dict[str, Any]:
    if summary is None:
        return {}
    return {
        "execution_intelligence_summary_id": summary.summary_id,
        "execution_intelligence_run_id": summary.run_id,
        "execution_intelligence_market_id": summary.market_id,
        "execution_intelligence_side": summary.side,
        "execution_intelligence_quote_imbalance_score": summary.quote_imbalance_score,
        "execution_intelligence_top_of_book_stability": summary.top_of_book_stability,
        "execution_intelligence_book_update_intensity": summary.book_update_intensity,
        "execution_intelligence_spread_regime": summary.spread_regime,
        "execution_intelligence_visible_size_shock_flag": summary.visible_size_shock_flag,
        "execution_intelligence_book_pressure_side": summary.book_pressure_side,
        "execution_intelligence_expected_capture_regime": summary.expected_capture_regime,
        "execution_intelligence_expected_slippage_regime": summary.expected_slippage_regime,
        "execution_intelligence_score": summary.execution_intelligence_score,
        "execution_intelligence_reason_codes": list(summary.reason_codes),
        "execution_intelligence_source_window_start": summary.source_window_start,
        "execution_intelligence_source_window_end": summary.source_window_end,
        "execution_intelligence_materialized_at": summary.materialized_at,
    }


def load_execution_intelligence_summary(
    con,
    *,
    market_id: str,
    side: str | None = None,
) -> ExecutionIntelligenceSummary | None:
    if not _table_exists(con, "runtime.execution_intelligence_summaries"):
        return None
    params: list[Any] = [str(market_id)]
    where = ["market_id = ?"]
    if side:
        where.append("side = ?")
        params.append(str(side).upper())
    row = con.execute(
        f"""
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
        FROM runtime.execution_intelligence_summaries
        WHERE {" AND ".join(where)}
        ORDER BY materialized_at DESC, execution_intelligence_score DESC, summary_id DESC
        LIMIT 1
        """,
        params,
    ).fetchone()
    if row is None:
        return None
    return ExecutionIntelligenceSummary(
        summary_id=str(row[0]),
        run_id=str(row[1]),
        market_id=str(row[2]),
        side=str(row[3]),
        quote_imbalance_score=float(row[4] or 0.0),
        top_of_book_stability=float(row[5] or 0.0),
        book_update_intensity=float(row[6] or 0.0),
        spread_regime=str(row[7] or "unknown"),
        visible_size_shock_flag=bool(row[8]),
        book_pressure_side=str(row[9] or "neutral"),
        expected_capture_regime=str(row[10] or "low"),
        expected_slippage_regime=str(row[11] or "high"),
        execution_intelligence_score=float(row[12] or 0.0),
        reason_codes=_json_list(row[13]),
        source_window_start=row[14],
        source_window_end=row[15],
        materialized_at=row[16],
    )


def materialize_execution_intelligence(
    con,
    *,
    as_of: datetime | None = None,
    lookback_days: int = 30,
    job_name: str = "execution_intelligence_v1",
    run_id: str | None = None,
) -> tuple[dict[str, Any], list[ExecutionIntelligenceSummary]]:
    materialized_at = _normalize_datetime(as_of) if as_of is not None else datetime.now(UTC).replace(tzinfo=None, microsecond=0)
    window_end = materialized_at
    window_start = (materialized_at - timedelta(days=max(1, int(lookback_days)))).replace(tzinfo=None)
    effective_run_id = run_id or stable_object_id(
        "eirun",
        {
            "job_name": job_name,
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
        },
    )
    if not _table_exists(con, "runtime.trade_tickets"):
        run = {
            "run_id": effective_run_id,
            "job_name": job_name,
            "window_start": window_start,
            "window_end": window_end,
            "input_ticket_count": 0,
            "summary_count": 0,
            "materialized_at": materialized_at,
        }
        return run, []

    tickets = con.execute(
        """
        SELECT
            ticket_id,
            market_id,
            strategy_id,
            wallet_id,
            side,
            reference_price,
            size,
            provenance_json,
            created_at
        FROM runtime.trade_tickets
        WHERE created_at >= ?
        """,
        [window_start],
    ).fetchdf()
    if tickets.empty:
        run = {
            "run_id": effective_run_id,
            "job_name": job_name,
            "window_start": window_start,
            "window_end": window_end,
            "input_ticket_count": 0,
            "summary_count": 0,
            "materialized_at": materialized_at,
        }
        return run, []

    submit_attempts = _optional_df(
        con,
        """
        SELECT ticket_id, order_id, status, created_at
        FROM runtime.submit_attempts
        WHERE attempt_kind = 'submit_order'
        """,
    )
    external_orders = _optional_df(
        con,
        """
        SELECT ticket_id, order_id, external_status AS status, observed_at
        FROM runtime.external_order_observations
        """,
    )
    fills = _optional_df(
        con,
        """
        SELECT
            ord.market_id,
            ord.side,
            ord.order_id,
            ord.size AS order_size,
            ord.price AS order_price,
            SUM(fill.size) AS filled_size,
            SUM(fill.price * fill.size) / NULLIF(SUM(fill.size), 0) AS avg_fill_price,
            MIN(fill.filled_at) AS first_fill_at
        FROM trading.orders ord
        LEFT JOIN trading.fills fill ON fill.order_id = ord.order_id
        GROUP BY ord.market_id, ord.side, ord.order_id, ord.size, ord.price
        """,
    )
    retrospective = _optional_df(
        con,
        """
        SELECT market_id, side, AVG(fill_capture_ratio) AS avg_fill_capture_ratio
        FROM runtime.ranking_retrospective_rows
        GROUP BY market_id, side
        """,
    )

    latest_submit_by_ticket = _latest_by_key(submit_attempts, key_col="ticket_id", ts_col="created_at")
    latest_external_order_by_ticket = _latest_by_key(external_orders, key_col="ticket_id", ts_col="observed_at")
    fills_by_order = {
        str(row["order_id"]): row.to_dict()
        for _, row in fills.iterrows()
        if row.get("order_id") not in {None, ""}
    }
    retrospective_by_market_side = {
        (str(row["market_id"]), str(row["side"]).upper()): float(row["avg_fill_capture_ratio"] or 0.0)
        for _, row in retrospective.iterrows()
        if row.get("market_id") not in {None, ""} and row.get("side") not in {None, ""}
    }

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for _, ticket in tickets.iterrows():
        market_id = str(ticket["market_id"])
        side = str(ticket["side"] or "").upper()
        provenance = _json_dict(ticket.get("provenance_json"))
        pricing_context = _json_dict(provenance.get("pricing_context"))
        submit = latest_submit_by_ticket.get(str(ticket["ticket_id"])) or {}
        external = latest_external_order_by_ticket.get(str(ticket["ticket_id"])) or {}
        order_id = _coerce_text(submit.get("order_id")) or _coerce_text(external.get("order_id"))
        fill_row = fills_by_order.get(order_id) or {}
        order_size = _coerce_float(fill_row.get("order_size")) or _coerce_float(ticket.get("size")) or 0.0
        filled_size = _coerce_float(fill_row.get("filled_size")) or 0.0
        fill_ratio = min(max(filled_size / order_size, 0.0), 1.0) if order_size > 0.0 else 0.0
        avg_fill_price = _coerce_float(fill_row.get("avg_fill_price"))
        order_price = _coerce_float(fill_row.get("order_price")) or _coerce_float(ticket.get("reference_price"))
        slippage_bps = 0.0
        if order_price is not None and avg_fill_price is not None and order_price > 0.0:
            slippage_bps = abs(avg_fill_price - order_price) * 10_000.0
        submit_ok = str(submit.get("status") or "").lower() in {"accepted", "working", "posted", "partial_filled", "filled", "cancelled"}
        fill_ok = filled_size > 0.0
        external_status = str(external.get("status") or "").lower()
        grouped.setdefault((market_id, side), []).append(
            {
                "depth_proxy": _coerce_float(pricing_context.get("depth_proxy")) or 0.0,
                "spread_bps": _coerce_float(pricing_context.get("spread_bps")) or 0.0,
                "submit_ok": 1.0 if submit_ok else 0.0,
                "fill_ok": 1.0 if fill_ok else 0.0,
                "partial_fill": 1.0 if 0.0 < fill_ratio < 1.0 else 0.0,
                "fill_ratio": fill_ratio,
                "slippage_bps": slippage_bps,
                "source_freshness_status": str(pricing_context.get("source_freshness_status") or "missing"),
                "external_status": external_status,
            }
        )

    summaries: list[ExecutionIntelligenceSummary] = []
    for (market_id, side), rows in grouped.items():
        sample_count = len(rows)
        avg_depth = _mean([item["depth_proxy"] for item in rows])
        avg_spread = _mean([item["spread_bps"] for item in rows])
        submit_rate = _mean([item["submit_ok"] for item in rows])
        fill_rate = _mean([item["fill_ok"] for item in rows])
        partial_fill_rate = _mean([item["partial_fill"] for item in rows])
        avg_fill_ratio = _mean([item["fill_ratio"] for item in rows])
        avg_slippage_bps = _mean([item["slippage_bps"] for item in rows])
        freshness_degraded_rate = _mean([1.0 if item["source_freshness_status"] != "fresh" else 0.0 for item in rows])
        retrospective_capture = retrospective_by_market_side.get((market_id, side), fill_rate)

        quote_imbalance_score = round(_clamp(((fill_rate - 0.5) * 1.4) + ((avg_depth - 0.5) * 0.6), -1.0, 1.0), 6)
        top_of_book_stability = round(
            _clamp(
                1.0
                - min(avg_spread / 150.0, 1.0) * 0.45
                - partial_fill_rate * 0.25
                - freshness_degraded_rate * 0.20
                - max(0.0, 0.6 - avg_depth) * 0.10,
                0.0,
                1.0,
            ),
            6,
        )
        book_update_intensity = round(_clamp((sample_count / 12.0) + (partial_fill_rate * 0.35), 0.0, 1.0), 6)
        if avg_spread <= 50.0:
            spread_regime = "tight"
        elif avg_spread <= 120.0:
            spread_regime = "normal"
        else:
            spread_regime = "wide"
        visible_size_shock_flag = partial_fill_rate >= 0.35 or avg_fill_ratio <= 0.60
        book_pressure_side = side if abs(quote_imbalance_score) >= 0.15 else "neutral"
        capture_signal = max(fill_rate, retrospective_capture)
        if capture_signal >= 0.75:
            expected_capture_regime = "high"
        elif capture_signal >= 0.45:
            expected_capture_regime = "medium"
        else:
            expected_capture_regime = "low"
        if avg_slippage_bps <= 25.0:
            expected_slippage_regime = "low"
        elif avg_slippage_bps <= 60.0:
            expected_slippage_regime = "medium"
        else:
            expected_slippage_regime = "high"
        execution_intelligence_score = round(
            _clamp(
                (submit_rate * 0.20)
                + (fill_rate * 0.25)
                + (retrospective_capture * 0.20)
                + (top_of_book_stability * 0.20)
                + ((1.0 - min(avg_slippage_bps / 100.0, 1.0)) * 0.15),
                0.0,
                1.0,
            ),
            6,
        )
        reason_codes: list[str] = []
        if spread_regime == "wide":
            reason_codes.append("spread_regime:wide")
        if visible_size_shock_flag:
            reason_codes.append("visible_size_shock")
        if expected_capture_regime == "low":
            reason_codes.append("capture_regime:low")
        if expected_slippage_regime == "high":
            reason_codes.append("slippage_regime:high")
        if top_of_book_stability < 0.50:
            reason_codes.append("book_stability:low")
        if not reason_codes:
            reason_codes.append("microstructure_balanced")
        summaries.append(
            ExecutionIntelligenceSummary(
                summary_id=stable_object_id(
                    "eisum",
                    {
                        "run_id": effective_run_id,
                        "market_id": market_id,
                        "side": side,
                    },
                ),
                run_id=effective_run_id,
                market_id=market_id,
                side=side,
                quote_imbalance_score=quote_imbalance_score,
                top_of_book_stability=top_of_book_stability,
                book_update_intensity=book_update_intensity,
                spread_regime=spread_regime,
                visible_size_shock_flag=visible_size_shock_flag,
                book_pressure_side=book_pressure_side,
                expected_capture_regime=expected_capture_regime,
                expected_slippage_regime=expected_slippage_regime,
                execution_intelligence_score=execution_intelligence_score,
                reason_codes=reason_codes,
                source_window_start=window_start,
                source_window_end=window_end,
                materialized_at=materialized_at,
            )
        )
    run = {
        "run_id": effective_run_id,
        "job_name": job_name,
        "window_start": window_start,
        "window_end": window_end,
        "input_ticket_count": int(len(tickets.index)),
        "summary_count": len(summaries),
        "materialized_at": materialized_at,
    }
    return run, summaries


def persist_execution_intelligence_materialization(
    con,
    *,
    as_of: datetime | None = None,
    lookback_days: int = 30,
    job_name: str = "execution_intelligence_v1",
    run_id: str | None = None,
) -> tuple[dict[str, Any], list[ExecutionIntelligenceSummary]]:
    if getattr(con, "_guard_mode", None) == "reader":
        materialized_at = _normalize_datetime(as_of) if as_of is not None else datetime.now(UTC).replace(tzinfo=None, microsecond=0)
        effective_run_id = run_id or stable_object_id(
            "eirun",
            {"job_name": job_name, "materialized_at": materialized_at.isoformat()},
        )
        return (
            {
                "run_id": effective_run_id,
                "job_name": job_name,
                "window_start": materialized_at,
                "window_end": materialized_at,
                "input_ticket_count": 0,
                "summary_count": 0,
                "materialized_at": materialized_at,
            },
            [],
        )
    if not _table_exists(con, "runtime.execution_intelligence_runs") or not _table_exists(con, "runtime.execution_intelligence_summaries"):
        return materialize_execution_intelligence(
            con,
            as_of=as_of,
            lookback_days=lookback_days,
            job_name=job_name,
            run_id=run_id,
        )

    run, summaries = materialize_execution_intelligence(
        con,
        as_of=as_of,
        lookback_days=lookback_days,
        job_name=job_name,
        run_id=run_id,
    )
    con.execute(
        """
        INSERT OR REPLACE INTO runtime.execution_intelligence_runs
        (run_id, job_name, window_start, window_end, input_ticket_count, summary_count, materialized_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run["run_id"],
            run["job_name"],
            _sql_timestamp(run["window_start"]),
            _sql_timestamp(run["window_end"]),
            int(run["input_ticket_count"]),
            int(run["summary_count"]),
            _sql_timestamp(run["materialized_at"]),
        ],
    )
    if summaries:
        rows = [execution_intelligence_summary_to_row(item) for item in summaries]
        placeholders = ", ".join(["?"] * len(RUNTIME_EXECUTION_INTELLIGENCE_SUMMARY_COLUMNS))
        con.executemany(
            f"""
            INSERT OR REPLACE INTO runtime.execution_intelligence_summaries
            ({", ".join(RUNTIME_EXECUTION_INTELLIGENCE_SUMMARY_COLUMNS)})
            VALUES ({placeholders})
            """,
            rows,
        )
    return run, summaries


def execution_intelligence_summary_to_row(record: ExecutionIntelligenceSummary) -> list[Any]:
    return [
        record.summary_id,
        record.run_id,
        record.market_id,
        record.side,
        record.quote_imbalance_score,
        record.top_of_book_stability,
        record.book_update_intensity,
        record.spread_regime,
        record.visible_size_shock_flag,
        record.book_pressure_side,
        record.expected_capture_regime,
        record.expected_slippage_regime,
        record.execution_intelligence_score,
        safe_json_dumps(record.reason_codes),
        _sql_timestamp(record.source_window_start),
        _sql_timestamp(record.source_window_end),
        _sql_timestamp(record.materialized_at),
    ]


def _latest_by_key(frame: pd.DataFrame, *, key_col: str, ts_col: str) -> dict[str, dict[str, Any]]:
    if frame.empty or key_col not in frame.columns:
        return {}
    working = frame.copy()
    working[ts_col] = pd.to_datetime(working[ts_col], errors="coerce")
    working = working.sort_values(by=[ts_col], ascending=[False], na_position="last")
    return {
        str(row[key_col]): row.to_dict()
        for _, row in working.iterrows()
        if row.get(key_col) not in {None, ""}
    }


def _optional_df(con, query: str) -> pd.DataFrame:
    try:
        return con.execute(query).fetchdf()
    except Exception:  # noqa: BLE001
        return pd.DataFrame()


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


def _coerce_float(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        coerced = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(coerced):
        return None
    return coerced


def _coerce_text(value: Any) -> str | None:
    if value in {None, ""}:
        return None
    text = str(value).strip()
    return text or None


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    finite_values = [float(value) for value in values if math.isfinite(float(value))]
    if not finite_values:
        return 0.0
    return float(sum(finite_values) / len(finite_values))


def _clamp(value: float, lower: float, upper: float) -> float:
    if not math.isfinite(value):
        return lower
    return max(lower, min(upper, value))


def _normalize_datetime(value: datetime) -> datetime:
    return value.astimezone(UTC).replace(tzinfo=None, microsecond=0)


def _sql_timestamp(value: Any) -> Any:
    if isinstance(value, datetime):
        normalized = value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return normalized.astimezone(UTC).replace(tzinfo=None)
    return value


def _table_exists(con, table_name: str) -> bool:
    schema_name, _, short_name = table_name.partition(".")
    row = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        LIMIT 1
        """,
        [schema_name, short_name],
    ).fetchone()
    return row is not None


__all__ = [
    "RUNTIME_EXECUTION_INTELLIGENCE_RUN_COLUMNS",
    "RUNTIME_EXECUTION_INTELLIGENCE_SUMMARY_COLUMNS",
    "build_execution_intelligence_summary_from_context",
    "execution_intelligence_context_fields",
    "execution_intelligence_summary_to_row",
    "load_execution_intelligence_summary",
    "materialize_execution_intelligence",
    "persist_execution_intelligence_materialization",
]
