from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd

from asterion_core.contracts import (
    RankingRetrospectiveRow,
    RankingRetrospectiveRun,
    RankingRetrospectiveSummary,
    stable_object_id,
)
from asterion_core.storage.os_queue import enqueue_upsert_rows_v1
from asterion_core.storage.utils import safe_json_dumps
from asterion_core.storage.write_queue import WriteQueueConfig
from .resolved_execution_projection import build_resolved_execution_projection


RANKING_RETROSPECTIVE_RUN_COLUMNS = [
    "run_id",
    "baseline_version",
    "window_start",
    "window_end",
    "snapshot_count",
    "row_count",
    "summary_json",
    "created_at",
]

RANKING_RETROSPECTIVE_ROW_COLUMNS = [
    "row_id",
    "run_id",
    "market_id",
    "strategy_id",
    "side",
    "ranking_decile",
    "top_k_bucket",
    "evaluation_status",
    "submitted_capture_ratio",
    "fill_capture_ratio",
    "resolution_capture_ratio",
    "avg_ranking_score",
    "avg_edge_bps_executable",
    "avg_realized_pnl",
    "avg_predicted_vs_realized_gap",
    "forecast_replay_change_rate",
    "top_rank_share_of_realized_pnl",
    "window_start",
    "window_end",
    "created_at",
]


def compare_retrospective_uplift(
    *,
    baseline_summary: RankingRetrospectiveSummary,
    candidate_summary: RankingRetrospectiveSummary,
    baseline_rows: list[RankingRetrospectiveRow],
    candidate_rows: list[RankingRetrospectiveRow],
) -> dict[str, Any]:
    baseline_top_rows = [row for row in baseline_rows if int(row.ranking_decile) == 1]
    candidate_top_rows = [row for row in candidate_rows if int(row.ranking_decile) == 1]
    return {
        "baseline_version": baseline_summary.baseline_version,
        "candidate_version": candidate_summary.baseline_version,
        "baseline_snapshot_count": int(baseline_summary.snapshot_count),
        "candidate_snapshot_count": int(candidate_summary.snapshot_count),
        "top_decile_fill_capture_uplift": round(
            float(candidate_summary.top_decile_fill_capture_ratio) - float(baseline_summary.top_decile_fill_capture_ratio),
            6,
        ),
        "top_decile_resolution_capture_uplift": round(
            float(candidate_summary.top_decile_resolution_capture_ratio)
            - float(baseline_summary.top_decile_resolution_capture_ratio),
            6,
        ),
        "top_decile_realized_pnl_uplift": _optional_delta(
            candidate_summary.top_decile_realized_pnl,
            baseline_summary.top_decile_realized_pnl,
        ),
        "top_decile_realized_share_uplift": round(
            float(candidate_summary.top_decile_realized_pnl_share) - float(baseline_summary.top_decile_realized_pnl_share),
            6,
        ),
        "top_row_fill_capture_uplift": _optional_delta(
            _mean_rows(candidate_top_rows, "fill_capture_ratio"),
            _mean_rows(baseline_top_rows, "fill_capture_ratio"),
        ),
        "top_row_resolution_capture_uplift": _optional_delta(
            _mean_rows(candidate_top_rows, "resolution_capture_ratio"),
            _mean_rows(baseline_top_rows, "resolution_capture_ratio"),
        ),
        "top_row_realized_pnl_uplift": _optional_delta(
            _mean_rows(candidate_top_rows, "avg_realized_pnl"),
            _mean_rows(baseline_top_rows, "avg_realized_pnl"),
        ),
        "candidate_outperformed": any(
            value > 0.0
            for value in [
                float(candidate_summary.top_decile_fill_capture_ratio) - float(baseline_summary.top_decile_fill_capture_ratio),
                float(candidate_summary.top_decile_resolution_capture_ratio)
                - float(baseline_summary.top_decile_resolution_capture_ratio),
                float(candidate_summary.top_decile_realized_pnl_share) - float(baseline_summary.top_decile_realized_pnl_share),
            ]
        ),
    }


def materialize_ranking_retrospective(
    con,
    *,
    as_of: datetime | None = None,
    lookback_days: int = 30,
    baseline_version: str = "ranking_retro_v1",
    run_id: str | None = None,
) -> tuple[RankingRetrospectiveRun, list[RankingRetrospectiveRow], RankingRetrospectiveSummary]:
    created_at = _normalize_datetime(as_of) if as_of is not None else datetime.now(UTC).replace(tzinfo=None, microsecond=0)
    window_end = created_at
    window_start = (created_at - timedelta(days=max(1, int(lookback_days)))).replace(tzinfo=None)
    effective_run_id = run_id or stable_object_id(
        "rrun",
        {
            "baseline_version": baseline_version,
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
        },
    )

    snapshot_frame = _load_snapshot_frame(con, window_start=window_start)
    if snapshot_frame.empty:
        summary = RankingRetrospectiveSummary(
            baseline_version=baseline_version,
            snapshot_count=0,
            row_count=0,
            top_decile_submitted_capture_ratio=0.0,
            top_decile_fill_capture_ratio=0.0,
            top_decile_resolution_capture_ratio=0.0,
            top_decile_realized_pnl=None,
            top_decile_realized_pnl_share=0.0,
        )
        run = RankingRetrospectiveRun(
            run_id=effective_run_id,
            baseline_version=baseline_version,
            window_start=window_start,
            window_end=window_end,
            snapshot_count=0,
            row_count=0,
            summary_json=asdict(summary),
            created_at=created_at,
        )
        return run, [], summary

    ticket_frame = _load_ticket_frame(con, window_start=window_start)
    submit_frame = _load_submit_frame(con)
    fill_frame = _load_fill_frame(con)
    resolution_by_market = _load_resolution_by_market(con)
    replay_change_ids = _load_replay_change_snapshot_ids(con, window_start=window_start)

    latest_submit_by_ticket = _latest_by_key(submit_frame, key_col="ticket_id", ts_col="created_at")
    ticket_by_snapshot = _latest_by_key(ticket_frame, key_col="watch_snapshot_id", ts_col="created_at")
    fill_by_order = {
        str(row["order_id"]): row
        for _, row in fill_frame.iterrows()
        if row.get("order_id") not in {None, ""}
    }

    snapshot_rows: list[dict[str, Any]] = []
    for _, row in snapshot_frame.iterrows():
        snapshot_id = str(row["snapshot_id"])
        pricing_context = _json_dict(row.get("pricing_context_json"))
        ranking_score = float(pricing_context.get("ranking_score") or 0.0)
        edge_bps_executable = float(pricing_context.get("edge_bps_executable") or row.get("edge_bps") or 0.0)
        ticket = ticket_by_snapshot.get(snapshot_id)
        submitted = ticket is not None
        strategy_id = str(ticket.get("strategy_id")) if ticket is not None and ticket.get("strategy_id") else "watch_only"
        ticket_side = str(ticket.get("side")).upper() if ticket is not None and ticket.get("side") else str(row.get("side") or "").upper()
        order_id = None
        filled = False
        realized_pnl = None
        evaluation_status = "watch_only"
        if ticket is not None:
            submit = latest_submit_by_ticket.get(str(ticket["ticket_id"]))
            order_id = _coerce_optional_text(submit.get("order_id") if submit is not None else None)
            fill = fill_by_order.get(order_id or "")
            projection = build_resolved_execution_projection(
                outcome=row.get("outcome"),
                side=ticket_side,
                expected_outcome=resolution_by_market.get(str(row["market_id"])),
                filled_quantity=float(fill.get("filled_size") or 0.0) if fill is not None else 0.0,
                ticket_size=float(ticket.get("size") or 0.0),
                expected_fill_price=_coerce_optional_float(ticket.get("reference_price")),
                realized_fill_price=_coerce_optional_float(fill.get("avg_fill_price")) if fill is not None else None,
                total_fee=(_coerce_optional_float(fill.get("total_fee")) or 0.0) if fill is not None else 0.0,
                predicted_edge_bps=_coerce_optional_float(pricing_context.get("edge_bps_executable") or row.get("edge_bps")),
                execution_result=None,
                order_status=None,
                latest_submit_status=submit.get("status") if submit is not None else None,
                live_prereq_execution_status=None,
                external_order_status=None,
                gate_allowed=None,
                latest_sign_attempt_id=None,
                latest_submit_attempt_id=submit.get("attempt_id") if submit is not None else None,
                latest_fill_at=fill.get("first_fill_at") if fill is not None else None,
                latest_resolution_at=None,
            )
            filled = bool(fill is not None and float(fill.get("filled_size") or 0.0) > 0.0)
            realized_pnl = projection.realized_pnl
            evaluation_status = projection.evaluation_status if filled else ("submitted_only" if submitted else "watch_only")
        resolved = evaluation_status == "resolved"
        expected_dollar_pnl = _coerce_optional_float(pricing_context.get("expected_dollar_pnl"))
        if expected_dollar_pnl is None:
            expected_dollar_pnl = abs(edge_bps_executable) / 10_000.0
        snapshot_rows.append(
            {
                "snapshot_id": snapshot_id,
                "market_id": str(row["market_id"]),
                "strategy_id": strategy_id,
                "side": ticket_side or str(row.get("side") or "").upper() or "UNKNOWN",
                "ranking_score": ranking_score,
                "edge_bps_executable": edge_bps_executable,
                "submitted": 1.0 if submitted else 0.0,
                "filled": 1.0 if filled else 0.0,
                "resolved": 1.0 if resolved else 0.0,
                "realized_pnl": realized_pnl,
                "predicted_vs_realized_gap": None if realized_pnl is None else abs(float(expected_dollar_pnl) - float(realized_pnl)),
                "forecast_replay_changed": 1.0 if snapshot_id in replay_change_ids else 0.0,
                "evaluation_status": evaluation_status,
            }
        )

    detail_frame = pd.DataFrame(snapshot_rows)
    if detail_frame.empty:
        summary = RankingRetrospectiveSummary(
            baseline_version=baseline_version,
            snapshot_count=0,
            row_count=0,
            top_decile_submitted_capture_ratio=0.0,
            top_decile_fill_capture_ratio=0.0,
            top_decile_resolution_capture_ratio=0.0,
            top_decile_realized_pnl=None,
            top_decile_realized_pnl_share=0.0,
        )
        run = RankingRetrospectiveRun(
            run_id=effective_run_id,
            baseline_version=baseline_version,
            window_start=window_start,
            window_end=window_end,
            snapshot_count=0,
            row_count=0,
            summary_json=asdict(summary),
            created_at=created_at,
        )
        return run, [], summary

    detail_frame = _assign_rank_buckets(detail_frame)
    detail_frame["positive_realized_pnl"] = detail_frame["realized_pnl"].fillna(0.0).clip(lower=0.0)
    detail_frame["base_group_key"] = (
        detail_frame["market_id"].astype(str)
        + "|"
        + detail_frame["strategy_id"].astype(str)
        + "|"
        + detail_frame["side"].astype(str)
    )
    base_totals = detail_frame.groupby("base_group_key", dropna=False)["positive_realized_pnl"].sum().to_dict()

    rows: list[RankingRetrospectiveRow] = []
    grouped = detail_frame.groupby(
        ["market_id", "strategy_id", "side", "ranking_decile", "top_k_bucket", "evaluation_status"],
        dropna=False,
    )
    for (market_id, strategy_id, side, ranking_decile, top_k_bucket, evaluation_status), frame in grouped:
        base_group_key = f"{market_id}|{strategy_id}|{side}"
        group_realized = float(frame["positive_realized_pnl"].sum())
        total_realized = float(base_totals.get(base_group_key, 0.0))
        row_id = stable_object_id(
            "rretro",
            {
                "run_id": effective_run_id,
                "market_id": market_id,
                "strategy_id": strategy_id,
                "side": side,
                "ranking_decile": int(ranking_decile),
                "top_k_bucket": str(top_k_bucket),
                "evaluation_status": str(evaluation_status),
            },
        )
        rows.append(
            RankingRetrospectiveRow(
                row_id=row_id,
                run_id=effective_run_id,
                market_id=str(market_id),
                strategy_id=str(strategy_id),
                side=str(side),
                ranking_decile=int(ranking_decile),
                top_k_bucket=str(top_k_bucket),
                evaluation_status=str(evaluation_status),
                submitted_capture_ratio=round(float(frame["submitted"].mean()), 6),
                fill_capture_ratio=round(float(frame["filled"].mean()), 6),
                resolution_capture_ratio=round(float(frame["resolved"].mean()), 6),
                avg_ranking_score=round(float(frame["ranking_score"].mean()), 6),
                avg_edge_bps_executable=round(float(frame["edge_bps_executable"].mean()), 6),
                avg_realized_pnl=_mean_optional(frame["realized_pnl"]),
                avg_predicted_vs_realized_gap=_mean_optional(frame["predicted_vs_realized_gap"]),
                forecast_replay_change_rate=round(float(frame["forecast_replay_changed"].mean()), 6),
                top_rank_share_of_realized_pnl=round(group_realized / total_realized, 6) if total_realized > 0.0 else 0.0,
                window_start=window_start,
                window_end=window_end,
                created_at=created_at,
            )
        )

    top_decile = detail_frame[detail_frame["ranking_decile"] == 1]
    top_decile_positive = float(top_decile["positive_realized_pnl"].sum())
    all_positive = float(detail_frame["positive_realized_pnl"].sum())
    summary = RankingRetrospectiveSummary(
        baseline_version=baseline_version,
        snapshot_count=int(len(detail_frame.index)),
        row_count=len(rows),
        top_decile_submitted_capture_ratio=round(float(top_decile["submitted"].mean()) if not top_decile.empty else 0.0, 6),
        top_decile_fill_capture_ratio=round(float(top_decile["filled"].mean()) if not top_decile.empty else 0.0, 6),
        top_decile_resolution_capture_ratio=round(float(top_decile["resolved"].mean()) if not top_decile.empty else 0.0, 6),
        top_decile_realized_pnl=_mean_optional(top_decile["realized_pnl"]) if not top_decile.empty else None,
        top_decile_realized_pnl_share=round(top_decile_positive / all_positive, 6) if all_positive > 0.0 else 0.0,
    )
    run = RankingRetrospectiveRun(
        run_id=effective_run_id,
        baseline_version=baseline_version,
        window_start=window_start,
        window_end=window_end,
        snapshot_count=int(len(detail_frame.index)),
        row_count=len(rows),
        summary_json=asdict(summary),
        created_at=created_at,
    )
    return run, rows, summary


def enqueue_ranking_retrospective_run_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    runs: list[RankingRetrospectiveRun],
    run_id: str | None = None,
) -> str | None:
    if not runs:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="runtime.ranking_retrospective_runs",
        pk_cols=["run_id"],
        columns=list(RANKING_RETROSPECTIVE_RUN_COLUMNS),
        rows=[ranking_retrospective_run_to_row(item) for item in runs],
        run_id=run_id,
    )


def enqueue_ranking_retrospective_row_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    rows: list[RankingRetrospectiveRow],
    run_id: str | None = None,
) -> str | None:
    if not rows:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="runtime.ranking_retrospective_rows",
        pk_cols=["row_id"],
        columns=list(RANKING_RETROSPECTIVE_ROW_COLUMNS),
        rows=[ranking_retrospective_row_to_row(item) for item in rows],
        run_id=run_id,
    )


def ranking_retrospective_run_to_row(record: RankingRetrospectiveRun) -> list[Any]:
    return [
        record.run_id,
        record.baseline_version,
        _sql_timestamp(record.window_start),
        _sql_timestamp(record.window_end),
        record.snapshot_count,
        record.row_count,
        safe_json_dumps(record.summary_json),
        _sql_timestamp(record.created_at),
    ]


def ranking_retrospective_row_to_row(record: RankingRetrospectiveRow) -> list[Any]:
    return [
        record.row_id,
        record.run_id,
        record.market_id,
        record.strategy_id,
        record.side,
        record.ranking_decile,
        record.top_k_bucket,
        record.evaluation_status,
        record.submitted_capture_ratio,
        record.fill_capture_ratio,
        record.resolution_capture_ratio,
        record.avg_ranking_score,
        record.avg_edge_bps_executable,
        record.avg_realized_pnl,
        record.avg_predicted_vs_realized_gap,
        record.forecast_replay_change_rate,
        record.top_rank_share_of_realized_pnl,
        _sql_timestamp(record.window_start),
        _sql_timestamp(record.window_end),
        _sql_timestamp(record.created_at),
    ]


def _load_snapshot_frame(con, *, window_start: datetime) -> pd.DataFrame:
    if not _table_exists(con, "weather.weather_watch_only_snapshots"):
        return pd.DataFrame()
    return con.execute(
        """
        SELECT snapshot_id, market_id, outcome, side, edge_bps, pricing_context_json, created_at
        FROM weather.weather_watch_only_snapshots
        WHERE created_at >= ?
        """,
        [window_start],
    ).fetchdf()


def _load_ticket_frame(con, *, window_start: datetime) -> pd.DataFrame:
    if not _table_exists(con, "runtime.trade_tickets"):
        return pd.DataFrame()
    return con.execute(
        """
        SELECT ticket_id, watch_snapshot_id, strategy_id, side, outcome, reference_price, size, created_at
        FROM runtime.trade_tickets
        WHERE created_at >= ?
        """,
        [window_start],
    ).fetchdf()


def _load_submit_frame(con) -> pd.DataFrame:
    if not _table_exists(con, "runtime.submit_attempts"):
        return pd.DataFrame()
    return con.execute(
        """
        SELECT ticket_id, order_id, attempt_id, status, created_at
        FROM runtime.submit_attempts
        WHERE attempt_kind = 'submit_order'
        """
    ).fetchdf()


def _load_fill_frame(con) -> pd.DataFrame:
    if not _table_exists(con, "trading.fills"):
        return pd.DataFrame()
    return con.execute(
        """
        SELECT
            order_id,
            SUM(size) AS filled_size,
            SUM(price * size) / NULLIF(SUM(size), 0) AS avg_fill_price,
            MIN(filled_at) AS first_fill_at,
            SUM(fee) AS total_fee
        FROM trading.fills
        GROUP BY order_id
        """
    ).fetchdf()


def _load_resolution_by_market(con) -> dict[str, str]:
    if not _table_exists(con, "resolution.settlement_verifications"):
        return {}
    rows = con.execute(
        """
        SELECT market_id, expected_outcome, created_at, verification_id
        FROM resolution.settlement_verifications
        QUALIFY ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY created_at DESC, verification_id DESC) = 1
        """
    ).fetchdf()
    return {
        str(row["market_id"]): str(row["expected_outcome"])
        for _, row in rows.iterrows()
        if row["market_id"] is not None and row["expected_outcome"] is not None
    }


def _load_replay_change_snapshot_ids(con, *, window_start: datetime) -> set[str]:
    if not _table_exists(con, "weather.weather_forecast_replay_diffs"):
        return set()
    rows = con.execute(
        """
        SELECT original_entity_id, replayed_entity_id, status
        FROM weather.weather_forecast_replay_diffs
        WHERE entity_type = 'watch_only_snapshot'
          AND created_at >= ?
          AND status <> 'MATCH'
        """,
        [window_start],
    ).fetchdf()
    changed: set[str] = set()
    for _, row in rows.iterrows():
        for key in ("original_entity_id", "replayed_entity_id"):
            value = _coerce_optional_text(row.get(key))
            if value is not None:
                changed.add(value)
    return changed


def _assign_rank_buckets(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["_rank_index"] = 0
    out["ranking_decile"] = 10
    out["top_k_bucket"] = "other"
    # Rank opportunities against the same market/side cohort so executed and watch-only
    # snapshots compete on the same ordering baseline.
    grouped = out.groupby(["market_id", "side"], dropna=False)
    ranked_frames: list[pd.DataFrame] = []
    for _, group in grouped:
        ordered = group.sort_values(["ranking_score", "snapshot_id"], ascending=[False, True]).copy()
        count = max(len(ordered.index), 1)
        ordered["_rank_index"] = list(range(count))
        ordered["ranking_decile"] = ordered["_rank_index"].apply(lambda item: min(10, int((item * 10) / count) + 1))
        ordered["top_k_bucket"] = ordered["_rank_index"].apply(_top_k_bucket)
        ranked_frames.append(ordered)
    return pd.concat(ranked_frames, ignore_index=True) if ranked_frames else out


def _top_k_bucket(index: int) -> str:
    if index == 0:
        return "top_1"
    if index < 3:
        return "top_3"
    if index < 5:
        return "top_5"
    return "other"


def _latest_by_key(frame: pd.DataFrame, *, key_col: str, ts_col: str) -> dict[str, dict[str, Any]]:
    if frame.empty:
        return {}
    ordered = frame.sort_values([ts_col, key_col], ascending=[False, True], na_position="last")
    deduped = ordered.drop_duplicates(subset=[key_col], keep="first")
    out: dict[str, dict[str, Any]] = {}
    for _, row in deduped.iterrows():
        if row[key_col] in {None, ""}:
            continue
        out[str(row[key_col])] = dict(row)
    return out


def _json_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if raw in {None, ""}:
        return {}
    try:
        value = json.loads(str(raw))
    except Exception:  # noqa: BLE001
        return {}
    return value if isinstance(value, dict) else {}


def _table_exists(con, table_name: str) -> bool:
    schema_name, table = table_name.split(".", 1)
    row = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        LIMIT 1
        """,
        [schema_name, table],
    ).fetchone()
    return row is not None


def _coerce_optional_text(value: Any) -> str | None:
    if value in {None, ""}:
        return None
    text = str(value).strip()
    return text or None


def _coerce_optional_float(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except Exception:  # noqa: BLE001
        return None


def _mean_optional(series) -> float | None:
    cleaned = pd.to_numeric(series, errors="coerce").dropna()
    if cleaned.empty:
        return None
    return round(float(cleaned.mean()), 6)


def _mean_rows(rows: list[RankingRetrospectiveRow], field_name: str) -> float | None:
    values = [float(getattr(row, field_name)) for row in rows if getattr(row, field_name) is not None]
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def _optional_delta(candidate: float | None, baseline: float | None) -> float | None:
    if candidate is None or baseline is None:
        return None
    return round(float(candidate) - float(baseline), 6)


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(microsecond=0)
    return value.astimezone(UTC).replace(tzinfo=None, microsecond=0)


def _sql_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _normalize_datetime(value).isoformat(sep=" ")


__all__ = [
    "RANKING_RETROSPECTIVE_ROW_COLUMNS",
    "RANKING_RETROSPECTIVE_RUN_COLUMNS",
    "compare_retrospective_uplift",
    "enqueue_ranking_retrospective_row_upserts",
    "enqueue_ranking_retrospective_run_upserts",
    "materialize_ranking_retrospective",
    "ranking_retrospective_row_to_row",
    "ranking_retrospective_run_to_row",
]
