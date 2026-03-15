from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd

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
    "agent_review_summary": "ui.agent_review_summary",
}


def _maybe_load_project_dotenv() -> None:
    path = ROOT / ".env"
    if not path.exists():
        return
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if not key:
            continue
        os.environ.setdefault(key, value)


_maybe_load_project_dotenv()


def _safe_read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None
    return payload if isinstance(payload, dict) else None


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


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame()


def _read_ui_table(db_path: Path, table: str) -> pd.DataFrame:
    if duckdb is None or not db_path.exists():
        return _empty_df()
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        return con.execute(f"SELECT * FROM {table}").df()
    except Exception:  # noqa: BLE001
        return _empty_df()
    finally:
        con.close()


def _read_agent_review_from_runtime(db_path: Path) -> pd.DataFrame:
    if duckdb is None or not db_path.exists():
        return _empty_df()
    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except Exception:  # noqa: BLE001
        return _empty_df()
    try:
        return con.execute(
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
    except Exception:  # noqa: BLE001
        return _empty_df()
    finally:
        con.close()


def _read_weather_market_rows_from_runtime(db_path: Path) -> pd.DataFrame:
    if duckdb is None or not db_path.exists():
        return _empty_df()
    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except Exception:  # noqa: BLE001
        return _empty_df()
    try:
        return con.execute(
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
    except Exception:  # noqa: BLE001
        return _empty_df()
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
    if any(status == "failure" for status in statuses):
        return "agent_failure"
    verdicts = [item.get("rule2spec_verdict"), item.get("data_qa_verdict"), item.get("resolution_verdict")]
    if any(verdict == "review" for verdict in verdicts):
        return "review_required"
    if any(status == "success" for status in statuses):
        return "passed"
    return "no_agent_signal"


def _confidence_from_market_row(item: dict[str, Any], agent_review_status: str) -> float:
    if agent_review_status == "agent_failure":
        return 35.0
    if agent_review_status == "review_required":
        return 60.0
    if agent_review_status == "passed":
        return 85.0
    return 50.0


def _liquidity_proxy_for_row(item: dict[str, Any], market_price: float | None) -> float:
    accepting_orders = _normalize_bool(item.get("accepting_orders"))
    if not accepting_orders:
        return 25.0
    if market_price is not None and 0.10 <= market_price <= 0.90:
        return 70.0
    return 55.0


def _compute_actionability_status(
    *,
    edge_bps: float | None,
    best_side: str | None,
    accepting_orders: bool,
    live_prereq_status: str,
    agent_review_status: str,
) -> str:
    if edge_bps is None or edge_bps <= 0 or not best_side:
        return "no_trade"
    if not accepting_orders or live_prereq_status == "attention_required":
        return "blocked"
    if agent_review_status in {"agent_failure", "review_required", "no_agent_signal"}:
        return "review_required"
    return "actionable"


def _compute_opportunity_score(
    *,
    edge_bps: float | None,
    liquidity_proxy: float,
    confidence_proxy: float,
    accepting_orders: bool,
    live_prereq_status: str,
) -> float:
    edge_component = max(edge_bps or 0.0, 0.0) / 50.0
    live_bonus = 10.0 if live_prereq_status == "shadow_aligned" else 6.0 if live_prereq_status == "not_started" else 0.0
    score = edge_component + (liquidity_proxy * 0.25) + (confidence_proxy * 0.25) + (12.0 if accepting_orders else 0.0) + live_bonus
    return round(min(score, 100.0), 2)


def _sort_market_opportunities(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    sortable = frame.copy()
    sortable["_actionability_rank"] = sortable.get("actionability_status", pd.Series(dtype="object")).map(_ACTIONABILITY_ORDER).fillna(9)
    sortable["_opportunity_score_value"] = pd.to_numeric(sortable.get("opportunity_score"), errors="coerce").fillna(-1.0)
    sortable["_edge_bps_value"] = pd.to_numeric(sortable.get("edge_bps"), errors="coerce").fillna(-999999.0)
    sortable["_market_close_time_value"] = sortable.get("market_close_time", pd.Series(dtype="object")).fillna("")
    sortable = sortable.sort_values(
        by=["_actionability_rank", "_opportunity_score_value", "_edge_bps_value", "_market_close_time_value"],
        ascending=[True, False, False, True],
        kind="stable",
    )
    return sortable.drop(columns=["_actionability_rank", "_opportunity_score_value", "_edge_bps_value", "_market_close_time_value"], errors="ignore")


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
                    -float(signal.get("edge_bps") or 0.0),
                ),
            )[0]
        if best_signal is not None:
            signal_outcome = best_signal.get("outcome")
            signal_market_price = _coerce_float(best_signal.get("reference_price"))
            signal_fair_value = _coerce_float(best_signal.get("fair_value"))
            if signal_market_price is None and signal_outcome in market_prices:
                best_signal["reference_price"] = market_prices.get(signal_outcome)
            if signal_fair_value is None and signal_outcome in fair_value_map:
                best_signal["fair_value"] = fair_value_map.get(signal_outcome)
            signal_market_price = _coerce_float(best_signal.get("reference_price"))
            signal_fair_value = _coerce_float(best_signal.get("fair_value"))
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
                        -float(signal.get("edge_bps") or 0.0),
                    ),
                )[0]
        market_price = _coerce_float((best_signal or {}).get("reference_price"))
        fair_value = _coerce_float((best_signal or {}).get("fair_value"))
        edge_bps = _coerce_float((best_signal or {}).get("edge_bps"))
        best_side = _ensure_text((best_signal or {}).get("side")) or None
        agent_review_status = _derive_agent_review_status(item)
        live_prereq_status = "not_started"
        accepting_orders = _normalize_bool(item.get("accepting_orders"))
        liquidity_proxy = _liquidity_proxy_for_row(item, market_price)
        confidence_proxy = _confidence_from_market_row(item, agent_review_status)
        actionability_status = _compute_actionability_status(
            edge_bps=edge_bps,
            best_side=best_side,
            accepting_orders=accepting_orders,
            live_prereq_status=live_prereq_status,
            agent_review_status=agent_review_status,
        )
        rows.append(
            {
                "market_id": market_id,
                "question": item.get("question"),
                "location_name": item.get("location_name"),
                "station_id": item.get("station_id"),
                "market_close_time": item.get("close_time"),
                "accepting_orders": accepting_orders,
                "best_side": best_side,
                "best_outcome": (best_signal or {}).get("outcome"),
                "best_decision": (best_signal or {}).get("decision"),
                "market_price": market_price,
                "fair_value": fair_value,
                "edge_bps": edge_bps,
                "liquidity_proxy": liquidity_proxy,
                "confidence_proxy": confidence_proxy,
                "agent_review_status": agent_review_status,
                "live_prereq_status": live_prereq_status,
                "opportunity_bucket": "high_edge" if (edge_bps or 0) >= 1500 else "medium_edge" if (edge_bps or 0) >= 750 else "low_edge" if (edge_bps or 0) > 0 else "negative_edge",
                "actionability_status": actionability_status,
                "opportunity_score": _compute_opportunity_score(
                    edge_bps=edge_bps,
                    liquidity_proxy=liquidity_proxy,
                    confidence_proxy=confidence_proxy,
                    accepting_orders=accepting_orders,
                    live_prereq_status=live_prereq_status,
                ),
                "latest_run_source": (report.get("forecast_service") or {}).get("source_used"),
                "latest_forecast_target_time": None,
                "threshold_bps": (best_signal or {}).get("threshold_bps"),
                "signal_created_at": report.get("timestamp"),
            }
        )
    return _sort_market_opportunities(pd.DataFrame(rows))


def load_ui_lite_snapshot() -> dict[str, Any]:
    db_path = _resolve_ui_lite_db_path()
    tables = {name: _read_ui_table(db_path, table) for name, table in UI_TABLES.items()}
    return {
        "db_path": str(db_path),
        "exists": db_path.exists(),
        "tables": tables,
        "table_row_counts": {name: int(len(frame.index)) for name, frame in tables.items()},
    }


def load_readiness_summary() -> dict[str, Any]:
    snapshot = load_ui_lite_snapshot()
    frame = snapshot["tables"]["phase_readiness_summary"]
    report_path = _resolve_readiness_report_path()
    report = _safe_read_json(report_path)

    target = None
    go_decision = None
    decision_reason = None
    updated_at = None
    if report:
        target = report.get("target")
        go_decision = report.get("go_decision")
        decision_reason = report.get("decision_reason")
        updated_at = report.get("evaluated_at")

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
        "failed_gate_names": failed_gate_names,
        "source": "ui_lite+json" if (snapshot["exists"] or report_path.exists()) else "missing",
    }


def load_execution_console_data() -> dict[str, pd.DataFrame]:
    snapshot = load_ui_lite_snapshot()
    tickets = _sort_desc(snapshot["tables"]["execution_ticket_summary"], "latest_transition_at", "last_fill_at")
    runs = _sort_desc(snapshot["tables"]["execution_run_summary"], "latest_event_at")
    exceptions = _sort_desc(snapshot["tables"]["execution_exception_summary"], "latest_transition_at", "latest_event_at")
    live_prereq = _sort_desc(snapshot["tables"]["live_prereq_execution_summary"], "latest_submit_created_at", "latest_sign_attempt_created_at")
    journal = _sort_desc(snapshot["tables"]["paper_run_journal_summary"], "latest_event_at")
    daily_ops = _sort_desc(snapshot["tables"]["daily_ops_summary"], "latest_event_at")
    return {
        "tickets": tickets,
        "runs": runs,
        "exceptions": exceptions,
        "live_prereq": live_prereq,
        "journal": journal,
        "daily_ops": daily_ops,
    }


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
    frame = _sort_market_opportunities(snapshot["tables"]["market_opportunity_summary"])
    if not frame.empty:
        return {"source": "ui_lite", "frame": frame}
    report_frame = _derive_market_opportunities_from_report(load_real_weather_smoke_report())
    if not report_frame.empty:
        return {"source": "smoke_report", "frame": report_frame}
    runtime_frame = _sort_market_opportunities(_read_weather_market_rows_from_runtime(_resolve_real_weather_chain_db_path()))
    return {"source": "weather_smoke_db", "frame": runtime_frame}


def load_agent_review_data() -> dict[str, Any]:
    snapshot = load_ui_lite_snapshot()
    frame = _sort_desc(snapshot["tables"]["agent_review_summary"], "updated_at")
    if not frame.empty:
        return {"source": "ui_lite", "frame": frame}

    runtime_frame = _sort_desc(_read_agent_review_from_runtime(_resolve_canonical_db_path()), "updated_at")
    if not runtime_frame.empty:
        return {"source": "runtime_db", "frame": runtime_frame}

    smoke_runtime_frame = _sort_desc(_read_agent_review_from_runtime(_resolve_real_weather_chain_db_path()), "updated_at")
    if not smoke_runtime_frame.empty:
        return {"source": "weather_smoke_db", "frame": smoke_runtime_frame}

    smoke_frame = _sort_desc(_agent_rows_from_smoke_report(load_real_weather_smoke_report()), "updated_at")
    return {"source": "smoke_report", "frame": smoke_frame}


def load_market_chain_analysis_data() -> dict[str, Any]:
    market_payload = load_market_watch_data()
    opportunity_payload = load_market_opportunity_data()
    report = market_payload["weather_smoke_report"] or {}
    discovery = report.get("market_discovery") or {}
    selected_markets = discovery.get("selected_markets") or []
    specs_by_market = {
        item.get("market_id"): item for item in (report.get("rule_parse") or {}).get("selected_specs") or []
    }
    forecasts_by_market = {
        item.get("market_id"): item for item in (report.get("forecast_service") or {}).get("markets") or []
    }
    pricing_by_market = {
        item.get("market_id"): item for item in (report.get("pricing_engine") or {}).get("markets") or []
    }
    signals_by_market = {
        item.get("market_id"): item for item in (report.get("opportunity_discovery") or {}).get("markets") or []
    }
    detail_rows: list[dict[str, Any]] = []
    for market in selected_markets:
        market_id = market.get("market_id")
        detail_rows.append(
            {
                **market,
                "spec": specs_by_market.get(market_id) or {},
                "forecast": forecasts_by_market.get(market_id) or {},
                "pricing": pricing_by_market.get(market_id) or {},
                "signals": signals_by_market.get(market_id) or {},
            }
        )
    if not detail_rows:
        runtime_rows = _read_weather_market_rows_from_runtime(_resolve_real_weather_chain_db_path())
        if not runtime_rows.empty:
            detail_rows = [
                {
                    **row.to_dict(),
                    "spec": {
                        "location_name": row.get("location_name"),
                        "station_id": row.get("station_id"),
                        "authoritative_source": row.get("authoritative_source"),
                        "metric": row.get("metric"),
                        "bucket_min_value": row.get("bucket_min_value"),
                        "bucket_max_value": row.get("bucket_max_value"),
                        "observation_window_local": row.get("observation_window_local"),
                    },
                    "forecast": {},
                    "pricing": {},
                    "signals": {},
                    "forecast_status": "not_started",
                    "forecast_summary": "forecast stage has not completed yet",
                }
                for _, row in runtime_rows.iterrows()
            ]
    details_by_market = {str(item.get("market_id")): item for item in detail_rows if item.get("market_id") is not None}
    opportunities = opportunity_payload["frame"]
    rows: list[dict[str, Any]] = []
    if not opportunities.empty:
        for _, row in opportunities.iterrows():
            market_id = str(row.get("market_id"))
            details = details_by_market.get(market_id, {})
            payload = row.to_dict()
            payload.update(
                {
                    "spec": details.get("spec") or {},
                    "forecast": details.get("forecast") or {},
                    "pricing": details.get("pricing") or {},
                    "signals": details.get("signals") or {},
                    "forecast_status": details.get("forecast_status"),
                    "forecast_summary": details.get("forecast_summary"),
                    "rule2spec_status": details.get("rule2spec_status"),
                    "rule2spec_verdict": details.get("rule2spec_verdict"),
                    "rule2spec_summary": details.get("rule2spec_summary"),
                    "data_qa_status": details.get("data_qa_status"),
                    "data_qa_verdict": details.get("data_qa_verdict"),
                    "data_qa_summary": details.get("data_qa_summary"),
                    "resolution_status": details.get("resolution_status"),
                    "resolution_verdict": details.get("resolution_verdict"),
                    "resolution_summary": details.get("resolution_summary"),
                }
            )
            rows.append(payload)
    else:
        rows = detail_rows
    return {
        "market_watch": market_payload["market_watch"],
        "market_opportunities": opportunities,
        "market_opportunity_source": opportunity_payload["source"],
        "weather_smoke_report": report,
        "market_rows": rows,
    }


def load_agent_runtime_status() -> dict[str, Any]:
    provider = os.getenv("ASTERION_AGENT_PROVIDER", "").strip() or "openai_compatible"
    model = (
        os.getenv("ASTERION_OPENAI_COMPATIBLE_MODEL", "").strip()
        or os.getenv("ASTERION_AGENT_MODEL", "").strip()
        or os.getenv("QWEN_MODEL", "").strip()
        or "unconfigured"
    )
    has_qwen_key = bool(os.getenv("QWEN_API_KEY", "").strip())
    has_alibaba_key = bool(os.getenv("ALIBABA_API_KEY", "").strip())
    has_compatible_key = bool(os.getenv("ASTERION_OPENAI_COMPATIBLE_API_KEY", "").strip())
    effective_key_source = "missing"
    if has_compatible_key:
        effective_key_source = "ASTERION_OPENAI_COMPATIBLE_API_KEY"
    elif has_alibaba_key:
        effective_key_source = "ALIBABA_API_KEY"
    elif has_qwen_key:
        effective_key_source = "QWEN_API_KEY"
    return {
        "provider": provider,
        "model": model,
        "key_source": effective_key_source,
        "configured": effective_key_source != "missing" and model != "unconfigured",
        "agents": [
            {
                "agent_name": "Rule2Spec Agent",
                "file": str(ROOT / "agents" / "weather" / "rule2spec_agent.py"),
                "role": "规则文本 -> WeatherMarketSpec",
            },
            {
                "agent_name": "DataQA Agent",
                "file": str(ROOT / "agents" / "weather" / "data_qa_agent.py"),
                "role": "预测/定价/回放质量检查",
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
    snapshot = load_ui_lite_snapshot()
    report = load_real_weather_smoke_report()
    opportunities = load_market_opportunity_data()["frame"]
    agent_data = load_agent_review_data()["frame"]
    return {
        "ui_lite_db_path": snapshot["db_path"],
        "ui_lite_exists": snapshot["exists"],
        "ui_replica_db_path": str(_resolve_ui_replica_db_path()),
        "ui_replica_exists": _resolve_ui_replica_db_path().exists(),
        "readiness_report_path": readiness["report_path"],
        "readiness_report_exists": readiness["report_exists"],
        "readiness_report_markdown_path": readiness["report_markdown_path"],
        "readiness_report_markdown_exists": Path(readiness["report_markdown_path"]).exists(),
        "weather_smoke_report_path": str(_resolve_real_weather_smoke_report_path()),
        "weather_smoke_report_exists": _resolve_real_weather_smoke_report_path().exists(),
        "weather_smoke_status": (report or {}).get("chain_status"),
        "table_row_counts": snapshot["table_row_counts"],
        "opportunity_row_count": int(len(opportunities.index)),
        "actionable_market_count": int((opportunities["actionability_status"] == "actionable").sum()) if "actionability_status" in opportunities.columns else 0,
        "agent_row_count": int(len(agent_data.index)),
    }


def build_ops_console_overview() -> dict[str, Any]:
    readiness = load_readiness_summary()
    execution = load_execution_console_data()
    wallets = load_wallet_readiness_data()
    market_watch_data = load_market_watch_data()
    market_analysis = load_market_chain_analysis_data()
    agent_data = load_agent_review_data()

    live_execution = execution["live_prereq"]
    exceptions = execution["exceptions"]
    weather_report = market_watch_data["weather_smoke_report"] or {}
    opportunities = market_analysis["market_opportunities"]
    wallet_attention = (
        wallets[wallets["attention_required"] == True]  # noqa: E712
        if "attention_required" in wallets.columns
        else wallets.iloc[0:0]
    )
    actionable = (
        opportunities[opportunities["actionability_status"] == "actionable"]
        if "actionability_status" in opportunities.columns
        else opportunities.iloc[0:0]
    )
    top_opportunities = actionable.head(5) if not actionable.empty else opportunities.head(5)
    if readiness.get("failed_gate_names"):
        largest_blocker = " / ".join(readiness.get("failed_gate_names") or [])
        blocker_source = "readiness"
    elif not wallet_attention.empty and "wallet_readiness_status" in wallet_attention.columns:
        largest_blocker = _ensure_text(wallet_attention.iloc[0].get("wallet_readiness_status")) or "wallet blocker"
        blocker_source = "wallet"
    elif not exceptions.empty:
        largest_blocker = (
            _ensure_text(exceptions.iloc[0].get("live_prereq_execution_status"))
            or _ensure_text(exceptions.iloc[0].get("execution_result"))
            or "execution attention required"
        )
        blocker_source = "execution"
    else:
        largest_blocker = "No material blocker"
        blocker_source = "clear"

    return {
        "readiness": readiness,
        "execution": execution,
        "wallets": wallets,
        "market_data": market_analysis,
        "market_watch_data": market_watch_data,
        "agent_data": agent_data,
        "top_opportunities": top_opportunities,
        "largest_blocker": {"summary": largest_blocker, "source": blocker_source},
        "metrics": {
            "go_decision": readiness.get("go_decision") or "UNKNOWN",
            "failed_gate_count": len(readiness.get("failed_gate_names") or []),
            "wallet_ready_count": int((wallets["wallet_readiness_status"] == "ready").sum()) if "wallet_readiness_status" in wallets.columns else 0,
            "wallet_total_count": int(len(wallets.index)),
            "live_prereq_attention_count": int((live_execution["live_prereq_attention_required"] == True).sum()) if "live_prereq_attention_required" in live_execution.columns else 0,  # noqa: E712
            "exception_count": int(len(exceptions.index)),
            "weather_chain_status": weather_report.get("chain_status") or "unknown",
            "weather_market_question": ((weather_report.get("market_discovery") or {}).get("question") or "未发现实时市场"),
            "weather_market_count": int(len(opportunities.index)),
            "actionable_market_count": int(len(actionable.index)),
            "top_opportunity_score": float(top_opportunities.iloc[0]["opportunity_score"]) if (not top_opportunities.empty and "opportunity_score" in top_opportunities.columns) else 0.0,
            "highest_edge_bps": float(pd.to_numeric(opportunities["edge_bps"], errors="coerce").max()) if ("edge_bps" in opportunities.columns and not opportunities.empty) else 0.0,
            "liquidity_ready_count": int(((pd.to_numeric(opportunities["liquidity_proxy"], errors="coerce").fillna(0) >= 60.0) & (opportunities["accepting_orders"] == True)).sum()) if ({"liquidity_proxy", "accepting_orders"} <= set(opportunities.columns)) else 0,  # noqa: E712
            "weather_locations": sorted({str(value) for value in opportunities["location_name"].dropna().tolist()}) if "location_name" in opportunities.columns else [],
            "agent_activity_count": int(len(agent_data["frame"].index)),
            "agent_review_required_count": int((agent_data["frame"]["human_review_required"] == True).sum()) if ("human_review_required" in agent_data["frame"].columns and not agent_data["frame"].empty) else 0,  # noqa: E712
        },
        "wallet_attention": wallet_attention,
    }


def load_home_decision_snapshot() -> dict[str, Any]:
    overview = build_ops_console_overview()
    agent_frame = overview["agent_data"]["frame"]
    top_agent_row = agent_frame.iloc[0].to_dict() if not agent_frame.empty else {}
    return {
        **overview,
        "recent_agent_summary": {
            "agent_type": top_agent_row.get("agent_type"),
            "verdict": top_agent_row.get("verdict"),
            "summary": top_agent_row.get("summary"),
            "updated_at": top_agent_row.get("updated_at"),
        },
    }
