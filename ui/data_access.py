from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd
from domains.weather.opportunity import build_weather_opportunity_assessment, derive_opportunity_side

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
        return {
            "frame": con.execute(
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
        ).df(),
            "error": None,
        }
    except Exception as exc:  # noqa: BLE001
        return {"frame": _empty_df(), "error": str(exc)}
    finally:
        con.close()


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
        return {
            "frame": con.execute(
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
        ).df(),
            "error": None,
        }
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
    if any(status == "failure" for status in statuses):
        return "agent_failure"
    verdicts = [item.get("rule2spec_verdict"), item.get("data_qa_verdict"), item.get("resolution_verdict")]
    if any(verdict == "review" for verdict in verdicts):
        return "review_required"
    if any(status == "success" for status in statuses):
        return "passed"
    return "no_agent_signal"


def _sort_market_opportunities(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    sortable = frame.copy()
    sortable["_actionability_rank"] = sortable.get("actionability_status", pd.Series(dtype="object")).map(_ACTIONABILITY_ORDER).fillna(9)
    if "ranking_score" in sortable.columns:
        sortable["_opportunity_score_value"] = pd.to_numeric(sortable.get("ranking_score"), errors="coerce").fillna(-1.0)
    else:
        sortable["_opportunity_score_value"] = pd.to_numeric(sortable.get("opportunity_score"), errors="coerce").fillna(-1.0)
    sortable["_edge_bps_value"] = pd.to_numeric(sortable.get("edge_bps"), errors="coerce").fillna(-999999.0)
    sortable["_market_close_time_value"] = sortable.get("market_close_time", pd.Series(dtype="object")).fillna("")
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
    sample_count: int = 0,
    calibration_multiplier: float | None = None,
    calibration_reason_codes: list[str] | None = None,
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
        sample_count=sample_count,
        calibration_multiplier=calibration_multiplier,
        calibration_reason_codes=calibration_reason_codes,
        source_context={
            "calibration_health_status": calibration_health_status,
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
        "agent_review_status": agent_review_status,
        "live_prereq_status": live_prereq_status,
        "opportunity_bucket": "high_edge" if edge_magnitude >= 1500 else "medium_edge" if edge_magnitude >= 750 else "low_edge" if edge_magnitude > 0 else "negative_edge",
        "opportunity_score": assessment.ranking_score,
        "actionability_status": assessment.actionability_status,
        "latest_run_source": latest_run_source,
        "latest_forecast_target_time": latest_forecast_target_time,
        "threshold_bps": threshold_bps,
        "signal_created_at": signal_created_at,
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
                    "calibration_health_status": _ensure_text((best_signal or {}).get("calibration_health_status")) or "lookup_missing",
                    "sample_count": int(_coerce_float((best_signal or {}).get("sample_count")) or 0),
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
                    "ops_readiness_score": 10.0,
                    "expected_value_score": 0.0,
                    "expected_pnl_score": 0.0,
                    "ranking_score": 0.0,
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
    frame = _sort_desc(snapshot["tables"]["predicted_vs_realized_summary"], "latest_fill_at", "latest_resolution_at")
    return {
        "source": "ui_lite" if not frame.empty else ("ui_lite" if snapshot["exists"] else "missing"),
        "frame": frame,
        "read_error": snapshot.get("read_error"),
    }


def load_execution_console_data() -> dict[str, pd.DataFrame]:
    snapshot = load_ui_lite_snapshot()
    tickets = _sort_desc(snapshot["tables"]["execution_ticket_summary"], "latest_transition_at", "last_fill_at")
    runs = _sort_desc(snapshot["tables"]["execution_run_summary"], "latest_event_at")
    exceptions = _sort_desc(snapshot["tables"]["execution_exception_summary"], "latest_transition_at", "latest_event_at")
    live_prereq = _sort_desc(snapshot["tables"]["live_prereq_execution_summary"], "latest_submit_created_at", "latest_sign_attempt_created_at")
    journal = _sort_desc(snapshot["tables"]["paper_run_journal_summary"], "latest_event_at")
    daily_ops = _sort_desc(snapshot["tables"]["daily_ops_summary"], "latest_event_at")
    predicted_vs_realized = _sort_desc(snapshot["tables"]["predicted_vs_realized_summary"], "latest_fill_at", "latest_resolution_at")
    watch_only_vs_executed = _sort_desc(snapshot["tables"]["watch_only_vs_executed_summary"], "fill_capture_ratio", "avg_executable_edge_bps")
    execution_science = _sort_desc(snapshot["tables"]["execution_science_summary"], "resolution_capture_ratio", "fill_capture_ratio", "submission_capture_ratio")
    market_research = _sort_desc(snapshot["tables"]["market_research_summary"], "resolution_capture_ratio", "avg_post_trade_error")
    calibration_health = _sort_desc(snapshot["tables"]["calibration_health_summary"], "sample_count", "mean_abs_residual")
    return {
        "tickets": tickets,
        "runs": runs,
        "exceptions": exceptions,
        "live_prereq": live_prereq,
        "journal": journal,
        "daily_ops": daily_ops,
        "predicted_vs_realized": predicted_vs_realized,
        "watch_only_vs_executed": watch_only_vs_executed,
        "execution_science": execution_science,
        "market_research": market_research,
        "calibration_health": calibration_health,
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
        return {"source": "ui_lite", "frame": frame, "read_error": snapshot.get("read_error")}
    report = load_real_weather_smoke_report()
    report_frame = _derive_market_opportunities_from_report(report)
    if not report_frame.empty:
        return {"source": "smoke_report", "frame": report_frame, "read_error": snapshot.get("read_error")}
    if report:
        chain_status = _ensure_text(report.get("chain_status"))
        refresh_state = _ensure_text(report.get("refresh_state"))
        if chain_status not in {"initializing", "unknown"} and refresh_state != "initializing":
            return {"source": "smoke_report", "frame": report_frame, "read_error": snapshot.get("read_error")}
    runtime_result = _read_weather_market_rows_from_runtime_result(_resolve_real_weather_chain_db_path())
    runtime_frame = _sort_market_opportunities(runtime_result["frame"])
    return {"source": "weather_smoke_db", "frame": runtime_frame, "read_error": snapshot.get("read_error") or runtime_result["error"]}


def load_agent_review_data() -> dict[str, Any]:
    snapshot = load_ui_lite_snapshot()
    frame = _sort_desc(snapshot["tables"]["agent_review_summary"], "updated_at")
    if not frame.empty:
        return {"source": "ui_lite", "frame": frame, "read_error": snapshot.get("read_error")}

    runtime_result = _read_agent_review_from_runtime_result(_resolve_canonical_db_path())
    runtime_frame = _sort_desc(runtime_result["frame"], "updated_at")
    if not runtime_frame.empty:
        return {"source": "runtime_db", "frame": runtime_frame, "read_error": snapshot.get("read_error") or runtime_result["error"]}

    smoke_runtime_result = _read_agent_review_from_runtime_result(_resolve_real_weather_chain_db_path())
    smoke_runtime_frame = _sort_desc(smoke_runtime_result["frame"], "updated_at")
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
    market_payload = load_market_watch_data()
    opportunity_payload = load_market_opportunity_data()
    predicted_vs_realized_payload = load_predicted_vs_realized_data()
    execution_payload = load_execution_console_data()
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
    predicted_vs_realized = predicted_vs_realized_payload["frame"]
    watch_only_vs_executed = execution_payload["watch_only_vs_executed"]
    market_research = execution_payload["market_research"]
    execution_summary_by_market: dict[str, dict[str, Any]] = {}
    if not predicted_vs_realized.empty and "market_id" in predicted_vs_realized.columns:
        for market_id, frame in predicted_vs_realized.groupby("market_id", dropna=False):
            sorted_frame = _sort_desc(frame, "latest_fill_at", "latest_resolution_at")
            latest = sorted_frame.iloc[0].to_dict() if not sorted_frame.empty else {}
            execution_summary_by_market[str(market_id)] = {
                "has_executed_evidence": not sorted_frame.empty,
                "latest_ticket_id": latest.get("ticket_id"),
                "latest_order_id": latest.get("order_id"),
                "predicted_edge_bps": latest.get("predicted_edge_bps"),
                "expected_fill_price": latest.get("expected_fill_price"),
                "realized_fill_price": latest.get("realized_fill_price"),
                "realized_pnl": latest.get("realized_pnl"),
                "resolution_value": latest.get("resolution_value"),
                "post_trade_error": latest.get("post_trade_error"),
                "source_disagreement": latest.get("source_disagreement"),
                "evaluation_status": latest.get("evaluation_status"),
                "execution_lifecycle_stage": latest.get("execution_lifecycle_stage"),
                "fill_ratio": latest.get("fill_ratio"),
                "adverse_fill_slippage_bps": latest.get("adverse_fill_slippage_bps"),
                "resolution_lag_hours": latest.get("resolution_lag_hours"),
                "miss_reason_bucket": latest.get("miss_reason_bucket"),
                "distortion_reason_codes_json": latest.get("distortion_reason_codes_json"),
                "latest_fill_at": latest.get("latest_fill_at"),
                "latest_resolution_at": latest.get("latest_resolution_at"),
            }
    watch_only_vs_executed_by_market: dict[str, dict[str, Any]] = {}
    if not watch_only_vs_executed.empty and "market_id" in watch_only_vs_executed.columns:
        for _, row in watch_only_vs_executed.iterrows():
            watch_only_vs_executed_by_market[str(row.get("market_id"))] = row.to_dict()
    research_by_market: dict[str, dict[str, Any]] = {}
    if not market_research.empty and "market_id" in market_research.columns:
        for _, row in market_research.iterrows():
            research_by_market[str(row.get("market_id"))] = row.to_dict()
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
                    "executed_evidence": execution_summary_by_market.get(market_id) or {"has_executed_evidence": False},
                    "watch_only_vs_executed": watch_only_vs_executed_by_market.get(market_id) or {},
                    "market_research": research_by_market.get(market_id) or {},
                }
            )
            rows.append(payload)
    else:
        rows = [
            {
                **item,
                "executed_evidence": execution_summary_by_market.get(str(item.get("market_id"))) or {"has_executed_evidence": False},
                "watch_only_vs_executed": watch_only_vs_executed_by_market.get(str(item.get("market_id"))) or {},
                "market_research": research_by_market.get(str(item.get("market_id"))) or {},
            }
            for item in detail_rows
        ]
    return {
        "market_watch": market_payload["market_watch"],
        "market_opportunities": opportunities,
        "market_opportunity_source": opportunity_payload["source"],
        "predicted_vs_realized": predicted_vs_realized,
        "watch_only_vs_executed": watch_only_vs_executed,
        "market_research": market_research,
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
    evidence = load_readiness_evidence_bundle()
    snapshot = load_ui_lite_snapshot()
    report_result = _read_json_result(_resolve_real_weather_smoke_report_path())
    report = report_result["payload"]
    opportunity_payload = load_market_opportunity_data()
    opportunities = opportunity_payload["frame"]
    agent_payload = load_agent_review_data()
    agent_data = agent_payload["frame"]
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
        "opportunity_read_error": opportunity_payload.get("read_error"),
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
            "Agent 工作暂无数据",
            "当前没有可见的 agent work rows。",
            agent_source,
            None,
        )
    elif agent_source in {"smoke_report", "weather_smoke_db"}:
        agent_surface = _surface_status(
            "degraded_source",
            "Agent 工作来自降级数据源",
            "当前 agent work 通过 smoke/runtime fallback 暴露，尚未进入 UI lite 主读面。",
            agent_source,
            agent_frame.iloc[0].get("updated_at") if not agent_frame.empty else None,
        )
    else:
        agent_surface = _surface_status(
            "ok",
            "Agent 工作正常",
            "agent review rows 可正常读取。",
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
    readiness = load_readiness_summary()
    evidence = load_readiness_evidence_bundle()
    execution = load_execution_console_data()
    wallets = load_wallet_readiness_data()
    market_watch_data = load_market_watch_data()
    market_analysis = load_market_chain_analysis_data()
    agent_data = load_agent_review_data()
    predicted_vs_realized = load_predicted_vs_realized_data()["frame"]
    watch_only_vs_executed = execution["watch_only_vs_executed"]
    execution_science = execution["execution_science"]
    calibration_health = execution["calibration_health"]

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
    resolved_rows = (
        predicted_vs_realized[predicted_vs_realized["evaluation_status"] == "resolved"]
        if ("evaluation_status" in predicted_vs_realized.columns and not predicted_vs_realized.empty)
        else predicted_vs_realized.iloc[0:0]
    )
    uncaptured_high_edge = watch_only_vs_executed.iloc[0:0]
    if not watch_only_vs_executed.empty:
        uncaptured_high_edge = watch_only_vs_executed[
            (pd.to_numeric(watch_only_vs_executed["avg_executable_edge_bps"], errors="coerce").fillna(0) > 0)
            & (pd.to_numeric(watch_only_vs_executed["submission_capture_ratio"], errors="coerce").fillna(0) <= 0)
        ]
    total_opportunities = float(pd.to_numeric(watch_only_vs_executed["opportunity_count"], errors="coerce").fillna(0).sum()) if ("opportunity_count" in watch_only_vs_executed.columns and not watch_only_vs_executed.empty) else 0.0
    total_submitted = float(pd.to_numeric(watch_only_vs_executed["submitted_ticket_count"], errors="coerce").fillna(0).sum()) if ("submitted_ticket_count" in watch_only_vs_executed.columns and not watch_only_vs_executed.empty) else 0.0
    total_filled = float(pd.to_numeric(watch_only_vs_executed["filled_ticket_count"], errors="coerce").fillna(0).sum()) if ("filled_ticket_count" in watch_only_vs_executed.columns and not watch_only_vs_executed.empty) else 0.0
    total_resolved = float(pd.to_numeric(watch_only_vs_executed["resolved_ticket_count"], errors="coerce").fillna(0).sum()) if ("resolved_ticket_count" in watch_only_vs_executed.columns and not watch_only_vs_executed.empty) else 0.0
    submission_capture_ratio = (total_submitted / total_opportunities) if total_opportunities > 0 else 0.0
    fill_capture_ratio = (total_filled / total_opportunities) if total_opportunities > 0 else 0.0
    resolution_capture_ratio = (total_resolved / total_opportunities) if total_opportunities > 0 else 0.0
    degraded_inputs: list[str] = []
    if evidence.get("stale_dependencies"):
        degraded_inputs.extend([f"stale:{item}" for item in evidence.get("stale_dependencies") or []])
    if market_analysis.get("market_opportunity_source") in {"smoke_report", "weather_smoke_db"}:
        degraded_inputs.append(f"market_source:{market_analysis.get('market_opportunity_source')}")
    if evidence.get("capability_manifest_status") not in {None, "valid"}:
        degraded_inputs.append(f"manifest:{evidence.get('capability_manifest_status') or 'missing'}")
    if readiness.get("failed_gate_names"):
        largest_blocker = " / ".join(readiness.get("failed_gate_names") or [])
        blocker_source = "readiness"
    elif evidence.get("blockers"):
        largest_blocker = " / ".join(str(item) for item in evidence.get("blockers") or [])
        blocker_source = "evidence"
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
        "readiness_evidence": evidence,
        "predicted_vs_realized": predicted_vs_realized,
        "watch_only_vs_executed_summary": watch_only_vs_executed,
        "execution_science_summary": execution_science,
        "calibration_health_summary": calibration_health,
        "uncaptured_high_edge_markets": uncaptured_high_edge,
        "surface_status": load_operator_surface_status(),
        "top_opportunities": top_opportunities,
        "degraded_inputs": degraded_inputs,
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
            "top_opportunity_score": float(top_opportunities.iloc[0]["ranking_score"]) if (not top_opportunities.empty and "ranking_score" in top_opportunities.columns) else float(top_opportunities.iloc[0]["opportunity_score"]) if (not top_opportunities.empty and "opportunity_score" in top_opportunities.columns) else 0.0,
            "highest_edge_bps": float(pd.to_numeric(opportunities["edge_bps"], errors="coerce").abs().max()) if ("edge_bps" in opportunities.columns and not opportunities.empty) else 0.0,
            "liquidity_ready_count": int(((pd.to_numeric(opportunities["liquidity_proxy"], errors="coerce").fillna(0) >= 60.0) & (opportunities["accepting_orders"] == True)).sum()) if ({"liquidity_proxy", "accepting_orders"} <= set(opportunities.columns)) else 0,  # noqa: E712
            "weather_locations": sorted({str(value) for value in opportunities["location_name"].dropna().tolist()}) if "location_name" in opportunities.columns else [],
            "agent_activity_count": int(len(agent_data["frame"].index)),
            "agent_review_required_count": int((agent_data["frame"]["human_review_required"] == True).sum()) if ("human_review_required" in agent_data["frame"].columns and not agent_data["frame"].empty) else 0,  # noqa: E712
            "predicted_vs_realized_count": int(len(predicted_vs_realized.index)),
            "resolved_trade_count": int(len(resolved_rows.index)),
            "pending_resolution_count": int((predicted_vs_realized["evaluation_status"] == "pending_resolution").sum()) if ("evaluation_status" in predicted_vs_realized.columns and not predicted_vs_realized.empty) else 0,
            "avg_predicted_edge_bps": float(pd.to_numeric(predicted_vs_realized["predicted_edge_bps"], errors="coerce").dropna().mean()) if ("predicted_edge_bps" in predicted_vs_realized.columns and not predicted_vs_realized.empty) else 0.0,
            "avg_realized_pnl": float(pd.to_numeric(resolved_rows["realized_pnl"], errors="coerce").dropna().mean()) if ("realized_pnl" in resolved_rows.columns and not resolved_rows.empty) else 0.0,
            "submission_capture_ratio": submission_capture_ratio,
            "fill_capture_ratio": fill_capture_ratio,
            "resolution_capture_ratio": resolution_capture_ratio,
            "execution_capture_ratio": fill_capture_ratio,
            "uncaptured_high_edge_count": int(len(uncaptured_high_edge.index)),
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
        "predicted_vs_realized_snapshot": overview["predicted_vs_realized"].head(5),
    }
