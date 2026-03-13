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
    con = duckdb.connect(str(db_path), read_only=True)
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
    rows: list[dict[str, Any]] = []
    for market in selected_markets:
        market_id = market.get("market_id")
        rows.append(
            {
                **market,
                "spec": specs_by_market.get(market_id) or {},
                "forecast": forecasts_by_market.get(market_id) or {},
                "pricing": pricing_by_market.get(market_id) or {},
                "signals": signals_by_market.get(market_id) or {},
            }
        )
    return {
        "market_watch": market_payload["market_watch"],
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
    }


def build_ops_console_overview() -> dict[str, Any]:
    readiness = load_readiness_summary()
    execution = load_execution_console_data()
    wallets = load_wallet_readiness_data()
    market_data = load_market_watch_data()
    agent_data = load_agent_review_data()

    live_execution = execution["live_prereq"]
    exceptions = execution["exceptions"]
    weather_report = market_data["weather_smoke_report"] or {}
    wallet_attention = (
        wallets[wallets["attention_required"] == True]  # noqa: E712
        if "attention_required" in wallets.columns
        else wallets.iloc[0:0]
    )

    return {
        "readiness": readiness,
        "execution": execution,
        "wallets": wallets,
        "market_data": market_data,
        "agent_data": agent_data,
        "metrics": {
            "go_decision": readiness.get("go_decision") or "UNKNOWN",
            "failed_gate_count": len(readiness.get("failed_gate_names") or []),
            "wallet_ready_count": int((wallets["wallet_readiness_status"] == "ready").sum()) if "wallet_readiness_status" in wallets.columns else 0,
            "wallet_total_count": int(len(wallets.index)),
            "live_prereq_attention_count": int((live_execution["live_prereq_attention_required"] == True).sum()) if "live_prereq_attention_required" in live_execution.columns else 0,  # noqa: E712
            "exception_count": int(len(exceptions.index)),
            "weather_chain_status": weather_report.get("chain_status") or "unknown",
            "weather_market_question": ((weather_report.get("market_discovery") or {}).get("question") or "未发现实时市场"),
            "weather_market_count": int((weather_report.get("market_discovery") or {}).get("selected_market_count") or 0),
            "weather_locations": sorted(
                {
                    str(item.get("location_name"))
                    for item in ((weather_report.get("market_discovery") or {}).get("selected_markets") or [])
                    if item.get("location_name")
                }
            ),
            "agent_activity_count": int(len(agent_data["frame"].index)),
            "agent_review_required_count": int((agent_data["frame"]["human_review_required"] == True).sum()) if ("human_review_required" in agent_data["frame"].columns and not agent_data["frame"].empty) else 0,  # noqa: E712
        },
        "wallet_attention": wallet_attention,
    }
