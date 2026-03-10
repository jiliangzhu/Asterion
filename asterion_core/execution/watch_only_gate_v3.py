from __future__ import annotations

from typing import Any


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def decide_watch_only(
    *,
    was_watch_only: bool,
    rolling_backlog_p95_ms: int,
    backlog_p95_ms_max: int,
    backlog_p95_ms_recover: int,
    dq_pass_rate_5m: float | None,
    dq_pass_rate_min: float,
    ws_coverage_5m: float | None,
    ws_coverage_min: float,
    risk_source_prior_share: float | None,
    risk_source_prior_share_max: float,
    risk_source_sample_n: int,
    risk_source_min_samples: int,
) -> dict[str, Any]:
    reason_codes: list[str] = []
    reasons: list[str] = []

    rolling = int(max(0, int(rolling_backlog_p95_ms)))
    max_th = int(max(0, int(backlog_p95_ms_max)))
    recover_th = int(max(0, int(backlog_p95_ms_recover)))

    backlog_watch = rolling > (recover_th if was_watch_only else max_th)
    if backlog_watch:
        threshold = recover_th if was_watch_only else max_th
        reason_codes.append("backlog")
        reasons.append(f"writer_queue_backlog_p95_ms={rolling}>{threshold}")

    dq_v = _as_float(dq_pass_rate_5m)
    dq_min = float(dq_pass_rate_min)
    dq_watch = dq_v is not None and dq_v < dq_min
    if dq_watch:
        reason_codes.append("dq")
        reasons.append(f"dq_pass_rate_5m={dq_v:.4f}<{dq_min:.4f}")

    ws_v = _as_float(ws_coverage_5m)
    ws_min = float(ws_coverage_min)
    ws_watch = ws_v is not None and ws_v < ws_min
    if ws_watch:
        reason_codes.append("ws")
        reasons.append(f"ws_coverage_5m={ws_v:.4f}<{ws_min:.4f}")

    prior_share = _as_float(risk_source_prior_share)
    prior_max = float(risk_source_prior_share_max)
    n = int(max(0, int(risk_source_sample_n)))
    n_min = int(max(0, int(risk_source_min_samples)))
    risk_watch = prior_share is not None and n >= n_min and prior_share > prior_max
    if risk_watch:
        reason_codes.append("risk_source_prior")
        reasons.append(f"risk_source_prior_share={prior_share:.4f}>{prior_max:.4f} (n={n})")

    watch_only = bool(backlog_watch or dq_watch or ws_watch or risk_watch)

    return {
        "watch_only": watch_only,
        "reason": "; ".join(reasons) if reasons else "",
        "reason_codes": reason_codes,
        "signals": {
            "backlog_watch": bool(backlog_watch),
            "dq_watch": bool(dq_watch),
            "ws_watch": bool(ws_watch),
            "risk_source_prior_watch": bool(risk_watch),
        },
    }
