from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class ResolvedExecutionProjection:
    resolution_value: float | None
    realized_pnl: float | None
    post_trade_error: float | None
    evaluation_status: str
    execution_lifecycle_stage: str
    latest_resolution_at: Any
    fill_ratio: float
    adverse_fill_slippage_bps: float | None
    resolution_lag_hours: float | None


def resolution_value_from_expected_outcome(expected_outcome: Any) -> float | None:
    text = str(expected_outcome or "").strip().upper()
    if text == "YES":
        return 1.0
    if text == "NO":
        return 0.0
    return None


def evaluation_status_for_ticket(*, filled_quantity: float, resolution_value: float | None) -> str:
    if resolution_value is not None and filled_quantity > 0:
        return "resolved"
    if filled_quantity > 0:
        return "pending_resolution"
    return "pending_fill"


def execution_lifecycle_stage(
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
    execution_result_text = str(execution_result or "").strip().lower()
    order_status_text = str(order_status or "").strip().lower()
    live_status_text = str(live_prereq_execution_status or "").strip().lower()
    submit_status_text = str(latest_submit_status or "").strip().lower()
    external_order_text = str(external_order_status or "").strip().lower()
    gate_allowed_bool = bool(gate_allowed) if gate_allowed is not None else None

    if evaluation_status == "resolved":
        return "resolved"
    if execution_result_text == "partial_filled" or (0.0 < fill_ratio < 1.0):
        return "partially_filled"
    if filled_quantity > 0 and evaluation_status == "pending_resolution":
        return "filled_unresolved"
    if execution_result_text == "cancelled" or order_status_text == "cancelled":
        return "cancelled"
    if submit_status_text == "accepted":
        return "submitted_ack"
    if order_status_text == "posted" and filled_quantity <= 0:
        return "working_unfilled"
    if live_status_text == "submit_rejected" or external_order_text == "rejected":
        return "submit_rejected"
    if live_status_text == "sign_rejected":
        return "sign_rejected"
    if gate_allowed_bool is False:
        return "gate_rejected"
    if latest_sign_attempt_id and not latest_submit_attempt_id:
        return "signed_not_submitted"
    return "ticket_created"


def build_resolved_execution_projection(
    *,
    outcome: Any,
    side: Any,
    expected_outcome: Any,
    filled_quantity: float,
    ticket_size: float,
    expected_fill_price: float | None,
    realized_fill_price: float | None,
    total_fee: float,
    predicted_edge_bps: float | None,
    execution_result: Any,
    order_status: Any,
    latest_submit_status: Any,
    live_prereq_execution_status: Any,
    external_order_status: Any,
    gate_allowed: Any,
    latest_sign_attempt_id: Any,
    latest_submit_attempt_id: Any,
    latest_fill_at: Any,
    latest_resolution_at: Any,
) -> ResolvedExecutionProjection:
    normalized_filled_quantity = max(0.0, float(filled_quantity or 0.0))
    normalized_ticket_size = max(0.0, float(ticket_size or 0.0))
    normalized_total_fee = max(0.0, float(total_fee or 0.0))
    fill_ratio = normalized_filled_quantity / normalized_ticket_size if normalized_ticket_size > 0 else 0.0
    resolution_value = resolution_value_from_expected_outcome(expected_outcome)
    evaluation_status = evaluation_status_for_ticket(
        filled_quantity=normalized_filled_quantity,
        resolution_value=resolution_value,
    )
    stage = execution_lifecycle_stage(
        evaluation_status=evaluation_status,
        filled_quantity=normalized_filled_quantity,
        fill_ratio=fill_ratio,
        execution_result=execution_result,
        order_status=order_status,
        latest_submit_status=latest_submit_status,
        live_prereq_execution_status=live_prereq_execution_status,
        external_order_status=external_order_status,
        gate_allowed=gate_allowed,
        latest_sign_attempt_id=latest_sign_attempt_id,
        latest_submit_attempt_id=latest_submit_attempt_id,
    )

    normalized_side = str(side or "").strip().upper()
    adverse_fill_slippage_bps = None
    if expected_fill_price is not None and realized_fill_price is not None:
        expected_price = float(expected_fill_price)
        realized_price = float(realized_fill_price)
        if normalized_side == "SELL":
            adverse_fill_slippage_bps = max((expected_price - realized_price) * 10000.0, 0.0)
        else:
            adverse_fill_slippage_bps = max((realized_price - expected_price) * 10000.0, 0.0)

    realized_pnl = None
    post_trade_error = None
    if resolution_value is not None and realized_fill_price is not None and normalized_filled_quantity > 0:
        contract_value = resolution_value if str(outcome or "").strip().upper() == "YES" else 1.0 - resolution_value
        realized_price = float(realized_fill_price)
        if normalized_side == "SELL":
            realized_pnl = (realized_price - contract_value) * normalized_filled_quantity - normalized_total_fee
        else:
            realized_pnl = (contract_value - realized_price) * normalized_filled_quantity - normalized_total_fee
        if predicted_edge_bps is not None:
            implied_pnl = (float(predicted_edge_bps) / 10000.0) * normalized_filled_quantity
            post_trade_error = realized_pnl - implied_pnl

    resolution_lag_hours = None
    if latest_fill_at is not None and latest_resolution_at is not None:
        fill_ts = _coerce_datetime(latest_fill_at)
        resolution_ts = _coerce_datetime(latest_resolution_at)
        if fill_ts is not None and resolution_ts is not None:
            resolution_lag_hours = float((resolution_ts - fill_ts).total_seconds() / 3600.0)

    return ResolvedExecutionProjection(
        resolution_value=resolution_value,
        realized_pnl=realized_pnl,
        post_trade_error=post_trade_error,
        evaluation_status=evaluation_status,
        execution_lifecycle_stage=stage,
        latest_resolution_at=latest_resolution_at,
        fill_ratio=fill_ratio,
        adverse_fill_slippage_bps=adverse_fill_slippage_bps,
        resolution_lag_hours=resolution_lag_hours,
    )


def _coerce_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if value in {None, ""}:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None
