from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class MarketQualityAssessment:
    market_id: str
    price_staleness_ms: int
    source_freshness_status: str
    mapping_confidence: float
    spread_bps: int
    depth_proxy: float
    market_quality_status: str
    market_quality_reason_codes: list[str]

    def __post_init__(self) -> None:
        if not self.market_id:
            raise ValueError("market_id is required")
        if self.price_staleness_ms < 0:
            raise ValueError("price_staleness_ms must be non-negative")
        if not (0.0 <= float(self.mapping_confidence) <= 1.0):
            raise ValueError("mapping_confidence must be between 0 and 1")
        if self.spread_bps < 0:
            raise ValueError("spread_bps must be non-negative")
        if float(self.depth_proxy) < 0.0:
            raise ValueError("depth_proxy must be non-negative")
        if self.source_freshness_status not in {"fresh", "stale", "degraded", "missing"}:
            raise ValueError("source_freshness_status must be fresh/stale/degraded/missing")
        if self.market_quality_status not in {"pass", "review_required", "blocked"}:
            raise ValueError("market_quality_status must be pass/review_required/blocked")
        if not isinstance(self.market_quality_reason_codes, list):
            raise ValueError("market_quality_reason_codes must be a list")


@dataclass(frozen=True)
class ExecutionPriorKey:
    market_id: str | None
    strategy_id: str | None
    wallet_id: str | None
    side: str | None
    horizon_bucket: str | None
    liquidity_bucket: str | None

    def __post_init__(self) -> None:
        for name in ("market_id", "strategy_id", "wallet_id", "side", "horizon_bucket", "liquidity_bucket"):
            value = getattr(self, name)
            if value is not None and not str(value).strip():
                raise ValueError(f"{name} must be non-empty when provided")


@dataclass(frozen=True)
class ExecutionPriorSummary:
    prior_key: ExecutionPriorKey
    sample_count: int
    submit_ack_rate: float
    fill_rate: float
    resolution_rate: float
    partial_fill_rate: float
    cancel_rate: float
    adverse_fill_slippage_bps_p50: float | None
    adverse_fill_slippage_bps_p90: float | None
    avg_realized_pnl: float | None
    avg_post_trade_error: float | None
    prior_quality_status: str
    feedback_prior: "ExecutionFeedbackPrior | None" = None

    def __post_init__(self) -> None:
        if int(self.sample_count) < 0:
            raise ValueError("sample_count must be non-negative")
        for name in ("submit_ack_rate", "fill_rate", "resolution_rate", "partial_fill_rate", "cancel_rate"):
            value = float(getattr(self, name))
            if not (0.0 <= value <= 1.0):
                raise ValueError(f"{name} must be between 0 and 1")
        if not self.prior_quality_status:
            raise ValueError("prior_quality_status is required")
        if self.feedback_prior is not None and not isinstance(self.feedback_prior, ExecutionFeedbackPrior):
            raise ValueError("feedback_prior must be an ExecutionFeedbackPrior when provided")


@dataclass(frozen=True)
class ExecutionFeedbackPrior:
    feedback_penalty: float
    feedback_status: str
    cohort_prior_version: str | None
    dominant_miss_reason_bucket: str
    dominant_distortion_reason_bucket: str
    scope_breakdown: dict[str, Any]

    def __post_init__(self) -> None:
        if not (0.0 <= float(self.feedback_penalty) <= 1.0):
            raise ValueError("feedback_penalty must be between 0 and 1")
        if self.feedback_status not in {"heuristic_only", "ready", "watch", "sparse", "degraded", "missing"}:
            raise ValueError("feedback_status must be heuristic_only/ready/watch/sparse/degraded/missing")
        if not self.dominant_miss_reason_bucket:
            raise ValueError("dominant_miss_reason_bucket is required")
        if not self.dominant_distortion_reason_bucket:
            raise ValueError("dominant_distortion_reason_bucket is required")
        if not isinstance(self.scope_breakdown, dict):
            raise ValueError("scope_breakdown must be a dictionary")


@dataclass(frozen=True)
class CohortDistortionSummary:
    cohort_type: str
    cohort_key: str
    ticket_count: int
    submitted_ack_count: int
    filled_ticket_count: int
    resolved_ticket_count: int
    partial_fill_count: int
    cancelled_count: int
    rejected_count: int
    working_unfilled_count: int
    miss_rate: float
    distortion_rate: float
    dominant_miss_reason_bucket: str
    dominant_distortion_reason_bucket: str

    def __post_init__(self) -> None:
        if self.cohort_type not in {"market", "strategy", "wallet"}:
            raise ValueError("cohort_type must be market/strategy/wallet")
        if not self.cohort_key:
            raise ValueError("cohort_key is required")
        for name in (
            "ticket_count",
            "submitted_ack_count",
            "filled_ticket_count",
            "resolved_ticket_count",
            "partial_fill_count",
            "cancelled_count",
            "rejected_count",
            "working_unfilled_count",
        ):
            if int(getattr(self, name)) < 0:
                raise ValueError(f"{name} must be non-negative")
        for name in ("miss_rate", "distortion_rate"):
            value = float(getattr(self, name))
            if not (0.0 <= value <= 1.0):
                raise ValueError(f"{name} must be between 0 and 1")
        if not self.dominant_miss_reason_bucket:
            raise ValueError("dominant_miss_reason_bucket is required")
        if not self.dominant_distortion_reason_bucket:
            raise ValueError("dominant_distortion_reason_bucket is required")


@dataclass(frozen=True)
class ExecutionFeedbackMaterializationStatus:
    materialization_id: str
    run_id: str
    job_name: str
    prior_version: str
    status: str
    lookback_days: int
    source_window_start: Any
    source_window_end: Any
    input_ticket_count: int
    output_prior_count: int
    degraded_prior_count: int
    materialized_at: Any
    error: str | None = None

    def __post_init__(self) -> None:
        for name in ("materialization_id", "run_id", "job_name", "prior_version", "status"):
            if not str(getattr(self, name) or "").strip():
                raise ValueError(f"{name} is required")
        if int(self.lookback_days) <= 0:
            raise ValueError("lookback_days must be positive")
        for name in ("input_ticket_count", "output_prior_count", "degraded_prior_count"):
            if int(getattr(self, name)) < 0:
                raise ValueError(f"{name} must be non-negative")


@dataclass(frozen=True)
class RankingScoreV2Decomposition:
    expected_dollar_pnl: float
    capture_probability: float
    risk_penalty: float
    capital_efficiency: float
    ops_tie_breaker: float
    ranking_score: float
    why_ranked_json: dict[str, Any]

    def __post_init__(self) -> None:
        if not (0.0 <= float(self.capture_probability) <= 1.0):
            raise ValueError("capture_probability must be between 0 and 1")
        for name in ("expected_dollar_pnl", "risk_penalty", "capital_efficiency", "ops_tie_breaker", "ranking_score"):
            if float(getattr(self, name)) < 0.0:
                raise ValueError(f"{name} must be non-negative")
        if not isinstance(self.why_ranked_json, dict):
            raise ValueError("why_ranked_json must be a dictionary")


@dataclass(frozen=True)
class OpportunityAssessment:
    assessment_id: str
    market_id: str
    token_id: str
    outcome: str
    reference_price: float
    model_fair_value: float
    execution_adjusted_fair_value: float
    fees_bps: int
    slippage_bps: int
    fill_probability: float
    depth_proxy: float
    liquidity_penalty_bps: int
    edge_bps_model: int
    edge_bps_executable: int
    confidence_score: float
    calibration_health_status: str
    calibration_bias_quality: str
    threshold_probability_quality: str
    sample_count: int
    uncertainty_multiplier: float
    uncertainty_penalty_bps: int
    ranking_penalty_reasons: list[str]
    ops_readiness_score: float
    expected_value_score: float
    expected_pnl_score: float
    expected_dollar_pnl: float
    capture_probability: float
    risk_penalty: float
    capital_efficiency: float
    feedback_penalty: float
    feedback_status: str
    cohort_prior_version: str | None
    execution_prior_key: str | None
    why_ranked_json: dict[str, Any]
    ranking_score: float
    actionability_status: str
    rationale: str
    assessment_context_json: dict[str, Any]

    def __post_init__(self) -> None:
        if not self.assessment_id:
            raise ValueError("assessment_id is required")
        if not self.market_id or not self.token_id or not self.outcome:
            raise ValueError("market_id, token_id, and outcome are required")
        for name, value in [
            ("reference_price", self.reference_price),
            ("model_fair_value", self.model_fair_value),
            ("execution_adjusted_fair_value", self.execution_adjusted_fair_value),
        ]:
            if not (0.0 <= float(value) <= 1.0):
                raise ValueError(f"{name} must be between 0 and 1")
        if not (0.0 <= float(self.fill_probability) <= 1.0):
            raise ValueError("fill_probability must be between 0 and 1")
        if float(self.depth_proxy) < 0.0:
            raise ValueError("depth_proxy must be non-negative")
        if float(self.confidence_score) < 0.0:
            raise ValueError("confidence_score must be non-negative")
        if not self.calibration_health_status:
            raise ValueError("calibration_health_status is required")
        if not self.calibration_bias_quality:
            raise ValueError("calibration_bias_quality is required")
        if not self.threshold_probability_quality:
            raise ValueError("threshold_probability_quality is required")
        if int(self.sample_count) < 0:
            raise ValueError("sample_count must be non-negative")
        if not (0.0 <= float(self.uncertainty_multiplier) <= 1.0):
            raise ValueError("uncertainty_multiplier must be between 0 and 1")
        if int(self.uncertainty_penalty_bps) < 0:
            raise ValueError("uncertainty_penalty_bps must be non-negative")
        if not isinstance(self.ranking_penalty_reasons, list):
            raise ValueError("ranking_penalty_reasons must be a list")
        if float(self.ops_readiness_score) < 0.0:
            raise ValueError("ops_readiness_score must be non-negative")
        if float(self.expected_value_score) < 0.0:
            raise ValueError("expected_value_score must be non-negative")
        if float(self.expected_pnl_score) < 0.0:
            raise ValueError("expected_pnl_score must be non-negative")
        if float(self.expected_dollar_pnl) < 0.0:
            raise ValueError("expected_dollar_pnl must be non-negative")
        if not (0.0 <= float(self.capture_probability) <= 1.0):
            raise ValueError("capture_probability must be between 0 and 1")
        if float(self.risk_penalty) < 0.0:
            raise ValueError("risk_penalty must be non-negative")
        if float(self.capital_efficiency) < 0.0:
            raise ValueError("capital_efficiency must be non-negative")
        if not (0.0 <= float(self.feedback_penalty) <= 1.0):
            raise ValueError("feedback_penalty must be between 0 and 1")
        if self.feedback_status not in {"heuristic_only", "ready", "watch", "sparse", "degraded", "missing"}:
            raise ValueError("feedback_status must be heuristic_only/ready/watch/sparse/degraded/missing")
        if self.cohort_prior_version is not None and not str(self.cohort_prior_version).strip():
            raise ValueError("cohort_prior_version must be non-empty when provided")
        if self.execution_prior_key is not None and not str(self.execution_prior_key).strip():
            raise ValueError("execution_prior_key must be non-empty when provided")
        if not isinstance(self.why_ranked_json, dict):
            raise ValueError("why_ranked_json must be a dictionary")
        if float(self.ranking_score) < 0.0:
            raise ValueError("ranking_score must be non-negative")
        if self.actionability_status not in {"actionable", "review_required", "blocked", "no_trade"}:
            raise ValueError("actionability_status must be actionable/review_required/blocked/no_trade")
        if not self.rationale:
            raise ValueError("rationale is required")
        if not isinstance(self.assessment_context_json, dict):
            raise ValueError("assessment_context_json must be a dictionary")
