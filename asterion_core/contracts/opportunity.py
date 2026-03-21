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
    station_id: str | None = None
    metric: str | None = None
    side: str | None = None
    horizon_bucket: str | None = None
    liquidity_bucket: str | None = None
    market_age_bucket: str | None = None
    hours_to_close_bucket: str | None = None
    calibration_quality_bucket: str | None = None
    source_freshness_bucket: str | None = None

    def __post_init__(self) -> None:
        for name in (
            "market_id",
            "strategy_id",
            "wallet_id",
            "station_id",
            "metric",
            "side",
            "horizon_bucket",
            "liquidity_bucket",
            "market_age_bucket",
            "hours_to_close_bucket",
            "calibration_quality_bucket",
            "source_freshness_bucket",
        ):
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
    submit_latency_ms_p50: float | None = None
    submit_latency_ms_p90: float | None = None
    fill_latency_ms_p50: float | None = None
    fill_latency_ms_p90: float | None = None
    realized_edge_retention_bps_p50: float | None = None
    realized_edge_retention_bps_p90: float | None = None
    avg_realized_pnl: float | None = None
    avg_post_trade_error: float | None = None
    prior_quality_status: str = "missing"
    prior_lookup_mode: str = "exact_market"
    prior_feature_scope: dict[str, Any] | None = None
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
        if self.prior_lookup_mode not in {"exact_market", "station_metric_fallback", "heuristic_fallback"}:
            raise ValueError("prior_lookup_mode must be exact_market/station_metric_fallback/heuristic_fallback")
        if self.prior_feature_scope is not None and not isinstance(self.prior_feature_scope, dict):
            raise ValueError("prior_feature_scope must be a dictionary when provided")
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
class RankingRetrospectiveRun:
    run_id: str
    baseline_version: str
    window_start: Any
    window_end: Any
    snapshot_count: int
    row_count: int
    summary_json: dict[str, Any]
    created_at: Any

    def __post_init__(self) -> None:
        if not self.run_id or not self.baseline_version:
            raise ValueError("run_id and baseline_version are required")
        if int(self.snapshot_count) < 0 or int(self.row_count) < 0:
            raise ValueError("snapshot_count and row_count must be non-negative")
        if not isinstance(self.summary_json, dict):
            raise ValueError("summary_json must be a dictionary")


@dataclass(frozen=True)
class RankingRetrospectiveRow:
    row_id: str
    run_id: str
    market_id: str
    strategy_id: str
    side: str
    ranking_decile: int
    top_k_bucket: str
    evaluation_status: str
    submitted_capture_ratio: float
    fill_capture_ratio: float
    resolution_capture_ratio: float
    avg_ranking_score: float
    avg_edge_bps_executable: float
    avg_realized_pnl: float | None
    avg_predicted_vs_realized_gap: float | None
    forecast_replay_change_rate: float
    top_rank_share_of_realized_pnl: float
    window_start: Any
    window_end: Any
    created_at: Any

    def __post_init__(self) -> None:
        if not self.row_id or not self.run_id or not self.market_id or not self.strategy_id or not self.side:
            raise ValueError("row_id, run_id, market_id, strategy_id, and side are required")
        if int(self.ranking_decile) <= 0:
            raise ValueError("ranking_decile must be positive")
        if not self.top_k_bucket or not self.evaluation_status:
            raise ValueError("top_k_bucket and evaluation_status are required")
        for name in (
            "submitted_capture_ratio",
            "fill_capture_ratio",
            "resolution_capture_ratio",
            "forecast_replay_change_rate",
            "top_rank_share_of_realized_pnl",
        ):
            value = float(getattr(self, name))
            if not (0.0 <= value <= 1.0):
                raise ValueError(f"{name} must be between 0 and 1")


@dataclass(frozen=True)
class RankingRetrospectiveSummary:
    baseline_version: str
    snapshot_count: int
    row_count: int
    top_decile_submitted_capture_ratio: float
    top_decile_fill_capture_ratio: float
    top_decile_resolution_capture_ratio: float
    top_decile_realized_pnl: float | None
    top_decile_realized_pnl_share: float

    def __post_init__(self) -> None:
        if not self.baseline_version:
            raise ValueError("baseline_version is required")
        if int(self.snapshot_count) < 0 or int(self.row_count) < 0:
            raise ValueError("snapshot_count and row_count must be non-negative")
        for name in (
            "top_decile_submitted_capture_ratio",
            "top_decile_fill_capture_ratio",
            "top_decile_resolution_capture_ratio",
            "top_decile_realized_pnl_share",
        ):
            value = float(getattr(self, name))
            if not (0.0 <= value <= 1.0):
                raise ValueError(f"{name} must be between 0 and 1")


@dataclass(frozen=True)
class CapitalAllocationRun:
    allocation_run_id: str
    run_id: str
    wallet_id: str
    strategy_id: str | None
    source_kind: str
    requested_decision_count: int
    decision_count: int
    approved_count: int
    resized_count: int
    blocked_count: int
    policy_missing_count: int
    requested_buy_notional_total: float
    recommended_buy_notional_total: float
    created_at: Any

    def __post_init__(self) -> None:
        for name in ("allocation_run_id", "run_id", "wallet_id", "source_kind"):
            if not str(getattr(self, name) or "").strip():
                raise ValueError(f"{name} is required")
        if self.strategy_id is not None and not str(self.strategy_id).strip():
            raise ValueError("strategy_id must be non-empty when provided")
        for name in (
            "requested_decision_count",
            "decision_count",
            "approved_count",
            "resized_count",
            "blocked_count",
            "policy_missing_count",
        ):
            if int(getattr(self, name)) < 0:
                raise ValueError(f"{name} must be non-negative")
        for name in ("requested_buy_notional_total", "recommended_buy_notional_total"):
            if float(getattr(self, name)) < 0.0:
                raise ValueError(f"{name} must be non-negative")


@dataclass(frozen=True)
class AllocationDecision:
    allocation_decision_id: str
    allocation_run_id: str
    run_id: str
    decision_id: str
    watch_snapshot_id: str
    wallet_id: str
    strategy_id: str
    market_id: str
    token_id: str
    side: str
    ranking_score: float
    base_ranking_score: float
    deployable_expected_pnl: float
    deployable_notional: float
    max_deployable_size: float
    capital_scarcity_penalty: float
    concentration_penalty: float
    requested_size: float
    recommended_size: float
    requested_notional: float
    recommended_notional: float
    allocation_status: str
    reason_codes: tuple[str, ...]
    budget_impact: dict[str, Any]
    policy_id: str | None
    policy_version: str | None
    capital_policy_id: str | None
    capital_policy_version: str | None
    source_kind: str
    binding_limit_scope: str | None
    binding_limit_key: str | None
    regime_bucket: str | None
    calibration_gate_status: str | None
    capital_scaling_reason_codes: tuple[str, ...]
    created_at: Any
    pre_budget_deployable_size: float = 0.0
    pre_budget_deployable_notional: float = 0.0
    pre_budget_deployable_expected_pnl: float = 0.0
    rerank_position: int | None = None
    rerank_reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in (
            "allocation_decision_id",
            "allocation_run_id",
            "run_id",
            "decision_id",
            "watch_snapshot_id",
            "wallet_id",
            "strategy_id",
            "market_id",
            "token_id",
            "side",
            "source_kind",
        ):
            if not str(getattr(self, name) or "").strip():
                raise ValueError(f"{name} is required")
        if self.allocation_status not in {"approved", "resized", "blocked", "policy_missing"}:
            raise ValueError("allocation_status must be approved/resized/blocked/policy_missing")
        for name in (
            "ranking_score",
            "base_ranking_score",
            "deployable_expected_pnl",
            "deployable_notional",
            "max_deployable_size",
            "capital_scarcity_penalty",
            "concentration_penalty",
            "requested_size",
            "recommended_size",
            "requested_notional",
            "recommended_notional",
            "pre_budget_deployable_size",
            "pre_budget_deployable_notional",
            "pre_budget_deployable_expected_pnl",
        ):
            if float(getattr(self, name)) < 0.0:
                raise ValueError(f"{name} must be non-negative")
        if not isinstance(self.reason_codes, tuple):
            raise ValueError("reason_codes must be a tuple")
        if not isinstance(self.rerank_reason_codes, tuple):
            raise ValueError("rerank_reason_codes must be a tuple")
        if not isinstance(self.capital_scaling_reason_codes, tuple):
            raise ValueError("capital_scaling_reason_codes must be a tuple")
        if not isinstance(self.budget_impact, dict):
            raise ValueError("budget_impact must be a dictionary")
        for name in (
            "policy_id",
            "policy_version",
            "capital_policy_id",
            "capital_policy_version",
            "binding_limit_scope",
            "binding_limit_key",
            "regime_bucket",
            "calibration_gate_status",
        ):
            value = getattr(self, name)
            if value is not None and not str(value).strip():
                raise ValueError(f"{name} must be non-empty when provided")
        if self.rerank_position is not None and int(self.rerank_position) <= 0:
            raise ValueError("rerank_position must be strictly positive when provided")


@dataclass(frozen=True)
class PositionLimitCheck:
    check_id: str
    allocation_decision_id: str
    limit_id: str
    limit_scope: str
    scope_key: str
    observed_gross_notional: float
    candidate_gross_notional: float
    remaining_capacity: float | None
    check_status: str
    created_at: Any

    def __post_init__(self) -> None:
        for name in ("check_id", "allocation_decision_id", "limit_id", "limit_scope", "scope_key", "check_status"):
            if not str(getattr(self, name) or "").strip():
                raise ValueError(f"{name} is required")
        for name in ("observed_gross_notional", "candidate_gross_notional"):
            if float(getattr(self, name)) < 0.0:
                raise ValueError(f"{name} must be non-negative")
        if self.remaining_capacity is not None and float(self.remaining_capacity) < 0.0:
            raise ValueError("remaining_capacity must be non-negative when provided")


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
    base_ranking_score: float | None
    deployable_expected_pnl: float | None
    deployable_notional: float | None
    max_deployable_size: float | None
    capital_scarcity_penalty: float | None
    concentration_penalty: float | None
    recommended_size: float | None
    allocation_status: str | None
    budget_impact: dict[str, Any] | None
    execution_prior_key: str | None
    why_ranked_json: dict[str, Any]
    ranking_score: float
    actionability_status: str
    calibration_gate_status: str
    calibration_gate_reason_codes: list[str]
    calibration_impacted_market: bool
    capital_policy_id: str | None
    capital_policy_version: str | None
    capital_scaling_reason_codes: list[str] | None
    regime_bucket: str | None
    rationale: str
    assessment_context_json: dict[str, Any]
    pre_budget_deployable_size: float | None = None
    pre_budget_deployable_notional: float | None = None
    pre_budget_deployable_expected_pnl: float | None = None
    rerank_position: int | None = None
    rerank_reason_codes: list[str] | None = None

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
        for name in (
            "base_ranking_score",
            "deployable_expected_pnl",
            "deployable_notional",
            "max_deployable_size",
            "capital_scarcity_penalty",
            "concentration_penalty",
            "pre_budget_deployable_size",
            "pre_budget_deployable_notional",
            "pre_budget_deployable_expected_pnl",
        ):
            value = getattr(self, name)
            if value is not None and float(value) < 0.0:
                raise ValueError(f"{name} must be non-negative when provided")
        if self.recommended_size is not None and float(self.recommended_size) < 0.0:
            raise ValueError("recommended_size must be non-negative when provided")
        if self.allocation_status is not None and self.allocation_status not in {
            "approved",
            "resized",
            "blocked",
            "policy_missing",
        }:
            raise ValueError("allocation_status must be approved/resized/blocked/policy_missing when provided")
        if self.budget_impact is not None and not isinstance(self.budget_impact, dict):
            raise ValueError("budget_impact must be a dictionary when provided")
        if self.execution_prior_key is not None and not str(self.execution_prior_key).strip():
            raise ValueError("execution_prior_key must be non-empty when provided")
        if not isinstance(self.why_ranked_json, dict):
            raise ValueError("why_ranked_json must be a dictionary")
        if float(self.ranking_score) < 0.0:
            raise ValueError("ranking_score must be non-negative")
        if self.actionability_status not in {"actionable", "review_required", "blocked", "no_trade"}:
            raise ValueError("actionability_status must be actionable/review_required/blocked/no_trade")
        if self.calibration_gate_status not in {"clear", "review_required", "research_only", "blocked"}:
            raise ValueError("calibration_gate_status must be clear/review_required/research_only/blocked")
        if not isinstance(self.calibration_gate_reason_codes, list):
            raise ValueError("calibration_gate_reason_codes must be a list")
        if not isinstance(bool(self.calibration_impacted_market), bool):
            raise ValueError("calibration_impacted_market must be boolean-like")
        for name in ("capital_policy_id", "capital_policy_version", "regime_bucket"):
            value = getattr(self, name)
            if value is not None and not str(value).strip():
                raise ValueError(f"{name} must be non-empty when provided")
        if self.capital_scaling_reason_codes is not None and not isinstance(self.capital_scaling_reason_codes, list):
            raise ValueError("capital_scaling_reason_codes must be a list when provided")
        if not self.rationale:
            raise ValueError("rationale is required")
        if not isinstance(self.assessment_context_json, dict):
            raise ValueError("assessment_context_json must be a dictionary")
        if self.rerank_position is not None and int(self.rerank_position) <= 0:
            raise ValueError("rerank_position must be strictly positive when provided")
        if self.rerank_reason_codes is not None and not isinstance(self.rerank_reason_codes, list):
            raise ValueError("rerank_reason_codes must be a list when provided")
