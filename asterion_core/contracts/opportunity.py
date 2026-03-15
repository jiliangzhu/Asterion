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
    ops_readiness_score: float
    expected_value_score: float
    expected_pnl_score: float
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
        if float(self.ops_readiness_score) < 0.0:
            raise ValueError("ops_readiness_score must be non-negative")
        if float(self.expected_value_score) < 0.0:
            raise ValueError("expected_value_score must be non-negative")
        if float(self.expected_pnl_score) < 0.0:
            raise ValueError("expected_pnl_score must be non-negative")
        if float(self.ranking_score) < 0.0:
            raise ValueError("ranking_score must be non-negative")
        if self.actionability_status not in {"actionable", "review_required", "blocked", "no_trade"}:
            raise ValueError("actionability_status must be actionable/review_required/blocked/no_trade")
        if not self.rationale:
            raise ValueError("rationale is required")
        if not isinstance(self.assessment_context_json, dict):
            raise ValueError("assessment_context_json must be a dictionary")
