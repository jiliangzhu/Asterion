from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from asterion_core.contracts import (
    ExecutionIntelligenceSummary,
    ExecutionPriorSummary,
    MarketQualityAssessment,
    OpportunityAssessment,
    RankingScoreV2Decomposition,
    SourceHealthSnapshotRecord,
    stable_object_id,
)

from .execution_intelligence import (
    build_execution_intelligence_summary_from_context,
    execution_intelligence_context_fields,
)
from .execution_priors import (
    build_execution_prior_summary_from_context,
    execution_prior_context_fields,
    execution_prior_key_id,
)


_REVIEW_FAILURE_STATUSES = {"agent_failure"}
_REVIEW_REQUIRED_STATUSES = {"review_required", "no_agent_signal"}


def derive_opportunity_side(edge_bps: int) -> str | None:
    if edge_bps > 0:
        return "BUY"
    if edge_bps < 0:
        return "SELL"
    return None


def build_weather_opportunity_assessment(
    *,
    market_id: str,
    token_id: str,
    outcome: str,
    reference_price: float,
    model_fair_value: float,
    accepting_orders: bool,
    enable_order_book: bool | None,
    threshold_bps: int = 0,
    fees_bps: int | None = None,
    agent_review_status: str = "no_agent_signal",
    live_prereq_status: str = "not_started",
    confidence_score: float | None = None,
    mapping_confidence: float = 1.0,
    price_staleness_ms: int = 0,
    source_freshness_status: str = "fresh",
    spread_bps: int | None = None,
    calibration_health_status: str | None = None,
    calibration_bias_quality: str | None = None,
    threshold_probability_quality: str | None = None,
    sample_count: int | None = None,
    calibration_multiplier: float | None = None,
    calibration_reason_codes: list[str] | None = None,
    execution_prior_summary: ExecutionPriorSummary | None = None,
    execution_intelligence_summary: ExecutionIntelligenceSummary | None = None,
    forecast_distribution_summary_v2: dict[str, Any] | None = None,
    source_context: dict[str, Any] | None = None,
    recommended_size: float | None = None,
    allocation_status: str | None = None,
    budget_impact: dict[str, Any] | None = None,
    allocation_decision_id: str | None = None,
    policy_id: str | None = None,
    policy_version: str | None = None,
    base_ranking_score: float | None = None,
    deployable_expected_pnl: float | None = None,
    deployable_notional: float | None = None,
    max_deployable_size: float | None = None,
    capital_scarcity_penalty: float | None = None,
    concentration_penalty: float | None = None,
    deployable_ranking_score: float | None = None,
    pre_budget_deployable_size: float | None = None,
    pre_budget_deployable_notional: float | None = None,
    pre_budget_deployable_expected_pnl: float | None = None,
    rerank_position: int | None = None,
    rerank_reason_codes: list[str] | None = None,
    capital_policy_id: str | None = None,
    capital_policy_version: str | None = None,
    capital_scaling_reason_codes: list[str] | None = None,
) -> OpportunityAssessment:
    normalized_fees_bps = max(0, int(fees_bps or 0))
    normalized_reference_price = float(reference_price)
    normalized_model_fair_value = float(model_fair_value)
    normalized_threshold_bps = max(0, int(threshold_bps))
    normalized_confidence_score = max(0.0, float(confidence_score if confidence_score is not None else 50.0))

    slippage_bps = _slippage_bps(
        accepting_orders=accepting_orders,
        enable_order_book=enable_order_book,
        reference_price=normalized_reference_price,
    )
    liquidity_penalty_bps = _liquidity_penalty_bps(
        accepting_orders=accepting_orders,
        enable_order_book=enable_order_book,
        reference_price=normalized_reference_price,
    )
    fill_probability = _fill_probability(
        accepting_orders=accepting_orders,
        agent_review_status=agent_review_status,
        live_prereq_status=live_prereq_status,
    )
    depth_proxy = _depth_proxy(
        accepting_orders=accepting_orders,
        enable_order_book=enable_order_book,
        reference_price=normalized_reference_price,
    )
    ops_readiness_score = _ops_tie_breaker(live_prereq_status)
    market_quality = build_market_quality_assessment(
        market_id=market_id,
        accepting_orders=accepting_orders,
        enable_order_book=enable_order_book,
        reference_price=normalized_reference_price,
        mapping_confidence=mapping_confidence,
        price_staleness_ms=price_staleness_ms,
        source_freshness_status=source_freshness_status,
        depth_proxy=depth_proxy,
        spread_bps=spread_bps,
    )

    edge_bps_model = int(round((normalized_model_fair_value - normalized_reference_price) * 10_000))
    model_side = derive_opportunity_side(edge_bps_model)
    execution_adjusted_fair_value, edge_bps_executable = _execution_adjusted_edge(
        reference_price=normalized_reference_price,
        model_fair_value=normalized_model_fair_value,
        total_cost_bps=normalized_fees_bps + slippage_bps + liquidity_penalty_bps,
        model_side=model_side,
    )
    opportunity_side = derive_opportunity_side(edge_bps_executable)
    effective_edge_bps = abs(edge_bps_executable)
    calibration_health_value = calibration_health_status or source_context_value(source_context, "calibration_health_status")
    bias_quality_value = (
        calibration_bias_quality
        or source_context_value(source_context, "calibration_bias_quality")
        or source_context_value(source_context, "bias_quality_status")
    )
    threshold_probability_quality_value = (
        threshold_probability_quality
        or source_context_value(source_context, "threshold_probability_quality")
        or source_context_value(source_context, "threshold_probability_quality_status")
    )
    calibration_freshness_value = source_context_value(source_context, "calibration_freshness_status")
    sample_count_value = sample_count if sample_count is not None else source_context_value(source_context, "sample_count")
    calibration_gate_has_inputs = any(
        value is not None
        for value in (
            calibration_health_value,
            threshold_probability_quality_value,
            calibration_freshness_value,
            sample_count_value,
        )
    )
    normalized_calibration_health_status = _normalize_calibration_health_status(calibration_health_value)
    normalized_bias_quality = _normalize_quality_status(bias_quality_value)
    normalized_threshold_probability_quality = _normalize_quality_status(threshold_probability_quality_value)
    normalized_calibration_freshness_status = _normalize_calibration_freshness_status(calibration_freshness_value)
    normalized_sample_count = max(
        0,
        int(sample_count_value or 0),
    )
    if calibration_gate_has_inputs:
        calibration_gate_status, calibration_gate_reason_codes = _calibration_gate_status(
            calibration_freshness_status=normalized_calibration_freshness_status,
            calibration_health_status=normalized_calibration_health_status,
            threshold_probability_quality_status=normalized_threshold_probability_quality,
            sample_count=normalized_sample_count,
        )
    else:
        calibration_gate_status, calibration_gate_reason_codes = "clear", []
    calibration_impacted_market = calibration_gate_status != "clear"
    calibration_multiplier_value = _calibration_multiplier(
        calibration_health_status=normalized_calibration_health_status,
        explicit_multiplier=calibration_multiplier if calibration_multiplier is not None else source_context_value(source_context, "calibration_multiplier"),
        calibration_freshness_status=normalized_calibration_freshness_status,
    )
    freshness_multiplier = _freshness_multiplier(source_freshness_status)
    mapping_multiplier = _mapping_multiplier(float(mapping_confidence))
    market_quality_multiplier = _market_quality_multiplier(market_quality.market_quality_status)
    bias_quality_multiplier = _quality_status_multiplier(
        normalized_bias_quality,
        healthy=1.00,
        watch=0.90,
        degraded=0.70,
        sparse=0.60,
        lookup_missing=0.50,
    )
    threshold_probability_multiplier = _quality_status_multiplier(
        normalized_threshold_probability_quality,
        healthy=1.00,
        watch=0.85,
        degraded=0.65,
        sparse=0.55,
        lookup_missing=0.50,
    )
    regime_stability_score = _regime_stability_score(
        forecast_distribution_summary_v2 if forecast_distribution_summary_v2 is not None else source_context_value(source_context, "distribution_summary_v2") or source_context
    )
    regime_stability_multiplier = _regime_stability_multiplier(regime_stability_score)
    uncertainty_multiplier = round(
        _clamp_multiplier(
            calibration_multiplier_value
            * freshness_multiplier
            * mapping_multiplier
            * market_quality_multiplier
            * bias_quality_multiplier
            * threshold_probability_multiplier
            * regime_stability_multiplier
        ),
        4,
    )
    base_expected_value_score = round(effective_edge_bps * fill_probability, 4)
    base_expected_pnl_score = round(base_expected_value_score * depth_proxy, 4)
    expected_value_score = round(base_expected_value_score * uncertainty_multiplier, 4)
    expected_pnl_score = round(base_expected_pnl_score * uncertainty_multiplier, 4)
    uncertainty_penalty_bps = max(
        0,
        effective_edge_bps - int(round(effective_edge_bps * uncertainty_multiplier)),
    )
    ranking_penalty_reasons = _ranking_penalty_reasons(
        calibration_health_status=normalized_calibration_health_status,
        calibration_reason_codes=calibration_reason_codes
        if calibration_reason_codes is not None
        else _source_context_reason_codes(source_context, "calibration_reason_codes"),
        freshness_status=source_freshness_status,
        mapping_confidence=float(mapping_confidence),
        market_quality=market_quality,
        calibration_bias_quality=normalized_bias_quality,
        threshold_probability_quality=normalized_threshold_probability_quality,
        regime_stability_score=regime_stability_score,
    )
    resolved_execution_prior_summary = execution_prior_summary or build_execution_prior_summary_from_context(source_context)
    resolved_execution_intelligence_summary = (
        execution_intelligence_summary or build_execution_intelligence_summary_from_context(source_context)
    )
    ranking_v2 = _ranking_score_v2_decomposition(
        edge_bps_executable=edge_bps_executable,
        fill_probability=fill_probability,
        reference_price=normalized_reference_price,
        depth_proxy=depth_proxy,
        side=opportunity_side,
        slippage_bps=slippage_bps,
        ops_tie_breaker=ops_readiness_score,
        execution_prior_summary=resolved_execution_prior_summary,
        execution_intelligence_summary=resolved_execution_intelligence_summary,
    )
    feedback_prior = (
        resolved_execution_prior_summary.feedback_prior
        if resolved_execution_prior_summary is not None
        else None
    )
    pre_feedback_ranking_score = round(ranking_v2.ranking_score * uncertainty_multiplier, 6)
    feedback_penalty = round(float(feedback_prior.feedback_penalty), 6) if feedback_prior is not None else 0.0
    feedback_status = str(feedback_prior.feedback_status) if feedback_prior is not None else "heuristic_only"
    cohort_prior_version = (
        str(feedback_prior.cohort_prior_version)
        if feedback_prior is not None and feedback_prior.cohort_prior_version is not None
        else None
    )
    final_ranking_score = round(pre_feedback_ranking_score * max(0.0, 1.0 - feedback_penalty), 6)
    output_base_ranking_score = round(float(base_ranking_score), 6) if base_ranking_score is not None else final_ranking_score
    output_ranking_score = round(float(deployable_ranking_score), 6) if deployable_ranking_score is not None else final_ranking_score
    preview_budget = (budget_impact or {}).get("preview") if isinstance(budget_impact, dict) else {}
    if not isinstance(preview_budget, dict):
        preview_budget = {}
    preview_binding_limit_scope = source_context_value(source_context, "preview_binding_limit_scope") or preview_budget.get("preview_binding_limit_scope")
    preview_binding_limit_key = source_context_value(source_context, "preview_binding_limit_key") or preview_budget.get("preview_binding_limit_key")
    preview_requested_size = source_context_value(source_context, "requested_size")
    if preview_requested_size is None:
        preview_requested_size = preview_budget.get("requested_size")
    preview_requested_notional = source_context_value(source_context, "requested_notional")
    if preview_requested_notional is None:
        preview_requested_notional = preview_budget.get("requested_notional")
    why_ranked_json = dict(ranking_v2.why_ranked_json)
    why_ranked_json.update(
        {
            "base_ranking_score": output_base_ranking_score,
            "ranking_v2_base_score": ranking_v2.ranking_score,
            "pre_feedback_ranking_score": pre_feedback_ranking_score,
            "calibration_v2_mode": "profile_v2"
            if bool(source_context_value(source_context, "distribution_summary_v2") or forecast_distribution_summary_v2)
            else "sigma_fallback",
            "corrected_mean": source_context_value(source_context, "corrected_mean"),
            "corrected_std_dev": source_context_value(source_context, "corrected_std_dev"),
            "bias_quality_status": normalized_bias_quality,
            "threshold_probability_quality_status": normalized_threshold_probability_quality,
            "calibration_freshness_status": normalized_calibration_freshness_status,
            "threshold_probability_summary_json": source_context_value(source_context, "threshold_probability_summary_json"),
            "regime_bucket": source_context_value(source_context, "regime_bucket"),
            "regime_stability_score": regime_stability_score,
            "calibration_profile_materialized_at": source_context_value(source_context, "profile_materialized_at"),
            "calibration_profile_window_end": source_context_value(source_context, "profile_window_end"),
            "profile_age_hours": source_context_value(source_context, "profile_age_hours"),
            "calibration_gate_status": calibration_gate_status,
            "calibration_gate_reason_codes": list(calibration_gate_reason_codes),
            "calibration_impacted_market": calibration_impacted_market,
            "uncertainty_multiplier": uncertainty_multiplier,
            "feedback_penalty": feedback_penalty,
            "feedback_status": feedback_status,
            "cohort_prior_version": cohort_prior_version,
            "feedback_scope_breakdown": dict(feedback_prior.scope_breakdown) if feedback_prior is not None else {},
            "recommended_size": recommended_size,
            "allocation_status": allocation_status,
            "budget_impact": dict(budget_impact or {}),
            "allocation_decision_id": allocation_decision_id,
            "policy_id": policy_id,
            "policy_version": policy_version,
            "deployable_expected_pnl": deployable_expected_pnl,
            "deployable_notional": deployable_notional,
            "max_deployable_size": max_deployable_size,
            "capital_scarcity_penalty": capital_scarcity_penalty,
            "concentration_penalty": concentration_penalty,
            "pre_budget_deployable_size": pre_budget_deployable_size,
            "pre_budget_deployable_notional": pre_budget_deployable_notional,
            "pre_budget_deployable_expected_pnl": pre_budget_deployable_expected_pnl,
            "preview_binding_limit_scope": preview_binding_limit_scope,
            "preview_binding_limit_key": preview_binding_limit_key,
            "requested_size": preview_requested_size,
            "requested_notional": preview_requested_notional,
            "rerank_position": rerank_position,
            "rerank_reason_codes": list(rerank_reason_codes or []),
            "capital_policy_id": capital_policy_id,
            "capital_policy_version": capital_policy_version,
            "capital_scaling_reason_codes": list(capital_scaling_reason_codes or []),
            "binding_limit_scope": source_context_value(source_context, "binding_limit_scope")
            or (budget_impact or {}).get("binding_limit_scope"),
            "binding_limit_key": source_context_value(source_context, "binding_limit_key")
            or (budget_impact or {}).get("binding_limit_key"),
            "ranking_score": output_ranking_score,
        }
    )
    actionability_status = _actionability_status(
        edge_bps_executable=edge_bps_executable,
        threshold_bps=normalized_threshold_bps,
        opportunity_side=opportunity_side,
        accepting_orders=accepting_orders,
        live_prereq_status=live_prereq_status,
        agent_review_status=agent_review_status,
        market_quality_status=market_quality.market_quality_status,
        calibration_gate_status=calibration_gate_status,
    )
    rationale = (
        f"model_edge_bps={edge_bps_model}, executable_edge_bps={edge_bps_executable}, "
        f"fees_bps={normalized_fees_bps}, slippage_bps={slippage_bps}, "
        f"liquidity_penalty_bps={liquidity_penalty_bps}, fill_probability={fill_probability:.2f}, "
        f"market_quality_status={market_quality.market_quality_status}, model_side={model_side}, "
        f"uncertainty_multiplier={uncertainty_multiplier:.2f}, ranking_mode={why_ranked_json.get('mode')}"
    )
    context = {
        "accepting_orders": bool(accepting_orders),
        "actionability_status": actionability_status,
        "agent_review_status": agent_review_status,
        "best_side": opportunity_side,
        "confidence_score": normalized_confidence_score,
        "depth_proxy": depth_proxy,
        "edge_bps_executable": edge_bps_executable,
        "edge_bps_model": edge_bps_model,
        "enable_order_book": bool(enable_order_book),
        "execution_adjusted_fair_value": execution_adjusted_fair_value,
        "calibration_health_status": normalized_calibration_health_status,
        "calibration_bias_quality": normalized_bias_quality,
        "threshold_probability_quality": normalized_threshold_probability_quality,
        "calibration_freshness_status": normalized_calibration_freshness_status,
        "calibration_gate_status": calibration_gate_status,
        "calibration_gate_reason_codes": list(calibration_gate_reason_codes),
        "calibration_impacted_market": calibration_impacted_market,
        "sample_count": normalized_sample_count,
        "calibration_multiplier": calibration_multiplier_value,
        "bias_quality_multiplier": bias_quality_multiplier,
        "threshold_probability_multiplier": threshold_probability_multiplier,
        "regime_stability_multiplier": regime_stability_multiplier,
        "freshness_multiplier": freshness_multiplier,
        "mapping_multiplier": mapping_multiplier,
        "market_quality_multiplier": market_quality_multiplier,
        "uncertainty_multiplier": uncertainty_multiplier,
        "uncertainty_penalty_bps": uncertainty_penalty_bps,
        "ranking_penalty_reasons": ranking_penalty_reasons,
        "base_expected_value_score": base_expected_value_score,
        "base_expected_pnl_score": base_expected_pnl_score,
        "expected_pnl_score": expected_pnl_score,
        "expected_value_score": expected_value_score,
        "fees_bps": normalized_fees_bps,
        "fill_probability": fill_probability,
        "liquidity_penalty_bps": liquidity_penalty_bps,
        "live_prereq_status": live_prereq_status,
        "market_id": market_id,
        "market_quality_reason_codes": list(market_quality.market_quality_reason_codes),
        "market_quality_status": market_quality.market_quality_status,
        "mapping_confidence": max(0.0, min(1.0, float(mapping_confidence))),
        "model_side": model_side,
        "model_fair_value": normalized_model_fair_value,
        "ops_readiness_score": ops_readiness_score,
        "price_staleness_ms": max(0, int(price_staleness_ms)),
        "pre_feedback_ranking_score": pre_feedback_ranking_score,
        "feedback_penalty": feedback_penalty,
        "feedback_status": feedback_status,
        "cohort_prior_version": cohort_prior_version,
        "recommended_size": recommended_size,
        "allocation_status": allocation_status,
        "budget_impact": dict(budget_impact or {}) if budget_impact is not None else None,
        "allocation_decision_id": allocation_decision_id,
        "policy_id": policy_id,
        "policy_version": policy_version,
        "base_ranking_score": output_base_ranking_score,
        "ranking_v2_base_score": ranking_v2.ranking_score,
        "deployable_expected_pnl": deployable_expected_pnl,
        "deployable_notional": deployable_notional,
        "max_deployable_size": max_deployable_size,
        "capital_scarcity_penalty": capital_scarcity_penalty,
        "concentration_penalty": concentration_penalty,
        "pre_budget_deployable_size": pre_budget_deployable_size,
        "pre_budget_deployable_notional": pre_budget_deployable_notional,
        "pre_budget_deployable_expected_pnl": pre_budget_deployable_expected_pnl,
        "preview_binding_limit_scope": preview_binding_limit_scope,
        "preview_binding_limit_key": preview_binding_limit_key,
        "requested_size": preview_requested_size,
        "requested_notional": preview_requested_notional,
        "rerank_position": rerank_position,
        "rerank_reason_codes": list(rerank_reason_codes or []),
        "capital_policy_id": capital_policy_id,
        "capital_policy_version": capital_policy_version,
        "capital_scaling_reason_codes": list(capital_scaling_reason_codes or []),
        "ranking_score": output_ranking_score,
        "expected_dollar_pnl": ranking_v2.expected_dollar_pnl,
        "capture_probability": ranking_v2.capture_probability,
        "risk_penalty": ranking_v2.risk_penalty,
        "capital_efficiency": ranking_v2.capital_efficiency,
        "why_ranked_json": why_ranked_json,
        "reference_price": normalized_reference_price,
        "source_freshness_status": source_freshness_status,
        "spread_bps": market_quality.spread_bps,
        "slippage_bps": slippage_bps,
        "threshold_bps": normalized_threshold_bps,
        "regime_stability_score": regime_stability_score,
    }
    if source_context:
        context.update(source_context)
    if forecast_distribution_summary_v2:
        context["distribution_summary_v2"] = dict(forecast_distribution_summary_v2)
    context.update(
        {
            "actionability_status": actionability_status,
            "best_side": opportunity_side,
            "calibration_health_status": normalized_calibration_health_status,
            "calibration_bias_quality": normalized_bias_quality,
            "threshold_probability_quality": normalized_threshold_probability_quality,
            "calibration_freshness_status": normalized_calibration_freshness_status,
            "calibration_gate_status": calibration_gate_status,
            "calibration_gate_reason_codes": list(calibration_gate_reason_codes),
            "calibration_impacted_market": calibration_impacted_market,
            "sample_count": normalized_sample_count,
            "calibration_multiplier": calibration_multiplier_value,
            "bias_quality_multiplier": bias_quality_multiplier,
            "threshold_probability_multiplier": threshold_probability_multiplier,
            "regime_stability_multiplier": regime_stability_multiplier,
            "freshness_multiplier": freshness_multiplier,
            "mapping_multiplier": mapping_multiplier,
            "market_quality_multiplier": market_quality_multiplier,
            "uncertainty_multiplier": uncertainty_multiplier,
            "uncertainty_penalty_bps": uncertainty_penalty_bps,
            "ranking_penalty_reasons": ranking_penalty_reasons,
            "expected_pnl_score": expected_pnl_score,
            "expected_value_score": expected_value_score,
            "pre_feedback_ranking_score": pre_feedback_ranking_score,
            "feedback_penalty": feedback_penalty,
            "feedback_status": feedback_status,
            "cohort_prior_version": cohort_prior_version,
            "recommended_size": recommended_size,
            "allocation_status": allocation_status,
            "budget_impact": dict(budget_impact or {}) if budget_impact is not None else None,
            "allocation_decision_id": allocation_decision_id,
            "policy_id": policy_id,
            "policy_version": policy_version,
            "base_ranking_score": output_base_ranking_score,
            "ranking_v2_base_score": ranking_v2.ranking_score,
            "deployable_expected_pnl": deployable_expected_pnl,
            "deployable_notional": deployable_notional,
            "max_deployable_size": max_deployable_size,
            "capital_scarcity_penalty": capital_scarcity_penalty,
            "concentration_penalty": concentration_penalty,
            "pre_budget_deployable_size": pre_budget_deployable_size,
            "pre_budget_deployable_notional": pre_budget_deployable_notional,
            "pre_budget_deployable_expected_pnl": pre_budget_deployable_expected_pnl,
            "preview_binding_limit_scope": preview_binding_limit_scope,
            "preview_binding_limit_key": preview_binding_limit_key,
            "requested_size": preview_requested_size,
            "requested_notional": preview_requested_notional,
            "rerank_position": rerank_position,
            "rerank_reason_codes": list(rerank_reason_codes or []),
            "capital_policy_id": capital_policy_id,
            "capital_policy_version": capital_policy_version,
            "capital_scaling_reason_codes": list(capital_scaling_reason_codes or []),
            "ranking_score": output_ranking_score,
            "expected_dollar_pnl": ranking_v2.expected_dollar_pnl,
            "capture_probability": ranking_v2.capture_probability,
            "risk_penalty": ranking_v2.risk_penalty,
            "capital_efficiency": ranking_v2.capital_efficiency,
            "why_ranked_json": why_ranked_json,
            "regime_stability_score": regime_stability_score,
        }
    )
    context.update(execution_prior_context_fields(resolved_execution_prior_summary))
    context.update(execution_intelligence_context_fields(resolved_execution_intelligence_summary))

    assessment_id = stable_object_id(
        "opp",
        {
            "market_id": market_id,
            "token_id": token_id,
            "outcome": outcome,
            "reference_price": normalized_reference_price,
            "model_fair_value": normalized_model_fair_value,
            "fees_bps": normalized_fees_bps,
            "slippage_bps": slippage_bps,
            "liquidity_penalty_bps": liquidity_penalty_bps,
            "agent_review_status": agent_review_status,
            "live_prereq_status": live_prereq_status,
            "mapping_confidence": round(float(mapping_confidence), 4),
            "source_freshness_status": source_freshness_status,
            "price_staleness_ms": max(0, int(price_staleness_ms)),
            "calibration_health_status": normalized_calibration_health_status,
            "calibration_bias_quality": normalized_bias_quality,
            "threshold_probability_quality": normalized_threshold_probability_quality,
            "sample_count": normalized_sample_count,
        },
    )
    return OpportunityAssessment(
        assessment_id=assessment_id,
        market_id=market_id,
        token_id=token_id,
        outcome=outcome,
        reference_price=normalized_reference_price,
        model_fair_value=normalized_model_fair_value,
        execution_adjusted_fair_value=execution_adjusted_fair_value,
        fees_bps=normalized_fees_bps,
        slippage_bps=slippage_bps,
        fill_probability=fill_probability,
        depth_proxy=depth_proxy,
        liquidity_penalty_bps=liquidity_penalty_bps,
        edge_bps_model=edge_bps_model,
        edge_bps_executable=edge_bps_executable,
        confidence_score=normalized_confidence_score,
        calibration_health_status=normalized_calibration_health_status,
        calibration_bias_quality=normalized_bias_quality,
        threshold_probability_quality=normalized_threshold_probability_quality,
        sample_count=normalized_sample_count,
        uncertainty_multiplier=uncertainty_multiplier,
        uncertainty_penalty_bps=uncertainty_penalty_bps,
        ranking_penalty_reasons=ranking_penalty_reasons,
        ops_readiness_score=ops_readiness_score,
        expected_value_score=expected_value_score,
        expected_pnl_score=expected_pnl_score,
        expected_dollar_pnl=ranking_v2.expected_dollar_pnl,
        capture_probability=ranking_v2.capture_probability,
        risk_penalty=ranking_v2.risk_penalty,
        capital_efficiency=ranking_v2.capital_efficiency,
        base_ranking_score=output_base_ranking_score,
        deployable_expected_pnl=deployable_expected_pnl,
        deployable_notional=deployable_notional,
        max_deployable_size=max_deployable_size,
        capital_scarcity_penalty=capital_scarcity_penalty,
        concentration_penalty=concentration_penalty,
        pre_budget_deployable_size=pre_budget_deployable_size,
        pre_budget_deployable_notional=pre_budget_deployable_notional,
        pre_budget_deployable_expected_pnl=pre_budget_deployable_expected_pnl,
        rerank_position=rerank_position,
        rerank_reason_codes=list(rerank_reason_codes or []),
        execution_prior_key=context.get("execution_prior_key"),
        why_ranked_json=why_ranked_json,
        feedback_penalty=feedback_penalty,
        feedback_status=feedback_status,
        cohort_prior_version=cohort_prior_version,
        recommended_size=recommended_size,
        allocation_status=allocation_status,
        budget_impact=dict(budget_impact or {}) if budget_impact is not None else None,
        ranking_score=output_ranking_score,
        actionability_status=actionability_status,
        calibration_gate_status=calibration_gate_status,
        calibration_gate_reason_codes=list(calibration_gate_reason_codes),
        calibration_impacted_market=calibration_impacted_market,
        capital_policy_id=capital_policy_id,
        capital_policy_version=capital_policy_version,
        capital_scaling_reason_codes=list(capital_scaling_reason_codes or []),
        regime_bucket=source_context_value(source_context, "regime_bucket"),
        rationale=rationale,
        assessment_context_json=context,
    )


def build_market_quality_assessment(
    *,
    market_id: str,
    accepting_orders: bool,
    enable_order_book: bool | None,
    reference_price: float,
    mapping_confidence: float,
    price_staleness_ms: int,
    source_freshness_status: str,
    depth_proxy: float,
    spread_bps: int | None = None,
) -> MarketQualityAssessment:
    normalized_mapping_confidence = max(0.0, min(1.0, float(mapping_confidence)))
    normalized_staleness = max(0, int(price_staleness_ms))
    normalized_freshness = source_freshness_status if source_freshness_status in {"fresh", "stale", "degraded", "missing"} else "degraded"
    effective_spread_bps = max(0, int(spread_bps if spread_bps is not None else _spread_bps(enable_order_book=enable_order_book, reference_price=reference_price)))
    reasons: list[str] = []
    status = "pass"

    if not accepting_orders:
        reasons.append("accepting_orders_false")
        status = "blocked"
    if normalized_freshness == "missing":
        reasons.append("source_missing")
        status = "blocked"
    elif normalized_freshness == "degraded" and status != "blocked":
        reasons.append("source_degraded")
        status = "review_required"
    elif normalized_freshness == "stale" and status == "pass":
        reasons.append("source_stale")
        status = "review_required"

    if normalized_mapping_confidence < 0.35:
        reasons.append("mapping_confidence_low")
        status = "blocked"
    elif normalized_mapping_confidence < 0.75 and status == "pass":
        reasons.append("mapping_confidence_review")
        status = "review_required"

    if normalized_staleness >= 3_600_000:
        reasons.append("price_staleness_blocked")
        status = "blocked"
    elif normalized_staleness >= 900_000 and status == "pass":
        reasons.append("price_staleness_review")
        status = "review_required"

    if effective_spread_bps >= 200:
        reasons.append("spread_too_wide")
        status = "blocked"
    elif effective_spread_bps >= 100 and status == "pass":
        reasons.append("spread_review")
        status = "review_required"

    if float(depth_proxy) < 0.30 and status == "pass":
        reasons.append("depth_low")
        status = "review_required"

    return MarketQualityAssessment(
        market_id=market_id,
        price_staleness_ms=normalized_staleness,
        source_freshness_status=normalized_freshness,
        mapping_confidence=normalized_mapping_confidence,
        spread_bps=effective_spread_bps,
        depth_proxy=float(depth_proxy),
        market_quality_status=status,
        market_quality_reason_codes=reasons,
    )


def build_source_health_snapshot(
    *,
    market_id: str,
    station_id: str,
    source: str,
    market_updated_at: datetime | None,
    forecast_created_at: datetime | None,
    snapshot_created_at: datetime | None,
    now: datetime | None = None,
) -> SourceHealthSnapshotRecord:
    observed_now = _normalize_datetime(now) or datetime.now(UTC).replace(tzinfo=None)
    latest_market = _normalize_datetime(market_updated_at)
    latest_forecast = _normalize_datetime(forecast_created_at)
    latest_snapshot = _normalize_datetime(snapshot_created_at)
    reason_codes: list[str] = []
    if latest_market is None:
        price_staleness_ms = 0
        reason_codes.append("market_timestamp_missing")
    else:
        price_staleness_ms = max(0, int((observed_now - latest_market).total_seconds() * 1000))
    if latest_forecast is None:
        freshness = "missing"
        reason_codes.append("forecast_missing")
    else:
        age_ms = max(0, int((observed_now - latest_forecast).total_seconds() * 1000))
        if age_ms <= 15 * 60 * 1000:
            freshness = "fresh"
        elif age_ms <= 60 * 60 * 1000:
            freshness = "stale"
            reason_codes.append("forecast_stale")
        else:
            freshness = "degraded"
            reason_codes.append("forecast_degraded")
    if latest_snapshot is None:
        reason_codes.append("snapshot_missing")
    return SourceHealthSnapshotRecord(
        snapshot_id=stable_object_id(
            "shs",
            {
                "market_id": market_id,
                "source": source,
                "forecast_created_at": latest_forecast.isoformat() if latest_forecast is not None else None,
                "snapshot_created_at": latest_snapshot.isoformat() if latest_snapshot is not None else None,
            },
        ),
        market_id=market_id,
        station_id=station_id,
        source=source,
        latest_market_updated_at=latest_market,
        latest_forecast_created_at=latest_forecast,
        latest_snapshot_created_at=latest_snapshot,
        price_staleness_ms=price_staleness_ms,
        source_freshness_status=freshness,
        degraded_reason_codes=reason_codes,
        created_at=observed_now,
    )


def _slippage_bps(*, accepting_orders: bool, enable_order_book: bool | None, reference_price: float) -> int:
    if not accepting_orders:
        return 0
    if bool(enable_order_book) and 0.10 <= reference_price <= 0.90:
        return 40
    return 80


def _liquidity_penalty_bps(*, accepting_orders: bool, enable_order_book: bool | None, reference_price: float) -> int:
    if not accepting_orders:
        return 999_999
    if bool(enable_order_book) and 0.10 <= reference_price <= 0.90:
        return 25
    return 60


def _fill_probability(*, accepting_orders: bool, agent_review_status: str, live_prereq_status: str) -> float:
    if not accepting_orders:
        return 0.0
    if live_prereq_status == "attention_required" or agent_review_status in _REVIEW_FAILURE_STATUSES:
        return 0.25
    if agent_review_status in _REVIEW_REQUIRED_STATUSES:
        return 0.50
    if agent_review_status == "passed" or live_prereq_status == "shadow_aligned":
        return 0.75
    return 0.60


def _depth_proxy(*, accepting_orders: bool, enable_order_book: bool | None, reference_price: float) -> float:
    if not accepting_orders:
        return 0.25
    if bool(enable_order_book) and 0.10 <= reference_price <= 0.90:
        return 0.85
    return 0.55


def _ops_tie_breaker(live_prereq_status: str) -> float:
    if live_prereq_status == "shadow_aligned":
        return 0.001
    if live_prereq_status == "not_started":
        return 0.0005
    return 0.0


def _quality_confidence_multiplier(summary: ExecutionPriorSummary | None) -> float:
    if summary is None:
        return 0.65
    quality_factor = {
        "ready": 1.00,
        "watch": 0.92,
        "sparse": 0.78,
        "missing": 0.65,
        "degraded": 0.70,
    }.get(str(summary.prior_quality_status or "missing"), 0.70)
    sample_count = int(summary.sample_count)
    if sample_count >= 20:
        sample_factor = 1.00
    elif sample_count >= 10:
        sample_factor = 0.95
    elif sample_count >= 5:
        sample_factor = 0.85
    else:
        sample_factor = 0.75
    calibration_bucket = summary.prior_key.calibration_quality_bucket
    freshness_bucket = summary.prior_key.source_freshness_bucket
    calibration_factor = 1.00 if calibration_bucket is None else {
        "healthy": 1.00,
        "watch": 0.93,
        "degraded": 0.78,
        "sparse_or_missing": 0.70,
    }.get(str(calibration_bucket), 0.70)
    freshness_factor = 1.00 if freshness_bucket is None else {
        "fresh": 1.00,
        "stale": 0.90,
        "degraded_or_missing": 0.75,
    }.get(str(freshness_bucket), 0.75)
    return round(max(0.25, min(1.0, quality_factor * sample_factor * calibration_factor * freshness_factor)), 6)


def _ranking_score_v2_decomposition(
    *,
    edge_bps_executable: int,
    fill_probability: float,
    reference_price: float,
    depth_proxy: float,
    side: str | None,
    slippage_bps: int,
    ops_tie_breaker: float,
    execution_prior_summary: ExecutionPriorSummary | None,
    execution_intelligence_summary: ExecutionIntelligenceSummary | None,
) -> RankingScoreV2Decomposition:
    gross_unit_edge = max(abs(int(edge_bps_executable)) / 10_000.0, 0.0)
    prior_mode = "prior_backed" if execution_prior_summary is not None else "fallback_heuristic"
    prior_lookup_mode = execution_prior_summary.prior_lookup_mode if execution_prior_summary is not None else "heuristic_fallback"
    submit_ack_rate = execution_prior_summary.submit_ack_rate if execution_prior_summary is not None else 1.0
    fill_rate = execution_prior_summary.fill_rate if execution_prior_summary is not None else 1.0
    resolution_rate = execution_prior_summary.resolution_rate if execution_prior_summary is not None else 1.0
    cancel_rate = execution_prior_summary.cancel_rate if execution_prior_summary is not None else 0.0
    partial_fill_rate = execution_prior_summary.partial_fill_rate if execution_prior_summary is not None else 0.0
    prior_quality_status = execution_prior_summary.prior_quality_status if execution_prior_summary is not None else "missing"
    prior_key = execution_prior_summary.prior_key if execution_prior_summary is not None else None
    prior_feature_scope = (
        dict(execution_prior_summary.prior_feature_scope or {})
        if execution_prior_summary is not None
        else {}
    )
    sample_count = int(execution_prior_summary.sample_count) if execution_prior_summary is not None else 0
    heuristic_capture_probability = max(0.0, min(1.0, float(fill_probability)))
    empirical_capture_probability = max(
        0.0,
        min(
            1.0,
            float(submit_ack_rate) * float(fill_rate) * float(resolution_rate),
        ),
    )
    if execution_prior_summary is None:
        economics_path = "heuristic_fallback"
        capture_probability = heuristic_capture_probability
    elif str(prior_quality_status) == "ready" and sample_count >= 10:
        economics_path = "empirical_primary"
        capture_probability = empirical_capture_probability
    elif sample_count >= 5 or str(prior_quality_status) in {"watch", "sparse"}:
        economics_path = "blended_empirical"
        capture_probability = max(
            0.0,
            min(
                1.0,
                (0.65 * empirical_capture_probability) + (0.35 * heuristic_capture_probability),
            ),
        )
    else:
        economics_path = "heuristic_fallback"
        capture_probability = heuristic_capture_probability
    expected_dollar_pnl = gross_unit_edge * capture_probability
    prior_p50_slippage = execution_prior_summary.adverse_fill_slippage_bps_p50 if execution_prior_summary is not None else None
    prior_p90_slippage = execution_prior_summary.adverse_fill_slippage_bps_p90 if execution_prior_summary is not None else None
    slippage_penalty = max(
        max(0, int(slippage_bps)) / 10_000.0,
        (float(prior_p50_slippage) / 10_000.0) if prior_p50_slippage is not None else 0.0,
    )
    tail_slippage_penalty = max(
        (
            max(
                float(prior_p90_slippage or 0.0)
                - max(float(prior_p50_slippage or 0.0), float(max(0, int(slippage_bps)))),
                0.0,
            )
            / 10_000.0
        )
        * 0.50,
        0.0,
    )
    submit_latency_penalty = 0.0
    fill_latency_penalty = 0.0
    edge_retention_penalty = 0.0
    if execution_prior_summary is not None:
        submit_latency_penalty = gross_unit_edge * min(max(float(execution_prior_summary.submit_latency_ms_p90 or 0.0), 0.0) / 900_000.0, 0.20)
        fill_latency_penalty = gross_unit_edge * min(max(float(execution_prior_summary.fill_latency_ms_p90 or 0.0), 0.0) / 1_200_000.0, 0.20)
        if (
            execution_prior_summary.realized_edge_retention_bps_p50 is not None
            or execution_prior_summary.realized_edge_retention_bps_p90 is not None
        ):
            retained_p50 = max(float(execution_prior_summary.realized_edge_retention_bps_p50 or 0.0), 0.0) / 10_000.0
            retained_p90 = max(float(execution_prior_summary.realized_edge_retention_bps_p90 or 0.0), 0.0) / 10_000.0
            if gross_unit_edge > 0.0:
                shortfall_ratio_p50 = max(gross_unit_edge - retained_p50, 0.0) / gross_unit_edge
                shortfall_ratio_p90 = max(gross_unit_edge - retained_p90, 0.0) / gross_unit_edge
                edge_retention_penalty = gross_unit_edge * (
                    min(shortfall_ratio_p50, 1.0) * 0.20
                    + min(shortfall_ratio_p90, 1.0) * 0.10
                )
    latency_penalty = submit_latency_penalty + fill_latency_penalty
    cancel_penalty = max(0.0, float(cancel_rate)) * gross_unit_edge * 0.50
    partial_fill_penalty = max(0.0, float(partial_fill_rate)) * gross_unit_edge * 0.25
    microstructure_penalty = 0.0
    microstructure_score = None
    microstructure_reason_codes: list[str] = []
    top_of_book_stability = None
    book_update_intensity = None
    spread_regime = None
    visible_size_shock_flag = None
    book_pressure_side = None
    expected_capture_regime = None
    expected_slippage_regime = None
    if execution_intelligence_summary is not None:
        microstructure_score = round(float(execution_intelligence_summary.execution_intelligence_score), 6)
        microstructure_reason_codes = list(execution_intelligence_summary.reason_codes)
        top_of_book_stability = round(float(execution_intelligence_summary.top_of_book_stability), 6)
        book_update_intensity = round(float(execution_intelligence_summary.book_update_intensity), 6)
        spread_regime = str(execution_intelligence_summary.spread_regime)
        visible_size_shock_flag = bool(execution_intelligence_summary.visible_size_shock_flag)
        book_pressure_side = str(execution_intelligence_summary.book_pressure_side)
        expected_capture_regime = str(execution_intelligence_summary.expected_capture_regime)
        expected_slippage_regime = str(execution_intelligence_summary.expected_slippage_regime)
        microstructure_penalty = gross_unit_edge * max(0.0, 1.0 - float(execution_intelligence_summary.top_of_book_stability)) * 0.25
        if execution_intelligence_summary.visible_size_shock_flag:
            microstructure_penalty += gross_unit_edge * 0.10
        if expected_slippage_regime == "high":
            microstructure_penalty += gross_unit_edge * 0.08
        elif expected_slippage_regime == "medium":
            microstructure_penalty += gross_unit_edge * 0.03
        if expected_capture_regime == "low":
            microstructure_penalty += gross_unit_edge * 0.06
        elif expected_capture_regime == "medium":
            microstructure_penalty += gross_unit_edge * 0.02
    risk_penalty = (
        slippage_penalty
        + tail_slippage_penalty
        + cancel_penalty
        + partial_fill_penalty
        + latency_penalty
        + edge_retention_penalty
        + microstructure_penalty
    )

    unit_capital_cost = max(reference_price if side == "BUY" else (1.0 - reference_price if side == "SELL" else 0.50), 0.05)
    capital_efficiency = max(0.0, float(depth_proxy)) / unit_capital_cost
    quality_confidence_multiplier = _quality_confidence_multiplier(execution_prior_summary)
    economic_score = max(expected_dollar_pnl - risk_penalty, 0.0) * capital_efficiency * quality_confidence_multiplier
    ranking_score = round(economic_score + max(0.0, float(ops_tie_breaker)), 6)
    why_ranked_json = {
        "version": "ranking_v2",
        "mode": prior_mode,
        "execution_prior_key": None if prior_key is None else {
            "prior_key": execution_prior_key_id(prior_key),
            "market_id": prior_key.market_id,
            "strategy_id": prior_key.strategy_id,
            "wallet_id": prior_key.wallet_id,
            "station_id": prior_key.station_id,
            "metric": prior_key.metric,
            "side": prior_key.side,
            "horizon_bucket": prior_key.horizon_bucket,
            "liquidity_bucket": prior_key.liquidity_bucket,
            "market_age_bucket": prior_key.market_age_bucket,
            "hours_to_close_bucket": prior_key.hours_to_close_bucket,
            "calibration_quality_bucket": prior_key.calibration_quality_bucket,
            "source_freshness_bucket": prior_key.source_freshness_bucket,
        },
        "prior_quality_status": prior_quality_status,
        "prior_lookup_mode": prior_lookup_mode,
        "prior_feature_scope": prior_feature_scope,
        "economics_path": economics_path,
        "edge_bps_executable": int(edge_bps_executable),
        "fill_probability_heuristic": round(float(fill_probability), 6),
        "heuristic_capture_probability": round(heuristic_capture_probability, 6),
        "empirical_capture_probability": round(empirical_capture_probability, 6),
        "empirical_sample_count": sample_count,
        "submit_ack_rate": round(float(submit_ack_rate), 6),
        "fill_rate": round(float(fill_rate), 6),
        "resolution_rate": round(float(resolution_rate), 6),
        "capture_probability": round(capture_probability, 6),
        "expected_dollar_pnl": round(expected_dollar_pnl, 6),
        "latency_penalty": round(latency_penalty, 6),
        "tail_slippage_penalty": round(tail_slippage_penalty, 6),
        "edge_retention_penalty": round(edge_retention_penalty, 6),
        "microstructure_penalty": round(microstructure_penalty, 6),
        "risk_penalty": round(risk_penalty, 6),
        "capital_efficiency": round(capital_efficiency, 6),
        "quality_confidence_multiplier": quality_confidence_multiplier,
        "ops_tie_breaker": round(max(0.0, float(ops_tie_breaker)), 6),
        "execution_intelligence_score": microstructure_score,
        "microstructure_reason_codes": microstructure_reason_codes,
        "top_of_book_stability": top_of_book_stability,
        "book_update_intensity": book_update_intensity,
        "spread_regime": spread_regime,
        "visible_size_shock_flag": visible_size_shock_flag,
        "book_pressure_side": book_pressure_side,
        "expected_capture_regime": expected_capture_regime,
        "expected_slippage_regime": expected_slippage_regime,
        "retrospective_baseline_version": "ranking_retro_v1",
        "ranking_score": ranking_score,
    }
    return RankingScoreV2Decomposition(
        expected_dollar_pnl=round(expected_dollar_pnl, 6),
        capture_probability=round(capture_probability, 6),
        risk_penalty=round(risk_penalty, 6),
        capital_efficiency=round(capital_efficiency, 6),
        ops_tie_breaker=round(max(0.0, float(ops_tie_breaker)), 6),
        ranking_score=ranking_score,
        why_ranked_json=why_ranked_json,
    )


def _execution_adjusted_edge(
    *,
    reference_price: float,
    model_fair_value: float,
    total_cost_bps: int,
    model_side: str | None,
) -> tuple[float, int]:
    normalized_cost = max(0, int(total_cost_bps)) / 10_000.0
    if model_side == "BUY":
        execution_adjusted_fair_value = max(0.0, min(1.0, model_fair_value - normalized_cost))
        return execution_adjusted_fair_value, max(int(round((execution_adjusted_fair_value - reference_price) * 10_000)), 0)
    if model_side == "SELL":
        execution_adjusted_fair_value = max(0.0, min(1.0, model_fair_value + normalized_cost))
        return execution_adjusted_fair_value, min(int(round((execution_adjusted_fair_value - reference_price) * 10_000)), 0)
    execution_adjusted_fair_value = max(0.0, min(1.0, model_fair_value))
    return execution_adjusted_fair_value, 0


def _actionability_status(
    *,
    edge_bps_executable: int,
    threshold_bps: int,
    opportunity_side: str | None,
    accepting_orders: bool,
    live_prereq_status: str,
    agent_review_status: str,
    market_quality_status: str,
    calibration_gate_status: str,
) -> str:
    if not accepting_orders or live_prereq_status == "attention_required" or market_quality_status == "blocked":
        return "blocked"
    if opportunity_side is None or abs(int(edge_bps_executable)) <= max(0, int(threshold_bps)):
        return "no_trade"
    if calibration_gate_status == "research_only":
        return "no_trade"
    if calibration_gate_status == "blocked":
        return "blocked"
    if calibration_gate_status == "review_required":
        return "review_required"
    if agent_review_status != "passed" or market_quality_status == "review_required":
        return "review_required"
    return "actionable"


def _calibration_gate_status(
    *,
    calibration_freshness_status: str,
    calibration_health_status: str,
    threshold_probability_quality_status: str,
    sample_count: int,
) -> tuple[str, list[str]]:
    freshness = _normalize_calibration_freshness_status(calibration_freshness_status)
    health = _normalize_calibration_health_status(calibration_health_status)
    threshold_quality = _normalize_quality_status(threshold_probability_quality_status)
    reasons: list[str] = []

    if freshness == "stale":
        reasons.append("calibration_freshness_stale")
    elif freshness == "degraded_or_missing":
        reasons.append("calibration_freshness_degraded_or_missing")

    if health == "degraded":
        reasons.append("calibration_health_degraded")
    elif health in {"insufficient_samples", "limited_samples", "sparse"}:
        reasons.append("calibration_health_sparse")
    elif health == "lookup_missing":
        reasons.append("calibration_health_lookup_missing")

    if threshold_quality == "degraded":
        reasons.append("threshold_probability_quality_degraded")
    elif threshold_quality == "sparse":
        reasons.append("threshold_probability_quality_sparse")
    elif threshold_quality == "lookup_missing":
        reasons.append("threshold_probability_quality_lookup_missing")

    if sample_count < 5:
        reasons.append("calibration_sample_count_low")

    if freshness == "degraded_or_missing" and (
        health in {"degraded", "insufficient_samples", "limited_samples", "sparse", "lookup_missing"}
        or threshold_quality in {"sparse", "lookup_missing"}
        or sample_count < 5
    ):
        return "research_only", _stable_unique(reasons)
    if freshness in {"stale", "degraded_or_missing"}:
        return "review_required", _stable_unique(reasons)
    if health in {"degraded", "insufficient_samples", "limited_samples", "sparse", "lookup_missing"}:
        return "review_required", _stable_unique(reasons)
    if threshold_quality in {"degraded", "sparse", "lookup_missing"}:
        return "review_required", _stable_unique(reasons)
    return "clear", []


def _spread_bps(*, enable_order_book: bool | None, reference_price: float) -> int:
    if bool(enable_order_book) and 0.10 <= reference_price <= 0.90:
        return 35
    return 125


def _normalize_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is not None:
        return value.astimezone(UTC).replace(tzinfo=None)
    return value


def source_context_value(source_context: dict[str, Any] | None, key: str) -> Any:
    if not source_context:
        return None
    return source_context.get(key)


def _stable_unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def _source_context_reason_codes(source_context: dict[str, Any] | None, key: str) -> list[str] | None:
    value = source_context_value(source_context, key)
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    return None


def _normalize_calibration_health_status(value: Any) -> str:
    normalized = str(value or "lookup_missing").strip().lower()
    if normalized in {"healthy", "watch", "degraded", "insufficient_samples", "limited_samples", "lookup_missing", "sparse"}:
        return normalized
    return "lookup_missing"


def _calibration_multiplier(*, calibration_health_status: str, explicit_multiplier: Any, calibration_freshness_status: str) -> float:
    if explicit_multiplier is not None:
        try:
            return _clamp_multiplier(
                float(explicit_multiplier) * _calibration_freshness_multiplier(calibration_freshness_status)
            )
        except (TypeError, ValueError):
            pass
    mapping = {
        "healthy": 1.0,
        "watch": 0.85,
        "degraded": 0.60,
        "limited_samples": 0.75,
        "insufficient_samples": 0.55,
        "lookup_missing": 0.50,
        "sparse": 0.55,
    }
    base = mapping.get(calibration_health_status, 0.50)
    return _clamp_multiplier(base * _calibration_freshness_multiplier(calibration_freshness_status))


def _normalize_calibration_freshness_status(value: Any) -> str:
    normalized = str(value or "fresh").strip().lower()
    if normalized in {"fresh", "stale", "degraded_or_missing"}:
        return normalized
    return "fresh"


def _calibration_freshness_multiplier(value: str) -> float:
    return {
        "fresh": 1.0,
        "stale": 0.92,
        "degraded_or_missing": 0.78,
    }.get(_normalize_calibration_freshness_status(value), 1.0)


def _normalize_quality_status(value: Any) -> str:
    normalized = str(value or "lookup_missing").strip().lower()
    if normalized in {"healthy", "watch", "degraded", "sparse", "lookup_missing"}:
        return normalized
    return "lookup_missing"


def _quality_status_multiplier(
    value: str,
    *,
    healthy: float,
    watch: float,
    degraded: float,
    sparse: float,
    lookup_missing: float,
) -> float:
    return {
        "healthy": healthy,
        "watch": watch,
        "degraded": degraded,
        "sparse": sparse,
        "lookup_missing": lookup_missing,
    }.get(_normalize_quality_status(value), lookup_missing)


def _regime_stability_score(source: Any) -> float:
    if isinstance(source, dict):
        value = source.get("regime_stability_score")
    else:
        value = None
    try:
        return max(0.0, min(1.0, float(value)))
    except (TypeError, ValueError):
        return 0.5


def _regime_stability_multiplier(score: float) -> float:
    normalized = max(0.0, min(1.0, float(score)))
    if normalized >= 0.80:
        return 1.0
    if normalized >= 0.60:
        return 0.85
    return 0.65


def _freshness_multiplier(source_freshness_status: str) -> float:
    return {
        "fresh": 1.0,
        "stale": 0.85,
        "degraded": 0.65,
        "missing": 0.45,
    }.get(str(source_freshness_status), 0.65)


def _mapping_multiplier(mapping_confidence: float) -> float:
    if mapping_confidence >= 0.90:
        return 1.0
    if mapping_confidence >= 0.75:
        return 0.90
    if mapping_confidence >= 0.50:
        return 0.70
    return 0.50


def _market_quality_multiplier(market_quality_status: str) -> float:
    return {
        "pass": 1.0,
        "review_required": 0.80,
        "blocked": 0.0,
    }.get(market_quality_status, 0.80)


def _clamp_multiplier(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _ranking_penalty_reasons(
    *,
    calibration_health_status: str,
    calibration_reason_codes: list[str] | None,
    freshness_status: str,
    mapping_confidence: float,
    market_quality: MarketQualityAssessment,
    calibration_bias_quality: str,
    threshold_probability_quality: str,
    regime_stability_score: float,
) -> list[str]:
    reasons: list[str] = []
    if calibration_reason_codes:
        reasons.extend(str(code) for code in calibration_reason_codes if str(code))
    elif calibration_health_status == "lookup_missing":
        reasons.append("calibration_lookup_missing")
    elif calibration_health_status == "insufficient_samples":
        reasons.append("calibration_insufficient_samples")
    elif calibration_health_status == "limited_samples":
        reasons.append("calibration_limited_samples")
    elif calibration_health_status == "watch":
        reasons.append("calibration_watch")
    elif calibration_health_status == "degraded":
        reasons.append("calibration_degraded")
    elif calibration_health_status == "sparse":
        reasons.append("calibration_sparse")

    if calibration_bias_quality == "watch":
        reasons.append("calibration_bias_watch")
    elif calibration_bias_quality == "degraded":
        reasons.append("calibration_bias_degraded")
    elif calibration_bias_quality == "sparse":
        reasons.append("calibration_bias_sparse")
    elif calibration_bias_quality == "lookup_missing":
        reasons.append("calibration_bias_lookup_missing")

    if threshold_probability_quality == "watch":
        reasons.append("threshold_probability_watch")
    elif threshold_probability_quality == "degraded":
        reasons.append("threshold_probability_degraded")
    elif threshold_probability_quality == "sparse":
        reasons.append("threshold_probability_sparse")
    elif threshold_probability_quality == "lookup_missing":
        reasons.append("threshold_probability_lookup_missing")

    if regime_stability_score < 0.60:
        reasons.append("regime_unstable")
    elif regime_stability_score < 0.80:
        reasons.append("regime_watch")

    if freshness_status == "stale":
        reasons.append("freshness_stale")
    elif freshness_status == "degraded":
        reasons.append("freshness_degraded")
    elif freshness_status == "missing":
        reasons.append("freshness_missing")

    if mapping_confidence < 0.50:
        reasons.append("mapping_confidence_low")
    elif mapping_confidence < 0.75:
        reasons.append("mapping_confidence_reduced")
    elif mapping_confidence < 0.90:
        reasons.append("mapping_confidence_watch")

    reasons.extend(str(code) for code in market_quality.market_quality_reason_codes if str(code))
    seen: set[str] = set()
    deduped: list[str] = []
    for code in reasons:
        if code in seen:
            continue
        seen.add(code)
        deduped.append(code)
    return deduped


__all__ = [
    "build_market_quality_assessment",
    "build_source_health_snapshot",
    "build_weather_opportunity_assessment",
    "derive_opportunity_side",
]
