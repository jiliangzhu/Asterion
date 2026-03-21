from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict
from datetime import UTC, datetime
from decimal import Decimal, ROUND_DOWN
from typing import Any

from asterion_core.contracts import (
    AllocationDecision,
    BalanceType,
    CapitalAllocationRun,
    ExposureSnapshot,
    InventoryPosition,
    PositionLimitCheck,
    StrategyDecision,
    stable_object_id,
)
from asterion_core.storage.os_queue import enqueue_upsert_rows_v1
from asterion_core.storage.utils import safe_json_dumps
from asterion_core.storage.write_queue import WriteQueueConfig


TRADING_ALLOCATION_POLICY_COLUMNS = [
    "policy_id",
    "wallet_id",
    "strategy_id",
    "status",
    "policy_version",
    "max_buy_notional_per_run",
    "max_buy_notional_per_ticket",
    "min_recommended_size",
    "size_rounding_increment",
    "created_at",
    "updated_at",
]

TRADING_POSITION_LIMIT_POLICY_COLUMNS = [
    "limit_id",
    "policy_id",
    "wallet_id",
    "limit_scope",
    "scope_key",
    "max_gross_notional",
    "max_position_quantity",
    "status",
    "created_at",
    "updated_at",
]

TRADING_CAPITAL_BUDGET_POLICY_COLUMNS = [
    "capital_policy_id",
    "wallet_id",
    "strategy_id",
    "regime_bucket",
    "calibration_gate_status",
    "status",
    "policy_version",
    "max_buy_notional_per_run",
    "max_buy_notional_per_ticket",
    "max_open_markets",
    "max_same_station_markets",
    "min_recommended_size",
    "created_at",
    "updated_at",
]

RUNTIME_CAPITAL_ALLOCATION_RUN_COLUMNS = [
    "allocation_run_id",
    "run_id",
    "wallet_id",
    "strategy_id",
    "source_kind",
    "requested_decision_count",
    "decision_count",
    "approved_count",
    "resized_count",
    "blocked_count",
    "policy_missing_count",
    "requested_buy_notional_total",
    "recommended_buy_notional_total",
    "created_at",
]

RUNTIME_ALLOCATION_DECISION_COLUMNS = [
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
    "ranking_score",
    "base_ranking_score",
    "deployable_expected_pnl",
    "deployable_notional",
    "max_deployable_size",
    "capital_scarcity_penalty",
    "concentration_penalty",
    "pre_budget_deployable_size",
    "pre_budget_deployable_notional",
    "pre_budget_deployable_expected_pnl",
    "rerank_position",
    "rerank_reason_codes_json",
    "requested_size",
    "recommended_size",
    "requested_notional",
    "recommended_notional",
    "allocation_status",
    "reason_codes_json",
    "budget_impact_json",
    "policy_id",
    "policy_version",
    "capital_policy_id",
    "capital_policy_version",
    "source_kind",
    "binding_limit_scope",
    "binding_limit_key",
    "regime_bucket",
    "calibration_gate_status",
    "capital_scaling_reason_codes_json",
    "created_at",
]

RUNTIME_POSITION_LIMIT_CHECK_COLUMNS = [
    "check_id",
    "allocation_decision_id",
    "limit_id",
    "limit_scope",
    "scope_key",
    "observed_gross_notional",
    "candidate_gross_notional",
    "remaining_capacity",
    "check_status",
    "created_at",
]

_ACTIVE_POLICY_STATUSES = {"active"}
_OPEN_RESERVATION_STATUSES = {"open", "partially_consumed"}
_ALLOCATION_REASON_ORDER = {
    "policy_missing": 0,
    "buy_budget_exhausted": 1,
    "per_ticket_budget_cap": 2,
    "inventory_constrained": 3,
    "market_limit_exceeded": 4,
    "station_limit_exceeded": 5,
    "below_min_recommended_size": 6,
    "calibration_gate_review_required": 7,
    "calibration_gate_research_only": 8,
    "capital_open_markets_cap": 9,
    "capital_same_station_markets_cap": 10,
}


def build_market_station_map(con, *, market_ids: list[str] | None = None) -> dict[str, str]:
    if not _table_exists(con, "weather.weather_market_specs"):
        return {}
    if market_ids:
        placeholders = ", ".join(["?"] * len(market_ids))
        rows = con.execute(
            f"""
            SELECT market_id, station_id
            FROM weather.weather_market_specs
            WHERE market_id IN ({placeholders})
            """,
            [str(item) for item in market_ids],
        ).fetchall()
    else:
        rows = con.execute("SELECT market_id, station_id FROM weather.weather_market_specs").fetchall()
    return {
        str(row[0]): str(row[1])
        for row in rows
        if row[0] is not None and row[1] is not None and str(row[1]).strip()
    }


def materialize_capital_allocation(
    con,
    *,
    decisions: list[StrategyDecision],
    wallet_id: str,
    run_id: str,
    source_kind: str,
    current_inventory_positions: list[InventoryPosition] | None = None,
    market_station_map: dict[str, str] | None = None,
    created_at: datetime | None = None,
) -> tuple[CapitalAllocationRun, list[AllocationDecision], list[PositionLimitCheck]]:
    timestamp = _normalize_datetime(created_at)
    canonical_decisions = _canonicalize_allocator_decisions(decisions)
    market_station_lookup = dict(market_station_map or {})
    if not market_station_lookup:
        market_station_lookup = build_market_station_map(
            con,
            market_ids=[str(item.market_id) for item in canonical_decisions if item.market_id],
        )
    inventory_positions = list(current_inventory_positions or load_inventory_positions_for_allocator(con, wallet_id=wallet_id))
    exact_policies = _load_active_allocation_policies(con, wallet_id=wallet_id)
    capital_budget_policies = _load_active_capital_budget_policies(con, wallet_id=wallet_id)
    default_policy = _load_default_allocation_policy(exact_policies)
    policies_by_strategy = {
        str(item["strategy_id"]): item
        for item in exact_policies
        if item.get("strategy_id") is not None
    }

    policy_remaining_budget: dict[str, Decimal] = {}
    policy_id_to_version: dict[str, str] = {}
    position_limits_by_policy: dict[str, list[dict[str, Any]]] = {}
    capital_remaining_budget: dict[str, Decimal] = {}
    capital_policy_id_to_version: dict[str, str] = {}
    for item in exact_policies:
        policy_id = str(item["policy_id"])
        policy_remaining_budget[policy_id] = _decimal(item["max_buy_notional_per_run"])
        policy_id_to_version[policy_id] = str(item["policy_version"])
        position_limits_by_policy[policy_id] = _load_active_position_limit_policies(
            con,
            wallet_id=wallet_id,
            policy_id=policy_id,
        )
    for item in capital_budget_policies:
        capital_policy_id = str(item["capital_policy_id"])
        capital_remaining_budget[capital_policy_id] = _decimal(item["max_buy_notional_per_run"])
        capital_policy_id_to_version[capital_policy_id] = str(item["policy_version"])

    available_by_key = _build_available_balances(inventory_positions)
    exposure_basis = _load_current_exposure_basis(
        con,
        wallet_id=wallet_id,
        market_station_map=market_station_lookup,
        reference_price_by_market={str(item.market_id): item.reference_price for item in canonical_decisions},
        fallback_positions=inventory_positions,
    )
    observed_notional_by_scope = defaultdict(Decimal, exposure_basis["notional_by_scope"])
    observed_quantity_by_scope = defaultdict(Decimal, exposure_basis["quantity_by_scope"])
    observed_open_markets = set(exposure_basis["open_markets"])
    observed_station_market_sets = {
        key: set(value)
        for key, value in exposure_basis["station_market_sets"].items()
    }

    allocation_run_id = stable_object_id(
        "allocrun",
        {
            "run_id": run_id,
            "wallet_id": wallet_id,
            "source_kind": source_kind,
        },
    )
    allocation_decisions: list[AllocationDecision] = []
    position_limit_checks: list[PositionLimitCheck] = []
    requested_buy_notional_total = Decimal("0")
    recommended_buy_notional_total = Decimal("0")

    preview_by_decision_id: dict[str, dict[str, Any]] = {}
    for decision in canonical_decisions:
        preview = _build_structural_preview(
            decision=decision,
            wallet_id=wallet_id,
            policies_by_strategy=policies_by_strategy,
            default_policy=default_policy,
            capital_budget_policies=capital_budget_policies,
            position_limits_by_policy=position_limits_by_policy,
            market_station_lookup=market_station_lookup,
            available_by_key=available_by_key,
            observed_notional_by_scope=observed_notional_by_scope,
            observed_quantity_by_scope=observed_quantity_by_scope,
            observed_open_markets=observed_open_markets,
            observed_station_market_sets=observed_station_market_sets,
            allocation_run_id=allocation_run_id,
            created_at=timestamp,
        )
        preview_by_decision_id[decision.decision_id] = preview
        if preview["side"] == "buy":
            requested_buy_notional_total += _decimal(preview["requested_notional"])

    reranked_decisions = sorted(
        canonical_decisions,
        key=lambda item: (
            -float(preview_by_decision_id[item.decision_id]["pre_budget_deployable_expected_pnl"]),
            -float(preview_by_decision_id[item.decision_id]["base_ranking_score"]),
            int(item.decision_rank),
            str(item.decision_id),
        ),
    )
    for index, decision in enumerate(reranked_decisions, start=1):
        preview = preview_by_decision_id[decision.decision_id]
        rerank_reason_codes = list(preview["reason_codes"])
        if index != int(decision.decision_rank):
            rerank_reason_codes.insert(0, "reranked_vs_base_order")
        preview_scope = preview.get("preview_binding_limit_scope")
        if preview_scope:
            rerank_reason_codes.append(f"pre_budget_binding_limit:{preview_scope}")
        if float(preview["pre_budget_deployable_size"]) < float(preview["requested_size"]):
            rerank_reason_codes.append("pre_budget_structural_resize")
        preview["rerank_position"] = index
        preview["rerank_reason_codes"] = tuple(_stable_unique(rerank_reason_codes))

    for decision in reranked_decisions:
        preview = preview_by_decision_id[decision.decision_id]
        base_ranking_score = _decimal(preview["base_ranking_score"])
        unit_expected_dollar_pnl = _decimal(preview["unit_expected_dollar_pnl"])
        requested_size = _decimal(preview["requested_size"])
        reference_price = _decimal(preview["reference_price"])
        requested_notional = _decimal(preview["requested_notional"])
        scope_station = preview["station_id"]
        policy = preview["policy"]
        capital_policy = preview["capital_policy"]
        effective_policy_id = (
            str(capital_policy["capital_policy_id"])
            if capital_policy is not None
            else str(policy["policy_id"]) if policy is not None else None
        )

        if policy is None:
            allocation_decisions.append(
                AllocationDecision(
                    allocation_decision_id=stable_object_id(
                        "allocdec",
                        {
                            "allocation_run_id": allocation_run_id,
                            "decision_id": decision.decision_id,
                        },
                    ),
                    allocation_run_id=allocation_run_id,
                    run_id=run_id,
                    decision_id=decision.decision_id,
                    watch_snapshot_id=decision.watch_snapshot_id,
                    wallet_id=wallet_id,
                    strategy_id=decision.strategy_id,
                    market_id=decision.market_id,
                    token_id=decision.token_id,
                    side=str(decision.side).lower(),
                    ranking_score=0.0,
                    base_ranking_score=float(base_ranking_score),
                    deployable_expected_pnl=0.0,
                    deployable_notional=0.0,
                    max_deployable_size=0.0,
                    capital_scarcity_penalty=0.0,
                    concentration_penalty=0.0,
                    requested_size=float(requested_size),
                    recommended_size=0.0,
                    requested_notional=float(requested_notional),
                    recommended_notional=0.0,
                    allocation_status="policy_missing",
                    reason_codes=("policy_missing",),
                    budget_impact={
                        "policy_missing": True,
                        "binding_limit_scope": None,
                        "binding_limit_key": None,
                        "remaining_run_budget": None,
                        "preview": _preview_budget_impact(preview),
                        "rerank_position": preview["rerank_position"],
                        "rerank_reason_codes": list(preview["rerank_reason_codes"]),
                    },
                    policy_id=None,
                    policy_version=None,
                    capital_policy_id=None,
                    capital_policy_version=None,
                    source_kind=source_kind,
                    binding_limit_scope=None,
                    binding_limit_key=None,
                    regime_bucket=preview.get("regime_bucket"),
                    calibration_gate_status=preview.get("calibration_gate_status"),
                    capital_scaling_reason_codes=tuple(preview.get("capital_scaling_reason_codes") or ()),
                    created_at=timestamp,
                    pre_budget_deployable_size=float(preview["pre_budget_deployable_size"]),
                    pre_budget_deployable_notional=float(preview["pre_budget_deployable_notional"]),
                    pre_budget_deployable_expected_pnl=float(preview["pre_budget_deployable_expected_pnl"]),
                    rerank_position=int(preview["rerank_position"]),
                    rerank_reason_codes=tuple(preview["rerank_reason_codes"]),
                )
            )
            continue

        policy_id = str(policy["policy_id"])
        policy_version = str(policy["policy_version"])
        capital_policy_id = str(capital_policy["capital_policy_id"]) if capital_policy is not None else None
        capital_policy_version = str(capital_policy["policy_version"]) if capital_policy is not None else None
        min_recommended_size = max(
            Decimal("0"),
            _decimal(capital_policy["min_recommended_size"]) if capital_policy is not None else _decimal(policy["min_recommended_size"]),
        )
        rounding_increment = max(Decimal("0.00000001"), _decimal(policy["size_rounding_increment"]))
        remaining_run_budget = (
            capital_remaining_budget.get(capital_policy_id, Decimal("0"))
            if capital_policy_id is not None
            else policy_remaining_budget.get(policy_id, Decimal("0"))
        )
        candidate_size, binding_limit_scope, binding_limit_key, concentration_penalty, ordered_reason_codes, limit_checks = _apply_structural_constraints(
            decision=decision,
            wallet_id=wallet_id,
            station_id=scope_station,
            policy=policy,
            capital_policy=capital_policy,
            limit_policies=position_limits_by_policy.get(policy_id, []),
            available_by_key=available_by_key,
            observed_notional_by_scope=observed_notional_by_scope,
            observed_quantity_by_scope=observed_quantity_by_scope,
            observed_open_markets=observed_open_markets,
            observed_station_market_sets=observed_station_market_sets,
            allocation_run_id=allocation_run_id,
            created_at=timestamp,
            min_recommended_size=min_recommended_size,
            rounding_increment=rounding_increment,
        )
        position_limit_checks.extend(limit_checks)
        max_deployable_size = candidate_size
        capital_scarcity_penalty = Decimal("0")
        reason_codes = list(ordered_reason_codes)
        capital_scaling_reason_codes = list(preview.get("capital_scaling_reason_codes") or ())

        calibration_gate_status = str(preview.get("calibration_gate_status") or "clear")
        if calibration_gate_status in {"review_required", "research_only", "blocked"}:
            candidate_size = Decimal("0")
            if calibration_gate_status == "review_required":
                capital_scaling_reason_codes.append("calibration_gate_review_required")
            else:
                capital_scaling_reason_codes.append("calibration_gate_research_only")

        if str(decision.side).lower() == "buy":
            run_budget_size = (
                _round_down_size(remaining_run_budget / reference_price, rounding_increment)
                if reference_price > Decimal("0")
                else Decimal("0")
            )
            if run_budget_size < candidate_size:
                candidate_size = run_budget_size
                reason_codes.append("buy_budget_exhausted")
                if max_deployable_size > Decimal("0"):
                    capital_scarcity_penalty = min(
                        (max_deployable_size - candidate_size) / max_deployable_size,
                        Decimal("1"),
                    )
                if binding_limit_scope is None:
                    binding_limit_scope = "run_budget"
                    binding_limit_key = effective_policy_id

        candidate_size = _round_down_size(candidate_size, rounding_increment)
        if candidate_size < min_recommended_size:
            candidate_size = Decimal("0")
            if "below_min_recommended_size" not in reason_codes:
                reason_codes.append("below_min_recommended_size")

        recommended_notional = candidate_size * reference_price
        deployable_expected_pnl = unit_expected_dollar_pnl * candidate_size
        deployable_ranking_score = deployable_expected_pnl
        if str(decision.side).lower() == "buy" and candidate_size > Decimal("0"):
            if capital_policy_id is not None:
                capital_remaining_budget[capital_policy_id] = max(Decimal("0"), remaining_run_budget - recommended_notional)
            else:
                policy_remaining_budget[policy_id] = max(Decimal("0"), remaining_run_budget - recommended_notional)
            available_by_key[("usdc_e", "_cash_", "_cash_")] = max(
                Decimal("0"),
                available_by_key.get(("usdc_e", "_cash_", "_cash_"), Decimal("0")) - recommended_notional,
            )
            recommended_buy_notional_total += recommended_notional
        elif candidate_size > Decimal("0"):
            token_key = ("outcome_token", str(decision.token_id), str(decision.market_id), str(decision.outcome))
            available_by_key[token_key] = max(Decimal("0"), available_by_key.get(token_key, Decimal("0")) - candidate_size)

        if candidate_size > Decimal("0"):
            market_scope = f"market:{decision.market_id}"
            observed_notional_by_scope[market_scope] += recommended_notional
            observed_quantity_by_scope[market_scope] += candidate_size
            observed_open_markets.add(str(decision.market_id))
            if scope_station:
                station_scope = f"station:{scope_station}"
                observed_notional_by_scope[station_scope] += recommended_notional
                observed_quantity_by_scope[station_scope] += candidate_size
                observed_station_market_sets.setdefault(str(scope_station), set()).add(str(decision.market_id))

        allocation_status = (
            "blocked"
            if candidate_size <= Decimal("0")
            else "approved"
            if candidate_size == requested_size
            else "resized"
        )
        ordered_reason_codes = tuple(sorted(set(reason_codes), key=lambda item: _ALLOCATION_REASON_ORDER.get(item, 99)))
        allocation_decisions.append(
            AllocationDecision(
                allocation_decision_id=stable_object_id(
                    "allocdec",
                    {
                        "allocation_run_id": allocation_run_id,
                        "decision_id": decision.decision_id,
                    },
                ),
                allocation_run_id=allocation_run_id,
                run_id=run_id,
                decision_id=decision.decision_id,
                watch_snapshot_id=decision.watch_snapshot_id,
                wallet_id=wallet_id,
                strategy_id=decision.strategy_id,
                market_id=decision.market_id,
                token_id=decision.token_id,
                side=str(decision.side).lower(),
                ranking_score=float(deployable_ranking_score),
                base_ranking_score=float(base_ranking_score),
                deployable_expected_pnl=float(deployable_expected_pnl),
                deployable_notional=float(recommended_notional),
                max_deployable_size=float(max_deployable_size),
                capital_scarcity_penalty=float(capital_scarcity_penalty),
                concentration_penalty=float(concentration_penalty),
                requested_size=float(requested_size),
                recommended_size=float(candidate_size),
                requested_notional=float(requested_notional),
                recommended_notional=float(recommended_notional),
                allocation_status=allocation_status,
                reason_codes=ordered_reason_codes,
                budget_impact={
                    "binding_limit_scope": binding_limit_scope,
                    "binding_limit_key": binding_limit_key,
                    "remaining_run_budget": float(
                        capital_remaining_budget.get(capital_policy_id, Decimal("0"))
                        if capital_policy_id is not None
                        else policy_remaining_budget.get(policy_id, Decimal("0"))
                    )
                    if str(decision.side).lower() == "buy"
                    else None,
                    "policy_missing": False,
                    "requested_notional": float(requested_notional),
                    "recommended_notional": float(recommended_notional),
                    "base_ranking_score": float(base_ranking_score),
                    "deployable_expected_pnl": float(deployable_expected_pnl),
                    "deployable_notional": float(recommended_notional),
                    "max_deployable_size": float(max_deployable_size),
                    "capital_scarcity_penalty": float(capital_scarcity_penalty),
                    "concentration_penalty": float(concentration_penalty),
                    "capital_policy_id": capital_policy_id,
                    "capital_policy_version": capital_policy_version,
                    "regime_bucket": preview.get("regime_bucket"),
                    "calibration_gate_status": calibration_gate_status,
                    "capital_scaling_reason_codes": _stable_unique(capital_scaling_reason_codes),
                    "preview": _preview_budget_impact(preview),
                    "rerank_position": preview["rerank_position"],
                    "rerank_reason_codes": list(preview["rerank_reason_codes"]),
                },
                policy_id=policy_id,
                policy_version=policy_version,
                capital_policy_id=capital_policy_id,
                capital_policy_version=capital_policy_version,
                source_kind=source_kind,
                binding_limit_scope=binding_limit_scope,
                binding_limit_key=binding_limit_key,
                regime_bucket=preview.get("regime_bucket"),
                calibration_gate_status=calibration_gate_status,
                capital_scaling_reason_codes=tuple(_stable_unique(capital_scaling_reason_codes)),
                created_at=timestamp,
                pre_budget_deployable_size=float(preview["pre_budget_deployable_size"]),
                pre_budget_deployable_notional=float(preview["pre_budget_deployable_notional"]),
                pre_budget_deployable_expected_pnl=float(preview["pre_budget_deployable_expected_pnl"]),
                rerank_position=int(preview["rerank_position"]),
                rerank_reason_codes=tuple(preview["rerank_reason_codes"]),
            )
        )

    strategy_ids = {item.strategy_id for item in canonical_decisions}
    allocation_run = CapitalAllocationRun(
        allocation_run_id=allocation_run_id,
        run_id=run_id,
        wallet_id=wallet_id,
        strategy_id=next(iter(strategy_ids)) if len(strategy_ids) == 1 else None,
        source_kind=source_kind,
        requested_decision_count=len(canonical_decisions),
        decision_count=len(allocation_decisions),
        approved_count=sum(1 for item in allocation_decisions if item.allocation_status == "approved"),
        resized_count=sum(1 for item in allocation_decisions if item.allocation_status == "resized"),
        blocked_count=sum(1 for item in allocation_decisions if item.allocation_status == "blocked"),
        policy_missing_count=sum(1 for item in allocation_decisions if item.allocation_status == "policy_missing"),
        requested_buy_notional_total=float(requested_buy_notional_total),
        recommended_buy_notional_total=float(recommended_buy_notional_total),
        created_at=timestamp,
    )
    return allocation_run, allocation_decisions, position_limit_checks


def _build_structural_preview(
    *,
    decision: StrategyDecision,
    wallet_id: str,
    policies_by_strategy: dict[str, dict[str, Any]],
    default_policy: dict[str, Any] | None,
    capital_budget_policies: list[dict[str, Any]],
    position_limits_by_policy: dict[str, list[dict[str, Any]]],
    market_station_lookup: dict[str, str],
    available_by_key: dict[tuple[str, str, str, str], Decimal],
    observed_notional_by_scope: dict[str, Decimal],
    observed_quantity_by_scope: dict[str, Decimal],
    observed_open_markets: set[str],
    observed_station_market_sets: dict[str, set[str]],
    allocation_run_id: str,
    created_at: datetime,
) -> dict[str, Any]:
    side = str(decision.side).lower()
    base_ranking_score = _decimal(decision.pricing_context_json.get("ranking_score") or 0)
    unit_expected_dollar_pnl = _decimal(
        decision.pricing_context_json.get("expected_dollar_pnl")
        or decision.pricing_context_json.get("ranking_score")
        or 0
    )
    requested_size = _decimal(decision.size)
    reference_price = _decimal(decision.reference_price)
    requested_notional = requested_size * reference_price
    scope_station = market_station_lookup.get(str(decision.market_id))
    regime_bucket = _coerce_optional_text(decision.pricing_context_json.get("regime_bucket"))
    calibration_gate_status = _coerce_optional_text(decision.pricing_context_json.get("calibration_gate_status")) or "clear"
    policy = policies_by_strategy.get(str(decision.strategy_id)) or default_policy
    capital_policy = _lookup_capital_budget_policy(
        capital_budget_policies,
        strategy_id=decision.strategy_id,
        regime_bucket=regime_bucket,
        calibration_gate_status=calibration_gate_status,
    )
    if policy is None:
        return {
            "policy": None,
            "capital_policy": capital_policy,
            "policy_id": None,
            "policy_version": None,
            "base_ranking_score": float(base_ranking_score),
            "unit_expected_dollar_pnl": float(unit_expected_dollar_pnl),
            "requested_size": float(requested_size),
            "requested_notional": float(requested_notional),
            "reference_price": float(reference_price),
            "side": side,
            "station_id": scope_station,
            "regime_bucket": regime_bucket,
            "calibration_gate_status": calibration_gate_status,
            "pre_budget_deployable_size": 0.0,
            "pre_budget_deployable_notional": 0.0,
            "pre_budget_deployable_expected_pnl": 0.0,
            "preview_binding_limit_scope": None,
            "preview_binding_limit_key": None,
            "preview_concentration_penalty": 0.0,
            "capital_scaling_reason_codes": (),
            "reason_codes": ("policy_missing",),
        }

    min_recommended_size = max(
        Decimal("0"),
        _decimal(capital_policy["min_recommended_size"]) if capital_policy is not None else _decimal(policy["min_recommended_size"]),
    )
    rounding_increment = max(Decimal("0.00000001"), _decimal(policy["size_rounding_increment"]))
    candidate_size, binding_limit_scope, binding_limit_key, concentration_penalty, reason_codes, _ = _apply_structural_constraints(
        decision=decision,
        wallet_id=wallet_id,
        station_id=scope_station,
        policy=policy,
        capital_policy=capital_policy,
        limit_policies=position_limits_by_policy.get(str(policy["policy_id"]), []),
        available_by_key=available_by_key,
        observed_notional_by_scope=observed_notional_by_scope,
        observed_quantity_by_scope=observed_quantity_by_scope,
        observed_open_markets=observed_open_markets,
        observed_station_market_sets=observed_station_market_sets,
        allocation_run_id=allocation_run_id,
        created_at=created_at,
        min_recommended_size=min_recommended_size,
        rounding_increment=rounding_increment,
        collect_limit_checks=False,
    )
    capital_scaling_reason_codes: list[str] = []
    if calibration_gate_status in {"review_required", "research_only", "blocked"}:
        capital_scaling_reason_codes.append(
            "calibration_gate_review_required"
            if calibration_gate_status == "review_required"
            else "calibration_gate_research_only"
        )
    preview_notional = candidate_size * reference_price
    preview_expected_pnl = unit_expected_dollar_pnl * candidate_size
    return {
        "policy": policy,
        "capital_policy": capital_policy,
        "policy_id": str(policy["policy_id"]),
        "policy_version": str(policy["policy_version"]),
        "base_ranking_score": float(base_ranking_score),
        "unit_expected_dollar_pnl": float(unit_expected_dollar_pnl),
        "requested_size": float(requested_size),
        "requested_notional": float(requested_notional),
        "reference_price": float(reference_price),
        "side": side,
        "station_id": scope_station,
        "regime_bucket": regime_bucket,
        "calibration_gate_status": calibration_gate_status,
        "pre_budget_deployable_size": float(candidate_size),
        "pre_budget_deployable_notional": float(preview_notional),
        "pre_budget_deployable_expected_pnl": float(preview_expected_pnl),
        "preview_binding_limit_scope": binding_limit_scope,
        "preview_binding_limit_key": binding_limit_key,
        "preview_concentration_penalty": float(concentration_penalty),
        "capital_scaling_reason_codes": tuple(capital_scaling_reason_codes),
        "reason_codes": reason_codes,
    }


def _apply_structural_constraints(
    *,
    decision: StrategyDecision,
    wallet_id: str,
    station_id: str | None,
    policy: dict[str, Any],
    capital_policy: dict[str, Any] | None,
    limit_policies: list[dict[str, Any]],
    available_by_key: dict[tuple[str, str, str, str], Decimal],
    observed_notional_by_scope: dict[str, Decimal],
    observed_quantity_by_scope: dict[str, Decimal],
    observed_open_markets: set[str],
    observed_station_market_sets: dict[str, set[str]],
    allocation_run_id: str,
    created_at: datetime,
    min_recommended_size: Decimal,
    rounding_increment: Decimal,
    collect_limit_checks: bool = True,
) -> tuple[Decimal, str | None, str | None, Decimal, tuple[str, ...], list[PositionLimitCheck]]:
    reference_price = _decimal(decision.reference_price)
    requested_size = _decimal(decision.size)
    per_ticket_buy_cap = max(
        Decimal("0"),
        _decimal(capital_policy["max_buy_notional_per_ticket"]) if capital_policy is not None else _decimal(policy["max_buy_notional_per_ticket"]),
    )
    reason_codes: list[str] = []
    binding_limit_scope = None
    binding_limit_key = None
    concentration_penalty = Decimal("0")

    candidate_size = requested_size
    if str(decision.side).lower() == "buy" and per_ticket_buy_cap > Decimal("0"):
        capped_size = _round_down_size(per_ticket_buy_cap / reference_price, rounding_increment)
        if capped_size < candidate_size:
            candidate_size = capped_size
            reason_codes.append("per_ticket_budget_cap")
            if binding_limit_scope is None:
                binding_limit_scope = "per_ticket"
                binding_limit_key = (
                    str(capital_policy["capital_policy_id"])
                    if capital_policy is not None
                    else str(policy["policy_id"])
                )

    open_market_cap = int(capital_policy["max_open_markets"]) if capital_policy is not None and capital_policy.get("max_open_markets") is not None else None
    current_market_id = str(decision.market_id)
    if (
        open_market_cap is not None
        and open_market_cap >= 0
        and current_market_id not in observed_open_markets
        and len(observed_open_markets) >= open_market_cap
    ):
        candidate_size = Decimal("0")
        reason_codes.append("capital_open_markets_cap")
        if binding_limit_scope is None:
            binding_limit_scope = "capital_open_markets"
            binding_limit_key = str(capital_policy["capital_policy_id"])

    same_station_cap = int(capital_policy["max_same_station_markets"]) if capital_policy is not None and capital_policy.get("max_same_station_markets") is not None else None
    if (
        station_id
        and same_station_cap is not None
        and same_station_cap >= 0
        and current_market_id not in observed_station_market_sets.get(str(station_id), set())
        and len(observed_station_market_sets.get(str(station_id), set())) >= same_station_cap
    ):
        candidate_size = Decimal("0")
        reason_codes.append("capital_same_station_markets_cap")
        if binding_limit_scope is None:
            binding_limit_scope = "capital_same_station_markets"
            binding_limit_key = str(capital_policy["capital_policy_id"])

    inventory_available = _available_quantity(
        available_by_key,
        wallet_id=wallet_id,
        side=str(decision.side).lower(),
        market_id=decision.market_id,
        token_id=decision.token_id,
        outcome=decision.outcome,
    )
    if inventory_available < candidate_size:
        candidate_size = inventory_available
        reason_codes.append("inventory_constrained")
        if binding_limit_scope is None:
            binding_limit_scope = "inventory"
            binding_limit_key = str(decision.token_id)

    pre_concentration_size = _round_down_size(candidate_size, rounding_increment)
    candidate_size = pre_concentration_size
    raw_limit_checks = _evaluate_position_limits(
        decision=decision,
        allocation_run_id=allocation_run_id,
        wallet_id=wallet_id,
        candidate_size=candidate_size,
        observed_notional_by_scope=observed_notional_by_scope,
        observed_quantity_by_scope=observed_quantity_by_scope,
        station_id=station_id,
        limit_policies=limit_policies,
        created_at=created_at,
    )
    for check in raw_limit_checks:
        if check.check_status == "fail":
            before_limit_size = candidate_size
            candidate_size = _apply_limit_cap(
                decision=decision,
                candidate_size=candidate_size,
                check=check,
                rounding_increment=rounding_increment,
            )
            if check.limit_scope == "market":
                reason_codes.append("market_limit_exceeded")
            elif check.limit_scope == "station":
                reason_codes.append("station_limit_exceeded")
            if candidate_size < before_limit_size and pre_concentration_size > Decimal("0"):
                concentration_penalty = max(
                    concentration_penalty,
                    min((before_limit_size - candidate_size) / pre_concentration_size, Decimal("1")),
                )
            if binding_limit_scope is None and candidate_size < before_limit_size:
                binding_limit_scope = check.limit_scope
                binding_limit_key = check.scope_key

    candidate_size = _round_down_size(candidate_size, rounding_increment)
    if candidate_size < min_recommended_size:
        candidate_size = Decimal("0")
        if "below_min_recommended_size" not in reason_codes:
            reason_codes.append("below_min_recommended_size")

    ordered_reason_codes = tuple(sorted(set(reason_codes), key=lambda item: _ALLOCATION_REASON_ORDER.get(item, 99)))
    return (
        candidate_size,
        binding_limit_scope,
        binding_limit_key,
        concentration_penalty,
        ordered_reason_codes,
        raw_limit_checks if collect_limit_checks else [],
    )


def _preview_budget_impact(preview: dict[str, Any]) -> dict[str, Any]:
    return {
        "requested_size": float(preview["requested_size"]),
        "requested_notional": float(preview["requested_notional"]),
        "pre_budget_deployable_size": float(preview["pre_budget_deployable_size"]),
        "pre_budget_deployable_notional": float(preview["pre_budget_deployable_notional"]),
        "pre_budget_deployable_expected_pnl": float(preview["pre_budget_deployable_expected_pnl"]),
        "preview_binding_limit_scope": preview["preview_binding_limit_scope"],
        "preview_binding_limit_key": preview["preview_binding_limit_key"],
        "preview_concentration_penalty": float(preview["preview_concentration_penalty"]),
    }


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


def enqueue_capital_allocation_run_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    runs: list[CapitalAllocationRun],
    run_id: str | None = None,
) -> str | None:
    if not runs:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="runtime.capital_allocation_runs",
        pk_cols=["allocation_run_id"],
        columns=list(RUNTIME_CAPITAL_ALLOCATION_RUN_COLUMNS),
        rows=[capital_allocation_run_to_row(item) for item in runs],
        run_id=run_id,
    )


def enqueue_allocation_decision_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    decisions: list[AllocationDecision],
    run_id: str | None = None,
) -> str | None:
    if not decisions:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="runtime.allocation_decisions",
        pk_cols=["allocation_decision_id"],
        columns=list(RUNTIME_ALLOCATION_DECISION_COLUMNS),
        rows=[allocation_decision_to_row(item) for item in decisions],
        run_id=run_id,
    )


def enqueue_position_limit_check_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    checks: list[PositionLimitCheck],
    run_id: str | None = None,
) -> str | None:
    if not checks:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="runtime.position_limit_checks",
        pk_cols=["check_id"],
        columns=list(RUNTIME_POSITION_LIMIT_CHECK_COLUMNS),
        rows=[position_limit_check_to_row(item) for item in checks],
        run_id=run_id,
    )


def capital_allocation_run_to_row(record: CapitalAllocationRun) -> list[Any]:
    return [
        record.allocation_run_id,
        record.run_id,
        record.wallet_id,
        record.strategy_id,
        record.source_kind,
        record.requested_decision_count,
        record.decision_count,
        record.approved_count,
        record.resized_count,
        record.blocked_count,
        record.policy_missing_count,
        record.requested_buy_notional_total,
        record.recommended_buy_notional_total,
        _sql_timestamp(record.created_at),
    ]


def allocation_decision_to_row(record: AllocationDecision) -> list[Any]:
    return [
        record.allocation_decision_id,
        record.allocation_run_id,
        record.run_id,
        record.decision_id,
        record.watch_snapshot_id,
        record.wallet_id,
        record.strategy_id,
        record.market_id,
        record.token_id,
        record.side,
        record.ranking_score,
        record.base_ranking_score,
        record.deployable_expected_pnl,
        record.deployable_notional,
        record.max_deployable_size,
        record.capital_scarcity_penalty,
        record.concentration_penalty,
        record.pre_budget_deployable_size,
        record.pre_budget_deployable_notional,
        record.pre_budget_deployable_expected_pnl,
        record.rerank_position,
        safe_json_dumps(list(record.rerank_reason_codes)),
        record.requested_size,
        record.recommended_size,
        record.requested_notional,
        record.recommended_notional,
        record.allocation_status,
        safe_json_dumps(list(record.reason_codes)),
        safe_json_dumps(record.budget_impact),
        record.policy_id,
        record.policy_version,
        record.capital_policy_id,
        record.capital_policy_version,
        record.source_kind,
        record.binding_limit_scope,
        record.binding_limit_key,
        record.regime_bucket,
        record.calibration_gate_status,
        safe_json_dumps(list(record.capital_scaling_reason_codes)),
        _sql_timestamp(record.created_at),
    ]


def _canonicalize_allocator_decisions(decisions: list[StrategyDecision]) -> list[StrategyDecision]:
    ordered = sorted(
        list(decisions),
        key=lambda item: (int(item.decision_rank), str(item.decision_id)),
    )
    seen_ranks: set[int] = set()
    seen_ids: set[str] = set()
    for decision in ordered:
        rank = int(decision.decision_rank)
        decision_id = str(decision.decision_id)
        if rank <= 0:
            raise ValueError("allocator decisions must have strictly positive decision_rank")
        if decision_id in seen_ids:
            raise ValueError(f"duplicate decision_id in allocator input: {decision_id}")
        if rank in seen_ranks:
            raise ValueError(f"duplicate decision_rank in allocator input: {rank}")
        seen_ids.add(decision_id)
        seen_ranks.add(rank)
    return ordered


def position_limit_check_to_row(record: PositionLimitCheck) -> list[Any]:
    return [
        record.check_id,
        record.allocation_decision_id,
        record.limit_id,
        record.limit_scope,
        record.scope_key,
        record.observed_gross_notional,
        record.candidate_gross_notional,
        record.remaining_capacity,
        record.check_status,
        _sql_timestamp(record.created_at),
    ]


def load_inventory_positions_for_allocator(con, *, wallet_id: str) -> list[InventoryPosition]:
    from .portfolio_v3 import load_inventory_positions

    return load_inventory_positions(con, wallet_id=wallet_id)


def _load_active_allocation_policies(con, *, wallet_id: str) -> list[dict[str, Any]]:
    if not _table_exists(con, "trading.allocation_policies"):
        return []
    rows = con.execute(
        """
        SELECT
            policy_id,
            wallet_id,
            strategy_id,
            status,
            policy_version,
            max_buy_notional_per_run,
            max_buy_notional_per_ticket,
            min_recommended_size,
            size_rounding_increment,
            created_at,
            updated_at
        FROM trading.allocation_policies
        WHERE wallet_id = ?
        ORDER BY updated_at DESC, created_at DESC, policy_id DESC
        """,
        [wallet_id],
    ).fetchall()
    policies: list[dict[str, Any]] = []
    for row in rows:
        status = str(row[3] or "").lower()
        if status not in _ACTIVE_POLICY_STATUSES:
            continue
        policies.append(
            {
                "policy_id": str(row[0]),
                "wallet_id": str(row[1]),
                "strategy_id": _coerce_optional_text(row[2]),
                "status": status,
                "policy_version": str(row[4]),
                "max_buy_notional_per_run": float(row[5] or 0.0),
                "max_buy_notional_per_ticket": float(row[6] or 0.0),
                "min_recommended_size": float(row[7] or 0.0),
                "size_rounding_increment": float(row[8] or 0.0),
            }
        )
    return policies


def _load_default_allocation_policy(policies: list[dict[str, Any]]) -> dict[str, Any] | None:
    for item in policies:
        if item.get("strategy_id") is None:
            return item
    return None


def _load_active_position_limit_policies(con, *, wallet_id: str, policy_id: str) -> list[dict[str, Any]]:
    if not _table_exists(con, "trading.position_limit_policies"):
        return []
    rows = con.execute(
        """
        SELECT
            limit_id,
            policy_id,
            wallet_id,
            limit_scope,
            scope_key,
            max_gross_notional,
            max_position_quantity,
            status
        FROM trading.position_limit_policies
        WHERE wallet_id = ? AND policy_id = ?
        ORDER BY limit_scope ASC, scope_key ASC, limit_id ASC
        """,
        [wallet_id, policy_id],
    ).fetchall()
    limits: list[dict[str, Any]] = []
    for row in rows:
        status = str(row[7] or "").lower()
        if status not in _ACTIVE_POLICY_STATUSES:
            continue
        limits.append(
            {
                "limit_id": str(row[0]),
                "policy_id": str(row[1]),
                "wallet_id": str(row[2]),
                "limit_scope": str(row[3]),
                "scope_key": str(row[4]),
                "max_gross_notional": _coerce_optional_decimal(row[5]),
                "max_position_quantity": _coerce_optional_decimal(row[6]),
            }
        )
    return limits


def _load_active_capital_budget_policies(con, *, wallet_id: str) -> list[dict[str, Any]]:
    if not _table_exists(con, "trading.capital_budget_policies"):
        return []
    rows = con.execute(
        """
        SELECT
            capital_policy_id,
            wallet_id,
            strategy_id,
            regime_bucket,
            calibration_gate_status,
            status,
            policy_version,
            max_buy_notional_per_run,
            max_buy_notional_per_ticket,
            max_open_markets,
            max_same_station_markets,
            min_recommended_size
        FROM trading.capital_budget_policies
        WHERE wallet_id = ?
        ORDER BY updated_at DESC, created_at DESC, capital_policy_id DESC
        """,
        [wallet_id],
    ).fetchall()
    policies: list[dict[str, Any]] = []
    for row in rows:
        status = str(row[5] or "").lower()
        if status not in _ACTIVE_POLICY_STATUSES:
            continue
        policies.append(
            {
                "capital_policy_id": str(row[0]),
                "wallet_id": str(row[1]),
                "strategy_id": _coerce_optional_text(row[2]),
                "regime_bucket": _coerce_optional_text(row[3]),
                "calibration_gate_status": _coerce_optional_text(row[4]),
                "status": status,
                "policy_version": str(row[6]),
                "max_buy_notional_per_run": float(row[7] or 0.0),
                "max_buy_notional_per_ticket": float(row[8] or 0.0),
                "max_open_markets": None if row[9] is None else int(row[9]),
                "max_same_station_markets": None if row[10] is None else int(row[10]),
                "min_recommended_size": float(row[11] or 0.0),
            }
        )
    return policies


def _lookup_capital_budget_policy(
    policies: list[dict[str, Any]],
    *,
    strategy_id: str | None,
    regime_bucket: str | None,
    calibration_gate_status: str | None,
) -> dict[str, Any] | None:
    strategy_value = _coerce_optional_text(strategy_id)
    regime_value = _coerce_optional_text(regime_bucket)
    gate_value = _coerce_optional_text(calibration_gate_status)
    precedence = (
        (strategy_value, regime_value, gate_value),
        (strategy_value, regime_value, None),
        (strategy_value, None, None),
    )
    for expected_strategy, expected_regime, expected_gate in precedence:
        for item in policies:
            if item.get("strategy_id") != expected_strategy:
                continue
            if item.get("regime_bucket") != expected_regime:
                continue
            if item.get("calibration_gate_status") != expected_gate:
                continue
            return item
    return None


def _load_current_exposure_basis(
    con,
    *,
    wallet_id: str,
    market_station_map: dict[str, str],
    reference_price_by_market: dict[str, Decimal],
    fallback_positions: list[InventoryPosition],
) -> dict[str, dict[str, Decimal]]:
    exposure_rows = _load_latest_exposure_snapshot_rows(con, wallet_id=wallet_id)
    notional_by_scope: dict[str, Decimal] = defaultdict(Decimal)
    quantity_by_scope: dict[str, Decimal] = defaultdict(Decimal)
    if exposure_rows:
        open_markets = {str(row.market_id) for row in exposure_rows if row.market_id is not None}
        for row in exposure_rows:
            market_id = str(row.market_id)
            station_id = market_station_map.get(market_id)
            quantity = row.open_order_size + row.filled_position_size + row.settled_position_size + row.redeemable_size
            price = reference_price_by_market.get(market_id, Decimal("0"))
            gross_notional = row.reserved_notional_usdc + (quantity * price)
            market_scope = f"market:{market_id}"
            notional_by_scope[market_scope] += gross_notional
            quantity_by_scope[market_scope] += quantity
            if station_id:
                station_scope = f"station:{station_id}"
                notional_by_scope[station_scope] += gross_notional
                quantity_by_scope[station_scope] += quantity
        return {
            "notional_by_scope": notional_by_scope,
            "quantity_by_scope": quantity_by_scope,
            "open_markets": open_markets,
            "station_market_sets": _build_station_market_sets(
                market_station_map=market_station_map,
                market_ids=list(open_markets),
            ),
        }

    open_markets: set[str] = set()
    if _table_exists(con, "trading.reservations"):
        rows = con.execute(
            """
            SELECT market_id, outcome, token_id, asset_type, remaining_quantity, reserved_notional, status
            FROM trading.reservations
            WHERE wallet_id = ?
            """,
            [wallet_id],
        ).fetchall()
        for row in rows:
            status = str(row[6] or "").lower()
            if status not in _OPEN_RESERVATION_STATUSES:
                continue
            market_id = _coerce_optional_text(row[0])
            if market_id is None:
                continue
            open_markets.add(market_id)
            market_scope = f"market:{market_id}"
            station_id = market_station_map.get(market_id)
            gross_notional = _decimal(row[5] or 0.0)
            quantity = _decimal(row[4] or 0.0) if str(row[3]) == "outcome_token" else Decimal("0")
            notional_by_scope[market_scope] += gross_notional
            quantity_by_scope[market_scope] += quantity
            if station_id:
                station_scope = f"station:{station_id}"
                notional_by_scope[station_scope] += gross_notional
                quantity_by_scope[station_scope] += quantity

    for position in fallback_positions:
        if position.asset_type != "outcome_token" or position.balance_type not in {
            BalanceType.AVAILABLE,
            BalanceType.RESERVED,
            BalanceType.SETTLED,
            BalanceType.REDEEMABLE,
        }:
            continue
        market_id = position.market_id or ""
        if not market_id:
            continue
        open_markets.add(market_id)
        price = reference_price_by_market.get(market_id, Decimal("0"))
        gross_notional = position.quantity * price
        market_scope = f"market:{market_id}"
        notional_by_scope[market_scope] += gross_notional
        quantity_by_scope[market_scope] += position.quantity
        station_id = market_station_map.get(market_id)
        if station_id:
            station_scope = f"station:{station_id}"
            notional_by_scope[station_scope] += gross_notional
            quantity_by_scope[station_scope] += position.quantity

    return {
        "notional_by_scope": notional_by_scope,
        "quantity_by_scope": quantity_by_scope,
        "open_markets": open_markets,
        "station_market_sets": _build_station_market_sets(
            market_station_map=market_station_map,
            market_ids=list(open_markets),
        ),
    }


def _build_station_market_sets(*, market_station_map: dict[str, str], market_ids: list[str]) -> dict[str, set[str]]:
    station_market_sets: dict[str, set[str]] = defaultdict(set)
    for market_id in market_ids:
        station_id = market_station_map.get(str(market_id))
        if station_id:
            station_market_sets[str(station_id)].add(str(market_id))
    return station_market_sets


def _load_latest_exposure_snapshot_rows(con, *, wallet_id: str) -> list[ExposureSnapshot]:
    if not _table_exists(con, "trading.exposure_snapshots"):
        return []
    max_row = con.execute(
        "SELECT MAX(captured_at) FROM trading.exposure_snapshots WHERE wallet_id = ?",
        [wallet_id],
    ).fetchone()
    captured_at = max_row[0] if max_row is not None else None
    if captured_at is None:
        return []
    rows = con.execute(
        """
        SELECT
            snapshot_id,
            wallet_id,
            funder,
            signature_type,
            market_id,
            token_id,
            outcome,
            open_order_size,
            reserved_notional_usdc,
            filled_position_size,
            settled_position_size,
            redeemable_size,
            captured_at
        FROM trading.exposure_snapshots
        WHERE wallet_id = ? AND captured_at = ?
        """,
        [wallet_id, captured_at],
    ).fetchall()
    return [
        ExposureSnapshot(
            snapshot_id=str(row[0]),
            wallet_id=str(row[1]),
            funder=str(row[2]),
            signature_type=int(row[3]),
            market_id=str(row[4]),
            token_id=str(row[5]),
            outcome=str(row[6]),
            open_order_size=_decimal(row[7]),
            reserved_notional_usdc=_decimal(row[8]),
            filled_position_size=_decimal(row[9]),
            settled_position_size=_decimal(row[10]),
            redeemable_size=_decimal(row[11]),
            captured_at=row[12],
        )
        for row in rows
    ]


def _build_available_balances(positions: list[InventoryPosition]) -> dict[tuple[str, ...], Decimal]:
    balances: dict[tuple[str, ...], Decimal] = {}
    for position in positions:
        if position.balance_type is not BalanceType.AVAILABLE:
            continue
        if position.asset_type == "usdc_e":
            balances[("usdc_e", "_cash_", "_cash_")] = position.quantity
        elif position.asset_type == "outcome_token":
            balances[("outcome_token", str(position.token_id), str(position.market_id), str(position.outcome))] = position.quantity
    return balances


def _available_quantity(
    balances: dict[tuple[str, ...], Decimal],
    *,
    wallet_id: str,
    side: str,
    market_id: str,
    token_id: str,
    outcome: str,
) -> Decimal:
    del wallet_id
    if side == "buy":
        return balances.get(("usdc_e", "_cash_", "_cash_"), Decimal("0"))
    return balances.get(("outcome_token", str(token_id), str(market_id), str(outcome)), Decimal("0"))


def _evaluate_position_limits(
    *,
    decision: StrategyDecision,
    allocation_run_id: str,
    wallet_id: str,
    candidate_size: Decimal,
    observed_notional_by_scope: dict[str, Decimal],
    observed_quantity_by_scope: dict[str, Decimal],
    station_id: str | None,
    limit_policies: list[dict[str, Any]],
    created_at: datetime,
) -> list[PositionLimitCheck]:
    checks: list[PositionLimitCheck] = []
    candidate_notional = _decimal(decision.reference_price) * candidate_size
    for policy in limit_policies:
        scope = str(policy["limit_scope"])
        scope_key = str(policy["scope_key"])
        if scope == "market" and scope_key != str(decision.market_id):
            continue
        if scope == "station" and scope_key != str(station_id or ""):
            continue
        scope_name = f"{scope}:{scope_key}"
        observed_notional = observed_notional_by_scope.get(scope_name, Decimal("0"))
        observed_quantity = observed_quantity_by_scope.get(scope_name, Decimal("0"))
        gross_cap = policy.get("max_gross_notional")
        qty_cap = policy.get("max_position_quantity")
        remaining_notional = None if gross_cap is None else max(Decimal("0"), gross_cap - observed_notional)
        remaining_quantity = None if qty_cap is None else max(Decimal("0"), qty_cap - observed_quantity)
        status = "pass"
        if (remaining_notional is not None and candidate_notional > remaining_notional) or (
            remaining_quantity is not None and candidate_size > remaining_quantity
        ):
            status = "fail"
        checks.append(
            PositionLimitCheck(
                check_id=stable_object_id(
                    "limitcheck",
                    {
                        "allocation_run_id": allocation_run_id,
                        "decision_id": decision.decision_id,
                        "limit_id": policy["limit_id"],
                    },
                ),
                allocation_decision_id=stable_object_id(
                    "allocdec",
                    {
                        "allocation_run_id": allocation_run_id,
                        "decision_id": decision.decision_id,
                    },
                ),
                limit_id=str(policy["limit_id"]),
                limit_scope=scope,
                scope_key=scope_key,
                observed_gross_notional=float(observed_notional),
                candidate_gross_notional=float(candidate_notional),
                remaining_capacity=float(remaining_notional) if remaining_notional is not None else None,
                check_status=status,
                created_at=created_at,
            )
        )
    return checks


def _apply_limit_cap(
    *,
    decision: StrategyDecision,
    candidate_size: Decimal,
    check: PositionLimitCheck,
    rounding_increment: Decimal,
) -> Decimal:
    remaining_notional = _coerce_optional_decimal(check.remaining_capacity)
    if remaining_notional is None:
        return candidate_size
    if _decimal(decision.reference_price) <= Decimal("0"):
        return Decimal("0")
    capped_by_notional = _round_down_size(remaining_notional / _decimal(decision.reference_price), rounding_increment)
    return min(candidate_size, capped_by_notional)


def _round_down_size(size: Decimal, increment: Decimal) -> Decimal:
    if size <= Decimal("0"):
        return Decimal("0")
    if increment <= Decimal("0"):
        return size
    steps = (size / increment).to_integral_value(rounding=ROUND_DOWN)
    return steps * increment


def _coerce_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_optional_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    return _decimal(value)


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value or 0))


def _normalize_datetime(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(UTC).replace(tzinfo=None, microsecond=0)
    if value.tzinfo is None:
        return value.replace(microsecond=0)
    return value.astimezone(UTC).replace(tzinfo=None, microsecond=0)


def _sql_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _normalize_datetime(value).isoformat(sep=" ")


def _table_exists(con, name: str) -> bool:
    try:
        row = con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = ? AND table_name = ?
            LIMIT 1
            """,
            name.split(".", 1),
        ).fetchone()
    except Exception:  # noqa: BLE001
        return False
    return row is not None
