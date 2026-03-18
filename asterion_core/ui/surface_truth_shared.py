from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import pandas as pd


TRUTH_SOURCE_DOC = "docs/10-implementation/versions/v2.0/phase-plans/V2_Implementation_Plan.md"
SYSTEM_POSITIONING = "operator console + constrained execution infra"
CURRENT_PHASE_STATUS = "P4 accepted; post-P4 remediation accepted; v2.0 implementation active"
PRIMARY_SCORE_FIELD = "ranking_score"
PRIMARY_SCORE_LABEL = "Ranking Score"
PRIMARY_SCORE_DIAGNOSTICS = (
    "expected_value_score",
    "expected_pnl_score",
    "ops_readiness_score",
    "confidence_score",
)
_FALLBACK_ORIGINS = {"smoke_report", "weather_smoke_db", "runtime_db"}


@dataclass(frozen=True)
class SurfaceTruthDescriptor:
    surface_id: str
    primary_table: str
    fallback_sources: list[str]
    primary_score: str
    boundary_copy_key: str
    supports_source_badges: bool

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BoundarySidebarSummary:
    system_positioning: str
    current_phase_status: str
    capability_boundary: list[str]
    live_negations: list[str]
    truth_source_doc: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class OpportunityRowSourceBadge:
    source_badge: str
    source_truth_status: str
    is_degraded_source: bool
    reason_codes: list[str]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PrimaryScoreDescriptor:
    primary_score: str
    primary_score_label: str
    diagnostics: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_opportunity_row_source_badge(
    *,
    source_origin: str,
    source_freshness_status: Any = None,
    derived: bool = False,
    degraded_reason_codes: list[str] | None = None,
) -> OpportunityRowSourceBadge:
    freshness = _coerce_text(source_freshness_status).lower()
    reasons = [str(item) for item in (degraded_reason_codes or []) if item not in {None, ""}]
    if source_origin in _FALLBACK_ORIGINS:
        badge = "fallback"
    elif freshness in {"degraded", "missing"}:
        badge = "degraded"
    elif freshness == "stale":
        badge = "stale"
    elif derived:
        badge = "derived"
    else:
        badge = "canonical"
    return OpportunityRowSourceBadge(
        source_badge=badge,
        source_truth_status=badge,
        is_degraded_source=badge in {"fallback", "stale", "degraded"},
        reason_codes=reasons,
    )


def ensure_primary_score_fields(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        result = frame.copy()
        if "primary_score_label" not in result.columns:
            result["primary_score_label"] = pd.Series(dtype="string")
        return result

    result = frame.copy()
    if "ranking_score" not in result.columns and "opportunity_score" in result.columns:
        result["ranking_score"] = pd.to_numeric(result.get("opportunity_score"), errors="coerce")
    if "opportunity_score" not in result.columns and "ranking_score" in result.columns:
        result["opportunity_score"] = pd.to_numeric(result.get("ranking_score"), errors="coerce")
    result["primary_score_label"] = PRIMARY_SCORE_FIELD
    return result


def annotate_frame_with_source_truth(
    frame: pd.DataFrame,
    *,
    source_origin: str,
    derived: bool = False,
    freshness_column: str = "source_freshness_status",
    degraded_reason_column: str | None = None,
) -> pd.DataFrame:
    result = ensure_primary_score_fields(frame)
    if result.empty:
        for column in ["source_badge", "source_truth_status", "is_degraded_source"]:
            if column not in result.columns:
                result[column] = pd.Series(dtype="string" if column != "is_degraded_source" else "boolean")
        return result

    badges: list[OpportunityRowSourceBadge] = []
    for _, row in result.iterrows():
        reasons: list[str] = []
        if degraded_reason_column and degraded_reason_column in result.columns:
            reasons = _coerce_list(row.get(degraded_reason_column))
        badges.append(
            build_opportunity_row_source_badge(
                source_origin=source_origin,
                source_freshness_status=row.get(freshness_column) if freshness_column in result.columns else None,
                derived=derived,
                degraded_reason_codes=reasons,
            )
        )
    result["source_badge"] = [item.source_badge for item in badges]
    result["source_truth_status"] = [item.source_truth_status for item in badges]
    result["is_degraded_source"] = [item.is_degraded_source for item in badges]
    return result


def _coerce_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if item not in {None, ""}]
    return []


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()
