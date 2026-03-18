from __future__ import annotations

from asterion_core.ui.surface_truth_shared import (
    BoundarySidebarSummary,
    CURRENT_PHASE_STATUS,
    PRIMARY_SCORE_DIAGNOSTICS,
    PRIMARY_SCORE_FIELD,
    PRIMARY_SCORE_LABEL,
    SYSTEM_POSITIONING,
    TRUTH_SOURCE_DOC,
    OpportunityRowSourceBadge,
    PrimaryScoreDescriptor,
    SurfaceTruthDescriptor,
    annotate_frame_with_source_truth,
    build_opportunity_row_source_badge,
    ensure_primary_score_fields,
)


def load_surface_truth_descriptors() -> dict[str, SurfaceTruthDescriptor]:
    return {
        "home_top_opportunities": SurfaceTruthDescriptor(
            surface_id="home_top_opportunities",
            primary_table="ui.market_opportunity_summary",
            fallback_sources=["smoke_report", "weather_smoke_db"],
            primary_score=PRIMARY_SCORE_FIELD,
            boundary_copy_key="operator_boundary",
            supports_source_badges=True,
        ),
        "markets_coverage": SurfaceTruthDescriptor(
            surface_id="markets_coverage",
            primary_table="ui.market_opportunity_summary",
            fallback_sources=["smoke_report", "weather_smoke_db"],
            primary_score=PRIMARY_SCORE_FIELD,
            boundary_copy_key="operator_boundary",
            supports_source_badges=True,
        ),
        "execution_science": SurfaceTruthDescriptor(
            surface_id="execution_science",
            primary_table="ui.execution_science_summary",
            fallback_sources=[],
            primary_score=PRIMARY_SCORE_FIELD,
            boundary_copy_key="execution_boundary",
            supports_source_badges=True,
        ),
    }


def load_primary_score_descriptor() -> PrimaryScoreDescriptor:
    return PrimaryScoreDescriptor(
        primary_score=PRIMARY_SCORE_FIELD,
        primary_score_label=PRIMARY_SCORE_LABEL,
        diagnostics=PRIMARY_SCORE_DIAGNOSTICS,
    )


def load_boundary_sidebar_summary() -> BoundarySidebarSummary:
    from ui.data_access import (
        load_readiness_evidence_bundle,
        load_readiness_summary,
        load_system_runtime_status,
    )

    readiness = load_readiness_summary()
    evidence = load_readiness_evidence_bundle()
    runtime_status = load_system_runtime_status()
    boundary = (
        evidence.get("capability_boundary_summary")
        or readiness.get("capability_boundary_summary")
        or runtime_status.get("capability_boundary_summary")
        or {}
    )

    capability_boundary: list[str] = []
    if boundary.get("manual_only"):
        capability_boundary.append("manual-only")
    if boundary.get("default_off"):
        capability_boundary.append("default-off")
    if boundary.get("approve_usdc_only"):
        capability_boundary.append("approve_usdc only")
    if boundary.get("constrained_real_submit_enabled"):
        capability_boundary.append("constrained real submit")
    elif boundary.get("shadow_submitter_only"):
        capability_boundary.append("shadow submitter only")

    if not capability_boundary:
        capability_boundary = [
            "manual-only",
            "default-off",
            "approve_usdc only",
            "constrained real submit",
        ]

    return BoundarySidebarSummary(
        system_positioning=SYSTEM_POSITIONING,
        current_phase_status=CURRENT_PHASE_STATUS,
        capability_boundary=capability_boundary,
        live_negations=["not unattended live", "not unrestricted live"],
        truth_source_doc=TRUTH_SOURCE_DOC,
    )

