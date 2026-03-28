from __future__ import annotations

from ui.loaders.shared_truth_source import SurfaceLoaderContract, build_truth_source_summary, validate_surface_loader_contract


def load_agents_surface_contract() -> SurfaceLoaderContract:
    from ui import data_access as compat

    payload = {
        "agent_review": compat.load_resolution_review_data(),
        "opportunity_triage": compat.load_opportunity_triage_data(),
        "agent_runtime": compat.load_agent_runtime_status(),
    }
    contract = SurfaceLoaderContract(
        surface_id="agents",
        primary_dataframe_name="opportunity_triage",
        supporting_payload=payload,
        truth_source_summary=build_truth_source_summary(
            surface_id="agents",
            primary_table="ui.opportunity_triage_summary",
            source=payload["opportunity_triage"].get("source") or "missing",
            supports_source_badges=False,
        ),
    )
    validate_surface_loader_contract(contract)
    return contract
