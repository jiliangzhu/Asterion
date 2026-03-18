from __future__ import annotations

from ui.loaders.shared_truth_source import SurfaceLoaderContract, build_truth_source_summary, validate_surface_loader_contract


def load_agents_surface_contract() -> SurfaceLoaderContract:
    from ui import data_access as compat

    payload = {
        "agent_review": compat.load_agent_review_data(),
        "agent_runtime": compat.load_agent_runtime_status(),
    }
    contract = SurfaceLoaderContract(
        surface_id="agents",
        primary_dataframe_name="agent_review",
        supporting_payload=payload,
        truth_source_summary=build_truth_source_summary(
            surface_id="agents",
            primary_table="ui.agent_review_summary",
            source=payload["agent_review"].get("source") or "missing",
            supports_source_badges=False,
        ),
    )
    validate_surface_loader_contract(contract)
    return contract
