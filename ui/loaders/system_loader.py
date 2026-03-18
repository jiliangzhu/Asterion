from __future__ import annotations

from ui.loaders.shared_truth_source import SurfaceLoaderContract, build_truth_source_summary, validate_surface_loader_contract


def load_system_surface_contract() -> SurfaceLoaderContract:
    from ui import data_access as compat

    payload = {
        "system_runtime": compat.load_system_runtime_status(),
        "boundary_sidebar": compat.load_boundary_sidebar_truth(),
        "surface_status": compat.load_operator_surface_status(),
    }
    contract = SurfaceLoaderContract(
        surface_id="system",
        primary_dataframe_name="system_runtime",
        supporting_payload=payload,
        truth_source_summary=build_truth_source_summary(
            surface_id="system",
            primary_table="ui.readiness_evidence_summary",
            source="ui_lite",
            supports_source_badges=False,
        ),
    )
    validate_surface_loader_contract(contract)
    return contract
