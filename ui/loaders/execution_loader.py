from __future__ import annotations

from ui.loaders.shared_truth_source import SurfaceLoaderContract, build_truth_source_summary, validate_surface_loader_contract


def load_execution_surface_contract() -> SurfaceLoaderContract:
    from ui import data_access as compat

    payload = compat.load_execution_console_data()
    contract = SurfaceLoaderContract(
        surface_id="execution",
        primary_dataframe_name="execution_science",
        supporting_payload=payload,
        truth_source_summary=build_truth_source_summary(
            surface_id="execution",
            primary_table="ui.execution_science_summary",
            source="ui_lite",
            supports_source_badges=True,
        ),
    )
    validate_surface_loader_contract(contract)
    return contract
