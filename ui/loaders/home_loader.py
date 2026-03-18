from __future__ import annotations

from ui.loaders.shared_truth_source import SurfaceLoaderContract, build_truth_source_summary, validate_surface_loader_contract


def load_home_surface_contract() -> SurfaceLoaderContract:
    from ui import data_access as compat

    payload = compat.load_home_decision_snapshot()
    contract = SurfaceLoaderContract(
        surface_id="home",
        primary_dataframe_name="top_opportunities",
        supporting_payload=payload,
        truth_source_summary=build_truth_source_summary(
            surface_id="home",
            primary_table="ui.market_opportunity_summary",
            source=(payload.get("market_data") or {}).get("market_opportunity_source") or "missing",
            supports_source_badges=True,
        ),
    )
    validate_surface_loader_contract(contract)
    return contract
