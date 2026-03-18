from __future__ import annotations

from ui.loaders.shared_truth_source import SurfaceLoaderContract, build_truth_source_summary, validate_surface_loader_contract


def load_markets_surface_contract() -> SurfaceLoaderContract:
    from ui import data_access as compat

    payload = compat.load_market_chain_analysis_data()
    contract = SurfaceLoaderContract(
        surface_id="markets",
        primary_dataframe_name="market_opportunities",
        supporting_payload=payload,
        truth_source_summary=build_truth_source_summary(
            surface_id="markets",
            primary_table="ui.market_opportunity_summary",
            source=payload.get("market_opportunity_source") or "missing",
            supports_source_badges=True,
        ),
    )
    validate_surface_loader_contract(contract)
    return contract
