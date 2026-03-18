from __future__ import annotations

from ui.loaders.shared_truth_source import SurfaceLoaderContract, build_truth_source_summary, validate_surface_loader_contract


def load_readiness_surface_contract() -> SurfaceLoaderContract:
    from ui import data_access as compat

    payload = {
        "readiness_summary": compat.load_readiness_summary(),
        "readiness_evidence": compat.load_readiness_evidence_bundle(),
        "wallet_readiness": compat.load_wallet_readiness_data(),
    }
    contract = SurfaceLoaderContract(
        surface_id="system",
        primary_dataframe_name="wallet_readiness",
        supporting_payload=payload,
        truth_source_summary=build_truth_source_summary(
            surface_id="system",
            primary_table="ui.phase_readiness_summary",
            source=payload["readiness_summary"].get("source") or "missing",
            supports_source_badges=False,
        ),
    )
    validate_surface_loader_contract(contract)
    return contract
