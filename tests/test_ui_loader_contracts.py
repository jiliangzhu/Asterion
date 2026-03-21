from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from tests.ui_read_model_test_utils import build_minimal_ui_read_model_db
from ui.loaders.agents_loader import load_agents_surface_contract
from ui.loaders.execution_loader import load_execution_surface_contract
from ui.loaders.home_loader import load_home_surface_contract
from ui.loaders.markets_loader import load_markets_surface_contract
from ui.loaders.readiness_loader import load_readiness_surface_contract
from ui.loaders.shared_truth_source import validate_surface_loader_contract
from ui.loaders.system_loader import load_system_surface_contract


class UiLoaderContractsTest(unittest.TestCase):
    def test_loader_contracts_match_facade_payloads(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ui_lite.duckdb"
            build_minimal_ui_read_model_db(db_path)
            with patch.dict(os.environ, {"ASTERION_UI_LITE_DB_PATH": str(db_path)}, clear=False):
                with patch(
                    "ui.loaders.home_loader.load_home_decision_snapshot",
                    return_value={"top_opportunities": pd.DataFrame(), "market_data": {"market_opportunity_source": "ui_lite"}, "action_queue": pd.DataFrame()},
                ):
                    home_contract = load_home_surface_contract()
                with patch(
                    "ui.loaders.markets_loader.load_market_chain_analysis_data",
                    return_value={"market_opportunities": pd.DataFrame(), "market_opportunity_source": "ui_lite", "market_rows": [], "cohort_history": pd.DataFrame()},
                ):
                    markets_contract = load_markets_surface_contract()
                with patch(
                    "ui.loaders.execution_loader.load_execution_console_data",
                    return_value={"execution_science": pd.DataFrame(), "cohort_history": pd.DataFrame()},
                ):
                    execution_contract = load_execution_surface_contract()
                with patch("ui.data_access.load_resolution_review_data", return_value={"source": "ui_lite", "frame": pd.DataFrame()}), patch(
                    "ui.data_access.load_agent_runtime_status", return_value={"mode": "review_only"}
                ):
                    agents_contract = load_agents_surface_contract()
                with patch("ui.data_access.load_readiness_summary", return_value={"source": "ui_lite"}), patch(
                    "ui.data_access.load_readiness_evidence_bundle", return_value={"exists": True}
                ), patch("ui.data_access.load_wallet_readiness_data", return_value=pd.DataFrame()):
                    readiness_contract = load_readiness_surface_contract()
                with patch("ui.data_access.load_system_runtime_status", return_value={"ui_lite_exists": True}), patch(
                    "ui.data_access.load_boundary_sidebar_truth", return_value={"truth_source_doc": "docs/10-implementation/versions/v2.0/phase-plans/V2_Implementation_Plan.md"}
                ), patch("ui.data_access.load_operator_surface_status", return_value={"overall": {"status": "ok"}}):
                    system_contract = load_system_surface_contract()

        for contract in [home_contract, markets_contract, execution_contract, agents_contract, readiness_contract, system_contract]:
            validate_surface_loader_contract(contract)
            self.assertIn("schema_versions", contract.truth_source_summary)

        self.assertEqual(home_contract.primary_dataframe_name, "top_opportunities")
        self.assertEqual(markets_contract.primary_dataframe_name, "market_opportunities")
        self.assertEqual(execution_contract.primary_dataframe_name, "execution_science")
        self.assertIn("action_queue", home_contract.supporting_payload)
        self.assertIn("market_rows", markets_contract.supporting_payload)
        self.assertIn("cohort_history", markets_contract.supporting_payload)
        self.assertIn("cohort_history", execution_contract.supporting_payload)


if __name__ == "__main__":
    unittest.main()
