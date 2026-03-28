from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class DeepAuditPlanDocsTest(unittest.TestCase):
    def test_phase10_plus_supporting_docs_exist(self) -> None:
        expected = [
            ROOT / "docs" / "30-trading" / "Controlled_Live_Boundary_Design.md",
            ROOT / "docs" / "30-trading" / "Execution_Economics_Design.md",
            ROOT / "docs" / "40-weather" / "Forecast_Calibration_v2_Design.md",
            ROOT / "docs" / "50-operations" / "Operator_Console_Truth_Source_Design.md",
            ROOT / "docs" / "20-architecture" / "UI_Read_Model_Design.md",
            ROOT / "docs" / "10-implementation" / "versions" / "v1.0-remediation" / "checklists" / "Post_P4_P10_Closeout_Checklist.md",
            ROOT / "docs" / "10-implementation" / "versions" / "v1.0-remediation" / "checklists" / "Post_P4_P11_Closeout_Checklist.md",
            ROOT / "docs" / "10-implementation" / "versions" / "v1.0-remediation" / "checklists" / "Post_P4_P12_Closeout_Checklist.md",
            ROOT / "docs" / "10-implementation" / "versions" / "v1.0-remediation" / "checklists" / "Post_P4_P13_Closeout_Checklist.md",
            ROOT / "docs" / "10-implementation" / "versions" / "v1.0-remediation" / "checklists" / "Post_P4_P14_Closeout_Checklist.md",
            ROOT / "docs" / "10-implementation" / "versions" / "v1.0-remediation" / "checklists" / "Post_P4_P15_Closeout_Checklist.md",
        ]
        missing = [str(path) for path in expected if not path.exists()]
        self.assertEqual(missing, [])

    def test_navigation_docs_reference_deep_audit_materials(self) -> None:
        expected_refs = {
            ROOT / "README.md": [
                "historical accepted remediation record",
                "Controlled_Live_Boundary_Design.md",
                "Execution_Economics_Design.md",
                "Forecast_Calibration_v2_Design.md",
                "Operator_Console_Truth_Source_Design.md",
                "UI_Read_Model_Design.md",
            ],
            ROOT / "docs" / "10-implementation" / "Implementation_Index.md": [
                "Post-P4 Phase 10` 到 `Post-P4 Phase 15",
                "Post_P4_P10_Closeout_Checklist.md",
                "Post_P4_P15_Closeout_Checklist.md",
                "Controlled_Live_Boundary_Design.md",
            ],
            ROOT / "docs" / "00-overview" / "Documentation_Index.md": [
                "Post_P4_P10_Closeout_Checklist.md",
                "Controlled_Live_Boundary_Design.md",
                "Forecast_Calibration_v2_Design.md",
                "Operator_Console_Truth_Source_Design.md",
                "UI_Read_Model_Design.md",
            ],
            ROOT / "docs" / "00-overview" / "versions" / "v2.0" / "DEVELOPMENT_ROADMAP.md": [
                "Deep Audit Improvement Roadmap",
                "Post-P4 Phase 10",
                "Post-P4 Phase 15",
            ],
            ROOT / "docs" / "00-overview" / "versions" / "v2.0" / "Asterion_Project_Plan.md": [
                "Post-P4 Phase 10` 到 `Post-P4 Phase 15",
                "Controlled_Live_Boundary_Design.md",
                "UI_Read_Model_Design.md",
            ],
            ROOT / "docs" / "10-implementation" / "versions" / "v1.0-remediation" / "phase-plans" / "Post_P4_Remediation_Implementation_Plan.md": [
                "## 11. Deep Audit Improvement Roadmap",
                "### 11.3 Post-P4 Phase 10: Boundary Hardening v2",
                "### 11.8 Post-P4 Phase 15: UI Read-Model and Truth-Source Refactor",
            ],
        }
        missing_refs: list[str] = []
        for path, refs in expected_refs.items():
            text = path.read_text(encoding="utf-8")
            for ref in refs:
                if ref not in text:
                    missing_refs.append(f"{path}:{ref}")
        self.assertEqual(missing_refs, [])

    def test_only_one_active_implementation_entry_is_described(self) -> None:
        docs = [
            ROOT / "README.md",
            ROOT / "docs" / "10-implementation" / "Implementation_Index.md",
            ROOT / "docs" / "00-overview" / "Documentation_Index.md",
            ROOT / "docs" / "00-overview" / "versions" / "v2.0" / "DEVELOPMENT_ROADMAP.md",
            ROOT / "docs" / "00-overview" / "versions" / "v2.0" / "Asterion_Project_Plan.md",
        ]
        missing: list[str] = []
        for path in docs:
            text = path.read_text(encoding="utf-8")
            if "V2_Implementation_Plan.md" not in text:
                missing.append(str(path))
        self.assertEqual(missing, [])

    def test_v2_plan_is_full_active_contract(self) -> None:
        plan = (
            ROOT
            / "docs"
            / "10-implementation"
            / "versions"
            / "v2.0"
            / "phase-plans"
            / "V2_Implementation_Plan.md"
        ).read_text(encoding="utf-8")
        self.assertIn("active implementation contract", plan)
        self.assertIn("当前唯一 active implementation entry", plan)
        self.assertIn("P8_Implementation_Plan.md", plan)
        self.assertIn("P9_Implementation_Plan.md", plan)
        self.assertIn("P10_Implementation_Plan.md", plan)
        self.assertIn("P11_Implementation_Plan.md", plan)
        self.assertIn("P7_Implementation_Plan.md", plan)
        self.assertIn("P6_Implementation_Plan.md", plan)
        self.assertIn("WS0. Truth-Source and Delivery Baseline", plan)
        self.assertIn("Phase 0. Stabilize Current HEAD", plan)
        self.assertIn("Phase 6. Capital-Aware Ranking and Deployable Action Queue", plan)
        self.assertIn("Phase 7. Deployable Rerank, Allocator v2, and Execution Economics Closure", plan)
        self.assertIn("Phase 8. Calibration Hard Gates and Scaling-Aware Capital Discipline", plan)
        self.assertIn("Phase 9. Operator Surface Delivery and Throughput Scaling", plan)
        self.assertIn("Phase 10. Deterministic ROI Repair and Execution Intelligence Foundation", plan)
        self.assertIn("Phase 11. Opportunity Triage / Execution Intelligence Agent", plan)
        self.assertIn("ui.daily_review_input.item_id", plan)
        self.assertIn("recommended_size", plan)
        self.assertIn("runtime.capital_allocation_runs", plan)
        self.assertIn("runtime.calibration_profile_materializations", plan)
        self.assertIn("manual-only / default-off / constrained", plan)
        self.assertNotIn("planning placeholder", plan)

    def test_p6_plan_is_accepted_baseline_record(self) -> None:
        plan_path = (
            ROOT
            / "docs"
            / "10-implementation"
            / "versions"
            / "v2.0"
            / "phase-plans"
            / "P6_Implementation_Plan.md"
        )
        self.assertTrue(plan_path.exists())
        plan = plan_path.read_text(encoding="utf-8")
        self.assertIn("accepted tranche baseline record", plan)
        self.assertIn("Capital-Aware Ranking and Deployable Action Queue", plan)
        self.assertIn("Phase 0` 到 `Phase 6` 已 accepted", plan)
        self.assertIn("deployable_expected_pnl", plan)
        self.assertIn("ui.action_queue_summary", plan)
        self.assertIn("P7", plan)
        self.assertIn("P8", plan)
        self.assertIn("allocator self-sorting / invariant hardening", plan)

    def test_p7_plan_is_accepted_closeout_record(self) -> None:
        plan_path = (
            ROOT
            / "docs"
            / "10-implementation"
            / "versions"
            / "v2.0"
            / "phase-plans"
            / "P7_Implementation_Plan.md"
        )
        self.assertTrue(plan_path.exists())
        plan = plan_path.read_text(encoding="utf-8")
        self.assertIn("accepted closeout record", plan)
        self.assertIn("Deployable Rerank, Allocator v2, and Execution Economics Closure", plan)
        self.assertIn("Phase 0` 到 `Phase 7` 已 accepted", plan)
        self.assertIn("pre_budget_deployable_expected_pnl", plan)
        self.assertIn("runtime.allocation_decisions", plan)
        self.assertIn("weather.weather_execution_priors", plan)
        self.assertIn("P8", plan)
        self.assertIn("allocator pass-1 structural preview", plan)

    def test_p8_plan_is_accepted_closeout_record(self) -> None:
        plan_path = (
            ROOT
            / "docs"
            / "10-implementation"
            / "versions"
            / "v2.0"
            / "phase-plans"
            / "P8_Implementation_Plan.md"
        )
        self.assertTrue(plan_path.exists())
        plan = plan_path.read_text(encoding="utf-8")
        self.assertIn("accepted closeout record", plan)
        self.assertIn("Calibration Hard Gates and Scaling-Aware Capital Discipline", plan)
        self.assertIn("Phase 0` 到 `Phase 8` 已 accepted", plan)
        self.assertIn("trading.capital_budget_policies", plan)
        self.assertIn("calibration_gate_status", plan)
        self.assertIn("tests.test_allocator_scaling_discipline_acceptance", plan)
        checklist = (
            ROOT
            / "docs"
            / "10-implementation"
            / "versions"
            / "v2.0"
            / "checklists"
            / "P8_Closeout_Checklist.md"
        )
        self.assertTrue(checklist.exists())

    def test_p9_plan_is_current_tranche_record(self) -> None:
        plan_path = (
            ROOT
            / "docs"
            / "10-implementation"
            / "versions"
            / "v2.0"
            / "phase-plans"
            / "P9_Implementation_Plan.md"
        )
        self.assertTrue(plan_path.exists())
        plan = plan_path.read_text(encoding="utf-8")
        self.assertIn("current tranche implementation plan", plan)
        self.assertIn("Operator Surface Delivery and Throughput Scaling", plan)
        self.assertIn("runtime.operator_surface_refresh_runs", plan)
        self.assertIn("ui.surface_delivery_summary", plan)
        self.assertIn("weather_operator_surface_refresh", plan)

    def test_p10_and_p11_are_accepted_records(self) -> None:
        p10_path = (
            ROOT
            / "docs"
            / "10-implementation"
            / "versions"
            / "v2.0"
            / "phase-plans"
            / "P10_Implementation_Plan.md"
        )
        p11_path = (
            ROOT
            / "docs"
            / "10-implementation"
            / "versions"
            / "v2.0"
            / "phase-plans"
            / "P11_Implementation_Plan.md"
        )
        self.assertTrue(p10_path.exists())
        self.assertTrue(p11_path.exists())
        p10 = p10_path.read_text(encoding="utf-8")
        p11 = p11_path.read_text(encoding="utf-8")
        self.assertIn("accepted closeout record", p10)
        self.assertIn("Deterministic ROI Repair and Execution Intelligence Foundation", p10)
        self.assertIn("00_0322_Asterion_Assessment.md", p10)
        self.assertIn("runtime.execution_intelligence_runs", p10)
        self.assertIn("run_operator_surface_refresh", p10)
        self.assertIn("tests.test_home_action_queue_excludes_blocked_items", p10)
        p10_checklist = (
            ROOT
            / "docs"
            / "10-implementation"
            / "versions"
            / "v2.0"
            / "checklists"
            / "P10_Closeout_Checklist.md"
        )
        self.assertTrue(p10_checklist.exists())
        self.assertIn("accepted closeout record", p11)
        self.assertIn("Opportunity Triage / Execution Intelligence Agent", p11)
        self.assertIn("Phase 10 accepted", p11)
        self.assertIn("ui.opportunity_triage_summary", p11)
        self.assertIn("agent.operator_review_decisions", p11)


if __name__ == "__main__":
    unittest.main()
