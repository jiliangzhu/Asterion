from __future__ import annotations

import json
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from asterion_core.monitoring import (
    ReadinessEvidenceBundle,
    ReadinessGateResult,
    ReadinessReport,
    ReadinessTarget,
    build_readiness_evidence_bundle,
    load_readiness_evidence_bundle,
    write_readiness_evidence_bundle,
)


class ReadinessEvidenceBundleTest(unittest.TestCase):
    def test_bundle_propagates_capability_boundary_and_paths(self) -> None:
        report = ReadinessReport(
            target=ReadinessTarget.P4_LIVE_PREREQUISITES,
            generated_at=datetime(2026, 3, 15, 10, 0, tzinfo=UTC),
            all_passed=True,
            go_decision="GO",
            decision_reason="all readiness gates passed; ready for controlled live rollout decision",
            data_hash="hash_1",
            gate_results=[
                ReadinessGateResult(
                    gate_name="live_prereq_operator_surface",
                    passed=True,
                    checks={"ok": True},
                    violations=[],
                    warnings=[],
                    metadata={},
                )
            ],
            capability_boundary_summary={
                "manual_only": True,
                "default_off": True,
                "approve_usdc_only": True,
                "shadow_submitter_only": True,
                "manifest_status": "valid",
            },
            capability_manifest_path="data/meta/controlled_live_capability_manifest.json",
            capability_manifest_status="valid",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            report_json = root / "readiness.json"
            report_md = root / "readiness.md"
            manifest = root / "manifest.json"
            report_json.write_text("{}", encoding="utf-8")
            report_md.write_text("# readiness", encoding="utf-8")
            manifest.write_text("{}", encoding="utf-8")
            bundle = build_readiness_evidence_bundle(
                report,
                readiness_report_json_path=str(report_json),
                readiness_report_markdown_path=str(report_md),
                capability_manifest_path=str(manifest),
            )
        self.assertEqual(bundle.go_decision, "GO")
        self.assertEqual(bundle.capability_manifest_status, "valid")
        self.assertIn("readiness_report_json", bundle.evidence_paths)
        self.assertEqual(bundle.latest_verification_summary["failed_gate_names"], [])

    def test_bundle_marks_missing_manifest_as_blocker(self) -> None:
        report = ReadinessReport(
            target=ReadinessTarget.P4_LIVE_PREREQUISITES,
            generated_at=datetime(2026, 3, 15, 10, 0, tzinfo=UTC),
            all_passed=True,
            go_decision="GO",
            decision_reason="ok",
            data_hash="hash_2",
            gate_results=[],
            capability_manifest_status="missing",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            report_json = root / "readiness.json"
            report_md = root / "readiness.md"
            report_json.write_text("{}", encoding="utf-8")
            report_md.write_text("# readiness", encoding="utf-8")
            bundle = build_readiness_evidence_bundle(
                report,
                readiness_report_json_path=str(report_json),
                readiness_report_markdown_path=str(report_md),
                capability_manifest_path=str(root / "missing-manifest.json"),
            )
        self.assertIn("capability_manifest:missing", bundle.blockers)
        self.assertIn("missing:capability_manifest", bundle.blockers)

    def test_bundle_round_trip(self) -> None:
        bundle = ReadinessEvidenceBundle(
            schema_version="v1",
            generated_at=datetime(2026, 3, 15, 10, 0, tzinfo=UTC),
            go_decision="NO_GO",
            decision_reason="blocked",
            capability_boundary_summary={"manual_only": True},
            capability_manifest_path="manifest.json",
            capability_manifest_status="invalid",
            dependency_statuses={"manifest": {"status": "missing"}},
            artifact_freshness={"manifest": {"age_seconds": None}},
            latest_verification_summary={"gate_count": 1},
            stale_dependencies=["manifest"],
            blockers=["missing:manifest"],
            warnings=["stale:weather_smoke_report"],
            evidence_paths={"manifest": "manifest.json"},
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            artifact = Path(tmpdir) / "bundle.json"
            write_readiness_evidence_bundle(bundle, json_path=str(artifact))
            loaded = load_readiness_evidence_bundle(artifact)
            payload = json.loads(artifact.read_text(encoding="utf-8"))
        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertEqual(loaded.go_decision, "NO_GO")
        self.assertEqual(loaded.blockers, ["missing:manifest"])
        self.assertEqual(payload["schema_version"], "v1")


if __name__ == "__main__":
    unittest.main()
