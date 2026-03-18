from __future__ import annotations

import unittest
from datetime import UTC, datetime

from asterion_core.contracts import (
    SUBMITTER_BOUNDARY_ATTESTATION_KIND_V2,
    SubmitterBoundaryInputs,
    compute_boundary_attestation_mac,
    compute_boundary_decision_fingerprint,
    evaluate_submitter_boundary,
    mint_submitter_boundary_attestation_v2,
)


def _inputs(**overrides) -> SubmitterBoundaryInputs:
    payload = {
        "request_id": "subreq_live_1",
        "wallet_id": "wallet_weather_1",
        "source_attempt_id": "sign_1",
        "ticket_id": "tt_1",
        "execution_context_id": "ectx_1",
        "submit_mode": "live_submit",
        "submitter_backend_kind": "real_clob_submit",
        "signer_backend_kind": "env_private_key_tx",
        "chain_tx_backend_kind": "real_broadcast",
        "submitter_endpoint_fingerprint": "fingerprint_1",
        "manifest_payload": {
            "manifest_status": "valid",
            "controlled_live_mode": "manual_only",
            "allowed_wallet_ids": ["wallet_weather_1"],
        },
        "manifest_path": "data/meta/controlled_live_capability_manifest.json",
        "readiness_report_payload": {
            "go_decision": "GO",
            "decision_reason": "ready for controlled live rollout decision",
        },
        "wallet_readiness_status": "ready",
        "approval_token_matches": True,
        "armed": True,
        "evaluated_at": datetime(2026, 3, 16, 10, 0, tzinfo=UTC),
    }
    payload.update(overrides)
    return SubmitterBoundaryInputs(**payload)


class SubmitterBoundaryAttestationTest(unittest.TestCase):
    def test_approved_path(self) -> None:
        attestation = mint_submitter_boundary_attestation_v2(
            evaluate_submitter_boundary(_inputs()),
            attestation_secret="phase10-test-secret",
            issued_at=datetime(2026, 3, 16, 10, 0, tzinfo=UTC),
        )
        self.assertEqual(attestation.attestation_status, "approved")
        self.assertEqual(attestation.reason_codes, [])
        self.assertEqual(attestation.attestation_kind, SUBMITTER_BOUNDARY_ATTESTATION_KIND_V2)
        self.assertIsNotNone(attestation.attestation_mac)

    def test_manifest_missing(self) -> None:
        attestation = evaluate_submitter_boundary(_inputs(manifest_payload=None))
        self.assertEqual(attestation.attestation_status, "blocked")
        self.assertIn("manifest_missing", attestation.reason_codes)

    def test_manifest_invalid(self) -> None:
        attestation = evaluate_submitter_boundary(_inputs(manifest_payload={"manifest_status": "invalid"}))
        self.assertEqual(attestation.attestation_status, "blocked")
        self.assertIn("manifest_invalid", attestation.reason_codes)

    def test_backend_kind_mismatch(self) -> None:
        attestation = evaluate_submitter_boundary(_inputs(submitter_backend_kind="shadow_stub"))
        self.assertEqual(attestation.attestation_status, "blocked")
        self.assertIn("submitter_backend_not_real_clob_submit", attestation.reason_codes)

    def test_approval_token_mismatch(self) -> None:
        attestation = evaluate_submitter_boundary(_inputs(approval_token_matches=False))
        self.assertEqual(attestation.attestation_status, "blocked")
        self.assertIn("approval_token_mismatch", attestation.reason_codes)

    def test_readiness_not_go(self) -> None:
        attestation = evaluate_submitter_boundary(_inputs(readiness_report_payload={"go_decision": "NO_GO"}))
        self.assertEqual(attestation.attestation_status, "blocked")
        self.assertIn("p4_live_prereq_not_go", attestation.reason_codes)

    def test_wallet_not_ready(self) -> None:
        attestation = evaluate_submitter_boundary(_inputs(wallet_readiness_status="allowance_action_required"))
        self.assertEqual(attestation.attestation_status, "blocked")
        self.assertIn("wallet_not_ready", attestation.reason_codes)

    def test_endpoint_fingerprint_missing(self) -> None:
        attestation = evaluate_submitter_boundary(_inputs(submitter_endpoint_fingerprint=None))
        self.assertEqual(attestation.attestation_status, "blocked")
        self.assertIn("submitter_endpoint_fingerprint_mismatch", attestation.reason_codes)

    def test_v2_attestation_uses_decision_fingerprint_and_mac(self) -> None:
        attestation = mint_submitter_boundary_attestation_v2(
            evaluate_submitter_boundary(_inputs()),
            attestation_secret="phase10-test-secret",
            issued_at=datetime(2026, 3, 16, 10, 0, tzinfo=UTC),
        )
        self.assertEqual(
            attestation.decision_fingerprint,
            compute_boundary_decision_fingerprint(attestation.attestation_payload_json),
        )
        self.assertEqual(
            attestation.attestation_mac,
            compute_boundary_attestation_mac(
                secret="phase10-test-secret",
                issuer=str(attestation.issuer),
                attestation_id=attestation.attestation_id,
                nonce=str(attestation.nonce),
                issued_at=attestation.issued_at,
                expires_at=attestation.expires_at,
                decision_fingerprint=str(attestation.decision_fingerprint),
            ),
        )

    def test_v2_attestation_blocks_when_secret_missing(self) -> None:
        attestation = mint_submitter_boundary_attestation_v2(
            evaluate_submitter_boundary(_inputs()),
            attestation_secret="",
            issued_at=datetime(2026, 3, 16, 10, 0, tzinfo=UTC),
        )
        self.assertEqual(attestation.attestation_status, "blocked")
        self.assertIn("attestation_secret_missing", attestation.reason_codes)


if __name__ == "__main__":
    unittest.main()
