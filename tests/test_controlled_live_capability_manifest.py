from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from asterion_core.monitoring import build_controlled_live_capability_manifest


class ControlledLiveCapabilityManifestTest(unittest.TestCase):
    def test_manifest_valid(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            policy_path = _write_policy(tmpdir)
            with patch.dict(
                os.environ,
                {
                    "ASTERION_CONTROLLED_LIVE_SECRET_ARMED": "false",
                    "ASTERION_CONTROLLED_LIVE_SECRET_APPROVAL_TOKEN": "live-token",
                    "ASTERION_CONTROLLED_LIVE_SECRET_PK_WALLET_WEATHER_1": "0xabc",
                },
                clear=True,
            ):
                manifest = build_controlled_live_capability_manifest(
                    policy_path=policy_path,
                    signer_backend_kind="env_private_key_tx",
                    chain_tx_backend_kind="real_broadcast",
                    submitter_backend_kind="shadow_stub",
                )
        self.assertEqual(manifest["manifest_status"], "valid")
        self.assertEqual(manifest["allowed_wallet_ids"], ["wallet_weather_1"])

    def test_policy_incomplete_is_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            policy_path = Path(tmpdir) / "policy.json"
            policy_path.write_text(json.dumps({"chain_id": 137, "wallets": [{"wallet_id": "wallet_weather_1"}]}), encoding="utf-8")
            manifest = build_controlled_live_capability_manifest(
                policy_path=policy_path,
                signer_backend_kind="env_private_key_tx",
                chain_tx_backend_kind="real_broadcast",
                submitter_backend_kind="shadow_stub",
            )
        self.assertEqual(manifest["manifest_status"], "invalid")
        self.assertTrue(any(str(item).startswith("policy_invalid:") for item in manifest["blockers"]))

    def test_backend_mismatch_is_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            policy_path = _write_policy(tmpdir)
            manifest = build_controlled_live_capability_manifest(
                policy_path=policy_path,
                signer_backend_kind="disabled",
                chain_tx_backend_kind="real_broadcast",
                submitter_backend_kind="shadow_stub",
            )
        self.assertEqual(manifest["manifest_status"], "invalid")
        self.assertIn("signer_backend_kind_mismatch:disabled", manifest["blockers"])

    def test_missing_secret_envs_are_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            policy_path = _write_policy(tmpdir)
            with patch.dict(os.environ, {}, clear=True):
                manifest = build_controlled_live_capability_manifest(
                    policy_path=policy_path,
                    signer_backend_kind="env_private_key_tx",
                    chain_tx_backend_kind="real_broadcast",
                    submitter_backend_kind="shadow_stub",
                )
        self.assertEqual(manifest["manifest_status"], "blocked")
        self.assertTrue(any("missing_secret_env:ASTERION_CONTROLLED_LIVE_SECRET_PK_WALLET_WEATHER_1" == item for item in manifest["blockers"]))


def _write_policy(tmpdir: str) -> str:
    path = Path(tmpdir) / "controlled_live_smoke.json"
    path.write_text(
        json.dumps(
            {
                "chain_id": 137,
                "wallets": [
                    {
                        "wallet_id": "wallet_weather_1",
                        "allowed_tx_kinds": ["approve_usdc"],
                        "allowed_spenders": ["0x2222222222222222222222222222222222222222"],
                        "max_approve_amount": "100",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return str(path)


if __name__ == "__main__":
    unittest.main()
