from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ui.runtime_env import (
    export_ui_runtime_env_shell,
    hydrate_ui_runtime_env,
    load_ui_runtime_boundary_status,
    resolve_ui_runtime_env,
)


class UiRuntimeEnvTest(unittest.TestCase):
    def test_resolve_ui_runtime_env_only_keeps_allowlisted_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / ".env"
            env_path.write_text(
                "\n".join(
                    [
                        "ASTERION_UI_USERNAME=operator",
                        "ASTERION_UI_PASSWORD_HASH=hash123",
                        "ASTERION_OPENAI_COMPATIBLE_API_KEY=secret-should-not-leak",
                        "ASTERION_CONTROLLED_LIVE_SECRET_APPROVAL_TOKEN=secret-token",
                        "ASTERION_CONTROLLED_LIVE_CAPABILITY_MANIFEST_PATH=data/meta/manifest.json",
                        "ASTERION_REAL_WEATHER_CHAIN_REPORT_PATH=data/dev/report.json",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            with patch.dict(os.environ, {}, clear=True):
                payload = resolve_ui_runtime_env(env_path=env_path)
        self.assertEqual(payload["ASTERION_UI_USERNAME"], "operator")
        self.assertEqual(payload["ASTERION_UI_PASSWORD_HASH"], "hash123")
        self.assertEqual(
            payload["ASTERION_CONTROLLED_LIVE_CAPABILITY_MANIFEST_PATH"],
            "data/meta/manifest.json",
        )
        self.assertNotIn("ASTERION_OPENAI_COMPATIBLE_API_KEY", payload)
        self.assertNotIn("ASTERION_CONTROLLED_LIVE_SECRET_APPROVAL_TOKEN", payload)

    def test_current_environment_overrides_env_file_for_allowlisted_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / ".env"
            env_path.write_text("ASTERION_UI_USERNAME=file-user\nQWEN_MODEL=file-model\n", encoding="utf-8")
            with patch.dict(
                os.environ,
                {
                    "ASTERION_UI_USERNAME": "env-user",
                    "QWEN_MODEL": "env-model",
                    "ASTERION_CONTROLLED_LIVE_SECRET_PK_WALLET_WEATHER_1": "0xabc",
                },
                clear=True,
            ):
                payload = resolve_ui_runtime_env(env_path=env_path)
                exported = export_ui_runtime_env_shell(env_path=env_path)
        self.assertEqual(payload["ASTERION_UI_USERNAME"], "env-user")
        self.assertEqual(payload["QWEN_MODEL"], "env-model")
        self.assertNotIn("ASTERION_CONTROLLED_LIVE_SECRET_PK_WALLET_WEATHER_1", payload)
        self.assertIn("export ASTERION_UI_USERNAME='env-user'", exported)
        self.assertNotIn("CONTROLLED_LIVE_SECRET", exported)

    def test_banned_env_detection_blocks_ui_boundary(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ASTERION_UI_USERNAME": "operator",
                "ASTERION_CONTROLLED_LIVE_SECRET_APPROVAL_TOKEN": "secret-token",
            },
            clear=True,
        ):
            status = load_ui_runtime_boundary_status()
        self.assertEqual(status.status, "blocked")
        self.assertIn("controlled_live_secrets_present", status.banned_env_categories)

    def test_public_bind_requires_explicit_opt_in(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ASTERION_UI_BIND_ADDRESS": "0.0.0.0",
                "ASTERION_UI_USERNAME": "operator",
            },
            clear=True,
        ):
            status = load_ui_runtime_boundary_status()
        self.assertEqual(status.status, "blocked")
        self.assertIn("public_bind_requires_opt_in", status.reason_codes)

    def test_public_bind_allows_explicit_opt_in(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ASTERION_UI_BIND_ADDRESS": "0.0.0.0",
                "ASTERION_UI_ALLOW_PUBLIC_BIND": "true",
                "ASTERION_UI_USERNAME": "operator",
            },
            clear=True,
        ):
            status = load_ui_runtime_boundary_status()
        self.assertEqual(status.status, "ok")
        self.assertEqual(status.bind_scope, "public")

    def test_hydrate_ui_runtime_env_populates_missing_allowlisted_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / ".env"
            env_path.write_text(
                "\n".join(
                    [
                        "ASTERION_UI_USERNAME=operator",
                        "ASTERION_UI_PASSWORD_HASH=hash123",
                        "ASTERION_OPENAI_COMPATIBLE_API_KEY=secret-should-not-leak",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            with patch.dict(os.environ, {}, clear=True):
                payload = hydrate_ui_runtime_env(env_path=env_path)
                self.assertEqual(os.environ["ASTERION_UI_USERNAME"], "operator")
                self.assertEqual(os.environ["ASTERION_UI_PASSWORD_HASH"], "hash123")
                self.assertNotIn("ASTERION_OPENAI_COMPATIBLE_API_KEY", os.environ)
                self.assertEqual(payload["ASTERION_UI_USERNAME"], "operator")


if __name__ == "__main__":
    unittest.main()
