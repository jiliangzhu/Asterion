from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ui.runtime_env import export_ui_runtime_env_shell, resolve_ui_runtime_env


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


if __name__ == "__main__":
    unittest.main()
