from __future__ import annotations

import hashlib
import os
import unittest
from unittest.mock import patch

from ui.auth import ui_auth_config_status, verify_ui_credentials


class UiAuthTest(unittest.TestCase):
    def test_missing_credentials_denies(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(ui_auth_config_status(), "missing_credentials")
            self.assertEqual(verify_ui_credentials("operator", "secret"), "missing_credentials")

    def test_invalid_credentials_denies(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ASTERION_UI_USERNAME": "operator",
                "ASTERION_UI_PASSWORD_HASH": hashlib.sha256("secret".encode()).hexdigest(),
            },
            clear=True,
        ):
            self.assertEqual(verify_ui_credentials("operator", "wrong"), "invalid_credentials")

    def test_valid_credentials_pass(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ASTERION_UI_USERNAME": "operator",
                "ASTERION_UI_PASSWORD_HASH": hashlib.sha256("secret".encode()).hexdigest(),
            },
            clear=True,
        ):
            self.assertEqual(ui_auth_config_status(), "configured")
            self.assertEqual(verify_ui_credentials("operator", "secret"), "authenticated")


if __name__ == "__main__":
    unittest.main()
