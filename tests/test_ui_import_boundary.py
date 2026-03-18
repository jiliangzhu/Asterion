from __future__ import annotations

import importlib
import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class UiImportBoundaryTest(unittest.TestCase):
    def test_ui_lite_db_import_does_not_depend_on_top_level_ui_surface_truth(self) -> None:
        module = importlib.import_module("asterion_core.ui.ui_lite_db")
        self.assertEqual(module.annotate_frame_with_source_truth.__module__, "asterion_core.ui.surface_truth_shared")
        self.assertEqual(module.ensure_primary_score_fields.__module__, "asterion_core.ui.surface_truth_shared")

    def test_real_weather_chain_smoke_help_imports_cleanly(self) -> None:
        proc = subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "run_real_weather_chain_smoke.py"), "--help"],
            cwd=str(ROOT),
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(proc.returncode, 0, msg=proc.stderr or proc.stdout)


if __name__ == "__main__":
    unittest.main()
