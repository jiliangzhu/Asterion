from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "refresh_operator_console_surfaces.py"


def _load_script_module():
    spec = importlib.util.spec_from_file_location("refresh_operator_console_surfaces", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load refresh_operator_console_surfaces.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RefreshOperatorConsoleSurfacesScriptTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.script = _load_script_module()

    def test_script_calls_explicit_operator_surface_refresh_job(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "asterion.duckdb"
            db_path.touch()
            runtime = SimpleNamespace(
                resolve_ui_replica_db_path=lambda: str(Path(tmpdir) / "ui_replica.duckdb"),
                resolve_ui_replica_meta_path=lambda: str(Path(tmpdir) / "ui_replica.meta.json"),
                resolve_ui_lite_db_path=lambda: str(Path(tmpdir) / "ui_lite.duckdb"),
                resolve_ui_lite_meta_path=lambda: str(Path(tmpdir) / "ui_lite.meta.json"),
                resolve_readiness_report_json_path=lambda: str(Path(tmpdir) / "readiness.json"),
                resolve_readiness_evidence_json_path=lambda: str(Path(tmpdir) / "readiness_evidence.json"),
            )
            fake_con = MagicMock()
            result = SimpleNamespace(job_name="weather_operator_surface_refresh", metadata={"surface_refresh_run_id": "refresh_1"})
            with patch.object(
                self.script.AsterionColdPathSettings,
                "from_env",
                return_value=SimpleNamespace(db_path=str(db_path), write_queue_path=str(Path(tmpdir) / "write_queue.sqlite")),
            ), patch.object(
                self.script,
                "LivePrereqReadinessRuntimeResource",
                return_value=runtime,
            ), patch.object(
                self.script,
                "connect_duckdb",
                return_value=fake_con,
            ), patch.object(
                self.script,
                "init_queue",
            ), patch.object(
                self.script,
                "run_weather_operator_surface_refresh_job",
                return_value=result,
            ) as refresh_mock:
                exit_code = self.script.main()
        self.assertEqual(exit_code, 0)
        refresh_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
