from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "run_real_weather_chain_loop.py"


def _load_loop_module():
    spec = importlib.util.spec_from_file_location("real_weather_chain_loop", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load run_real_weather_chain_loop.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RealWeatherChainLoopTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.loop = _load_loop_module()

    def test_build_smoke_command_passes_canonical_db_path(self) -> None:
        args = self.loop.parse_args.__globals__["argparse"].Namespace(
            output_dir="data/dev/real_weather_chain",
            db_path="data/runtime.duckdb",
            recent_within_days=14,
            market_limit=24,
            triage_limit=12,
            skip_agent=False,
        )
        cmd = self.loop.build_smoke_command(args, force_rebuild=False)
        self.assertIn("--db-path", cmd)
        self.assertIn("data/runtime.duckdb", cmd)
        self.assertIn("--market-limit", cmd)
        self.assertIn("24", cmd)
        self.assertIn("--triage-limit", cmd)
        self.assertIn("12", cmd)

    def test_initializing_report_preserves_previous_market_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "real_weather_chain_report.json"
            previous = {
                "timestamp": "2026-03-15T00:00:00+00:00",
                "chain_status": "ok",
                "market_discovery": {
                    "status": "ok",
                    "input_mode": "live_weather_market_auto_horizon",
                    "discovered_count": 12,
                    "selected_market_count": 3,
                    "selected_horizon_days": 14,
                    "selected_markets": [
                        {"market_id": "mkt_1", "question": "Seattle market"},
                        {"market_id": "mkt_2", "question": "Miami market"},
                    ],
                    "note": "previous success",
                },
                "forecast_service": {"status": "ok"},
                "pricing_engine": {"status": "ok"},
                "opportunity_discovery": {"status": "ok"},
            }
            report_path.write_text(json.dumps(previous, ensure_ascii=False), encoding="utf-8")

            self.loop.write_status_report(
                report_path,
                chain_status="initializing",
                note="refresh in progress",
                recent_within_days=14,
                command=["python", "run_real_weather_chain_smoke.py"],
            )

            updated = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(updated["chain_status"], "ok")
            self.assertEqual(updated["refresh_state"], "initializing")
            self.assertEqual(updated["refresh_note"], "refresh in progress")
            self.assertEqual(updated["market_discovery"]["selected_market_count"], 3)
            self.assertEqual(len(updated["market_discovery"]["selected_markets"]), 2)
            self.assertEqual(updated["market_discovery"]["refresh_note"], "refresh in progress")


if __name__ == "__main__":
    unittest.main()
