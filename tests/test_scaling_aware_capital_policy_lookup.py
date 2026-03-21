from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path

import duckdb

from asterion_core.risk.allocator_v1 import _load_active_capital_budget_policies, _lookup_capital_budget_policy


HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required")
class ScalingAwareCapitalPolicyLookupTest(unittest.TestCase):
    def _apply_migrations(self, db_path: str) -> None:
        con = duckdb.connect(db_path)
        try:
            migrations_dir = Path(__file__).resolve().parents[1] / "sql" / "migrations"
            for path in sorted(migrations_dir.glob("*.sql")):
                sql = path.read_text(encoding="utf-8").strip()
                if sql:
                    con.execute(sql)
        finally:
            con.close()

    def test_lookup_prefers_wallet_strategy_regime_and_gate_then_falls_back(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "capital_policy.duckdb")
            self._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                con.execute(
                    """
                    INSERT INTO trading.capital_budget_policies VALUES
                    ('cap_exact', 'wallet_weather_1', 'weather_primary', 'warm', 'review_required', 'active', 'cap_v1', 5.0, 2.0, 2, 1, 1.0, '2026-03-20 00:00:00', '2026-03-20 00:00:00'),
                    ('cap_regime', 'wallet_weather_1', 'weather_primary', 'warm', NULL, 'active', 'cap_v1', 7.0, 3.0, 3, 2, 1.0, '2026-03-20 00:00:00', '2026-03-20 00:00:00'),
                    ('cap_strategy', 'wallet_weather_1', 'weather_primary', NULL, NULL, 'active', 'cap_v1', 9.0, 4.0, 4, 3, 1.0, '2026-03-20 00:00:00', '2026-03-20 00:00:00')
                    """
                )
                policies = _load_active_capital_budget_policies(con, wallet_id="wallet_weather_1")
            finally:
                con.close()

        exact = _lookup_capital_budget_policy(
            policies,
            strategy_id="weather_primary",
            regime_bucket="warm",
            calibration_gate_status="review_required",
        )
        regime = _lookup_capital_budget_policy(
            policies,
            strategy_id="weather_primary",
            regime_bucket="warm",
            calibration_gate_status="clear",
        )
        fallback = _lookup_capital_budget_policy(
            policies,
            strategy_id="weather_primary",
            regime_bucket="cool",
            calibration_gate_status="clear",
        )

        assert exact is not None
        assert regime is not None
        assert fallback is not None
        self.assertEqual(exact["capital_policy_id"], "cap_exact")
        self.assertEqual(regime["capital_policy_id"], "cap_regime")
        self.assertEqual(fallback["capital_policy_id"], "cap_strategy")


if __name__ == "__main__":
    unittest.main()
