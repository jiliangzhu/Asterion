from __future__ import annotations

import unittest

from asterion_core.runtime import StrategyContext


class StrategyBaseTest(unittest.TestCase):
    def test_strategy_context_constructs_and_exposes_compat_alias(self) -> None:
        ctx = StrategyContext(
            data_snapshot_id="snap-1",
            universe_snapshot_id="uni-1",
            asof_ts_ms=123_000,
            dq_level="PASS",
            quote_snapshot_refs=["/tmp/q1.parquet"],
        )
        self.assertEqual(ctx.quote_snapshot_refs, ["/tmp/q1.parquet"])
        self.assertEqual(ctx.bbo_parquet_files, ["/tmp/q1.parquet"])

    def test_strategy_context_rejects_invalid_dq_level(self) -> None:
        with self.assertRaises(ValueError):
            StrategyContext(
                data_snapshot_id="snap-1",
                universe_snapshot_id=None,
                asof_ts_ms=123_000,
                dq_level="BAD",
                quote_snapshot_refs=[],
            )


if __name__ == "__main__":
    unittest.main()
