from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

import duckdb

from asterion_core.storage.database import DuckDBConfig, connect_duckdb
from asterion_core.ui import build_ui_lite_db_once
from dagster_asterion.handlers import run_operator_surface_refresh
from domains.weather.opportunity.execution_intelligence import (
    load_execution_intelligence_summary,
    persist_execution_intelligence_materialization,
)


HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required")
class ExecutionIntelligenceSummaryTest(unittest.TestCase):
    def _apply_migrations(self, db_path: str) -> None:
        bootstrap = duckdb.connect(db_path)
        migrations_dir = Path(__file__).resolve().parents[1] / "sql" / "migrations"
        for path in sorted(migrations_dir.glob("*.sql")):
            sql = path.read_text(encoding="utf-8").strip()
            if sql:
                bootstrap.execute(sql)
        bootstrap.close()

    def _insert_ticket_case(
        self,
        con,
        *,
        market_id: str,
        ticket_id: str,
        order_id: str,
        spread_bps: int,
        depth_proxy: float,
        fill_size: float,
        order_size: float = 10.0,
        side: str = "BUY",
    ) -> None:
        provenance = json.dumps(
            {
                "pricing_context": {
                    "spread_bps": spread_bps,
                    "depth_proxy": depth_proxy,
                    "source_freshness_status": "fresh" if spread_bps <= 60 else "stale",
                }
            }
        )
        con.execute(
            """
            INSERT INTO runtime.trade_tickets (
                ticket_id, run_id, strategy_id, strategy_version, market_id, token_id, outcome, side, reference_price,
                fair_value, edge_bps, threshold_bps, route_action, size, signal_ts_ms, forecast_run_id,
                watch_snapshot_id, request_id, ticket_hash, provenance_json, created_at, wallet_id, execution_context_id
            ) VALUES (
                ?, 'run_1', 'weather_primary', 'v1', ?, 'tok_yes', 'YES', ?, 0.40, 0.58,
                900, 100, 'FAK', ?, 1710000000000, 'frun_1', ?, ?, ?, ?, '2026-03-20 10:00:00', 'wallet_weather_1', ?
            )
            """,
            [ticket_id, market_id, side, order_size, f"snap_{ticket_id}", f"req_{ticket_id}", f"hash_{ticket_id}", provenance, f"ectx_{ticket_id}"],
        )
        con.execute(
            """
            INSERT INTO runtime.submit_attempts (
                attempt_id, request_id, ticket_id, order_id, wallet_id, execution_context_id,
                exchange, attempt_kind, attempt_mode, canonical_order_hash, payload_hash,
                submit_payload_json, signed_payload_ref, status, error, created_at
            ) VALUES (?, ?, ?, ?, 'wallet_weather_1', ?, 'polymarket_clob', 'submit_order', 'live_submit', ?, ?, '{}', ?, 'accepted', NULL, '2026-03-20 10:00:05')
            """,
            [f"attempt_{ticket_id}", f"subreq_{ticket_id}", ticket_id, order_id, f"ectx_{ticket_id}", f"coh_{ticket_id}", f"ph_{ticket_id}", f"sign_{ticket_id}"],
        )
        con.execute(
            """
            INSERT INTO runtime.gate_decisions (
                gate_id, ticket_id, allowed, reason, reason_codes_json, metrics_json, created_at
            ) VALUES (?, ?, TRUE, 'passed', '[]', '{}', '2026-03-20 10:00:01')
            """,
            [f"gate_{ticket_id}", ticket_id],
        )
        con.execute(
            """
            INSERT INTO runtime.external_order_observations (
                observation_id, attempt_id, request_id, ticket_id, order_id, wallet_id, execution_context_id,
                exchange, observation_kind, submit_mode, canonical_order_hash, external_order_id, external_status,
                observed_at, error, raw_observation_json
            ) VALUES (?, ?, ?, ?, ?, 'wallet_weather_1', ?, 'polymarket_clob', 'submit_order', 'live_submit', ?, ?, 'working', '2026-03-20 10:00:06', NULL, '{}')
            """,
            [f"eord_{ticket_id}", f"attempt_{ticket_id}", f"subreq_{ticket_id}", ticket_id, order_id, f"ectx_{ticket_id}", f"coh_{ticket_id}", f"ext_{order_id}"],
        )
        con.execute(
            """
            INSERT INTO trading.orders (
                order_id, client_order_id, wallet_id, market_id, token_id, outcome, side, price, size, route_action,
                time_in_force, expiration, fee_rate_bps, signature_type, funder, status, filled_size, remaining_size,
                avg_fill_price, reservation_id, exchange_order_id, created_at, submitted_at, updated_at
            ) VALUES (
                ?, ?, 'wallet_weather_1', ?, 'tok_yes', 'YES', ?, 0.40, ?, 'FAK',
                'FAK', NULL, 30, 1, '0xfunder', ?, ?, ?, ?, ?, ?, '2026-03-20 10:00:00', '2026-03-20 10:00:05', '2026-03-20 10:01:00'
            )
            """,
            [
                order_id,
                f"client_{ticket_id}",
                market_id,
                side,
                order_size,
                "partial_filled" if fill_size < order_size else "filled",
                fill_size,
                max(order_size - fill_size, 0.0),
                0.41 if fill_size > 0 else None,
                f"res_{ticket_id}",
                f"paper_{ticket_id}",
            ],
        )
        if fill_size > 0:
            con.execute(
                """
                INSERT INTO trading.fills (
                    fill_id, order_id, wallet_id, market_id, token_id, outcome, side, price, size, fee,
                    fee_rate_bps, trade_id, exchange_order_id, filled_at
                ) VALUES (
                    ?, ?, 'wallet_weather_1', ?, 'tok_yes', 'YES', ?, 0.41, ?, 0.10,
                    30, ?, ?, '2026-03-20 10:01:00'
                )
                """,
                [f"fill_{ticket_id}", order_id, market_id, side, fill_size, f"trade_{ticket_id}", f"paper_{ticket_id}"],
            )

    def test_materialization_persists_runtime_and_ui_microstructure_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "execution_intelligence.duckdb")
            lite_path = str(Path(tmpdir) / "ui_lite.duckdb")
            self._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                self._insert_ticket_case(con, market_id="mkt_stable", ticket_id="tt_stable_1", order_id="ord_stable_1", spread_bps=30, depth_proxy=0.95, fill_size=10.0)
                self._insert_ticket_case(con, market_id="mkt_stable", ticket_id="tt_stable_2", order_id="ord_stable_2", spread_bps=35, depth_proxy=0.92, fill_size=10.0)
                self._insert_ticket_case(con, market_id="mkt_unstable", ticket_id="tt_unstable_1", order_id="ord_unstable_1", spread_bps=180, depth_proxy=0.35, fill_size=2.0)
                self._insert_ticket_case(con, market_id="mkt_unstable", ticket_id="tt_unstable_2", order_id="ord_unstable_2", spread_bps=160, depth_proxy=0.40, fill_size=0.0)
                run, summaries = persist_execution_intelligence_materialization(
                    con,
                    job_name="weather_operator_surface_refresh",
                    run_id="eirun_test",
                )
                stable = load_execution_intelligence_summary(con, market_id="mkt_stable", side="BUY")
                unstable = load_execution_intelligence_summary(con, market_id="mkt_unstable", side="BUY")
            finally:
                con.close()

            result = build_ui_lite_db_once(src_db_path=db_path, dst_db_path=lite_path)
            self.assertTrue(result.ok, result.error)
            lite_con = duckdb.connect(lite_path, read_only=True)
            try:
                ui_rows = lite_con.execute(
                    """
                    SELECT market_id, execution_intelligence_score, top_of_book_stability, spread_regime
                    FROM ui.market_microstructure_summary
                    ORDER BY market_id
                    """
                ).fetchall()
            finally:
                lite_con.close()

        self.assertEqual(run["run_id"], "eirun_test")
        self.assertEqual(len(summaries), 2)
        self.assertIsNotNone(stable)
        self.assertIsNotNone(unstable)
        assert stable is not None
        assert unstable is not None
        self.assertGreater(stable.execution_intelligence_score, unstable.execution_intelligence_score)
        self.assertGreater(stable.top_of_book_stability, unstable.top_of_book_stability)
        self.assertTrue(unstable.visible_size_shock_flag)
        self.assertIn("spread_regime:wide", unstable.reason_codes)
        self.assertIn(("mkt_stable", stable.execution_intelligence_score, stable.top_of_book_stability, "tight"), ui_rows)
        self.assertIn(("mkt_unstable", unstable.execution_intelligence_score, unstable.top_of_book_stability, "wide"), ui_rows)

    def test_operator_surface_refresh_persists_execution_intelligence_from_reader_connection(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "execution_intelligence_refresh.duckdb")
            replica_path = str(Path(tmpdir) / "ui_replica.duckdb")
            lite_path = str(Path(tmpdir) / "ui_lite.duckdb")
            self._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                self._insert_ticket_case(con, market_id="mkt_reader", ticket_id="tt_reader_1", order_id="ord_reader_1", spread_bps=30, depth_proxy=0.95, fill_size=10.0)
            finally:
                con.close()

            with unittest.mock.patch.dict(
                "os.environ",
                {
                    "ASTERION_DB_ROLE": "reader",
                    "ASTERION_STRICT_SINGLE_WRITER": "1",
                    "WRITERD": "0",
                },
                clear=False,
            ):
                guarded = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    metadata = run_operator_surface_refresh(
                        guarded,
                        job_name="test_execution_intelligence_refresh",
                        trigger_mode="manual",
                        ui_replica_db_path=replica_path,
                        ui_replica_meta_path=f"{replica_path}.meta.json",
                        ui_lite_db_path=lite_path,
                        ui_lite_meta_path=f"{lite_path}.meta.json",
                        readiness_report_json_path=str(Path(tmpdir) / "missing_readiness.json"),
                        readiness_evidence_json_path=str(Path(tmpdir) / "missing_evidence.json"),
                    )
                finally:
                    guarded.close()

            verify = duckdb.connect(db_path, read_only=True)
            try:
                counts = verify.execute(
                    "SELECT COUNT(*) FROM runtime.execution_intelligence_summaries"
                ).fetchone()
            finally:
                verify.close()

        self.assertGreater(metadata["execution_intelligence_summary_count"], 0)
        self.assertEqual(int(counts[0]), 1)


if __name__ == "__main__":
    unittest.main()
