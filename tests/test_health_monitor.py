from __future__ import annotations

import json
import os
import tempfile
import time
import unittest
from datetime import datetime
from pathlib import Path

from asterion_core.monitoring import (
    collect_chain_tx_health,
    collect_degrade_status,
    collect_external_execution_health,
    collect_live_prereq_health,
    collect_queue_health,
    collect_quote_health,
    collect_signer_health,
    collect_submitter_health,
    collect_system_health,
    collect_ws_health,
)
from asterion_core.storage.write_queue import WriteQueueConfig, enqueue_task, init_queue, mark_task_failed, mark_task_succeeded
from tests.test_p2_closeout import _apply_schema


class _Quote:
    def __init__(self, last_updated_ms: int) -> None:
        self.last_updated_ms = last_updated_ms


class _StateStore:
    def __init__(self, now_ms: int) -> None:
        self._ws_delay_samples_ms = [50, 100, 150]
        self.reconnect_count_1h = 2
        self.latest_quote_by_market_token = {
            "m1:t1": _Quote(now_ms - 100),
            "m2:t2": _Quote(now_ms - 10_000),
        }


class HealthMonitorTest(unittest.TestCase):
    def test_collect_ws_health_and_quote_health(self) -> None:
        now_ms = int(time.time() * 1000)
        state_store = _StateStore(now_ms)

        ws = collect_ws_health(state_store)
        quote = collect_quote_health(state_store, stale_threshold_ms=5_000)

        self.assertTrue(ws.connected)
        self.assertEqual(ws.reconnect_count_1h, 2)
        self.assertEqual(quote.active_markets, 2)
        self.assertEqual(quote.stale_markets, 1)

    def test_collect_queue_health_and_degrade_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            queue_path = os.path.join(tmpdir, "write_queue.sqlite")
            flag_path = os.path.join(tmpdir, "watch_only.json")
            cfg = WriteQueueConfig(path=queue_path)
            init_queue(cfg)

            task_id = enqueue_task(cfg, task_type="UPSERT_ROWS_V1", payload={"table": "x"})
            succeeded = enqueue_task(cfg, task_type="UPSERT_ROWS_V1", payload={"table": "y"})
            dead = enqueue_task(cfg, task_type="UPSERT_ROWS_V1", payload={"table": "z"}, max_attempts=1)
            mark_task_succeeded(cfg, task_id=succeeded)
            mark_task_failed(cfg, task_id=dead, error_message="boom")

            with open(flag_path, "w", encoding="utf-8") as handle:
                json.dump({"reason": "degraded", "since_ts_ms": 123, "watch_only": True}, handle)

            queue = collect_queue_health(queue_path)
            degrade = collect_degrade_status(flag_path)

            self.assertEqual(queue.pending_tasks, 1)
            self.assertGreaterEqual(queue.write_rate_per_min, 0.0)
            self.assertEqual(queue.dead_tasks_1h, 1)
            self.assertTrue(degrade.active)
            self.assertTrue(degrade.watch_only)
            self.assertEqual(degrade.reason, "degraded")

            system = collect_system_health(_StateStore(int(time.time() * 1000)), queue_path, flag_path, "unused.duckdb")
            self.assertTrue(system.degrade_status.active)

    def test_collect_live_prereq_health(self) -> None:
        import duckdb

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            queue_path = str(Path(tmpdir) / "write_queue.sqlite")
            _apply_schema(db_path)
            cfg = WriteQueueConfig(path=queue_path)
            init_queue(cfg)
            con = duckdb.connect(db_path)
            try:
                con.execute(
                    """
                    INSERT INTO meta.signature_audit_logs (
                        log_id, request_id, signature_type, payload_hash, signature, status, requester, timestamp, error,
                        wallet_type, signer_address, funder, api_key_ref, chain_id, token_id, fee_rate_bps, signing_purpose, created_at
                    ) VALUES (
                        'sig_1', 'req_1', '1', 'ph', 'sig', 'succeeded', 'operator', ?, NULL,
                        'eoa', '0xfunder', '0xfunder', NULL, 137, 'tok_yes', 30, 'order', ?
                    )
                    """,
                    [datetime(2026, 3, 12, 10, 0), datetime(2026, 3, 12, 10, 0)],
                )
                con.execute(
                    """
                    INSERT INTO runtime.submit_attempts (
                        attempt_id, request_id, ticket_id, order_id, wallet_id, execution_context_id, exchange,
                        attempt_kind, attempt_mode, canonical_order_hash, payload_hash, submit_payload_json,
                        signed_payload_ref, status, error, created_at
                    ) VALUES
                    ('sign_1', 'req_1', 'tt_1', 'ordr_1', 'wallet_weather_1', 'ectx_1', 'polymarket_clob',
                     'sign_order', 'sign_only', 'coh_1', 'ph_1', '{}', 'sign_1', 'signed', NULL, ?),
                    ('submit_1', 'req_2', 'tt_1', 'ordr_1', 'wallet_weather_1', 'ectx_1', 'polymarket_clob',
                     'submit_order', 'shadow_submit', 'coh_1', 'ph_2', '{}', 'sign_1', 'accepted', NULL, ?)
                    """,
                    [datetime(2026, 3, 12, 10, 5), datetime(2026, 3, 12, 10, 10)],
                )
                con.execute(
                    """
                    INSERT INTO runtime.chain_tx_attempts (
                        attempt_id, request_id, wallet_id, tx_kind, tx_mode, chain_id, funder, token_id, allowance_target,
                        nonce, gas_limit, max_fee_per_gas, max_priority_fee_per_gas, payload_hash, tx_payload_json,
                        signed_payload_ref, tx_hash, status, error, created_at
                    ) VALUES (
                        'ctx_1', 'req_3', 'wallet_weather_1', 'approve_usdc', 'shadow_broadcast', 137, '0xfunder', 'usdc_e',
                        '0xrelayer', 1, 120000, 100, 2, 'ph_3', '{}', 'sig_1', '0xtx', 'accepted', NULL, ?
                    )
                    """,
                    [datetime(2026, 3, 12, 10, 15)],
                )
                con.execute(
                    """
                    INSERT INTO runtime.external_order_observations (
                        observation_id, attempt_id, request_id, ticket_id, order_id, wallet_id, execution_context_id,
                        exchange, observation_kind, submit_mode, canonical_order_hash, external_order_id, external_status,
                        observed_at, error, raw_observation_json
                    ) VALUES (
                        'eord_1', 'submit_1', 'req_2', 'tt_1', 'ordr_1', 'wallet_weather_1', 'ectx_1', 'polymarket_clob',
                        'shadow_submit_ack', 'shadow_submit', 'coh_1', 'ext_1', 'accepted', ?, NULL, '{}'
                    )
                    """,
                    [datetime(2026, 3, 12, 10, 11)],
                )
                con.execute(
                    """
                    INSERT INTO runtime.external_fill_observations (
                        observation_id, attempt_id, request_id, ticket_id, order_id, wallet_id, execution_context_id, exchange,
                        observation_kind, external_order_id, external_trade_id, market_id, token_id, outcome, side, price, size,
                        fee, fee_rate_bps, external_status, observed_at, error, raw_observation_json
                    ) VALUES (
                        'efill_1', 'submit_1', 'req_2', 'tt_1', 'ordr_1', 'wallet_weather_1', 'ectx_1', 'polymarket_clob',
                        'shadow_fill_full', 'ext_1', 'trade_1', 'mkt_weather_1', 'tok_yes', 'YES', 'buy', 0.60, 10.00, 0.18, 30,
                        'filled', ?, NULL, '{}'
                    )
                    """,
                    [datetime(2026, 3, 12, 10, 12)],
                )
                con.execute(
                    """
                    INSERT INTO trading.reconciliation_results (
                        reconciliation_id, wallet_id, funder, signature_type, asset_type, token_id, market_id, balance_type,
                        local_quantity, remote_quantity, discrepancy, status, resolution, created_at, order_id, ticket_id,
                        execution_context_id, reconciliation_scope, source_system, local_state, remote_state,
                        external_order_observation_id, external_fill_observation_id, external_balance_observation_id
                    ) VALUES (
                        'recon_1', 'wallet_weather_1', '0xfunder', 1, 'execution', 'tok_yes', 'mkt_weather_1', 'position',
                        10.0, 10.0, 0.0, 'ok', 'external_execution_match', ?, 'ordr_1', 'tt_1', 'ectx_1',
                        'external_execution', 'shadow_submit', 'filled', 'accepted', 'eord_1', 'efill_agg', 'ebal_1'
                    )
                    """,
                    [datetime(2026, 3, 12, 10, 20)],
                )
            finally:
                con.close()

            con = duckdb.connect(db_path, read_only=True)
            try:
                signer = collect_signer_health(con)
                submitter = collect_submitter_health(con)
                chain_tx = collect_chain_tx_health(con)
                external = collect_external_execution_health(con)
                live = collect_live_prereq_health(con, queue_path=queue_path)
            finally:
                con.close()

            self.assertEqual(signer.request_count, 1)
            self.assertEqual(submitter.sign_only_signed_count, 1)
            self.assertEqual(submitter.submit_accepted_count, 1)
            self.assertEqual(chain_tx.approve_attempt_count, 1)
            self.assertEqual(external.external_order_observation_count, 1)
            self.assertEqual(external.external_fill_observation_count, 1)
            self.assertEqual(external.external_reconciliation_ok_count, 1)
            self.assertEqual(live.signer_health.request_count, 1)
            self.assertEqual(live.submitter_health.submit_accepted_count, 1)
            self.assertEqual(live.chain_tx_health.latest_approve_status, "accepted")


if __name__ == "__main__":
    unittest.main()
