from __future__ import annotations

import unittest
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

from asterion_core.blockchain import (
    ChainTxKind,
    ChainTxMode,
    ChainTxServiceShell,
    DisabledChainTxBackend,
    GasEstimate,
    NonceSelection,
    PolygonChainRegistry,
    ShadowBroadcastBackend,
    build_approve_usdc_request,
    build_chain_tx_attempt_record,
    build_transaction_signer_request,
)
from asterion_core.contracts import AccountTradingCapability


class _Reader:
    def select_nonce(self, funder: str) -> NonceSelection:
        if not funder.startswith("0x"):
            raise AssertionError("expected address-like funder")
        return NonceSelection(nonce=7)

    def estimate_approve_usdc_gas(self) -> GasEstimate:
        return GasEstimate(
            gas_limit=120000,
            max_fee_per_gas=100,
            max_priority_fee_per_gas=10,
        )


def _account_capability() -> AccountTradingCapability:
    return AccountTradingCapability(
        wallet_id="wallet_weather_1",
        wallet_type="eoa",
        signature_type=1,
        funder="0x1111111111111111111111111111111111111111",
        allowance_targets=["0x2222222222222222222222222222222222222222"],
        can_use_relayer=True,
        can_trade=True,
        restricted_reason=None,
    )


def _chain_registry() -> PolygonChainRegistry:
    return PolygonChainRegistry(
        chain_id=137,
        native_gas_asset_type="native_gas",
        native_gas_symbol="POL",
        native_gas_decimals=18,
        usdc_e_asset_type="usdc_e",
        usdc_e_token_id="usdc_e",
        usdc_e_contract_address="0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
        usdc_e_decimals=6,
        allowance_targets={"relayer": "0x2222222222222222222222222222222222222222"},
    )


class ChainTxScaffoldUnitTest(unittest.TestCase):
    def test_build_approve_usdc_request_is_stable(self) -> None:
        left = build_approve_usdc_request(
            account_capability=_account_capability(),
            chain_registry=_chain_registry(),
            chain_tx_reader=_Reader(),
            requester="operator",
            request_id="req_chain_1",
            timestamp=datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
            tx_mode=ChainTxMode.DRY_RUN,
            spender="0x2222222222222222222222222222222222222222",
            amount=Decimal("100"),
        )
        right = build_approve_usdc_request(
            account_capability=_account_capability(),
            chain_registry=_chain_registry(),
            chain_tx_reader=_Reader(),
            requester="operator",
            request_id="req_chain_1",
            timestamp=datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
            tx_mode=ChainTxMode.DRY_RUN,
            spender="0x2222222222222222222222222222222222222222",
            amount=Decimal("100"),
        )
        self.assertEqual(left.nonce, 7)
        self.assertEqual(left.gas_estimate.gas_limit, 120000)
        self.assertEqual(left, right)

    def test_unknown_spender_fails(self) -> None:
        with self.assertRaises(ValueError):
            build_approve_usdc_request(
                account_capability=_account_capability(),
                chain_registry=_chain_registry(),
                chain_tx_reader=_Reader(),
                requester="operator",
                request_id="req_chain_1",
                timestamp=datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
                tx_mode=ChainTxMode.DRY_RUN,
                spender="0x3333333333333333333333333333333333333333",
                amount=Decimal("100"),
            )

    def test_build_transaction_signer_request_uses_transaction_purpose(self) -> None:
        chain_request = build_approve_usdc_request(
            account_capability=_account_capability(),
            chain_registry=_chain_registry(),
            chain_tx_reader=_Reader(),
            requester="operator",
            request_id="req_chain_1",
            timestamp=datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
            tx_mode=ChainTxMode.DRY_RUN,
            spender="0x2222222222222222222222222222222222222222",
            amount=Decimal("100"),
        )
        signer_request = build_transaction_signer_request(chain_request, _account_capability())
        self.assertEqual(signer_request.context.signing_purpose.value, "transaction")
        self.assertEqual(signer_request.payload["tx_kind"], "approve_usdc")

    def test_disabled_backend_rejects(self) -> None:
        chain_request = build_approve_usdc_request(
            account_capability=_account_capability(),
            chain_registry=_chain_registry(),
            chain_tx_reader=_Reader(),
            requester="operator",
            request_id="req_chain_1",
            timestamp=datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
            tx_mode=ChainTxMode.SHADOW_BROADCAST,
            spender="0x2222222222222222222222222222222222222222",
            amount=Decimal("100"),
        )
        result = DisabledChainTxBackend().broadcast(chain_request, signed_payload_json={"signature": "stub"})
        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.error, "chain_tx_backend_disabled")

    def test_shadow_broadcast_generates_deterministic_tx_hash(self) -> None:
        chain_request = build_approve_usdc_request(
            account_capability=_account_capability(),
            chain_registry=_chain_registry(),
            chain_tx_reader=_Reader(),
            requester="operator",
            request_id="req_chain_1",
            timestamp=datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
            tx_mode=ChainTxMode.SHADOW_BROADCAST,
            spender="0x2222222222222222222222222222222222222222",
            amount=Decimal("100"),
        )
        left = ShadowBroadcastBackend().broadcast(chain_request, signed_payload_json={"signature": "stub"})
        right = ShadowBroadcastBackend().broadcast(chain_request, signed_payload_json={"signature": "stub"})
        self.assertEqual(left.status, "accepted")
        self.assertEqual(left.tx_hash, right.tx_hash)

    def test_chain_tx_service_rejects_non_enabled_kinds(self) -> None:
        chain_request = build_approve_usdc_request(
            account_capability=_account_capability(),
            chain_registry=_chain_registry(),
            chain_tx_reader=_Reader(),
            requester="operator",
            request_id="req_chain_1",
            timestamp=datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
            tx_mode=ChainTxMode.DRY_RUN,
            spender="0x2222222222222222222222222222222222222222",
            amount=Decimal("100"),
        )
        rejected_request = type(chain_request)(
            request_id=chain_request.request_id,
            requester=chain_request.requester,
            timestamp=chain_request.timestamp,
            wallet_id=chain_request.wallet_id,
            tx_kind=ChainTxKind.SPLIT,
            tx_mode=chain_request.tx_mode,
            chain_id=chain_request.chain_id,
            funder=chain_request.funder,
            spender=chain_request.spender,
            token_address=chain_request.token_address,
            token_id=chain_request.token_id,
            amount=chain_request.amount,
            nonce=chain_request.nonce,
            gas_estimate=chain_request.gas_estimate,
        )
        with patch("asterion_core.blockchain.chain_tx_v1.enqueue_journal_event_upserts", return_value="task_ctx_journal"):
            result = ChainTxServiceShell().submit_transaction(
                rejected_request,
                signed_payload_json={"signature": "stub"},
                queue_cfg=type("QueueCfg", (), {"path": ":memory:"})(),
                run_id="run_chain_1",
            )
        self.assertEqual(result.response.status, "rejected")
        self.assertEqual(result.response.error, "tx_kind_not_enabled_in_p4_07")

    def test_build_chain_tx_attempt_record_is_stable(self) -> None:
        chain_request = build_approve_usdc_request(
            account_capability=_account_capability(),
            chain_registry=_chain_registry(),
            chain_tx_reader=_Reader(),
            requester="operator",
            request_id="req_chain_1",
            timestamp=datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
            tx_mode=ChainTxMode.DRY_RUN,
            spender="0x2222222222222222222222222222222222222222",
            amount=Decimal("100"),
        )
        with patch("asterion_core.blockchain.chain_tx_v1.enqueue_journal_event_upserts", return_value="task_ctx_journal"):
            result = ChainTxServiceShell().submit_transaction(
                chain_request,
                signed_payload_json={"signature": "stub"},
                queue_cfg=type("QueueCfg", (), {"path": ":memory:"})(),
                run_id="run_chain_1",
            ).response
        left = build_chain_tx_attempt_record(chain_request, result, signed_payload_ref="txsref_1")
        right = build_chain_tx_attempt_record(chain_request, result, signed_payload_ref="txsref_1")
        self.assertEqual(left.attempt_id, right.attempt_id)
        self.assertEqual(left.payload_hash, right.payload_hash)


if __name__ == "__main__":
    unittest.main()
