from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from asterion_core.contracts import WeatherMarket
from asterion_core.execution import (
    SafeDefaultChainAccountCapabilityReader,
    WalletRegistryEntry,
    build_account_capability_from_sources,
    build_market_capability_from_sources,
    expand_market_tokens,
    load_wallet_registry,
)


def _weather_market(*, outcomes: list[str] | None = None, token_ids: list[str] | None = None) -> WeatherMarket:
    return WeatherMarket(
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        event_id="evt_weather_1",
        slug="nyc-high-temp-mar-8",
        title="Will the high temperature in New York City on March 8, 2026 be 50-59°F?",
        description="Template weather market",
        rules="Resolve to Yes if the observed high temperature is within range.",
        status="active",
        active=True,
        closed=False,
        archived=False,
        accepting_orders=True,
        enable_order_book=True,
        tags=["Weather", "Temperature"],
        outcomes=list(outcomes or ["Yes", "No"]),
        token_ids=list(token_ids or ["tok_yes", "tok_no"]),
        close_time=datetime(2026, 3, 8, 23, 59, 59),
        end_date=datetime(2026, 3, 8, 23, 59, 59),
        raw_market={"source": "unit_test"},
    )


class CapabilityRefreshUnitTest(unittest.TestCase):
    def test_expand_market_tokens_normalizes_binary_pairs(self) -> None:
        pairs = expand_market_tokens(_weather_market())
        self.assertEqual(pairs, [("tok_yes", "YES"), ("tok_no", "NO")])

    def test_expand_market_tokens_rejects_malformed_market(self) -> None:
        with self.assertRaises(ValueError):
            expand_market_tokens(_weather_market(outcomes=["Yes"], token_ids=["tok_yes", "tok_no"]))
        with self.assertRaises(ValueError):
            expand_market_tokens(_weather_market(outcomes=["Maybe", "No"]))

    def test_build_market_capability_merges_clob_and_overrides(self) -> None:
        capability = build_market_capability_from_sources(
            market=_weather_market(),
            token_id="tok_yes",
            outcome="Yes",
            book_summary={"tick_size": "0.01", "min_order_size": "1", "neg_risk": False},
            fee_rate_payload={"fee_rate_bps": 30},
            override_values={
                "tradable": "false",
                "tick_size": "0.005",
                "fees_enabled": "true",
            },
            observed_at=datetime(2026, 3, 11, 10, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(capability.token_id, "tok_yes")
        self.assertEqual(capability.outcome, "YES")
        self.assertEqual(capability.tick_size, Decimal("0.005"))
        self.assertEqual(capability.fee_rate_bps, 30)
        self.assertFalse(capability.tradable)
        self.assertTrue(capability.fees_enabled)
        self.assertEqual(capability.data_sources, ["gamma", "clob_public", "capability_overrides"])

    def test_build_market_capability_rejects_unknown_override_field(self) -> None:
        with self.assertRaises(ValueError):
            build_market_capability_from_sources(
                market=_weather_market(),
                token_id="tok_yes",
                outcome="YES",
                book_summary={"tick_size": "0.01", "min_order_size": "1", "neg_risk": False},
                fee_rate_payload={"fee_rate_bps": 30},
                override_values={"unknown_field": "x"},
                observed_at=datetime(2026, 3, 11, 10, 0, tzinfo=timezone.utc),
            )

    def test_build_account_capability_merges_wallet_chain_and_overrides(self) -> None:
        wallet_entry = WalletRegistryEntry(
            wallet_id="wallet_weather_1",
            wallet_type="eoa",
            signature_type=1,
            funder="0xfunder",
            can_use_relayer=True,
            allowance_targets=["0xseed"],
            enabled=True,
        )
        chain_state = type(
            "ChainStateStub",
            (),
            {"approved_targets": ["0xrelayer"], "can_trade": True, "restricted_reason": None},
        )()
        capability = build_account_capability_from_sources(
            wallet_entry=wallet_entry,
            chain_state=chain_state,
            override_values={"signature_type": "2", "can_use_relayer": "false"},
        )
        self.assertEqual(capability.signature_type, 2)
        self.assertFalse(capability.can_use_relayer)
        self.assertEqual(capability.allowance_targets, ["0xrelayer"])
        self.assertTrue(capability.can_trade)

    def test_build_account_capability_rejects_unknown_override_field(self) -> None:
        wallet_entry = WalletRegistryEntry(
            wallet_id="wallet_weather_1",
            wallet_type="eoa",
            signature_type=1,
            funder="0xfunder",
            can_use_relayer=True,
            allowance_targets=["0xseed"],
            enabled=True,
        )
        chain_state = type(
            "ChainStateStub",
            (),
            {"approved_targets": ["0xrelayer"], "can_trade": True, "restricted_reason": None},
        )()
        with self.assertRaises(ValueError):
            build_account_capability_from_sources(
                wallet_entry=wallet_entry,
                chain_state=chain_state,
                override_values={"wallet_type": "contract"},
            )

    def test_load_wallet_registry_parses_repo_json_shape(self) -> None:
        payload = {
            "wallets": [
                {
                    "wallet_id": "wallet_weather_1",
                    "wallet_type": "eoa",
                    "signature_type": 1,
                    "funder": "0xfunder",
                    "can_use_relayer": True,
                    "allowance_targets": ["0xrelayer"],
                    "enabled": True,
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "wallet_registry.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            entries = load_wallet_registry(path)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].wallet_id, "wallet_weather_1")
        self.assertEqual(entries[0].allowance_targets, ["0xrelayer"])

    def test_safe_default_chain_reader_is_fail_closed(self) -> None:
        reader = SafeDefaultChainAccountCapabilityReader()
        state = reader.read_account_state(
            WalletRegistryEntry(
                wallet_id="wallet_weather_1",
                wallet_type="eoa",
                signature_type=1,
                funder="0xfunder",
                can_use_relayer=True,
                allowance_targets=["0xrelayer"],
                enabled=True,
            )
        )
        self.assertEqual(state.approved_targets, [])
        self.assertFalse(state.can_trade)
        self.assertEqual(state.restricted_reason, "chain_read_unconfigured")
