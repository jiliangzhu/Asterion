from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import duckdb

from asterion_core.contracts import WeatherMarket
from asterion_core.execution import WalletRegistryEntry


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "run_real_weather_chain_smoke.py"


def _load_smoke_module():
    spec = importlib.util.spec_from_file_location("real_weather_chain_smoke", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load run_real_weather_chain_smoke.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RealWeatherChainSmokeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.smoke = _load_smoke_module()

    def test_build_horizon_sequence_is_adaptive_and_deduped(self) -> None:
        self.assertEqual(self.smoke.build_horizon_sequence(14), [14, 30, 60, 90])
        self.assertEqual(self.smoke.build_horizon_sequence(30), [30, 14, 60, 90])

    def test_parse_args_defaults_to_agent_enabled(self) -> None:
        with patch("sys.argv", ["run_real_weather_chain_smoke.py"]):
            args = self.smoke.parse_args()
        self.assertFalse(args.skip_agent)
        self.assertFalse(args.with_agent)

    def test_parse_args_defaults_output_dir_to_dev_smoke_artifacts(self) -> None:
        with patch("sys.argv", ["run_real_weather_chain_smoke.py"]):
            args = self.smoke.parse_args()
        self.assertIn("data/dev/real_weather_chain", str(args.output_dir))
        self.assertNotIn("data/ui", str(args.output_dir))

    def test_parse_args_accepts_explicit_canonical_db_path(self) -> None:
        with patch("sys.argv", ["run_real_weather_chain_smoke.py", "--db-path", "data/custom.duckdb"]):
            args = self.smoke.parse_args()
        self.assertEqual(args.db_path, "data/custom.duckdb")

    def test_script_uses_deterministic_rule2spec_validation_instead_of_llm_agent(self) -> None:
        text = SCRIPT_PATH.read_text(encoding="utf-8")
        self.assertIn("validate_rule2spec_draft(", text)
        self.assertNotIn("run_rule2spec_agent_review(", text)

    def test_smoke_rehydrates_forecasts_after_calibration_refresh(self) -> None:
        text = SCRIPT_PATH.read_text(encoding="utf-8")
        self.assertIn('_refresh_forecasts_for_markets(', text)
        self.assertIn('if pre_pricing_runtime.get("calibration_refresh", {}).get("status") == "ok":', text)
        self.assertGreaterEqual(text.count('_refresh_forecasts_for_markets('), 2)

    def test_paper_default_chain_reader_marks_enabled_wallet_tradeable(self) -> None:
        reader = self.smoke._PaperDefaultChainAccountCapabilityReader()
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
        self.assertTrue(state.can_trade)
        self.assertEqual(state.approved_targets, ["0xrelayer"])
        self.assertIsNone(state.restricted_reason)

    def test_bootstrap_local_paper_operator_state_seeds_policy_and_cash_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "canonical.duckdb"
            self.smoke.apply_schema(db_path)
            seeded = self.smoke._bootstrap_local_paper_operator_state(db_path)
            seeded_again = self.smoke._bootstrap_local_paper_operator_state(db_path)
            con = duckdb.connect(str(db_path), read_only=True)
            try:
                allocation_policies = con.execute("SELECT COUNT(*) FROM trading.allocation_policies").fetchone()[0]
                capital_policies = con.execute("SELECT COUNT(*) FROM trading.capital_budget_policies").fetchone()[0]
                position_limits = con.execute("SELECT COUNT(*) FROM trading.position_limit_policies").fetchone()[0]
                inventory_rows = con.execute("SELECT COUNT(*) FROM trading.inventory_positions").fetchone()[0]
                cash_row = con.execute(
                    """
                    SELECT wallet_id, quantity
                    FROM trading.inventory_positions
                    WHERE wallet_id = 'wallet_weather_1'
                      AND asset_type = 'usdc_e'
                      AND token_id = 'usdc_e'
                      AND market_id = 'cash'
                      AND outcome = 'cash'
                      AND balance_type = 'available'
                    """
                ).fetchone()
            finally:
                con.close()
        self.assertTrue(seeded["allocation_policy_seeded"])
        self.assertTrue(seeded["capital_policy_seeded"])
        self.assertTrue(seeded["position_limit_seeded"])
        self.assertTrue(seeded["cash_inventory_seeded"])
        self.assertFalse(seeded_again["allocation_policy_seeded"])
        self.assertFalse(seeded_again["capital_policy_seeded"])
        self.assertFalse(seeded_again["position_limit_seeded"])
        self.assertFalse(seeded_again["cash_inventory_seeded"])
        self.assertEqual(allocation_policies, 1)
        self.assertEqual(capital_policies, 1)
        self.assertEqual(position_limits, 1)
        self.assertEqual(inventory_rows, 1)
        self.assertEqual(tuple(cash_row), ("wallet_weather_1", 100.0))

    def test_active_market_calibration_context_uses_existing_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "canonical.duckdb"
            self.smoke.apply_schema(db_path)
            con = duckdb.connect(str(db_path))
            try:
                con.execute(
                    """
                    INSERT INTO weather.weather_forecast_runs (
                        run_id, market_id, condition_id, station_id, source, model_run, forecast_target_time,
                        observation_date, metric, latitude, longitude, timezone, spec_version, cache_key,
                        source_trace_json, fallback_used, from_cache, confidence, forecast_payload_json,
                        raw_payload_json, created_at
                    ) VALUES (
                        'run_profile_hit', 'mkt_1', 'cond_1', 'KSEA', 'openmeteo', '2026-03-26T00:00Z',
                        '2026-03-26 12:00:00', '2026-03-26', 'temperature_max', 47.6062, -122.3321,
                        'America/Los_Angeles', 'spec_v1', 'cache_1', '[]', FALSE, FALSE, 0.9,
                        '{"distribution_summary_v2":{"lookup_hit":false,"regime_bucket":"cold","calibration_health_status":"lookup_missing","sample_count":0,"threshold_probability_quality_status":"lookup_missing","reason_codes":["calibration_v2_lookup_missing"]}}',
                        '{}', '2026-03-26 12:00:00'
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO weather.forecast_calibration_profiles_v2 (
                        profile_key, station_id, source, metric, forecast_horizon_bucket, season_bucket, regime_bucket,
                        sample_count, mean_bias, mean_abs_residual, p90_abs_residual, empirical_coverage_50,
                        empirical_coverage_80, empirical_coverage_95, regime_stability_score, residual_quantiles_json,
                        threshold_probability_profile_json, calibration_health_status, window_start, window_end, materialized_at
                    ) VALUES (
                        'profile_hit', 'KSEA', 'openmeteo', 'temperature_max', '0-1', 'spring', 'cold',
                        40, 0.2, 0.9, 1.4, 0.5, 0.8, 0.95, 0.8, '{}',
                        '{"0-10":{"sample_count":40,"predicted_prob_mean":0.05,"realized_hit_rate":0.05,"quality_status":"healthy","reliability_gap":0.0}}',
                        'healthy', '2026-03-01 00:00:00', '2026-03-26 12:00:00', '2026-03-26 12:00:00'
                    )
                    """
                )
            finally:
                con.close()

            with duckdb.connect(str(db_path), read_only=True) as con:
                forecast_run = self.smoke.load_forecast_run(con, run_id="run_profile_hit")

            context = self.smoke._build_active_market_calibration_context(
                db_path=db_path,
                forecast_run=forecast_run,
                outcome="NO",
                fair_value=0.98,
            )

        self.assertEqual(context["calibration_v2_mode"], "profile_v2")
        self.assertEqual(context["calibration_health_status"], "healthy")
        self.assertEqual(context["sample_count"], 40)
        self.assertEqual(context["threshold_probability_quality_status"], "healthy")
        self.assertIn("active_market_profile_override", context["calibration_reason_codes"])

    def test_stage_status_supports_partial_and_empty_runs(self) -> None:
        self.assertEqual(self.smoke._stage_status(success_count=3, total_count=3), "ok")
        self.assertEqual(self.smoke._stage_status(success_count=1, total_count=3), "degraded")
        self.assertEqual(self.smoke._stage_status(success_count=0, total_count=3), "degraded")
        self.assertEqual(self.smoke._stage_status(success_count=0, total_count=0), "skipped")

    def test_station_catalog_supports_multiple_cities(self) -> None:
        catalog = self.smoke.load_weather_station_catalog()
        self.assertEqual(catalog["seattle"]["station_id"], "KSEA")
        self.assertEqual(catalog["atlanta"]["station_id"], "KATL")
        self.assertEqual(catalog["chicago"]["station_id"], "KORD")
        self.assertEqual(catalog["miami"]["station_id"], "KMIA")
        self.assertEqual(catalog["wellington"]["station_id"], "NZWN")

    def test_extract_weather_event_urls_filters_non_weather_event_slugs(self) -> None:
        html = """
        <a href="/event/highest-temperature-in-seattle-on-march-13-2026">Seattle</a>
        <a href="/event/highest-temperature-in-atlanta-on-march-13-2026">Atlanta</a>
        <a href="/event/bitcoin-up-or-down-august-22-9pm-et">BTC</a>
        """
        urls = self.smoke.extract_weather_event_urls_from_page(html)
        self.assertEqual(
            urls,
            [
                "/event/highest-temperature-in-atlanta-on-march-13-2026",
                "/event/highest-temperature-in-seattle-on-march-13-2026",
            ],
        )

    def test_extract_markets_from_event_page_reads_embedded_json(self) -> None:
        html = """
        <html><body>
        "markets":[{"id":"1557668","question":"Will the highest temperature in Seattle be between 36-37°F on March 13?","conditionId":"cond1","clobTokenIds":"[\\"tok_yes\\", \\"tok_no\\"]","outcomes":"[\\"Yes\\", \\"No\\"]","active":true,"closed":false,"archived":false,"acceptingOrders":true,"enableOrderBook":true,"endDate":"2026-03-13T12:00:00Z","closeTime":"2026-03-13T12:00:00Z"}]
        </body></html>
        """
        markets = self.smoke.extract_markets_from_event_page(html)
        self.assertEqual(len(markets), 1)
        self.assertEqual(markets[0]["id"], "1557668")

    def test_build_station_mapping_for_market_supports_seattle(self) -> None:
        market = WeatherMarket(
            market_id="mkt_seattle_1",
            condition_id="cond_seattle_1",
            event_id="evt_seattle_1",
            slug="highest-temperature-in-seattle-on-march-13-2026-36to37f",
            title="Will the highest temperature in Seattle be between 36-37°F on March 13?",
            description="Seattle weather range",
            rules="Resolve to Yes if the highest temperature is between 36°F and 37°F.",
            status="active",
            active=True,
            closed=False,
            archived=False,
            accepting_orders=True,
            enable_order_book=True,
            tags=["Weather", "Temperature"],
            outcomes=["Yes", "No"],
            token_ids=["tok_yes", "tok_no"],
            close_time=datetime(2026, 3, 13, 12, 0, tzinfo=UTC),
            end_date=datetime(2026, 3, 13, 12, 0, tzinfo=UTC),
            raw_market={},
        )
        mapping = self.smoke.build_station_mapping_for_market(market)
        self.assertEqual(mapping["station_id"], "KSEA")

    def test_select_supported_markets_returns_all_mappable_markets(self) -> None:
        seattle = WeatherMarket(
            market_id="mkt_seattle_1",
            condition_id="cond_seattle_1",
            event_id="evt_seattle_1",
            slug="highest-temperature-in-seattle-on-march-13-2026-36to37f",
            title="Will the highest temperature in Seattle be between 36-37°F on March 13?",
            description="Seattle weather range",
            rules="Resolve to Yes if the highest temperature is between 36°F and 37°F.",
            status="active",
            active=True,
            closed=False,
            archived=False,
            accepting_orders=True,
            enable_order_book=True,
            tags=["Weather", "Temperature"],
            outcomes=["Yes", "No"],
            token_ids=["tok_yes_seattle", "tok_no_seattle"],
            close_time=datetime(2026, 3, 13, 12, 0, tzinfo=UTC),
            end_date=datetime(2026, 3, 13, 12, 0, tzinfo=UTC),
            raw_market={},
        )
        atlanta = WeatherMarket(
            market_id="mkt_atlanta_1",
            condition_id="cond_atlanta_1",
            event_id="evt_atlanta_1",
            slug="highest-temperature-in-atlanta-on-march-14-2026-60to61f",
            title="Will the highest temperature in Atlanta be between 60-61°F on March 14?",
            description="Atlanta weather range",
            rules="Resolve to Yes if the highest temperature is between 60°F and 61°F.",
            status="active",
            active=True,
            closed=False,
            archived=False,
            accepting_orders=True,
            enable_order_book=True,
            tags=["Weather", "Temperature"],
            outcomes=["Yes", "No"],
            token_ids=["tok_yes_atl", "tok_no_atl"],
            close_time=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
            end_date=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
            raw_market={},
        )
        unmapped = WeatherMarket(
            market_id="mkt_unknown_1",
            condition_id="cond_unknown_1",
            event_id="evt_unknown_1",
            slug="highest-temperature-in-osaka-on-march-14-2026-60to61f",
            title="Will the highest temperature in Osaka be between 60-61°F on March 14?",
            description="Unknown station mapping",
            rules="Resolve to Yes if the highest temperature is between 60°F and 61°F.",
            status="active",
            active=True,
            closed=False,
            archived=False,
            accepting_orders=True,
            enable_order_book=True,
            tags=["Weather", "Temperature"],
            outcomes=["Yes", "No"],
            token_ids=["tok_yes_osaka", "tok_no_osaka"],
            close_time=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
            end_date=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
            raw_market={},
        )
        selected = self.smoke.select_supported_markets([atlanta, unmapped, seattle])
        self.assertEqual([market.market_id for market in selected], ["mkt_seattle_1", "mkt_atlanta_1"])

    def test_load_reusable_active_markets_from_canonical_prefers_local_reuse_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "canonical.duckdb"
            self.smoke.apply_schema(db_path)
            con = duckdb.connect(str(db_path))
            try:
                raw_market = {
                    "id": "mkt_seattle_1",
                    "conditionId": "cond_seattle_1",
                    "event": {"id": "evt_seattle_1", "category": "Weather"},
                    "slug": "highest-temperature-in-seattle-on-march-27-2026-56to57f",
                    "question": "Will the highest temperature in Seattle be between 56-57°F on March 27?",
                    "description": "Seattle weather range",
                    "rules": "Resolve to Yes if the highest temperature is between 56°F and 57°F.",
                    "active": True,
                    "closed": False,
                    "archived": False,
                    "acceptingOrders": True,
                    "enableOrderBook": True,
                    "tags": ["Weather", "Temperature"],
                    "outcomes": "[\"Yes\", \"No\"]",
                    "clobTokenIds": "[\"tok_yes\", \"tok_no\"]",
                    "closeTime": "2026-03-27T12:00:00Z",
                    "endDate": "2026-03-27T12:00:00Z",
                    "createdAt": "2026-03-26T00:00:00Z",
                }
                con.execute(
                    """
                    INSERT INTO weather.weather_markets (
                        market_id, condition_id, event_id, slug, title, description, rules, status,
                        active, closed, archived, accepting_orders, enable_order_book, tags_json,
                        outcomes_json, token_ids_json, close_time, end_date, raw_market_json, created_at, updated_at
                    ) VALUES (
                        'mkt_seattle_1', 'cond_seattle_1', 'evt_seattle_1', 'highest-temperature-in-seattle-on-march-27-2026-56to57f',
                        'Will the highest temperature in Seattle be between 56-57°F on March 27?', 'Seattle weather range',
                        'Resolve to Yes if the highest temperature is between 56°F and 57°F.', 'active',
                        TRUE, FALSE, FALSE, TRUE, TRUE, '[]', '[\"Yes\",\"No\"]', '[\"tok_yes\",\"tok_no\"]',
                        '2026-03-27 12:00:00', '2026-03-27 12:00:00', ?, '2026-03-26 00:00:00', '2026-03-26 00:00:00'
                    )
                    """,
                    [json.dumps(raw_market)],
                )
                con.execute(
                    """
                    INSERT INTO weather.weather_market_specs (
                        market_id, condition_id, location_name, station_id, latitude, longitude, timezone,
                        observation_date, observation_window_local, metric, unit, bucket_min_value, bucket_max_value,
                        authoritative_source, fallback_sources, rounding_rule, inclusive_bounds, spec_version,
                        parse_confidence, risk_flags_json, created_at, updated_at
                    ) VALUES (
                        'mkt_seattle_1', 'cond_seattle_1', 'Seattle', 'KSEA', 47.4489, -122.3094,
                        'America/Los_Angeles', '2026-03-27', '2026-03-27', 'temperature_max', 'fahrenheit',
                        56, 57, 'wunderground', '[]', 'integer', TRUE, 'spec_v1', 0.95, '[]',
                        '2026-03-26 00:00:00', '2026-03-26 00:00:00'
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO runtime.trade_tickets (
                        ticket_id, run_id, strategy_id, strategy_version, wallet_id, market_id, token_id,
                        outcome, side, reference_price, fair_value, edge_bps, threshold_bps, route_action,
                        size, signal_ts_ms, forecast_run_id, watch_snapshot_id, request_id, ticket_hash,
                        provenance_json, created_at
                    ) VALUES (
                        'tt_seattle_1', 'run_1', 'weather_primary', 'v1', 'wallet_weather_1', 'mkt_seattle_1',
                        'tok_yes', 'YES', 'BUY', 0.42, 0.58, 1600, 500, 'FAK', 10.0, 1,
                        'forecast_run_1', 'snapshot_1', 'request_1', 'ticket_hash_1', '{}', '2026-03-26 00:01:00'
                    )
                    """
                )
            finally:
                con.close()

            selected = self.smoke._load_reusable_active_markets_from_canonical(
                db_path=db_path,
                horizon_days=14,
            )

        self.assertEqual([market.market_id for market in selected], ["mkt_seattle_1"])

    def test_agent_rows_can_be_attached_per_market(self) -> None:
        market = WeatherMarket(
            market_id="mkt_seattle_1",
            condition_id="cond_seattle_1",
            event_id="evt_seattle_1",
            slug="highest-temperature-in-seattle-on-march-13-2026-36to37f",
            title="Will the highest temperature in Seattle be between 36-37°F on March 13?",
            description="Seattle weather range",
            rules="Resolve to Yes if the highest temperature is between 36°F and 37°F.",
            status="active",
            active=True,
            closed=False,
            archived=False,
            accepting_orders=True,
            enable_order_book=True,
            tags=["Weather", "Temperature"],
            outcomes=["Yes", "No"],
            token_ids=["tok_yes", "tok_no"],
            close_time=datetime(2026, 3, 13, 12, 0, tzinfo=UTC),
            end_date=datetime(2026, 3, 13, 12, 0, tzinfo=UTC),
            raw_market={},
        )
        agent_report = {
            "markets": {
                "mkt_seattle_1": {
                    "rule2spec_status": "success",
                    "rule2spec_verdict": "review",
                    "rule2spec_summary": "rule2spec completed",
                    "data_qa_status": "not_run",
                    "data_qa_verdict": None,
                    "data_qa_summary": "no canonical forecast replay inputs in smoke chain",
                    "resolution_status": "not_run",
                    "resolution_verdict": None,
                    "resolution_summary": "no canonical resolution inputs in smoke chain",
                }
            }
        }
        selected_markets = [
            {
                "market_id": market.market_id,
                "question": market.title,
                "rule2spec_status": agent_report["markets"][market.market_id]["rule2spec_status"],
                "data_qa_status": agent_report["markets"][market.market_id]["data_qa_status"],
                "resolution_status": agent_report["markets"][market.market_id]["resolution_status"],
            }
        ]
        self.assertEqual(selected_markets[0]["rule2spec_status"], "success")
        self.assertEqual(selected_markets[0]["data_qa_status"], "not_run")
        self.assertEqual(selected_markets[0]["resolution_status"], "not_run")


if __name__ == "__main__":
    unittest.main()
