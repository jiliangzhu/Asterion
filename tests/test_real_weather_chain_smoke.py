from __future__ import annotations

import importlib.util
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from asterion_core.contracts import WeatherMarket


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

    def test_script_uses_deterministic_rule2spec_validation_instead_of_llm_agent(self) -> None:
        text = SCRIPT_PATH.read_text(encoding="utf-8")
        self.assertIn("validate_rule2spec_draft(", text)
        self.assertNotIn("run_rule2spec_agent_review(", text)

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
