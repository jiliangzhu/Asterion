import unittest
import time
from datetime import datetime

from domains.weather.forecast.cache import InMemoryForecastCache
from domains.weather.forecast.service import ForecastDistribution


def _mock_distribution(market_id: str = "test") -> ForecastDistribution:
    return ForecastDistribution(
        market_id=market_id,
        condition_id="cond1",
        station_id="KNYC",
        source="test",
        model_run="2026-03-13T00:00Z",
        forecast_target_time=datetime(2026, 3, 13),
        observation_date=datetime(2026, 3, 13).date(),
        metric="temperature_max",
        latitude=40.7,
        longitude=-74.0,
        timezone="America/New_York",
        spec_version="v1",
        temperature_distribution={65: 1.0},
        source_trace=["test"],
        raw_payload={},
        from_cache=False,
        fallback_used=False,
        cache_key="test_key",
    )


class CacheTTLTest(unittest.TestCase):
    def test_cache_expires_after_ttl(self):
        cache = InMemoryForecastCache(default_ttl_seconds=1)
        dist = _mock_distribution()
        cache.put("key1", dist)

        self.assertIsNotNone(cache.get("key1"))
        time.sleep(1.1)
        self.assertIsNone(cache.get("key1"))

    def test_cache_lru_eviction(self):
        cache = InMemoryForecastCache(max_size=2)
        cache.put("key1", _mock_distribution("m1"))
        cache.put("key2", _mock_distribution("m2"))
        cache.put("key3", _mock_distribution("m3"))

        self.assertIsNone(cache.get("key1"))
        self.assertIsNotNone(cache.get("key2"))
        self.assertIsNotNone(cache.get("key3"))

    def test_cache_access_refreshes_lru_order(self):
        cache = InMemoryForecastCache(max_size=2)
        cache.put("key1", _mock_distribution("m1"))
        cache.put("key2", _mock_distribution("m2"))
        self.assertIsNotNone(cache.get("key1"))
        cache.put("key3", _mock_distribution("m3"))

        self.assertIsNotNone(cache.get("key1"))
        self.assertIsNone(cache.get("key2"))
        self.assertIsNotNone(cache.get("key3"))
