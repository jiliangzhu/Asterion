from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

from .service import ForecastDistribution


@dataclass
class CacheEntry:
    value: ForecastDistribution
    cached_at: datetime
    last_accessed_at: datetime
    ttl_seconds: int


@dataclass
class InMemoryForecastCache:
    _store: dict[str, CacheEntry] = field(default_factory=dict)
    default_ttl_seconds: int = 3600
    max_size: int = 1000

    def get(self, cache_key: str) -> ForecastDistribution | None:
        entry = self._store.get(cache_key)
        if entry is None:
            return None

        age = (datetime.now(timezone.utc) - entry.cached_at).total_seconds()
        if age > entry.ttl_seconds:
            del self._store[cache_key]
            return None

        entry.last_accessed_at = datetime.now(timezone.utc)
        return entry.value

    def put(self, cache_key: str, distribution: ForecastDistribution, ttl_seconds: int | None = None) -> None:
        now = datetime.now(timezone.utc)
        if len(self._store) >= self.max_size:
            oldest_key = min(self._store.keys(), key=lambda k: self._store[k].last_accessed_at)
            del self._store[oldest_key]

        self._store[cache_key] = CacheEntry(
            value=distribution,
            cached_at=now,
            last_accessed_at=now,
            ttl_seconds=ttl_seconds or self.default_ttl_seconds,
        )

    def size(self) -> int:
        return len(self._store)
