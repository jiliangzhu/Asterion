from __future__ import annotations

from dataclasses import dataclass, field

from .service import ForecastDistribution


@dataclass
class InMemoryForecastCache:
    _store: dict[str, ForecastDistribution] = field(default_factory=dict)

    def get(self, cache_key: str) -> ForecastDistribution | None:
        return self._store.get(cache_key)

    def put(self, cache_key: str, distribution: ForecastDistribution) -> None:
        self._store[cache_key] = distribution

    def size(self) -> int:
        return len(self._store)
