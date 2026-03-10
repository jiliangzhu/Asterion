from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


VALID_DQ_LEVELS = {"PASS", "WARN", "FAIL"}


@dataclass(frozen=True)
class StrategyContext:
    """Deterministic runtime context for watch-only strategy evaluation."""

    data_snapshot_id: str
    universe_snapshot_id: str | None
    asof_ts_ms: int
    dq_level: str
    quote_snapshot_refs: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.data_snapshot_id:
            raise ValueError("data_snapshot_id is required")
        if self.universe_snapshot_id == "":
            raise ValueError("universe_snapshot_id must be None or non-empty")
        if int(self.asof_ts_ms) < 0:
            raise ValueError("asof_ts_ms must be non-negative")
        if self.dq_level not in VALID_DQ_LEVELS:
            raise ValueError(f"dq_level must be one of {sorted(VALID_DQ_LEVELS)}")

    @property
    def bbo_parquet_files(self) -> list[str]:
        """Compatibility alias for AlphaDesk naming during migration."""
        return self.quote_snapshot_refs


class StrategyV3(Protocol):
    strategy_id: str
    strategy_version: str
    required_features: list[str]
    default_params: dict[str, Any]
    params_schema: dict[str, Any]

    def generate(self, con: Any, *, ctx: StrategyContext, params: dict[str, Any]) -> list[dict[str, Any]]:
        """Return minimal strategy outputs.

        Required fields in each result dict:
        - `market_id`
        - `token_id`
        - `side` (`BUY` or `SELL`)
        - `signal_ts_ms`
        """

