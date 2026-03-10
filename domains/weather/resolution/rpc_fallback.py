from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from .watcher_replay import UMAEvent


class BackfillRpcClient(Protocol):
    def get_finalized_block_number(self) -> int: ...

    def get_events(self, from_block: int, to_block: int) -> list[UMAEvent]: ...

    def get_proposal_state(
        self,
        *,
        proposal_id: str | None = None,
        tx_hash: str | None = None,
        condition_id: str | None = None,
    ) -> dict[str, Any] | None: ...


@dataclass(frozen=True)
class RpcEndpointConfig:
    name: str
    url: str
    priority: int
    timeout_seconds: float

    def __post_init__(self) -> None:
        if not self.name or not self.url:
            raise ValueError("name and url are required")
        if self.priority < 0:
            raise ValueError("priority must be non-negative")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")


@dataclass(frozen=True)
class RpcCallTrace:
    operation: str
    attempted_endpoints: list[str]
    selected_endpoint: str | None
    fallback_used: bool
    errors: list[str]


class RpcFallbackError(RuntimeError):
    def __init__(self, *, operation: str, trace: RpcCallTrace) -> None:
        super().__init__(f"all RPC endpoints failed for {operation}")
        self.operation = operation
        self.trace = trace


class FallbackRpcPool:
    def __init__(self, endpoints: list[tuple[RpcEndpointConfig, BackfillRpcClient]]) -> None:
        if not endpoints:
            raise ValueError("at least one RPC endpoint is required")
        self._endpoints = sorted(endpoints, key=lambda item: item[0].priority)

    def get_finalized_block_number(self) -> tuple[int, RpcCallTrace]:
        return self._call(
            "get_finalized_block_number",
            lambda client: client.get_finalized_block_number(),
            _validate_finalized_block_number,
        )

    def get_events(self, from_block: int, to_block: int) -> tuple[list[UMAEvent], RpcCallTrace]:
        return self._call(
            "get_events",
            lambda client: client.get_events(from_block, to_block),
            _validate_events,
        )

    def get_proposal_state(
        self,
        *,
        proposal_id: str | None = None,
        tx_hash: str | None = None,
        condition_id: str | None = None,
    ) -> tuple[dict[str, Any] | None, RpcCallTrace]:
        return self._call(
            "get_proposal_state",
            lambda client: client.get_proposal_state(
                proposal_id=proposal_id,
                tx_hash=tx_hash,
                condition_id=condition_id,
            ),
            _validate_proposal_state,
        )

    def _call(self, operation: str, fn, validator):
        attempted: list[str] = []
        errors: list[str] = []
        for index, (config, client) in enumerate(self._endpoints):
            attempted.append(config.name)
            try:
                result = fn(client)
                validator(result)
                trace = RpcCallTrace(
                    operation=operation,
                    attempted_endpoints=list(attempted),
                    selected_endpoint=config.name,
                    fallback_used=index > 0,
                    errors=list(errors),
                )
                return result, trace
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{config.name}: {exc}")
        trace = RpcCallTrace(
            operation=operation,
            attempted_endpoints=attempted,
            selected_endpoint=None,
            fallback_used=len(attempted) > 1,
            errors=errors,
        )
        raise RpcFallbackError(operation=operation, trace=trace)


def rpc_trace_to_json(trace: RpcCallTrace) -> dict[str, Any]:
    return {
        "operation": trace.operation,
        "attempted_endpoints": list(trace.attempted_endpoints),
        "selected_endpoint": trace.selected_endpoint,
        "fallback_used": trace.fallback_used,
        "errors": list(trace.errors),
    }


def _validate_finalized_block_number(value: Any) -> None:
    if not isinstance(value, int):
        raise ValueError("finalized block number must be an integer")
    if value < 0:
        raise ValueError("finalized block number must be non-negative")


def _validate_events(value: Any) -> None:
    if not isinstance(value, list):
        raise ValueError("events must be a list")
    for item in value:
        if not isinstance(item, UMAEvent):
            raise ValueError("events list must contain UMAEvent instances")


def _validate_proposal_state(value: Any) -> None:
    if value is not None and not isinstance(value, dict):
        raise ValueError("proposal state must be a dictionary or None")
