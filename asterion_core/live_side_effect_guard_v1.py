from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LiveSideEffectGuard:
    mode: str
    armed: bool

    def __post_init__(self) -> None:
        if not str(self.mode).strip():
            raise ValueError("mode is required")


def build_live_side_effect_guard(*, mode: str, armed: bool) -> LiveSideEffectGuard:
    return LiveSideEffectGuard(mode=str(mode).strip(), armed=bool(armed))


def validate_live_side_effect_guard(*, expected_mode: str, guard: LiveSideEffectGuard | None) -> str | None:
    normalized_expected_mode = str(expected_mode).strip()
    if not normalized_expected_mode:
        raise ValueError("expected_mode is required")
    if guard is None:
        return f"{normalized_expected_mode}_guard_missing"
    if str(guard.mode).strip() != normalized_expected_mode:
        return f"{normalized_expected_mode}_guard_mode_mismatch"
    if not bool(guard.armed):
        return f"{normalized_expected_mode}_not_armed"
    return None


__all__ = [
    "LiveSideEffectGuard",
    "build_live_side_effect_guard",
    "validate_live_side_effect_guard",
]
