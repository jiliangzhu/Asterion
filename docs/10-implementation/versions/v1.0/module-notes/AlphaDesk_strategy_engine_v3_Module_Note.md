# AlphaDesk strategy_engine_v3 Module Note

**Source**: `AlphaDesk/alphadesk/strategy_engine_v3.py`
**Target**: `asterion_core/runtime/strategy_engine_v3.py`
**Classification**: `keep_shell_rewrite_content`
**Status**: `ported`

## 保留什么

- 稳定排序和 `run_id` 生成壳
- 多 strategy 注册与 priority 调度壳
- deterministic runtime 输入输出约束

## 改什么

- 上游输入改为 `weather.weather_watch_only_snapshots`
- 输出改为 Asterion `StrategyRun + StrategyDecision`
- 排序键固定为 `strategy priority -> signal_ts_ms -> market_id -> token_id -> side`

## 不保留什么

- `opportunities_v1/v2/v3`
- capital engine / arb hardening
- 旧 `asset_id / planned_notional_usd` 语义
