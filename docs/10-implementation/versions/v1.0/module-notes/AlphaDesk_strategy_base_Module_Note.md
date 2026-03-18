# AlphaDesk strategy_base Module Note

**Source**: `AlphaDesk/alphadesk/strategies/base.py`
**Target**: `asterion_core/runtime/strategy_base.py`
**Classification**: `direct_reuse`
**Status**: `ported`

## 保留什么

- `StrategyContext` 作为最小 deterministic runtime context
- `StrategyV3` 协议边界
- `generate(con, *, ctx, params)` 的调用形式

## 改什么

- `bbo_parquet_files` 收口为更通用的 `quote_snapshot_refs`
- 保留 `bbo_parquet_files` 兼容 alias，避免迁移期间重复改调用点
- 结果字段说明从 `asset_id` 改成 Asterion 的 `token_id`
- 增加 `dq_level` 和快照字段的基本约束校验

## 不保留什么

- AlphaDesk 的 `asset_id` 结果语义
- 对旧 opportunities schema 的隐式字段假设

## 接入的 Asterion Contracts

- watch-only strategy runtime 的统一输入上下文
- `market_id + token_id + side + signal_ts_ms` 的最小输出约定
- 后续 `strategy_engine_v3` / `watch_only_gate_v3` 的共同接口

## Smoke Test

- `StrategyContext` 可稳定构造
- 非法 `dq_level` 会被拒绝
- `bbo_parquet_files` 兼容 alias 仍可读取 `quote_snapshot_refs`
