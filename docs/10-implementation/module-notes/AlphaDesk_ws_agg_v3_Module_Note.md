# AlphaDesk ws_agg_v3 Module Note

**Source**: `AlphaDesk/alphadesk/ws_agg_v3.py`  
**Target**: `asterion_core/ws/ws_agg_v3.py`  
**Classification**: `direct_reuse`  
**Status**: `ported`

## 保留什么

- minute-level quote aggregation 的核心思路
- carry-forward BBO state
- `best_bid / best_ask / mid / spread / updates_count / coverage` 计算骨架
- quote delay 的 p50 / p90 统计

## 改什么

- 从 AlphaDesk 的 parquet + manifest + write_queue 主链，改成 Asterion 的逻辑层聚合函数
- `asset_id` 统一改成 `token_id`
- 旧 `agg_state` / `manifest` / `gold` 目录依赖不直接迁入

## 不保留什么

- 对 AlphaDesk `agg_state.py` 的 SQLite finalized/revision 管理依赖
- 对 AlphaDesk `manifest.py` 与 `gold.agg_files_manifest_v1` 的写入耦合
- parquet 文件输出与 write queue enqueue 行为

## 接入的 Asterion Contracts

- watch-only / replay 的 WS quote minute aggregation
- `market_id + token_id` 维度的 quote state
- 后续 pricing / monitor / UI replica 的分钟级输入

## Smoke Test

- 同一分钟多条 quote 事件能按 `(market_id, token_id)` 聚合
- 无更新 token 能从 prior state carry-forward
- raw bids/asks 可回推出 `best_bid / best_ask`
- coverage / delay quantiles 计算稳定
