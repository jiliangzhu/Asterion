# AlphaDesk clients.data_api Module Note

**Source**: `AlphaDesk/alphadesk/clients/data_api.py`  
**Target**: `asterion_core/clients/data_api.py`  
**Classification**: `direct_reuse`  
**Status**: `ported`

## 保留什么

- 分页抓取壳
- `market_param` fallback 机制
- watermark / since 过滤思路

## 改什么

- 去掉对 AlphaDesk shared 的依赖
- 改为 Asterion `clients/shared.py`
- 上游输出对齐 Asterion watch-only / replay 数据抓取

## 不保留什么

- AlphaDesk 特定 endpoint 假设

## 接入的 Asterion Contracts

- history fetch / replay fetch
- market-scoped fills / orders / public data backfill

## Smoke Test

- 第一个 market param 失败后自动切换 fallback param
- `watermark_ms` 能过滤旧记录
- 返回 `max_seen_ts`
