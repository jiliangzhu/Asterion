# AlphaDesk ws_subscribe Module Note

**Source**: `AlphaDesk/alphadesk/ws_subscribe.py`  
**Target**: `asterion_core/ws/ws_subscribe.py`  
**Classification**: `direct_reuse`  
**Status**: `ported`

## 保留什么

- 从市场原始字段中提取 token ids 的递归逻辑
- 对双层 JSON string 的容错解码
- 基于 market universe 过滤订阅 token ids 的思路

## 改什么

- 从 `silver.dim_market` 专用读取改为 Asterion 通用 `load_token_ids_from_market_table()`
- 增加 `load_token_ids_from_market_capabilities()`，默认对齐 `capability.market_capabilities`
- 对 AlphaDesk `utils.as_str` 的依赖改为 Asterion `clients.shared.as_str`

## 不保留什么

- 对 AlphaDesk `silver.dim_market` 的硬编码依赖
- `asof_date` 回退逻辑
- 与旧 `outcomes/raw` 列命名的单一强绑定

## 接入的 Asterion Contracts

- `capability.market_capabilities.token_id`
- market discovery / WS subscribe 的 token universe 解析
- watch-only / replay 的统一 WS 订阅入口

## Smoke Test

- 能从 nested raw/outcomes 中提取 token ids
- 能从 `capability.market_capabilities` 表按 `market_id / condition_id / tradable` 过滤 token ids
- JSON string / double-encoded JSON 不会导致 token id 丢失
