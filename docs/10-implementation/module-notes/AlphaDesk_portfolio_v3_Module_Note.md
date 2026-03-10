# AlphaDesk portfolio_v3 Module Note

**Source**: `AlphaDesk/alphadesk/portfolio_v3.py`  
**Target**: `asterion_core/risk/portfolio_v3.py`  
**Classification**: `keep_shell_rewrite_content`  
**Status**: `ported`

## 保留什么

- reserve-on-pass 思路
- sequential gate 之后再更新 inventory 的壳
- exposure snapshot 捕获壳

## 改什么

- BUY 预留 `USDC.e`
- SELL 预留对应 `token_id`
- 输出改为 `Reservation + InventoryPosition + ExposureSnapshot`

## 不保留什么

- `event/topic` 聚合 exposure
- 旧 `size_usd` 聚合模型
