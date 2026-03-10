# AlphaDesk clients.gamma Module Note

**Source**: `AlphaDesk/alphadesk/clients/gamma.py`  
**Target**: `asterion_core/clients/gamma.py`  
**Classification**: `direct_reuse`  
**Status**: `ported`

## 保留什么

- `infer_condition_id()`
- market/event 提取逻辑
- `scan_gamma_markets()` 的分页扫描壳

## 改什么

- 输出字段映射到 Asterion `MarketCapability` 与 Weather market discovery
- 删除 AlphaDesk 旧 universe 假设
- 加入 Weather MVP 所需的 station/spec onboarding 字段透传

## 不保留什么

- 与 AlphaDesk `dim_market` / `dim_event` 的直接写入假设

## 接入的 Asterion Contracts

- `MarketCapability`
- market discovery records
- Rule2Spec / station onboarding 输入

## Smoke Test

- gamma market 扫描返回稳定 market/event 结构
- `condition_id` 推导在缺省字段下行为可预测
- 输出能映射到 Asterion market discovery schema
