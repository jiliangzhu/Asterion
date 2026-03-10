# AlphaDesk determinism Module Note

**Source**: `AlphaDesk/alphadesk/determinism.py`  
**Target**: `asterion_core/storage/determinism.py`  
**Classification**: `direct_reuse`  
**Status**: `ported`

## 保留什么

- canonical JSON normalization
- stable SHA256 payload hash
- float rounding for deterministic serialization

## 改什么

- 模块命名与 import path 改成 Asterion
- 增加对 Asterion shared contracts 的 golden tests

## 不保留什么

- AlphaDesk 特定 run-log version 约束

## 接入的 Asterion Contracts

- request / order / proposal IDs
- forecast cache key hashing
- contract snapshot hashing

## Smoke Test

- 相同 payload 多次哈希结果一致
- 字段顺序变化不影响哈希
- 非 ASCII 字符串和 Decimal/string 化输入结果稳定
