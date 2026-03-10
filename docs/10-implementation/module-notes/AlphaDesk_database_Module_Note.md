# AlphaDesk database Module Note

**Source**: `AlphaDesk/alphadesk/database.py`  
**Target**: `asterion_core/storage/database.py`  
**Classification**: `direct_reuse`  
**Status**: `ported`

## 保留什么

- `DuckDBConfig`
- `GuardedConnection`
- `connect_duckdb()`
- `meta_start_run()`
- `meta_finish_run()`
- `meta_get_watermark_ms()`
- `meta_set_watermark_ms()`

## 改什么

- `ALPHADESK_*` 环境变量改成 `ASTERION_*`
- 读写 guard 的 schema allow-list 改成 Asterion 表
- run log / watermark 表名与 Asterion migration 对齐

## 不保留什么

- AlphaDesk 的 `gold/silver/meta` 固定表写入假设
- Debug writer 对旧 schema 的特例

## 接入的 Asterion Contracts

- block watermark
- ingest run log
- finalized block replay watermark

## Smoke Test

- reader mode 拒绝写 SQL
- writer mode 能打开并应用 schema
- watermark get/set 在 Asterion 表上可运行

当前状态：

- 代码已迁入
- duckdb 运行级 smoke test 已完成
