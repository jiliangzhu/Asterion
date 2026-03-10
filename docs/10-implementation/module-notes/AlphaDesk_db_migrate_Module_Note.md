# AlphaDesk db_migrate Module Note

**Source**: `AlphaDesk/alphadesk/db_migrate.py`  
**Target**: `asterion_core/storage/db_migrate.py`  
**Classification**: `direct_reuse`  
**Status**: `ported`

## 保留什么

- migration runner 入口
- 按版本顺序应用 SQL 文件的方式

## 改什么

- 迁移来源改成 `sql/migrations/*.sql`
- schema 目标改成 Asterion `meta / capability / trading / weather / resolution`
- 使用 Asterion `connect_duckdb()`

## 不保留什么

- AlphaDesk 旧 schema 兼容补丁
- 围绕旧 `gold/silver/meta` 的语义迁移

## 接入的 Asterion Contracts

- `0001_core_meta.sql`
- `0002_market_and_capability.sql`
- `0003_orders_inventory.sql`
- `0004_weather_specs_and_forecasts.sql`
- `0005_uma_watcher.sql`

## Smoke Test

- 空库按顺序应用 migration
- `meta.schema_migrations` 记录已应用版本

当前状态：

- migration runner 已实现
- duckdb 运行级验证已完成
