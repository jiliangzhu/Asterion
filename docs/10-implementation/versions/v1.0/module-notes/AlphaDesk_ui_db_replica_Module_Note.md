# AlphaDesk ui_db_replica Module Note

**Source**: `AlphaDesk/alphadesk/ui_db_replica.py`
**Target**: `asterion_core/ui/ui_db_replica.py`
**Classification**: `direct_reuse`
**Status**: `ported`

## 保留什么

- `ReplicaRefreshResult`
- replica meta 文件读写
- DuckDB 文件 copy -> validate -> replace 的刷新主路径
- cross-device snapshot + rsync 的兜底路径
- `run_ui_db_replica_loop()` 的周期刷新壳

## 改什么

- 默认 replica 路径改成 `data/ui/asterion_ui.duckdb`
- 环境变量前缀统一改成 `ASTERION_UI_REPLICA_*`
- 日志依赖改成 Asterion 自己的 `asterion_core.storage.logger`
- 只保留读副本工具，不引入 AlphaDesk 旧 UI 页面耦合

## 不保留什么

- `ALPHADESK_*` 环境变量命名
- AlphaDesk UI 页面和 dashboard 的隐式依赖
- 对旧页面 schema 的直接绑定

## 接入的 Asterion Contracts

- operator 读路径的只读 DuckDB 副本
- 后续 UI / readiness / health 页面读取主库快照的基础能力
- watch-only / replay 阶段的安全只读查询入口

## Smoke Test

- 缺失 source DB 时写入失败 meta
- 真实 DuckDB 文件复制后可只读打开并执行查询
- source 未变化时二次 refresh 不重复覆盖 replica
