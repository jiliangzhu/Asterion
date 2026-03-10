# AlphaDesk writerd Module Note

**Source**: `AlphaDesk/alphadesk/writerd.py`  
**Target**: `asterion_core/storage/writerd.py`  
**Classification**: `direct_reuse`  
**Status**: `ported`

## 保留什么

- 单写者进程模式
- UPSERT / UPDATE 批量 merge
- batch failure -> fallback single-task
- allow-list gate

## 改什么

- allow-list 改成 Asterion schema/table
- 写入对象切到 Asterion migrations
- archive / prune 相关任务后续按 Asterion 数据保留策略调整

## 不保留什么

- AlphaDesk `gold/silver/meta` 表集合
- 旧 archive 分层策略里的业务表名单

## 接入的 Asterion Contracts

- inventory / exposure / proposal transitions
- market capability / account capability snapshots

## Smoke Test

- 单条 UPSERT 成功
- 同签名批处理 merge 成功
- 错误 payload 时 fallback single-task 不阻断整批
- allow-list 拦截非授权表写入

当前状态：

- `UPSERT_ROWS_V1 / UPDATE_ROWS_V1` 最小主链已迁入
- duckdb 真实进程级 smoke test 已完成
