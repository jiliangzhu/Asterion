# AlphaDesk write_queue Module Note

**Source**: `AlphaDesk/alphadesk/write_queue.py`  
**Target**: `asterion_core/storage/write_queue.py`  
**Classification**: `direct_reuse`  
**Status**: `ported`

## 保留什么

- `WriteQueueConfig`
- `WriteTask`
- `enqueue_task()`
- `claim_next_tasks()`
- `mark_task_succeeded()`
- `mark_task_failed()`
- retry / stale-running / archive 机制

## 改什么

- 默认 queue path 改到 Asterion
- task payload 改成 Asterion tables / migrations
- health 指标命名改成 Asterion queue metrics

## 不保留什么

- AlphaDesk 任务类型与旧表字段假设

## 接入的 Asterion Contracts

- orders / fills / reservations
- capability snapshots
- uma events / transitions

## Smoke Test

- enqueue -> claim -> succeed 闭环
- failed -> retry 闭环
- stale running -> pending 回收
- DEAD task archive 输出有效 JSONL
