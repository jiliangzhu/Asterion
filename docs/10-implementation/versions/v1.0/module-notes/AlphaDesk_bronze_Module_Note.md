# AlphaDesk bronze Module Note

**Source**: `AlphaDesk/alphadesk/bronze.py`
**Target**: `asterion_core/ingest/bronze.py`
**Classification**: `direct_reuse`
**Status**: `ported`

## 保留什么

- `BronzeJsonlRollingWriter`
- UTC minute rolling
- `.tmp -> .jsonl` 原子 rename finalize 语义

## 改什么

- env/config 命名改为 `ASTERION_*`
- 输出目录命名与 Asterion ingest 规范对齐
- 下游消费者从 AlphaDesk realtime sidecar 改为 Asterion watch-only / replay

## 不保留什么

- AlphaDesk 特定目录约定
- 与旧 ws sidecar 的隐式耦合

## 接入的 Asterion Contracts

- raw ingest run metadata
- replay / cold path 文件分区约定

## Smoke Test

- 连续写同一分钟数据只生成一个 `.tmp`
- 分钟切换后旧文件 finalize 为 `.jsonl`
- 重启后新分片不覆盖旧分片
