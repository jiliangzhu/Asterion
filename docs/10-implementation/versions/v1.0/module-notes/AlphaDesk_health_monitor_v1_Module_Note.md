# AlphaDesk health_monitor_v1 Module Note

**Source**: `AlphaDesk/alphadesk/health_monitor_v1.py`
**Target**: `asterion_core/monitoring/health_monitor_v1.py`
**Classification**: `direct_reuse`
**Status**: `ported`

## 保留什么

- `WSHealthSnapshot / QuoteHealthSnapshot / QueueHealthSnapshot / DegradeStatus / SystemHealthSnapshot`
- WS、quote、queue、degrade 四类健康采集函数
- `collect_system_health()` 的统一拼装入口

## 改什么

- quote state 读取同时兼容 `latest_quote_by_market_token` 和 `latest_quote_by_market_asset`
- 保留 queue 表名 `write_queue_tasks`，直接对齐 Asterion write queue
- `db_path` 先保持占位输入，不在 `P1` 引入更多读库耦合

## 不保留什么

- AlphaDesk 的旧 quote/source 命名强绑定
- 对未迁入 readiness / UI 页面 的隐式依赖

## 接入的 Asterion Contracts

- watch-only runtime 的健康采集
- queue / ws / degrade 监控输入
- 后续 operator / readiness 的基础指标来源

## Smoke Test

- 从 mock state store 采集 WS/quote 健康快照
- 从真实 queue sqlite 采集 pending/write/error/dead 指标
- 能读取 watch-only flag file 并生成 degrade status
