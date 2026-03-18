# AlphaDesk journal_v3 Module Note

**Source**: `AlphaDesk/alphadesk/journal_v3.py`
**Target**: `asterion_core/journal/journal_v3.py`
**Classification**: `keep_shell_rewrite_content`
**Status**: `ported`

## 保留什么

- queue-backed upsert shell
- 稳定 journal event id
- 运行时审计投影思路

## 改什么

- canonical ledger 固定为 `trading.*`
- 新增 `runtime.strategy_runs / runtime.trade_tickets / runtime.gate_decisions / runtime.journal_events`
- journal payload 改为 strategy/ticket/gate/order/fill/reservation/exposure 审计链

## 不保留什么

- `journal_trades`
- `asset_id / size_usd / order_type` 旧 schema
