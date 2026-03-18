# AlphaDesk execution_gate_v1 Module Note

**Source**: `AlphaDesk/alphadesk/execution_gate_v1.py`
**Target**: `asterion_core/execution/execution_gate_v1.py`
**Classification**: `keep_shell_rewrite_content`
**Status**: `ported`

## 保留什么

- 分层 gate pipeline
- 统一 gate result 对象
- `reason_codes` 审计壳

## 改什么

- gate 输入改成 `TradeTicket + SignalOrderIntent`
- pipeline 对齐 `watch_only/degrade -> market capability -> account capability -> inventory -> economic`
- 经济判断改成 Asterion `edge_bps / threshold_bps` 语义

## 不保留什么

- AlphaDesk 旧 fillability bucket 公式
- 旧 notional / spread / quote-age 成本模型
