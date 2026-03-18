# AlphaDesk signal_to_order_v1 Module Note

**Source**: `AlphaDesk/alphadesk/signal_to_order_v1.py`
**Target**: `asterion_core/execution/signal_to_order_v1.py`
**Classification**: `keep_shell_rewrite_content`
**Status**: `ported`

## 保留什么

- ticket 到订单对象的 handoff 壳
- dedup / deterministic build 思路

## 改什么

- 输出改为 `SignalOrderIntent`
- 闭合对象改为 `CanonicalOrderContract + ExecutionContext`
- capability source-of-truth 改为 `capability.market_capabilities` 与 `capability.account_trading_capabilities`

## 不保留什么

- `exec_plan_v3`
- exec template 依赖
