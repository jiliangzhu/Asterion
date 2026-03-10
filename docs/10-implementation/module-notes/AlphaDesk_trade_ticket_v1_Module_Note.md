# AlphaDesk trade_ticket_v1 Module Note

**Source**: `AlphaDesk/alphadesk/trade_ticket_v1.py`  
**Target**: `asterion_core/execution/trade_ticket_v1.py`  
**Classification**: `keep_shell_rewrite_content`  
**Status**: `ported`

## 保留什么

- provenance payload 组织方式
- 稳定 `ticket_hash`
- `request_id` 闭合壳

## 改什么

- `opp_id / plan_id / asset_id / planned_notional_usd` 全部替换成 Asterion `TradeTicket`
- provenance 改为 `forecast_run_id / watch_snapshot_id / strategy_id / route_action`
- ticket 输入改为 `StrategyDecision`

## 不保留什么

- 旧 `recommended_exec_template_id`
- 旧机会表字段
