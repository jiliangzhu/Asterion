# P1 Closeout Checklist

**版本**: v1.0  
**更新日期**: 2026-03-09  
**目标**: 作为 `P1-15` 的关闭审查清单，正式收口 Weather MVP 的 `watch-only / replay / cold path` 入口、runbook、验收动作，以及 `P1` 结束时 AlphaDesk 剩余依赖的边界。

---

## 1. P1 关闭标准

`P1` 只有在以下六类事项全部满足时才能关闭：

1. AlphaDesk 的 `P1-blocker` 模块已迁入 Asterion
2. Weather `watch-only` 主链路已经闭合
3. UMA watcher replay 与 settlement verification 已闭合
4. operator 最小只读面与 runbook 已固定
5. `.venv + duckdb + unittest` 验证通过
6. AlphaDesk 剩余依赖已分类，不再阻塞 `P1`

---

## 2. 当前状态

### 2.1 AlphaDesk P1-blocker 迁移

- `DONE` `ws_subscribe.py`
- `DONE` `ws_agg_v3.py`
- `DONE` `strategy_base.py`
- `DONE` `watch_only_gate_v3.py`
- `DONE` `health_monitor_v1.py`
- `DONE` `ui_db_replica.py`

说明：

- 对应状态已同步到 [AlphaDesk_Migration_Ledger.md](../migration-ledger/AlphaDesk_Migration_Ledger.md)
- 对应 module notes 已补齐

### 2.2 Weather watch-only 主链路

- `DONE` `market discovery -> weather.weather_markets`
- `DONE` `Rule2Spec -> ResolutionSpec`
- `DONE` `StationMapper / weather.weather_station_map`
- `DONE` `forecast adapters + cache`
- `DONE` `weather.weather_forecast_runs`
- `DONE` `weather.weather_fair_values`
- `DONE` `weather.weather_watch_only_snapshots`

### 2.3 Resolution replay 与 verification

- `DONE` finalized block watermark
- `DONE` processed event idempotency
- `DONE` restart replay
- `DONE` `resolution.uma_proposals`
- `DONE` `resolution.proposal_state_transitions`
- `DONE` `resolution.settlement_verifications`
- `DONE` `resolution.proposal_evidence_links`
- `DONE` `resolution.redeem_readiness_suggestions`

### 2.4 Operator 只读面与运行入口

- `DONE` 当前 canonical operator 只读面定义为 `DuckDB tables + UI replica + runbook`
- `DONE` 已有 [P1_Watch_Only_Replay_Cold_Path_Runbook.md](../runbooks/P1_Watch_Only_Replay_Cold_Path_Runbook.md)
- `DONE` 当前无需再以“P1 必须交付产品化 Streamlit 页面”作为关闭前提

### 2.5 工程验证

- `DONE` 项目依赖已固定到 `pyproject.toml`
- `DONE` `.venv` 中 DuckDB 运行级测试通过
- `DONE` `python3 -m unittest discover -s tests -v` 在 `.venv` 下通过

当前确认结果：

- `.venv` 下 `62` 个测试通过，`0` 个 skip
- settlement verification / evidence linkage / redeem suggestion 已通过 DuckDB 集成验证

---

## 3. P1 关闭时的残留项

以下项仍未完成，但它们属于 `P2` 或 AlphaDesk Exit Gate 范围，不阻塞 `P1` 关闭：

- `strategy_engine_v3.py`
- `trade_ticket_v1.py`
- `signal_to_order_v1.py`
- `execution_gate_v1.py`
- `portfolio_v3.py`
- `journal_v3.py`
- `readiness_checker_v1.py`
- `ui_lite_db.py`
- `dagster_asterion/resources.py`
- `dagster_asterion/schedules.py`

说明：

- 这些项后续已统一收口到 [P2_Closeout_Checklist.md](./P2_Closeout_Checklist.md) 与 [AlphaDesk_Migration_Ledger.md](../migration-ledger/AlphaDesk_Migration_Ledger.md)
- 当前不影响 Weather MVP 的 `watch-only / replay / cold path` 基础开发

---

## 4. P1 关闭验证命令

```bash
source .venv/bin/activate
python3 -m unittest discover -s tests -v
```

建议同时重点查看：

- `tests/test_weather_market_discovery.py`
- `tests/test_rule2spec.py`
- `tests/test_station_mapper.py`
- `tests/test_forecast_service.py`
- `tests/test_forecast_persistence.py`
- `tests/test_weather_pricing.py`
- `tests/test_uma_watcher_replay.py`
- `tests/test_settlement_verification.py`

---

## 5. P1 关闭结论

`P1` 退出条件已经满足，可以关闭。

关闭依据：

- AlphaDesk 的 `P1-blocker` 模块均已迁入并验证
- Weather `watch-only` 链路已从 market discovery 贯通到 watch-only snapshots
- UMA watcher 已从 replay 贯通到 settlement verification、evidence linkage、redeem suggestion
- operator 读路径已以 `DuckDB + UI replica + runbook` 形式固定
- `P1` 后续开发不再需要回头查 AlphaDesk 才能继续 Weather `watch-only / replay / cold path`

---

## 6. P1 关闭后的下一阶段入口

`P1` 关闭后，进入：

- `P2` replay / backfill / cold path 强化
- AlphaDesk Wave B / Wave C 剩余模块迁移
- execution foundation、journal、risk、readiness 收口
- AlphaDesk Exit Gate 审查
