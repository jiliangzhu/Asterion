# AlphaDesk Migration Ledger

**版本**: v1.2
**更新日期**: 2026-03-10
**阶段**: `P2-21 closeout`
**目标**: 把 AlphaDesk -> Asterion 的复用范围、迁移顺序、目标路径和适配动作固定成显式台账。

> 当前 AlphaDesk 迁移与 exit gate 的 canonical 入口是本台账 + [P2_Closeout_Checklist.md](../checklists/P2_Closeout_Checklist.md)。
> [P1_P2_AlphaDesk_Remaining_Migration_Checklist.md](../checklists/P1_P2_AlphaDesk_Remaining_Migration_Checklist.md) 现在只保留为 archive redirect note。

---

## 1. 使用规则

1. 只有出现在本台账中的模块，才允许进入迁移队列。
2. 每个迁移模块必须绑定目标路径和 module note。
3. `status=planned` 不允许直接复制代码进入 Asterion。
4. `status=in_progress` 才表示可以开始迁移和适配。
5. `status=ported` 必须同时满足：
   - 代码已迁入
   - AlphaDesk 假设已处理
   - smoke test 或 contract test 已补

---

## 2. Wave A: Platform Foundation

| Source Module | Target Path | Classification | Status | Notes |
| --- | --- | --- | --- | --- |
| `alphadesk/determinism.py` | `asterion_core/storage/determinism.py` | direct_reuse | ported | 已补稳定哈希 smoke tests |
| `alphadesk/write_guard_audit.py` | `asterion_core/storage/write_guard_audit.py` | direct_reuse | ported | 已补审计落地与计数 tests |
| `alphadesk/write_queue.py` | `asterion_core/storage/write_queue.py` | direct_reuse | ported | 已补 enqueue/claim/retry smoke tests |
| `alphadesk/os_queue.py` | `asterion_core/storage/os_queue.py` | direct_reuse | ported | 已接入 queue producer API |
| `alphadesk/database.py` | `asterion_core/storage/database.py` | direct_reuse | ported | 已通过 duckdb reader/writer guard 与 watermark 运行级测试 |
| `alphadesk/db_migrate.py` | `asterion_core/storage/db_migrate.py` | direct_reuse | ported | 已通过 duckdb migration apply 与 schema version 记录测试 |
| `alphadesk/writerd.py` | `asterion_core/storage/writerd.py` | direct_reuse | ported | 已通过 duckdb UPSERT/UPDATE 运行级测试 |
| `alphadesk/bronze.py` | `asterion_core/ingest/bronze.py` | direct_reuse | ported | 已补 rolling/finalize smoke test |
| `alphadesk/clients/data_api.py` | `asterion_core/clients/data_api.py` | direct_reuse | ported | 已补分页与 fallback param smoke test |
| `alphadesk/clients/gamma.py` | `asterion_core/clients/gamma.py` | direct_reuse | ported | 已补 universe filter/event extraction smoke test |
| `alphadesk/ws_subscribe.py` | `asterion_core/ws/ws_subscribe.py` | direct_reuse | ported | 已迁通用 token universe 提取与 capability 表读取，并补 smoke tests |
| `alphadesk/ws_agg_v3.py` | `asterion_core/ws/ws_agg_v3.py` | direct_reuse | ported | 已迁分钟聚合/coverage/carry-forward 逻辑壳，并补 smoke tests |

说明：

- `P1` 结束时，所有 `direct_reuse` 的 WS / watch-only / monitoring / UI replica 基础模块均已迁入
- `P2-21` closeout 时，所有 `direct_reuse` / `keep_shell_rewrite_content` 项均已收口为 `ported` 或 `do_not_port`

---

## 3. Wave B: Runtime Skeleton

| Source Module | Target Path | Classification | Status | Notes |
| --- | --- | --- | --- | --- |
| `alphadesk/strategies/base.py` | `asterion_core/runtime/strategy_base.py` | direct_reuse | ported | 已迁 `StrategyContext` / `StrategyV3`，并将 `asset_id` 语义改为 `token_id` |
| `alphadesk/strategy_engine_v3.py` | `asterion_core/runtime/strategy_engine_v3.py` | keep_shell_rewrite_content | ported | 已迁稳定排序 / run_id / watch-only snapshot 输入壳，禁止迁旧 `opportunities_v*` contract |
| `alphadesk/trade_ticket_v1.py` | `asterion_core/execution/trade_ticket_v1.py` | keep_shell_rewrite_content | ported | 已改成 Asterion `TradeTicket` / provenance / hash / request_id |
| `alphadesk/signal_to_order_v1.py` | `asterion_core/execution/signal_to_order_v1.py` | keep_shell_rewrite_content | ported | 已闭合 `TradeTicket -> CanonicalOrderContract + ExecutionContext`，不依赖 `exec_plan_v3` |
| `alphadesk/execution_gate_v1.py` | `asterion_core/execution/execution_gate_v1.py` | keep_shell_rewrite_content | ported | 已保留 gate pipeline 分层，重写为 capability + inventory + economic gate |
| `alphadesk/portfolio_v3.py` | `asterion_core/risk/portfolio_v3.py` | keep_shell_rewrite_content | ported | 已改为 `Reservation + InventoryPosition + ExposureSnapshot` 语义 |
| `alphadesk/journal_v3.py` | `asterion_core/journal/journal_v3.py` | keep_shell_rewrite_content | ported | 已改为 `runtime.*` 审计表 + `trading.*` ledger 投影写入壳 |
| `alphadesk/watch_only_gate_v3.py` | `asterion_core/execution/watch_only_gate_v3.py` | direct_reuse | ported | 已迁最小 watch-only gate 判定壳，并补 smoke tests |

---

## 4. Wave C: Ops / UI / Orchestration

| Source Module | Target Path | Classification | Status | Notes |
| --- | --- | --- | --- | --- |
| `alphadesk/health_monitor_v1.py` | `asterion_core/monitoring/health_monitor_v1.py` | direct_reuse | ported | 已迁 queue/ws/degrade 健康采集壳，并补 smoke tests |
| `alphadesk/readiness_checker_v1.py` | `asterion_core/monitoring/readiness_checker_v1.py` | keep_shell_rewrite_content | ported | 已改为 `P3 paper execution` readiness gate，并补 closeout/integration tests |
| `alphadesk/ui_db_replica.py` | `asterion_core/ui/ui_db_replica.py` | direct_reuse | ported | 已迁 replica copy/validate/meta 主路径，并补 smoke tests |
| `alphadesk/ui_lite_db.py` | `asterion_core/ui/ui_lite_db.py` | keep_shell_rewrite_content | ported | 已改为独立 Lite DB + `ui.*` summary tables，并补 integration tests |
| `dagster_alphadesk/resources.py` | `dagster_asterion/resources.py` | direct_reuse | ported | 已作为 Asterion cold-path runtime resources 落地 |
| `dagster_alphadesk/schedules.py` | `dagster_asterion/schedules.py` | direct_reuse | ported | 已作为 job-map 驱动的可选 Dagster schedule 壳落地 |

---

## 5. Blocked / Do Not Port

| Source Module | Classification | Reason |
| --- | --- | --- |
| `alphadesk/opportunities_v1/v2/v3` 相关旧 schema 逻辑 | do_not_port | 与 Asterion canonical contract 冲突 |
| `alphadesk/exec_plan_v3` 旧执行计划链路 | do_not_port | 与 Asterion OMS/Router/Signer 边界冲突 |
| `alphadesk/capital engine` | do_not_port | crypto-first 假设过强 |
| `smart money / wallet feature / crypto arb` | do_not_port | 不属于 Weather MVP |
| `pages/dashboard.py` 等旧 UI 业务页 | do_not_port | 只保留信息架构，不迁旧页面 |

---

## 6. 首批 Module Notes

`P0` 第一批必须先写并维护下面这些 notes：

- [AlphaDesk_bronze_Module_Note.md](../module-notes/AlphaDesk_bronze_Module_Note.md)
- [AlphaDesk_clients_data_api_Module_Note.md](../module-notes/AlphaDesk_clients_data_api_Module_Note.md)
- [AlphaDesk_database_Module_Note.md](../module-notes/AlphaDesk_database_Module_Note.md)
- [AlphaDesk_determinism_Module_Note.md](../module-notes/AlphaDesk_determinism_Module_Note.md)
- [AlphaDesk_db_migrate_Module_Note.md](../module-notes/AlphaDesk_db_migrate_Module_Note.md)
- [AlphaDesk_write_guard_audit_Module_Note.md](../module-notes/AlphaDesk_write_guard_audit_Module_Note.md)
- [AlphaDesk_write_queue_Module_Note.md](../module-notes/AlphaDesk_write_queue_Module_Note.md)
- [AlphaDesk_writerd_Module_Note.md](../module-notes/AlphaDesk_writerd_Module_Note.md)
- [AlphaDesk_clients_gamma_Module_Note.md](../module-notes/AlphaDesk_clients_gamma_Module_Note.md)
- [AlphaDesk_strategy_base_Module_Note.md](../module-notes/AlphaDesk_strategy_base_Module_Note.md)
- [AlphaDesk_ws_subscribe_Module_Note.md](../module-notes/AlphaDesk_ws_subscribe_Module_Note.md)
- [AlphaDesk_ws_agg_v3_Module_Note.md](../module-notes/AlphaDesk_ws_agg_v3_Module_Note.md)
- [AlphaDesk_watch_only_gate_v3_Module_Note.md](../module-notes/AlphaDesk_watch_only_gate_v3_Module_Note.md)
- [AlphaDesk_health_monitor_v1_Module_Note.md](../module-notes/AlphaDesk_health_monitor_v1_Module_Note.md)
- [AlphaDesk_ui_db_replica_Module_Note.md](../module-notes/AlphaDesk_ui_db_replica_Module_Note.md)
- [AlphaDesk_readiness_checker_v1_Module_Note.md](../module-notes/AlphaDesk_readiness_checker_v1_Module_Note.md)
- [AlphaDesk_ui_lite_db_Module_Note.md](../module-notes/AlphaDesk_ui_lite_db_Module_Note.md)

---

## 7. 下一步更新规则

- 迁移开始时，把 `status` 从 `planned` 改成 `in_progress`
- 迁移完成并通过 smoke test 后，改成 `ported`
- 如果模块边界发生变化，必须同步更新 module note 和 `phase-plans/P0_Implementation_Plan.md`
- `P2-21` 关闭时，若所有 `direct_reuse` / `keep_shell_rewrite_content` 项均为 `ported` 或 `do_not_port`，即可宣告 AlphaDesk Exit Gate 通过
