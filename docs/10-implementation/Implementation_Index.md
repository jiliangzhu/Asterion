# Asterion Implementation Index

**版本**: v1.3
**更新日期**: 2026-03-16
**目标**: 作为 `docs/10-implementation/` 的统一入口，按“阶段计划 / 检查清单 / runbook / 迁移台账 / module notes”分类组织实施文档，便于快速定位每个阶段的实施计划、关闭条件和运行入口。

---

## 1. 使用规则

1. `docs/10-implementation/` 只放实施类文档，不放架构总览或 subsystem 设计。
2. 阶段实施计划统一放到 `phase-plans/`。
3. 阶段检查清单、关闭条件、跨阶段收口清单统一放到 `checklists/`。
4. AlphaDesk 迁移总台账统一放到 `migration-ledger/`。
5. 运行入口、操作顺序、只读面说明统一放到 `runbooks/`。
6. 单模块迁移说明统一放到 `module-notes/`。
7. 新增实施文档时，必须先更新本索引，再更新根目录 [README.md](../../README.md) 或项目级 [Documentation_Index.md](../00-overview/Documentation_Index.md)。

---

## 2. 当前目录结构

```text
docs/10-implementation/
  Implementation_Index.md
  phase-plans/
    Post_P4_Remediation_Implementation_Plan.md
    P0_Implementation_Plan.md
    P1_Implementation_Plan.md
    P2_Implementation_Plan.md
    P3_Implementation_Plan.md
    P4_Implementation_Plan.md
  checklists/
    P0_Closeout_Checklist.md
    P1_Closeout_Checklist.md
    P2_Closeout_Checklist.md
    P3_Closeout_Checklist.md
    P4_Closeout_Checklist.md
    P1_P2_AlphaDesk_Remaining_Migration_Checklist.md
  runbooks/
    P1_Watch_Only_Replay_Cold_Path_Runbook.md
    P2_Cold_Path_Orchestration_Job_Map_Runbook.md
    P3_Paper_Execution_Runbook.md
    P4_Real_Weather_Chain_Smoke_Runbook.md
    P4_Controlled_Live_Smoke_Runbook.md
    P4_Controlled_Rollout_Decision_Runbook.md
  migration-ledger/
    AlphaDesk_Migration_Ledger.md
  module-notes/
    AlphaDesk_bronze_Module_Note.md
    AlphaDesk_clients_data_api_Module_Note.md
    AlphaDesk_clients_gamma_Module_Note.md
    AlphaDesk_database_Module_Note.md
    AlphaDesk_db_migrate_Module_Note.md
    AlphaDesk_determinism_Module_Note.md
    AlphaDesk_write_guard_audit_Module_Note.md
    AlphaDesk_write_queue_Module_Note.md
    AlphaDesk_writerd_Module_Note.md
    AlphaDesk_strategy_base_Module_Note.md
    AlphaDesk_ws_subscribe_Module_Note.md
    AlphaDesk_ws_agg_v3_Module_Note.md
    AlphaDesk_watch_only_gate_v3_Module_Note.md
    AlphaDesk_health_monitor_v1_Module_Note.md
    AlphaDesk_readiness_checker_v1_Module_Note.md
    AlphaDesk_ui_db_replica_Module_Note.md
    AlphaDesk_ui_lite_db_Module_Note.md
```

---

## 3. 快速入口

### 3.1 当前阶段计划

- [Post_P4_Remediation_Implementation_Plan.md](./phase-plans/Post_P4_Remediation_Implementation_Plan.md)
  - 当前 active canonical plan；已包含 accepted `Phase 0` 到 `Phase 9`、residual gaps repair status，以及 reassessment 后续 `Phase 5+` 路线
- [P4_Implementation_Plan.md](./phase-plans/P4_Implementation_Plan.md)
- [P3_Implementation_Plan.md](./phase-plans/P3_Implementation_Plan.md)
- [P0_Implementation_Plan.md](./phase-plans/P0_Implementation_Plan.md)
- [P1_Implementation_Plan.md](./phase-plans/P1_Implementation_Plan.md)
- [P2_Implementation_Plan.md](./phase-plans/P2_Implementation_Plan.md)

### 3.2 当前检查清单

- [P0_Closeout_Checklist.md](./checklists/P0_Closeout_Checklist.md)
- [P1_Closeout_Checklist.md](./checklists/P1_Closeout_Checklist.md)
- [P2_Closeout_Checklist.md](./checklists/P2_Closeout_Checklist.md)
- [P3_Closeout_Checklist.md](./checklists/P3_Closeout_Checklist.md)
- [P4_Closeout_Checklist.md](./checklists/P4_Closeout_Checklist.md)
- [P1_P2_AlphaDesk_Remaining_Migration_Checklist.md](./checklists/P1_P2_AlphaDesk_Remaining_Migration_Checklist.md)

### 3.3 当前 Runbooks

- [P1_Watch_Only_Replay_Cold_Path_Runbook.md](./runbooks/P1_Watch_Only_Replay_Cold_Path_Runbook.md)
- [P2_Cold_Path_Orchestration_Job_Map_Runbook.md](./runbooks/P2_Cold_Path_Orchestration_Job_Map_Runbook.md)
- [P3_Paper_Execution_Runbook.md](./runbooks/P3_Paper_Execution_Runbook.md)
- [P4_Real_Weather_Chain_Smoke_Runbook.md](./runbooks/P4_Real_Weather_Chain_Smoke_Runbook.md)
- [P4_Controlled_Live_Smoke_Runbook.md](./runbooks/P4_Controlled_Live_Smoke_Runbook.md)
- [P4_Controlled_Rollout_Decision_Runbook.md](./runbooks/P4_Controlled_Rollout_Decision_Runbook.md)

### 3.4 迁移台账

- [AlphaDesk_Migration_Ledger.md](./migration-ledger/AlphaDesk_Migration_Ledger.md)

### 3.5 Module Notes

- [AlphaDesk_bronze_Module_Note.md](./module-notes/AlphaDesk_bronze_Module_Note.md)
- [AlphaDesk_clients_data_api_Module_Note.md](./module-notes/AlphaDesk_clients_data_api_Module_Note.md)
- [AlphaDesk_clients_gamma_Module_Note.md](./module-notes/AlphaDesk_clients_gamma_Module_Note.md)
- [AlphaDesk_database_Module_Note.md](./module-notes/AlphaDesk_database_Module_Note.md)
- [AlphaDesk_db_migrate_Module_Note.md](./module-notes/AlphaDesk_db_migrate_Module_Note.md)
- [AlphaDesk_determinism_Module_Note.md](./module-notes/AlphaDesk_determinism_Module_Note.md)
- [AlphaDesk_write_guard_audit_Module_Note.md](./module-notes/AlphaDesk_write_guard_audit_Module_Note.md)
- [AlphaDesk_write_queue_Module_Note.md](./module-notes/AlphaDesk_write_queue_Module_Note.md)
- [AlphaDesk_writerd_Module_Note.md](./module-notes/AlphaDesk_writerd_Module_Note.md)
- [AlphaDesk_strategy_base_Module_Note.md](./module-notes/AlphaDesk_strategy_base_Module_Note.md)
- [AlphaDesk_ws_subscribe_Module_Note.md](./module-notes/AlphaDesk_ws_subscribe_Module_Note.md)
- [AlphaDesk_ws_agg_v3_Module_Note.md](./module-notes/AlphaDesk_ws_agg_v3_Module_Note.md)
- [AlphaDesk_watch_only_gate_v3_Module_Note.md](./module-notes/AlphaDesk_watch_only_gate_v3_Module_Note.md)
- [AlphaDesk_health_monitor_v1_Module_Note.md](./module-notes/AlphaDesk_health_monitor_v1_Module_Note.md)
- [AlphaDesk_readiness_checker_v1_Module_Note.md](./module-notes/AlphaDesk_readiness_checker_v1_Module_Note.md)
- [AlphaDesk_ui_db_replica_Module_Note.md](./module-notes/AlphaDesk_ui_db_replica_Module_Note.md)
- [AlphaDesk_ui_lite_db_Module_Note.md](./module-notes/AlphaDesk_ui_lite_db_Module_Note.md)

---

## 4. 阶段文档定位

### `phase-plans/`

用途：

- 每个阶段的实施顺序
- 工作包拆解
- 交付物和验收条件

命名规则：

- `P<Phase>_Implementation_Plan.md`

### `checklists/`

用途：

- 阶段关闭条件
- 跨阶段迁移清单
- 开工前/关闭前审查项

命名规则：

- `P<Phase>_Closeout_Checklist.md`
- `<Topic>_Checklist.md`

### `runbooks/`

用途：

- 已落地链路的运行入口
- canonical 表和 operator 只读面
- human-in-the-loop 边界
- 阶段收尾后的交接文档

命名规则：

- `P<Phase>_<Topic>_Runbook.md`

### `migration-ledger/`

用途：

- 跨阶段持续维护的迁移总台账

命名规则：

- `<Source>_Migration_Ledger.md`

### `module-notes/`

用途：

- 单模块迁移适配说明
- 保留什么、删什么、测什么

命名规则：

- `<Source>_<Module>_Module_Note.md`

---

## 5. 当前建议阅读顺序

1. [Implementation_Index.md](./Implementation_Index.md)
2. [Post_P4_Remediation_Implementation_Plan.md](./phase-plans/Post_P4_Remediation_Implementation_Plan.md)
   - 当前 canonical remediation plan，同时承载 reassessment residual gaps 的 `Phase 5+` 路线
3. [P4_Implementation_Plan.md](./phase-plans/P4_Implementation_Plan.md)
4. [P4_Closeout_Checklist.md](./checklists/P4_Closeout_Checklist.md)
5. [P4_Controlled_Rollout_Decision_Runbook.md](./runbooks/P4_Controlled_Rollout_Decision_Runbook.md)
6. [P4_Real_Weather_Chain_Smoke_Runbook.md](./runbooks/P4_Real_Weather_Chain_Smoke_Runbook.md)
7. [P4_Controlled_Live_Smoke_Runbook.md](./runbooks/P4_Controlled_Live_Smoke_Runbook.md)
8. [P3_Closeout_Checklist.md](./checklists/P3_Closeout_Checklist.md)
9. [P3_Paper_Execution_Runbook.md](./runbooks/P3_Paper_Execution_Runbook.md)
10. [P3_Implementation_Plan.md](./phase-plans/P3_Implementation_Plan.md)
11. [P2_Closeout_Checklist.md](./checklists/P2_Closeout_Checklist.md)
12. [P2_Implementation_Plan.md](./phase-plans/P2_Implementation_Plan.md)
13. [P1_Watch_Only_Replay_Cold_Path_Runbook.md](./runbooks/P1_Watch_Only_Replay_Cold_Path_Runbook.md)
14. [P2_Cold_Path_Orchestration_Job_Map_Runbook.md](./runbooks/P2_Cold_Path_Orchestration_Job_Map_Runbook.md)
15. [P1_P2_AlphaDesk_Remaining_Migration_Checklist.md](./checklists/P1_P2_AlphaDesk_Remaining_Migration_Checklist.md)
16. [AlphaDesk_Migration_Ledger.md](./migration-ledger/AlphaDesk_Migration_Ledger.md)
17. 对应模块的 module note

---

## 6. 维护要求

- 不要再把新的实施文档直接平铺到 `docs/10-implementation/` 根目录
- 一个阶段只保留一个 canonical 实施计划
- post-P4 remediation 统一以 [Post_P4_Remediation_Implementation_Plan.md](./phase-plans/Post_P4_Remediation_Implementation_Plan.md) 为实施入口
- 同类清单放到 `checklists/`，不要混入 `phase-plans/`
- 运行入口、读路径、交接说明统一放到 `runbooks/`
- module note 更新时，同时检查 [AlphaDesk_Migration_Ledger.md](./migration-ledger/AlphaDesk_Migration_Ledger.md) 的状态字段是否需要同步
