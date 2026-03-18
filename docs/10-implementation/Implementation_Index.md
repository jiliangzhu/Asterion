# Asterion Implementation Index

**版本**: v1.5
**更新日期**: 2026-03-18
**目标**: 作为 `docs/10-implementation/` 的统一入口，按“版本桶 -> 阶段计划 / 检查清单 / runbook / 迁移台账 / module notes”分类组织实施文档，便于快速定位 active version 与 historical versions 的 canonical materials。

> 当前仓库阶段状态：`P4 accepted; post-P4 remediation accepted; v2.0 implementation active`

---

## 1. 使用规则

1. `docs/10-implementation/` 只放实施类文档，不放架构总览或 subsystem 设计。
2. active / historical implementation materials 统一放到 `versions/<version>/...`。
3. 顶层 `checklists/Checklist_Index.md` 继续保留为全局 checklist 路由。
4. AlphaDesk 迁移总台账统一归档到 `versions/v1.0/migration-ledger/`。
5. 运行入口、操作顺序、只读面说明按版本进入 `versions/<version>/runbooks/`。
6. 单模块迁移说明统一归档到 `versions/v1.0/module-notes/`。
7. 新增实施文档时，必须先更新本索引，再更新根目录 [README.md](../../README.md) 或项目级 [Documentation_Index.md](../00-overview/Documentation_Index.md)。

---

## 2. 当前目录结构

```text
docs/10-implementation/
  Implementation_Index.md
  checklists/
    Checklist_Index.md
    # archived redirect note:
    P1_P2_AlphaDesk_Remaining_Migration_Checklist.md
  versions/
    v1.0/
      phase-plans/
      checklists/
      runbooks/
      migration-ledger/
      module-notes/
    v1.0-remediation/
      phase-plans/
      checklists/
    v2.0/
      phase-plans/
      checklists/
      runbooks/
```

---

## 3. 快速入口

### 3.1 当前阶段计划

- [V2_Implementation_Plan.md](./versions/v2.0/phase-plans/V2_Implementation_Plan.md)
  - 当前唯一 active implementation entry；已锁定 `Weather-first` v2.0 的 workstreams、phases、planned interfaces 与 acceptance 结构
- [Post_P4_Remediation_Implementation_Plan.md](./versions/v1.0-remediation/phase-plans/Post_P4_Remediation_Implementation_Plan.md)
  - historical accepted remediation record；已包含 accepted `Phase 0` 到 `Phase 9`、accepted `Post-P4 Phase 10` 到 `Post-P4 Phase 15`
- [P4_Implementation_Plan.md](./versions/v1.0/phase-plans/P4_Implementation_Plan.md)
  - historical accepted `P4` phase record
- [P3_Implementation_Plan.md](./versions/v1.0/phase-plans/P3_Implementation_Plan.md)
- [P0_Implementation_Plan.md](./versions/v1.0/phase-plans/P0_Implementation_Plan.md)
- [P1_Implementation_Plan.md](./versions/v1.0/phase-plans/P1_Implementation_Plan.md)
- [P2_Implementation_Plan.md](./versions/v1.0/phase-plans/P2_Implementation_Plan.md)

### 3.2 当前检查清单

- [Checklist_Index.md](./checklists/Checklist_Index.md)
- [V2_Closeout_Checklist.md](./versions/v2.0/checklists/V2_Closeout_Checklist.md)
- [P0_Closeout_Checklist.md](./versions/v1.0/checklists/P0_Closeout_Checklist.md)
- [P1_Closeout_Checklist.md](./versions/v1.0/checklists/P1_Closeout_Checklist.md)
- [P2_Closeout_Checklist.md](./versions/v1.0/checklists/P2_Closeout_Checklist.md)
- [P3_Closeout_Checklist.md](./versions/v1.0/checklists/P3_Closeout_Checklist.md)
- [P4_Closeout_Checklist.md](./versions/v1.0/checklists/P4_Closeout_Checklist.md)
  - archived accepted `P4` closeout record
- [Post_P4_P10_Closeout_Checklist.md](./versions/v1.0-remediation/checklists/Post_P4_P10_Closeout_Checklist.md)
- [Post_P4_P11_Closeout_Checklist.md](./versions/v1.0-remediation/checklists/Post_P4_P11_Closeout_Checklist.md)
- [Post_P4_P12_Closeout_Checklist.md](./versions/v1.0-remediation/checklists/Post_P4_P12_Closeout_Checklist.md)
- [Post_P4_P13_Closeout_Checklist.md](./versions/v1.0-remediation/checklists/Post_P4_P13_Closeout_Checklist.md)
- [Post_P4_P14_Closeout_Checklist.md](./versions/v1.0-remediation/checklists/Post_P4_P14_Closeout_Checklist.md)
- [Post_P4_P15_Closeout_Checklist.md](./versions/v1.0-remediation/checklists/Post_P4_P15_Closeout_Checklist.md)

归档历史：

- [P1_P2_AlphaDesk_Remaining_Migration_Checklist.md](./checklists/P1_P2_AlphaDesk_Remaining_Migration_Checklist.md)
  - 当前只保留为历史 redirect note；AlphaDesk exit gate 现状统一以 [AlphaDesk_Migration_Ledger.md](./versions/v1.0/migration-ledger/AlphaDesk_Migration_Ledger.md) 和 [P2_Closeout_Checklist.md](./versions/v1.0/checklists/P2_Closeout_Checklist.md) 为准

### 3.3 当前 Runbooks

- [P1_Watch_Only_Replay_Cold_Path_Runbook.md](./versions/v1.0/runbooks/P1_Watch_Only_Replay_Cold_Path_Runbook.md)
- [P2_Cold_Path_Orchestration_Job_Map_Runbook.md](./versions/v1.0/runbooks/P2_Cold_Path_Orchestration_Job_Map_Runbook.md)
- [P3_Paper_Execution_Runbook.md](./versions/v1.0/runbooks/P3_Paper_Execution_Runbook.md)
- [P4_Real_Weather_Chain_Smoke_Runbook.md](./versions/v1.0/runbooks/P4_Real_Weather_Chain_Smoke_Runbook.md)
- [P4_Controlled_Live_Smoke_Runbook.md](./versions/v1.0/runbooks/P4_Controlled_Live_Smoke_Runbook.md)
- [P4_Controlled_Rollout_Decision_Runbook.md](./versions/v1.0/runbooks/P4_Controlled_Rollout_Decision_Runbook.md)

说明：

- `P4_*` runbooks 当前都保留为 archived historical operator records
- 当前 active implementation entry 不再从 `P4` runbooks 开始

### 3.4 迁移台账

- [AlphaDesk_Migration_Ledger.md](./versions/v1.0/migration-ledger/AlphaDesk_Migration_Ledger.md)

### 3.5 Supporting Design Docs

这些 supporting design docs 不放在 `docs/10-implementation/`；它们既是 accepted `Post-P4 Phase 10` 到 `Post-P4 Phase 15` 的 frozen supporting designs，也是 `v2.0` 继续沿用同一 seam 时的参考输入：

- [Controlled_Live_Boundary_Design.md](../30-trading/Controlled_Live_Boundary_Design.md)
- [Execution_Economics_Design.md](../30-trading/Execution_Economics_Design.md)
- [Forecast_Calibration_v2_Design.md](../40-weather/Forecast_Calibration_v2_Design.md)
- [Operator_Console_Truth_Source_Design.md](../50-operations/Operator_Console_Truth_Source_Design.md)
- [UI_Read_Model_Design.md](../20-architecture/UI_Read_Model_Design.md)

### 3.6 Module Notes

- [AlphaDesk_bronze_Module_Note.md](./versions/v1.0/module-notes/AlphaDesk_bronze_Module_Note.md)
- [AlphaDesk_clients_data_api_Module_Note.md](./versions/v1.0/module-notes/AlphaDesk_clients_data_api_Module_Note.md)
- [AlphaDesk_clients_gamma_Module_Note.md](./versions/v1.0/module-notes/AlphaDesk_clients_gamma_Module_Note.md)
- [AlphaDesk_database_Module_Note.md](./versions/v1.0/module-notes/AlphaDesk_database_Module_Note.md)
- [AlphaDesk_db_migrate_Module_Note.md](./versions/v1.0/module-notes/AlphaDesk_db_migrate_Module_Note.md)
- [AlphaDesk_determinism_Module_Note.md](./versions/v1.0/module-notes/AlphaDesk_determinism_Module_Note.md)
- [AlphaDesk_write_guard_audit_Module_Note.md](./versions/v1.0/module-notes/AlphaDesk_write_guard_audit_Module_Note.md)
- [AlphaDesk_write_queue_Module_Note.md](./versions/v1.0/module-notes/AlphaDesk_write_queue_Module_Note.md)
- [AlphaDesk_writerd_Module_Note.md](./versions/v1.0/module-notes/AlphaDesk_writerd_Module_Note.md)
- [AlphaDesk_strategy_base_Module_Note.md](./versions/v1.0/module-notes/AlphaDesk_strategy_base_Module_Note.md)
- [AlphaDesk_ws_subscribe_Module_Note.md](./versions/v1.0/module-notes/AlphaDesk_ws_subscribe_Module_Note.md)
- [AlphaDesk_ws_agg_v3_Module_Note.md](./versions/v1.0/module-notes/AlphaDesk_ws_agg_v3_Module_Note.md)
- [AlphaDesk_watch_only_gate_v3_Module_Note.md](./versions/v1.0/module-notes/AlphaDesk_watch_only_gate_v3_Module_Note.md)
- [AlphaDesk_health_monitor_v1_Module_Note.md](./versions/v1.0/module-notes/AlphaDesk_health_monitor_v1_Module_Note.md)
- [AlphaDesk_readiness_checker_v1_Module_Note.md](./versions/v1.0/module-notes/AlphaDesk_readiness_checker_v1_Module_Note.md)
- [AlphaDesk_ui_db_replica_Module_Note.md](./versions/v1.0/module-notes/AlphaDesk_ui_db_replica_Module_Note.md)
- [AlphaDesk_ui_lite_db_Module_Note.md](./versions/v1.0/module-notes/AlphaDesk_ui_lite_db_Module_Note.md)

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
2. [V2_Implementation_Plan.md](./versions/v2.0/phase-plans/V2_Implementation_Plan.md)
   - 当前唯一 active implementation entry
3. [Post_P4_Remediation_Implementation_Plan.md](./versions/v1.0-remediation/phase-plans/Post_P4_Remediation_Implementation_Plan.md)
   - historical accepted remediation record，同时保留 `Post-P4 Phase 10` 到 `Post-P4 Phase 15`
4. [Controlled_Live_Boundary_Design.md](../30-trading/Controlled_Live_Boundary_Design.md)
5. [Operator_Console_Truth_Source_Design.md](../50-operations/Operator_Console_Truth_Source_Design.md)
6. [Execution_Economics_Design.md](../30-trading/Execution_Economics_Design.md)
7. [Forecast_Calibration_v2_Design.md](../40-weather/Forecast_Calibration_v2_Design.md)
8. [UI_Read_Model_Design.md](../20-architecture/UI_Read_Model_Design.md)
9. [V2_Closeout_Checklist.md](./versions/v2.0/checklists/V2_Closeout_Checklist.md)
10. [Post_P4_P10_Closeout_Checklist.md](./versions/v1.0-remediation/checklists/Post_P4_P10_Closeout_Checklist.md)
11. [Post_P4_P11_Closeout_Checklist.md](./versions/v1.0-remediation/checklists/Post_P4_P11_Closeout_Checklist.md)
12. [Post_P4_P12_Closeout_Checklist.md](./versions/v1.0-remediation/checklists/Post_P4_P12_Closeout_Checklist.md)
13. [Post_P4_P13_Closeout_Checklist.md](./versions/v1.0-remediation/checklists/Post_P4_P13_Closeout_Checklist.md)
14. [Post_P4_P14_Closeout_Checklist.md](./versions/v1.0-remediation/checklists/Post_P4_P14_Closeout_Checklist.md)
15. [Post_P4_P15_Closeout_Checklist.md](./versions/v1.0-remediation/checklists/Post_P4_P15_Closeout_Checklist.md)
16. [P4_Implementation_Plan.md](./versions/v1.0/phase-plans/P4_Implementation_Plan.md)
17. [P4_Closeout_Checklist.md](./versions/v1.0/checklists/P4_Closeout_Checklist.md)
18. [P4_Controlled_Rollout_Decision_Runbook.md](./versions/v1.0/runbooks/P4_Controlled_Rollout_Decision_Runbook.md)
19. [P4_Real_Weather_Chain_Smoke_Runbook.md](./versions/v1.0/runbooks/P4_Real_Weather_Chain_Smoke_Runbook.md)
20. [P4_Controlled_Live_Smoke_Runbook.md](./versions/v1.0/runbooks/P4_Controlled_Live_Smoke_Runbook.md)
21. [P3_Closeout_Checklist.md](./versions/v1.0/checklists/P3_Closeout_Checklist.md)
22. [P3_Paper_Execution_Runbook.md](./versions/v1.0/runbooks/P3_Paper_Execution_Runbook.md)
23. [P3_Implementation_Plan.md](./versions/v1.0/phase-plans/P3_Implementation_Plan.md)
24. [P2_Closeout_Checklist.md](./versions/v1.0/checklists/P2_Closeout_Checklist.md)
25. [P2_Implementation_Plan.md](./versions/v1.0/phase-plans/P2_Implementation_Plan.md)
26. [P1_Watch_Only_Replay_Cold_Path_Runbook.md](./versions/v1.0/runbooks/P1_Watch_Only_Replay_Cold_Path_Runbook.md)
27. [P2_Cold_Path_Orchestration_Job_Map_Runbook.md](./versions/v1.0/runbooks/P2_Cold_Path_Orchestration_Job_Map_Runbook.md)
28. [Checklist_Index.md](./checklists/Checklist_Index.md)
29. [AlphaDesk_Migration_Ledger.md](./versions/v1.0/migration-ledger/AlphaDesk_Migration_Ledger.md)
30. 对应模块的 module note

---

## 6. 维护要求

- 不要再把新的实施文档直接平铺到 `docs/10-implementation/` 根目录
- 一个阶段只保留一个 canonical 实施计划
- 当前新阶段实施统一以 [V2_Implementation_Plan.md](./versions/v2.0/phase-plans/V2_Implementation_Plan.md) 为 active implementation entry
- [Post_P4_Remediation_Implementation_Plan.md](./versions/v1.0-remediation/phase-plans/Post_P4_Remediation_Implementation_Plan.md) 保留为 historical accepted remediation record
- 同类清单放到 `checklists/`，不要混入 `phase-plans/`
- 运行入口、读路径、交接说明统一放到 `runbooks/`
- module note 更新时，同时检查 [AlphaDesk_Migration_Ledger.md](./versions/v1.0/migration-ledger/AlphaDesk_Migration_Ledger.md) 的状态字段是否需要同步
