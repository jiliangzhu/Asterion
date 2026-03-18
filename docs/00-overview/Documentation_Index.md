# Asterion Documentation Index

**版本**: v1.5
**更新日期**: 2026-03-17
**目标**: 统一 Asterion 文档的目录结构、分类规则和 source-of-truth，避免后续开发中出现重复文档、重复接口定义和失效导航。

> 当前仓库阶段状态：`P4 accepted; post-P4 remediation accepted; v2.0 planning`

---

## 1. 总规则

1. `README.md` 是根目录唯一保留的文档，用于项目导航和高层介绍。
2. 其他所有项目文档统一进入 `docs/`。
3. 同一个主题只允许有一个 canonical 文档；不要为同一接口再写第二份“补充版”设计。
4. 新文档落地时，必须同时更新本索引；如果它是高层入口文档，也要同步更新根目录 `README.md`。
5. 设计变更优先改原文档，不优先新建“v2 临时说明”。

---

## 2. 目录分层

### `docs/00-overview/`

用途：

- 项目级总览
- 主计划
- 开发路线图
- 文档索引

当前文档：

- `Documentation_Index.md`
- `Asterion_Project_Plan.md`
- `DEVELOPMENT_ROADMAP.md`

source-of-truth：

- 项目范围、阶段、总体边界
- 文档归档规则
- 跨模块的开发顺序

### `docs/10-implementation/`

用途：

- 按阶段拆解的实施文档
- 任务顺序
- 交付物与验收动作
- 迁移说明、module notes、实施检查表
- runbook / 运行交接文档

当前文档：

- `Implementation_Index.md`
- `phase-plans/V2_Implementation_Plan.md`
- `phase-plans/P0_Implementation_Plan.md`
- `phase-plans/P1_Implementation_Plan.md`
- `phase-plans/P2_Implementation_Plan.md`
- `phase-plans/P3_Implementation_Plan.md`
- `phase-plans/P4_Implementation_Plan.md`
- `phase-plans/Post_P4_Remediation_Implementation_Plan.md`
- `checklists/V2_Closeout_Checklist.md`
- `checklists/P0_Closeout_Checklist.md`
- `checklists/Checklist_Index.md`
- `checklists/P1_Closeout_Checklist.md`
- `checklists/P2_Closeout_Checklist.md`
- `checklists/P3_Closeout_Checklist.md`
- `checklists/P4_Closeout_Checklist.md`
- `checklists/Post_P4_P10_Closeout_Checklist.md`
- `checklists/Post_P4_P11_Closeout_Checklist.md`
- `checklists/Post_P4_P12_Closeout_Checklist.md`
- `checklists/Post_P4_P13_Closeout_Checklist.md`
- `checklists/Post_P4_P14_Closeout_Checklist.md`
- `checklists/Post_P4_P15_Closeout_Checklist.md`
- `runbooks/P1_Watch_Only_Replay_Cold_Path_Runbook.md`
- `runbooks/P2_Cold_Path_Orchestration_Job_Map_Runbook.md`
- `runbooks/P3_Paper_Execution_Runbook.md`
- `runbooks/P4_Real_Weather_Chain_Smoke_Runbook.md`
- `runbooks/P4_Controlled_Live_Smoke_Runbook.md`
- `runbooks/P4_Controlled_Rollout_Decision_Runbook.md`
- `migration-ledger/AlphaDesk_Migration_Ledger.md`
- `module-notes/AlphaDesk_bronze_Module_Note.md`
- `module-notes/AlphaDesk_clients_data_api_Module_Note.md`
- `module-notes/AlphaDesk_database_Module_Note.md`
- `module-notes/AlphaDesk_db_migrate_Module_Note.md`
- `module-notes/AlphaDesk_determinism_Module_Note.md`
- `module-notes/AlphaDesk_write_guard_audit_Module_Note.md`
- `module-notes/AlphaDesk_write_queue_Module_Note.md`
- `module-notes/AlphaDesk_writerd_Module_Note.md`
- `module-notes/AlphaDesk_clients_gamma_Module_Note.md`
- `module-notes/AlphaDesk_strategy_base_Module_Note.md`
- `module-notes/AlphaDesk_ws_subscribe_Module_Note.md`
- `module-notes/AlphaDesk_ws_agg_v3_Module_Note.md`
- `module-notes/AlphaDesk_watch_only_gate_v3_Module_Note.md`
- `module-notes/AlphaDesk_health_monitor_v1_Module_Note.md`
- `module-notes/AlphaDesk_readiness_checker_v1_Module_Note.md`
- `module-notes/AlphaDesk_ui_db_replica_Module_Note.md`
- `module-notes/AlphaDesk_ui_lite_db_Module_Note.md`

后续建议新增：

- `module-notes/AlphaDesk_<Module>_Module_Note.md`

source-of-truth：

- “这一阶段具体先做什么、后做什么、交付什么”
- “每个阶段计划、检查清单、迁移台账分别放在哪里”
- `v2.0` 的 active planning entry，以及 `P4` / post-P4 remediation 的历史 accepted 记录
- historical remediation 路径已完成到 `Post-P4 Phase 15`
- reassessment 后续 `Phase 5+` 路线保留在历史 remediation record 中，不再作为当前 active planning entry
- 当前状态与 operator 边界的 truth-source，以 `V2_Implementation_Plan.md` 和入口文档同步口径为准；`Post_P4_Remediation_Implementation_Plan.md` 保留为 historical accepted remediation record
- `docs/analysis/*.md` 固定作为 analysis input，不升格为 implementation truth-source
- `checklists/P1_P2_AlphaDesk_Remaining_Migration_Checklist.md` 当前只保留为 archive redirect note；不要把它当成 active closeout 入口

### `docs/20-architecture/`

用途：

- 跨模块架构
- 存储与事件流
- 热路径/冷路径边界

当前文档：

- `Database_Architecture_Design.md`
- `Event_Sourcing_Design.md`
- `Hot_Cold_Path_Architecture.md`
- `UI_Read_Model_Design.md`

source-of-truth：

- 系统级结构与持久化原则
- 事件、存储、冷热路径边界

### `docs/30-trading/`

用途：

- 交易执行
- 订单路由
- OMS / inventory
- capability registry
- signer
- gas / 链上执行辅助

当前文档：

- `CLOB_Order_Router_Design.md`
- `OMS_Design.md`
- `Market_Capability_Registry_Design.md`
- `Signer_Service_Design.md`
- `Gas_Manager_Design.md`
- `Controlled_Live_Boundary_Design.md`
- `Execution_Economics_Design.md`

source-of-truth：

- canonical order contract
- inventory / reservation / fill
- execution context
- signing boundary

### `docs/40-weather/`

用途：

- Weather MVP 领域逻辑
- 预测
- resolution / settlement 监控

当前文档：

- `Forecast_Ensemble_Design.md`
- `UMA_Watcher_Design.md`
- `Forecast_Calibration_v2_Design.md`

source-of-truth：

- station-first contract
- forecast / resolution / settlement verifier 边界
- watcher 状态机与 replay 规则

### `docs/50-operations/`

用途：

- 监控
- readiness
- operator workflow
- 非交易型运营文档

当前文档：

- `Agent_Monitor_Design.md`
- `Operator_Console_Truth_Source_Design.md`

source-of-truth：

- 监控、评估、运营和长期可观测性

### `docs/analysis/`

用途：

- current code reassessment
- deep audit inputs
- 历史评估快照
- 非 canonical 的专项分析

当前文档：

- `Analysis_Index.md`
- `01_Current_Code_Reassessment.md`
- `02_Current_Deep_Audit_and_Improvement_Plan.md`
- `10_Claude_Asterion_Project_Assessment.md`
- `11_Project_Full_Assessment.md`
- `12_Remediation_Plan.md`
- `13_UI_Redesign_Assessment.md`

source-of-truth：

- 这些文档只作为 analysis input 和历史材料
- 当前 planning truth-source 是 `docs/10-implementation/phase-plans/V2_Implementation_Plan.md`
- `docs/10-implementation/phase-plans/Post_P4_Remediation_Implementation_Plan.md` 继续保留为 historical accepted remediation record

---

## 3. 新文档命名规则

### 设计文档

- 使用 `<Subsystem>_Design.md`
- 例子：`OMS_Design.md`

### 实施文档

- 使用 `P<Phase>_Implementation_Plan.md`
- 例子：`P0_Implementation_Plan.md`

建议路径：

- `docs/10-implementation/phase-plans/`

### Analysis 文档

- 使用 `<NN>_<Name>.md`
- `01-09` 表示 current analysis inputs
- `10+` 表示 historical / archived analysis snapshots
- 统一先从 `docs/analysis/Analysis_Index.md` 进入

### 迁移说明 / module note

- 使用 `AlphaDesk_<Module>_Module_Note.md`
- 例子：`AlphaDesk_write_queue_Module_Note.md`

建议路径：

- `docs/10-implementation/module-notes/`

### Runbook

- 使用 `P<Phase>_<Topic>_Runbook.md`
- 例子：`P1_Watch_Only_Replay_Cold_Path_Runbook.md`

建议路径：

- `docs/10-implementation/runbooks/`

### 决策记录

- 若后续需要 ADR，统一放到 `docs/00-overview/adr/`
- 使用 `ADR_XXXX_<Topic>.md`

---

## 4. 文档更新规则

### 项目范围变化

更新：

- `docs/00-overview/Asterion_Project_Plan.md`
- `docs/00-overview/DEVELOPMENT_ROADMAP.md`

### 阶段实施顺序变化

更新：

- 对应 `docs/10-implementation/phase-plans/P*_Implementation_Plan.md`
- 如影响整体顺序，再同步 `DEVELOPMENT_ROADMAP.md`

### 接口、数据契约、状态机变化

更新：

- 对应 subsystem 设计文档
- 若影响项目总语义，再同步 `Asterion_Project_Plan.md`

### 只影响导航或归档结构

更新：

- `Documentation_Index.md`
- 必要时同步 `README.md`

---

## 5. 当前推荐阅读顺序

1. `README.md`
2. `docs/00-overview/Documentation_Index.md`
3. `docs/00-overview/Asterion_Project_Plan.md`
4. `docs/00-overview/DEVELOPMENT_ROADMAP.md`
5. `docs/10-implementation/Implementation_Index.md`
6. `docs/10-implementation/phase-plans/P4_Implementation_Plan.md`
7. `docs/10-implementation/checklists/P4_Closeout_Checklist.md`
8. `docs/10-implementation/runbooks/P4_Controlled_Rollout_Decision_Runbook.md`
9. `docs/10-implementation/runbooks/P4_Controlled_Live_Smoke_Runbook.md`
10. `docs/10-implementation/checklists/P3_Closeout_Checklist.md`
11. `docs/10-implementation/runbooks/P3_Paper_Execution_Runbook.md`
12. `docs/10-implementation/phase-plans/P3_Implementation_Plan.md`
13. 进入对应模块设计文档

---

## 6. 维护要求

- 不在根目录新增第二个 `.md` 文档
- 不创建“临时版本”文档覆盖正式设计
- 一个目录不要同时放 overview、implementation、design 三种混合文档
- 每次新增文档，都补分类、入口和 source-of-truth 说明
- `docs/10-implementation/` 根目录只保留索引页，不再平铺阶段计划和 module notes
- 阶段 closeout 后，必须补至少一份 checklist 或 runbook，避免后续实现继续依赖口头上下文
