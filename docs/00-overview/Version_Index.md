# Asterion Version Index

**版本**: v1.0
**更新日期**: 2026-03-18
**目标**: 作为 `docs/00-overview/` 的跨版本导航入口，明确 current active version、historical versions 和未来扩展模式。

> 当前 active version: `v2.0`

---

## 1. Current Active Version

- `v2.0`
  - 状态：`implementation active`
  - overview:
    - [Asterion_Project_Plan.md](./versions/v2.0/Asterion_Project_Plan.md)
    - [DEVELOPMENT_ROADMAP.md](./versions/v2.0/DEVELOPMENT_ROADMAP.md)
  - implementation:
    - [V2_Implementation_Plan.md](../10-implementation/versions/v2.0/phase-plans/V2_Implementation_Plan.md)
    - [V2_Closeout_Checklist.md](../10-implementation/versions/v2.0/checklists/V2_Closeout_Checklist.md)

## 2. Historical Versions

- `v1.0`
  - 状态：historical accepted version record
  - summary:
    - [Version_Summary.md](./versions/v1.0/Version_Summary.md)
  - implementation bucket:
    - [phase-plans](../10-implementation/versions/v1.0/phase-plans/)
    - [checklists](../10-implementation/versions/v1.0/checklists/)
    - [runbooks](../10-implementation/versions/v1.0/runbooks/)
    - [migration-ledger](../10-implementation/versions/v1.0/migration-ledger/)
    - [module-notes](../10-implementation/versions/v1.0/module-notes/)

- `v1.0-remediation`
  - 状态：historical accepted remediation record
  - summary:
    - [Version_Summary.md](./versions/v1.0-remediation/Version_Summary.md)
  - implementation bucket:
    - [phase-plans](../10-implementation/versions/v1.0-remediation/phase-plans/)
    - [checklists](../10-implementation/versions/v1.0-remediation/checklists/)

## 3. Shared Design and Analysis Rules

- `docs/20-architecture/`、`docs/30-trading/`、`docs/40-weather/`、`docs/50-operations/`
  - 当前继续保留主题目录，不做物理版本拆分
  - 身份通常是 `shared historical + active reference` 或 `frozen supporting design`
- `docs/analysis/`
  - 固定是 `analysis input only`
  - 不作为 implementation truth-source

## 4. Future Pattern

未来新增主版本时，沿用同一模式：

- `docs/00-overview/versions/v3.0/`
- `docs/10-implementation/versions/v3.0/`

不要再把新主版本直接平铺回 `phase-plans/` 或 `checklists/` 顶层。
