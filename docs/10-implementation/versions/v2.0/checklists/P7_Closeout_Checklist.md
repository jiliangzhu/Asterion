# P7 Closeout Checklist

**版本**: v1.0  
**更新日期**: 2026-03-21  
**阶段**: `v2.0 / Phase 7`  
**状态**: accepted closeout checklist  
**主题**: Deployable Rerank, Allocator v2, and Execution Economics Closure

---

> 本文件用于收口 `Phase 7` 的 closeout 条件。  
> `Phase 7` 已 accepted；本文件保留为 historical accepted closeout checklist。  
> umbrella active implementation contract 仍是 [V2_Implementation_Plan.md](../phase-plans/V2_Implementation_Plan.md)。

## 1. Closeout Items

1. rerank acceptance file landed
   - `tests.test_deployable_rerank_acceptance`
2. allocator / paper / UI order consistency landed
   - `tests.test_allocator_rerank_surface_consistency`
3. preview vs final constraint explanation landed
   - `Home` / `Markets` / persisted read models 能直接展示：
     - `base_ranking_score`
     - `pre_budget_deployable_expected_pnl`
     - final deployable `ranking_score`
     - preview dominant structural constraint
     - final dominant constraint
     - rerank reason
4. retrospective uplift integration acceptance landed
   - `tests.test_retrospective_uplift_integration`
5. `P7` / `V2` current-state wording refreshed
6. doc/index hygiene tests updated

## 2. Minimum Acceptance

- 同一 fixture 下：
  - allocator final order
  - paper path order / size
  - `ui.action_queue_summary` order
  必须一致
- retrospective comparison seam 必须能对真实 materialized rows 输出 deterministic uplift summary
- `V2` umbrella 不再把已落地 `P7` 工作写成未实现
- `git diff --check` 通过
