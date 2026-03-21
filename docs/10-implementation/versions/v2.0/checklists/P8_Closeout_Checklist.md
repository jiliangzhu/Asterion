# P8 Closeout Checklist

**版本**: v1.0  
**更新日期**: 2026-03-21  
**阶段**: `v2.0 / Phase 8`  
**状态**: accepted closeout checklist  
**主题**: Calibration Hard Gates and Scaling-Aware Capital Discipline

---

> 本文件用于收口 `Phase 8` 的 closeout 条件。  
> `Phase 8` 已 accepted；本文件保留为最近 accepted tranche 的 closeout checklist。  
> umbrella active implementation contract 仍是 [V2_Implementation_Plan.md](../phase-plans/V2_Implementation_Plan.md)。

## 1. Closeout Items

1. calibration hard gate acceptance landed
   - `tests.test_calibration_hard_gate_acceptance`
2. calibration impacted-market summary landed
   - `tests.test_calibration_impacted_market_summary`
3. scaling-aware capital policy lookup landed
   - `tests.test_scaling_aware_capital_policy_lookup`
4. allocator scaling discipline acceptance landed
   - `tests.test_allocator_scaling_discipline_acceptance`
5. operator surface acceptance landed
   - `tests.test_p8_operator_surface_acceptance`
6. no-calibration-context default-clear regression landed
   - `tests.test_calibration_gate_default_clear`
7. fallback / degraded source gate preservation landed
   - `tests.test_calibration_gate_fallback_surfaces`
   - `tests.test_market_chain_degraded_source_preserves_gate_fields`
8. `P8` / `V2` / entry docs accepted wording refreshed
9. doc/index hygiene tests updated

## 2. Minimum Acceptance

- stale calibration 不再只作为 soft penalty，必须改变 actionability / operator bucket
- degraded_or_missing calibration 的 non-actionable 语义必须稳定持久化到 UI surfaces
- scaling-aware capital policy lookup 与 allocator output deterministic
- fallback / degraded source 下，gate fields 不会 silently 丢失
- `V2` umbrella 与入口导航不再把 `P8` 写成 current tranche
- `git diff --check` 通过
