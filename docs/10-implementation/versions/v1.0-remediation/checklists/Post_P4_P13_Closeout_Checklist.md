# Post-P4 Phase 13 Closeout Checklist

**阶段**: `Post-P4 Phase 13: Calibration v2 and Threshold Probability Quality`
**状态**: accepted (`2026-03-17`)

---

## 1. Phase Objective

把 calibration 从 sigma lookup + multiplier，升级成 bias / variance / threshold-probability aware 的 probability quality layer。

---

## 2. Delivery Lock

必须完成：

- `weather.forecast_calibration_profiles_v2`
- bias correction
- quantile / conformal uncertainty summary
- threshold probability profiles
- richer calibration health dimensions

不得顺带做：

- new forecast foundation model
- black-box calibration service

---

## 3. Must-Run Tests

- `.venv/bin/python3 -m unittest tests.test_calibration_profile_v2 -v`
- `.venv/bin/python3 -m unittest tests.test_threshold_probability_profile -v`
- `.venv/bin/python3 -m unittest tests.test_forecast_adapter_correction_layer -v`
- `.venv/bin/python3 -m unittest tests.test_weather_threshold_pricing_quality -v`
- `.venv/bin/python3 -m unittest tests.test_phase13_calibration_penalty_integration -v`

---

## 4. Required Docs Sync

- `Post_P4_Remediation_Implementation_Plan.md`
- `Documentation_Index.md`
- `Forecast_Calibration_v2_Design.md`
- `Forecast_Ensemble_Design.md`

---

## 5. Required Migration Review

必须审查：

- `weather.forecast_calibration_profiles_v2` schema
- backfill / windowing strategy
- coexistence with `weather.forecast_calibration_samples`

---

## 6. Explicit Non-Goals Not Violated

- 没有替换 canonical forecast facts
- 没有引入新模型 serving stack
- 没有把 calibration profile 变成第二套 pricing truth-source

---

## 7. Acceptance Evidence To Record

- profile materialization sample rows
- threshold probability diagnostics sample
- lookup hit / miss / sparse fallback evidence
- targeted test output

---

## 8. Ready To Mark Accepted

- threshold probability quality 已进入定价主链
- calibration health 不再只是单一 multiplier
- sparse / regime unstable / lookup miss 都可解释
- 所有必跑测试通过
