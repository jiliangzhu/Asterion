# Forecast Calibration v2 Design

**版本**: v1.1
**更新日期**: 2026-03-17
**状态**: historical accepted supporting design (`Post-P4 Phase 13`)
**对应阶段**: `Post-P4 Phase 13: Calibration v2 and Threshold Probability Quality`

---

## 1. 背景与问题

当前 calibration 已经不是空壳：

- `weather.forecast_calibration_samples` 已存在
- sigma lookup 已接入 runtime
- sample sufficiency / degraded status 已进入 penalty-aware ranking

但当前模型仍然偏粗：

- 主要使用 Gaussian sigma lookup
- 缺少显式 bias correction
- 缺少 threshold-sensitive probability quality
- calibration health 仍更像单一 multiplier，而不是完整质量画像

`Post-P4 Phase 13` 的目标，是把 calibration 从“有 deterministic fallback 的 uncertainty hint”升级成真正能服务 threshold market pricing 的 probability quality layer。

---

## 2. 当前代码事实

当前相关代码落点：

- `domains/weather/forecast/calibration.py`
- `domains/weather/forecast/adapters.py`
- `domains/weather/opportunity/service.py`

当前事实判断：

- `weather.forecast_calibration_profiles_v2` 已落地
- profile 继续复用 `weather.forecast_calibration_samples` 作为原始残差事实，不替代原表
- adapter 层已经增加 deterministic correction layer，并把 `distribution_summary_v2` 写入 `ForecastDistribution` / `ForecastRunRecord.forecast_payload`
- `OpportunityAssessment` 已新增 `calibration_bias_quality` / `threshold_probability_quality`
- ranking 主公式未改，但最终 `ranking_score` 已真正消费更细的 calibration penalty

---

## 3. 锁定决策

### 3.1 Calibration Profile v2

固定新增：

- `CalibrationProfileV2`

固定维度：

- `station_id`
- `source`
- `metric`
- `forecast_horizon_bucket`
- `season_bucket`
- `regime_bucket`

固定质量维度：

- bias
- variance
- sample sufficiency
- regime stability

### 3.2 Threshold Probability Profiles

固定新增：

- `ThresholdProbabilityProfile`

固定用途：

- 直接服务 weather threshold market
- 通过历史 `weather_forecast_runs.temperature_distribution` 联合 market spec / observed facts materialize 概率分箱质量
- 不从 `forecast_calibration_samples` 单表反推 tail probability

### 3.3 Forecast Distribution Summary v2

adapter 层固定继续输出 distribution summary，但增强为：

- corrected mean
- corrected spread
- quantile summary
- threshold probability summary

### 3.4 Conformal / Quantile Layer

固定不重写整条 forecast adapter，而是在当前 adapter 旁边增加 deterministic correction layer：

- bias correction
- quantile spread correction
- conformal interval coverage

---

## 4. 接口 / Contract / Schema

### 4.1 CalibrationProfileV2

建议新增：

```python
@dataclass(frozen=True)
class CalibrationProfileV2:
    station_id: str
    source: str
    forecast_horizon_bucket: str
    season_bucket: str
    regime_bucket: str
    sample_count: int
    mean_bias: float
    mean_abs_residual: float
    p90_abs_residual: float
    empirical_coverage_50: float | None
    empirical_coverage_80: float | None
    empirical_coverage_95: float | None
    calibration_health_status: str
```

### 4.2 ThresholdProbabilityProfile

建议新增：

```python
@dataclass(frozen=True)
class ThresholdProbabilityProfile:
    threshold_bucket: str
    sample_count: int
    predicted_prob_mean: float
    realized_hit_rate: float
    brier_score: float | None
    log_loss: float | None
    calibration_curve_json: dict[str, Any]
```

### 4.3 ForecastDistributionSummaryV2

已在 adapter / pricing seam 输出：

```python
@dataclass(frozen=True)
class ForecastDistributionSummaryV2:
    raw_mean: float
    raw_std_dev: float
    corrected_mean: float
    corrected_std_dev: float
    quantiles_json: dict[str, float]
    empirical_coverage_json: dict[str, float | None]
    threshold_probability_summary_json: dict[str, Any] | None
    lookup_hit: bool
    sample_count: int
    regime_bucket: str
    calibration_health_status: str
    bias_quality_status: str
    threshold_probability_quality_status: str
```

### 4.4 Storage Contract

固定新增：

- `weather.forecast_calibration_profiles_v2`

建议列：

- `profile_key TEXT PRIMARY KEY`
- `station_id TEXT NOT NULL`
- `source TEXT NOT NULL`
- `metric TEXT NOT NULL`
- `forecast_horizon_bucket TEXT NOT NULL`
- `season_bucket TEXT NOT NULL`
- `regime_bucket TEXT NOT NULL`
- `sample_count BIGINT NOT NULL`
- `mean_bias DOUBLE NOT NULL`
- `mean_abs_residual DOUBLE NOT NULL`
- `p90_abs_residual DOUBLE NOT NULL`
- `empirical_coverage_50 DOUBLE`
- `empirical_coverage_80 DOUBLE`
- `empirical_coverage_95 DOUBLE`
- `regime_stability_score DOUBLE NOT NULL`
- `residual_quantiles_json TEXT NOT NULL`
- `threshold_probability_profile_json TEXT`
- `calibration_health_status TEXT NOT NULL`
- `window_start TIMESTAMP NOT NULL`
- `window_end TIMESTAMP NOT NULL`
- `materialized_at TIMESTAMP NOT NULL`

固定保留：

- `weather.forecast_calibration_samples`

它继续作为原始事实，不被 v2 profile 取代。

---

## 5. 数据流

```text
weather.forecast_calibration_samples
+ weather.weather_forecast_runs
+ weather.weather_market_specs
-> manual profile materialization
-> weather.forecast_calibration_profiles_v2
-> forecast adapter correction layer
-> ForecastDistributionSummaryV2
-> pricing_context / opportunity assessment / ranking
```

---

## 6. 失败模式与边界

固定失败模式：

- station/source bucket lookup miss
- regime bucket sparse
- threshold profile sparse
- empirical coverage drift

固定处理：

- fallback 到 current sigma lookup
- 显式标记：
  - `calibration_v2_lookup_missing`
  - `calibration_v2_sparse`
  - `threshold_profile_missing`
  - `regime_unstable`
- ranking 继续降权，不 silent fallback

固定边界：

- 本阶段不做新的 weather model training
- 不做 black-box probabilistic model serving
- 不把 calibration profile 写成第二套 canonical forecast truth

---

## 7. 测试策略

`Post-P4 Phase 13` 至少补：

- `tests.test_calibration_profile_v2`
- `tests.test_threshold_probability_profile`
- `tests.test_forecast_adapter_correction_layer`
- `tests.test_weather_threshold_pricing_quality`
- `tests.test_phase13_calibration_penalty_integration`

最小 acceptance：

- `.venv/bin/python3 -m unittest tests.test_calibration_profile_v2 -v`
- `.venv/bin/python3 -m unittest tests.test_forecast_adapter_correction_layer -v`
- `.venv/bin/python3 -m unittest tests.test_weather_threshold_pricing_quality -v`

---

## 8. 文档同步要求

实现本设计时必须同步：

- `Post_P4_Remediation_Implementation_Plan.md`
- `README.md`
- `Documentation_Index.md`
- `Forecast_Ensemble_Design.md`

如果 threshold probability 进入 UI diagnostics，还需要同步：

- `Execution_Economics_Design.md`
- `Operator_Console_Truth_Source_Design.md`

---

## 9. Deferred / Non-Goals

本设计明确不做：

- 重新训练 forecast foundation model
- full Bayesian forecasting stack
- cross-domain shared probability engine
- external feature store
