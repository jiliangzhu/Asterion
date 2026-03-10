# Forecast Ensemble 详细设计

**模块**: `domains/weather/forecast/forecast_ensemble.py`  
**版本**: v3.1  
**更新日期**: 2026-03-08  
**状态**: Interface Freeze Candidate

---

## 1. 模块概述

### 1.1 冻结目标

本设计冻结 Forecast / Resolution / Settlement Verifier 之间的接口，供 Phase 1 进入 watch-only / replay / cold path 开发。

本轮选择的唯一 canonical 模式：

- `station-first`

不再保留其他并行定位模式。

### 1.2 设计原则

- `ResolutionSpec` 是结算与预测共享的权威输入
- `ForecastRequest` 必须显式携带 `station_id`
- forecast adapters 使用 `latitude / longitude`
- `StationMapper` 只负责从规则/spec 找到站点元数据
- geocode 不进入热路径

---

## 2. Forecast-Resolution Contract

这是 Forecast、Settlement Verifier、ResolutionSpec 之间共享的最小契约。

```python
from dataclasses import dataclass
from datetime import datetime
from typing import List

@dataclass
class ForecastResolutionContract:
    market_id: str
    condition_id: str
    station_id: str
    location_name: str
    latitude: float
    longitude: float
    timezone: str
    observation_window_local: str
    authoritative_source: str
    fallback_sources: list[str]
    rounding_rule: str
    inclusive_bounds: bool
    spec_version: str
```

字段语义：

- `station_id`: canonical 站点主键
- `location_name`: 展示用地点名
- `latitude / longitude`: forecast adapter 真正使用的坐标
- `observation_window_local`: 本地时间窗口表达
- `authoritative_source`: 结算权威源
- `fallback_sources`: 预测或验证时的降级源
- `spec_version`: cache key 与 replay 的必要组成

---

## 3. ResolutionSpec

`ResolutionSpec` 是 Rule2Spec 产出的结构化结果，也是 Forecast 和 Settlement Verifier 的唯一上游输入。

```python
from dataclasses import dataclass
from datetime import date

@dataclass
class ResolutionSpec:
    market_id: str
    condition_id: str
    location_name: str
    station_id: str
    latitude: float
    longitude: float
    timezone: str
    observation_date: date
    observation_window_local: str
    metric: str
    unit: str
    authoritative_source: str
    fallback_sources: list[str]
    rounding_rule: str
    inclusive_bounds: bool
    spec_version: str
```

强制约束：

- `station_id` 必填
- `latitude / longitude` 必填
- `timezone` 必填
- `authoritative_source` 与 `fallback_sources` 必须显式区分
- 不允许只给 `city` 再在热路径中临时找站点

---

## 4. ForecastRequest

`ForecastRequest` 从 `ResolutionSpec` 派生，字段必须闭合。

```python
from dataclasses import dataclass
from datetime import date, datetime

@dataclass
class ForecastRequest:
    market_id: str
    condition_id: str
    station_id: str
    source: str
    model_run: str
    forecast_target_time: datetime
    observation_date: date
    metric: str
    latitude: float
    longitude: float
    timezone: str
    spec_version: str
```

说明：

- `station_id` 用于 cache key、trace、accuracy attribution
- `latitude / longitude` 是 adapter 请求参数
- `source / model_run / forecast_target_time` 进入缓存与 replay 维度

---

## 5. StationMapper

### 5.1 职责

`StationMapper` 只负责从规则/spec 找到站点元数据，不负责在热路径做模糊 geocode。

```python
from dataclasses import dataclass

@dataclass
class StationMetadata:
    station_id: str
    location_name: str
    latitude: float
    longitude: float
    timezone: str
    source: str

class StationMapper:
    async def resolve_from_spec_inputs(
        self,
        market_id: str,
        location_name: str,
        authoritative_source: str,
    ) -> StationMetadata: ...

    async def get_station_metadata(self, station_id: str) -> StationMetadata: ...
```

### 5.2 边界

- onboarding / spec 生成阶段：允许使用 geocode 辅助选站
- 热路径：只允许 `station_id -> StationMetadata`
- replay：使用落库的 `station_id + lat/lon + spec_version`

---

## 6. Geocode 策略

### 6.1 MVP 结论

geocode 不是 MVP 强依赖。

### 6.2 使用范围

- geocode 仅用于 onboarding / spec 生成
- geocode 不在热路径调用
- geocode 不参与 watch-only replay 的关键路径

因此在 Phase 1：

- Rule2Spec / operator 确认站点后，`ResolutionSpec` 必须已闭合
- ForecastService 不再接受“只有 city 没有 station_id”的输入

---

## 7. Forecast Adapter Contract

所有 forecast adapters 统一只吃 `ForecastRequest`，并使用坐标访问外部源。

```python
from abc import ABC, abstractmethod

class ForecastAdapter(ABC):
    @abstractmethod
    async def fetch_forecast(self, request: ForecastRequest) -> "ForecastDistribution":
        ...
```

适配器约束：

- Open-Meteo：使用 `latitude / longitude`
- NWS：使用 `latitude / longitude` 或已知站点衍生 endpoint，但上游 contract 不变
- 任何 adapter 都不能要求额外的 `city`

---

## 8. Forecast Cache

### 8.1 Cache Key

cache key 至少包括：

- `market_id`
- `station_id`
- `spec_version`
- `source`
- `model_run`
- `forecast_target_time`

### 8.2 数据结构

```python
from dataclasses import dataclass
from datetime import datetime

@dataclass
class CacheKey:
    market_id: str
    station_id: str
    spec_version: str
    source: str
    model_run: str
    forecast_target_time: datetime
```

### 8.3 实现骨架

```python
import hashlib
import json

class ForecastCache:
    def make_key(self, request: ForecastRequest) -> str:
        payload = {
            "market_id": request.market_id,
            "station_id": request.station_id,
            "spec_version": request.spec_version,
            "source": request.source,
            "model_run": request.model_run,
            "forecast_target_time": request.forecast_target_time.isoformat(),
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True).encode("utf-8")
        ).hexdigest()
```

禁止项：

- cache key 不能依赖未出现在 `ForecastRequest` 的字段
- cache key 不再使用 `city`

---

## 9. Forecast Service

```python
class ForecastService:
    async def get_forecast(
        self,
        resolution_spec: ResolutionSpec,
        source: str,
        model_run: str,
        forecast_target_time: datetime,
    ) -> "ForecastDistribution":
        request = ForecastRequest(
            market_id=resolution_spec.market_id,
            condition_id=resolution_spec.condition_id,
            station_id=resolution_spec.station_id,
            source=source,
            model_run=model_run,
            forecast_target_time=forecast_target_time,
            observation_date=resolution_spec.observation_date,
            metric=resolution_spec.metric,
            latitude=resolution_spec.latitude,
            longitude=resolution_spec.longitude,
            timezone=resolution_spec.timezone,
            spec_version=resolution_spec.spec_version,
        )
        return await self.adapter_router.fetch(request)
```

---

## 10. Settlement Verifier Interface

Settlement Verifier 与 Forecast 共享同一个 `ResolutionSpec` / contract，但用途不同：

- Forecast：估计结果分布
- Settlement Verifier：校验已发生的观测值与结算条件

```python
class SettlementVerifier:
    async def verify(
        self,
        resolution_spec: ResolutionSpec,
        observed_value: float,
    ) -> "VerificationResult":
        ...
```

说明：

- `authoritative_source` 由 `ResolutionSpec` 指定
- `fallback_sources` 仅作为验证降级和证据补充
- `rounding_rule` / `inclusive_bounds` 必须在 verifier 中直接消费

---

## 11. 持久化建议

### 11.1 Weather Market Specs

```sql
CREATE TABLE weather_market_specs (
    spec_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    spec_version TEXT NOT NULL,
    location_name TEXT NOT NULL,
    station_id TEXT NOT NULL,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    timezone TEXT NOT NULL,
    observation_date DATE NOT NULL,
    observation_window_local TEXT NOT NULL,
    metric TEXT NOT NULL,
    unit TEXT NOT NULL,
    authoritative_source TEXT NOT NULL,
    fallback_sources TEXT NOT NULL,
    rounding_rule TEXT NOT NULL,
    inclusive_bounds BOOLEAN NOT NULL,
    rules_hash TEXT,
    parsed_at TIMESTAMP NOT NULL
);
```

### 11.2 Forecast Runs

```sql
CREATE TABLE weather_forecast_runs (
    run_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    spec_id TEXT NOT NULL,
    station_id TEXT NOT NULL,
    source TEXT NOT NULL,
    model_run TEXT NOT NULL,
    forecast_target_time TIMESTAMP NOT NULL,
    spec_version TEXT NOT NULL,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    timezone_used TEXT NOT NULL,
    data_staleness_seconds INTEGER,
    temperature_distribution TEXT NOT NULL,
    confidence REAL NOT NULL,
    created_at TIMESTAMP NOT NULL
);
```

---

## 12. MVP 结论

已冻结的接口：

- `ResolutionSpec`
- `ForecastResolutionContract`
- `ForecastRequest`
- `StationMapper`
- cache key 组成

Phase 1 可以基于这套 contract 开始：

- watch-only forecast pipeline
- replay / cold-path forecast recompute
- Settlement Verifier 的证据包生成

仍需保持人工参与的部分：

- 站点选择与 spec onboarding
- authoritative source 异常时的最终裁定
