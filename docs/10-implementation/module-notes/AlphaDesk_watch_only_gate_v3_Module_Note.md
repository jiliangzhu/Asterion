# AlphaDesk watch_only_gate_v3 Module Note

**Source**: `AlphaDesk/alphadesk/watch_only_gate_v3.py`  
**Target**: `asterion_core/execution/watch_only_gate_v3.py`  
**Classification**: `direct_reuse`  
**Status**: `ported`

## 保留什么

- `decide_watch_only()` 的最小门控判定逻辑
- backlog / DQ / WS coverage / risk source prior 四类信号
- watch-only 恢复阈值与进入阈值分离的 hysteresis 设计

## 改什么

- 模块位置迁到 Asterion `execution/`
- 输出直接服务 Asterion watch-only runtime，而不是 AlphaDesk 旧机会链路
- 异常类型收口为常规 `TypeError/ValueError` 容错，不保留宽泛异常吞并

## 不保留什么

- 与 AlphaDesk opportunities schema 的任何耦合
- 与旧 execution plan / journal 的隐式联动

## 接入的 Asterion Contracts

- watch-only runtime 的统一 degrade / gate 判定
- queue / ws / DQ / fallback 质量信号到 `watch_only` 开关的收口

## Smoke Test

- backlog 超阈值时进入 watch-only
- 已在 watch-only 时使用 recover 阈值而不是 entry 阈值
- DQ / WS coverage / risk_source_prior 任一异常都会触发 watch-only
