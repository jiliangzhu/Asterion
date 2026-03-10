# CLOB Order Router 详细设计

**模块**: `asterion_core/execution/order_router_v1.py`  
**版本**: v1.1  
**更新日期**: 2026-03-08  
**状态**: Interface Freeze Candidate

---

## 1. 模块概述

### 1.1 职责

CLOB Order Router 是 Asterion 执行层的决策入口，负责：

1. 基于订单簿、流动性、费率和策略目标生成唯一的执行动作
2. 输出可被冻结的 canonical order contract
3. 将“策略偏好”与“交易所订单语义”彻底解耦
4. 在最终下单前与账户能力合并为 `ExecutionContext`

### 1.2 接口冻结结论

Polymarket 当前在 Asterion 中只保留四种 canonical 订单动作：

```python
from enum import Enum

class RouteAction(Enum):
    POST_ONLY_GTC = "post_only_gtc"
    POST_ONLY_GTD = "post_only_gtd"
    FAK = "fak"
    FOK = "fok"
```

强制约束：

- `postOnly` 只能用于 `GTC / GTD`
- `postOnly` 不能用于 `FAK / FOK`
- `Adaptive` 不是订单类型，而是 routing policy / 决策器
- Router 输出的是 `RouteAction`
- OMS 接收的是 `RouteAction`
- CLOB adapter 负责把 `RouteAction` 映射成官方请求里的 `orderType` 和 `postOnly`

### 1.3 非目标

以下概念不再作为 Asterion 的交易接口语义：

- 旧版“立即成交”别名
- `Adaptive` 订单类型
- `postOnly + FAK/FOK` 混合表达
- 写死的 maker/taker 费率假设

---

## 2. Canonical Order Contract

这是 Router、OMS、Signer、审计日志之间唯一共享的订单契约。

```python
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional

class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"

class TimeInForce(Enum):
    GTC = "gtc"
    GTD = "gtd"
    FAK = "fak"
    FOK = "fok"

class RouteAction(Enum):
    POST_ONLY_GTC = "post_only_gtc"
    POST_ONLY_GTD = "post_only_gtd"
    FAK = "fak"
    FOK = "fok"

@dataclass
class CanonicalOrderContract:
    market_id: str
    token_id: str
    outcome: str
    side: OrderSide
    price: Decimal
    size: Decimal
    route_action: RouteAction
    expiration: Optional[datetime]
    time_in_force: TimeInForce
    fee_rate_bps: int
    signature_type: int
    funder: str
```

字段语义：

- `market_id`: Polymarket condition / market 维度标识
- `token_id`: 实际下单的 outcome token 标识
- `outcome`: 业务语义上的 `YES` / `NO` 或 domain outcome label
- `route_action`: 唯一订单动作
- `expiration`: 仅 `POST_ONLY_GTD` 必填；其余动作可为空
- `time_in_force`: 由 `route_action` 归一化得出，不允许调用方另行发明取值
- `fee_rate_bps`: 从 market capability / 官方市场配置解析后的订单费率
- `signature_type`: 传递给官方签名/下单能力的签名类型
- `funder`: Polymarket 订单和资金归属地址

补充约束：

- `outcome` 是展示标签，不是 inventory 主键
- Router 不负责生成 reservation
- inventory / funder / signature_type 约束在 `ExecutionContext` 阶段闭合

归一化规则：

| RouteAction | time_in_force | postOnly | expiration |
|---|---|---|---|
| `POST_ONLY_GTC` | `GTC` | `true` | `None` |
| `POST_ONLY_GTD` | `GTD` | `true` | 必填 |
| `FAK` | `FAK` | `false` | 可空 |
| `FOK` | `FOK` | `false` | 可空 |

---

## 3. 路由策略与执行动作解耦

### 3.1 Routing Policy

策略层可以表达自己的意图，但不能越过 Router 直接发明订单类型。

```python
class RoutingPolicy(Enum):
    PASSIVE = "passive"
    URGENT = "urgent"
    ADAPTIVE = "adaptive"
```

说明：

- `PASSIVE` 倾向挂单，通常落到 `POST_ONLY_GTC` 或 `POST_ONLY_GTD`
- `URGENT` 倾向立即成交，通常落到 `FAK` 或 `FOK`
- `ADAPTIVE` 只是决策模式，不是订单动作

### 3.2 决策输出

```python
from dataclasses import dataclass
from decimal import Decimal

@dataclass
class RoutingDecision:
    route_action: RouteAction
    limit_price: Decimal
    time_in_force: TimeInForce
    expiration: Optional[datetime]
    fee_rate_bps: int
    expected_slippage_bps: int
    total_cost_bps: int
    confidence: float
    reason: str
```

`RoutingDecision` 是 Router 的最终产物；后续模块不得再把它转换为别的“内部订单类型”。

---

## 4. Market Capability 与费率模型

### 4.1 原则

- Router 必须从 Market Capability Registry 读取市场能力
- Router 只消费 `MarketCapability`
- 费率是 market/token 级动态配置，不是全局常量
- `fee_rate_bps` 必须进入 canonical order contract
- Weather MVP 即使当前大多 fee-free，也不允许写死固定 maker/taker 费率

### 4.2 数据结构

```python
from dataclasses import dataclass
from decimal import Decimal

@dataclass
class MarketCapability:
    market_id: str
    condition_id: str
    token_id: str
    outcome: str
    fees_enabled: bool
    fee_rate_bps: int
    tick_size: Decimal
    neg_risk: bool
    min_order_size: Decimal
    tradable: bool
    data_sources: list[str]
    updated_at: datetime
```

说明：

- 对 Router 来说，最重要的是“这个 market/token 当前允许什么动作”以及“当前费率是多少”
- `tick_size`、`fee_rate_bps`、`neg_risk` 都必须按 `token_id` 获取
- Router 不消费 `AccountTradingCapability`

---

## 5. 核心组件

### 5.1 Liquidity Estimator

负责：

- 读取订单簿快照
- 评估深度、spread、可成交数量
- 估算不同 `size` 下的成交完成度

### 5.2 Slippage Model

负责：

- 对 `FAK` / `FOK` 估算立即成交滑点
- 对 `POST_ONLY_*` 估算排队位置和预期成交时间，而不是把它们伪装成“零滑点”

### 5.3 Fee Resolver

负责：

- 从 Market Capability Registry 取回 `fee_rate_bps`
- 将费率装入 `CanonicalOrderContract`
- 将费率暴露给策略评估与审计日志

### 5.4 Routing Engine

负责：

- 把 `RoutingPolicy` 归一化为 `RouteAction`
- 校验 capability 与动作组合是否合法
- 产生唯一 `RoutingDecision`

### 5.5 ExecutionContext Handoff

Router 不直接做账户与签名校验；它输出 `CanonicalOrderContract` 后，由执行编排层合并：

```python
@dataclass
class ExecutionContext:
    market_capability: MarketCapability
    account_capability: "AccountTradingCapability"
    token_id: str
    route_action: RouteAction
    fee_rate_bps: int
    tick_size: Decimal
    signature_type: int
    funder: str
    risk_gate_result: str
```

说明：

- `market_capability` 来自 Capability Registry 的市场层
- `account_capability` 来自 Capability Registry 的账户层
- Router 只负责提供 `route_action`
- 最终 submit 前必须通过 `ExecutionContext`

---

## 6. CLOB Adapter 映射契约

Adapter 是唯一允许接触交易所细节的层。

```python
class PolymarketClobAdapter:
    def map_route_action(
        self,
        route_action: RouteAction,
        expiration: Optional[datetime],
    ) -> tuple[str, bool, TimeInForce]:
        if route_action == RouteAction.POST_ONLY_GTC:
            return ("GTC", True, TimeInForce.GTC)
        if route_action == RouteAction.POST_ONLY_GTD:
            if expiration is None:
                raise ValueError("POST_ONLY_GTD requires expiration")
            return ("GTD", True, TimeInForce.GTD)
        if route_action == RouteAction.FAK:
            return ("FAK", False, TimeInForce.FAK)
        if route_action == RouteAction.FOK:
            return ("FOK", False, TimeInForce.FOK)
        raise ValueError(f"Unsupported route_action: {route_action}")
```

适配器层约束：

- 不接受单独的 `postOnly` 布尔值作为上游输入
- 不接受任何旧版“立即成交”别名
- 不在 adapter 内部做“Adaptive 路由”
- adapter 只做映射，不做策略决策

---

## 7. 参考决策流程

```python
def route(intent, snapshot, capability, policy) -> RoutingDecision:
    if policy == RoutingPolicy.PASSIVE:
        action = (
            RouteAction.POST_ONLY_GTD
            if intent.expiration is not None
            else RouteAction.POST_ONLY_GTC
        )
    elif policy == RoutingPolicy.URGENT:
        action = RouteAction.FOK if can_fully_fill(snapshot, intent.size) else RouteAction.FAK
    else:
        # ADAPTIVE 是 policy，不是订单类型
        if capability.fees_enabled and queue_alpha_is_positive(intent, snapshot):
            action = RouteAction.POST_ONLY_GTC
        elif can_fully_fill(snapshot, intent.size):
            action = RouteAction.FOK
        else:
            action = RouteAction.FAK

    tif = normalize_time_in_force(action)
    fee_rate_bps = capability.fee_rate_bps
    limit_price = choose_limit_price(intent.side, snapshot, action)

    return RoutingDecision(
        route_action=action,
        limit_price=limit_price,
        time_in_force=tif,
        expiration=intent.expiration,
        fee_rate_bps=fee_rate_bps,
        expected_slippage_bps=estimate_slippage(snapshot, intent.size, action),
        total_cost_bps=estimate_total_cost(snapshot, intent.size, action, fee_rate_bps),
        confidence=0.8,
        reason="policy normalized to canonical route action",
    )
```

最终下单前：

```python
execution_context = capability_registry.build_execution_context(
    wallet_id=intent.wallet_id,
    token_id=intent.token_id,
    route_action=decision.route_action,
    risk_gate_result=risk_gate_result,
)
```

---

## 8. 审计与持久化

Router 日志建议至少记录：

- `request_id`
- `market_id`
- `token_id`
- `outcome`
- `side`
- `price`
- `size`
- `route_action`
- `time_in_force`
- `expiration`
- `fee_rate_bps`
- `tick_size`
- `reason`

这样可以把“为什么选 FAK 而不是 POST_ONLY_GTC”追溯到具体 market capability 与订单簿状态。

---

## 9. 冻结判断

本设计冻结的接口边界如下：

- 策略层只能表达 `RoutingPolicy`
- Router 只能输出 `RouteAction`
- OMS 只能接收 `CanonicalOrderContract`
- CLOB adapter 只能把 `RouteAction` 映射到官方 `orderType + postOnly`

只要这四层保持不变，后续实现可以替换内部算法，但不会引发执行层语义返工。
