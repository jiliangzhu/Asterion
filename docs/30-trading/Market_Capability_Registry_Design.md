# Market Capability Registry 详细设计

**模块**: `domains/markets/capability_registry.py`  
**版本**: v1.1  
**更新日期**: 2026-03-08  
**状态**: Interface Freeze Candidate

---

## 1. 模块概述

### 1.1 目标

Capability Registry 是执行前检查的统一来源，但必须拆成两层：

1. `MarketCapability`
2. `AccountTradingCapability`

如果把 market 与 account 能力混在同一对象里，执行前检查、风控、签名和下单会持续返工。

### 1.2 消费边界

- Router / Pricing 消费 `MarketCapability`
- Signer / OMS 消费 `AccountTradingCapability`
- 最终下单前将二者合并成 `ExecutionContext`

---

## 2. 两层能力模型

### 2.1 MarketCapability

```python
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import List

@dataclass
class MarketCapability:
    market_id: str
    condition_id: str
    token_id: str
    outcome: str
    tick_size: Decimal
    fee_rate_bps: int
    neg_risk: bool
    min_order_size: Decimal
    tradable: bool
    fees_enabled: bool
    data_sources: list[str]
    updated_at: datetime
```

说明：

- 能力粒度下沉到 `token_id`
- `fee_rate_bps` 必须按 `token_id` 动态获取
- `tick_size` 必须按 `token_id` 获取
- `neg_risk` 必须按 `token_id` 获取
- fee 不允许写死为全局默认值

### 2.2 AccountTradingCapability

```python
from dataclasses import dataclass
from typing import Optional

@dataclass
class AccountTradingCapability:
    wallet_id: str
    wallet_type: str
    signature_type: int
    funder: str
    allowance_targets: list[str]
    can_use_relayer: bool
    can_trade: bool
    restricted_reason: Optional[str]
```

说明：

- `wallet_type` 用于区分 EOA / Proxy / Safe
- `signature_type` 用于对齐官方签名模式
- `funder` 用于执行和 inventory 绑定
- `allowance_targets` 是链上 approve / spending 检查的输入

---

## 3. Capability Source Map

### 3.1 字段来源表

| Field | Capability | Primary Source | Notes |
|---|---|---|---|
| `market_id` | MarketCapability | Gamma / Markets API | 市场维度标识 |
| `condition_id` | MarketCapability | chain read | CTF condition 来源链上更可靠 |
| `token_id` | MarketCapability | Gamma / Markets API + CLOB public methods | 必须落到 token 粒度 |
| `outcome` | MarketCapability | Gamma / Markets API | 展示标签，不是账户能力 |
| `tick_size` | MarketCapability | CLOB public methods | 执行前校验使用 |
| `fee_rate_bps` | MarketCapability | CLOB public methods | 按 token 动态获取 |
| `neg_risk` | MarketCapability | Gamma / Markets API | 如有链上确认需求可二次校验 |
| `min_order_size` | MarketCapability | CLOB public methods | 不从本地猜测 |
| `tradable` | MarketCapability | Gamma / Markets API + operator override | 可被人工禁用 |
| `fees_enabled` | MarketCapability | CLOB public methods | 与 `fee_rate_bps` 配套 |
| `data_sources` | MarketCapability | local config | 记录聚合来源 |
| `updated_at` | MarketCapability | local registry timestamp | 注册表写入时间 |
| `wallet_id` | AccountTradingCapability | local config | 内部钱包引用 |
| `wallet_type` | AccountTradingCapability | local config | EOA / Proxy / Safe |
| `signature_type` | AccountTradingCapability | local config / operator override | 官方签名模式 |
| `funder` | AccountTradingCapability | local config | 资金归属地址 |
| `allowance_targets` | AccountTradingCapability | chain read + local config | spender 白名单 |
| `can_use_relayer` | AccountTradingCapability | local config / operator override | 账户级策略 |
| `can_trade` | AccountTradingCapability | chain read + local config + operator override | 账户是否可交易 |
| `restricted_reason` | AccountTradingCapability | operator override | 人工或风控限制说明 |

### 3.2 Source-of-Truth 规则

- `Gamma / Markets API` 负责市场元信息与 outcome 映射
- `CLOB public methods` 负责 `fee_rate_bps`、`tick_size`、`min_order_size` 等执行前参数
- `chain read` 负责 allowance、condition、账户授权状态
- `local config` 负责 wallet registry、source priority、静态白名单
- `operator override` 负责紧急封禁、人工限制、临时放行

---

## 4. 核心接口

### 4.1 Registry Interface

```python
class CapabilityRegistry:
    async def get_market_capability(self, token_id: str) -> MarketCapability: ...
    async def get_account_capability(self, wallet_id: str) -> AccountTradingCapability: ...
    async def build_execution_context(
        self,
        wallet_id: str,
        token_id: str,
        route_action: str,
        risk_gate_result: str,
    ) -> "ExecutionContext": ...
```

约束：

- Router 不直接读 account capability
- Signer 不直接猜 market capability
- 最终下单前必须构造 `ExecutionContext`

---

## 5. ExecutionContext

这是执行前最后的统一对象。

```python
from dataclasses import dataclass

@dataclass
class ExecutionContext:
    market_capability: MarketCapability
    account_capability: AccountTradingCapability
    token_id: str
    route_action: str
    fee_rate_bps: int
    tick_size: Decimal
    signature_type: int
    funder: str
    risk_gate_result: str
```

构造规则：

- `fee_rate_bps` 来自 `market_capability`
- `tick_size` 来自 `market_capability`
- `signature_type` 来自 `account_capability`
- `funder` 来自 `account_capability`
- `route_action` 来自 Router
- `risk_gate_result` 来自风险闸门

---

## 6. 缓存与刷新策略

### 6.1 MarketCapability

- 短 TTL 缓存
- 允许按 `token_id` 精确失效
- 在下单前二次校验高风险字段：`tick_size`、`fee_rate_bps`、`tradable`

### 6.2 AccountTradingCapability

- 长 TTL + 事件驱动刷新
- allowance / relayer / can_trade 变化时立刻失效
- operator override 优先级最高

---

## 7. 实现骨架

```python
class MarketCapabilityProvider:
    async def load(self, token_id: str) -> MarketCapability:
        gamma_data = await self._load_gamma_market(token_id)
        clob_data = await self._load_clob_market(token_id)
        chain_data = await self._load_chain_market(token_id)
        return MarketCapability(
            market_id=gamma_data["market_id"],
            condition_id=chain_data["condition_id"],
            token_id=token_id,
            outcome=gamma_data["outcome"],
            tick_size=clob_data["tick_size"],
            fee_rate_bps=clob_data["fee_rate_bps"],
            neg_risk=gamma_data["neg_risk"],
            min_order_size=clob_data["min_order_size"],
            tradable=gamma_data["tradable"],
            fees_enabled=clob_data["fees_enabled"],
            data_sources=["gamma", "clob", "chain"],
            updated_at=utcnow(),
        )

class AccountCapabilityProvider:
    async def load(self, wallet_id: str) -> AccountTradingCapability:
        wallet_cfg = await self._load_wallet_config(wallet_id)
        chain_state = await self._load_wallet_chain_state(wallet_cfg)
        override = await self._load_operator_override(wallet_id)
        return AccountTradingCapability(
            wallet_id=wallet_id,
            wallet_type=wallet_cfg["wallet_type"],
            signature_type=override.get("signature_type", wallet_cfg["signature_type"]),
            funder=wallet_cfg["funder"],
            allowance_targets=chain_state["allowance_targets"],
            can_use_relayer=wallet_cfg["can_use_relayer"],
            can_trade=chain_state["can_trade"] and not override.get("blocked", False),
            restricted_reason=override.get("reason"),
        )
```

---

## 8. 数据库设计

### 8.1 Market Capabilities

```sql
CREATE TABLE market_capabilities (
    market_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    token_id TEXT PRIMARY KEY,
    outcome TEXT NOT NULL,
    tick_size DECIMAL NOT NULL,
    fee_rate_bps INTEGER NOT NULL,
    neg_risk BOOLEAN NOT NULL,
    min_order_size DECIMAL NOT NULL,
    tradable BOOLEAN NOT NULL,
    fees_enabled BOOLEAN NOT NULL,
    data_sources TEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
```

### 8.2 Account Trading Capabilities

```sql
CREATE TABLE account_trading_capabilities (
    wallet_id TEXT PRIMARY KEY,
    wallet_type TEXT NOT NULL,
    signature_type INTEGER NOT NULL,
    funder TEXT NOT NULL,
    allowance_targets TEXT NOT NULL,
    can_use_relayer BOOLEAN NOT NULL,
    can_trade BOOLEAN NOT NULL,
    restricted_reason TEXT,
    updated_at TIMESTAMP NOT NULL
);
```

### 8.3 Capability Overrides

```sql
CREATE TABLE capability_overrides (
    override_id TEXT PRIMARY KEY,
    scope TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    field_name TEXT NOT NULL,
    value TEXT NOT NULL,
    reason TEXT,
    created_at TIMESTAMP NOT NULL
);
```

---

## 9. 冻结判断

本设计已经把 source-of-truth 收口为可实现结构：

- market capability 与 account capability 已彻底分开
- fee / tick / negRisk 已明确到 token 粒度和来源
- Gamma、CLOB public methods、chain read、local config、operator override 的职责已拆清
- 最终下单前统一收敛到 `ExecutionContext`

后续实现可以替换 provider 细节，但不能再把 market 与 account 能力重新混回一个对象。
