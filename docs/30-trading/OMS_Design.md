# OMS + Inventory + Reconciliation 详细设计

**模块**: `domains/trading/oms/`  
**版本**: v1.1  
**更新日期**: 2026-03-08  
**状态**: Interface Freeze Candidate

---

## 1. 模块概述

### 1.1 职责

OMS + Inventory + Reconciliation 是执行路径里负责“订单状态、资金占用、成交落账、链上链下一致性”的最小闭环模块，负责：

1. 维护订单生命周期
2. 维护 reservation 生命周期
3. 维护 inventory positions 与 exposure snapshots
4. 对账 OMS、CLOB、链上余额与 allowance

### 1.2 设计目标

本设计要解决的不是“功能很多”，而是“实现时不走偏”：

- reservation 接口必须统一
- BUY 不再使用固定常数估算占资
- SELL 必须按 `token_id` 精确扣减
- outcome 展示标签不作为库存唯一主键
- inventory 必须与 `wallet_id / funder / signature_type` 绑定

---

## 2. Canonical Trading Objects

### 2.1 Order

```python
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional

class OrderStatus(Enum):
    CREATED = "created"
    RESERVED = "reserved"
    POSTED = "posted"
    PARTIAL_FILLED = "partial_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    REJECTED = "rejected"

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
class Order:
    order_id: str
    client_order_id: str
    wallet_id: str
    market_id: str
    token_id: str
    outcome: str
    side: OrderSide
    price: Decimal
    size: Decimal
    route_action: RouteAction
    time_in_force: TimeInForce
    expiration: Optional[datetime]
    fee_rate_bps: int
    signature_type: int
    funder: str
    status: OrderStatus
    filled_size: Decimal
    remaining_size: Decimal
    avg_fill_price: Optional[Decimal]
    reservation_id: Optional[str]
    exchange_order_id: Optional[str]
    created_at: datetime
    updated_at: datetime
```

### 2.2 Fill

```python
@dataclass
class Fill:
    fill_id: str
    order_id: str
    wallet_id: str
    market_id: str
    token_id: str
    outcome: str
    side: OrderSide
    price: Decimal
    size: Decimal
    fee: Decimal
    fee_rate_bps: int
    trade_id: str
    exchange_order_id: str
    filled_at: datetime
```

### 2.3 Reservation

```python
class ReservationStatus(Enum):
    OPEN = "open"
    PARTIALLY_CONSUMED = "partially_consumed"
    RELEASED = "released"
    CONVERTED = "converted"

@dataclass
class Reservation:
    reservation_id: str
    order_id: str
    wallet_id: str
    asset_type: str
    token_id: Optional[str]
    market_id: Optional[str]
    outcome: Optional[str]
    funder: str
    signature_type: int
    reserved_quantity: Decimal
    remaining_quantity: Decimal
    reserved_notional: Decimal
    status: ReservationStatus
    created_at: datetime
    updated_at: datetime
```

### 2.4 InventoryPosition

```python
class BalanceType(Enum):
    AVAILABLE = "available"
    RESERVED = "reserved"
    SETTLED = "settled"
    REDEEMABLE = "redeemable"

@dataclass
class InventoryPosition:
    wallet_id: str
    asset_type: str
    token_id: Optional[str]
    market_id: Optional[str]
    outcome: Optional[str]
    balance_type: BalanceType
    quantity: Decimal
    funder: str
    signature_type: int
    updated_at: datetime
```

### 2.5 ExposureSnapshot

```python
@dataclass
class ExposureSnapshot:
    snapshot_id: str
    wallet_id: str
    funder: str
    signature_type: int
    market_id: str
    token_id: str
    outcome: str
    open_order_size: Decimal
    reserved_notional_usdc: Decimal
    filled_position_size: Decimal
    settled_position_size: Decimal
    redeemable_size: Decimal
    captured_at: datetime
```

---

## 3. 库存主键模型

### 3.1 Primary Key

Asterion 的库存唯一键固定为：

- `wallet_id`
- `asset_type`
- `token_id`
- `market_id`
- `outcome`
- `balance_type`

说明：

- `wallet_id` 是内部钱包引用键
- `asset_type` 推荐最少包含 `usdc_e`、`outcome_token`
- `token_id` 是 outcome token 的真正主键；USDC.e 可为空
- `market_id` 和 `outcome` 主要用于定位和展示
- `balance_type` 区分 `available`、`reserved`、`settled`、`redeemable`

### 3.2 关键约束

- `outcome` 是展示标签，不是库存唯一键
- SELL reservation 永远按对应 `token_id` 扣减
- BUY reservation 永远预留 `USDC.e`
- inventory 记录必须能追溯到 `funder` 和 `signature_type`

---

## 4. Reservation Semantics

### 4.1 统一规则

- BUY 订单预留的是 `USDC.e`
- SELL 订单预留的是对应 `token_id`
- reservation 必须在 `order created` 后立刻建立
- reservation 金额按 `下单价格 * size` 计算
- fill 后按真实成交结果更新
- cancel / expire / reject 后释放剩余 reservation

### 4.2 计算规则

BUY:

```python
reserved_quantity = order.price * order.size
asset_type = "usdc_e"
token_id = None
```

SELL:

```python
reserved_quantity = order.size
asset_type = "outcome_token"
token_id = order.token_id
```

禁止规则：

- 不允许使用固定常数估算 BUY reservation
- 不允许按 `market_id` 或 `YES / NO` 模糊扣减 SELL inventory

### 4.3 Reservation 接口

```python
class InventoryManager:
    async def reserve_for_order(self, order: Order) -> Reservation: ...
    async def consume_reservation_for_fill(self, order: Order, fill: Fill) -> None: ...
    async def release_reservation(self, reservation_id: str, reason: str) -> None: ...
```

`reserve()` 不再接受模糊参数组合；订单 reservation 统一以 `Order` 为输入，避免接口和调用参数不一致。

---

## 5. Polymarket Inventory Semantics

### 5.1 余额语义

- `available`: 当前可再次下单或发起链上操作的数量
- `reserved`: 已被 open orders 或待确认操作占用的数量
- `settled`: 已成交、已入账、可用于持仓与PnL计算的数量
- `redeemable`: 已满足赎回条件、等待 merge / redeem 的数量

### 5.2 open orders 的影响

open orders 会占用可用余额：

- BUY open order 占用 `USDC.e.available`
- SELL open order 占用对应 `token_id.available`

因此系统不能只看链上余额；必须同时扣除尚未结束的 CLOB open orders。

### 5.3 必须对账的三套状态

系统必须持续对账：

1. `OMS internal state`
2. `CLOB open orders / fills`
3. `on-chain balances / allowances`

### 5.4 funder / signature_type 关联

同一地址体系下，不同 `signature_type` 或不同 `funder` 可能对应不同交易能力与余额解释范围，因此：

- inventory positions 必须带 `funder`
- inventory positions 必须带 `signature_type`
- reconciliation 也必须在这一维度上分桶执行

---

## 6. 最小状态流

```text
order created
-> funds reserved
-> order posted
-> partial fill
-> full fill / cancel / expire
-> reservation released / converted
```

展开说明：

1. `order created`
   订单进入 OMS，状态为 `CREATED`
2. `funds reserved`
   建立 `Reservation`，订单状态推进到 `RESERVED`
3. `order posted`
   提交到 CLOB 并拿到 `exchange_order_id`
4. `partial fill`
   reservation 被部分消耗，inventory 从 `reserved` 向 `settled`/持仓转换
5. `full fill / cancel / expire`
   订单终态确定
6. `reservation released / converted`
   未成交部分释放；已成交部分转为 settled position 或 USDC proceeds

---

## 7. OMS / Inventory 核心流程

### 7.1 创建订单

```python
class OrderManager:
    async def create_order(self, order: Order) -> Order:
        reservation = await self.inventory.reserve_for_order(order)
        order.reservation_id = reservation.reservation_id
        order.status = OrderStatus.RESERVED
        await self._save_order(order)
        return order
```

### 7.2 提交订单

```python
class OrderManager:
    async def post_order(self, order_id: str) -> Order:
        order = await self._load_order(order_id)
        exchange_order_id = await self.exchange.submit_order(order)
        order.exchange_order_id = exchange_order_id
        order.status = OrderStatus.POSTED
        await self._save_order(order)
        return order
```

### 7.3 处理成交

```python
class OrderManager:
    async def handle_fill(self, fill: Fill) -> None:
        order = await self._load_order(fill.order_id)
        await self.inventory.consume_reservation_for_fill(order, fill)
        order.filled_size += fill.size
        order.remaining_size -= fill.size
        order.avg_fill_price = recalc_avg_price(order, fill)
        order.status = (
            OrderStatus.FILLED
            if order.remaining_size == 0
            else OrderStatus.PARTIAL_FILLED
        )
        await self._save_fill(fill)
        await self._save_order(order)
```

### 7.4 取消 / 过期

```python
class OrderManager:
    async def close_order(self, order_id: str, terminal_status: OrderStatus) -> None:
        order = await self._load_order(order_id)
        await self.inventory.release_reservation(
            reservation_id=order.reservation_id,
            reason=terminal_status.value,
        )
        order.status = terminal_status
        await self._save_order(order)
```

---

## 8. Inventory Conversion Rules

### 8.1 BUY fill

- 从 `USDC.e / reserved` 扣减真实成交成本
- 将相应 `token_id / settled` 增加 `fill.size`
- 多余 reservation 留在 `reserved`，直到后续 fill 或释放

### 8.2 SELL fill

- 从 `token_id / reserved` 扣减 `fill.size`
- 将 `USDC.e / settled` 增加实际成交所得
- 成交 fee 单独记录，不允许吞并到“模糊净额”

### 8.3 cancel / expire

- 只释放剩余 reservation
- 不改动已 settle 的 fill 结果

---

## 9. Reconciliation

### 9.1 对账目标

每次对账要回答三件事：

1. OMS 认为哪些订单还在 open
2. CLOB 实际哪些订单 open / filled / cancelled
3. 链上 `balance + allowance` 是否覆盖 internal inventory

### 9.2 对账维度

- `wallet_id`
- `funder`
- `signature_type`
- `asset_type`
- `token_id`
- `market_id`
- `balance_type`

### 9.3 最小对账输出

```python
@dataclass
class ReconciliationResult:
    reconciliation_id: str
    wallet_id: str
    funder: str
    signature_type: int
    asset_type: str
    token_id: Optional[str]
    market_id: Optional[str]
    balance_type: str
    local_quantity: Decimal
    remote_quantity: Decimal
    discrepancy: Decimal
    status: str
    reason: Optional[str]
    created_at: datetime
```

---

## 10. 数据库设计

### 10.1 Orders

```sql
CREATE TABLE orders (
    order_id TEXT PRIMARY KEY,
    client_order_id TEXT UNIQUE NOT NULL,
    wallet_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    side TEXT NOT NULL,
    price DECIMAL NOT NULL,
    size DECIMAL NOT NULL,
    route_action TEXT NOT NULL,
    time_in_force TEXT NOT NULL,
    expiration TIMESTAMP,
    fee_rate_bps INTEGER NOT NULL,
    signature_type INTEGER NOT NULL,
    funder TEXT NOT NULL,
    status TEXT NOT NULL,
    filled_size DECIMAL NOT NULL DEFAULT 0,
    remaining_size DECIMAL NOT NULL,
    avg_fill_price DECIMAL,
    reservation_id TEXT,
    exchange_order_id TEXT,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
```

### 10.2 Fills

```sql
CREATE TABLE fills (
    fill_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    wallet_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    side TEXT NOT NULL,
    price DECIMAL NOT NULL,
    size DECIMAL NOT NULL,
    fee DECIMAL NOT NULL,
    fee_rate_bps INTEGER NOT NULL,
    trade_id TEXT UNIQUE NOT NULL,
    exchange_order_id TEXT NOT NULL,
    filled_at TIMESTAMP NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);
```

### 10.3 Reservations

```sql
CREATE TABLE reservations (
    reservation_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    wallet_id TEXT NOT NULL,
    asset_type TEXT NOT NULL,
    token_id TEXT,
    market_id TEXT,
    outcome TEXT,
    funder TEXT NOT NULL,
    signature_type INTEGER NOT NULL,
    reserved_quantity DECIMAL NOT NULL,
    remaining_quantity DECIMAL NOT NULL,
    reserved_notional DECIMAL NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);
```

### 10.4 Inventory Positions

```sql
CREATE TABLE inventory_positions (
    wallet_id TEXT NOT NULL,
    asset_type TEXT NOT NULL,
    token_id TEXT,
    market_id TEXT,
    outcome TEXT,
    balance_type TEXT NOT NULL,
    quantity DECIMAL NOT NULL,
    funder TEXT NOT NULL,
    signature_type INTEGER NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (wallet_id, asset_type, token_id, market_id, outcome, balance_type)
);
```

### 10.5 Exposure Snapshots

```sql
CREATE TABLE exposure_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    wallet_id TEXT NOT NULL,
    funder TEXT NOT NULL,
    signature_type INTEGER NOT NULL,
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    open_order_size DECIMAL NOT NULL,
    reserved_notional_usdc DECIMAL NOT NULL,
    filled_position_size DECIMAL NOT NULL,
    settled_position_size DECIMAL NOT NULL,
    redeemable_size DECIMAL NOT NULL,
    captured_at TIMESTAMP NOT NULL
);
```

### 10.6 Reconciliation Results

```sql
CREATE TABLE reconciliation_results (
    reconciliation_id TEXT PRIMARY KEY,
    wallet_id TEXT NOT NULL,
    funder TEXT NOT NULL,
    signature_type INTEGER NOT NULL,
    asset_type TEXT NOT NULL,
    token_id TEXT,
    market_id TEXT,
    balance_type TEXT NOT NULL,
    local_quantity DECIMAL NOT NULL,
    remote_quantity DECIMAL NOT NULL,
    discrepancy DECIMAL NOT NULL,
    status TEXT NOT NULL,
    reason TEXT,
    created_at TIMESTAMP NOT NULL
);
```

---

## 11. 冻结判断

本设计已经把最容易返工的库存语义固定下来：

- reservation 输入统一为 `Order`
- BUY reservation 固定为 `price * size` 的 `USDC.e`
- SELL reservation 固定为对应 `token_id`
- inventory key 不再依赖 `YES / NO`
- reconciliation 明确覆盖 OMS、CLOB、链上三套状态

这已经足够指导实现 OMS / inventory / reservation 的最小闭环，但还不代表系统已经具备 live readiness。
