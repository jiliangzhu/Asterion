# Event Sourcing 详细设计

**版本**: v1.0
**创建日期**: 2026-03-08
**状态**: P1 - High

---

## 1. 模块概述

### 1.1 什么是 Event Sourcing

Event Sourcing 是一种架构模式，将所有状态变更记录为事件序列，而不是直接更新状态。

**核心理念**:
- 事件是不可变的
- 当前状态 = 初始状态 + 所有事件的累积
- 完整的审计追踪
- 支持时间旅行和重放

### 1.2 为什么需要 Event Sourcing

**问题**:
- 缺少完整的审计追踪
- 无法重现历史状态
- 难以调试复杂的状态变更
- 缺少因果关系追踪

**解决方案**:
- 记录所有领域事件
- 通过事件重建状态
- 完整的因果链追踪
- 支持事件重放和调试

### 1.3 架构设计

```
┌─────────────────────────────────────────────────────────────┐
│                    Event Sourcing                            │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  Business Logic                                               │
│       │                                                       │
│       │ (产生事件)                                            │
│       ▼                                                       │
│  ┌─────────────────┐                                         │
│  │  Event Store    │  (domain_events 表)                     │
│  └─────────────────┘                                         │
│       │                                                       │
│       │ (发布事件)                                            │
│       ▼                                                       │
│  ┌─────────────────┐                                         │
│  │  Event Bus      │                                         │
│  └─────────────────┘                                         │
│       │                                                       │
│       ├──────────┬──────────┬──────────┐                     │
│       ▼          ▼          ▼          ▼                     │
│  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐               │
│  │Handler1│ │Handler2│ │Handler3│ │Handler4│               │
│  └────────┘ └────────┘ └────────┘ └────────┘               │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

---

## 2. 核心组件设计

### 2.1 领域事件定义

```python
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum
import uuid

class EventType(Enum):
    """事件类型"""
    # 市场事件
    MARKET_DISCOVERED = "market_discovered"
    MARKET_ACTIVATED = "market_activated"
    MARKET_SETTLED = "market_settled"

    # 订单事件
    ORDER_CREATED = "order_created"
    ORDER_SUBMITTED = "order_submitted"
    ORDER_FILLED = "order_filled"
    ORDER_CANCELLED = "order_cancelled"

    # 库存事件
    INVENTORY_RESERVED = "inventory_reserved"
    INVENTORY_RELEASED = "inventory_released"
    INVENTORY_UPDATED = "inventory_updated"

    # CTF 事件
    CTF_SPLIT_INITIATED = "ctf_split_initiated"
    CTF_SPLIT_CONFIRMED = "ctf_split_confirmed"
    CTF_MERGE_INITIATED = "ctf_merge_initiated"
    CTF_MERGE_CONFIRMED = "ctf_merge_confirmed"
    CTF_REDEEM_INITIATED = "ctf_redeem_initiated"
    CTF_REDEEM_CONFIRMED = "ctf_redeem_confirmed"

    # UMA 事件
    UMA_PROPOSAL_DETECTED = "uma_proposal_detected"
    UMA_PROPOSAL_VERIFIED = "uma_proposal_verified"
    UMA_DISPUTE_DECIDED = "uma_dispute_decided"

    # 对账事件
    RECONCILIATION_COMPLETED = "reconciliation_completed"
    DISCREPANCY_DETECTED = "discrepancy_detected"

@dataclass
class DomainEvent:
    """领域事件"""
    event_id: str
    event_type: EventType
    aggregate_id: str  # 聚合根 ID（如 order_id, market_id）
    aggregate_type: str  # 聚合根类型（如 'order', 'market'）
    payload: Dict[str, Any]
    timestamp: datetime

    # 因果关系追踪
    causation_id: Optional[str]  # 导致此事件的事件 ID
    correlation_id: str  # 关联 ID（用于追踪整个流程）

    # 幂等性
    idempotency_key: Optional[str]

    # 元数据
    metadata: Dict[str, Any]

    @classmethod
    def create(
        cls,
        event_type: EventType,
        aggregate_id: str,
        aggregate_type: str,
        payload: Dict[str, Any],
        causation_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> 'DomainEvent':
        """创建领域事件"""
        return cls(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            payload=payload,
            timestamp=datetime.now(),
            causation_id=causation_id,
            correlation_id=correlation_id or str(uuid.uuid4()),
            idempotency_key=idempotency_key,
            metadata=metadata or {},
        )
```

### 2.2 Event Store（事件存储）

```python
import json
from typing import List, Optional

class EventStore:
    """事件存储"""

    def __init__(self, db_connection):
        self.conn = db_connection

    def append(self, event: DomainEvent):
        """追加事件"""

        # 检查幂等性
        if event.idempotency_key:
            existing = self._get_by_idempotency_key(event.idempotency_key)
            if existing:
                return  # 已存在，跳过

        # 插入事件
        self.conn.execute("""
            INSERT INTO domain_events (
                event_id, event_type, aggregate_id, aggregate_type,
                payload, timestamp, causation_id, correlation_id,
                idempotency_key, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event.event_id,
            event.event_type.value,
            event.aggregate_id,
            event.aggregate_type,
            json.dumps(event.payload),
            event.timestamp,
            event.causation_id,
            event.correlation_id,
            event.idempotency_key,
            json.dumps(event.metadata),
        ))
        self.conn.commit()

    def get_by_aggregate(
        self,
        aggregate_id: str,
        aggregate_type: str
    ) -> List[DomainEvent]:
        """获取聚合根的所有事件"""

        cursor = self.conn.execute("""
            SELECT * FROM domain_events
            WHERE aggregate_id = ? AND aggregate_type = ?
            ORDER BY timestamp ASC
        """, (aggregate_id, aggregate_type))

        events = []
        for row in cursor.fetchall():
            events.append(self._row_to_event(row))

        return events

    def get_by_correlation(self, correlation_id: str) -> List[DomainEvent]:
        """获取关联的所有事件"""

        cursor = self.conn.execute("""
            SELECT * FROM domain_events
            WHERE correlation_id = ?
            ORDER BY timestamp ASC
        """, (correlation_id,))

        events = []
        for row in cursor.fetchall():
            events.append(self._row_to_event(row))

        return events

    def get_stream(
        self,
        from_timestamp: Optional[datetime] = None,
        event_types: Optional[List[EventType]] = None,
    ) -> List[DomainEvent]:
        """获取事件流"""

        query = "SELECT * FROM domain_events WHERE 1=1"
        params = []

        if from_timestamp:
            query += " AND timestamp >= ?"
            params.append(from_timestamp)

        if event_types:
            placeholders = ','.join('?' * len(event_types))
            query += f" AND event_type IN ({placeholders})"
            params.extend([et.value for et in event_types])

        query += " ORDER BY timestamp ASC"

        cursor = self.conn.execute(query, params)

        events = []
        for row in cursor.fetchall():
            events.append(self._row_to_event(row))

        return events

    def _get_by_idempotency_key(self, key: str) -> Optional[DomainEvent]:
        """通过幂等性 key 查询事件"""
        cursor = self.conn.execute("""
            SELECT * FROM domain_events
            WHERE idempotency_key = ?
        """, (key,))

        row = cursor.fetchone()
        if row:
            return self._row_to_event(row)
        return None

    def _row_to_event(self, row) -> DomainEvent:
        """将数据库行转为事件对象"""
        return DomainEvent(
            event_id=row['event_id'],
            event_type=EventType(row['event_type']),
            aggregate_id=row['aggregate_id'],
            aggregate_type=row['aggregate_type'],
            payload=json.loads(row['payload']),
            timestamp=row['timestamp'],
            causation_id=row['causation_id'],
            correlation_id=row['correlation_id'],
            idempotency_key=row['idempotency_key'],
            metadata=json.loads(row['metadata']),
        )
```

### 2.3 Event Bus（事件总线）

```python
from typing import Callable, List, Dict
import asyncio

class EventBus:
    """事件总线"""

    def __init__(self):
        self.handlers: Dict[EventType, List[Callable]] = {}

    def subscribe(self, event_type: EventType, handler: Callable):
        """订阅事件"""
        if event_type not in self.handlers:
            self.handlers[event_type] = []
        self.handlers[event_type].append(handler)

    async def publish(self, event: DomainEvent):
        """发布事件"""

        handlers = self.handlers.get(event.event_type, [])

        # 并行执行所有 handler
        tasks = []
        for handler in handlers:
            task = asyncio.create_task(handler(event))
            tasks.append(task)

        # 等待所有 handler 完成
        await asyncio.gather(*tasks, return_exceptions=True)
```

### 2.4 Event Handlers（事件处理器）

```python
class OrderEventHandler:
    """订单事件处理器"""

    def __init__(self, inventory_manager, notification_service):
        self.inventory = inventory_manager
        self.notification = notification_service

    async def handle_order_filled(self, event: DomainEvent):
        """处理订单成交事件"""

        # 1. 更新库存
        await self.inventory.update_from_fill(event.payload)

        # 2. 发送通知
        await self.notification.send(
            f"Order {event.aggregate_id} filled: {event.payload}"
        )

class ReconciliationEventHandler:
    """对账事件处理器"""

    def __init__(self, alert_service):
        self.alert = alert_service

    async def handle_discrepancy_detected(self, event: DomainEvent):
        """处理差异检测事件"""

        # 发送告警
        await self.alert.send_critical(
            f"Discrepancy detected: {event.payload}"
        )
```

---

## 3. 完整事件流示例

### 3.1 订单生命周期事件流

```python
# 1. 创建订单
order_created_event = DomainEvent.create(
    event_type=EventType.ORDER_CREATED,
    aggregate_id="order_123",
    aggregate_type="order",
    payload={
        "market_id": "market_456",
        "side": "buy",
        "price": 0.65,
        "size": 100,
    },
    correlation_id="corr_789",
)

event_store.append(order_created_event)
await event_bus.publish(order_created_event)

# 2. 提交订单
order_submitted_event = DomainEvent.create(
    event_type=EventType.ORDER_SUBMITTED,
    aggregate_id="order_123",
    aggregate_type="order",
    payload={
        "exchange_order_id": "exch_999",
    },
    causation_id=order_created_event.event_id,
    correlation_id="corr_789",
)

event_store.append(order_submitted_event)
await event_bus.publish(order_submitted_event)

# 3. 订单成交
order_filled_event = DomainEvent.create(
    event_type=EventType.ORDER_FILLED,
    aggregate_id="order_123",
    aggregate_type="order",
    payload={
        "fill_price": 0.65,
        "fill_size": 100,
        "fee": 0.65,
    },
    causation_id=order_submitted_event.event_id,
    correlation_id="corr_789",
)

event_store.append(order_filled_event)
await event_bus.publish(order_filled_event)
```

### 3.2 事件流可视化

```
correlation_id: corr_789

order_created (event_1)
    │
    │ causation_id = event_1
    ▼
order_submitted (event_2)
    │
    │ causation_id = event_2
    ▼
order_filled (event_3)
    │
    │ causation_id = event_3
    ▼
inventory_updated (event_4)
```

---

## 4. 数据库设计

### 4.1 领域事件表

```sql
CREATE TABLE domain_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    payload TEXT NOT NULL,  -- JSON
    timestamp TIMESTAMP NOT NULL,

    -- 因果关系
    causation_id TEXT,
    correlation_id TEXT NOT NULL,

    -- 幂等性
    idempotency_key TEXT UNIQUE,

    -- 元数据
    metadata TEXT,  -- JSON

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_domain_events_aggregate ON domain_events(aggregate_id, aggregate_type);
CREATE INDEX idx_domain_events_correlation ON domain_events(correlation_id);
CREATE INDEX idx_domain_events_type ON domain_events(event_type);
CREATE INDEX idx_domain_events_timestamp ON domain_events(timestamp);
CREATE INDEX idx_domain_events_idempotency ON domain_events(idempotency_key);
```

---

## 5. 使用示例

### 5.1 初始化

```python
# 1. 创建 Event Store
event_store = EventStore(db_connection)

# 2. 创建 Event Bus
event_bus = EventBus()

# 3. 注册 Event Handlers
order_handler = OrderEventHandler(inventory_manager, notification_service)
event_bus.subscribe(EventType.ORDER_FILLED, order_handler.handle_order_filled)

reconciliation_handler = ReconciliationEventHandler(alert_service)
event_bus.subscribe(
    EventType.DISCREPANCY_DETECTED,
    reconciliation_handler.handle_discrepancy_detected
)
```

### 5.2 发布事件

```python
# 业务逻辑中发布事件
async def create_order(market_id, side, price, size):
    # 1. 创建订单
    order = Order(...)

    # 2. 创建事件
    event = DomainEvent.create(
        event_type=EventType.ORDER_CREATED,
        aggregate_id=order.order_id,
        aggregate_type="order",
        payload={
            "market_id": market_id,
            "side": side,
            "price": price,
            "size": size,
        },
    )

    # 3. 保存事件
    event_store.append(event)

    # 4. 发布事件
    await event_bus.publish(event)

    return order
```

### 5.3 事件重放

```python
def rebuild_order_state(order_id: str) -> Order:
    """通过事件重放重建订单状态"""

    # 1. 获取订单的所有事件
    events = event_store.get_by_aggregate(order_id, "order")

    # 2. 初始状态
    order = Order(order_id=order_id)

    # 3. 应用所有事件
    for event in events:
        order = apply_event(order, event)

    return order

def apply_event(order: Order, event: DomainEvent) -> Order:
    """应用事件到订单"""

    if event.event_type == EventType.ORDER_CREATED:
        order.status = OrderStatus.PENDING
        order.market_id = event.payload['market_id']
        order.side = event.payload['side']
        order.price = event.payload['price']
        order.size = event.payload['size']

    elif event.event_type == EventType.ORDER_SUBMITTED:
        order.status = OrderStatus.SUBMITTED
        order.exchange_order_id = event.payload['exchange_order_id']

    elif event.event_type == EventType.ORDER_FILLED:
        order.status = OrderStatus.FILLED
        order.filled_size = event.payload['fill_size']
        order.avg_fill_price = event.payload['fill_price']

    return order
```

---

## 6. P1-8 修复总结

### 6.1 已实现的功能

✅ **统一的领域事件表**
- `domain_events` 表
- 支持所有事件类型

✅ **因果关系追踪**
- `causation_id` - 导致此事件的事件
- `correlation_id` - 关联整个流程
- 完整的事件链

✅ **幂等性保证**
- `idempotency_key` - 防止重复事件

✅ **事件流**
- market_discovered → ... → verification_completed
- 完整的业务流程追踪

### 6.2 架构改进

- **完整审计**: 所有状态变更都有事件记录
- **可重放**: 可以通过事件重建任意时刻的状态
- **可调试**: 完整的因果链追踪
- **解耦**: 通过事件总线解耦业务逻辑

### 6.3 事件类型覆盖

- ✅ 市场事件（discovered/activated/settled）
- ✅ 订单事件（created/submitted/filled/cancelled）
- ✅ 库存事件（reserved/released/updated）
- ✅ CTF 事件（split/merge/redeem）
- ✅ UMA 事件（proposal/verified/disputed）
- ✅ 对账事件（completed/discrepancy）
