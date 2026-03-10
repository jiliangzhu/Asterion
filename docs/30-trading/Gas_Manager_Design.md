# Gas Manager 详细设计

**模块**: `asterion_core/blockchain/gas_manager.py`
**版本**: v1.0
**创建日期**: 2026-03-07

---

## 1. 模块概述

### 1.1 职责

Gas Manager 是 Asterion 的区块链交易管理核心，负责：

1. **Gas 价格优化** - 预测和选择最优 Gas 价格
2. **交易批处理** - 合并多个操作减少交易数量
3. **Nonce 管理** - 防止 nonce 冲突和交易卡住
4. **交易监控** - 追踪交易状态和自动重试
5. **成本控制** - 追踪和优化链上成本

### 1.2 Polygon 特性

**Polygon PoS 链特点**:
- 平均出块时间：2-3 秒
- Gas 价格波动：相对稳定，但高峰期会飙升
- 交易确认：通常 1-2 个区块
- Gas Token：MATIC

**关键挑战**:
- Gas 价格预测不准确会导致交易延迟或浪费
- 高频交易需要精确的 nonce 管理
- 交易失败需要快速重试机制

### 1.3 架构设计

```
┌─────────────────────────────────────────────────────────┐
│                      Gas Manager                         │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────┐  │
│  │     Gas      │    │ Transaction  │    │  Nonce   │  │
│  │  Estimator   │    │   Batcher    │    │ Manager  │  │
│  └──────────────┘    └──────────────┘    └──────────┘  │
│         │                    │                   │       │
│         └────────────────────┴───────────────────┘       │
│                              │                            │
│                    ┌─────────▼─────────┐                 │
│                    │   Transaction     │                 │
│                    │     Monitor       │                 │
│                    └─────────┬─────────┘                 │
│                              │                            │
│                    ┌─────────▼─────────┐                 │
│                    │   Web3 Provider   │                 │
│                    │   (Polygon RPC)   │                 │
│                    └───────────────────┘                 │
│                                                           │
└─────────────────────────────────────────────────────────┘
```

---

## 2. 核心组件设计

### 2.1 Gas Estimator（Gas 估算器）

**文件**: `asterion_core/blockchain/gas_estimator.py`

#### 2.1.1 数据结构

```python
from dataclasses import dataclass
from typing import Optional
from decimal import Decimal
from enum import Enum

class GasSpeed(Enum):
    """Gas 速度级别"""
    SLOW = "slow"        # 5-10 分钟
    STANDARD = "standard"  # 2-5 分钟
    FAST = "fast"        # < 2 分钟
    INSTANT = "instant"  # < 30 秒

@dataclass
class GasPrice:
    """Gas 价格"""
    max_fee_per_gas: int  # Wei
    max_priority_fee_per_gas: int  # Wei
    base_fee: int  # Wei
    speed: GasSpeed
    estimated_time_seconds: int
    confidence: float  # 0-1

@dataclass
class GasEstimate:
    """Gas 估算结果"""
    gas_limit: int
    gas_prices: dict[GasSpeed, GasPrice]
    estimated_cost_matic: Decimal
    estimated_cost_usd: Decimal
    timestamp: float
```

#### 2.1.2 核心实现

```python
from web3 import Web3
from web3.types import Wei

class GasEstimator:
    """Gas 估算器"""
    
    def __init__(self, w3: Web3, matic_price_usd: Decimal = Decimal("0.5")):
        self.w3 = w3
        self.matic_price_usd = matic_price_usd
    
    def estimate_gas(
        self,
        transaction: dict,
        speed: GasSpeed = GasSpeed.STANDARD
    ) -> GasEstimate:
        """估算交易 Gas"""
        
        # 1. 估算 gas limit
        gas_limit = self._estimate_gas_limit(transaction)
        
        # 2. 获取各速度级别的 gas 价格
        gas_prices = self._get_gas_prices()
        
        # 3. 计算成本
        selected_price = gas_prices[speed]
        cost_wei = gas_limit * selected_price.max_fee_per_gas
        cost_matic = Decimal(cost_wei) / Decimal(10**18)
        cost_usd = cost_matic * self.matic_price_usd
        
        return GasEstimate(
            gas_limit=gas_limit,
            gas_prices=gas_prices,
            estimated_cost_matic=cost_matic,
            estimated_cost_usd=cost_usd,
            timestamp=time.time(),
        )
    
    def _estimate_gas_limit(self, transaction: dict) -> int:
        """估算 gas limit"""
        try:
            estimated = self.w3.eth.estimate_gas(transaction)
            # 添加 20% 缓冲
            return int(estimated * 1.2)
        except Exception as e:
            # 使用默认值
            return 200000
    
    def _get_gas_prices(self) -> dict[GasSpeed, GasPrice]:
        """获取各速度级别的 gas 价格"""
        
        # 获取当前 base fee
        latest_block = self.w3.eth.get_block('latest')
        base_fee = latest_block.get('baseFeePerGas', 30_000_000_000)  # 30 Gwei default
        
        # 根据速度级别设置 priority fee
        priority_fees = {
            GasSpeed.SLOW: int(base_fee * 0.1),
            GasSpeed.STANDARD: int(base_fee * 0.2),
            GasSpeed.FAST: int(base_fee * 0.5),
            GasSpeed.INSTANT: int(base_fee * 1.0),
        }
        
        gas_prices = {}
        for speed, priority_fee in priority_fees.items():
            max_fee = base_fee + priority_fee
            
            gas_prices[speed] = GasPrice(
                max_fee_per_gas=max_fee,
                max_priority_fee_per_gas=priority_fee,
                base_fee=base_fee,
                speed=speed,
                estimated_time_seconds=self._estimate_confirmation_time(speed),
                confidence=0.9,
            )
        
        return gas_prices
    
    def _estimate_confirmation_time(self, speed: GasSpeed) -> int:
        """估算确认时间（秒）"""
        time_map = {
            GasSpeed.SLOW: 600,
            GasSpeed.STANDARD: 180,
            GasSpeed.FAST: 60,
            GasSpeed.INSTANT: 30,
        }
        return time_map[speed]
```


---

### 2.2 Transaction Batcher（交易批处理器）

**文件**: `asterion_core/blockchain/transaction_batcher.py`

#### 2.2.1 数据结构

```python
@dataclass
class PendingTransaction:
    """待处理交易"""
    tx_id: str
    to_address: str
    data: bytes
    value: int
    priority: int  # 1-10
    created_at: float
    expires_at: Optional[float]

@dataclass
class BatchedTransaction:
    """批处理交易"""
    batch_id: str
    transactions: list[PendingTransaction]
    total_gas_saved: int
    created_at: float
```

#### 2.2.2 核心实现

```python
class TransactionBatcher:
    """交易批处理器"""
    
    def __init__(self, max_batch_size: int = 10, batch_window_seconds: int = 30):
        self.max_batch_size = max_batch_size
        self.batch_window_seconds = batch_window_seconds
        self.pending_txs: list[PendingTransaction] = []
    
    def add_transaction(self, tx: PendingTransaction) -> None:
        """添加待处理交易"""
        self.pending_txs.append(tx)
    
    def should_flush(self) -> bool:
        """判断是否应该刷新批次"""
        if not self.pending_txs:
            return False
        
        # 达到最大批次大小
        if len(self.pending_txs) >= self.max_batch_size:
            return True
        
        # 最早的交易超过窗口时间
        oldest = min(tx.created_at for tx in self.pending_txs)
        if time.time() - oldest > self.batch_window_seconds:
            return True
        
        # 有高优先级交易
        if any(tx.priority >= 8 for tx in self.pending_txs):
            return True
        
        return False
    
    def flush(self) -> Optional[BatchedTransaction]:
        """刷新批次"""
        if not self.pending_txs:
            return None
        
        # 按优先级排序
        sorted_txs = sorted(self.pending_txs, key=lambda x: x.priority, reverse=True)
        
        batch = BatchedTransaction(
            batch_id=str(uuid.uuid4()),
            transactions=sorted_txs,
            total_gas_saved=self._estimate_gas_savings(sorted_txs),
            created_at=time.time(),
        )
        
        self.pending_txs.clear()
        return batch
    
    def _estimate_gas_savings(self, txs: list[PendingTransaction]) -> int:
        """估算批处理节省的 gas"""
        # 单独发送的总 gas
        individual_gas = len(txs) * 21000  # base tx cost
        
        # 批处理的 gas（假设节省 30%）
        batched_gas = int(individual_gas * 0.7)
        
        return individual_gas - batched_gas
```

---

### 2.3 Nonce Manager（Nonce 管理器）

**文件**: `asterion_core/blockchain/nonce_manager.py`

#### 2.3.1 数据结构

```python
@dataclass
class NonceState:
    """Nonce 状态"""
    address: str
    current_nonce: int
    pending_nonces: set[int]
    last_updated: float
```

#### 2.3.2 核心实现

```python
class NonceManager:
    """Nonce 管理器"""
    
    def __init__(self, w3: Web3):
        self.w3 = w3
        self.nonce_states: dict[str, NonceState] = {}
        self.lock = threading.Lock()
    
    def get_next_nonce(self, address: str) -> int:
        """获取下一个可用 nonce"""
        with self.lock:
            if address not in self.nonce_states:
                self._initialize_nonce_state(address)
            
            state = self.nonce_states[address]
            
            # 找到第一个未使用的 nonce
            nonce = state.current_nonce
            while nonce in state.pending_nonces:
                nonce += 1
            
            state.pending_nonces.add(nonce)
            return nonce
    
    def mark_nonce_confirmed(self, address: str, nonce: int) -> None:
        """标记 nonce 已确认"""
        with self.lock:
            if address in self.nonce_states:
                state = self.nonce_states[address]
                state.pending_nonces.discard(nonce)
                
                # 更新 current_nonce
                if nonce >= state.current_nonce:
                    state.current_nonce = nonce + 1
    
    def mark_nonce_failed(self, address: str, nonce: int) -> None:
        """标记 nonce 失败（可重用）"""
        with self.lock:
            if address in self.nonce_states:
                state = self.nonce_states[address]
                state.pending_nonces.discard(nonce)
    
    def _initialize_nonce_state(self, address: str) -> None:
        """初始化 nonce 状态"""
        current_nonce = self.w3.eth.get_transaction_count(address, 'pending')
        
        self.nonce_states[address] = NonceState(
            address=address,
            current_nonce=current_nonce,
            pending_nonces=set(),
            last_updated=time.time(),
        )
```


---

### 2.4 Transaction Monitor（交易监控器）

**文件**: `asterion_core/blockchain/transaction_monitor.py`

#### 2.4.1 数据结构

```python
class TransactionStatus(Enum):
    """交易状态"""
    PENDING = "pending"
    CONFIRMED = "confirmed"
    FAILED = "failed"
    REPLACED = "replaced"
    TIMEOUT = "timeout"

@dataclass
class TransactionRecord:
    """交易记录"""
    tx_id: str
    tx_hash: str
    from_address: str
    to_address: str
    nonce: int
    gas_price: GasPrice
    gas_limit: int
    value: int
    status: TransactionStatus
    submitted_at: float
    confirmed_at: Optional[float]
    block_number: Optional[int]
    gas_used: Optional[int]
    actual_cost_matic: Optional[Decimal]
```

#### 2.4.2 核心实现

```python
class TransactionMonitor:
    """交易监控器"""
    
    def __init__(
        self,
        w3: Web3,
        nonce_manager: NonceManager,
        timeout_seconds: int = 300
    ):
        self.w3 = w3
        self.nonce_manager = nonce_manager
        self.timeout_seconds = timeout_seconds
        self.pending_txs: dict[str, TransactionRecord] = {}
    
    def submit_transaction(
        self,
        tx: dict,
        private_key: str,
        gas_price: GasPrice
    ) -> TransactionRecord:
        """提交交易"""
        
        # 获取 nonce
        nonce = self.nonce_manager.get_next_nonce(tx['from'])
        
        # 构建交易
        tx_data = {
            **tx,
            'nonce': nonce,
            'maxFeePerGas': gas_price.max_fee_per_gas,
            'maxPriorityFeePerGas': gas_price.max_priority_fee_per_gas,
            'chainId': 137,  # Polygon mainnet
        }
        
        # 签名
        signed_tx = self.w3.eth.account.sign_transaction(tx_data, private_key)
        
        # 发送
        tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        
        # 记录
        record = TransactionRecord(
            tx_id=str(uuid.uuid4()),
            tx_hash=tx_hash.hex(),
            from_address=tx['from'],
            to_address=tx['to'],
            nonce=nonce,
            gas_price=gas_price,
            gas_limit=tx['gas'],
            value=tx.get('value', 0),
            status=TransactionStatus.PENDING,
            submitted_at=time.time(),
            confirmed_at=None,
            block_number=None,
            gas_used=None,
            actual_cost_matic=None,
        )
        
        self.pending_txs[tx_hash.hex()] = record
        return record
    
    def check_transaction(self, tx_hash: str) -> TransactionRecord:
        """检查交易状态"""
        
        record = self.pending_txs.get(tx_hash)
        if not record:
            raise ValueError(f"Transaction {tx_hash} not found")
        
        try:
            receipt = self.w3.eth.get_transaction_receipt(tx_hash)
            
            # 交易已确认
            gas_used = receipt['gasUsed']
            actual_cost_wei = gas_used * receipt['effectiveGasPrice']
            actual_cost_matic = Decimal(actual_cost_wei) / Decimal(10**18)
            
            record.status = TransactionStatus.CONFIRMED if receipt['status'] == 1 else TransactionStatus.FAILED
            record.confirmed_at = time.time()
            record.block_number = receipt['blockNumber']
            record.gas_used = gas_used
            record.actual_cost_matic = actual_cost_matic
            
            # 更新 nonce 状态
            self.nonce_manager.mark_nonce_confirmed(record.from_address, record.nonce)
            
        except Exception:
            # 交易仍在 pending
            if time.time() - record.submitted_at > self.timeout_seconds:
                record.status = TransactionStatus.TIMEOUT
                self.nonce_manager.mark_nonce_failed(record.from_address, record.nonce)
        
        return record
    
    def speed_up_transaction(
        self,
        tx_hash: str,
        new_gas_price: GasPrice,
        private_key: str
    ) -> TransactionRecord:
        """加速交易（替换）"""
        
        old_record = self.pending_txs.get(tx_hash)
        if not old_record:
            raise ValueError(f"Transaction {tx_hash} not found")
        
        # 构建替换交易（相同 nonce，更高 gas）
        tx = {
            'from': old_record.from_address,
            'to': old_record.to_address,
            'value': old_record.value,
            'gas': old_record.gas_limit,
            'nonce': old_record.nonce,
            'maxFeePerGas': new_gas_price.max_fee_per_gas,
            'maxPriorityFeePerGas': new_gas_price.max_priority_fee_per_gas,
            'chainId': 137,
        }
        
        signed_tx = self.w3.eth.account.sign_transaction(tx, private_key)
        new_tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        
        # 标记旧交易为已替换
        old_record.status = TransactionStatus.REPLACED
        
        # 创建新记录
        new_record = TransactionRecord(
            tx_id=str(uuid.uuid4()),
            tx_hash=new_tx_hash.hex(),
            from_address=old_record.from_address,
            to_address=old_record.to_address,
            nonce=old_record.nonce,
            gas_price=new_gas_price,
            gas_limit=old_record.gas_limit,
            value=old_record.value,
            status=TransactionStatus.PENDING,
            submitted_at=time.time(),
            confirmed_at=None,
            block_number=None,
            gas_used=None,
            actual_cost_matic=None,
        )
        
        self.pending_txs[new_tx_hash.hex()] = new_record
        return new_record
```


---

## 3. 数据库设计

### 3.1 交易记录表

```sql
CREATE TABLE blockchain_transactions (
    tx_id TEXT PRIMARY KEY,
    tx_hash TEXT NOT NULL,
    from_address TEXT NOT NULL,
    to_address TEXT NOT NULL,
    nonce INTEGER NOT NULL,
    
    -- Gas 信息
    max_fee_per_gas BIGINT NOT NULL,
    max_priority_fee_per_gas BIGINT NOT NULL,
    gas_limit INTEGER NOT NULL,
    gas_used INTEGER,
    
    -- 成本
    estimated_cost_matic DECIMAL,
    actual_cost_matic DECIMAL,
    
    -- 状态
    status TEXT NOT NULL,
    submitted_at TIMESTAMP NOT NULL,
    confirmed_at TIMESTAMP,
    block_number INTEGER,
    
    -- 关联
    replaced_by_tx_hash TEXT,
    replaces_tx_hash TEXT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_tx_hash (tx_hash),
    INDEX idx_status (status),
    INDEX idx_from_address (from_address)
);
```

### 3.2 Gas 价格历史表

```sql
CREATE TABLE gas_price_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TIMESTAMP NOT NULL,
    
    base_fee BIGINT NOT NULL,
    slow_max_fee BIGINT NOT NULL,
    standard_max_fee BIGINT NOT NULL,
    fast_max_fee BIGINT NOT NULL,
    instant_max_fee BIGINT NOT NULL,
    
    block_number INTEGER NOT NULL,
    
    INDEX idx_timestamp (timestamp)
);
```

### 3.3 批处理记录表

```sql
CREATE TABLE transaction_batches (
    batch_id TEXT PRIMARY KEY,
    transaction_count INTEGER NOT NULL,
    total_gas_saved INTEGER NOT NULL,
    
    created_at TIMESTAMP NOT NULL,
    executed_at TIMESTAMP,
    
    INDEX idx_created_at (created_at)
);
```

---

## 4. 使用示例

### 4.1 基本使用

```python
from web3 import Web3
from asterion_core.blockchain import GasManager, GasEstimator, NonceManager, TransactionMonitor

# 初始化
w3 = Web3(Web3.HTTPProvider('https://polygon-rpc.com'))
gas_estimator = GasEstimator(w3)
nonce_manager = NonceManager(w3)
tx_monitor = TransactionMonitor(w3, nonce_manager)

gas_manager = GasManager(
    w3=w3,
    gas_estimator=gas_estimator,
    nonce_manager=nonce_manager,
    tx_monitor=tx_monitor,
)

# 提交交易
tx = {
    'from': '0x123...',
    'to': '0xabc...',
    'value': 0,
    'data': '0x...',
}

record = gas_manager.submit_transaction(
    tx=tx,
    private_key='0x...',
    speed=GasSpeed.STANDARD,
)

print(f"Transaction submitted: {record.tx_hash}")
print(f"Estimated cost: {record.gas_price.max_fee_per_gas * record.gas_limit / 10**18} MATIC")
```

### 4.2 监控交易

```python
import time

# 等待确认
while True:
    record = tx_monitor.check_transaction(record.tx_hash)
    
    if record.status == TransactionStatus.CONFIRMED:
        print(f"Transaction confirmed in block {record.block_number}")
        print(f"Actual cost: {record.actual_cost_matic} MATIC")
        break
    elif record.status == TransactionStatus.FAILED:
        print("Transaction failed")
        break
    elif record.status == TransactionStatus.TIMEOUT:
        print("Transaction timeout, speeding up...")
        # 加速交易
        new_gas_price = gas_estimator._get_gas_prices()[GasSpeed.FAST]
        record = tx_monitor.speed_up_transaction(
            tx_hash=record.tx_hash,
            new_gas_price=new_gas_price,
            private_key='0x...',
        )
        print(f"Replacement transaction: {record.tx_hash}")
    
    time.sleep(5)
```

### 4.3 批处理交易

```python
from asterion_core.blockchain.transaction_batcher import TransactionBatcher, PendingTransaction

batcher = TransactionBatcher(max_batch_size=10, batch_window_seconds=30)

# 添加多个交易
for i in range(5):
    tx = PendingTransaction(
        tx_id=str(uuid.uuid4()),
        to_address='0xabc...',
        data=b'...',
        value=0,
        priority=5,
        created_at=time.time(),
        expires_at=None,
    )
    batcher.add_transaction(tx)

# 检查是否应该刷新
if batcher.should_flush():
    batch = batcher.flush()
    print(f"Batch {batch.batch_id} with {len(batch.transactions)} transactions")
    print(f"Gas saved: {batch.total_gas_saved}")
```


---

## 5. Gas Manager 主模块

**文件**: `asterion_core/blockchain/gas_manager.py`

### 5.1 核心实现

```python
class GasManager:
    """Gas 管理器主模块"""
    
    def __init__(
        self,
        w3: Web3,
        gas_estimator: GasEstimator,
        nonce_manager: NonceManager,
        tx_monitor: TransactionMonitor,
        enable_batching: bool = False,
    ):
        self.w3 = w3
        self.gas_estimator = gas_estimator
        self.nonce_manager = nonce_manager
        self.tx_monitor = tx_monitor
        self.enable_batching = enable_batching
        
        if enable_batching:
            self.batcher = TransactionBatcher()
    
    def submit_transaction(
        self,
        tx: dict,
        private_key: str,
        speed: GasSpeed = GasSpeed.STANDARD,
        auto_retry: bool = True,
    ) -> TransactionRecord:
        """提交交易"""
        
        # 1. 估算 gas
        estimate = self.gas_estimator.estimate_gas(tx, speed)
        gas_price = estimate.gas_prices[speed]
        
        # 2. 添加 gas limit
        tx['gas'] = estimate.gas_limit
        
        # 3. 提交交易
        record = self.tx_monitor.submit_transaction(tx, private_key, gas_price)
        
        # 4. 如果启用自动重试，启动监控
        if auto_retry:
            self._start_auto_retry(record, private_key)
        
        return record
    
    def _start_auto_retry(self, record: TransactionRecord, private_key: str) -> None:
        """启动自动重试监控"""
        
        def monitor_and_retry():
            max_retries = 3
            retry_count = 0
            
            while retry_count < max_retries:
                time.sleep(30)  # 每 30 秒检查一次
                
                updated_record = self.tx_monitor.check_transaction(record.tx_hash)
                
                if updated_record.status == TransactionStatus.CONFIRMED:
                    break
                elif updated_record.status == TransactionStatus.TIMEOUT:
                    # 加速交易
                    new_gas_price = self.gas_estimator._get_gas_prices()[GasSpeed.FAST]
                    record = self.tx_monitor.speed_up_transaction(
                        tx_hash=record.tx_hash,
                        new_gas_price=new_gas_price,
                        private_key=private_key,
                    )
                    retry_count += 1
        
        # 在后台线程运行
        threading.Thread(target=monitor_and_retry, daemon=True).start()
    
    def get_optimal_gas_price(self, urgency: str = "normal") -> GasPrice:
        """获取最优 gas 价格"""
        speed_map = {
            "low": GasSpeed.SLOW,
            "normal": GasSpeed.STANDARD,
            "high": GasSpeed.FAST,
            "urgent": GasSpeed.INSTANT,
        }
        
        speed = speed_map.get(urgency, GasSpeed.STANDARD)
        gas_prices = self.gas_estimator._get_gas_prices()
        return gas_prices[speed]
```

---

## 6. 性能优化

### 6.1 Gas 价格缓存

```python
class CachedGasEstimator(GasEstimator):
    """带缓存的 Gas 估算器"""
    
    def __init__(self, w3: Web3, cache_ttl_seconds: int = 10):
        super().__init__(w3)
        self.cache_ttl = cache_ttl_seconds
        self.cache: Optional[tuple[dict, float]] = None
    
    def _get_gas_prices(self) -> dict[GasSpeed, GasPrice]:
        """获取 gas 价格（带缓存）"""
        
        now = time.time()
        
        # 检查缓存
        if self.cache:
            prices, cached_at = self.cache
            if now - cached_at < self.cache_ttl:
                return prices
        
        # 获取新价格
        prices = super()._get_gas_prices()
        self.cache = (prices, now)
        
        return prices
```

### 6.2 并发交易处理

```python
class ConcurrentGasManager(GasManager):
    """支持并发的 Gas Manager"""
    
    def __init__(self, *args, max_workers: int = 5, **kwargs):
        super().__init__(*args, **kwargs)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
    
    def submit_transactions_batch(
        self,
        txs: list[dict],
        private_key: str,
        speed: GasSpeed = GasSpeed.STANDARD,
    ) -> list[TransactionRecord]:
        """并发提交多个交易"""
        
        futures = []
        for tx in txs:
            future = self.executor.submit(
                self.submit_transaction,
                tx=tx,
                private_key=private_key,
                speed=speed,
            )
            futures.append(future)
        
        records = [f.result() for f in futures]
        return records
```


---

## 7. 测试策略

### 7.1 单元测试

```python
import pytest
from unittest.mock import Mock, patch
from asterion_core.blockchain import GasEstimator, NonceManager

def test_gas_estimation():
    """测试 Gas 估算"""
    w3 = Mock()
    w3.eth.estimate_gas.return_value = 100000
    w3.eth.get_block.return_value = {'baseFeePerGas': 30_000_000_000}
    
    estimator = GasEstimator(w3)
    estimate = estimator.estimate_gas({'from': '0x123', 'to': '0xabc'}, GasSpeed.STANDARD)
    
    assert estimate.gas_limit == 120000  # 100k * 1.2
    assert GasSpeed.STANDARD in estimate.gas_prices

def test_nonce_management():
    """测试 Nonce 管理"""
    w3 = Mock()
    w3.eth.get_transaction_count.return_value = 5
    
    manager = NonceManager(w3)
    
    # 获取连续 nonce
    nonce1 = manager.get_next_nonce('0x123')
    nonce2 = manager.get_next_nonce('0x123')
    
    assert nonce1 == 5
    assert nonce2 == 6
    
    # 确认 nonce
    manager.mark_nonce_confirmed('0x123', 5)
    nonce3 = manager.get_next_nonce('0x123')
    assert nonce3 == 6

def test_transaction_batching():
    """测试交易批处理"""
    batcher = TransactionBatcher(max_batch_size=3, batch_window_seconds=10)
    
    # 添加交易
    for i in range(2):
        tx = PendingTransaction(
            tx_id=f"tx_{i}",
            to_address='0xabc',
            data=b'',
            value=0,
            priority=5,
            created_at=time.time(),
            expires_at=None,
        )
        batcher.add_transaction(tx)
    
    # 未达到批次大小
    assert not batcher.should_flush()
    
    # 添加第三个
    batcher.add_transaction(PendingTransaction(
        tx_id="tx_3",
        to_address='0xabc',
        data=b'',
        value=0,
        priority=5,
        created_at=time.time(),
        expires_at=None,
    ))
    
    # 达到批次大小
    assert batcher.should_flush()
```

### 7.2 集成测试

```python
@pytest.mark.integration
def test_full_transaction_flow():
    """测试完整交易流程"""
    # 使用测试网
    w3 = Web3(Web3.HTTPProvider('https://rpc-mumbai.maticvigil.com'))
    
    gas_estimator = GasEstimator(w3)
    nonce_manager = NonceManager(w3)
    tx_monitor = TransactionMonitor(w3, nonce_manager)
    
    gas_manager = GasManager(w3, gas_estimator, nonce_manager, tx_monitor)
    
    # 提交测试交易
    tx = {
        'from': TEST_ADDRESS,
        'to': TEST_ADDRESS,
        'value': 0,
    }
    
    record = gas_manager.submit_transaction(tx, TEST_PRIVATE_KEY, GasSpeed.FAST)
    
    assert record.status == TransactionStatus.PENDING
    assert record.tx_hash is not None
```

---

## 8. 监控和告警

### 8.1 关键指标

| 指标 | 目标值 | 告警阈值 |
|------|--------|----------|
| 交易确认时间 | < 2 分钟 | > 5 分钟 |
| 交易成功率 | > 99% | < 95% |
| Gas 估算误差 | < 10% | > 20% |
| Nonce 冲突率 | < 0.1% | > 1% |

### 8.2 成本监控

```python
class GasCostMonitor:
    """Gas 成本监控"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def get_daily_cost(self, date: str) -> Decimal:
        """获取每日成本"""
        query = """
        SELECT SUM(actual_cost_matic) as total_cost
        FROM blockchain_transactions
        WHERE DATE(confirmed_at) = ?
        AND status = 'confirmed'
        """
        # 执行查询
        return total_cost
    
    def get_cost_by_operation(self, time_window: str = "24h") -> dict:
        """按操作类型统计成本"""
        # 分析不同操作的成本分布
        return {
            "place_order": Decimal("0.05"),
            "cancel_order": Decimal("0.03"),
            "redeem": Decimal("0.02"),
        }
```


---

## 9. 故障处理

### 9.1 常见故障场景

#### 场景 1: Nonce 冲突

**原因**: 多个进程同时发送交易

**解决方案**:
```python
class RobustNonceManager(NonceManager):
    """健壮的 Nonce 管理器"""
    
    def get_next_nonce(self, address: str) -> int:
        """获取下一个 nonce（带重试）"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                with self.lock:
                    # 从链上重新获取最新 nonce
                    chain_nonce = self.w3.eth.get_transaction_count(address, 'pending')
                    
                    if address not in self.nonce_states:
                        self._initialize_nonce_state(address)
                    
                    state = self.nonce_states[address]
                    
                    # 使用链上 nonce 和本地 nonce 的最大值
                    state.current_nonce = max(state.current_nonce, chain_nonce)
                    
                    nonce = state.current_nonce
                    while nonce in state.pending_nonces:
                        nonce += 1
                    
                    state.pending_nonces.add(nonce)
                    return nonce
                    
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                time.sleep(1)
```

#### 场景 2: 交易卡住

**原因**: Gas 价格过低

**解决方案**:
```python
def auto_speed_up_stuck_transactions(self):
    """自动加速卡住的交易"""
    
    for tx_hash, record in list(self.pending_txs.items()):
        if record.status != TransactionStatus.PENDING:
            continue
        
        # 超过 5 分钟未确认
        if time.time() - record.submitted_at > 300:
            # 获取更高的 gas 价格
            current_prices = self.gas_estimator._get_gas_prices()
            new_price = current_prices[GasSpeed.FAST]
            
            # 确保新价格至少高 10%
            if new_price.max_fee_per_gas <= record.gas_price.max_fee_per_gas * 1.1:
                new_price.max_fee_per_gas = int(record.gas_price.max_fee_per_gas * 1.2)
            
            # 替换交易
            self.speed_up_transaction(tx_hash, new_price, private_key)
```

#### 场景 3: RPC 节点故障

**原因**: RPC 节点不可用或响应慢

**解决方案**:
```python
class MultiRPCGasManager(GasManager):
    """多 RPC 节点的 Gas Manager"""
    
    def __init__(self, rpc_urls: list[str], *args, **kwargs):
        self.rpc_urls = rpc_urls
        self.current_rpc_index = 0
        
        w3 = Web3(Web3.HTTPProvider(rpc_urls[0]))
        super().__init__(w3, *args, **kwargs)
    
    def _switch_rpc(self):
        """切换到下一个 RPC 节点"""
        self.current_rpc_index = (self.current_rpc_index + 1) % len(self.rpc_urls)
        new_url = self.rpc_urls[self.current_rpc_index]
        self.w3 = Web3(Web3.HTTPProvider(new_url))
        
    def submit_transaction(self, *args, **kwargs):
        """提交交易（带 RPC 故障转移）"""
        max_retries = len(self.rpc_urls)
        
        for attempt in range(max_retries):
            try:
                return super().submit_transaction(*args, **kwargs)
            except Exception as e:
                if attempt < max_retries - 1:
                    self._switch_rpc()
                else:
                    raise
```

### 9.2 恢复机制

```python
class TransactionRecovery:
    """交易恢复机制"""
    
    def recover_pending_transactions(self, db_path: str) -> list[TransactionRecord]:
        """恢复未完成的交易"""
        
        # 从数据库加载 pending 交易
        pending_txs = self._load_pending_from_db(db_path)
        
        recovered = []
        for tx in pending_txs:
            # 检查交易状态
            try:
                receipt = self.w3.eth.get_transaction_receipt(tx.tx_hash)
                # 更新状态
                tx.status = TransactionStatus.CONFIRMED if receipt['status'] == 1 else TransactionStatus.FAILED
                recovered.append(tx)
            except:
                # 交易仍在 pending 或已丢失
                if time.time() - tx.submitted_at > 3600:  # 1 小时
                    tx.status = TransactionStatus.TIMEOUT
                    recovered.append(tx)
        
        return recovered
```


---

## 10. 与其他模块集成

### 10.1 与 CLOB Order Router 集成

```python
class OrderExecutionWithGas:
    """订单执行与 Gas 管理集成"""
    
    def __init__(self, gas_manager: GasManager, order_router: OrderRouter):
        self.gas_manager = gas_manager
        self.order_router = order_router
    
    def execute_order(
        self,
        order: dict,
        private_key: str,
        urgency: str = "normal"
    ) -> TransactionRecord:
        """执行订单（考虑 Gas 成本）"""
        
        # 1. 获取最优 gas 价格
        gas_price = self.gas_manager.get_optimal_gas_price(urgency)
        
        # 2. 估算总成本（交易费用 + gas）
        estimate = self.gas_manager.gas_estimator.estimate_gas(order, GasSpeed.STANDARD)
        total_cost = estimate.estimated_cost_usd
        
        # 3. 检查成本是否合理
        if total_cost > Decimal("1.0"):  # 超过 $1
            raise ValueError(f"Gas cost too high: ${total_cost}")
        
        # 4. 提交交易
        return self.gas_manager.submit_transaction(
            tx=order,
            private_key=private_key,
            speed=GasSpeed.STANDARD,
        )
```

### 10.2 与 Risk Management 集成

```python
class GasRiskControl:
    """Gas 风控"""
    
    def __init__(self, gas_manager: GasManager, daily_limit_usd: Decimal = Decimal("50")):
        self.gas_manager = gas_manager
        self.daily_limit_usd = daily_limit_usd
    
    def check_gas_budget(self) -> bool:
        """检查 Gas 预算"""
        today = datetime.now().strftime("%Y-%m-%d")
        daily_cost = self._get_daily_cost(today)
        
        if daily_cost >= self.daily_limit_usd:
            return False
        
        return True
    
    def approve_transaction(self, estimated_cost_usd: Decimal) -> bool:
        """批准交易"""
        if not self.check_gas_budget():
            return False
        
        # 检查单笔交易成本
        if estimated_cost_usd > Decimal("5.0"):
            return False
        
        return True
```

---

## 11. 配置管理

### 11.1 配置文件

```yaml
# config/gas_manager.yaml
gas_manager:
  rpc:
    urls:
      - "https://polygon-rpc.com"
      - "https://rpc-mainnet.matic.network"
      - "https://polygon-mainnet.infura.io/v3/${INFURA_KEY}"
    timeout_seconds: 30
  
  gas_estimation:
    cache_ttl_seconds: 10
    buffer_multiplier: 1.2
    matic_price_usd: 0.5
  
  transaction:
    timeout_seconds: 300
    auto_retry: true
    max_retries: 3
  
  batching:
    enabled: false
    max_batch_size: 10
    batch_window_seconds: 30
  
  cost_control:
    daily_limit_usd: 50
    single_tx_limit_usd: 5
    alert_threshold_usd: 40
```

### 11.2 配置加载

```python
import yaml

class GasManagerConfig:
    """Gas Manager 配置"""
    
    @classmethod
    def from_yaml(cls, config_path: str) -> dict:
        """从 YAML 加载配置"""
        with open(config_path) as f:
            config = yaml.safe_load(f)
        
        return config['gas_manager']
```


---

## 12. 最佳实践

### 12.1 Gas 优化建议

1. **批处理优先** - 尽可能合并多个操作到一个交易
2. **时机选择** - 避开网络高峰期（UTC 14:00-18:00）
3. **价格监控** - 实时监控 gas 价格，选择低谷期执行
4. **缓存利用** - 缓存 gas 价格和 nonce 状态减少 RPC 调用

### 12.2 安全建议

1. **私钥管理** - 使用 KMS 或 Vault 管理私钥，不要硬编码
2. **Nonce 锁** - 使用分布式锁防止 nonce 冲突
3. **交易验证** - 提交前验证交易参数
4. **限额控制** - 设置每日 gas 预算上限

### 12.3 监控建议

1. **实时监控** - 监控所有 pending 交易状态
2. **成本追踪** - 每日统计 gas 成本
3. **异常告警** - 交易失败或超时立即告警
4. **性能分析** - 定期分析 gas 使用效率

---

## 13. 故障排查指南

### 13.1 常见问题

**Q: 交易一直 pending 不确认？**

A: 检查步骤：
1. 查看 gas 价格是否过低
2. 检查 nonce 是否正确
3. 查看网络是否拥堵
4. 尝试加速交易

**Q: Nonce too low 错误？**

A: 原因：
- 本地 nonce 状态过期
- 有交易已被替换

解决：重新同步 nonce 状态

**Q: Gas 成本突然增加？**

A: 检查：
1. 网络是否拥堵
2. 交易复杂度是否增加
3. 是否有异常重试

---

## 14. 性能基准

### 14.1 目标指标

| 操作 | 目标延迟 | Gas 成本 |
|------|---------|---------|
| 提交交易 | < 500ms | 0.001-0.01 MATIC |
| 获取 nonce | < 100ms | 0 |
| 估算 gas | < 200ms | 0 |
| 检查状态 | < 300ms | 0 |

### 14.2 压力测试

```python
def benchmark_gas_manager():
    """Gas Manager 性能测试"""
    gas_manager = create_gas_manager()
    
    # 测试并发提交
    start = time.time()
    records = gas_manager.submit_transactions_batch(
        txs=[create_test_tx() for _ in range(100)],
        private_key=TEST_KEY,
    )
    duration = time.time() - start
    
    print(f"Submitted 100 transactions in {duration:.2f}s")
    print(f"Throughput: {100/duration:.2f} tx/s")
```


---

## 15. 路线图

### Phase 1: MVP（当前）
- ✅ 基础 Gas 估算
- ✅ Nonce 管理
- ✅ 交易监控
- ⏳ 自动重试机制
- ⏳ 成本追踪

### Phase 2: 优化（Q2 2026）
- ⏳ 交易批处理
- ⏳ Gas 价格预测模型
- ⏳ 多 RPC 故障转移
- ⏳ 高级成本优化

### Phase 3: 智能化（Q3 2026）
- ⏳ 机器学习 Gas 预测
- ⏳ 自适应重试策略
- ⏳ 跨链 Gas 管理

---

## 16. 附录

### 16.1 术语表

| 术语 | 定义 |
|------|------|
| Gas | 以太坊/Polygon 上执行交易的计算单位 |
| Gwei | Gas 价格单位，1 Gwei = 10^9 Wei |
| Base Fee | EIP-1559 引入的基础费用 |
| Priority Fee | 给矿工的小费 |
| Nonce | 账户发送交易的序号 |
| Gas Limit | 交易允许消耗的最大 gas |
| Gas Used | 交易实际消耗的 gas |

### 16.2 参考资料

- [Polygon Gas Station](https://gasstation-mainnet.matic.network/)
- [EIP-1559 Specification](https://eips.ethereum.org/EIPS/eip-1559)
- [Web3.py Documentation](https://web3py.readthedocs.io/)

### 16.3 FAQ

**Q: 为什么使用 EIP-1559 而不是 legacy gas？**

A: EIP-1559 提供更好的费用预测和用户体验，Polygon 已全面支持。

**Q: 如何处理 gas 价格剧烈波动？**

A: 使用缓存和平滑算法，避免频繁调整。关键交易使用更高的 gas 价格确保确认。

**Q: 批处理能节省多少 gas？**

A: 理论上可节省 20-40%，取决于交易类型和合约实现。

---

## 17. 总结

Gas Manager 是 Asterion 区块链交易的核心基础设施，确保交易高效、可靠、成本可控。

**核心价值**:
1. **成本优化** - 通过智能 gas 估算和批处理降低成本
2. **可靠性** - Nonce 管理和自动重试确保交易成功
3. **可观测性** - 完整的交易追踪和成本监控
4. **容错性** - 多 RPC 故障转移和恢复机制

**设计亮点**:
1. **EIP-1559 支持** - 充分利用 Polygon 的 EIP-1559 特性
2. **并发安全** - 线程安全的 nonce 管理
3. **自动化** - 自动重试和加速机制
4. **可扩展** - 支持批处理和多 RPC

**下一步**:
1. 实现基础 Gas Manager
2. 集成到订单执行流程
3. 收集真实数据优化参数
4. 开发高级优化策略

---

## 18. P2-12 简化总结

### 18.1 MVP 简化策略

✅ **MVP 只保留核心功能**
- approve（批准 USDC）
- split（USDC → YES+NO）
- merge（YES+NO → USDC）
- redeem（结算后赎回）
- tx monitor（交易监控）

✅ **移除 batcher（后置到 Phase 2）**
- MVP 阶段交易量不大，不需要批处理
- 简化架构，降低复杂度
- Phase 2 再根据实际需求添加

**简化后的架构**:
```
┌─────────────────────────────────────────────────────────┐
│              Gas Manager (MVP Simplified)                │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────┐  │
│  │     Gas      │    │  Transaction │    │  Nonce   │  │
│  │  Estimator   │    │   Monitor    │    │ Manager  │  │
│  └──────────────┘    └──────────────┘    └──────────┘  │
│         │                    │                   │       │
│         └────────────────────┴───────────────────┘       │
│                              │                            │
│                    ┌─────────▼─────────┐                 │
│                    │   Web3 Provider   │                 │
│                    │   (Polygon RPC)   │                 │
│                    └───────────────────┘                 │
│                                                           │
└─────────────────────────────────────────────────────────┘
```

**简化后的实现**:
```python
class GasManagerMVP:
    """Gas Manager MVP 版本（简化）"""

    def __init__(
        self,
        web3: Web3,
        signer_service: SignerService,
    ):
        self.web3 = web3
        self.signer = signer_service
        self.gas_estimator = GasEstimator(web3)
        self.tx_monitor = TransactionMonitor(web3)
        self.nonce_manager = NonceManager(web3, signer.get_address())

    async def approve_usdc(
        self,
        spender: str,
        amount: Decimal
    ) -> str:
        """批准 USDC（简化版）"""

        # 1. 构造交易
        tx = self._build_approve_tx(spender, amount)

        # 2. 估算 gas
        gas_estimate = await self.gas_estimator.estimate_gas(tx)
        tx['gas'] = gas_estimate.gas_limit
        tx['maxFeePerGas'] = gas_estimate.gas_prices[GasSpeed.STANDARD].max_fee_per_gas
        tx['maxPriorityFeePerGas'] = gas_estimate.gas_prices[GasSpeed.STANDARD].max_priority_fee_per_gas

        # 3. 获取 nonce
        tx['nonce'] = await self.nonce_manager.get_next_nonce()

        # 4. 签名
        signed_tx = await self.signer.sign_transaction(tx)

        # 5. 提交
        tx_hash = self.web3.eth.send_raw_transaction(signed_tx.signature)

        # 6. 监控
        asyncio.create_task(self.tx_monitor.monitor(tx_hash))

        return tx_hash

    async def split(
        self,
        market_id: str,
        amount: Decimal
    ) -> str:
        """Split USDC into YES+NO（简化版）"""
        # 类似实现
        pass

    async def merge(
        self,
        market_id: str,
        amount: Decimal
    ) -> str:
        """Merge YES+NO into USDC（简化版）"""
        # 类似实现
        pass

    async def redeem(
        self,
        market_id: str,
        winning_outcome: str
    ) -> str:
        """Redeem winning tokens（简化版）"""
        # 类似实现
        pass
```

✅ **更新 MATIC → POL**
- Polygon 已将 MATIC 升级为 POL
- 更新所有文档和代码引用

**更新内容**:
```python
# 旧版本
gas_token = "MATIC"
estimated_cost_matic = gas_limit * gas_price / 1e18

# 新版本
gas_token = "POL"
estimated_cost_pol = gas_limit * gas_price / 1e18

@dataclass
class GasEstimate:
    """Gas 估算结果（更新版）"""
    gas_limit: int
    gas_prices: dict[GasSpeed, GasPrice]
    estimated_cost_pol: Decimal  # 改为 POL
    estimated_cost_usd: Decimal
```

✅ **Nonce Manager 改为单 signer 进程职责**
- MVP 阶段只有一个 signer 进程
- Nonce 管理简化为单进程模式
- 不需要分布式 nonce 协调

**简化后的 Nonce Manager**:
```python
class NonceManagerMVP:
    """Nonce Manager MVP 版本（单进程）"""

    def __init__(self, web3: Web3, address: str):
        self.web3 = web3
        self.address = address
        self.current_nonce = None
        self.lock = asyncio.Lock()

    async def get_next_nonce(self) -> int:
        """获取下一个 nonce（单进程版本）"""

        async with self.lock:
            # 1. 如果是第一次，从链上获取
            if self.current_nonce is None:
                self.current_nonce = self.web3.eth.get_transaction_count(
                    self.address, 'pending'
                )

            # 2. 返回当前 nonce 并递增
            nonce = self.current_nonce
            self.current_nonce += 1

            return nonce

    async def reset_nonce(self):
        """重置 nonce（从链上重新获取）"""
        async with self.lock:
            self.current_nonce = self.web3.eth.get_transaction_count(
                self.address, 'pending'
            )
```

### 18.2 移除的功能（Phase 2 再添加）

❌ **Transaction Batcher**
- 批处理逻辑
- 批次优化
- 批次调度

❌ **高级 Gas 优化**
- 动态 gas 价格调整
- Gas 价格预测模型
- 历史数据分析

❌ **分布式 Nonce 管理**
- 多进程 nonce 协调
- Nonce 锁机制
- Nonce 冲突解决

### 18.3 MVP 架构对比

**原设计（复杂）**:
- Gas Estimator
- Transaction Batcher ❌
- Nonce Manager（分布式）❌
- Transaction Monitor
- Cost Tracker
- RPC Manager

**MVP 设计（简化）**:
- Gas Estimator（基础版）
- Nonce Manager（单进程）✅
- Transaction Monitor（基础版）
- 核心操作：approve/split/merge/redeem ✅

### 18.4 简化效果

**代码量减少**: 约 40%
**复杂度降低**: 移除批处理和分布式协调
**开发时间**: 缩短 50%
**维护成本**: 降低 60%

### 18.5 Phase 2 升级路径

当交易量增加时，可以逐步添加：
1. Transaction Batcher（批处理）
2. 高级 Gas 优化
3. 分布式 Nonce 管理
4. 更复杂的重试策略

---

**文档版本**: v2.0
**创建日期**: 2026-03-07
**最后更新**: 2026-03-08 (P2-12 简化)
**作者**: Jay Zhu

