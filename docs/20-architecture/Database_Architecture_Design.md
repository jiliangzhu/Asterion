# 数据库架构设计

**版本**: v2.0
**创建日期**: 2026-03-08
**状态**: P0 - Critical (数据库角色边界重新定义)

---

## 1. 问题背景

### 1.1 原有设计的问题

**问题**: DuckDB 被当作在线状态库使用
- 多进程写入冲突
- 热路径性能问题
- 并发控制复杂

**影响**:
- 交易系统无法实时更新状态
- 订单管理和库存管理受阻
- 系统可靠性降低

### 1.2 修复目标

明确定义各数据库的职责边界：
- **DuckDB** - 只用于分析、回测、报表（冷路径）
- **SQLite (WAL)** - 用于单写进程的 queue/outbox（热路径辅助）
- **在线状态库** - 使用 SQLite (单 writer) 或 Postgres（热路径核心）

---

## 2. 数据库角色定义

### 2.1 DuckDB - 分析引擎（冷路径）

**职责**:
- 历史数据分析
- 回测
- 报表生成
- 数据科学探索

**特点**:
- 列式存储，OLAP 性能优秀
- 支持 Parquet 直接查询
- 适合大规模数据扫描

**使用场景**:
```python
# ✅ 正确使用
# 1. 历史数据分析
SELECT date, AVG(price) as avg_price
FROM historical_trades
WHERE date >= '2026-01-01'
GROUP BY date;

# 2. 回测
SELECT *
FROM backtest_results
WHERE strategy_id = 'weather_v1'
ORDER BY timestamp;

# 3. 报表
SELECT market_id, SUM(pnl) as total_pnl
FROM daily_pnl
GROUP BY market_id;
```

**禁止使用场景**:
```python
# ❌ 错误使用
# 1. 实时订单状态更新
UPDATE orders SET status = 'filled' WHERE order_id = '123';  # 禁止！

# 2. 实时库存更新
UPDATE inventory SET available = available - 100;  # 禁止！

# 3. 多进程并发写入
# 多个进程同时写入 DuckDB  # 禁止！
```

**数据流**:
```
热路径数据 → SQLite/Postgres → 定期导出 → Parquet → DuckDB 分析
```

---

### 2.2 SQLite (WAL) - 队列和 Outbox（热路径辅助）

**职责**:
- Write Queue（单写者模式）
- Outbox Pattern（事件发布）
- 临时缓冲

**特点**:
- 轻量级
- 单写者模式（ALPHADESK_STRICT_SINGLE_WRITER=1）
- WAL 模式支持并发读

**使用场景**:
```python
# ✅ 正确使用
# 1. Write Queue
class WriteQueue:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")

    def enqueue(self, table: str, data: dict):
        """入队写入请求"""
        self.conn.execute(
            "INSERT INTO write_queue (table_name, data, created_at) VALUES (?, ?, ?)",
            (table, json.dumps(data), datetime.now())
        )
        self.conn.commit()

# 2. Outbox Pattern
class Outbox:
    def publish_event(self, event_type: str, payload: dict):
        """发布事件到 outbox"""
        self.conn.execute(
            "INSERT INTO outbox (event_type, payload, published) VALUES (?, ?, ?)",
            (event_type, json.dumps(payload), False)
        )
        self.conn.commit()
```

**数据流**:
```
Producer → Write Queue (SQLite) → Writerd → 主数据库
```

---

### 2.3 在线状态库 - SQLite 或 Postgres（热路径核心）

**职责**:
- 订单状态管理
- 库存管理
- 实时仓位追踪
- 对账记录

**选项 A: SQLite (单 writer)**

**适用场景**:
- 单机部署
- 单个交易进程
- 简单架构

**配置**:
```python
import sqlite3

conn = sqlite3.connect('asterion_state.db')
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA synchronous=NORMAL")
conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
```

**优点**:
- 零配置
- 低延迟
- 简单可靠

**缺点**:
- 单写者限制
- 不支持分布式

**选项 B: Postgres**

**适用场景**:
- 多进程部署
- 需要分布式
- 高可用要求

**配置**:
```python
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="asterion",
    user="asterion",
    password="***"
)
```

**优点**:
- 支持多写者
- 成熟的并发控制
- 丰富的生态

**缺点**:
- 需要额外部署
- 运维复杂度增加

**推荐**: MVP 阶段使用 SQLite，生产环境考虑 Postgres

---

## 3. 数据表分配

### 3.1 在线状态库（SQLite/Postgres）

**订单管理**:
- `orders` - 订单表
- `fills` - 成交表
- `order_state_transitions` - 订单状态转移表

**库存管理**:
- `inventory_positions` - 库存仓位表
- `inventory_ledger` - 库存账本表

**CTF 操作**:
- `ctf_operations` - CTF 操作表

**对账**:
- `reconciliation_results` - 对账结果表

**UMA 监控**:
- `uma_proposals` - UMA 提案表
- `settlement_verifications` - 结算验证表
- `dispute_decisions` - Dispute 决策表
- `proposal_state_transitions` - 提案状态转移表
- `block_watermarks` - 区块水位线表

**市场数据**:
- `markets` - 市场元数据
- `resolution_specs` - 结算规范表

### 3.2 Write Queue（SQLite）

```sql
CREATE TABLE write_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    operation TEXT NOT NULL,  -- 'insert', 'update', 'delete'
    data TEXT NOT NULL,        -- JSON
    created_at TIMESTAMP NOT NULL,
    processed BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMP
);

CREATE INDEX idx_write_queue_processed ON write_queue(processed);
```

### 3.3 Outbox（SQLite）

```sql
CREATE TABLE outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,     -- JSON
    published BOOLEAN DEFAULT FALSE,
    published_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_outbox_published ON outbox(published);
```

### 3.4 分析库（DuckDB）

**历史数据**:
- `historical_trades` - 历史成交
- `historical_orderbook` - 历史订单簿
- `historical_prices` - 历史价格

**回测**:
- `backtest_runs` - 回测运行记录
- `backtest_trades` - 回测交易记录
- `backtest_metrics` - 回测指标

**报表**:
- `daily_pnl` - 每日盈亏
- `position_snapshots` - 仓位快照
- `performance_metrics` - 性能指标

**数据导入**:
```python
import duckdb

# 从 Parquet 导入
conn = duckdb.connect('asterion_analytics.duckdb')
conn.execute("""
    CREATE TABLE historical_trades AS
    SELECT * FROM read_parquet('data/trades/*.parquet')
""")

# 从在线库导出
conn.execute("""
    COPY (SELECT * FROM sqlite_scan('asterion_state.db', 'orders'))
    TO 'data/orders.parquet' (FORMAT PARQUET)
""")
```

---

## 4. 数据流架构

### 4.1 热路径（实时交易）

```
┌─────────────────────────────────────────────────────────┐
│                    热路径数据流                          │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  Trading Logic                                            │
│       │                                                   │
│       ▼                                                   │
│  ┌─────────────────┐                                     │
│  │  在线状态库      │  (SQLite/Postgres)                 │
│  │  - orders       │                                     │
│  │  - inventory    │                                     │
│  │  - positions    │                                     │
│  └─────────────────┘                                     │
│       │                                                   │
│       │ (定期导出)                                        │
│       ▼                                                   │
│  ┌─────────────────┐                                     │
│  │   Parquet       │                                     │
│  │   Files         │                                     │
│  └─────────────────┘                                     │
│       │                                                   │
│       │ (加载)                                            │
│       ▼                                                   │
│  ┌─────────────────┐                                     │
│  │    DuckDB       │  (分析)                             │
│  │  - 回测         │                                     │
│  │  - 报表         │                                     │
│  └─────────────────┘                                     │
│                                                           │
└─────────────────────────────────────────────────────────┘
```

### 4.2 冷路径（分析回测）

```
┌─────────────────────────────────────────────────────────┐
│                    冷路径数据流                          │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  Dagster Jobs                                             │
│       │                                                   │
│       ▼                                                   │
│  ┌─────────────────┐                                     │
│  │   Parquet       │  (历史数据)                         │
│  │   Files         │                                     │
│  └─────────────────┘                                     │
│       │                                                   │
│       ▼                                                   │
│  ┌─────────────────┐                                     │
│  │    DuckDB       │                                     │
│  │  - 分析         │                                     │
│  │  - 回测         │                                     │
│  │  - 报表         │                                     │
│  └─────────────────┘                                     │
│       │                                                   │
│       ▼                                                   │
│  ┌─────────────────┐                                     │
│  │  Streamlit UI   │  (可视化)                           │
│  └─────────────────┘                                     │
│                                                           │
└─────────────────────────────────────────────────────────┘
```

---

## 5. 实现指南

### 5.1 在线状态库初始化

```python
import sqlite3
from pathlib import Path

class StateDatabase:
    """在线状态库"""

    def __init__(self, db_path: str = "asterion_state.db"):
        self.db_path = db_path
        self.conn = None

    def connect(self):
        """连接数据库"""
        self.conn = sqlite3.connect(self.db_path)

        # 配置 WAL 模式
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA cache_size=-64000")

        # 启用外键约束
        self.conn.execute("PRAGMA foreign_keys=ON")

    def init_schema(self):
        """初始化表结构"""
        # 创建所有表
        self._create_orders_tables()
        self._create_inventory_tables()
        self._create_ctf_tables()
        self._create_uma_tables()
        self._create_reconciliation_tables()

    def _create_orders_tables(self):
        """创建订单相关表"""
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id TEXT PRIMARY KEY,
                client_order_id TEXT UNIQUE NOT NULL,
                market_id TEXT NOT NULL,
                side TEXT NOT NULL,
                order_type TEXT NOT NULL,
                price DECIMAL NOT NULL,
                size DECIMAL NOT NULL,
                time_in_force TEXT NOT NULL,
                status TEXT NOT NULL,
                filled_size DECIMAL NOT NULL DEFAULT 0,
                remaining_size DECIMAL NOT NULL,
                avg_fill_price DECIMAL,
                created_at TIMESTAMP NOT NULL,
                submitted_at TIMESTAMP,
                updated_at TIMESTAMP NOT NULL,
                exchange_order_id TEXT,
                fee_paid DECIMAL NOT NULL DEFAULT 0,
                inventory_reserved BOOLEAN NOT NULL DEFAULT FALSE,
                inventory_updated BOOLEAN NOT NULL DEFAULT FALSE
            );

            CREATE INDEX IF NOT EXISTS idx_orders_market ON orders(market_id);
            CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
            CREATE INDEX IF NOT EXISTS idx_orders_created ON orders(created_at);

            -- 其他订单相关表...
        """)

    # ... 其他表创建方法
```

### 5.2 数据导出到 DuckDB

```python
import duckdb
import sqlite3
from datetime import datetime, timedelta

class DataExporter:
    """数据导出器"""

    def __init__(
        self,
        state_db_path: str,
        analytics_db_path: str,
        parquet_dir: str
    ):
        self.state_db = sqlite3.connect(state_db_path)
        self.analytics_db = duckdb.connect(analytics_db_path)
        self.parquet_dir = Path(parquet_dir)

    def export_daily(self):
        """每日导出"""
        yesterday = datetime.now() - timedelta(days=1)
        date_str = yesterday.strftime('%Y-%m-%d')

        # 1. 导出订单
        self._export_table(
            'orders',
            f"created_at >= '{date_str} 00:00:00' AND created_at < '{date_str} 23:59:59'",
            f'orders_{date_str}.parquet'
        )

        # 2. 导出成交
        self._export_table(
            'fills',
            f"timestamp >= '{date_str} 00:00:00' AND timestamp < '{date_str} 23:59:59'",
            f'fills_{date_str}.parquet'
        )

        # 3. 加载到 DuckDB
        self._load_to_duckdb()

    def _export_table(self, table: str, where: str, filename: str):
        """导出表到 Parquet"""
        query = f"SELECT * FROM {table} WHERE {where}"
        df = pd.read_sql_query(query, self.state_db)

        output_path = self.parquet_dir / filename
        df.to_parquet(output_path, compression='snappy')

    def _load_to_duckdb(self):
        """加载 Parquet 到 DuckDB"""
        self.analytics_db.execute(f"""
            CREATE TABLE IF NOT EXISTS historical_orders AS
            SELECT * FROM read_parquet('{self.parquet_dir}/orders_*.parquet')
        """)
```

---

## 6. 监控与维护

### 6.1 关键指标

**在线状态库**:
- 写入延迟（目标 < 10ms）
- 查询延迟（目标 < 50ms）
- 数据库大小
- WAL 文件大小

**Write Queue**:
- 队列长度
- 处理延迟
- 积压数量

**DuckDB**:
- 查询性能
- 存储大小
- Parquet 文件数量

### 6.2 维护任务

**每日**:
- 导出数据到 Parquet
- 清理已处理的 Write Queue
- 清理已发布的 Outbox

**每周**:
- VACUUM SQLite 数据库
- 压缩 Parquet 文件
- 检查数据一致性

**每月**:
- 归档历史数据
- 清理过期日志
- 性能优化

---

## 7. P0-5 修复总结

### 7.1 已修复的问题

✅ **明确 DuckDB 只用于分析、回测、报表**
- 不再用于在线状态存储
- 只处理冷路径数据

✅ **SQLite (WAL) 用于单写进程 queue/outbox**
- Write Queue 模式
- Outbox Pattern
- 单写者保证

✅ **在线状态用 SQLite (单 writer) 或 Postgres**
- MVP 使用 SQLite
- 生产环境可升级到 Postgres
- 明确的并发控制

✅ **更新所有设计文档的数据库使用说明**
- 明确各数据库职责
- 定义数据流
- 提供实现指南

### 7.2 架构改进

- **职责分离**: 热路径和冷路径使用不同数据库
- **性能优化**: 在线状态库专注于低延迟
- **可扩展性**: 支持从 SQLite 升级到 Postgres
- **数据流清晰**: 热路径 → Parquet → 冷路径

### 7.3 迁移路径

**Phase 1 (MVP)**:
- 在线状态库: SQLite (WAL)
- Write Queue: SQLite
- 分析库: DuckDB

**Phase 2 (生产)**:
- 在线状态库: Postgres
- Write Queue: SQLite
- 分析库: DuckDB

**Phase 3 (扩展)**:
- 在线状态库: Postgres (主从)
- Write Queue: Redis Streams
- 分析库: DuckDB + ClickHouse
