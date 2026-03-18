# Asterion 项目修复方案

> Analysis input only.
> Not implementation truth-source.
> Active implementation entry: `docs/10-implementation/versions/v2.0/phase-plans/V2_Implementation_Plan.md`

**状态**: historical remediation draft (`2026-03-13`)

**文档版本**: v1.0
**创建日期**: 2026-03-13
**基于**: 10_Claude_Asterion_Project_Assessment.md
**状态**: Draft

---

## 文档说明

本文档针对 Asterion 项目评估报告中识别的关键问题，提供具体的修复方案、代码示例和实施路径。

修复方案按优先级分为三个等级：
- **P0**: 阻塞核心功能，必须立即修复
- **P1**: 影响系统质量或安全，应在 1-2 周内修复
- **P2**: 技术债或优化项，可在 1-3 月内修复

---

## 目录

1. [P0 修复方案](#p0-修复方案)
   - 1.1 修复 Forecast Adapter 概率分布
   - 1.2 验证 P3/P4 Closeout Checklist
   - 1.3 补全 .env.example

2. [P1 修复方案](#p1-修复方案)
   - 2.1 实现 HTTP 重试和断路器
   - 2.2 添加 Streamlit UI 认证
   - 2.3 实现 Forecast Cache TTL
   - 2.4 修复文档漂移

3. [P2 修复方案](#p2-修复方案)
   - 3.1 拆分大型文件
   - 3.2 实现 Chain Tx Nonce 管理
   - 3.3 Write Queue 事务性改进
   - 3.4 WebSocket 集成

4. [实施路线图](#实施路线图)

---

## P0 修复方案

### 1.1 修复 Forecast Adapter 概率分布

**问题**: `domains/weather/forecast/adapters.py` 返回单点值 `{temperature: 1.0}`，导致 pricing engine 退化为 0/1 判断。

**影响**: 阻塞所有信号研究能力，fair value 计算无意义。

**修复方案**:

#### 方案 A: 基于历史预测误差的正态分布（推荐）

```python
# domains/weather/forecast/adapters.py

from dataclasses import dataclass
from decimal import Decimal
import math

@dataclass(frozen=True)
class ForecastDistributionParams:
    """预测分布参数"""
    mean: Decimal
    std_dev: Decimal  # 标准差
    source: str
    confidence: float

def build_normal_distribution(
    mean: Decimal,
    std_dev: Decimal,
    min_temp: int = -100,
    max_temp: int = 200,
) -> dict[int, float]:
    """
    基于正态分布构建温度概率分布

    Args:
        mean: 预测均值（如 65.0°F）
        std_dev: 标准差（如 3.0°F，基于历史误差）
        min_temp: 最小温度
        max_temp: 最大温度

    Returns:
        {temperature: probability} 字典，概率和为 1.0
    """
    distribution = {}
    total_prob = 0.0

    # 计算每个整数温度的概率密度
    for temp in range(min_temp, max_temp + 1):
        # 正态分布 PDF: (1 / (σ√(2π))) * e^(-((x-μ)²) / (2σ²))
        z = (float(temp) - float(mean)) / float(std_dev)
        prob = math.exp(-0.5 * z * z) / (float(std_dev) * math.sqrt(2 * math.pi))
        distribution[temp] = prob
        total_prob += prob

    # 归一化
    if total_prob > 0:
        distribution = {t: p / total_prob for t, p in distribution.items()}

    return distribution


class OpenMeteoAdapter:
    """Open-Meteo API 适配器（改进版）"""

    # 基于历史数据的预测误差标准差（单位：°F）
    # 这些值应该从历史回测中获得
    HISTORICAL_STD_DEV = {
        "1day": 3.0,   # 1天预测误差约 3°F
        "3day": 4.5,   # 3天预测误差约 4.5°F
        "7day": 6.0,   # 7天预测误差约 6°F
    }

    def fetch_forecast(
        self,
        lat: float,
        lon: float,
        target_date: str,
        metric: str,
    ) -> ForecastDistributionParams:
        """
        获取预测并返回分布参数

        Returns:
            ForecastDistributionParams 包含均值和标准差
        """
        # 调用 Open-Meteo API（保持原有逻辑）
        response = self._call_api(lat, lon, target_date, metric)

        # 提取预测值作为均值
        mean_temp = self._extract_temperature(response, metric)

        # 根据预测时长选择标准差
        days_ahead = self._calculate_days_ahead(target_date)
        if days_ahead <= 1:
            std_dev = Decimal(str(self.HISTORICAL_STD_DEV["1day"]))
        elif days_ahead <= 3:
            std_dev = Decimal(str(self.HISTORICAL_STD_DEV["3day"]))
        else:
            std_dev = Decimal(str(self.HISTORICAL_STD_DEV["7day"]))

        return ForecastDistributionParams(
            mean=mean_temp,
            std_dev=std_dev,
            source="open-meteo",
            confidence=0.85,  # 基于历史准确率
        )

    def build_distribution(
        self,
        params: ForecastDistributionParams,
    ) -> dict[int, float]:
        """构建完整的概率分布"""
        return build_normal_distribution(
            mean=params.mean,
            std_dev=params.std_dev,
        )
```

#### 方案 B: Ensemble 多模型加权（更准确但更复杂）

```python
# domains/weather/forecast/ensemble.py

@dataclass(frozen=True)
class EnsembleForecast:
    """多模型集成预测"""
    forecasts: list[ForecastDistributionParams]
    weights: list[float]  # 权重和为 1.0

def build_ensemble_distribution(
    ensemble: EnsembleForecast,
) -> dict[int, float]:
    """
    基于多个模型的加权平均构建分布

    Example:
        - Open-Meteo: mean=65, std=3, weight=0.4
        - NWS: mean=67, std=4, weight=0.3
        - Weather.com: mean=66, std=3.5, weight=0.3
    """
    combined = {}

    for forecast, weight in zip(ensemble.forecasts, ensemble.weights):
        dist = build_normal_distribution(forecast.mean, forecast.std_dev)
        for temp, prob in dist.items():
            combined[temp] = combined.get(temp, 0.0) + prob * weight

    return combined
```

**实施步骤**:

1. 收集历史预测误差数据（从 Open-Meteo/NWS 历史 API）
2. 计算不同预测时长的标准差
3. 修改 `OpenMeteoAdapter` 和 `NWSAdapter` 返回分布参数
4. 更新 `ForecastService` 调用新接口
5. 更新测试用例

**预估工作量**: 1-2 周

**依赖**: 需要访问历史天气预测数据

---

### 1.2 验证 P3/P4 Closeout Checklist

**问题**: `docs/10-implementation/versions/v1.0/checklists/P3_Closeout_Checklist.md` 和 `P4_Closeout_Checklist.md` 全部 `[ ]` 未勾选。

**影响**: 文档声称已关闭但验证未执行，项目状态不可信。

**修复方案**:

创建验证脚本，自动检查 closeout 条件：

```python
# scripts/verify_closeout.py
#!/usr/bin/env python3
"""
P3/P4 Closeout 自动验证脚本
"""
import sys
from pathlib import Path
from asterion_core.storage.database import connect_duckdb, DuckDBConfig
from asterion_core.monitoring.readiness_checker_v1 import (
    evaluate_p3_paper_execution_readiness,
    evaluate_p4_live_prereq_readiness,
)

def verify_p3_closeout(db_path: str) -> dict:
    """验证 P3 closeout 条件"""
    con = connect_duckdb(DuckDBConfig(db_path=db_path))

    checks = {
        "tests_passing": run_test_suite(),
        "paper_chain_complete": verify_paper_chain(con),
        "readiness_go": verify_p3_readiness(con),
        "ui_surfaces_exist": verify_ui_tables(con),
    }

    return checks

def verify_p4_closeout(db_path: str) -> dict:
    """验证 P4 closeout 条件"""
    con = connect_duckdb(DuckDBConfig(db_path=db_path))

    checks = {
        "p3_closed": verify_p3_closeout(db_path)["all_pass"],
        "live_prereq_jobs_exist": verify_live_prereq_jobs(con),
        "controlled_live_smoke_executed": verify_controlled_live(con),
        "readiness_go": verify_p4_readiness(con),
    }

    return checks

def generate_checklist_update(phase: str, checks: dict) -> str:
    """生成 checklist 更新内容"""
    lines = []
    for check_name, passed in checks.items():
        checkbox = "[x]" if passed else "[ ]"
        lines.append(f"{checkbox} {check_name}")
    return "\n".join(lines)

if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else "data/asterion.duckdb"

    print("=== P3 Closeout Verification ===")
    p3_checks = verify_p3_closeout(db_path)
    print(generate_checklist_update("P3", p3_checks))

    print("\n=== P4 Closeout Verification ===")
    p4_checks = verify_p4_closeout(db_path)
    print(generate_checklist_update("P4", p4_checks))
```

**实施步骤**:
1. 创建 `scripts/verify_closeout.py`
2. 运行验证脚本
3. 根据结果更新 checklist
4. 对未通过项制定修复计划

**预估工作量**: 2-3 天

---

### 1.3 补全 .env.example

**问题**: `.env.example` 只有 2 行，缺少关键配置。

**修复方案**:

```bash
# .env.example

# ============================================
# Agent Configuration
# ============================================
ALIBABA_API_KEY=replace-with-your-dashscope-key
QWEN_MODEL=qwen-max

# ============================================
# Database Configuration
# ============================================
ASTERION_DB_PATH=data/asterion.duckdb
ASTERION_DB_ROLE=reader  # reader or writer
ASTERION_STRICT_SINGLE_WRITER=1
ASTERION_WRITE_QUEUE=data/meta/write_queue.sqlite

# ============================================
# UI Configuration
# ============================================
ASTERION_UI_LITE_DB_PATH=data/ui/asterion_ui_lite.duckdb
ASTERION_UI_DB_REPLICA_PATH=data/ui/asterion_ui.duckdb
ASTERION_UI_REPLICA_COPY_MODE=auto  # auto, copy, or rsync

# ============================================
# RPC Configuration (Polygon)
# ============================================
ASTERION_POLYGON_RPC_URL_1=https://polygon-rpc.com
ASTERION_POLYGON_RPC_URL_2=https://rpc-mainnet.matic.network
ASTERION_POLYGON_RPC_URL_3=https://polygon-mainnet.infura.io/v3/YOUR_INFURA_KEY

# ============================================
# Controlled Live Smoke (DO NOT SET IN PRODUCTION)
# ============================================
# ASTERION_CONTROLLED_LIVE_SMOKE_PK_WALLET_WEATHER_1=0x...

# ============================================
# Optional: Dagster
# ============================================
# DAGSTER_HOME=~/.dagster

# ============================================
# Optional: Writerd
# ============================================
# ASTERION_WRITERD=1
# ASTERION_WRITERD_ALLOWED_TABLES=trading.*,runtime.*,weather.*
```

**预估工作量**: 1 小时

---

## P1 修复方案

### 2.1 实现 HTTP 重试和断路器

**问题**: Gamma API、OpenMeteo、NWS 调用无重试逻辑。

**修复方案**:

```python
# asterion_core/clients/http_retry.py

from functools import wraps
import time
from typing import Callable, Any

class CircuitBreaker:
    """简单的断路器实现"""
    def __init__(self, failure_threshold: int = 5, timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failures = 0
        self.last_failure_time = None
        self.state = "closed"  # closed, open, half_open

    def call(self, func: Callable, *args, **kwargs) -> Any:
        if self.state == "open":
            if time.time() - self.last_failure_time > self.timeout:
                self.state = "half_open"
            else:
                raise Exception("Circuit breaker is OPEN")

        try:
            result = func(*args, **kwargs)
            if self.state == "half_open":
                self.state = "closed"
                self.failures = 0
            return result
        except Exception as e:
            self.failures += 1
            self.last_failure_time = time.time()
            if self.failures >= self.failure_threshold:
                self.state = "open"
            raise

def retry_with_backoff(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (Exception,),
):
    """重试装饰器，带指数退避"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries - 1:
                        raise
                    time.sleep(delay)
                    delay *= backoff_factor
        return wrapper
    return decorator

# 使用示例
@retry_with_backoff(max_retries=3, initial_delay=1.0)
def fetch_gamma_markets(client, **kwargs):
    return client.get("/events", params=kwargs)
```

**实施步骤**:
1. 创建 `asterion_core/clients/http_retry.py`
2. 为 Gamma、OpenMeteo、NWS 客户端添加重试装饰器
3. 添加断路器到 RPC fallback pool
4. 更新测试

**预估工作量**: 3-5 天

---

### 2.2 添加 Streamlit UI 认证

**问题**: UI 无任何认证机制。

**修复方案**:

```python
# ui/auth.py

import streamlit as st
import hashlib
import os

def check_password() -> bool:
    """简单的密码认证"""

    def password_entered():
        """验证密码"""
        username = st.session_state["username"]
        password = st.session_state["password"]

        # 从环境变量读取凭证（生产环境应使用更安全的方式）
        expected_username = os.getenv("ASTERION_UI_USERNAME", "admin")
        expected_password_hash = os.getenv(
            "ASTERION_UI_PASSWORD_HASH",
            hashlib.sha256("changeme".encode()).hexdigest()
        )

        password_hash = hashlib.sha256(password.encode()).hexdigest()

        if username == expected_username and password_hash == expected_password_hash:
            st.session_state["authenticated"] = True
            del st.session_state["password"]  # 不保存密码
        else:
            st.session_state["authenticated"] = False

    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False

    if not st.session_state["authenticated"]:
        st.text_input("Username", key="username")
        st.text_input("Password", type="password", key="password")
        st.button("Login", on_click=password_entered)
        return False
    else:
        return True

# ui/app.py 中使用
if not check_password():
    st.stop()

# 正常页面内容
st.title("Asterion Ops Console")
```

**预估工作量**: 1-2 天

---

### 2.3 实现 Forecast Cache TTL

**问题**: `InMemoryForecastCache` 无过期机制。

**修复方案**:

```python
# domains/weather/forecast/cache.py

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

@dataclass
class CacheEntry:
    value: dict[int, float]
    cached_at: datetime
    ttl_seconds: int

class InMemoryForecastCache:
    def __init__(self, default_ttl_seconds: int = 3600):
        self._cache: dict[str, CacheEntry] = {}
        self._default_ttl = default_ttl_seconds
        self._max_size = 1000  # 最大缓存条目数

    def get(self, key: str) -> Optional[dict[int, float]]:
        entry = self._cache.get(key)
        if entry is None:
            return None

        # 检查是否过期
        age = (datetime.utcnow() - entry.cached_at).total_seconds()
        if age > entry.ttl_seconds:
            del self._cache[key]
            return None

        return entry.value

    def set(self, key: str, value: dict[int, float], ttl_seconds: Optional[int] = None):
        # 检查缓存大小，使用 LRU 策略
        if len(self._cache) >= self._max_size:
            oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k].cached_at)
            del self._cache[oldest_key]

        self._cache[key] = CacheEntry(
            value=value,
            cached_at=datetime.utcnow(),
            ttl_seconds=ttl_seconds or self._default_ttl,
        )

    def clear_expired(self):
        """清理过期条目"""
        now = datetime.utcnow()
        expired_keys = [
            key for key, entry in self._cache.items()
            if (now - entry.cached_at).total_seconds() > entry.ttl_seconds
        ]
        for key in expired_keys:
            del self._cache[key]
```

**预估工作量**: 半天

---

### 2.4 修复文档漂移

**问题**: README.md Phase 4 状态、daily_review_agent 提及等多处漂移。

**修复方案**:

1. **README.md 第 371-372 行**:
```markdown
# 修改前
- ⏳ chain tx scaffold
- ⏳ readiness / controlled rollout criteria

# 修改后
- ✅ chain tx scaffold
- ✅ readiness / controlled rollout criteria
```

2. **删除或标注 daily_review_agent 引用**:
```markdown
# AGENTS.md:106, README:269
# 修改前
- `agents/weather/daily_review_agent.py`

# 修改后
- `agents/weather/daily_review_agent.py` (planned, not yet implemented)
```

3. **创建文档一致性检查脚本**:
```python
# scripts/check_doc_consistency.py
def check_agent_references():
    """检查文档中提及的 agent 是否存在"""
    pass

def check_phase_status():
    """检查 README 和 closeout checklist 的状态一致性"""
    pass
```

**预估工作量**: 1 天

---

## P2 修复方案

### 3.1 拆分大型文件

**问题**: `dagster_asterion/handlers.py` (1,500+ 行) 和 `asterion_core/ui/ui_lite_db.py` (1,547 行) 过大。

**修复方案**:

```
# 拆分 handlers.py
dagster_asterion/
├── handlers/
│   ├── __init__.py
│   ├── weather_handlers.py      # 天气相关 jobs
│   ├── execution_handlers.py    # 执行相关 jobs
│   ├── agent_handlers.py        # Agent 相关 jobs
│   └── live_prereq_handlers.py  # Live prereq 相关 jobs
└── handlers.py (deprecated, 保留向后兼容)

# 拆分 ui_lite_db.py
asterion_core/ui/
├── lite_db/
│   ├── __init__.py
│   ├── builder.py        # 构建逻辑
│   ├── queries.py        # SQL 查询
│   └── validation.py     # 验证逻辑
└── ui_lite_db.py (deprecated)
```

**预估工作量**: 1 周

---

### 3.2 实现 Chain Tx Nonce 管理

**问题**: Nonce 通过 `eth.get_transaction_count()` 获取，无本地管理，并发交易可能冲突。

**修复方案**:

```python
# asterion_core/blockchain/nonce_manager.py

from dataclasses import dataclass
from threading import Lock

@dataclass
class NonceState:
    wallet_id: str
    chain_id: int
    current_nonce: int
    pending_count: int

class NonceManager:
    """本地 nonce 管理器"""

    def __init__(self):
        self._state: dict[tuple[str, int], NonceState] = {}
        self._lock = Lock()

    def get_next_nonce(self, wallet_id: str, chain_id: int, web3_client) -> int:
        """获取下一个可用 nonce"""
        with self._lock:
            key = (wallet_id, chain_id)

            if key not in self._state:
                # 首次获取，从链上读取
                on_chain_nonce = web3_client.eth.get_transaction_count(wallet_id)
                self._state[key] = NonceState(
                    wallet_id=wallet_id,
                    chain_id=chain_id,
                    current_nonce=on_chain_nonce,
                    pending_count=0,
                )

            state = self._state[key]
            next_nonce = state.current_nonce + state.pending_count
            state.pending_count += 1

            return next_nonce

    def confirm_nonce(self, wallet_id: str, chain_id: int, nonce: int):
        """确认 nonce 已上链"""
        with self._lock:
            key = (wallet_id, chain_id)
            if key in self._state:
                state = self._state[key]
                if nonce >= state.current_nonce:
                    state.current_nonce = nonce + 1
                    state.pending_count = max(0, state.pending_count - 1)

    def reset(self, wallet_id: str, chain_id: int):
        """重置 nonce（用于错误恢复）"""
        with self._lock:
            key = (wallet_id, chain_id)
            if key in self._state:
                del self._state[key]
```

**预估工作量**: 3-5 天

---

### 3.3 Write Queue 事务性改进

**问题**: SQLite queue → DuckDB 写入非原子，崩溃可能导致部分写入。

**修复方案**:

```python
# asterion_core/storage/write_queue.py

def process_task_with_checkpoint(task_id: str, con):
    """带 checkpoint 的任务处理"""

    # 1. 标记任务为 processing
    mark_task_processing(task_id)

    # 2. 执行写入
    try:
        execute_writes(task_id, con)

        # 3. 写入成功，标记为 completed
        mark_task_completed(task_id)

    except Exception as e:
        # 4. 写入失败，标记为 failed，保留 payload 用于重试
        mark_task_failed(task_id, error=str(e))
        raise

def resume_incomplete_tasks():
    """恢复未完成的任务"""
    incomplete = load_tasks_by_status(["processing", "failed"])
    for task in incomplete:
        if task.retry_count < MAX_RETRIES:
            retry_task(task)
        else:
            mark_task_dead_letter(task)
```

**预估工作量**: 1 周

---

### 3.4 WebSocket 集成

**问题**: `ws_subscribe.py` 和 `ws_agg_v3.py` 存在但未集成。

**修复方案**:

```python
# dagster_asterion/handlers/websocket_handler.py

def run_weather_websocket_quote_stream():
    """启动 WebSocket 实时 quote 流"""

    from asterion_core.ws import WebSocketSubscriber, QuoteAggregator

    # 1. 加载需要监控的 token_ids
    token_ids = load_active_weather_token_ids(con)

    # 2. 启动 WebSocket 订阅
    subscriber = WebSocketSubscriber(
        url="wss://ws-subscriptions-clob.polymarket.com/ws/market",
        token_ids=token_ids,
    )

    # 3. 启动聚合器
    aggregator = QuoteAggregator(
        window_seconds=60,
        output_handler=persist_quote_aggregates,
    )

    # 4. 连接订阅器和聚合器
    subscriber.on_message(aggregator.process_quote)

    # 5. 启动（阻塞）
    subscriber.run()
```

**实施步骤**:
1. 创建 WebSocket handler
2. 添加到 Dagster job map
3. 配置为后台长运行任务
4. 更新 pricing engine 使用实时 quote

**预估工作量**: 2-3 周

---

## 实施路线图

### 第 1 周：P0 关键修复

| 任务 | 负责人 | 输出 |
|------|--------|------|
| 修复 forecast adapter（方案 A） | 后端工程师 | 新的 `adapters.py` + 测试 |
| 验证 P3/P4 checklist | DevOps | 验证脚本 + 更新后的 checklist |
| 补全 .env.example | DevOps | 完整的 `.env.example` |

### 第 2-3 周：P1 质量改进

| 任务 | 负责人 | 输出 |
|------|--------|------|
| HTTP 重试和断路器 | 后端工程师 | `http_retry.py` + 集成 |
| UI 认证 | 前端工程师 | `auth.py` + 登录页面 |
| Forecast cache TTL | 后端工程师 | 改进的 `cache.py` |
| 文档漂移修复 | 技术写作 | 更新的文档 + 一致性脚本 |

### 第 4-6 周：P2 架构优化

| 任务 | 负责人 | 输出 |
|------|--------|------|
| 拆分大型文件 | 后端工程师 | 重构后的模块结构 |
| Nonce 管理器 | 区块链工程师 | `nonce_manager.py` |
| Write queue 改进 | 后端工程师 | 带 checkpoint 的 queue |

### 第 7-10 周：WebSocket 和回测

| 任务 | 负责人 | 输出 |
|------|--------|------|
| WebSocket 集成 | 后端工程师 | 实时 quote 流 |
| 历史回测框架 | 量化研究员 | 回测引擎 + 报告 |

---

## 验收标准

### P0 验收

- [ ] Forecast adapter 返回真实概率分布（至少 5 个温度点的非零概率）
- [ ] P3/P4 checklist 至少 80% 项目勾选
- [ ] `.env.example` 包含所有必需的环境变量

### P1 验收

- [ ] HTTP 调用失败后自动重试 3 次
- [ ] UI 需要用户名密码才能访问
- [ ] Forecast cache 1 小时后自动过期
- [ ] 文档中无明显的状态不一致

### P2 验收

- [ ] 单个文件不超过 800 行
- [ ] 并发交易不会出现 nonce 冲突
- [ ] Write queue 崩溃后可恢复
- [ ] WebSocket 实时接收 quote 数据

---

## 附录：快速参考

### 关键文件路径

```
domains/weather/forecast/adapters.py          # forecast 修复
docs/10-implementation/checklists/            # checklist 验证
.env.example                                  # 环境变量
asterion_core/clients/http_retry.py          # 重试逻辑（新建）
ui/auth.py                                    # UI 认证（新建）
domains/weather/forecast/cache.py            # cache TTL
dagster_asterion/handlers.py                 # 待拆分
asterion_core/blockchain/nonce_manager.py    # nonce 管理（新建）
asterion_core/storage/write_queue.py         # queue 改进
asterion_core/ws/                            # WebSocket 集成
```

### 测试命令

```bash
# 运行全部测试
python -m unittest discover -s tests -v

# 运行 forecast 相关测试
python -m unittest tests.test_forecast_service -v

# 验证 closeout
python scripts/verify_closeout.py

# 检查文档一致性
python scripts/check_doc_consistency.py
```

---

**文档结束**
