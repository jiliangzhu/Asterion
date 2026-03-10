# Signer Service 详细设计

**模块**: `asterion_core/signer/signer_service.py`  
**版本**: v1.1  
**更新日期**: 2026-03-08  
**状态**: Interface Freeze Candidate

---

## 1. 模块概述

### 1.1 职责

Signer Service 的边界固定为四件事：

1. 管理 L1 signer（EOA / Proxy / Safe）
2. 派生和管理 L2 API credentials
3. 负责链上交易签名
4. 负责调用订单签名能力，但不自定义订单签名协议

### 1.2 非职责

以下事情不属于 Signer Service：

- 自行拼接 JSON 作为订单签名载荷
- 用 defunct-message encoder 或通用消息签名去模拟 Polymarket 订单签名
- 自行定义“兼容官方”的订单字段排序规则
- 让 UI 或 Agent 直接拿到私钥或原始签名能力

### 1.3 设计原则

- Polymarket 订单签名必须优先依赖官方 `py-clob-client`
- 如不直接调用官方库，也必须兼容官方 EIP-712 订单结构
- Asterion 可以包装“调用方式”，不能重写“签名协议”

---

## 2. 签名上下文模型

### 2.1 枚举定义

```python
from enum import Enum

class WalletType(Enum):
    EOA = "eoa"
    PROXY = "proxy"
    SAFE = "safe"

class SigningPurpose(Enum):
    L1_AUTH = "l1_auth"
    L2_AUTH = "l2_auth"
    ORDER = "order"
    TRANSACTION = "transaction"

class SignatureType(Enum):
    EOA = 0
    MAGIC = 1
    PROXY = 2
```

### 2.2 Signing Context

```python
from dataclasses import dataclass
from typing import Optional

@dataclass
class SigningContext:
    wallet_type: WalletType
    signing_purpose: SigningPurpose
    signature_type: SignatureType
    funder: str
    signer_address: str
    api_key_ref: Optional[str]
    chain_id: int
    token_id: str
    fee_rate_bps: int
```

说明：

- `wallet_type`: 区分 EOA / Proxy / Safe 的托管与签名流程
- `signing_purpose`: 明确本次是 L1、L2、order 还是 transaction 上下文
- `signature_type`: 对齐官方签名模式，例如 EOA / Magic / Proxy
- `funder`: Polymarket / CLOB 资金归属地址
- `signer_address`: 实际发起签名的地址
- `api_key_ref`: L2 API 凭证引用，而不是明文密钥
- `chain_id`: 用于链上交易和官方订单域隔离
- `token_id`: 订单签名必须知道具体 outcome token
- `fee_rate_bps`: 与 canonical order contract 对齐，进入审计日志和签名输入校验

---

## 3. 外部 RPC 契约

Signer Service 只暴露三个 RPC：

```python
sign_order(request)
sign_transaction(request)
derive_api_credentials(wallet_ref)
```

强制约束：

- 所有请求必须带 `request_id`
- 所有请求必须落审计日志
- 不暴露“任意消息签名”接口
- UI 不直接调用底层签名实现
- Agent 不直接访问私钥、助记词、原始 signer handle

### 3.1 Request/Response

```python
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

@dataclass
class SignerRequest:
    request_id: str
    requester: str
    timestamp: datetime
    context: SigningContext
    payload: dict[str, Any]

@dataclass
class SignerResponse:
    request_id: str
    status: str
    signature: Optional[str]
    signed_payload_ref: Optional[str]
    error: Optional[str]
    completed_at: datetime
```

---

## 4. 订单签名方案

### 4.1 正确方案

订单签名流程固定为：

1. OMS 生成 canonical order contract
2. CLOB adapter 把 `RouteAction` 映射为官方 `orderType` / `postOnly`
3. Signer Service 校验 `SigningContext`
4. Signer Service 调用官方 `py-clob-client` 或兼容官方 EIP-712 encoder
5. 返回官方格式要求的签名结果

### 4.2 禁止方案

以下方案在 Asterion 中明确禁止：

- defunct-message encoder
- 手工 JSON 序列化后再做通用消息签名
- 自己拼订单 JSON 再签名
- 只凭 `market_id / side / price / size` 构造“自定义消息”

### 4.3 Order Request

```python
from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime
from typing import Optional

@dataclass
class SignOrderRequest:
    request_id: str
    requester: str
    wallet_ref: str
    context: SigningContext
    market_id: str
    token_id: str
    outcome: str
    side: str
    price: Decimal
    size: Decimal
    route_action: str
    time_in_force: str
    expiration: Optional[datetime]
    signature_type: int
    funder: str
```

这里保留 `signature_type`、`funder`、`token_id`、`fee_rate_bps`，是为了和官方订单结构、Signer 审计以及 Polymarket 钱包模式对齐。

### 4.4 实现骨架

```python
class OrderSigningBackend:
    def sign_order(self, request: SignOrderRequest) -> str:
        """
        1. 校验 request.context 与 canonical order contract
        2. 调用官方 py-clob-client 或兼容官方 EIP-712 order encoder
        3. 返回官方要求的签名
        """
        ...
```

约束：

- `OrderSigningBackend` 可以替换，但必须等价于官方签名语义
- Asterion 不拥有订单签名协议定义权

---

## 5. L1 / L2 / Order Signing 分层

### 5.1 L1

L1 signer 用于：

- onchain approve
- CTF split / merge / redeem
- 其他 Polygon 交易

### 5.2 L2

L2 凭证用于：

- Polymarket CLOB API 认证
- 下单/撤单会话管理

`derive_api_credentials(wallet_ref)` 的职责是从受控 wallet 引用派生或刷新 API credentials，并返回引用信息，而不是把私钥暴露给调用方。

### 5.3 Order Signing

Order signing 是独立上下文：

- 它不是简单的“任意消息签名”
- 它不是通用消息签名
- 它也不是普通链上 transaction signing
- 它必须显式区分 `wallet_type`、`signing_purpose`、`signature_type`、`funder`

---

## 6. Transaction Signing

链上交易签名仍由 Signer Service 负责，但路径和订单签名分离。

```python
@dataclass
class SignTransactionRequest:
    request_id: str
    requester: str
    wallet_ref: str
    context: SigningContext
    tx: dict
```

处理流程：

1. 校验 `wallet_ref` 与 `SigningContext`
2. 填充 nonce / gas / chain_id
3. 使用受控 L1 signer 签名
4. 记录审计日志

---

## 7. Key Management 与部署模型

### 7.1 MVP

MVP 不再使用占位式“以后再接后端”的写法，而是明确采用：

- 独立 signer 进程
- 进程级环境隔离
- 最小暴露面 RPC
- 单向调用链路：UI/Agent -> OMS/CTF Manager -> Signer Service

MVP 目标不是“最强托管”，而是先把错误签名路径彻底封死。

### 7.2 Production

Production 目标升级为：

- KMS
- Vault
- HSM

要求：

- 私钥不落业务进程内存
- signer 身份与权限最小化
- 审计日志与访问控制可独立审查

---

## 8. 安全约束

### 8.1 强约束

- Agent 不能直接访问私钥
- UI 不能直接调用原始签名接口
- 业务服务不能绕过 Signer Service 直接签单
- 所有签名请求必须包含 `request_id`
- 所有签名请求必须记录审计日志

### 8.2 审计日志字段

```python
@dataclass
class SignatureAuditLog:
    request_id: str
    requester: str
    signature_type: str
    wallet_type: str
    signer_address: str
    funder: str
    api_key_ref: Optional[str]
    chain_id: int
    token_id: str
    fee_rate_bps: int
    payload_hash: str
    status: str
    error: Optional[str]
    created_at: datetime
```

说明：

- 审计日志记录 `payload_hash` 和关键上下文，不保存可重放的敏感明文
- `request_id` 是跨 OMS / Signer / 下单适配器的关联键

---

## 9. 数据库设计

```sql
CREATE TABLE signature_audit_logs (
    request_id TEXT PRIMARY KEY,
    requester TEXT NOT NULL,
    signature_type TEXT NOT NULL,
    wallet_type TEXT NOT NULL,
    signer_address TEXT NOT NULL,
    funder TEXT NOT NULL,
    api_key_ref TEXT,
    chain_id INTEGER NOT NULL,
    token_id TEXT,
    fee_rate_bps INTEGER,
    payload_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    error TEXT,
    created_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_signature_audit_logs_requester
    ON signature_audit_logs(requester);

CREATE INDEX idx_signature_audit_logs_created_at
    ON signature_audit_logs(created_at);
```

---

## 10. 冻结判断

本设计已经把最容易误导实现的边界固定下来：

- 订单签名不再允许自定义 JSON 协议
- L1 / L2 / order signing 已显式区分
- Signer Service 只暴露有限 RPC
- MVP 与 Production 的密钥托管路线已明确

后续实现可以替换具体 SDK、后端和部署方式，但不能突破以上接口边界。
