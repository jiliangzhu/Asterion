# Test Framework

## 当前测试入口

Asterion 在 `P0` 阶段统一使用标准库 `unittest` 作为最小测试框架。

推荐先创建项目内虚拟环境并安装依赖：

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -e .
```

运行方式：

```bash
python3 -m unittest tests.test_p4_closeout -v
python3 -m unittest discover -s tests -v
```

两类入口都应保持可用：

- `python3 -m unittest tests.test_xxx -v`
- `python3 -m unittest discover -s tests -v`

## 覆盖范围

- `test_ids_and_contracts.py`
  - shared contracts
  - ID / cache key 规则
- `test_storage_modules.py`
  - determinism
  - write guard audit
  - write queue
  - database / writerd smoke tests
- `test_ingest_and_clients.py`
  - bronze rolling writer
  - gamma client
  - data_api pagination
- `test_migrations.py`
  - migration 文件顺序
  - migration 关键表存在性
  - migration apply smoke test

## 环境假设

- `duckdb` 是 `P0/P1` 的正式项目依赖
- macOS/Homebrew Python 默认受 PEP 668 约束，不能直接向系统解释器写入依赖
- 本项目默认使用仓库内 `.venv` 做 duckdb 运行级验证
- 若环境未安装 `duckdb`，以下测试会跳过：
  - database guard 运行级测试
  - writerd 运行级测试
  - migration apply 测试

## P0 测试目标

- contract serialization tests
- enum freeze tests
- ID / key stability tests
- queue / writer smoke tests
- migration shape tests

## 当前结论

在当前仓库中，即使没有 `duckdb`，也应保证：

- 所有非 duckdb 测试通过
- duckdb 相关测试以 skip 明确暴露环境缺口，而不是静默失败
