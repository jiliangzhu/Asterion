# P0 Closeout Checklist

**版本**: v1.0  
**更新日期**: 2026-03-09  
**目标**: 作为 `P0-15` 的关闭审查清单，明确哪些项已经完成、哪些项仍然阻塞 `P0` 关闭。

---

## 1. P0 关闭标准

`P0` 只有在以下五类事项全部满足时才能关闭：

1. 文档治理完成
2. AlphaDesk Wave A 基础模块完成迁移或明确阻塞
3. shared contracts、包骨架、migrations 已落地
4. 测试框架已建立并可稳定执行
5. duckdb 运行级验证完成

---

## 2. 当前状态

### 2.1 文档治理

- `DONE` 根目录只保留 `README.md`
- `DONE` 文档全部归档到 `docs/`
- `DONE` 已有 [Documentation_Index.md](../../00-overview/Documentation_Index.md)
- `DONE` 已有 [P0_Implementation_Plan.md](../phase-plans/P0_Implementation_Plan.md)

### 2.2 AlphaDesk Wave A

- `DONE` `determinism`
- `DONE` `write_guard_audit`
- `DONE` `write_queue`
- `DONE` `os_queue`
- `DONE` `bronze`
- `DONE` `clients/data_api`
- `DONE` `clients/gamma`
- `DONE` `database`
- `DONE` `db_migrate`
- `DONE` `writerd`

### 2.3 工程骨架

- `DONE` `asterion_core/*`
- `DONE` `domains/*`
- `DONE` `sql/migrations/*`
- `DONE` shared contracts

### 2.4 首版 migrations

- `DONE` `0001_core_meta.sql`
- `DONE` `0002_market_and_capability.sql`
- `DONE` `0003_orders_inventory.sql`
- `DONE` `0004_weather_specs_and_forecasts.sql`
- `DONE` `0005_uma_watcher.sql`

### 2.5 测试框架

- `DONE` `pyproject.toml`
- `DONE` `tests/README.md`
- `DONE` `duckdb` 已写入正式项目依赖
- `DONE` `unittest` 入口
- `DONE` contracts tests
- `DONE` storage smoke tests
- `DONE` ingest / clients smoke tests
- `DONE` migration shape tests
- `DONE` migration apply tests
- `DONE` database / writerd duckdb 运行级测试

---

## 3. 当前阻塞项

当前无阻塞项。

说明：

- `pyproject.toml` 已声明 `duckdb>=1.1`
- 已使用仓库内 `.venv` 完成依赖安装，规避系统 Python 的 PEP 668 限制
- `python3 -m unittest discover -s tests -v` 在 `.venv` 中已无 duckdb skip
- [AlphaDesk_Migration_Ledger.md](../migration-ledger/AlphaDesk_Migration_Ledger.md) 中 `database / db_migrate / writerd` 已改为 `ported`

---

## 4. P0 关闭验证命令

```bash
source .venv/bin/activate
python3 -m unittest discover -s tests -v
```

当前已确认：

- migration apply tests 通过
- reader / writer guard tests 通过
- writerd UPSERT / UPDATE tests 通过

---

## 5. P0 关闭结论

`P0` 退出条件已经满足，可以关闭。

关闭依据：

- 文档治理完成
- AlphaDesk Wave A 模块全部落到 `ported`
- shared contracts、包骨架、migrations 已落地
- 基础测试框架已建立并固定到 `.venv + unittest`
- duckdb 运行级验证已完成

---

## 6. P0 关闭后的下一阶段入口

只有当本清单中的阻塞项全部清空，才进入：

- `P1` Weather watch-only 主链路
- Rule2Spec -> ResolutionSpec
- forecast cache / forecast run persistence
- UMA watcher replay / watermark
