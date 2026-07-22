# SQL Server Bounded Catalog Evidence

Task ID: IF-T66-SQLSERVER / IF-T74-SQLSERVER

Result: COMPILE_PASS / PRODUCT_LIFECYCLE_LIVE_PASS

Run at (UTC): 2026-07-15T22:09:53Z

## Implemented contract

- The adapter independently advertises `sql.list_schemas_bounded` and
  `sql.list_tables_bounded`; the coarse `sql=true` capability does not imply
  either method.
- `ListLimiter` computes N+1 and converts it to SQL Server's signed `TOP`
  integer before acquiring the shared client or issuing a catalog query.
  Zero, `usize::MAX`, and values outside that integer range fail closed.
- Schema listing executes `SELECT TOP (N+1) ... ORDER BY name`.
- Table listing validates the schema identifier and executes
  `SELECT TOP (N+1) ... ORDER BY TABLE_NAME` against
  `INFORMATION_SCHEMA.TABLES`.
- Catalog cells are type-checked. A missing or non-text schema, table name, or
  table type is a serialization error rather than a silently skipped item.
- The already-shared CLI `sql schemas` / `sql tables` path negotiates these
  method operations and copies adapter `truncated` exactly; no legacy list
  fallback remains.

## Verification

```text
cargo test -p adapter-sqlserver --lib
cargo clippy -p adapter-sqlserver --all-targets -- -D warnings
```

Result: 4/4 adapter tests PASS; Clippy PASS. Tests cover explicit operation
declaration, N+1 conversion, zero/overflow rejection, identifier safety,
connection config construction, and value decoding.

The original slice was service-free because the local macOS arm64 Docker
environment has no supported SQL Server product image. A later hosted x86_64
run completed the public SQL CRUD/types/catalog/guard/cleanup lifecycle against
SQL Server 2022 CU26; see [`sqlserver.md`](sqlserver.md). The exact N/N+1 and
byte boundary assertions in this file remain adapter-level evidence and are not
retroactively relabeled as network product tests.

## IF-T74 scalar-byte extension（2026-07-16）

`sql.list_schemas_budgeted` 与 `sql.list_tables_budgeted` 已作为独立 exact
operations 广告。adapter 在建连/查询前验证 `ReadBudget`，用 `TOP (N+1)` 约束服务端，
再在 retention 前计费完整 schema String 或 TableInfo，最后校验完整 BoundedList envelope。

当前 adapter 8/8 与 all-target Clippy PASS，包含 item N/N+1、byte N/N-1 和
zero/overflow。macOS arm64 当前仍没有可运行的 SQL Server 产品容器；精确 byte
envelope 仍由 adapter 证据证明，后续 x86_64 产品目录生命周期已 LIVE_PASS。
