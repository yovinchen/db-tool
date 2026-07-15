# SQL Server Bounded Catalog Evidence

Task ID: IF-T66-SQLSERVER

Result: COMPILE_PASS / EXTERNAL_BLOCKED

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

The repository's existing `scripts/integration-sqlserver-test.sh` exercises
the public SQL lifecycle and therefore the bounded schema/table commands when
a SQL Server endpoint is supplied. A live run was not claimed here: the local
macOS arm64 Docker environment has no supported SQL Server product image. This
remains the explicit IF-T52 external-product blocker rather than being reported
as a false LIVE_PASS.
