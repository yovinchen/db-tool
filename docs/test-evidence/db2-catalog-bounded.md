# Db2 Bounded Catalog Evidence

Task ID: IF-T66-DB2

Result: COMPILE_PASS / EXTERNAL_BLOCKED

Run at (UTC): 2026-07-15T22:16:02Z

## Implemented contract

- The adapter independently advertises six method-level operations:
  `sql.list_schemas_bounded`, `sql.list_tables_bounded`,
  `db2.list_sequences_bounded`, `db2.list_routines_bounded`,
  `db2.list_tablespaces_bounded`, and `db2.list_foreign_keys_bounded`.
  The coarse `sql=true` / `db2=true` flags and legacy unbounded methods do not
  imply these stronger contracts.
- Every method calculates and validates its N+1 probe before opening an ODBC
  connection. Zero, `usize::MAX`, and values outside Db2's signed
  `FETCH FIRST` integer range fail closed.
- Schema, table, sequence, routine, and tablespace queries order their stable
  identity and issue `FETCH FIRST N+1 ROWS ONLY` at the server.
- Foreign keys are bounded by N+1 **constraint identities** in a CTE before
  joining key columns. A multi-column foreign key is therefore either returned
  in full or omitted as the probe constraint; it is never split at an arbitrary
  joined row.
- Bounded catalog cells are type-checked. Missing, non-text, malformed numeric,
  unknown routine-type, or internally inconsistent foreign-key rows are
  serialization errors rather than silently skipped catalog entries.
- Every Db2 CLI list command validates `--limit` before DSN resolution,
  negotiates the exact bounded operation, invokes only the bounded method, and
  copies adapter completeness to JSON `meta.truncated`. There is no legacy list
  fallback.

## Verification

```text
cargo test -p adapter-db2 --lib
cargo test -p dbtool-cli --bin dbtool cmd::db2::tests
cargo clippy -p adapter-db2 --all-targets -- -D warnings
cargo clippy -p dbtool-cli --bin dbtool -- -D warnings
```

Result: adapter 8/8 PASS; CLI Db2 3/3 PASS; both Clippy checks PASS.

Tests cover explicit operation declaration, zero/overflow rejection, N+1
conversion, identifier safety, strict composite foreign-key grouping, CLI
fail-closed negotiation, and validation before connection setup.

The existing `scripts/integration-db2-test.sh` remains the live product runner.
No live result is claimed here: this macOS arm64 host does not have the IBM Data
Server Driver for ODBC and CLI or a supplied Db2 endpoint. A future live run
must provide both the registered IBM driver and `DBTOOL_IT_DB2_DSN`; until then,
the product remains the explicit IF-T52 external blocker rather than a false
`LIVE_PASS`.
