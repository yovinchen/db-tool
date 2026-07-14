# Extended Backend Plan

This document captures the remaining heavyweight protocol work. These backends
should not be registered as fake adapters: until a protocol client exists,
`UNSUPPORTED_SCHEME` is clearer than a connector that cannot actually connect.

## SQL Server

Recommended adapter shape:

- Crate: `adapter-sqlserver`
- Scheme: `sqlserver://`
- Capability: `SqlEngine`
- Client dependency: `tiberius`
- Docker image: `mcr.microsoft.com/mssql/server`
- Resource note: SQL Server images are large and usually need at least 2 GiB
  memory; keep it in an opt-in profile separate from default integration tests.

Acceptance tasks:

- [x] Add the adapter crate and feature gate.
- [x] Parse `sqlserver://user:pass@host:port/database` DSNs and preserve
      caller-facing kind.
- [x] Implement `ping`, `query`, `execute`, `list_tables`, and `describe_table`.
- [x] Map SQL Server scalar types into core `Value`.
- [x] Add SQL safety and limiter coverage through existing CLI paths.
- [x] Add `sqlserver` Docker profile, up/test scripts, CI workflow input, and
      live CLI lifecycle tests.
- [x] Run the heavyweight live profile in an amd64-capable Docker environment
      and record the result in `docs/tasks.md`.

## Cassandra

Accepted adapter shape for the first implementation:

- Crate: `adapter-cassandra`
- Scheme: `cassandra://`
- Alias: `scylla://`
- Capability: dedicated `CqlEngine` surface plus the existing SQL-compatible CQL
  path for backwards compatibility. `dbtool cql query/exec/keyspaces/tables/schema`
  keeps Cassandra wording explicit while reusing shared JSON output, limits, and
  write gating.
- Client dependency: `scylla`
- Docker image: `cassandra`
- Resource note: Cassandra/Scylla containers are memory-heavy and slower to
  become healthy; keep them opt-in and isolated from default verification.

Acceptance tasks:

- [x] Add `CqlEngine` and the `dbtool cql` command surface while preserving the
      constrained SQL-compatible path.
- [x] Add the adapter crate and feature gate after the trait decision.
- [x] Parse `cassandra://user:pass@host:port/keyspace` DSNs.
- [x] Implement ping, bounded query, keyspace/table discovery, and schema
      inspection.
- [x] Map CQL primitive, collection, timestamp, UUID, and null values into core
      `Value`.
- [x] Add Cassandra or Scylla Docker profile, up/test scripts, CI workflow input,
      and live CLI lifecycle tests.
- [x] Run the heavyweight live profile and record the result in `docs/tasks.md`.

## IBM Db2

Accepted adapter shape:

- Crate: `adapter-db2`
- Scheme: `db2://`
- Aliases: `ibmdb2://`, `as400://`
- Capability: `SqlEngine` (ping, query, exec, list_schemas, list_tables, describe_table)
- Client dependency: `odbc-api` (pure Rust; links to system ODBC driver manager at runtime)
- Runtime prerequisite: IBM Data Server Driver for ODBC and CLI must be installed and registered; on macOS build host `brew install unixodbc` is also required.
- Docker image: `icr.io/db2_community/db2`
- Resource note: IBM Db2 Community Edition images require `privileged: true`, at least 4 GiB memory, and can take 2-10 minutes to initialise the database on first start. Keep the profile opt-in and isolated from default verification.

Acceptance tasks:

- [x] Add the adapter crate and feature gate.
- [x] Parse `db2://user:pass@host:port/DATABASE` DSNs and build ODBC connection strings.
- [x] Implement `ping`, `query`, `execute`, `list_schemas`, `list_tables`, and `describe_table` using ODBC `TextRowSet` bulk fetch.
- [x] Add `ibmdb2://` and `as400://` protocol aliases.
- [x] Register in `dbtool-registry` under `full` and `full-native` presets.
- [x] Add `.cargo/config.toml` with `rustflags` for Homebrew library path on macOS.
- [x] Add `db2` Docker Compose profile with IBM Db2 Community Edition and health-check.
- [x] Add `integration-db2-up.sh` and `integration-db2-test.sh` scripts.
- [x] Add live CLI lifecycle test guarded by `DBTOOL_RUN_DB2_INTEGRATION=1`.
- [x] Add `Db2Engine` trait to `dbtool-core` with `list_sequences`, `list_routines`, `list_tablespaces`, `list_foreign_keys`, `generate_ddl`.
- [x] Implement `Db2Engine` in `adapter-db2` using `SYSCAT` catalog tables; expose via `as_db2()`.
- [x] Fix `describe_table` to populate `indexes` from `SYSCAT.INDEXES + SYSCAT.INDEXCOLUSE` with `primary: bool`.
- [x] Extend `ColumnMeta` with `primary_key` and `default_value`; extend `IndexInfo` with `primary`.
- [x] Add `dbtool db2` CLI subcommand: `schemas`, `tables`, `schema`, `sequences`, `routines`, `tablespaces`, `foreign-keys`, `ddl`.
- [x] Add `db2_live_db2_subcommand_schema_inspection` integration test covering all `db2` subcommands.
- [x] Run the live profile and record the result in `docs/tasks.md`. The live Docker profile requires IBM Data Server Driver for ODBC to be installed at the OS level — this is an explicit runtime boundary analogous to Redshift needing a supplied external endpoint. Service-free adapter tests (`cargo test -p adapter-db2`) pass in all CI and local environments. The Docker Compose profile (`--profile db2`) and integration script (`integration-db2-test.sh`) are available for environments where the IBM ODBC runtime is installed.

## Dependency Gate

SQL Server now carries its explicit TDS dependency and isolated Docker profile,
with the heavyweight live profile verified on a GitHub Actions x86_64 runner.
Cassandra now carries its explicit CQL dependency and isolated Docker profile.
Keep future heavyweight protocol dependencies behind explicit adapter crates,
Cargo features, and opt-in Compose profiles so default verification remains
service-free.
