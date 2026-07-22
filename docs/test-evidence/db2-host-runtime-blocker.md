# IBM Db2 Hosted Runtime Blocker Evidence

Task ID: DB-DB2-001

Result: BLOCKED_SUPPORTED_HOST_CLIENT_REQUIRED

Last verified: 2026-07-22 UTC

## What Passed

- The content-addressed IBM Db2 Community V12.1 product image reached healthy on
  GitHub Actions x86_64.
- unixODBC 2.3.12 and the repository's Db2 feature compiled.
- The full-product instance `libdb2o.so` was found, extracted, and registered.
- `file -L` proved an ELF 64-bit x86-64 driver.
- With the product library path applied, `ldd` reported no missing dependency.
- `odbcinst` successfully queried `IBM DB2 ODBC DRIVER`.
- Both failed live attempts ran before any fixture mutation, and every Docker
  cleanup step passed.
- `adapter-db2` service-free suites continue to prove DSN mapping, safe
  identifiers, exact mutation inputs, typed result envelopes, catalog/schema
  budgets, DDL reconstruction, and strict metadata parsing.

## Blocking Result

The final hosted run was
[29904077064](https://github.com/yovinchen/db-tool/actions/runs/29904077064),
job
[88871232409](https://github.com/yovinchen/db-tool/actions/runs/29904077064/job/88871232409),
source `18aa18d`.

Both product tests stopped at the first connection before creating resources:

```text
State: IM004
[unixODBC][Driver Manager] Driver's SQLAllocHandle on SQL_HANDLE_HENV failed
```

The same boundary persisted after copying the complete instance-scoped
`sqllib` tree and setting `DB2INSTANCE`, `INSTHOME`, library/message/config,
client data, diagnostic, and CLI driver installation paths. This proves that a
server-container file transplant is not equivalent to an IBM-supported client
installation.

Earlier runs isolated each prerequisite instead of hiding it:

| Run | Proven boundary |
| --- | --- |
| [29902020306](https://github.com/yovinchen/db-tool/actions/runs/29902020306) | Pinned product healthy; container-to-host archive handling isolated. |
| [29902384452](https://github.com/yovinchen/db-tool/actions/runs/29902384452) | Canonical V12.1 driver resolved; symlink target identity isolated. |
| [29903017363](https://github.com/yovinchen/db-tool/actions/runs/29903017363) | Dereferenced x86-64 ELF proved; private loader path isolated. |
| [29903252985](https://github.com/yovinchen/db-tool/actions/runs/29903252985) | Driver dependencies and unixODBC registration passed; `IM004` first reproduced. |
| [29903645577](https://github.com/yovinchen/db-tool/actions/runs/29903645577) | Complete instance client tree still returned `IM004`; cleanup passed. |
| [29904077064](https://github.com/yovinchen/db-tool/actions/runs/29904077064) | IBM-documented instance/client paths still returned `IM004`; zero mutations. |

## Why The Standalone Archive Is Not Substituted

The official Db2 12.1 Linux x64 ODBC/CLI archive is published by IBM, and the
downloaded archive was independently hashed as
`27cc46b5e7309bae9a13c1c3adc705f1c0d6916ed3e1ac162f2e95430262822d`.
It contains `clidriver/lib/libdb2.so`, not `libdb2o.so`. The included header
defines 64-bit `SQLLEN` only when built with `ODBC64`; the standalone library
previously produced corrupted catalog NULL indicators through unixODBC's
64-bit `SQLBindCol` contract. Falling back would turn an obvious blocker into
silent metadata corruption.

IBM's unixODBC documentation specifies `libdb2o.so` for the full-product
instance path and requires the client environment to be installed/configured:

- https://www.ibm.com/docs/en/db2/12.1.x?topic=managers-installing-unixodbc-driver-manager
- https://www.ibm.com/docs/en/db2/12.1.x?topic=unix-linux-environment-variable-settings
- https://www.ibm.com/docs/en/db2/12.1.x?topic=variables-system-environment

## Unblock Contract

Provision a self-hosted GitHub runner labeled `self-hosted`, `linux`, `x64`,
and `db2-client` with all of the following:

1. A supported IBM 64-bit Data Server Client/Runtime Client installation.
2. `IBM DB2 ODBC DRIVER` registered to its instance/client `libdb2o.so` with
   `DontDLClose=1`.
3. `db2profile` sourced by the runner service so `DB2INSTANCE` and
   `LD_LIBRARY_PATH` are present.
4. Docker access for the pinned disposable V12.1 server.

Then dispatch `run_live_db2=true`. Promote DB-DB2-001 only after both live tests
complete CRUD/catalog/guard/limit/cleanup and the evidence ledger is updated.

Cleanup: PASS
