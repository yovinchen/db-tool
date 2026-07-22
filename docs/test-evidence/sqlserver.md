# SQL Server Completeness Evidence

Task ID: DB-SQLSERVER-001

Result: LIVE_PASS

Run at (UTC): 2026-07-17T17:10:48Z–2026-07-17T17:12:50Z

Environment: GitHub Actions `ubuntu-24.04` x86_64 runner; Docker; Rust stable;
SQL Server Developer edition container exposed only to the disposable job

Product version: Microsoft SQL Server 2022 CU26, official image
`mcr.microsoft.com/mssql/server:2022-CU26-ubuntu-22.04`

Command: `./scripts/integration-sqlserver-test.sh`

Workflow: [run 29598991078](https://github.com/yovinchen/db-tool/actions/runs/29598991078),
[SQL Server job 87946289930](https://github.com/yovinchen/db-tool/actions/runs/29598991078/job/87946289930)

Implementation commits: `c6d0c40`, `bd41fc8`, `1fd88e6`

## Executed gates

| Gate | Result |
| --- | --- |
| `adapter-sqlserver` input/result/catalog/metadata budget and mapping suite | 9/9 PASS |
| `live_services::sqlserver_live_sql_lifecycle_and_typed_values` | 1/1 PASS |
| Container health | PASS; pinned CU26 image reached healthy state |
| Container cleanup | PASS; `dbtool-it-ci-sqlserver-sqlserver` stopped and removed |

Resource operations:

| Resource | Create | Insert/write | Read all fixture data | Update | Targeted delete | Metadata/types | Guard/limit | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `dbo.dbtool_it_sqlserver_users_6145_1784308346010` | table with integer primary key and `nvarchar(64)` PASS | rows `(1,alice)`, `(2,bob)` 2/2 PASS; rejected probe row 99 was not written | both fixture rows read back exactly after stable ID sort PASS | ID 1 became `alice-updated` PASS | ID 2 removed; ID 1 remained PASS | schema/table catalogs, columns, non-null PK and primary index PASS; int/float/bit/nvarchar/null decoding PASS | unguarded INSERT returned `WRITE_NOT_ALLOWED`; CREATE/DROP and unbounded DELETE required target-bound confirmation; three-row query with limit 2 returned two rows and `truncated=true` PASS | table absent after DROP; container removed PASS |

The service-free adapter gates also prove exact statement/input limits,
unsupported-parameter rejection before access, N+1/TOP catalog bounds, complete
query/catalog byte budgets, nested metadata accounting, identifier safety and
DSN mapping. They run immediately before the real product lifecycle in the
same requested gate, but are not misreported as product network operations.

## Boundaries

- SQL Server dynamic parameters remain explicitly unsupported; statements with
  non-empty parameter arrays fail instead of silently ignoring values.
- This is a single-node Developer edition compatibility run. It does not prove
  Always On, failover, production load, every authentication mode, or rolling
  upgrade behavior.
- The disposable connection trusts the container's self-signed certificate.
  Product TLS chain validation and enterprise PKI remain separate security
  exercises.
- Microsoft SQL Server Linux containers are x86_64-only; local Apple Silicon
  cannot substitute emulation for this recorded x86_64 product result.

Cleanup: PASS
