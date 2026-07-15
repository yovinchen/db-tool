# Prometheus Completeness Evidence

Task ID: DB-PROMETHEUS-001

Result: LIVE_PASS

Run at (UTC): 2026-07-15T17:16:55Z (explicit range revalidation)

Environment: Docker on macOS arm64; Rust 1.96.0; single Prometheus with remote-write receiver enabled and one-hour disposable retention

Product version: Prometheus v2.55.1

Command: `./scripts/integration-observability-test.sh`

Resource operations:

| Resource | Create/write | Read all fixture data | Metadata/admin | Guard/limit | Update/delete | Cleanup |
| --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_probe_*{sample="first",source="dbtool_integration"}` | remote write value 41.25 at exact runtime anchor minus 8 seconds PASS | metric name, columns, tag values, sample value, evaluation timestamp, original source timestamp, and explicit `start_ms=first`/`end_ms=second+1000` readback with `step=1s` PASS | ping, `caps.time_series=true`, measurement list contains dynamic metric PASS | write guard; invalid/ambiguous ranges rejected before connection; global limit 1 returned one total sample with data/meta truncated true PASS | not applicable to Prometheus append/query model | disposable TSDB volume owns metric retention; each run uses a unique metric PASS |
| `dbtool_it_probe_*{sample="second",source="dbtool_integration"}` | remote write value 84.5 at exact runtime anchor minus 4 seconds PASS | same complete field/tag/timestamp assertions through both relative and explicit ranges PASS | same measurement PASS | included in exact two-series readback and global limit accounting PASS | N/A | unique metric namespace prevents cross-run reads PASS |

The Compose profile explicitly enabled `/api/v1/write`; earlier harness code
without that flag could not substantiate remote-write support. Query limiting
uses one sample budget across all returned series rather than once per series.
The CLI accepts either `--last-minutes` (default 60) or both `--start-ms` and
`--end-ms`; mixing modes, omitting one endpoint, reversing bounds, zero limits,
and limits above 1,000,000 are rejected before a backend connection.

Cleanup: PASS by disposable Docker teardown; public series deletion is N/A

Commits: `7a6bbdd`, `932655d`, `b9dd9fd`, IF-T48 TS current campaign
