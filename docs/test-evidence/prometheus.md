# Prometheus Completeness Evidence

Task ID: DB-PROMETHEUS-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T20:46:45Z

Environment: Docker on macOS arm64; Rust 1.96.0; single Prometheus with remote-write receiver enabled and one-hour disposable retention

Product version: Prometheus v2.55.1

Command: `./scripts/integration-observability-test.sh`

Resource operations:

| Resource | Create/write | Read all fixture data | Metadata/admin | Guard/limit | Update/delete | Cleanup |
| --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_probe_*{sample="first",source="dbtool_integration"}` | remote write value 41.25 at exact runtime anchor minus 8 seconds PASS | metric name, columns, tag values, sample value, evaluation timestamp, and original source timestamp through `timestamp(metric)` PASS | ping, `caps.time_series=true`, measurement list contains dynamic metric PASS | write guard; invalid last-minutes 0 rejected before connection; global limit 1 returned one total sample with data/meta truncated true PASS | not applicable to Prometheus append/query model | disposable TSDB volume removed PASS |
| `dbtool_it_probe_*{sample="second",source="dbtool_integration"}` | remote write value 84.5 at exact runtime anchor minus 4 seconds PASS | same complete field/tag/timestamp assertions PASS | same measurement PASS | included in unbounded exact two-series readback and global limit accounting PASS | N/A | disposable TSDB volume removed PASS |

The Compose profile explicitly enabled `/api/v1/write`; earlier harness code
without that flag could not substantiate remote-write support. Query limiting
uses one sample budget across all returned series rather than once per series.

Cleanup: PASS by disposable Docker teardown; public series deletion is N/A

Commits: `7a6bbdd`, `932655d`, this commit
