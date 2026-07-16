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
and limits above 1,000,000 are rejected before a backend connection. IF-T73
also requires exact `time_series.query_range_bounded` negotiation. `--max-series`,
global `--limit` (cumulative samples), and global `--max-bytes` independently
bound the complete portable result; Docker CLI revalidation passed a one-series
truncation and a one-byte `READ_BUDGET_EXCEEDED` failure without weakening the
independent 16 MiB HTTP transport ceiling.

## IF-T74 measurement scalar-byte envelope refresh

Run at (UTC): 2026-07-16T03:19:08Z

`TimeSeriesStore.list_measurements_budgeted` and the exact
`time_series.list_measurements_budgeted` operation now validate caller item and
byte budgets before HTTP dispatch. The adapter requests
`/api/v1/label/__name__/values?limit=N+1`, limits portable parsing and
construction to N+1 names, then charges every complete name plus the returned
`BoundedList` and probe. Unit coverage includes item N/N+1, exact byte N/N-1,
zero budgets and probe overflow.

Prometheus 2.55.1 Docker validation passed exact measurement item/byte
boundaries and operation declaration. The measurement fixture is written under
a unique per-run metric name. The independent raw HTTP JSON transport ceiling
remains 16 MiB because its protocol wrapper is not the caller-visible catalog
envelope.

Verification: `cargo test -p adapter-timeseries` 26/26 PASS; strict all-target
Clippy, rustfmt and diff check PASS; Prometheus live exact catalog 1/1 PASS.

## IF-T78 exact remote-write refresh

Run at (UTC): 2026-07-16T12:13:31Z

`prometheus_live_budgeted_write_rejects_before_send_and_cleans_series` passed
1/1 against Prometheus 2.55.1. N-1 complete-request bytes and N+1 point count
returned `INPUT_BUDGET_EXCEEDED`; the explicit range query contained zero
series afterward. The exact two-point batch produced two independently tagged
series and both were read back. Metric/label syntax, non-empty fields, reserved
`__name__`, compact JSON, protobuf, and Snappy body limits are checked before
TCP connect. All failures after request bytes may be written are
`OUTCOME_INDETERMINATE`.

The integration-only Compose profile now enables Prometheus's admin API so the
test can delete its unique series and clean tombstones. Final range polling was
empty. This is cleanup evidence, not a new portable `TimeSeriesStore` delete
capability; update/delete remain N/A for the append/query model.

IF-T78 fixture resource operations:

| Resource | Create | Insert/write | Read all fixture data | Update/overwrite | Targeted delete | Metadata/admin | Guard | Limit/timeout | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| accepted metric `dbtool_it_ts_input_<pid>_<epoch-ms>` with series `slot=one,two` | N/A; Prometheus series appear on append | exact remote write 2/2 points PASS | expected/actual 2/2 series; stable labels `slot=one`,`slot=two`; value `1.0` at the shared timestamp PASS | N/A for append-only portable model | N/A in public API | N/A in focused IF-T78; measurement catalog covered by the earlier IF-T74 run | complete metric/label/field/request validation before TCP connect PASS | exact 2-item/batch envelope and bounded range polling PASS | integration-only admin delete plus tombstone cleanup; final range 0/0 series PASS |
| rejected pre-write phase on the same metric/series | N/A | N-1 batch bytes and N+1 point count rejected; 0/2 points sent PASS | explicit range returned 0/0 series before accepted write PASS | N/A | N/A | N/A | `INPUT_BUDGET_EXCEEDED` before send PASS | N-1 bytes and max-items 1 both rejected PASS | no rejected series required cleanup |

Verification: adapter-timeseries 30/30 PASS; Prometheus Docker exact write
1/1 PASS; strict Clippy, rustfmt, Compose config, and diff check PASS.
Implementation commit: `3c9c2d4`.

Cleanup: PASS through integration admin delete-series/tombstone cleanup; public series deletion is N/A

Commits: `7a6bbdd`, `932655d`, `b9dd9fd`, `167f89f`, `73b8180`, `3c9c2d4`,
IF-T73/IF-T78
