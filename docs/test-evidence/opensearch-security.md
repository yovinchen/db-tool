# OpenSearch Security HTTPS Completeness Evidence

Task ID: DB-OPENSEARCH-TLS-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T20:46:45Z

Environment: Docker on macOS arm64; Rust 1.96.0; real security plugin, generated local CA/node certificate, random per-campaign admin password, loopback-only host port

Product version: OpenSearch 2.17.1; Security plugin 2.17.1.0

Command: `./scripts/integration-opensearch-security-test.sh`

Resource operations:

| Resource | Create/write | Read all fixture data | Metadata/admin | Guard/limit/security | Update/delete | Cleanup |
| --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_search_security_*` | authenticated HTTPS index creation and Alice/Bob/Carol 3/3 writes PASS | every name/role/source value exact PASS | ping, `caps.search=true`, exact index listing PASS | write guard; size clamp; from/truncated; wrong password rejected with HTTP 401; missing CA rejected at certificate validation PASS | public SearchEngine update/delete UNSUPPORTED | disposable OpenSearch volume removed PASS |

The campaign also proved that stale certificates regenerate before startup,
the CA signing key remains outside the mounted service directory with mode
0600, and the HTTPS port is published only on `127.0.0.1`.

Cleanup: PASS by disposable Docker teardown; public search delete is UNSUPPORTED

Commits: `2e93c35`, `bbb6323`, `932655d`, this commit
