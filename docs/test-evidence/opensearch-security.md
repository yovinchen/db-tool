# OpenSearch Security HTTPS Completeness Evidence

Task ID: DB-OPENSEARCH-TLS-001

Result: LIVE_PASS

Run at (UTC): 2026-07-17T17:34:00Z

Environment: Docker on macOS arm64; Rust 1.96.0; real security plugin, generated local CA/node certificate, random per-campaign admin password, loopback-only host port

Product version: OpenSearch 2.17.1; Security plugin 2.17.1.0

Command: `./scripts/integration-opensearch-security-test.sh`

Resource operations:

| Resource | Create/write | Read all fixture data | Metadata/admin | Guard/limit/security | Update/delete | Cleanup |
| --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_search_security_*` | authenticated HTTPS auto-ID create/delete plus stable Alice/Bob/Carol puts PASS | exact GET/search/aggregation; Bob update visible by GET and near-real-time search PASS | ping, exact capability operations and index listing PASS | write guard; 1-byte read budget; size clamp; pagination/truncation; wrong password HTTP 401; missing CA certificate rejection; target-bound delete-index token PASS | Bob patch/readback, Carol targeted delete/absence, temporary generated-ID delete, confirmed index delete PASS | index absent through public catalog after delete; disposable container/volume/network removed PASS |

The campaign also proved that stale certificates regenerate before startup,
the CA signing key remains outside the mounted service directory with mode
0600, and the HTTPS port is published only on `127.0.0.1`.

Cleanup: PASS by public confirmed index deletion/absence verification followed
by disposable Docker teardown

Commits: `2e93c35`, `bbb6323`, `932655d`, `b9dd9fd`, `e0ce46f`
