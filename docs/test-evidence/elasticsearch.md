# Elasticsearch Completeness Evidence

Task ID: DB-ELASTICSEARCH-001

Result: LIVE_PASS

Run at (UTC): 2026-07-15T17:16:55Z (full CRUD revalidation)

Environment: Docker on macOS arm64; Rust 1.96.0; single-node Elasticsearch with security disabled for the disposable HTTP profile

Product version: Elasticsearch 8.15.5

Command: `./scripts/integration-elasticsearch-test.sh`

Resource operations:

| Resource | Create/write | Read all fixture data | Metadata/admin | Guard/limit | Update/delete | Cleanup |
| --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_elasticsearch_*` | auto-ID write returned its ID and was deleted; stable IDs `alice`, `bob`, `carol` created with version 1 PASS | exact sources, get-by-ID and missing-ID null PASS | ping kind `elasticsearch`, `caps.search=true`, exact index list, total relation/took/timed_out and role aggregation preserved PASS | write guard; body size 1000 could not bypass limit 1; body and CLI offsets plus exact truncation PASS | `bob` update returned version 2 and exact patch; `carol` delete/get-null; wrong-resource token rejected; confirmed delete-index acknowledged PASS | `_cat/indices` returned an empty array after the public delete-index call PASS |

This is product-native plain HTTP compatibility evidence, not an HTTPS claim.
The registered `elasticsearch+https://` transport has service-free mapping
coverage, but no product-native Elasticsearch security/TLS profile was run.

Cleanup: PASS through public document delete and target-bound delete-index before container teardown

Commits: `2e93c35`, `932655d`, `b9dd9fd`, IF-T45 current campaign
