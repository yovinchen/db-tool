# OpenSearch Completeness Evidence

Task ID: DB-OPENSEARCH-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T20:46:45Z

Environment: Docker on macOS arm64; Rust 1.96.0; single-node security-disabled OpenSearch plus isolated TLS transport fixture

Product version: OpenSearch 2.17.1; Python 3.12 Alpine TLS compatibility fixture is supporting transport evidence only

Command: `./scripts/integration-observability-test.sh`

Resource operations:

| Resource | Create/write | Read all fixture data | Metadata/admin | Guard/limit | Update/delete | Cleanup |
| --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_search_*` on real OpenSearch | first index operation implicitly created the index; Alice/Bob/Carol 3/3 writes PASS | exact `(name, role, source)` tuples for all three documents PASS | ping, `caps.search=true`, exact index-list entry PASS | write without permission blocked; body `size=1000` clamped by global limit 1; body/CLI offsets returned Alice/Bob/Carol with true/true/false truncation PASS | public SearchEngine update/delete UNSUPPORTED | disposable OpenSearch container/volume removed PASS |
| `dbtool_it_seed` in TLS fixture | Dockerfile seed loaded Alice and Bob 2/2 PASS | all six name/role/source values exact PASS | HTTPS CA validation and index list PASS | limit 10 and complete result PASS | compatibility fixture only; not product CRUD evidence | fixture container removed PASS |
| `dbtool_it_search_tls_*` in TLS fixture | Alice/Bob/Carol 3/3 indexed over `opensearch+https://` PASS | all three documents and pagination exact PASS | HTTPS connector kind and index list PASS | same hard limit/truncation checks PASS | UNSUPPORTED | fixture container removed PASS |

The real OpenSearch row is the product-completion claim. The Python fixture
proves the HTTPS transport and file-backed seed path but is not represented as
a real OpenSearch security product run; that is tracked separately.

Cleanup: PASS by disposable Docker teardown; public search delete is UNSUPPORTED

Commits: `2e93c35`, `7a6bbdd`, `926be55`, `932655d`, `b9dd9fd`
