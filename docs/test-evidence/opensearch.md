# OpenSearch Completeness Evidence

Task ID: DB-OPENSEARCH-001

Result: LIVE_PASS

Run at (UTC): 2026-07-15T17:16:55Z (full CRUD revalidation)

Environment: Docker on macOS arm64; Rust 1.96.0; single-node security-disabled OpenSearch plus isolated TLS transport fixture

Product version: OpenSearch 2.17.1; Python 3.12 Alpine TLS compatibility fixture is supporting transport evidence only

Command: `./scripts/integration-observability-test.sh`

Resource operations:

| Resource | Create/write | Read all fixture data | Metadata/admin | Guard/limit | Update/delete | Cleanup |
| --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_search_*` on real OpenSearch | auto-ID temporary write returned generated ID then was deleted; stable IDs `alice`, `bob`, `carol` created with version 1 PASS | exact `search.search_budgeted/get_doc_budgeted`; get returned exact source; missing ID returned null; three exact `(name, role, source)` tuples and complete aggregation/metadata PASS | ping, `caps.search=true`, both exact read operations, exact index-list entry; total relation/took/timed_out and role aggregation (3 docs) preserved PASS | write without permission blocked; body `size=1000` clamped by global limit 1; get/search one-byte budgets returned `READ_BUDGET_EXCEEDED`; body/CLI offsets returned Alice/Bob/Carol with true/true/false truncation PASS | `bob` patched to `search-editor`, revision 2/version 2; `carol` delete/get-null; wrong-resource confirmation rejected; target-bound delete-index acknowledged PASS | failure guard plus public delete-index; `_cat/indices` contained no `dbtool_it_search_*` index PASS |
| `dbtool_it_seed` in TLS fixture | Dockerfile seed loaded Alice and Bob 2/2 PASS | all six name/role/source values exact PASS | HTTPS CA validation and index list PASS | limit 10 and complete result PASS | compatibility fixture only; not product CRUD evidence | fixture container removed PASS |
| `dbtool_it_search_tls_*` in TLS fixture | Alice/Bob/Carol 3/3 indexed over `opensearch+https://` PASS | all three documents and pagination exact PASS | HTTPS connector kind and index list PASS | same hard limit/truncation checks PASS | UNSUPPORTED | fixture container removed PASS |

The real OpenSearch row is the product-completion claim. The Python fixture
proves the HTTPS transport and file-backed seed path but is not represented as
a real OpenSearch security product run; that is tracked separately.

Cleanup: PASS through the public document delete and confirmed delete-index APIs; only OpenSearch system indices remained

Commits: `2e93c35`, `7a6bbdd`, `926be55`, `932655d`, `b9dd9fd`, `dbe1f32`, `ce19cb4`, IF-T76 caller/docs commit
