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

## IF-T74 index scalar-byte envelope refresh

Run at (UTC): 2026-07-16T03:19:08Z

`SearchEngine.list_indices_budgeted` and the exact
`search.list_indices_budgeted` operation now account each complete `IndexInfo`,
the returned `BoundedList`, and the N+1 probe against the caller's item and byte
budgets. Zero and probe-overflow item budgets and zero/oversized byte budgets
fail before HTTP dispatch. OpenSearch 2.17.1 Docker validation created two
isolated indices, passed item N/N+1 and exact byte N/N-1 boundaries, deleted
both indices, and left only the pre-existing system indices.

CAT indices has no reliable cross-version `size` or cursor parameter. The
adapter therefore caps its raw JSON body at the smaller of the caller byte
budget and 1 MiB, then constructs and observes no more than N+1 portable index
objects. This is an explicit transport boundary, not a claim that OpenSearch
scans only N+1 server-side rows.

Verification: `cargo test -p adapter-search` 37/37 PASS; strict all-target
Clippy, rustfmt and diff check PASS; OpenSearch live exact catalog 1/1 PASS.

## IF-T78 exact mutation refresh

Run at (UTC): 2026-07-16T12:13:31Z

The shared OpenSearch lifecycle now negotiates and executes all five exact
mutation operations. N-1 put left the target index absent. Exact auto-ID index,
stable-ID put, patch update and readback, document delete, search/get, and
index delete passed. N-1 document delete preserved the generated document, and
N-1 index delete preserved the complete index. The main and catalog-peer
indices were then removed through exact delete-index; final catalog polling
proved zero test indices. Target syntax, 255-byte index, 512-byte ID, complete
portable request, JSON body, and 16 MiB transport ceilings are validated before
HTTP request bytes; all later failures are outcome-indeterminate.

IF-T78 fixture resource operations (plain HTTP product run only):

| Resource | Create | Insert/write | Read all fixture data | Update/overwrite | Targeted delete | Metadata/admin | Guard | Limit/timeout | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| accepted main index `dbtool-it-budgeted-opensearch-<pid>-<suffix>` | auto-ID write created index PASS | generated document plus stable IDs `alice,bob,carol`, 4/4 writes PASS | after generated delete, expected/actual total 3/3; stable IDs `alice,bob,carol`; bounded page 2 and aggregation count 3 PASS | `alice` patch returned `updated` and exact get exposed updated source PASS | generated ID exact delete returned `deleted`; get returned null PASS | main and peer both visible in complete index catalog PASS | N-1 document delete preserved generated document; N-1 index delete preserved main index PASS | exact mutation inputs; catalog N/N+1 and byte N/N-1; search budget 2 PASS | target-bound exact delete-index acknowledged; final catalog excluded main PASS |
| rejected preflight phase on main index | N-1 stable-ID put created no index PASS | rejected before HTTP send PASS | index catalog count 0/0 before accepted write PASS | N/A | N/A | main absent after rejected put PASS | `INPUT_BUDGET_EXCEEDED` PASS | N-1 complete request bytes rejected PASS | N/A; rejected phase created no resource |
| accepted peer index `dbtool-it-budgeted-opensearch-<pid>-<suffix>-catalog-peer` | stable-ID put created index PASS | `catalog-probe` 1/1 PASS | exact get returned 1/1 stable ID with complete `product/purpose` payload PASS | N/A | N/A | peer and main both present in exact catalog PASS | exact mutation operation and target syntax enforced PASS | peer request exact; catalog limits shared with main PASS | target-bound exact delete-index acknowledged; final catalog excluded peer PASS |

The OpenSearch security-plugin HTTPS profile was not rerun for IF-T78. Its
earlier CA/auth evidence remains in `opensearch-security.md` and is not used to
claim the five exact mutations passed over HTTPS.

Peer readback follow-up (UTC): 2026-07-16T13:21:27Z. The exact lifecycle was
rerun after adding a stable-ID `get_doc_budgeted` assertion for the catalog-peer
index; the complete `product/purpose` source matched 1/1 before both indices
were deleted, and final catalog absence still passed.

Verification: adapter-search 40/40 PASS; OpenSearch Docker shared exact
lifecycle 1/1 PASS; strict Clippy, rustfmt, and diff check PASS.
Implementation commits: `3822948`, `4b6b6e2`.

Cleanup: PASS through the public document delete and confirmed delete-index APIs; only OpenSearch system indices remained

Commits: `2e93c35`, `7a6bbdd`, `926be55`, `932655d`, `b9dd9fd`, `dbe1f32`,
`ce19cb4`, `3822948`, `4b6b6e2`, IF-T76/IF-T78
