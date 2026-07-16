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
| `dbtool_it_elasticsearch_*` | auto-ID write returned its ID and was deleted; stable IDs `alice`, `bob`, `carol` created with version 1 PASS | exact `search.search_budgeted/get_doc_budgeted`; complete sources, aggregation/metadata, get-by-ID and missing-ID null PASS | ping kind `elasticsearch`, `caps.search=true`, both exact read operations, exact index list, total relation/took/timed_out and role aggregation preserved PASS | write guard; body size 1000 could not bypass limit 1; get/search one-byte budgets returned `READ_BUDGET_EXCEEDED`; body/CLI offsets plus exact truncation PASS | `bob` update returned version 2 and exact patch; `carol` delete/get-null; wrong-resource token rejected; confirmed delete-index acknowledged PASS | failure guard plus public delete-index; `_cat/indices` returned an empty array PASS |

This is product-native plain HTTP compatibility evidence, not an HTTPS claim.
The registered `elasticsearch+https://` transport has service-free mapping
coverage, but no product-native Elasticsearch security/TLS profile was run.

## IF-T74 index scalar-byte envelope refresh

Run at (UTC): 2026-07-16T03:19:08Z

The shared Search adapter now advertises and implements exact
`search.list_indices_budgeted` negotiation. It validates item/byte budgets
before HTTP dispatch, accounts complete `IndexInfo` values plus the returned
envelope and N+1 probe, and rejects exact byte N-1 while accepting N.
Elasticsearch 8.15.5 Docker validation created two isolated indices, passed
the item and byte boundaries, deleted both, and confirmed `_cat/indices` was
`[]` afterward.

Elasticsearch CAT indices shares the OpenSearch transport limitation: there is
no reliable cross-version page-size/cursor contract. Raw JSON is capped at
`min(caller max_bytes, 1 MiB)` before parsing, and only N+1 portable objects are
constructed and observed. Product-native HTTPS remains outside this run.

Verification: shared adapter-search 37/37 PASS; strict all-target Clippy,
rustfmt and diff check PASS; Elasticsearch live exact catalog 1/1 PASS.

## IF-T78 exact mutation refresh

Run at (UTC): 2026-07-16T12:13:31Z

Elasticsearch 8.15.5 ran the same five-operation exact mutation contract as
OpenSearch. N-1 put did not create an index; exact auto-ID index, stable-ID put,
patch update/readback and document delete passed. N-1 document delete left the
document readable and N-1 index delete left the index visible. Exact cleanup
deleted both unique indices and the final catalog excluded them. Request
targets/body are fully checked before HTTP dispatch and post-send errors cannot
be automatically retried.

IF-T78 fixture resource operations (plain HTTP product run only):

| Resource | Create | Insert/write | Read all fixture data | Update/overwrite | Targeted delete | Metadata/admin | Guard | Limit/timeout | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| accepted main index `dbtool-it-budgeted-elasticsearch-<pid>-<suffix>` | auto-ID write created index PASS | generated document plus stable IDs `alice,bob,carol`, 4/4 writes PASS | after generated delete, expected/actual total 3/3; stable IDs `alice,bob,carol`; bounded page 2 and aggregation count 3 PASS | `alice` patch returned `updated` and exact get exposed updated source PASS | generated ID exact delete returned `deleted`; get returned null PASS | main and peer both visible in complete index catalog PASS | N-1 document delete preserved generated document; N-1 index delete preserved main index PASS | exact mutation inputs; catalog N/N+1 and byte N/N-1; search budget 2 PASS | target-bound exact delete-index acknowledged; final catalog excluded main PASS |
| rejected preflight phase on main index | N-1 stable-ID put created no index PASS | rejected before HTTP send PASS | index catalog count 0/0 before accepted write PASS | N/A | N/A | main absent after rejected put PASS | `INPUT_BUDGET_EXCEEDED` PASS | N-1 complete request bytes rejected PASS | N/A; rejected phase created no resource |
| accepted peer index `dbtool-it-budgeted-elasticsearch-<pid>-<suffix>-catalog-peer` | stable-ID put created index PASS | `catalog-probe` 1/1 PASS | exact get returned 1/1 stable ID with complete `product/purpose` payload PASS | N/A | N/A | peer and main both present in exact catalog PASS | exact mutation operation and target syntax enforced PASS | peer request exact; catalog limits shared with main PASS | target-bound exact delete-index acknowledged; final catalog excluded peer PASS |

Product-native Elasticsearch HTTPS was not run in this campaign; the IF-T78
claim is limited to the disposable security-disabled HTTP profile.

Peer readback follow-up (UTC): 2026-07-16T13:21:27Z. The exact lifecycle was
rerun after adding a stable-ID `get_doc_budgeted` assertion for the catalog-peer
index; the complete `product/purpose` source matched 1/1 before both indices
were deleted, and final catalog absence still passed.

Verification: shared adapter-search 40/40 PASS; Elasticsearch Docker shared
exact lifecycle 1/1 PASS; strict Clippy, rustfmt, and diff check PASS.
Implementation commits: `3822948`, `4b6b6e2`. Product-native HTTPS remains outside this
focused run.

Cleanup: PASS through public document delete and target-bound delete-index before container teardown

Commits: `2e93c35`, `932655d`, `b9dd9fd`, `dbe1f32`, `ce19cb4`, `3822948`, `4b6b6e2`,
IF-T76/IF-T78
