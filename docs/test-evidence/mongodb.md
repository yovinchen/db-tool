# MongoDB Completeness Evidence

Task ID: DB-MONGO-001

Result: LIVE_PASS

Run at (UTC): 2026-07-15T20:07:47Z (focused explicit one/many refresh; original full-family run 2026-07-14T19:08:56Z)

Environment: Docker on macOS arm64; Rust 1.96.0; MongoDB 7.0.37 aarch64

Product version: MongoDB 7.0.37

Command: `DBTOOL_RUN_INTEGRATION=1 cargo test -p dbtool-cli --test live_services mongo_live_document_lifecycle -- --exact --nocapture`; `DBTOOL_RUN_INTEGRATION=1 cargo test -p dbtool-cli --test live_document mongo_live_full_find_options_bounded_aggregate_and_drop -- --exact --nocapture`; `./scripts/integration-fixture-data-test.sh`; `./scripts/integration-data-roundtrip-test.sh`

Latest interface run (UTC): 2026-07-15T20:07:47Z

Resource operations:

| Resource | Create | Insert/write | Read all fixture data | Update | Targeted delete | Metadata/admin | Guard | Limit/transfer | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_mongo_users_98495_1784055817045` | collection created on first insert PASS | `_id` Alice/Bob 2/2 PASS | both IDs/names/visit counts exact PASS | Alice visits 1 to 2; matched/modified 1 PASS | Bob then Alice deleted 2/2 PASS | collection listed PASS | insert without write permission, `$out`, and empty-filter delete blocked PASS | read aggregate projection PASS | find `{}` empty; collection removed by final volume teardown PASS |
| `dbtool_it_fixture_people` | source collection PASS | Alice/Bob/Carol 3/3 PASS | IDs, names, roles and active flags exact PASS | N/A | source documents removed 3/3 PASS | collection visible during run | bounded fixture filter only | exported all 3 documents PASS | documents deleted; volume removed PASS |
| `dbtool_it_fixture_people_restore_1784056024_6897` | restore collection PASS | public import restored 3/3 PASS | all 3 documents and every exported field equal to source PASS | N/A | restored documents removed 3/3 PASS | collection visible during run | import required `--allow-write`; generated `_id` dropped on transfer | JSON export/import roundtrip PASS | documents deleted; volume removed PASS |
| `dbtool_it_document_surface_<run>` | implicit collection creation PASS | Alice/Bob/Carol 3/3 PASS | filter + ascending sort + skip + projection returned exact Bob row; exact two-row page not truncated PASS | default update matched/modified 1/1; confirmed `--many` matched/modified 3/3 PASS | default delete 1/1; confirmed `--many` deleted remaining 2/2 PASS | caps declared all four explicit cardinality operations; collection list `[]` after drop PASS | empty/non-object filter rejected before connection; changed update and cross-operation token reuse rejected PASS | three-row aggregate returned two rows with `truncated=true`; exact two-row find returned `truncated=false` PASS | panic-safe public `doc drop`; final collection list `[]` PASS |

Assertions: connection and document capability, collection listing, complete
two-document insert/find/update/aggregate/delete lifecycle, skip/sort/projection,
exact `limit + 1` truncation, bounded aggregate, aggregate write-stage and empty
filter guards, explicit one/many operation negotiation, exact update/delete
cardinality, target/content/operation-bound bulk confirmation, target-bound
collection drop, complete three-document fixture readback, and public
export/import all passed.

## IF-T74 collection scalar-byte envelope refresh

Run at (UTC): 2026-07-16T03:19:08Z

`DocumentStore.list_collections_budgeted` and the exact
`document.list_collections_budgeted` operation are now implemented and advertised.
The adapter validates `ReadBudget` before opening a cursor, requests MongoDB
`listCollections` with `batchSize=N+1`, accounts every complete collection name
before retention, and accounts the complete `BoundedList` envelope plus the
truncation probe. Unit boundaries cover item N/N+1, exact byte N/N-1, zero and
probe-overflow budgets.

MongoDB 7 Docker validation created three isolated collections and passed the
complete item/byte boundary plus N+1 truncation checks. All three collections
were dropped and the test prefix was absent on final catalog read. The MongoDB
driver necessarily decodes one `CollectionSpecification` before dbtool extracts
and charges its name; `batchSize=N+1` bounds that protocol cursor batch.

Verification: `cargo test -p adapter-mongo` 15/15 PASS; strict all-target Clippy,
rustfmt and diff check PASS; Docker live exact catalog 1/1 PASS.

## IF-T78 exact mutation refresh

Run at (UTC): 2026-07-16T12:13:31Z

`mongo_live_budgeted_mutations_reject_before_write_and_clean_collection`
passed 1/1 against MongoDB 7.0.37. The initial six exact CRUD/lifecycle write operations were
advertised. N-1 insert did not create the unique collection. Exact insert
created three fully read-back documents; update-one returned 1/1, update-many
returned 3/3, delete-one returned 1, and delete-many returned 2. N-1
update/delete/drop preserved the observed document or collection state. The
exact drop removed the collection and the final catalog did not contain it.
Preflight also covers namespace syntax/length, each native BSON document,
write-operation BSON, the batch ceiling, and a conservative complete OP_MSG
size before driver dispatch.

Verification: adapter-mongo 18/18 PASS; MongoDB Docker six-operation exact
lifecycle 1/1 PASS; strict Clippy, rustfmt, and diff check PASS.
Implementation commit: `83db841`.

Aggregate-write follow-up run (UTC): 2026-07-16T12:32:48Z. The separate
`document.aggregate_write_budgeted` operation raises the exact mutation count
to seven. Ordinary `aggregate_budgeted` and legacy read aggregation reject
`$out/$merge` before driver dispatch. An N-1 complete-pipeline budget created
no target; exact `$out` copied all three source documents, exact `$merge`
reconciled the same three stable IDs, and both responses stayed inside an
independent `ReadBudget`. The test then dropped the target and source and a
direct final MongoDB catalog query returned no `dbtool_it_input_*` collection.
Post-dispatch cursor, decoding, transport, and response-budget failures map to
non-retryable `OUTCOME_INDETERMINATE`.

IF-T78 fixture resource operations:

| Resource | Create | Insert/write | Read all fixture data | Update/overwrite | Targeted delete | Metadata/admin | Guard | Limit/timeout | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| accepted source `dbtool_it_input_<suffix>` | implicit collection creation by exact insert PASS | 3/3 documents; stable `_id` values `1,2,3` PASS | 3/3 source documents accounted by `$out` and update-many; final source read 0/0 after deletes PASS | update-one 1/1 then update-many 3/3 PASS | delete-one 1 plus delete-many 2; total 3/3 PASS | collection catalog contained source after rejected drop and excluded it after exact drop PASS | seven exact operations advertised; read-only aggregate rejected `$out/$merge` PASS | exact document/pipeline/namespace/OP_MSG envelope PASS | exact public drop; final source catalog absence PASS |
| rejected preflight phases on source `dbtool_it_input_<suffix>` | N-1 insert created no collection PASS | rejected 0/3 before dispatch PASS | N-1 update preserved `_id=1`; N-1 delete preserved it; N-1 drop preserved collection PASS | 0 unintended modifications PASS | 0 unintended deletes PASS | catalog state matched every preflight assertion PASS | `INPUT_BUDGET_EXCEEDED` PASS | N-1 complete request envelopes rejected PASS | no separate rejected collection remained |
| accepted aggregate target `dbtool_it_input_<suffix>_archive` | exact `$out` created target PASS | `$out` copied 3/3 documents PASS | complete target find returned 3/3 stable IDs; `$merge` retained 3/3 PASS | exact `$merge` used replace/insert reconciliation PASS | N/A | target catalog present after `$out`, absent after exact public drop PASS | read aggregation could not execute write stage PASS | exact two-stage pipeline plus independent response budget PASS | exact public drop; final target catalog absence PASS |
| rejected aggregate target phase `dbtool_it_input_<suffix>_archive` | N-1 `$out` created no target PASS | rejected before dispatch PASS | target catalog count 0/0 PASS | N/A | N/A | target absent before accepted `$out` PASS | mutating pipeline required `aggregate_write_budgeted` PASS | N-1 complete-pipeline bytes rejected PASS | N/A; target was never created by rejected phase |

Follow-up verification: core 151/151 plus doctest; adapter-mongo 19/19; focused
MongoDB Docker aggregate-write lifecycle 1/1; strict Clippy and rustdoc PASS.
Implementation commit: `ab06b88`.

Cleanup: PASS

Lifecycle boundary: `DocumentStore.drop_collection` is now public. Connectors
without collection lifecycle support must return `UNSUPPORTED_CAPABILITY`
instead of silently succeeding.

Commits: `1cd4ed4`, `974886f`, `561ea93`, `bea6bed`, `fe7cfb9`, `83db841`,
`ab06b88`, IF-T61/IF-T78
