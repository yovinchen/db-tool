# MongoDB Completeness Evidence

Task ID: DB-MONGO-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T19:08:56Z

Environment: Docker on macOS arm64; Rust 1.96.0; MongoDB 7.0.37 aarch64

Product version: MongoDB 7.0.37

Command: `DBTOOL_RUN_INTEGRATION=1 cargo test -p dbtool-cli --test live_services mongo_live_document_lifecycle -- --exact --nocapture`; `DBTOOL_RUN_INTEGRATION=1 cargo test -p dbtool-cli --test live_document mongo_live_full_find_options_bounded_aggregate_and_drop -- --exact --nocapture`; `./scripts/integration-fixture-data-test.sh`; `./scripts/integration-data-roundtrip-test.sh`

Latest interface run (UTC): 2026-07-15T16:42:46Z

Resource operations:

| Resource | Create | Insert/write | Read all fixture data | Update | Targeted delete | Metadata/admin | Guard | Limit/transfer | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_mongo_users_98495_1784055817045` | collection created on first insert PASS | `_id` Alice/Bob 2/2 PASS | both IDs/names/visit counts exact PASS | Alice visits 1 to 2; matched/modified 1 PASS | Bob then Alice deleted 2/2 PASS | collection listed PASS | insert without write permission, `$out`, and empty-filter delete blocked PASS | read aggregate projection PASS | find `{}` empty; collection removed by final volume teardown PASS |
| `dbtool_it_fixture_people` | source collection PASS | Alice/Bob/Carol 3/3 PASS | IDs, names, roles and active flags exact PASS | N/A | source documents removed 3/3 PASS | collection visible during run | bounded fixture filter only | exported all 3 documents PASS | documents deleted; volume removed PASS |
| `dbtool_it_fixture_people_restore_1784056024_6897` | restore collection PASS | public import restored 3/3 PASS | all 3 documents and every exported field equal to source PASS | N/A | restored documents removed 3/3 PASS | collection visible during run | import required `--allow-write`; generated `_id` dropped on transfer | JSON export/import roundtrip PASS | documents deleted; volume removed PASS |
| `dbtool_it_document_surface_<run>` | implicit collection creation PASS | Alice/Bob/Carol 3/3 PASS | filter + ascending sort + skip + projection returned exact Bob row; exact two-row page not truncated PASS | empty-filter update rejected PASS | N/A | collection list excludes target after drop PASS | drop rejected without write flag, then returned target/resource-bound confirmation PASS | three-row aggregate returned two rows with `truncated=true`; exact two-row find returned `truncated=false` PASS | public `doc drop` removed collection PASS |

Assertions: connection and document capability, collection listing, complete
two-document insert/find/update/aggregate/delete lifecycle, skip/sort/projection,
exact `limit + 1` truncation, bounded aggregate, aggregate write-stage and empty
filter guards, target-bound collection drop, complete three-document fixture
readback, and public export/import all passed.

Cleanup: PASS

Lifecycle boundary: `DocumentStore.drop_collection` is now public. Connectors
without collection lifecycle support must return `UNSUPPORTED_CAPABILITY`
instead of silently succeeding.

Commits: `1cd4ed4`, `974886f`, `561ea93`, `bea6bed`, `fe7cfb9`
