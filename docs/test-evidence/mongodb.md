# MongoDB Completeness Evidence

Task ID: DB-MONGO-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T19:08:56Z

Environment: Docker on macOS arm64; Rust 1.96.0; MongoDB 7.0.37 aarch64

Command: `DBTOOL_RUN_INTEGRATION=1 cargo test -p dbtool-cli --test live_services mongo_live_document_lifecycle -- --exact --nocapture`; `./scripts/integration-fixture-data-test.sh`; `./scripts/integration-data-roundtrip-test.sh`

Resource operations:

| Resource | Create | Insert/write | Read all fixture data | Update | Targeted delete | Metadata/admin | Guard | Limit/transfer | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_mongo_users_98495_1784055817045` | collection created on first insert PASS | `_id` Alice/Bob 2/2 PASS | both IDs/names/visit counts exact PASS | Alice visits 1 to 2; matched/modified 1 PASS | Bob then Alice deleted 2/2 PASS | collection listed PASS | insert without write permission, `$out`, and empty-filter delete blocked PASS | read aggregate projection PASS | find `{}` empty; collection removed by final volume teardown PASS |
| `dbtool_it_fixture_people` | source collection PASS | Alice/Bob/Carol 3/3 PASS | IDs, names, roles and active flags exact PASS | N/A | source documents removed 3/3 PASS | collection visible during run | bounded fixture filter only | exported all 3 documents PASS | documents deleted; volume removed PASS |
| `dbtool_it_fixture_people_restore_1784056024_6897` | restore collection PASS | public import restored 3/3 PASS | all 3 documents and every exported field equal to source PASS | N/A | restored documents removed 3/3 PASS | collection visible during run | import required `--allow-write`; generated `_id` dropped on transfer | JSON export/import roundtrip PASS | documents deleted; volume removed PASS |

Assertions: connection and document capability, collection listing, complete
two-document insert/find/update/aggregate/delete lifecycle, aggregate write-stage
and empty-filter guards, complete three-document fixture readback, and public
export/import all passed.

Cleanup: PASS

Unsupported boundary: the public `DocumentStore` can empty a collection but
does not expose drop-collection; the disposable Docker volume was removed.

Commits: `1cd4ed4`, `974886f`, `561ea93`, `bea6bed`, this commit
