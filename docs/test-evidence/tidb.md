# TiDB Validation Evidence

Task ID: DB-TIDB-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T19:38:15Z

Environment: Docker on macOS arm64; Rust 1.96.0; TiDB/PD/TiKV Community 8.5.6; TiProxy 1.3.2

Command: `DBTOOL_RUN_TIDB_INTEGRATION=1 cargo test -p dbtool-cli --test live_services tidb_compat_live_sql_lifecycle_and_typed_values -- --exact --nocapture`; `./scripts/integration-tidb-secure-test.sh`; `./scripts/integration-tidb-logical-roundtrip-test.sh`; `./scripts/integration-tidb-tiproxy-test.sh`

Resource operations:

| Resource | Create | Insert/write | Read all fixture data | Update | Targeted delete | Metadata | Guard | Limit | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_tidb.dbtool_it_tidb_users_37569_1784056703875` | database-qualified table + PK PASS | rows `(1,alice)`, `(2,bob)` 2/2 PASS | IDs `1,2`, names exact 2/2 PASS | ID 1 became `alice-updated` PASS | ID 2 removed; ID 1 remained PASS | schemas, table, columns, non-null PK and primary index PASS | INSERT without write permission blocked; CREATE/DROP and unbounded DELETE required confirmation PASS | typed int/float/binary/null and limit 2/truncated PASS | table absent after DROP; database and volume removed PASS |
| `dbtool_it_tidb_secure.dbtool_it_tidb_secure_node1_51191_1784057225492` | TLS node-1 table + PK PASS | 2/2 rows PASS | all rows exact PASS | ID 1 updated PASS | ID 2 removed PASS | schema/PK/index PASS | wrong/no TLS rejected; write and confirmation guards PASS | typed values and truncation PASS | table absent; secure volume removed PASS |
| `dbtool_it_tidb_secure.dbtool_it_tidb_secure_node2_51191_1784057226283` | TLS node-2 table + PK PASS | 2/2 rows PASS | all rows exact PASS | ID 1 updated PASS | ID 2 removed PASS | schema/PK/index PASS | password-authenticated TLS; write/confirmation guards PASS | typed values and truncation PASS | table absent; secure volume removed PASS |
| `dbtool_it_tidb_secure.dbtool_it_tidb_x509_51191_1784057227836` | X.509 user table + PK PASS | 2/2 rows PASS | all rows exact PASS | ID 1 updated PASS | ID 2 removed PASS | schema/PK/index PASS | client certificate required; write/confirmation guards PASS | typed values and truncation PASS | table absent; secure volume removed PASS |
| `dbtool_it_tidb_secure.dbtool_it_tidb_roundtrip_src_1784057567_57716` | source table + PK through TLS node 1 PASS | 3/3 rows PASS | IDs `1,2,3`, notes and priorities exact in versioned export artifact PASS | N/A | N/A | export columns `id,note,priority` exact PASS | export remained read-only | public `export sql` PASS | source table and artifact removed PASS |
| `dbtool_it_tidb_secure.dbtool_it_tidb_roundtrip_restore_1784057567_57716` | restore table + PK through TLS node 2 PASS | public import inserted 3/3 PASS | all fields exact through node 2 and again through node 1 PASS | N/A | N/A | destination schema accepted artifact PASS | import required `--allow-write` | public `import sql` cross-node PASS | restore table and volume removed PASS |
| `dbtool_it_tidb_secure.dbtool_it_tidb_tiproxy_1784057758_63238` | table + PK through secure SQL node 1 PASS | row 1 before outage, row 2 while node 1 stopped, row 3 while node 2 stopped PASS | final ordered rows and all note values exact through TiProxy PASS | N/A | N/A | routed table remained accessible across both independent outages PASS | dedicated `REQUIRE SSL` user accepted only the TLS proxy DSN | new connections and writes through TiProxy during each SQL-node outage PASS | table and `dbtool_it_proxy_ssl_63238` user dropped; all containers, volumes, and network removed PASS |

Assertions: the real three-component TiDB stack accepted `tidb://`, exposed the
SQL capability, and passed the complete basic MySQL-protocol SQL checklist.

Defect found and fixed: TiDB exposes `STATISTICS.NON_UNIQUE` as text. The
adapter previously decoded it as `i64` and silently omitted the primary index.
Commit `c2fc4ec` normalizes and validates this catalog value; TiDB, MySQL, and
MariaDB live lifecycles passed after the fix. The failed diagnostic table was
removed with the disposable volume.

Secure phase: the regenerated CA/server/client chain was validated, both TLS
SQL nodes passed full lifecycles, password and X.509 users were accepted, and
missing/incorrect TLS identity paths were rejected.

Cross-node transfer phase: node 1 exported the complete three-row artifact,
node 2 imported it through the public CLI, and both nodes returned the same
three IDs, notes, and priorities.

TiProxy phase: TiProxy remained available while each SQL node was stopped in
turn. New TLS connections wrote one row during each outage; after both nodes
restarted, the proxy returned the exact three-row ordered fixture without loss
or duplication. The isolated table and proxy-only user were then removed.

Pending before campaign COMPLETE: the configured PD, PD-leader, TiKV-boundary,
HA, and certificate-regeneration drills. Basic, secure, logical transfer, and
TiProxy routing are LIVE_PASS and are not presented as those remaining
higher-level guarantees.

Cleanup: PASS

Commits: `974886f`, `c2fc4ec`, `5edf95a`, `b8fa88c`, `0aae9a3`, this commit
