# TiDB Validation Evidence

Task ID: DB-TIDB-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T19:54:57Z

Environment: Docker on macOS arm64; Rust 1.96.0; TiDB/PD/TiKV Community 8.5.6; TiProxy 1.3.2

Product version: TiDB, PD, and TiKV Community 8.5.6; TiProxy 1.3.2

Command: `DBTOOL_RUN_TIDB_INTEGRATION=1 cargo test -p dbtool-cli --test live_services tidb_compat_live_sql_lifecycle_and_typed_values -- --exact --nocapture`; `./scripts/integration-tidb-secure-test.sh`; `./scripts/integration-tidb-logical-roundtrip-test.sh`; `./scripts/integration-tidb-tiproxy-test.sh`; `./scripts/integration-tidb-ha-drill.sh`; `./scripts/integration-tidb-pd-drill.sh`; `./scripts/integration-tidb-pd-leader-drill.sh`; `./scripts/integration-tidb-tikv-outage-boundary.sh`; `./scripts/integration-tidb-cert-regeneration-test.sh`

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
| `dbtool_it_tidb_secure.dbtool_it_tidb_ha_drill_1784058017_81207` | table + PK through SQL node 1 PASS | baseline plus one write during each independent SQL-node outage, 3/3 PASS | exact ordered fixture returned through restarted node 2 PASS | N/A | N/A | both SQL frontends shared the same TiKV-backed data PASS | stopped node DSN rejected ping while surviving node remained writable PASS | SQL-node N-1 continuity PASS | table dropped; topology removed PASS |
| `dbtool_it_tidb_secure.dbtool_it_tidb_pd_drill_1784058179_95702` | table + PK PASS | baseline plus node-1/node-2 writes during each PD1/PD2/PD3 outage, 7/7 PASS | exact ordered seven-row fixture PASS | N/A | N/A | each restarted PD member reached healthy state PASS | always retained 2/3 PD quorum; no quorum-loss claim | PD N-1 continuity PASS | table dropped; topology removed PASS |
| `dbtool_it_tidb_secure.dbtool_it_tidb_pd_leader_drill_1784058361_99363` | table + PK PASS | baseline plus one write per SQL node while discovered leader `pd-1` was down, 3/3 PASS | exact ordered fixture after member recovery PASS | N/A | N/A | replacement leader `pd-2` elected; healthy leader remained after `pd-1` returned PASS | stopped the discovered leader, not an arbitrary member PASS | PD leader election continuity PASS | table dropped; topology removed PASS |
| `dbtool_it_tidb_secure.dbtool_it_tidb_tikv_outage_1784058555_3275` | table + PK PASS | baseline, timed-out outage write, deterministic post-restart write PASS | exact three-row recovered fixture; timed-out write was confirmed committed PASS | N/A | N/A | both SQL pings and baseline read stayed bounded; both SQL pings recovered PASS | hard timeouts fail; only recognized bounded storage failures are accepted | two-TiKV observation boundary, not production HA PASS | table dropped; topology removed PASS |
| `dbtool_it_tidb_secure.dbtool_it_tidb_cert_drill_1784058695_7080` | independent generation-1 and generation-2 tables + PK PASS | one generation-specific row in each cold-start cluster PASS | each generation returned exactly its one expected row PASS | N/A | N/A | CA, server, and client fingerprints all changed PASS | generation-1 CA rejected by generation-2 TLS endpoint PASS | cold regeneration only; no online-rotation claim | generation-2 table dropped; both topologies removed PASS |

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

Resilience phase: both SQL frontends passed independent N-1 outages; every PD
member passed an independent N-1 outage while retaining 2/3 quorum; the actual
PD leader was stopped and replaced; and all final fixtures were checked as
complete ordered datasets after recovery.

TiKV boundary phase: with one of two TiKV processes stopped, both SQL pings and
the baseline read remained bounded. The outage write returned a client timeout
but was present after recovery, proving that timeout is an ambiguous commit
outcome. Hard timeouts are now fatal, and the test is explicitly an observation
boundary rather than a production TiKV HA guarantee.

Certificate phase: two independent cold-start clusters used different CA,
server, and client certificate fingerprints. Both served their exact
generation-specific fixture, and the second cluster rejected the first CA.
This proves cold certificate regeneration, not online rotation or retained-data
continuity.

Completion boundary: COMPLETE covers the repository's declared local TiDB
8.5.6 profiles on macOS arm64: basic SQL, TLS/password/X.509, cross-node public
export/import, TiProxy routing, SQL-node N-1, PD N-1, PD leader election, the
two-TiKV outage observation, and cold certificate regeneration. It does not
claim production replica placement, multi-fault quorum survival, concurrent
load behavior, or online certificate rotation.

Cleanup: PASS

Commits: `974886f`, `c2fc4ec`, `5edf95a`, `b8fa88c`, `0aae9a3`, `5469d52`, `a8388ae`, `d7b18cc`, `2e38e41`, `961b173`, `4c2faa8`
