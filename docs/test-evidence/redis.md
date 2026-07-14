# Redis Completeness Evidence

Task ID: DB-REDIS-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T19:08:56Z

Environment: Docker on macOS arm64; Rust 1.96.0; Redis 7.4.9

Command: `DBTOOL_RUN_INTEGRATION=1 cargo test -p dbtool-cli --test live_services redis_live_kv_lifecycle_and_raw_safety -- --exact --nocapture`; `./scripts/integration-fixture-data-test.sh`; `./scripts/integration-data-roundtrip-test.sh`

Resource operations:

| Resource | Create | Insert/write | Read all fixture data | Update/overwrite | Targeted delete | Metadata/admin | Guard | Limit/transfer | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_redis_{key,ttl,raw,counter,scan}_96680_1784055775093*` | 7 isolated keys PASS | SET/raw SET/INCR and three scan keys PASS | all 7 exact values PASS | main value `alice` to `alice-updated` PASS | DEL reported 7/7 PASS | PING, SCAN and TTL 1..30 PASS | SET/raw SET/FLUSHALL without write permission blocked PASS | limit 2/truncated PASS | all 7 GETs null and scan empty PASS |
| `dbtool_it_fixture:user:{1,2,3}` | three source keys PASS | Alice/Bob/Carol 3/3 PASS | MGET exactly `alice,bob,carol` PASS | N/A | N/A | SCAN count 3 PASS | bounded prefix only | exported all 3 keys PASS | source keys deleted PASS |
| `dbtool_it_roundtrip:user:{1,2,3}` | import-created keys PASS | public import restored 3/3 PASS | MGET exactly `alice,bob,carol` PASS | N/A | DEL removed 3/3 PASS | SCAN count 3; TTL each 1..120 PASS | import required `--allow-write` | JSON export/import with prefix remap PASS | restored keys deleted PASS |

Assertions: connection and KV capability, SET/GET/overwrite, TTL, SCAN,
read-only and mutating raw commands, complete seven-key content verification,
complete deletion verification, and public export/import all passed.

Cleanup: PASS

Commits: `974886f`, `561ea93`, `bea6bed`, this commit
