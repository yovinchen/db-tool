# Redis Completeness Evidence

Task ID: DB-REDIS-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T20:13:29Z

Environment: Docker on macOS arm64; Rust 1.96.0; Redis 7.4.9

Product version: Redis 7.4.9

Command: `./scripts/integration-test.sh`; `./scripts/integration-fixture-data-test.sh`; `./scripts/integration-data-roundtrip-test.sh`

Resource operations:

| Resource | Create | Insert/write | Read all fixture data | Update/overwrite | Targeted delete | Metadata/admin | Guard | Limit/transfer | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_redis_{key,ttl,nx,raw,counter,scan}_38259_1784059908074*` | 8 isolated keys PASS | SET/raw SET/INCR, atomic NX+TTL, and three scan keys PASS | all 8 exact values PASS | main value `alice` to `alice-updated`; second NX rejected and preserved `created-once` PASS | DEL reported 8/8 PASS | PING, SCAN, ordinary TTL and NX TTL 1..30 PASS | SET/raw SET/FLUSHALL without write permission blocked; unmet NX returned explicit error PASS | limit 2/truncated PASS | all 8 GETs null and scan empty PASS |
| `dbtool_it_fixture:user:{1,2,3}` | three source keys PASS | Alice/Bob/Carol 3/3 PASS | MGET exactly `alice,bob,carol` PASS | N/A | N/A | SCAN count 3 PASS | bounded prefix only | exported all 3 keys PASS | source keys deleted PASS |
| `dbtool_it_roundtrip:user:{1,2,3}` | import-created keys PASS | public import restored 3/3 PASS | MGET exactly `alice,bob,carol` PASS | N/A | DEL removed 3/3 PASS | SCAN count 3; TTL each 1..120 PASS | import required `--allow-write` | JSON export/import with prefix remap PASS | restored keys deleted PASS |

Assertions: connection and KV capability, SET/GET/overwrite, atomic NX+TTL,
explicit NX conflict without overwrite, TTL, SCAN, read-only and mutating raw
commands, complete eight-key content verification, complete deletion
verification, and public export/import all passed.

Cleanup: PASS

Commits: `974886f`, `561ea93`, `bea6bed`, `74a4907`, `19a3527`, `1ceffc8`
