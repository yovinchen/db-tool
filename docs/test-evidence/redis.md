# Redis Completeness Evidence

Task ID: DB-REDIS-001

Result: LIVE_PASS

Run at (UTC): 2026-07-15T20:14:52Z (focused binary/raw refresh; SCAN/transfer refresh 2026-07-15T19:50:00Z; original full-family run 2026-07-14T20:13:29Z)

Environment: Docker on macOS arm64; Rust 1.96.0; Redis 7.4.9

Product version: Redis 7.4.9

Command: `./scripts/integration-test.sh`; `./scripts/integration-fixture-data-test.sh`; `./scripts/integration-data-roundtrip-test.sh`; focused `kv_binary_raw redis_live_binary_values_and_raw_policy_are_exact`; focused `live_services redis_live_kv_lifecycle_and_raw_safety`; focused Redis transfer artifact test; adapter live non-UTF-8 SCAN test

Resource operations:

| Resource | Create | Insert/write | Read all fixture data | Update/overwrite | Targeted delete | Metadata/admin | Guard | Limit/transfer | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_redis_{key,ttl,nx,raw,counter,scan}_*` | 30 isolated keys PASS | SET plus confirmed raw SET/INCR, atomic NX+TTL, and 25 scan keys PASS | five scalar fixtures plus the exact set of all 25 scan keys PASS | main value `alice` to `alice-updated`; second NX rejected and preserved `created-once` PASS | DEL reported 30/30 PASS | PING, ordinary TTL and NX TTL 1..30 PASS | raw mutation required write flag and two-step confirmation; FLUSHALL fail-closed; unmet NX explicit PASS | exact N=2 returned `truncated=false`; N+1 returned true; 25-key multi-page scan returned complete deduplicated set PASS | all scalar GETs null and scan empty PASS |
| `dbtool_it_kv_binary_raw_*` | binary, empty, text and confirmed-raw keys PASS | canonical base64 bytes `00 ff 68 65 6c 6c 6f`, zero bytes, compatible UTF-8 and raw SET PASS | get preserved exact typed bytes; empty differed from missing; raw GET remained binary PASS | changed raw value/key could not reuse token PASS | DEL reported 4 existing keys plus cleanup probe 1/1 PASS | encoding was binary/utf8/null exactly PASS | no-write raw SET blocked; FLUSHALL/SELECT/EVAL/FUNCTION/unknown/KEYS/HGETALL returned CONFIG_ERROR PASS | 1 MiB/arg, 8 MiB request/response and 10,000 RESP-node contracts covered offline/adapter PASS | all five keys missing; prefix scan `[]` PASS |
| `dbtool_it_non_utf8_*:<ff>` | one binary Redis key PASS | direct fixture SET PASS | portable scan returned `SERIALIZATION_ERROR` rather than lossy text or partial success PASS | N/A | direct fixture DEL 1/1 PASS | explicit cursor loop reached the matching page PASS | portable UTF-8 key boundary enforced PASS | page decode failure propagated PASS | binary fixture key deleted PASS |
| `dbtool_it_fixture:user:{1,2,3}` | three source keys PASS | Alice/Bob/Carol 3/3 PASS | MGET exactly `alice,bob,carol` PASS | N/A | N/A | SCAN count 3 PASS | bounded prefix only | exported all 3 keys PASS | source keys deleted PASS |
| `dbtool_it_roundtrip:user:{1,2,3}` | import-created keys PASS | public import restored 3/3 PASS | MGET exactly `alice,bob,carol` PASS | N/A | DEL removed 3/3 PASS | SCAN count 3; TTL each 1..120 PASS | import required `--allow-write` | JSON export/import with prefix remap PASS | restored keys deleted PASS |

Assertions: connection and KV capability, SET/GET/overwrite, atomic NX+TTL,
explicit NX conflict without overwrite, TTL, exact N/N+1 truncation, complete
multi-page SCAN with de-duplication, non-UTF-8 failure propagation, exact binary/
empty/text/missing distinction, canonical base64 input, typed bounded raw output,
confirmed raw mutations, fail-closed dangerous/unbounded/unknown commands,
complete content/deletion verification, and public export/import all passed.

Cleanup: PASS

Commits: `974886f`, `561ea93`, `bea6bed`, `74a4907`, `19a3527`, `1ceffc8`, IF-T57, IF-T62
