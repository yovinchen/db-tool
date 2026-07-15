# KeyDB Completeness Evidence

Task ID: DB-KEYDB-001

Result: LIVE_PASS

Run at (UTC): 2026-07-15T21:04:09Z (KV artifact v3 lifecycle refresh; original full-family run 2026-07-14T20:13:29Z)

Environment: Docker on macOS arm64; Rust 1.96.0; KeyDB 6.3.4

Product version: KeyDB 6.3.4

Command: `DBTOOL_IT_COMPAT_EXTRA=1 ./scripts/integration-compat-test.sh`; `DBTOOL_RUN_INTEGRATION=1 DBTOOL_IT_REDIS_DSN=keydb://127.0.0.1:26380/0 cargo test -p dbtool-cli --test transfer_artifacts redis_artifact_v3_preserves_lifetimes_skips_expired_and_binds_replacement -- --exact --nocapture`

Resource operations:

| Resource | Create/write | Read all fixture data | Overwrite | TTL/raw/admin | Guard/limit | Targeted delete | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_keydb_39084_1784059954924:value` | SET `keydb` PASS | GET exact PASS | became `keydb-updated` PASS | raw PING PASS | SET without write permission blocked; bounded SCAN PASS | included in DEL 4/4 PASS | GET null PASS |
| `dbtool_it_keydb_39084_1784059954924:ttl` | SET `short-lived` PASS | GET exact PASS | N/A | TTL 1..30 PASS | FLUSHALL without write permission blocked | included in DEL 4/4 PASS | GET null PASS |
| `dbtool_it_keydb_39084_1784059954924:nx` | atomic NX+TTL SET `created-once` PASS | GET exact PASS | second NX rejected; value unchanged PASS | TTL 1..30 PASS | unmet condition returned explicit error | included in DEL 4/4 PASS | GET null PASS |
| `dbtool_it_keydb_39084_1784059954924:raw` | raw SET `raw-value` PASS | GET exact PASS | N/A | raw command PASS | raw SET without write permission blocked; limit 2/truncated PASS | included in DEL 4/4 PASS | GET null and prefix SCAN empty PASS |
| `dbtool_it_artifact_*:{source,target}:*` | v3 source/target persistent, binary, empty, long and expired keys PASS | exact bytes plus persistent/absolute expiry PASS | expiry-bound confirmed replacement PASS | long deadline not extended; expired entry skipped without SET PASS | v2/missing expiry rejected before DSN; partial artifact rejected PASS | bounded explicit cleanup PASS | source and target scans both `[]` PASS |

Assertions: real KeyDB product connection/capability, every value, overwrite,
atomic NX+TTL and conflict retention, raw read/write policy, bounded scan,
exact deletion count, and complete post-delete absence all passed. The v3 refresh
also proved per-entry atomic lifetime restore and no expired-key revival.

Cleanup: PASS

Commits: `f1be977`, `74a4907`, `19a3527`, `1ceffc8`, `29b3126`, IF-T64
