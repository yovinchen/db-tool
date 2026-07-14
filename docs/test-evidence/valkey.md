# Valkey Completeness Evidence

Task ID: DB-VALKEY-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T19:13:16Z

Environment: Docker on macOS arm64; Rust 1.96.0; Valkey 8.1.8

Command: `DBTOOL_RUN_COMPAT_INTEGRATION=1 DBTOOL_RUN_VALKEY_COMPAT=1 cargo test -p dbtool-cli --test live_services valkey_compat_live_kv_lifecycle_and_raw_safety -- --exact --nocapture`

Resource operations:

| Resource | Create/write | Read all fixture data | Overwrite | TTL/raw/admin | Guard/limit | Targeted delete | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_valkey_18451_1784056323038:value` | SET `valkey` PASS | GET exact PASS | became `valkey-updated` PASS | raw PING PASS | SET without write permission blocked; bounded SCAN PASS | included in DEL 3/3 PASS | GET null PASS |
| `dbtool_it_valkey_18451_1784056323038:ttl` | SET `short-lived` PASS | GET exact PASS | N/A | TTL 1..30 PASS | FLUSHALL without write permission blocked | included in DEL 3/3 PASS | GET null PASS |
| `dbtool_it_valkey_18451_1784056323038:raw` | raw SET `raw-value` PASS | GET exact PASS | N/A | raw command PASS | raw SET without write permission blocked; limit 2/truncated PASS | included in DEL 3/3 PASS | GET null and prefix SCAN empty PASS |

Assertions: real Valkey product connection/capability, every value, overwrite,
TTL, raw read/write policy, bounded scan, exact deletion count, and complete
post-delete absence all passed.

Cleanup: PASS

Commits: `f1be977`, this commit
