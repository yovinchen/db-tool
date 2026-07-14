# Dragonfly Completeness Evidence

Task ID: DB-DRAGONFLY-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T20:13:29Z

Environment: Docker on macOS arm64; Rust 1.96.0; Dragonfly 1.39.0

Product version: Dragonfly 1.39.0

Command: `DBTOOL_IT_COMPAT_EXTRA=1 ./scripts/integration-compat-test.sh`

Resource operations:

| Resource | Create/write | Read all fixture data | Overwrite | TTL/raw/admin | Guard/limit | Targeted delete | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_dragonfly_39084_1784059954924:value` | SET `dragonfly` PASS | GET exact PASS | became `dragonfly-updated` PASS | raw PING PASS | SET without write permission blocked; bounded SCAN PASS | included in DEL 4/4 PASS | GET null PASS |
| `dbtool_it_dragonfly_39084_1784059954924:ttl` | SET `short-lived` PASS | GET exact PASS | N/A | TTL 1..30 PASS | FLUSHALL without write permission blocked | included in DEL 4/4 PASS | GET null PASS |
| `dbtool_it_dragonfly_39084_1784059954924:nx` | atomic NX+TTL SET `created-once` PASS | GET exact PASS | second NX rejected; value unchanged PASS | TTL 1..30 PASS | unmet condition returned explicit error | included in DEL 4/4 PASS | GET null PASS |
| `dbtool_it_dragonfly_39084_1784059954924:raw` | raw SET `raw-value` PASS | GET exact PASS | N/A | raw command PASS | raw SET without write permission blocked; limit 2/truncated PASS | included in DEL 4/4 PASS | GET null and prefix SCAN empty PASS |

Assertions: real Dragonfly product connection/capability, every value,
overwrite, atomic NX+TTL and conflict retention, raw read/write policy, bounded
scan, exact deletion count, and complete post-delete absence all passed.

Cleanup: PASS

Commits: `f1be977`, `74a4907`, `19a3527`, `1ceffc8`
