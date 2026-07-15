# Redis Messaging Completeness Evidence

Task ID: DB-REDIS-MQ-001

Result: LIVE_PASS

Run at (UTC): 2026-07-15T21:27:35Z

Environment: Docker on macOS arm64; Rust 1.96.0; Redis 7 Alpine, Valkey 8.1.8,
KeyDB 6.3.4, and Dragonfly 1.39.0 compatibility profiles

Product version: Redis 7.4.9, Valkey 8.1.8, KeyDB 6.3.4, Dragonfly 1.39.0

Command: focused adapter/CLI commands below, repeated for each compatible DSN

```text
cargo test -p adapter-redis
env DBTOOL_IT_REDIS_DSN=redis://127.0.0.1:16379/0 \
  cargo test -p adapter-redis \
  redis_live_stream_groups_replay_ack_and_report_complete_lag -- --nocapture
cargo clippy -p adapter-redis --all-targets -- -D warnings
env DBTOOL_RUN_MQ_INTEGRATION=1 DBTOOL_IT_REDIS_DSN=redis://127.0.0.1:16379/0 \
  cargo test -p dbtool-cli --test live_messaging \
  redis_live_stream_produce_detail_and_consume -- --exact --nocapture
# The adapter live command above was repeated unchanged with:
# valkey://127.0.0.1:26379/0
# keydb://127.0.0.1:26380/0
# dragonfly://127.0.0.1:26381/0
```

Resource operations: stateful Stream matrix

| Operation | Live assertion | Result |
| --- | --- | --- |
| Capability negotiation | Redis advertises `message.consume_group` and `message.consume_ack`; it does not advertise durable consumption | PASS |
| Group ownership | Existing group is required and an explicit member is required; a missing group returns `NOGROUP` and is not created | PASS |
| Bounded delivery | Three messages were written; the first group call returned two within `max=2`, `timeout=2s` | PASS |
| No-ACK replay | Repeating `ack=none` with the same member returned the same two native Stream IDs from its PEL | PASS |
| Complete-batch ACK | `ack=on-success` replayed and acknowledged those two IDs together, then a second invocation acknowledged the remaining new entry | PASS |
| Lag before ACK | `latest=3`, `committed=0`, `lag=3` (`pending=2` plus one undelivered entry) | PASS |
| Lag after partial ACK | `latest=3`, `committed=2`, `lag=1` | PASS |
| Lag after complete ACK | `latest=3`, `committed=3`, `lag=0` | PASS |
| Fail-closed conversion | An entry without `payload` returned a serialization error, remained in the PEL, and produced `latest=4`, `committed=3`, `lag=1` | PASS |
| Ordered RESP fidelity | Duplicate payload/key/header fields and duplicate native IDs fail before ACK; malformed response errors expose only type/count/length and omit marker bytes | PASS |
| Native ID range | A legal ID above `i64::MAX` retains its exact cursor and returns `offset/timestamp=null`; malformed IDs fail | PASS |
| Runtime lag negotiation | Redis/Valkey/Dragonfly protocol versions >=7 advertise exact lag; KeyDB 6.3 advertises group+ACK but not lag, and a lag call returns unsupported | PASS |
| Public CLI | `--group` + `--consumer` + explicit `--ack none/on-success` reproduced PEL replay and `lag 1 -> 0`; omitting `--allow-write` failed before consume | PASS |
| Cleanup | Public Redis Stream deletion reported acknowledged/verified absent with four messages and `TYPE` returned `none` | PASS |

Portable representation checks cover binary payload/key preservation, exact
native IDs, reserved header collision, missing/duplicate payload and key,
duplicate/invalid UTF-8 headers, unknown fields, duplicate IDs, oversized
batches, and malformed RESP2/RESP3 shapes. All lossy cases fail before `XACK`.

Pub/Sub regression coverage confirms that payload-only stateless consumption
remains supported while group/durable identity and acknowledgement modes are
rejected.

The earlier CLI messaging profiles remain the baseline evidence for Stream and
Pub/Sub produce/consume/admin/delete behavior under pure and full-native
feature bundles. This run specifically closes the Redis consumer-group, PEL,
explicit-ACK, and complete-lag semantics that those profiles did not prove.

Cleanup: PASS; all `dbtool_it_stream_group_*` and
`dbtool_it_redis_stream_*` scans were empty on Redis, Valkey, KeyDB, and
Dragonfly after the matrix.

Commits: `e24fb79`, `acff12b`, `1279cbd`, `d2c88a2`, `b3b6e34`, IF-T48
