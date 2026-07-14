# Redis Messaging Completeness Evidence

Task ID: DB-REDIS-MQ-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T20:45:08Z

Environment: Docker on macOS arm64; Rust 1.96.0; `redis:7-alpine`

Product version: Redis 7 rolling Alpine image used by the messaging profile

Command: `./scripts/integration-mq-test.sh`; `./scripts/integration-mq-native-test.sh`

Resource operations:

| Resource | Create/write | Read all fixture data | Metadata/lag | Guard/bounds | Cleanup |
| --- | --- | --- | --- | --- | --- |
| `dbtool_it_redis_stream_*` | one Stream entry with payload `redis-stream-payload` PASS | bounded consume returned exactly that payload and a Redis stream ID PASS | topics/detail reported stream kind and length 1; `dbtool_it_redis_group_*` reported lag at least 1 PASS | produce without `--allow-write` blocked; consume used max 1 and timeout 5 PASS | `XGROUP DESTROY=1`, `DEL=1`, GET null, and topics absence PASS |
| `pubsub:dbtool.it.redis.channel.*` | live publish of `redis-pubsub-payload` PASS | the already-running bounded subscriber received exactly one matching payload PASS | durable catalog N/A for Pub/Sub | publish guard, max 1, and timeout 5 PASS | channel is ephemeral; subscriber process exited PASS |

Assertions: the same Redis messaging workflow passed under both the pure and
full-native feature bundles, so native Kafka selection did not regress the
shared Redis protocol path.

Cleanup: PASS

Commits: `e24fb79`, `acff12b`, `1279cbd`, `d2c88a2`
