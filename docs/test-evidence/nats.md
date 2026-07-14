# NATS Completeness Evidence

Task ID: DB-NATS-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T20:45:08Z

Environment: Docker on macOS arm64; Rust 1.96.0; NATS with JetStream enabled

Product version: `nats:2.10-alpine`

Command: `./scripts/integration-mq-test.sh`; `./scripts/integration-mq-native-test.sh`

Resource operations:

| Resource | Create/write | Read all fixture data | Metadata/lag | Guard/bounds | Cleanup |
| --- | --- | --- | --- | --- | --- |
| `dbtool.it.nats.subject.*` | live core publish of `nats-payload` PASS | active subscriber received exactly one payload PASS | core subjects have no durable catalog N/A | write guard, max 1, timeout 5 PASS | subject ephemeral and subscriber exited PASS |
| `DBTOOL_IT_NATS_STREAM_*` | JetStream stream, subject, durable consumer, and acknowledged `nats-jetstream-payload` publish PASS | exact stored message represented by stream count 1 PASS | topics/detail reported stream kind, one message, one consumer; durable lag 1 PASS | publish acknowledgement and bounded admin calls PASS | delete_stream succeeded and topics no longer listed the stream PASS |

The workflow passed with both pure and full-native bundles. JetStream admin
semantics are not attributed to Core NATS subject discovery.

Cleanup: PASS

Commits: `acff12b`, `1279cbd`, `d2c88a2`
