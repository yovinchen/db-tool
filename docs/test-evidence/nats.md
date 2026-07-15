# NATS Completeness Evidence

Task ID: DB-NATS-001

Result: LIVE_PASS (stateful consume, verified ACK, exact lag, cleanup)

Run at (UTC): 2026-07-15T21:34:48Z

Environment: Docker on macOS arm64; Rust 1.96.0; NATS with JetStream enabled

Product version: `nats:2.10-alpine` (`2.10.29`, server `/varz`)

Commands:

```text
cargo test -p adapter-nats --all-targets
cargo clippy -p adapter-nats --all-targets -- -D warnings
DBTOOL_IT_NATS_DSN=nats://127.0.0.1:14222 \
  cargo test -p adapter-nats --test live_stateful -- --nocapture
cargo run -q -p dbtool-cli --features portable -- \
  --dsn nats://127.0.0.1:14222 --format json caps
curl -sS http://127.0.0.1:18222/jsz?streams=true
curl -sS http://127.0.0.1:18222/varz
```

Resource operations:

| Resource | Create/write | Read all fixture data | Metadata/lag | Guard/bounds | Cleanup |
| --- | --- | --- | --- | --- | --- |
| Core queue group subject | direct live publish of binary `\0core\xff` PASS | explicit group subscriber received the exact bytes PASS | stable member identity and ACK rejected; Core NATS durable lag N/A PASS | max 1, timeout 3 s PASS | subscription exited; Core subject is ephemeral PASS |
| Stateful JetStream | three messages (binary, empty, text) stored PASS | `ack none` returned two exact messages, then replayed the same two native cursors after ACK wait PASS | lag `(0,3,3)` included two ACK-pending plus one server-pending; verified ACK changed it to `(2,3,1)`, then `(3,3,0)` PASS | complete batch converted before ACK; each ACK used `double_ack` within the shared deadline PASS | public adapter deletion verified stream absence PASS |
| Auto-created and incompatible durables | absent durable created strictly; separate `deliver=new` fixture created PASS | partially filled `max=10` batch returned all three retained messages PASS | exact durable/filter/deliver-all/explicit-ACK configuration and ACK floor 3 verified; incompatible consumer sequence and ACK floor stayed 0 PASS | final deadline budget remained available for three verified ACKs; incompatible durable was rejected and its `deliver=new` config remained unchanged PASS | parent stream deletion removed both durables PASS |
| Malformed-header JetStream | one message with two values for the same header stored PASS | both initial delivery and redelivery rejected as unrepresentable PASS | ACK floor stayed 0; ACK-pending stayed 1; redelivery count advanced PASS | conversion error occurred before the first ACK PASS | public adapter deletion verified stream absence PASS |

Offline result: 11 unit tests and 2 environment-gated integration targets PASS;
Clippy PASS with `-D warnings`. Live result: 2/2 tests PASS in 1.02 s.
Portable CLI `caps` reported the exact `message.consume_group`,
`message.consume_durable`, and `message.consume_ack` operations PASS.

Post-cleanup `/jsz?streams=true` reported `streams=0`, `consumers=0`,
`messages=0`, and `bytes=0`. JetStream durable semantics are not attributed to
Core NATS queue groups.

Cleanup: PASS

Compatibility gaps: NATS 2.10.29 standalone was tested. Stateful TLS,
clustered/leaf-node behavior, ACL-denied strict creation, a concurrent creator
race, and a fault injected between verified ACKs were not rerun in this slice.
