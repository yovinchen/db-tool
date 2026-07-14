# TLS Messaging Completeness Evidence

Task ID: DB-MQ-TLS-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T20:45:08Z

Environment: Docker on macOS arm64; Rust 1.96.0; generated local CA; RabbitMQ TLS and NATS TLS

Product version: `rabbitmq:3.13-management-alpine`; `nats:2.10-alpine`

Command: `./scripts/integration-mq-tls-test.sh`

Resource operations:

| Resource | Create/write | Read all fixture data | Metadata/lag | Guard/TLS/bounds | Cleanup |
| --- | --- | --- | --- | --- | --- |
| `dbtool_it_amqps_queue_*` | AMQPS queue and acknowledged `amqps-payload` publish PASS | exact payload consumed and acknowledged PASS | passive detail count 1; raw AMQPS topics/lag explicitly UNSUPPORTED PASS | write guard, CA verification, max 1/timeout 5 PASS | queue drained to count 0; disposable volume removed PASS |
| `dbtool.it.nats.tls.subject.*` | NATS TLS core publish of `nats-tls-payload` PASS | active TLS subscriber received exact payload PASS | durable metadata N/A for core subject | write guard, CA verification, max 1/timeout 5 PASS | ephemeral subject and subscriber exit PASS |
| `DBTOOL_IT_NATS_TLS_STREAM_*` | TLS JetStream stream, durable consumer, and acknowledged `nats-tls-jetstream-payload` PASS | detail reported one stored message PASS | topics/detail/lag reported one stream, consumer, and lag 1 PASS | authenticated TLS transport and bounded calls PASS | delete_stream succeeded and topics absence verified PASS |

The first run detected expired cached certificates. The preparation script now
renews the complete CA/server set when stale or incomplete; the regenerated
certificates passed OpenSSL validity checks and the live suite passed 2/2.

Cleanup: PASS

Commits: `ad8d2da`, `acff12b`, `1279cbd`, this commit
