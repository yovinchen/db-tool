# RabbitMQ And AMQP Completeness Evidence

Task ID: DB-RABBITMQ-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T20:45:08Z

Environment: Docker on macOS arm64; Rust 1.96.0; single RabbitMQ node and isolated `dbtool_it` vhost

Product version: `rabbitmq:3.13-management-alpine`

Command: `./scripts/integration-mq-test.sh`; `./scripts/integration-mq-native-test.sh`

Resource operations:

| Resource | Create/write | Read all fixture data | Metadata/lag | Guard/ack/bounds | Cleanup |
| --- | --- | --- | --- | --- | --- |
| `dbtool_it_amqp_queue_*` | queue declaration and one `amqp-payload` publish PASS | bounded `basic.get` returned exact payload PASS | passive detail reported name/count 1; portable list/lag explicitly UNSUPPORTED PASS | write guard PASS; publisher confirms enabled and only Ack counts; consume acknowledged; max 1/timeout 5 PASS | queue drained to message_count 0; public queue delete UNSUPPORTED; volume removed PASS |
| `dbtool_it_rabbitmq_mgmt_queue_*` | AMQP declaration and one `rabbitmq-management-payload` publish PASS | exact payload consumed and acknowledged PASS | `rabbitmq+http` list/detail/lag reported count/latest/lag 1 before consume and 0 after consume PASS | management connector read-only; AMQP write guard and bounds PASS | queue drained; disposable RabbitMQ volume removed PASS |

Raw AMQP does not pretend to list queues or expose consumer-group lag. Queue
discovery and queue-depth lag are available only through the explicit RabbitMQ
management HTTP connector.

Cleanup: PASS by queue drain and disposable Docker volume; protocol-level queue delete is UNSUPPORTED

Commits: `e24fb79`, `acff12b`, `1279cbd`, this commit
