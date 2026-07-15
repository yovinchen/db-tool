# RabbitMQ And AMQP Completeness Evidence

Task ID: DB-RABBITMQ-001

Result: LIVE_PASS

Run at (UTC): 2026-07-15T19:55:00Z (focused management/delete refresh; original full-family run 2026-07-14T20:45:08Z)

Environment: Docker on macOS arm64; Rust 1.96.0; single RabbitMQ node and isolated `dbtool_it` vhost

Product version: `rabbitmq:3.13-management-alpine`

Command: `./scripts/integration-mq-test.sh`; `./scripts/integration-mq-native-test.sh`; focused `rabbitmq_management_live_lists_details_and_deletes_queues` with isolated full-feature target directory

Resource operations:

| Resource | Create/write | Read all fixture data | Metadata/lag | Guard/ack/bounds | Cleanup |
| --- | --- | --- | --- | --- | --- |
| `dbtool_it_amqp_queue_*` | queue declaration and one `amqp-payload` publish PASS | bounded `basic.get` returned exact payload PASS | passive detail reported name/count 1; portable list/consumer-group lag explicitly UNSUPPORTED PASS | write guard PASS; publisher confirms enabled and only Ack counts; consume acknowledged; max 1/timeout 5 PASS | queue drained and public confirmed AMQP queue delete verified absence PASS |
| `dbtool_it_rabbitmq_mgmt_queue_*` | AMQP declaration and one `rabbitmq-management-payload` publish PASS | exact payload consumed and acknowledged PASS | `rabbitmq+http` list and exact detail reported count 1 then 0; group lag correctly UNSUPPORTED PASS | management counts waited for a complete snapshot; missing/invalid/overflow fail closed; delete token bound kind/name/if-empty/if-unused PASS | `if-empty` delete acknowledged, subsequent detail was absent, final management queue list `[]` PASS |

Raw AMQP does not pretend to list queues or expose consumer-group lag. Queue
discovery and exact queue-depth detail are available only through the explicit
RabbitMQ management HTTP connector. Queue depth is not exposed as
consumer-group lag.

Cleanup: PASS; focused management API final queue list was `[]`

Commits: `e24fb79`, `acff12b`, `1279cbd`, `d2c88a2`, IF-T47, IF-T59
