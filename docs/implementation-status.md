# dbtool Implementation Status

Last updated: 2026-07-16

This document is the current implementation inventory for dbtool. It separates
implemented behavior, compatibility that has been live-tested, compatibility
that is only routed through an existing adapter, and features that are still not
usable.

## Overall Status

| Area | Status | Notes |
| --- | --- | --- |
| Core contracts | Implemented | `Connector`, capability traits, shared models, registry, DSN parsing, redaction, and protocol aliases are in place. |
| Embedded library path | Implemented | `dbtool-registry` has a service-free embedded smoke that builds the registry directly, reuses a connection through `ConnectionManager`, applies `SafetyGuard`, and runs SQL under `FlowControl` without spawning the CLI. |
| CLI | Implemented | `ping`, `caps`, `conn`, `sql`, `cql`, `db2`, `kv`, `doc`, `mq`, `search`, and `ts` command families exist with default backends for core read paths. Root and command-family help describe safety boundaries, JSON inputs, bounded reads, and examples. |
| Output formats | Implemented | JSON is the default. `--format table` and `--format ndjson` are implemented for successful command output; errors always stay JSON so `error.code` and confirmation tokens remain machine-readable. |
| SQL safety | Implemented | Query ASTs are recursively classified; data-modifying CTEs fail closed, `SELECT INTO` is destructive, locking SELECT is a write, all writes need `--allow-write`, and destructive SQL additionally needs a target-bound confirm token. Database least-privilege roles remain the final boundary for side-effectful vendor functions. |
| Flow control | Implemented | Core `FlowControl` covers per-process concurrency, optional token-bucket rate limiting, acquire timeout, request timeout, shared overall deadline, and retry budget. CLI data commands load `[defaults.limits]` and named-connection overrides from `connections.toml`, then apply CLI overrides such as `--rate`, `--request-timeout`, and `--deadline`; CLI execution uses the one-shot path so writes are not replayed by retries. |
| Docker integration | Implemented | Base databases, fixture-image databases, compatibility databases, SQL Server, Cassandra, TiDB, TiDB secure HA, messaging, messaging TLS, observability, OpenSearch security-plugin TLS, and product-native Elasticsearch profiles are available. A Dockerfile-backed dbtool CLI runtime image can be smoke-tested with the same SQLite core flow. |
| CI | Implemented | Service-free verification runs by default; feature-matrix gates prove minimal/default/portable/full/full-native composition and pure/native Kafka exclusivity; live Docker jobs are manual workflow inputs. |
| Release artifacts | Implemented | Tags must equal the Cargo workspace version. Six-platform `portable` binaries contain every self-contained adapter while excluding host-ODBC Db2 and native Kafka; archive/npm/wheel packaging accepts only target-specific binaries, preflights every selected target before writing output, includes generated completions/manpage, enforces executable permissions, install-smokes the host package, and attaches all artifacts to GitHub Release. |
| TUI | Implemented | Connection picker, capability-aware command dispatch, read limits, AST-based SQL write classification, readonly/one-shot confirmation, command history, per-capability forms, and RAII terminal restoration are covered by smoke and failure-path tests. |

## Usable Database And Protocol Matrix

| Backend | DSN schemes | Adapter | Usable operations | Verification |
| --- | --- | --- | --- | --- |
| SQLite | `sqlite:` | SQL | `ping`, adapter-bounded parameterized query, exec, tables/schema; scalar/bytes/timestamp/JSON binding; atomic artifact import | In-memory 10,000-row bounded stream, exact-limit, injection-safe import and late-constraint full rollback tests |
| PostgreSQL | `postgres://` | SQL | adapter-bounded parameterized query, exec/tables/schema with write safety; scalar/bytes/timestamptz/jsonb binding; atomic artifact import | PostgreSQL 16.14 Docker 10,000-row bounded stream, full parameter lifecycle and whole-batch rollback |
| PostgreSQL alias | `postgresql://` | SQL | Same as Postgres adapter | Registry alias test only |
| CockroachDB | `cockroach://` | SQL | Postgres-family SQL lifecycle, typed values, result limiting, table listing, schema inspection | Real CockroachDB compatibility live test |
| TimescaleDB | `timescale://` | SQL | Postgres-family SQL lifecycle, typed values, result limiting, table listing, schema inspection | Real TimescaleDB compatibility live test |
| Redshift | `redshift://` | SQL | Routed to Postgres adapter | Env-gated external Redshift smoke through `./scripts/integration-redshift-test.sh` when `DBTOOL_IT_REDSHIFT_DSN` is supplied |
| IBM Db2 | `db2://` | Db2 ODBC | `ping`, `sql query`, `sql exec`, `sql tables`, `sql schema`, `db2 schemas`, `db2 tables`, `db2 schema`, `db2 sequences`, `db2 routines`, `db2 tablespaces`, `db2 foreign-keys`, `db2 ddl` | Service-free adapter tests; live integration guarded by `DBTOOL_RUN_DB2_INTEGRATION=1` |
| IBM Db2 alias | `ibmdb2://`, `as400://` | Db2 ODBC | Same as Db2 adapter | Registry alias test only |
| SQL Server | `sqlserver://`, `mssql://` | SQL Server/TDS | SQL query/exec/tables/schema, typed scalar values, result limiting | Service-free adapter tests plus real SQL Server Docker live test on GitHub Actions x86_64 runner |
| Cassandra/ScyllaDB | `cassandra://`, `scylla://` | CQL | `ping`, page-bounded `cql query`, `cql exec`, `cql keyspaces`, `cql tables`, `cql schema`, SQL-compatible CQL path, primitive/collection typed values | Cassandra 5.0.8 Docker paged limit+1/exact-limit tests plus lifecycle |
| MySQL | `mysql://` | SQL | adapter-bounded parameterized query, exec/tables/schema; scalar/bytes/datetime/json binding; transactional-engine atomic artifact import | MySQL 8.4.9 Docker recursive large-result bound, full parameter/rollback lifecycle, and zero-write MyISAM rejection |
| MariaDB | `mariadb://` | SQL | MySQL-family SQL lifecycle, typed values, result limiting | Real MariaDB compatibility live test |
| TiDB | `tidb://` | SQL | MySQL-family SQL lifecycle, typed values, table listing, schema-qualified tables | Real PD/TiKV/TiDB live test |
| TiDB secure HA | `tidb://` with TLS params | SQL | SQL TLS, component TLS, `REQUIRE SSL`, `REQUIRE X509`, insecure-login rejection, two SQL-node lifecycle | Real 3 PD + 2 TiKV + 2 TiDB live test |
| TiDB PD quorum | `tidb://` with TLS params | SQL | Both SQL nodes continue TLS writes/reads while one PD node is stopped at a time | Local secure HA PD drill |
| TiDB PD leader | `tidb://` with TLS params | SQL | Current PD leader is discovered through the TLS PD API, stopped, replaced, and both SQL nodes continue TLS writes/reads | Local secure HA PD leader drill |
| TiDB TiKV outage boundary | `tidb://` with TLS params | SQL | One local TiKV service is stopped and dbtool SQL probes must either continue successfully or fail within bounded request/deadline time | Local secure HA TiKV outage boundary drill |
| TiDB certificate regeneration | `tidb://` with regenerated TLS params | SQL | Cold-restart secure HA after regenerated CA/server/client certificates, with TLS writes/reads through both generations | Local secure HA certificate regeneration drill |
| TiDB logical roundtrip | `tidb://` with TLS params | SQL | Rows are exported through one secure SQL node, restored through the other node, and read back from both nodes | Local secure HA logical roundtrip smoke |
| TiDB TiProxy | `tidb://` through TiProxy TLS port | SQL | TLS proxy entrypoint, `REQUIRE SSL` user, SQL lifecycle, new-connection routing while either TiDB SQL node is stopped | Opt-in TiProxy Docker drill |
| Redis | `redis://` | Redis | typed binary/text KV get/set, delete, exact scan, fail-closed bounded raw, TTL, Streams, Pub/Sub | Redis 7.4.9 Docker binary/empty/text/missing fidelity, raw confirmation/denylist, exact N/N+1 truncation, 25-key multi-page SCAN, strict non-UTF-8 error, transfer and messaging tests |
| Valkey | `valkey://` | Redis | Redis-compatible KV lifecycle, TTL, raw write guard | Real Valkey compatibility live test |
| KeyDB | `keydb://` | Redis | Redis-compatible KV lifecycle, TTL, raw write guard | Optional real KeyDB live test with `DBTOOL_IT_COMPAT_EXTRA=1` |
| Dragonfly | `dragonfly://` | Redis | Redis-compatible KV lifecycle, TTL, raw write guard | Optional real Dragonfly live test with `DBTOOL_IT_COMPAT_EXTRA=1` |
| MongoDB | `mongodb://` | MongoDB | collections, find（skip/sort/projection）, insert, explicit update-one/update-many/delete-one/delete-many, bounded aggregate, drop collection | MongoDB 7 Docker exact-cardinality lifecycle, target/content-bound bulk confirmation, and zero-residual cleanup |
| Kafka | `kafka://` | Kafka | ping, produce/consume with key, headers, partition, exact offset/timestamp/cursor, topics, detail/watermarks, confirmed topic delete; pure backend lag explicitly unsupported | Redpanda pure field-fidelity, cursor replay and deletion live tests |
| Kafka native | `kafka://` with `full-native` | Kafka | librdkafka-backed field-fidelity/cursor stateless consume, dynamic group subscription, explicit ack-none replay or whole-batch on-success offset commit, topics/detail/delete, and real committed-offset lag; ephemeral calls reject static member identity | Native two-partition replay/commit/zero-lag/delete lifecycle plus field fidelity and lag tests passed against Redpanda |
| Redpanda | `redpanda://` | Kafka | Kafka-compatible field-fidelity lifecycle through product-named scheme | Real Redpanda pure/native live tests |
| AutoMQ | `automq://` | Kafka | Routed to Kafka adapter; native backend accepts DSN-supplied SASL/TLS params | Env-gated external vendor smoke through `./scripts/integration-kafka-vendor-test.sh` when `DBTOOL_IT_AUTOMQ_DSN` is supplied |
| WarpStream | `warpstream://` | Kafka | Routed to Kafka adapter; native backend accepts DSN-supplied SASL/TLS params | Env-gated external vendor smoke through `./scripts/integration-kafka-vendor-test.sh` when `DBTOOL_IT_WARPSTREAM_DSN` is supplied |
| Confluent | `confluent://` | Kafka | Routed to Kafka adapter; native backend accepts DSN-supplied SASL/TLS params | Env-gated external vendor smoke through `./scripts/integration-kafka-vendor-test.sh` when `DBTOOL_IT_CONFLUENT_DSN` is supplied |
| AMQP/RabbitMQ | `amqp://`, `amqps://` | AMQP | produce, ACKing bounded consume, queue detail, confirmed conditional queue delete, native delivery diagnostics | RabbitMQ plain and AMQPS TLS live tests |
| RabbitMQ management | `rabbitmq+http://` | RabbitMQ HTTP admin | queue list, exact fail-closed detail and confirmed conditional delete; consumer-group lag explicitly not relabeled from depth | RabbitMQ 3.13 management production/detail/consume/delete/absence live test with zero queues left |
| NATS | `nats://`, `nats+tls://` | NATS | publish, subscribe, exact JetStream cursor, topics/detail/lag and confirmed stream delete | NATS plain/TLS cursor and lifecycle live tests |
| OpenSearch | `opensearch://`, `opensearch+https://` | Search HTTP/HTTPS | index list/search/aggregations, auto-ID index, stable-ID put/get/update/delete, confirmed delete-index, hard limit/pagination | OpenSearch 2.17.1 full CRUD with exact metadata and zero residual indices; HTTPS fixture and security-plugin transport/auth evidence retained |
| Elasticsearch | `elasticsearch://`, `elasticsearch+https://` | Search HTTP/HTTPS | OpenSearch-compatible list/search/aggregations and full document/index lifecycle | Elasticsearch 8.15.5 full CRUD with exact metadata and zero residual indices; product-native HTTPS remains explicit boundary |
| Prometheus | `prometheus://`, `prometheus+http://` | Time series HTTP | metric list, bounded range query with recent-minutes or explicit epoch-ms bounds, and remote write | Exact two-series tagged/timestamped remote-write readback and explicit start/end range against Prometheus 2.55.1 |

## Docker Service Profiles

| Script | Services | Main coverage | Resource note |
| --- | --- | --- | --- |
| `./scripts/integration-db-suite.sh` | Selectable local DB suite | Orchestrates Compose config validation, service-free checks, base DB workflows, flow-control, database-side SQL timeout checks, live connection config, custom environment smoke, fixture data/images, logical roundtrip, compatibility profiles, TiDB, and opt-in heavy DB/protocol phases | Default excludes heavy phases; `DBTOOL_IT_DB_SUITE_PHASES=all` includes every declared DB, messaging, observability, search, and external-endpoint phase |
| `./scripts/integration-test.sh` | Postgres, MySQL, Redis, MongoDB | Canonical SQL, KV, and document workflows plus PostgreSQL/MySQL adapter-level large-result bounds | Roughly 2 GiB container memory |
| `./scripts/integration-flow-control-test.sh` | Postgres, MySQL, Redis, MongoDB | Live request timeout, rate/admission flags, SQL/KV/document result limiting, and disposable fixture cleanup | Roughly 2 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-server-timeout-test.sh` | Postgres, MySQL | Database-side SQL timeout checks for PostgreSQL `statement_timeout`, PostgreSQL `idle_in_transaction_session_timeout`, PostgreSQL `lock_timeout`, and MySQL `innodb_lock_wait_timeout` | Roughly 1.25 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-connection-config-test.sh` | Postgres, MySQL, Redis, MongoDB | Temporary `connections.toml` named connections for SQL/KV/document workflows plus connection-level request timeout | Roughly 2 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-custom-env-test.sh` | Postgres, MySQL, Redis, MongoDB | Custom project name, database names, credentials, host ports, generated DSNs, and read/write verification | Roughly 2 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-fixture-data-test.sh` | Postgres, MySQL, Redis, MongoDB | File-backed reusable fixture loading for SQL rows, Redis keys, and MongoDB documents | Roughly 2 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-fixture-images-test.sh` | Dockerfile-built Postgres, MySQL, Redis, MongoDB | Fixture data baked into database images and verified through dbtool readback | Roughly 2 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-data-roundtrip-test.sh` | Postgres, MySQL, Redis, MongoDB | Public `dbtool export` / `dbtool import` logical roundtrip of fixture rows, keys, and documents into independent target resources | Roughly 2 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-compat-test.sh` | MariaDB, Valkey | MySQL and Redis compatible databases | Extra KeyDB/Dragonfly via `DBTOOL_IT_COMPAT_EXTRA=1` |
| `./scripts/integration-pg-compat-test.sh` | CockroachDB, TimescaleDB | PostgreSQL-family compatible databases | Roughly 1 GiB container memory |
| `./scripts/integration-redshift-test.sh` | Externally supplied Redshift endpoint | Env-gated SQL lifecycle, typed values, result limiting, table listing, and schema inspection; no secrets are committed | Skips when `DBTOOL_IT_REDSHIFT_DSN` is not supplied |
| `./scripts/integration-sqlserver-test.sh` | SQL Server | TDS SQL lifecycle, typed values, limiting, tables, and schema | Passed on GitHub Actions x86_64 runner; requires amd64-capable Docker locally; roughly 2 GiB container memory |
| `./scripts/integration-cassandra-test.sh` | Cassandra | CQL lifecycle, keyspace-qualified tables, schema inspection, typed values, paged limit+1 and exact-limit reads | Roughly 2 GiB container memory; startup can be slow |
| `./scripts/integration-cassandra-fixture-data-test.sh` | Cassandra | File-backed reusable CQL fixture loading, seeded row readback, table listing, and schema inspection | Roughly 2 GiB container memory; heavy/local-only while CI budget is frozen |
| `./scripts/integration-tidb-test.sh` | PD, TiKV, TiDB | Real TiDB compatibility | Roughly 1.75 GiB container memory |
| `./scripts/integration-tidb-secure-test.sh` | 3 PD, 2 TiKV, 2 TiDB SQL | TiDB auth/TLS/local HA | Roughly 3.75 GiB container memory |
| `./scripts/integration-tidb-ha-drill.sh` | 3 PD, 2 TiKV, 2 TiDB SQL | TiDB secure HA SQL-node failover with one SQL node stopped at a time | Roughly 3.75 GiB container memory |
| `./scripts/integration-tidb-pd-drill.sh` | 3 PD, 2 TiKV, 2 TiDB SQL | TiDB secure HA PD quorum continuity with one PD node stopped at a time | Roughly 3.75 GiB container memory |
| `./scripts/integration-tidb-pd-leader-drill.sh` | 3 PD, 2 TiKV, 2 TiDB SQL | TiDB secure HA continuity after stopping the discovered current PD leader | Roughly 3.75 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-tidb-tikv-outage-boundary.sh` | 3 PD, 2 TiKV, 2 TiDB SQL | TiDB secure HA behavior boundary after stopping one local TiKV service, with bounded dbtool probes | Roughly 3.75 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-tidb-cert-regeneration-test.sh` | 3 PD, 2 TiKV, 2 TiDB SQL | TiDB secure HA cold restart after regenerated CA/server/client certificates | Roughly 3.75 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-tidb-logical-roundtrip-test.sh` | 3 PD, 2 TiKV, 2 TiDB SQL | TiDB secure HA logical export/restore across the two TLS SQL nodes with cross-node readback | Roughly 3.75 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-tidb-tiproxy-test.sh` | 3 PD, 2 TiKV, 2 TiDB SQL, TiProxy | TiProxy TLS entrypoint and new-connection routing while one SQL node is stopped | Roughly 4 GiB container memory |
| `./scripts/integration-mq-test.sh` | Redis, Redpanda, RabbitMQ, NATS | Streams/PubSub, Kafka, AMQP, NATS | Roughly 2 GiB container memory |
| `./scripts/integration-mq-tls-test.sh` | RabbitMQ TLS, NATS TLS | AMQPS and NATS TLS aliases | Roughly 768 MiB container memory |
| `./scripts/integration-mq-native-test.sh` | Redis, Redpanda, RabbitMQ, NATS | Native Kafka backend plus messaging regression | Requires `full-native` build |
| `./scripts/integration-kafka-vendor-test.sh` | Externally supplied AutoMQ, WarpStream, Confluent endpoints | Env-gated native Kafka smoke for ping, topics, produce, detail, and consume; no secrets are committed | Skips when no vendor DSN env vars are supplied |
| `./scripts/integration-observability-test.sh` | OpenSearch, Dockerfile-built OpenSearch-compatible HTTPS harness, Prometheus | Search, seeded search TLS transport, and time-series workflows | Roughly 1.4 GiB container memory |
| `./scripts/integration-opensearch-security-test.sh` | OpenSearch security plugin | Real OpenSearch HTTPS/basic-auth with generated local CA/node certs and `tls-ca` validation | Roughly 1.5 GiB container memory; opt-in heavy/local-only while CI budget is frozen |
| `./scripts/integration-elasticsearch-test.sh` | Elasticsearch | Product-native `elasticsearch://` ping/caps, write guard, exact three-document indexing/readback, pagination, limit, truncation, and index listing | Roughly 1.5 GiB container memory; opt-in heavy/local-only while CI budget is frozen |
| `./scripts/integration-db2-test.sh` | IBM Db2 Community Edition | SQL lifecycle, schema inspection, write guard, alias verification, `db2` subcommand (sequences, routines, tablespaces, foreign-keys, ddl) | Requires IBM Data Server Driver for ODBC at runtime; roughly 4 GiB container memory; startup up to 10 min; opt-in heavy/local-only |

## Implemented CLI Operations

| Command family | Implemented commands | Write guard |
| --- | --- | --- |
| Connection | `conn list`, `conn add`, `conn remove` | list is read-only; add requires `--allow-write`; replace/remove also require a config-path/name/content-bound confirmation; file writes use same-directory 0600 temp + atomic replacement and never expose raw DSNs |
| General | `ping`, `caps` | Read-only |
| SQL | `sql query`, `sql exec`, `sql tables`, `sql schema`, `sql schemas` | query/exec accept `--params JSON_ARRAY`; query is adapter-bounded by positive `--limit` and strictly read-only; writes use `sql exec` with `--allow-write` and sometimes `--confirm`; the dead `sql query --schema` option has been removed, while table metadata uses reusable schema-qualified identities and exact list truncation |
| CQL | `cql query`, `cql exec`, `cql keyspaces`, `cql tables`, `cql schema` | query is adapter-page-bounded by positive `--limit`; `cql exec` requires `--allow-write` |
| Db2 | `db2 schemas`, `db2 tables`, `db2 schema`, `db2 sequences`, `db2 routines`, `db2 tablespaces`, `db2 foreign-keys`, `db2 ddl` | All read-only; no `--allow-write` required |
| KV | `kv get`, `kv set`, `kv scan`, `kv del`, `kv raw` | get exposes compatible UTF-8 plus typed exact bytes; set accepts text or canonical base64; set/del require `--allow-write`; every allowlisted raw mutation additionally requires argument/target-bound confirmation; raw reads are fail-closed and byte/item-bounded; scan performs an adapter-level N+1 probe, deduplicates pages, propagates page/decode errors, and marks truncation only when an extra key exists |
| Document | `doc collections`, `doc find`, `doc insert`, `doc update`, `doc delete`, `doc aggregate`, `doc drop` | find exposes skip/sort/projection and exact truncation; insert/update/delete require `--allow-write`; update/delete default to one match and reject empty/non-object filters; `--many` requires a connection/collection/operation/filter/content-bound confirmation; drop requires target-bound confirmation |
| Transfer | `export sql`, `export kv`, `export doc`, `import sql`, `import kv`, `import doc` | All exports are read-only and adapter-bounded; current artifacts retain typed values and explicit completeness metadata, while partial/legacy/inconsistent, over-256-MiB, or over-`--limit` artifacts fail before target connection. KV completeness currently covers key/value, not native per-key TTL; import supports one explicit replacement TTL. Imports require `--allow-write`; KV replacement additionally requires `--replace-existing` plus target/content/TTL-bound confirmation. SQLite/PostgreSQL/MySQL SQL import requires `sql.insert_rows_atomic`, binds every value and reports `atomic=true` only after one whole-batch transaction commits; other SQL adapters reject it. Generic KV/document imports remain `atomic=false`. |
| Messaging | `mq produce`, `mq consume`, `mq topics`, `mq detail`, `mq lag`, `mq delete` | produce requires `--allow-write`; consume supports typed stateless/group/durable identity and explicit none/on-success ACK, with stateful/ACK modes write-gated and method-level negotiated; cursor remains native/inclusive for stateless reads; AMQP requires explicit on-success; persistent-resource delete requires a token bound to kind/name/options; reaching max is a budget signal, not proof of another message |
| Search | `search indices`, `search search`, `search index`, `search put`, `search get`, `search update`, `search delete`, `search delete-index` | all mutations require `--allow-write`; `delete-index` additionally requires a target-bound `--confirm` token |
| Time series | `ts measurements`, `ts query [--last-minutes N | --start-ms MS --end-ms MS]`, `ts write` | query validates range pairing/order and a 1..=1,000,000 sample budget before connecting; Prometheus remote write requires `--allow-write` |

## Explicit Boundaries

These are protocol or product-specific boundaries rather than missing pieces of
the stated dbtool objective.

| Item | Boundary | Reason |
| --- | --- | --- |
| Production TiDB HA | Outside the local dbtool harness | Local secure HA topology, SQL-node failover, PD single-node outage, PD leader outage, TiKV outage boundary, certificate regeneration cold restart, logical roundtrip, and TiProxy new-connection routing are covered. Production TiKV failover, online certificate rotation, product-native backup/restore, and upgrade drills remain product-readiness exercises beyond dbtool's connector objective. |
| AMQP queue listing over pure AMQP | Not portable in AMQP 0.9.1 | RabbitMQ queue discovery is intentionally exposed through `rabbitmq+http://` management instead of pretending queue listing is portable AMQP behavior. |
| Redis Pub/Sub durable listing | Not a durable catalog | Pub/Sub channels are live subscriptions, not durable topics; durable list/detail semantics stay on Redis Streams. |
| NATS core subject listing | Not a durable catalog | Core NATS subjects are ephemeral routing names; durable list/detail/lag semantics stay on JetStream. |
| IBM Db2 end-to-end live Docker run | Requires IBM ODBC runtime at the OS level | The adapter, Docker Compose profile, and integration script exist. Running them requires IBM Data Server Driver for ODBC installed outside the container — this is an explicit runtime boundary analogous to Redshift needing a supplied external endpoint. Service-free adapter tests pass in all environments. |

## Active Verification Work

The implementation surface is broad, but real-product completeness is now
tracked separately in `docs/db-completeness-tasks.md`. A connector or test
script being present does not by itself mean that a product completed CRUD or
the equivalent family checklist. External DSN skips, compatible aliases, and
missing host runtimes remain explicit non-pass states in
`testdata/db-completeness.manifest`.

Design-only candidates such as Oracle, etcd, InfluxDB, VictoriaMetrics, Pulsar,
MQTT, and RocketMQ do not have registered factories and are not listed as
implemented backends.

## Completion Evidence

`docs/final-goal-audit.md` maps the final objective to concrete evidence, and
`./scripts/validate-final-goal.sh` verifies the repo-level completion evidence
without starting Docker.
