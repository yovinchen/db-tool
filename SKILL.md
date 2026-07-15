# dbtool Claude Skill

Use dbtool when you need to inspect SQL databases, key-value stores, document stores, search indexes, or message systems through a machine-readable CLI. Prefer this tool for workflows that need stable JSON, safety checks, and named connection resolution.

## Operating Flow

1. Inspect capabilities first:

   ```bash
   dbtool --conn <name> caps
   dbtool --dsn sqlite::memory: caps
   ```

2. Inspect structure before querying data:

   ```bash
   dbtool --conn <name> sql schemas
   dbtool --conn <name> sql tables
   dbtool --conn <name> sql schema users
   dbtool --conn search-local search indices
   dbtool --conn prometheus-local ts measurements
   ```

3. Query with explicit limits:

   ```bash
   dbtool --conn <name> --limit 50 sql query "select id, name from users"
   dbtool --conn search-local --limit 10 search search users --q '{"match_all":{}}'
   dbtool --conn prometheus-local ts query up --last-minutes 10
   ```

4. Use protocol-specific escape hatches only when the typed command is not enough:

   ```bash
   dbtool --conn redis-local kv raw XLEN mystream
   dbtool --conn redis-local --allow-write kv raw SET key value
   ```

## Output Contract

Default CLI output is JSON. The envelope is stable:

```json
{
  "ok": true,
  "kind": "sqlite",
  "data": {},
  "meta": {
    "elapsed_ms": 0,
    "truncated": false
  }
}
```

On failure, read `error.code` before interpreting the message. Useful codes include `UNSUPPORTED_CAPABILITY`, `WRITE_NOT_ALLOWED`, and `CONFIRM_REQUIRED`.

For human inspection or pipelines, request an alternate successful-output format:

```bash
dbtool --conn <name> --format table sql query "select id, name from users"
dbtool --conn <name> --format ndjson sql query "select id, name from users"
```

## Safety Workflow

Reads are allowed by default. Ordinary writes require `--allow-write`.

Destructive SQL such as `DROP`, `TRUNCATE`, `ALTER`, `CREATE`, or `DELETE` without `WHERE` uses a non-interactive two-step confirmation:

1. First call with `--allow-write` and no `--confirm`. dbtool refuses execution and returns `error.confirm_token` plus `error.impact`.
2. Review `error.impact`.
3. Repeat the exact same command with `--confirm <token>`.

The token is bound to the normalized statement, target connection, and impact summary. Do not reuse it for another connection or another statement.

Example:

```bash
dbtool --dsn sqlite::memory: --allow-write sql exec "create table users (id integer)"
dbtool --dsn sqlite::memory: --allow-write --confirm <token> sql exec "create table users (id integer)"
dbtool --dsn opensearch://127.0.0.1:9200 --allow-write search index users '{"name":"alice"}'
```

## Connection Resolution

Use `--dsn` for an explicit one-off target. Use `--conn <name>` for configured targets. Named connections resolve through:

1. `DBTOOL_CONN_<NAME>`
2. `connections.toml`

DSNs may reference environment variables as `${VAR}`. Never print raw DSNs with credentials; use redacted output from `conn list`. `conn add` requires `--allow-write`; replacing an existing file entry or `conn remove` additionally uses the returned target/content-bound confirmation token. Environment-managed `DBTOOL_CONN_*` entries cannot be changed by these commands.

## Local Integration Runs

For end-to-end verification with real services, use:

```bash
./scripts/integration-test.sh
```

For real messaging backends, use:

```bash
./scripts/integration-mq-test.sh
```

For real search and time-series backends, use:

```bash
./scripts/integration-observability-test.sh
```

For the heavier TiDB compatibility profile, use:

```bash
./scripts/integration-tidb-test.sh
```

For TiDB auth/TLS/HA coverage, use:

```bash
./scripts/integration-tidb-secure-test.sh
```

Override names and ports with `DBTOOL_IT_PROJECT`, `DBTOOL_IT_POSTGRES_DB`, `DBTOOL_IT_MYSQL_DB`, `DBTOOL_IT_TIDB_DB`, `DBTOOL_IT_MONGO_DB`, and the `DBTOOL_IT_*_PORT` variables documented in `docs/integration-testing.md`.

Message writes use the same safety flag as other write paths:

```bash
dbtool --dsn kafka://127.0.0.1:19092 --allow-write mq produce events '{"hello":"world"}'
dbtool --dsn kafka://127.0.0.1:19092 mq consume events --max 10 --timeout 5
dbtool --dsn kafka://127.0.0.1:19092 mq detail events
dbtool --dsn redis://127.0.0.1:16379/0 --allow-write mq produce stream:events '{"hello":"redis-stream"}'
dbtool --dsn redis://127.0.0.1:16379/0 mq consume stream:events --max 10 --timeout 5
dbtool --dsn redis://127.0.0.1:16379/0 mq detail pubsub:events
dbtool --dsn nats://127.0.0.1:14222 mq topics
dbtool --dsn nats://127.0.0.1:14222 mq detail EVENTS
dbtool --dsn nats://127.0.0.1:14222 mq lag DURABLE_CONSUMER
```
