# dbtool Claude Skill

Use dbtool when you need to inspect SQL databases, key-value stores, document stores, or message systems through a machine-readable CLI. Prefer this tool for workflows that need stable JSON, safety checks, and named connection resolution.

## Operating Flow

1. Inspect capabilities first:

   ```bash
   dbtool --conn <name> caps
   dbtool --dsn sqlite::memory: caps
   ```

2. Inspect structure before querying data:

   ```bash
   dbtool --conn <name> sql tables
   dbtool --conn <name> sql schema users
   ```

3. Query with explicit limits:

   ```bash
   dbtool --conn <name> --limit 50 sql query "select id, name from users"
   ```

4. Use protocol-specific escape hatches only when the typed command is not enough:

   ```bash
   dbtool --conn redis-local kv raw XLEN mystream
   ```

## Output Contract

Current CLI output is JSON-only. The envelope is stable:

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
```

## Connection Resolution

Use `--dsn` for an explicit one-off target. Use `--conn <name>` for configured targets. Named connections resolve through:

1. `DBTOOL_CONN_<NAME>`
2. `connections.toml`

DSNs may reference environment variables as `${VAR}`. Never print raw DSNs with credentials; use redacted output from `conn list`.

## Local Integration Runs

For end-to-end verification with real services, use:

```bash
./scripts/integration-test.sh
```

Override names and ports with `DBTOOL_IT_PROJECT`, `DBTOOL_IT_POSTGRES_DB`, `DBTOOL_IT_MYSQL_DB`, `DBTOOL_IT_MONGO_DB`, and the `DBTOOL_IT_*_PORT` variables documented in `docs/integration-testing.md`.
