# SQL Read-Only Guard Evidence

Task ID: IF-T56

Result: PASS

Run at (UTC): 2026-07-15T17:42:13Z

The shared SQL classifier now recursively inspects query bodies instead of
treating every row-returning `Statement::Query` as read-only.

Verified fail-closed cases:

| Input | Classification / CLI result |
| --- | --- |
| PostgreSQL data-modifying CTE (`WITH ... DELETE ... RETURNING`) | parser fallback is `Write`; `sql query` and `export sql` return `WRITE_NOT_ALLOWED` before connection |
| PostgreSQL/SQL Server `SELECT ... INTO` | `Destructive`; read-only entry points return `WRITE_NOT_ALLOWED` |
| `SELECT ... FOR UPDATE` / `FOR SHARE` | `Write`; read-only entry points return `WRITE_NOT_ALLOWED` |
| destructive SQL without `--allow-write` | `WRITE_NOT_ALLOWED` before any confirmation token is issued |
| ordinary and recursive read-only SELECT/CTE | `Read`; existing bounded query tests continue to pass |

Commands:

```text
cargo test -p dbtool-core service::safety::tests
cargo test -p dbtool-cli --test sql_readonly_safety
```

The classifier is a client-side safety layer, not a replacement for database
authorization. A database-specific function invoked from a syntactic SELECT may
have server-side side effects that a portable SQL AST cannot know. Production
read-only connections must still use least-privilege database roles; dbtool does
not claim that parsing can override server permissions.
