# Named Connection Config CRUD Evidence

Task ID: IF-T60

Result: PASS

Run at (UTC): 2026-07-15T19:58:00Z

Environment: macOS arm64; Rust 1.96.0; isolated temporary `XDG_CONFIG_HOME`; no database connection required

Commands: `cargo test -p dbtool-core config::file`; `cargo test -p dbtool-cli --bin dbtool cmd::conn::tests`; `cargo test -p dbtool-cli --test conn_crud`; core/CLI Clippy with warnings denied

| Resource | Create | Read/list | Update/replace | Delete | Guard | Persistence | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- |
| temporary `connections.toml` | `conn add` stored an unexpanded `${ENV}` DSN template and readonly flag PASS | list returned sorted env/file names with redacted DSNs PASS | duplicate rejected; replace required a token bound to path/name/old/new entry and retained existing limits PASS | remove required a token bound to path/name/current entry PASS | no-write, wrong action, wrong target, changed content, environment-managed name, invalid name and unsupported scheme all rejected before DB access PASS | same-directory exclusive temp, Unix 0600, file sync, atomic replace, parent sync; injected pre-rename failure retained old bytes and removed temp PASS | isolated config directory removed PASS |
| malformed config fixture | N/A | parse failed with stable `CONFIG_ERROR` without quoting the secret source line PASS | N/A | N/A | credential substring absent from error PASS | original fixture unchanged PASS | isolated directory removed PASS |

Boundaries: the typed TOML model preserves modeled defaults, connections, readonly flags and limits, but does not retain comments or source formatting. The CLI reports this as `comments_preserved=false`. Windows replacement uses `MoveFileExW(REPLACE_EXISTING | WRITE_THROUGH)`; this branch is compile-gated but was not runtime-executed on the macOS host.

Cleanup: PASS

Commit: IF-T60
