use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{error::Error, port::capability::SetOptions, Result};

#[derive(Args)]
pub struct KvCmd {
    #[command(subcommand)]
    pub action: KvAction,
}

#[derive(Subcommand)]
pub enum KvAction {
    /// Read one key and return its UTF-8 value when present.
    Get {
        /// Key name to read.
        key: String,
    },
    /// Write one string value, optionally with a TTL or only when absent.
    Set {
        /// Key name to write.
        key: String,
        /// String value to store.
        value: String,
        /// Expiration time in seconds.
        #[arg(long)]
        ttl: Option<u64>,
        /// Set only when the key does not already exist.
        #[arg(long)]
        nx: bool,
    },
    /// Scan keys matching a pattern, bounded by the global --limit.
    Scan {
        /// Redis glob-style pattern.
        #[arg(default_value = "*")]
        pattern: String,
    },
    /// Delete one or more keys.
    Del {
        /// Key names to delete.
        keys: Vec<String>,
    },
    /// Send a raw command, e.g.: dbtool kv raw XLEN mystream
    Raw {
        /// Raw command name followed by its arguments.
        args: Vec<String>,
    },
}

pub async fn run(ctx: &Context, cmd: KvCmd) -> Result<String> {
    match &cmd.action {
        KvAction::Set { .. } | KvAction::Del { .. } => ensure_write_allowed(ctx)?,
        KvAction::Raw { args } => {
            let Some(command) = args.first() else {
                return Err(Error::Config(
                    "raw command requires at least one argument".into(),
                ));
            };
            if !is_readonly_raw_command(command) {
                ensure_write_allowed(ctx)?;
            }
        }
        KvAction::Get { .. } | KvAction::Scan { .. } => {}
    }

    let dsn = ctx.resolve_dsn()?;
    let conn = ctx.registry.connect(&dsn).await?;
    let kv = conn.as_kv().ok_or_else(|| Error::UnsupportedCapability {
        kind: conn.kind().0.clone(),
        needed: "KeyValueStore",
    })?;

    let start = std::time::Instant::now();
    let elapsed = || start.elapsed().as_millis() as u64;
    let kind = conn.kind().0.clone();

    Ok(match cmd.action {
        KvAction::Get { key } => {
            let val = kv.get(&key).await?;
            let s = val.map(|b| String::from_utf8_lossy(&b).into_owned());
            ctx.render_success(
                &kind,
                serde_json::json!({"key": key, "value": s}),
                elapsed(),
                false,
            )
        }
        KvAction::Set {
            key,
            value,
            ttl,
            nx,
        } => {
            kv.set(&key, value.as_bytes(), SetOptions { ttl_secs: ttl, nx })
                .await?;
            ctx.render_success(&kind, serde_json::json!({"ok": true}), elapsed(), false)
        }
        KvAction::Scan { pattern } => {
            let keys = kv.scan(&pattern, ctx.limit).await?;
            let truncated = keys.len() >= ctx.limit;
            ctx.render_success(&kind, keys, elapsed(), truncated)
        }
        KvAction::Del { keys } => {
            let n = kv.delete(&keys).await?;
            ctx.render_success(&kind, serde_json::json!({"deleted": n}), elapsed(), false)
        }
        KvAction::Raw { args } => {
            let val = kv.raw_command(&args).await?;
            ctx.render_success(&kind, val, elapsed(), false)
        }
    })
}

fn ensure_write_allowed(ctx: &Context) -> Result<()> {
    ctx.ensure_write_allowed()
}

fn is_readonly_raw_command(command: &str) -> bool {
    matches!(
        command.to_ascii_uppercase().as_str(),
        "PING"
            | "ECHO"
            | "GET"
            | "MGET"
            | "EXISTS"
            | "TTL"
            | "PTTL"
            | "TYPE"
            | "STRLEN"
            | "DBSIZE"
            | "SCAN"
            | "KEYS"
            | "HGET"
            | "HMGET"
            | "HGETALL"
            | "HEXISTS"
            | "HLEN"
            | "HKEYS"
            | "HVALS"
            | "LLEN"
            | "LRANGE"
            | "LINDEX"
            | "SCARD"
            | "SISMEMBER"
            | "SMEMBERS"
            | "SRANDMEMBER"
            | "ZCARD"
            | "ZRANGE"
            | "ZREVRANGE"
            | "ZSCORE"
            | "XLEN"
            | "XRANGE"
            | "XREVRANGE"
            | "XINFO"
            | "XREAD"
            | "PUBSUB"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readonly_raw_commands_do_not_need_write_flag() {
        assert!(is_readonly_raw_command("ping"));
        assert!(is_readonly_raw_command("GET"));
        assert!(is_readonly_raw_command("xinfo"));
    }

    #[test]
    fn mutating_raw_commands_need_write_flag() {
        assert!(!is_readonly_raw_command("set"));
        assert!(!is_readonly_raw_command("incr"));
        assert!(!is_readonly_raw_command("xadd"));
    }
}
