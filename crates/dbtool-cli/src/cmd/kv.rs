use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{error::Error, port::capability::SetOptions, service::ResultLimiter, Result};

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
    /// Scan keys matching a pattern up to --limit and probe one extra key for exact truncation.
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
            let probe_limit = ResultLimiter::new(ctx.limit).probe_rows()?;
            let keys = kv.scan(&pattern, probe_limit).await?;
            let (keys, truncated) = limit_scan_keys(keys, ctx.limit);
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

fn limit_scan_keys(mut keys: Vec<String>, limit: usize) -> (Vec<String>, bool) {
    let truncated = keys.len() > limit;
    keys.truncate(limit);
    (keys, truncated)
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

    #[test]
    fn scan_truncation_requires_a_probe_key() {
        let (exact, exact_truncated) = limit_scan_keys(vec!["one".to_owned(), "two".to_owned()], 2);
        assert_eq!(exact, ["one", "two"]);
        assert!(!exact_truncated);

        let (limited, limited_truncated) = limit_scan_keys(
            vec!["one".to_owned(), "two".to_owned(), "three".to_owned()],
            2,
        );
        assert_eq!(limited, ["one", "two"]);
        assert!(limited_truncated);
    }

    #[test]
    fn scan_probe_limit_rejects_zero_and_overflow() {
        assert!(ResultLimiter::new(0).probe_rows().is_err());
        assert!(ResultLimiter::new(usize::MAX).probe_rows().is_err());
        assert_eq!(ResultLimiter::new(2).probe_rows().unwrap(), 3);
    }
}
