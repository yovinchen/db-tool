use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{
    error::Error, port::capability::SetOptions, service::formatter::Formatter, Result,
};

#[derive(Args)]
pub struct KvCmd {
    #[command(subcommand)]
    pub action: KvAction,
}

#[derive(Subcommand)]
pub enum KvAction {
    Get {
        key: String,
    },
    Set {
        key: String,
        value: String,
        #[arg(long)]
        ttl: Option<u64>,
    },
    Scan {
        #[arg(default_value = "*")]
        pattern: String,
    },
    Del {
        keys: Vec<String>,
    },
    /// Send a raw command, e.g.: dbtool kv raw XLEN mystream
    Raw {
        args: Vec<String>,
    },
}

pub async fn run(ctx: &Context, cmd: KvCmd) -> Result<String> {
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
            Formatter::success(
                &kind,
                serde_json::json!({"key": key, "value": s}),
                elapsed(),
                false,
            )
        }
        KvAction::Set { key, value, ttl } => {
            ensure_write_allowed(ctx)?;
            kv.set(
                &key,
                value.as_bytes(),
                SetOptions {
                    ttl_secs: ttl,
                    nx: false,
                },
            )
            .await?;
            Formatter::success(&kind, serde_json::json!({"ok": true}), elapsed(), false)
        }
        KvAction::Scan { pattern } => {
            let keys = kv.scan(&pattern, ctx.limit).await?;
            let truncated = keys.len() >= ctx.limit;
            Formatter::success(&kind, keys, elapsed(), truncated)
        }
        KvAction::Del { keys } => {
            ensure_write_allowed(ctx)?;
            let n = kv.delete(&keys).await?;
            Formatter::success(&kind, serde_json::json!({"deleted": n}), elapsed(), false)
        }
        KvAction::Raw { args } => {
            if args.is_empty() {
                return Err(Error::Config(
                    "raw command requires at least one argument".into(),
                ));
            }
            if !is_readonly_raw_command(&args[0]) {
                ensure_write_allowed(ctx)?;
            }
            let val = kv.raw_command(&args).await?;
            Formatter::success(&kind, val, elapsed(), false)
        }
    })
}

fn ensure_write_allowed(ctx: &Context) -> Result<()> {
    if ctx.allow_write {
        Ok(())
    } else {
        Err(Error::WriteNotAllowed)
    }
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
