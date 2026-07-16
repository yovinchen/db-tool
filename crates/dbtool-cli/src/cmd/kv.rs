use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{
    error::Error,
    model::{decode_canonical_base64, InputBudget, ReadBudget, Value},
    port::{capability::SetOptions, CapabilityOperation},
    service::{InputLimiter, ReadLimiter, SafetyGuard},
    Result,
};

const MAX_RAW_ARGUMENT_BYTES: usize = 1024 * 1024;
const MAX_RAW_REQUEST_BYTES: usize = 8 * 1024 * 1024;

#[derive(Args)]
pub struct KvCmd {
    #[command(subcommand)]
    pub action: KvAction,
}

#[derive(Subcommand)]
pub enum KvAction {
    /// Read one key and return its exact bytes plus optional UTF-8 text.
    Get {
        /// Key name to read.
        key: String,
    },
    /// Write one text or canonical-base64 value, optionally with a TTL.
    Set {
        /// Key name to write.
        key: String,
        /// UTF-8 text value to store. Mutually exclusive with --value-base64.
        value: Option<String>,
        /// Canonical RFC 4648 base64 bytes to store.
        #[arg(long, value_name = "BASE64")]
        value_base64: Option<String>,
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
    /// Send one explicitly allowlisted, bounded raw Redis command.
    Raw {
        /// Raw command name followed by its arguments.
        args: Vec<String>,
    },
}

pub async fn run(ctx: &Context, cmd: KvCmd) -> Result<String> {
    // Prepare every caller-controlled value and command policy before DSN
    // resolution or network access. Invalid base64 and unsafe raw commands are
    // configuration errors even when the target is unavailable.
    let prepared_set = match &cmd.action {
        KvAction::Set {
            value,
            value_base64,
            ..
        } => Some(prepare_set_value(
            value.as_deref(),
            value_base64.as_deref(),
        )?),
        _ => None,
    };
    let raw_plan = match &cmd.action {
        KvAction::Raw { args } => Some(classify_raw_command(args, ctx.limit)?),
        _ => None,
    };
    let read_budget = match (&cmd.action, raw_plan.as_ref()) {
        (KvAction::Get { .. }, _) => Some(ReadBudget::new(1, ctx.max_bytes)?),
        (KvAction::Scan { .. }, _) | (KvAction::Raw { .. }, _) => Some(ctx.read_budget()?),
        _ => None,
    };
    let input_budget = match (&cmd.action, raw_plan.as_ref()) {
        (KvAction::Set { key, ttl, nx, .. }, _) => {
            let budget = ctx.input_budget()?;
            preflight_set(
                key,
                prepared_set
                    .as_deref()
                    .ok_or_else(|| Error::Internal("prepared KV value is missing".into()))?,
                &SetOptions {
                    ttl_secs: *ttl,
                    nx: *nx,
                },
                budget,
            )?;
            Some(budget)
        }
        (KvAction::Del { keys }, _) => {
            let budget = ctx.input_budget()?;
            InputLimiter::new(budget, "CLI Redis DEL input")?.validate_batch(keys)?;
            Some(budget)
        }
        (KvAction::Raw { args }, Some(plan)) if plan.is_mutation() => {
            let budget = ctx.input_budget()?;
            InputLimiter::new(budget, "CLI Redis raw mutation input")?.validate_batch(args)?;
            Some(budget)
        }
        _ => None,
    };

    match &cmd.action {
        KvAction::Set { .. } | KvAction::Del { .. } => ensure_write_allowed(ctx)?,
        KvAction::Raw { .. } if raw_plan.as_ref().is_some_and(RawCommandPlan::is_mutation) => {
            ensure_write_allowed(ctx)?;
        }
        KvAction::Get { .. } | KvAction::Scan { .. } | KvAction::Raw { .. } => {}
    }

    let dsn = ctx.resolve_dsn()?;
    if let Some(plan) = raw_plan.as_ref().filter(|plan| plan.is_mutation()) {
        let scope = SafetyGuard::confirmation_scope_digest(&(
            plan.command.as_str(),
            plan.target.as_deref(),
            match &cmd.action {
                KvAction::Raw { args } => args.as_slice(),
                _ => &[],
            },
        ))?;
        SafetyGuard::check_destructive_operation_with_scope(
            "redis_raw_mutation",
            &plan.resource_label(),
            &ctx.confirmation_target(&dsn)?,
            &scope,
            ctx.allow_write,
            ctx.confirm.as_deref(),
        )?;
    }

    let conn = ctx.registry.connect(&dsn).await?;
    let operations = conn.operations();
    let kind = conn.kind().0.clone();
    let (operation, needed) = kv_operation_for_action(&cmd.action, raw_plan.as_ref());
    require_kv_operation(&operations, operation, &kind, needed)?;
    let kv = conn.as_kv().ok_or_else(|| Error::UnsupportedCapability {
        kind: kind.clone(),
        needed: "KeyValueStore",
    })?;

    let start = std::time::Instant::now();
    let elapsed = || start.elapsed().as_millis() as u64;

    Ok(match cmd.action {
        KvAction::Get { key } => {
            let value = kv
                .get_bounded(
                    &key,
                    read_budget.expect("GET actions construct a read budget"),
                )
                .await?;
            let (value, value_bytes, encoding) = match value {
                Some(bytes) => {
                    let text = std::str::from_utf8(&bytes).ok().map(str::to_owned);
                    let encoding = if text.is_some() { "utf8" } else { "binary" };
                    (text, Some(Value::Bytes(bytes.to_vec())), Some(encoding))
                }
                None => (None, None, None),
            };
            let data = enforce_kv_output_budget(
                serde_json::json!({
                    "key": key,
                    "value": value,
                    "value_bytes": value_bytes,
                    "encoding": encoding,
                }),
                ctx.max_bytes,
                "KV get CLI output",
            )?;
            ctx.render_success(&kind, data, elapsed(), false)
        }
        KvAction::Set {
            key,
            value: _,
            value_base64: _,
            ttl,
            nx,
        } => {
            let value = prepared_set
                .as_deref()
                .ok_or_else(|| Error::Internal("prepared KV value is missing".into()))?;
            kv.set_budgeted(
                &key,
                value,
                SetOptions { ttl_secs: ttl, nx },
                input_budget.expect("SET actions construct an input budget"),
            )
            .await?;
            ctx.render_success(&kind, serde_json::json!({"ok": true}), elapsed(), false)
        }
        KvAction::Scan { pattern } => {
            let result = kv
                .scan_bounded(
                    &pattern,
                    read_budget.expect("SCAN actions construct a read budget"),
                )
                .await?;
            let truncated = result.truncated;
            let keys = enforce_kv_output_budget(result.items, ctx.max_bytes, "KV scan CLI output")?;
            ctx.render_success(&kind, keys, elapsed(), truncated)
        }
        KvAction::Del { keys } => {
            let n = kv
                .delete_budgeted(
                    &keys,
                    input_budget.expect("DEL actions construct an input budget"),
                )
                .await?;
            ctx.render_success(&kind, serde_json::json!({"deleted": n}), elapsed(), false)
        }
        KvAction::Raw { args } => {
            let mutation = raw_plan.as_ref().is_some_and(RawCommandPlan::is_mutation);
            let val = if mutation {
                kv.raw_command_io_budgeted(
                    &args,
                    input_budget.expect("raw mutations construct an input budget"),
                    read_budget.expect("raw mutations construct a response budget"),
                )
                .await?
            } else {
                let value = kv
                    .raw_command_bounded(
                        &args,
                        read_budget.expect("raw read actions construct a read budget"),
                    )
                    .await?;
                enforce_kv_output_budget(value, ctx.max_bytes, "KV raw CLI output")?
            };
            ctx.render_success(&kind, val, elapsed(), false)
        }
    })
}

fn kv_operation_for_action(
    action: &KvAction,
    raw_plan: Option<&RawCommandPlan>,
) -> (CapabilityOperation, &'static str) {
    match action {
        KvAction::Get { .. } => (
            CapabilityOperation::KeyValueGetBounded,
            "KeyValueStore.get_bounded",
        ),
        KvAction::Set { .. } => (
            CapabilityOperation::KeyValueSetBudgeted,
            "KeyValueStore.set_budgeted",
        ),
        KvAction::Scan { .. } => (
            CapabilityOperation::KeyValueScanBounded,
            "KeyValueStore.scan_bounded",
        ),
        KvAction::Del { .. } => (
            CapabilityOperation::KeyValueDeleteBudgeted,
            "KeyValueStore.delete_budgeted",
        ),
        KvAction::Raw { .. } if raw_plan.is_some_and(RawCommandPlan::is_mutation) => (
            CapabilityOperation::KeyValueRawCommandIoBudgeted,
            "KeyValueStore.raw_command_io_budgeted",
        ),
        KvAction::Raw { .. } => (
            CapabilityOperation::KeyValueRawCommandBounded,
            "KeyValueStore.raw_command_bounded",
        ),
    }
}

fn preflight_set(key: &str, value: &[u8], options: &SetOptions, budget: InputBudget) -> Result<()> {
    InputLimiter::new(budget, "CLI Redis SET input")?.validate_request(&(key, value, options))
}

pub(crate) fn action_may_mutate(action: &KvAction) -> bool {
    match action {
        KvAction::Set { .. } | KvAction::Del { .. } => true,
        KvAction::Raw { args } => {
            normalized_raw_command(args).is_ok_and(|command| is_raw_mutation(&command))
        }
        KvAction::Get { .. } | KvAction::Scan { .. } => false,
    }
}

fn enforce_kv_output_budget<T: serde::Serialize>(
    value: T,
    max_bytes: usize,
    subject: &'static str,
) -> Result<T> {
    ReadLimiter::new(ReadBudget::new(1, max_bytes)?, subject)?.finish_single(value)
}

fn require_kv_operation(
    operations: &[CapabilityOperation],
    operation: CapabilityOperation,
    kind: &str,
    needed: &'static str,
) -> Result<()> {
    if operations.contains(&operation) {
        Ok(())
    } else {
        Err(Error::UnsupportedCapability {
            kind: kind.to_owned(),
            needed,
        })
    }
}

fn prepare_set_value(value: Option<&str>, value_base64: Option<&str>) -> Result<Vec<u8>> {
    match (value, value_base64) {
        (Some(_), Some(_)) => Err(Error::Config(
            "kv set accepts exactly one of positional VALUE or --value-base64".into(),
        )),
        (None, None) => Err(Error::Config(
            "kv set requires positional VALUE or --value-base64".into(),
        )),
        (Some(value), None) => Ok(value.as_bytes().to_vec()),
        (None, Some(value)) => decode_canonical_base64(value)
            .map_err(|error| Error::Config(format!("invalid --value-base64: {error}"))),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RawCommandAccess {
    ReadOnly,
    Mutation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RawCommandPlan {
    command: String,
    access: RawCommandAccess,
    target: Option<String>,
}

impl RawCommandPlan {
    fn readonly(command: String) -> Self {
        Self {
            command,
            access: RawCommandAccess::ReadOnly,
            target: None,
        }
    }

    fn mutation(command: String, target: String) -> Self {
        Self {
            command,
            access: RawCommandAccess::Mutation,
            target: Some(target),
        }
    }

    fn is_mutation(&self) -> bool {
        self.access == RawCommandAccess::Mutation
    }

    fn resource_label(&self) -> String {
        match self.target.as_deref() {
            Some(target) => format!("{}:{target}", self.command),
            None => self.command.clone(),
        }
    }
}

fn classify_raw_command(args: &[String], limit: usize) -> Result<RawCommandPlan> {
    if limit == 0 {
        return Err(Error::Config(
            "global --limit must be greater than zero for kv raw".into(),
        ));
    }
    validate_raw_request_size(args)?;
    let command = normalized_raw_command(args)?;

    if is_forbidden_raw_command(&command) {
        return Err(Error::Config(format!(
            "Redis raw command {command} is forbidden by the portable safety policy"
        )));
    }

    match command.as_str() {
        "PING" => expect_arity(args, &[1, 2], &command),
        "ECHO" => expect_arity(args, &[2], &command),
        "GET" | "TTL" | "PTTL" | "TYPE" | "STRLEN" | "HLEN" | "LLEN" | "SCARD" | "ZCARD"
        | "XLEN" => expect_arity(args, &[2], &command),
        "DBSIZE" | "TIME" | "LASTSAVE" => expect_arity(args, &[1], &command),
        "HGET" | "HEXISTS" | "SISMEMBER" | "ZSCORE" => expect_arity(args, &[3], &command),
        "LINDEX" => {
            expect_arity(args, &[3], &command)?;
            parse_i64_arg(args, 2, &command)?;
            Ok(())
        }
        "MGET" | "EXISTS" => {
            expect_min_arity(args, 2, &command)?;
            check_item_budget(&command, args.len() - 1, limit)
        }
        "HMGET" => {
            expect_min_arity(args, 3, &command)?;
            check_item_budget(&command, args.len() - 2, limit)
        }
        "LRANGE" => {
            expect_arity(args, &[4], &command)?;
            check_index_range_budget(args, 2, 3, &command, limit)
        }
        "ZRANGE" | "ZREVRANGE" => {
            expect_arity(args, &[4, 5], &command)?;
            if args.len() == 5 && !args[4].eq_ignore_ascii_case("WITHSCORES") {
                return Err(Error::Config(format!(
                    "kv raw {command} permits only the optional WITHSCORES modifier; use a dedicated bounded command for other modes"
                )));
            }
            check_index_range_budget(args, 2, 3, &command, limit)
        }
        "SRANDMEMBER" => {
            expect_arity(args, &[2, 3], &command)?;
            if args.len() == 3 {
                let count = parse_i64_arg(args, 2, &command)?;
                let count = count.checked_abs().ok_or_else(|| {
                    Error::Config(format!("kv raw {command} count is outside the i64 range"))
                })?;
                check_item_budget(&command, count as usize, limit)?;
            }
            Ok(())
        }
        "XRANGE" | "XREVRANGE" => {
            expect_arity(args, &[6], &command)?;
            if !args[4].eq_ignore_ascii_case("COUNT") {
                return Err(Error::Config(format!(
                    "kv raw {command} requires an explicit COUNT bounded by global --limit"
                )));
            }
            let count = parse_positive_usize_arg(args, 5, &command)?;
            check_item_budget(&command, count, limit)
        }

        "SET" => {
            expect_min_arity(args, 3, &command)?;
            if args[3..]
                .iter()
                .any(|argument| argument.eq_ignore_ascii_case("GET"))
            {
                return Err(value_returning_raw_mutation_error(&command));
            }
            Ok(())
        }
        "DEL" | "UNLINK" => {
            expect_min_arity(args, 2, &command)?;
            check_item_budget(&command, args.len() - 1, limit)
        }
        "INCR" | "DECR" | "PERSIST" => expect_arity(args, &[2], &command),
        "GETDEL" => return Err(value_returning_raw_mutation_error(&command)),
        "APPEND" | "INCRBY" | "INCRBYFLOAT" | "DECRBY" => expect_arity(args, &[3], &command),
        "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" => {
            expect_arity(args, &[3, 4], &command)?;
            if args.len() == 4
                && !matches!(
                    args[3].to_ascii_uppercase().as_str(),
                    "NX" | "XX" | "GT" | "LT"
                )
            {
                return Err(Error::Config(format!(
                    "kv raw {command} accepts only NX, XX, GT, or LT as its condition"
                )));
            }
            Ok(())
        }
        "RENAME" | "RENAMENX" => expect_arity(args, &[3], &command),
        "HSET" => {
            expect_min_arity(args, 4, &command)?;
            if !(args.len() - 2).is_multiple_of(2) {
                return Err(Error::Config(format!(
                    "kv raw {command} requires field/value pairs"
                )));
            }
            check_item_budget(&command, (args.len() - 2) / 2, limit)
        }
        "HDEL" | "SADD" | "SREM" | "LPUSH" | "RPUSH" | "LPUSHX" | "RPUSHX" | "ZREM" | "XDEL" => {
            expect_min_arity(args, 3, &command)?;
            check_item_budget(&command, args.len() - 2, limit)
        }
        "HINCRBY" | "HINCRBYFLOAT" | "LSET" | "ZINCRBY" => expect_arity(args, &[4], &command),
        "LTRIM" => {
            expect_arity(args, &[4], &command)?;
            check_index_range_budget(args, 2, 3, &command, limit)
        }
        "LPOP" | "RPOP" | "SPOP" => return Err(value_returning_raw_mutation_error(&command)),
        "ZADD" => validate_simple_zadd(args, &command, limit),
        _ => {
            return Err(Error::Config(format!(
                "Redis raw command {command} is not in the portable allowlist"
            )))
        }
    }?;

    if is_raw_mutation(&command) {
        Ok(RawCommandPlan::mutation(command, raw_mutation_target(args)))
    } else {
        Ok(RawCommandPlan::readonly(command))
    }
}

fn value_returning_raw_mutation_error(command: &str) -> Error {
    Error::Config(format!(
        "kv raw {command} is rejected because its value response cannot be budgeted after mutation; use a dedicated atomic API"
    ))
}

fn normalized_raw_command(args: &[String]) -> Result<String> {
    let raw = args
        .first()
        .ok_or_else(|| Error::Config("raw command requires at least one argument".into()))?;
    if raw.is_empty()
        || !raw
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(Error::Config(
            "Redis raw command name must contain only ASCII letters, digits, or underscore".into(),
        ));
    }
    Ok(raw.to_ascii_uppercase())
}

fn validate_raw_request_size(args: &[String]) -> Result<()> {
    let mut total = 0_usize;
    for (index, arg) in args.iter().enumerate() {
        if arg.len() > MAX_RAW_ARGUMENT_BYTES {
            return Err(Error::Config(format!(
                "kv raw argument {index} exceeds the 1 MiB safety budget"
            )));
        }
        total = total
            .checked_add(arg.len())
            .ok_or_else(|| Error::Config("kv raw request byte budget overflow".into()))?;
    }
    if total > MAX_RAW_REQUEST_BYTES {
        return Err(Error::Config(
            "kv raw request exceeds the 8 MiB safety budget".into(),
        ));
    }
    Ok(())
}

fn is_forbidden_raw_command(command: &str) -> bool {
    matches!(
        command,
        "ACL"
            | "ASKING"
            | "AUTH"
            | "BGREWRITEAOF"
            | "BGSAVE"
            | "CLIENT"
            | "CLUSTER"
            | "COMMAND"
            | "CONFIG"
            | "DEBUG"
            | "DISCARD"
            | "DUMP"
            | "EVAL"
            | "EVALSHA"
            | "EVAL_RO"
            | "EVALSHA_RO"
            | "EXEC"
            | "FAILOVER"
            | "FCALL"
            | "FCALL_RO"
            | "FLUSHALL"
            | "FLUSHDB"
            | "FUNCTION"
            | "INFO"
            | "LATENCY"
            | "MEMORY"
            | "MIGRATE"
            | "MODULE"
            | "MONITOR"
            | "MOVE"
            | "MULTI"
            | "PSUBSCRIBE"
            | "PSYNC"
            | "PUBLISH"
            | "PUBSUB"
            | "PUNSUBSCRIBE"
            | "READONLY"
            | "READWRITE"
            | "REPLICAOF"
            | "RESTORE"
            | "ROLE"
            | "SAVE"
            | "SCRIPT"
            | "SELECT"
            | "SHUTDOWN"
            | "SLOWLOG"
            | "SLAVEOF"
            | "SSUBSCRIBE"
            | "SUBSCRIBE"
            | "SUNSUBSCRIBE"
            | "SWAPDB"
            | "SYNC"
            | "UNSUBSCRIBE"
            | "UNWATCH"
            | "WATCH"
    )
}

fn is_raw_mutation(command: &str) -> bool {
    matches!(
        command,
        "SET"
            | "DEL"
            | "UNLINK"
            | "INCR"
            | "DECR"
            | "PERSIST"
            | "GETDEL"
            | "APPEND"
            | "INCRBY"
            | "INCRBYFLOAT"
            | "DECRBY"
            | "EXPIRE"
            | "PEXPIRE"
            | "EXPIREAT"
            | "PEXPIREAT"
            | "RENAME"
            | "RENAMENX"
            | "HSET"
            | "HDEL"
            | "SADD"
            | "SREM"
            | "LPUSH"
            | "RPUSH"
            | "LPUSHX"
            | "RPUSHX"
            | "ZREM"
            | "XDEL"
            | "HINCRBY"
            | "HINCRBYFLOAT"
            | "LSET"
            | "ZINCRBY"
            | "LTRIM"
            | "LPOP"
            | "RPOP"
            | "SPOP"
            | "ZADD"
    )
}

fn raw_mutation_target(args: &[String]) -> String {
    match args.first().map(|command| command.to_ascii_uppercase()) {
        Some(command) if matches!(command.as_str(), "RENAME" | "RENAMENX") => {
            format!("{} -> {}", args[1], args[2])
        }
        Some(command) if matches!(command.as_str(), "DEL" | "UNLINK") => {
            format!("{} key(s), first={}", args.len() - 1, args[1])
        }
        _ => args.get(1).cloned().unwrap_or_else(|| "<none>".into()),
    }
}

fn expect_arity(args: &[String], accepted: &[usize], command: &str) -> Result<()> {
    if accepted.contains(&args.len()) {
        return Ok(());
    }
    Err(Error::Config(format!(
        "kv raw {command} received {} argument(s); accepted total argument counts: {accepted:?}",
        args.len()
    )))
}

fn expect_min_arity(args: &[String], minimum: usize, command: &str) -> Result<()> {
    if args.len() >= minimum {
        Ok(())
    } else {
        Err(Error::Config(format!(
            "kv raw {command} requires at least {} argument(s) after the command",
            minimum - 1
        )))
    }
}

fn check_item_budget(command: &str, count: usize, limit: usize) -> Result<()> {
    if count == 0 {
        return Err(Error::Config(format!(
            "kv raw {command} requires at least one item"
        )));
    }
    if count > limit {
        return Err(Error::Config(format!(
            "kv raw {command} requests {count} item(s), exceeding global --limit {limit}"
        )));
    }
    Ok(())
}

fn parse_i64_arg(args: &[String], index: usize, command: &str) -> Result<i64> {
    args[index].parse::<i64>().map_err(|_| {
        Error::Config(format!(
            "kv raw {command} argument {} must be an i64 integer",
            index
        ))
    })
}

fn parse_positive_usize_arg(args: &[String], index: usize, command: &str) -> Result<usize> {
    let value = args[index].parse::<usize>().map_err(|_| {
        Error::Config(format!(
            "kv raw {command} argument {} must be a positive integer",
            index
        ))
    })?;
    if value == 0 {
        return Err(Error::Config(format!(
            "kv raw {command} argument {} must be greater than zero",
            index
        )));
    }
    Ok(value)
}

fn check_index_range_budget(
    args: &[String],
    start_index: usize,
    stop_index: usize,
    command: &str,
    limit: usize,
) -> Result<()> {
    let start = parse_i64_arg(args, start_index, command)?;
    let stop = parse_i64_arg(args, stop_index, command)?;
    if start < 0 || stop < 0 {
        return Err(Error::Config(format!(
            "kv raw {command} rejects negative indexes because their result size depends on remote collection length"
        )));
    }
    if stop < start {
        return Ok(());
    }
    let count = stop
        .checked_sub(start)
        .and_then(|span| span.checked_add(1))
        .and_then(|count| usize::try_from(count).ok())
        .ok_or_else(|| Error::Config(format!("kv raw {command} range size overflow")))?;
    check_item_budget(command, count, limit)
}

fn validate_simple_zadd(args: &[String], command: &str, limit: usize) -> Result<()> {
    expect_min_arity(args, 4, command)?;
    if !(args.len() - 2).is_multiple_of(2) {
        return Err(Error::Config(
            "kv raw ZADD accepts only score/member pairs in portable mode".into(),
        ));
    }
    for score_index in (2..args.len()).step_by(2) {
        let score = args[score_index]
            .parse::<f64>()
            .map_err(|_| Error::Config("kv raw ZADD score must be numeric".into()))?;
        if !score.is_finite() {
            return Err(Error::Config("kv raw ZADD score must be finite".into()));
        }
    }
    check_item_budget(command, (args.len() - 2) / 2, limit)
}

fn ensure_write_allowed(ctx: &Context) -> Result<()> {
    ctx.ensure_write_allowed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbtool_core::service::formatter::Format;

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    fn test_context(allow_write: bool) -> Context {
        Context {
            registry: dbtool_core::registry::Registry::default(),
            conn: None,
            dsn: Some("redis://127.0.0.1:1/0".to_owned()),
            format: Format::Json,
            limit: 2,
            max_bytes: dbtool_core::model::DEFAULT_READ_BYTES,
            max_item_bytes: dbtool_core::model::DEFAULT_INPUT_ITEM_BYTES,
            throttle_overrides: Default::default(),
            allow_write,
            confirm: None,
        }
    }

    #[test]
    fn set_values_are_exclusive_and_base64_is_strict() {
        assert_eq!(prepare_set_value(Some("hello"), None).unwrap(), b"hello");
        assert_eq!(prepare_set_value(None, Some("AP8=")).unwrap(), [0, 255]);
        assert_eq!(prepare_set_value(None, Some("")).unwrap(), Vec::<u8>::new());
        assert!(matches!(
            prepare_set_value(Some("hello"), Some("aGVsbG8=")),
            Err(Error::Config(_))
        ));
        assert!(matches!(
            prepare_set_value(None, None),
            Err(Error::Config(_))
        ));
        for invalid in ["aGVsbG8", "AB==", "aGVsbG8===", "aGVs bG8="] {
            assert!(matches!(
                prepare_set_value(None, Some(invalid)),
                Err(Error::Config(_))
            ));
        }
    }

    #[test]
    fn every_kv_action_requires_its_exact_operation_before_access() {
        assert_eq!(
            kv_operation_for_action(
                &KvAction::Set {
                    key: "key".to_owned(),
                    value: Some("value".to_owned()),
                    value_base64: None,
                    ttl: None,
                    nx: false,
                },
                None,
            ),
            (
                CapabilityOperation::KeyValueSetBudgeted,
                "KeyValueStore.set_budgeted"
            )
        );
        assert_eq!(
            kv_operation_for_action(
                &KvAction::Del {
                    keys: vec!["key".to_owned()],
                },
                None,
            ),
            (
                CapabilityOperation::KeyValueDeleteBudgeted,
                "KeyValueStore.delete_budgeted"
            )
        );
        let action = KvAction::Raw {
            args: strings(&["GET", "key"]),
        };
        let read_plan = classify_raw_command(&strings(&["GET", "key"]), 2).unwrap();
        assert_eq!(
            kv_operation_for_action(&action, Some(&read_plan)),
            (
                CapabilityOperation::KeyValueRawCommandBounded,
                "KeyValueStore.raw_command_bounded"
            )
        );
        assert!(matches!(
            require_kv_operation(
                &[CapabilityOperation::KeyValueRawCommand],
                CapabilityOperation::KeyValueRawCommandBounded,
                "partial-kv",
                "KeyValueStore.raw_command_bounded",
            ),
            Err(Error::UnsupportedCapability { kind, needed })
                if kind == "partial-kv" && needed == "KeyValueStore.raw_command_bounded"
        ));

        let mutation = KvAction::Raw {
            args: strings(&["SET", "key", "value"]),
        };
        let mutation_plan = classify_raw_command(&strings(&["SET", "key", "value"]), 2).unwrap();
        assert_eq!(
            kv_operation_for_action(&mutation, Some(&mutation_plan)),
            (
                CapabilityOperation::KeyValueRawCommandIoBudgeted,
                "KeyValueStore.raw_command_io_budgeted"
            )
        );
        assert!(matches!(
            require_kv_operation(
                &[CapabilityOperation::KeyValueRawCommand],
                CapabilityOperation::KeyValueRawCommandIoBudgeted,
                "legacy-kv",
                "KeyValueStore.raw_command_io_budgeted",
            ),
            Err(Error::UnsupportedCapability { needed, .. })
                if needed == "KeyValueStore.raw_command_io_budgeted"
        ));
    }

    #[tokio::test]
    async fn mutation_input_budget_is_rejected_before_connecting() {
        let mut ctx = test_context(true);
        ctx.max_item_bytes = 1;
        ctx.dsn = None;
        let error = run(
            &ctx,
            KvCmd {
                action: KvAction::Set {
                    key: "key".to_owned(),
                    value: Some("value".to_owned()),
                    value_base64: None,
                    ttl: None,
                    nx: false,
                },
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(error, Error::InputBudgetExceeded { .. }));
    }

    #[tokio::test]
    async fn invalid_base64_fails_before_connecting() {
        let error = run(
            &test_context(true),
            KvCmd {
                action: KvAction::Set {
                    key: "key".into(),
                    value: None,
                    value_base64: Some("not-base64".into()),
                    ttl: None,
                    nx: false,
                },
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(error, Error::Config(message) if message.contains("base64")));
    }

    #[test]
    fn raw_policy_has_three_fail_closed_classes() {
        assert_eq!(
            classify_raw_command(&strings(&["GET", "key"]), 2)
                .unwrap()
                .access,
            RawCommandAccess::ReadOnly
        );
        assert_eq!(
            classify_raw_command(&strings(&["SET", "key", "value"]), 2)
                .unwrap()
                .access,
            RawCommandAccess::Mutation
        );
        for command in [
            "FLUSHALL",
            "SELECT",
            "EVAL",
            "FUNCTION",
            "MULTI",
            "SUBSCRIBE",
            "MIGRATE",
        ] {
            assert!(matches!(
                classify_raw_command(&strings(&[command]), 2),
                Err(Error::Config(_))
            ));
        }
        assert!(matches!(
            classify_raw_command(&strings(&["TOTALLY_UNKNOWN"]), 2),
            Err(Error::Config(_))
        ));
        for args in [
            strings(&["GETDEL", "key"]),
            strings(&["LPOP", "list"]),
            strings(&["RPOP", "list", "2"]),
            strings(&["SPOP", "set"]),
            strings(&["SET", "key", "value", "GET"]),
        ] {
            assert!(matches!(
                classify_raw_command(&args, 2),
                Err(Error::Config(message)) if message.contains("cannot be budgeted after mutation")
            ));
        }
    }

    #[test]
    fn raw_collection_reads_are_bounded_before_connecting() {
        assert!(classify_raw_command(&strings(&["MGET", "one", "two"]), 2).is_ok());
        assert!(matches!(
            classify_raw_command(&strings(&["MGET", "one", "two", "three"]), 2),
            Err(Error::Config(message)) if message.contains("--limit")
        ));
        assert!(classify_raw_command(&strings(&["LRANGE", "list", "0", "1"]), 2).is_ok());
        assert!(classify_raw_command(&strings(&["LRANGE", "list", "-1", "-1"]), 2).is_err());
        for unbounded in ["KEYS", "HGETALL", "SMEMBERS", "SCAN", "XREAD"] {
            assert!(classify_raw_command(&strings(&[unbounded, "*"]), 2).is_err());
        }
    }

    #[test]
    fn raw_mutation_confirmation_binds_all_arguments_and_target() {
        let args = strings(&["SET", "key", "first"]);
        let plan = classify_raw_command(&args, 2).unwrap();
        let first = SafetyGuard::confirmation_scope_digest(&(
            plan.command.as_str(),
            plan.target.as_deref(),
            args.as_slice(),
        ))
        .unwrap();
        let changed_args = strings(&["SET", "key", "second"]);
        let changed = SafetyGuard::confirmation_scope_digest(&(
            plan.command.as_str(),
            plan.target.as_deref(),
            changed_args.as_slice(),
        ))
        .unwrap();
        let changed_target_args = strings(&["SET", "other", "first"]);
        let changed_target_plan = classify_raw_command(&changed_target_args, 2).unwrap();
        let changed_target = SafetyGuard::confirmation_scope_digest(&(
            changed_target_plan.command.as_str(),
            changed_target_plan.target.as_deref(),
            changed_target_args.as_slice(),
        ))
        .unwrap();
        assert_ne!(first, changed);
        assert_ne!(first, changed_target);
    }

    #[test]
    fn caller_visible_kv_output_has_an_exact_byte_boundary() {
        let output = serde_json::json!({
            "key": "binary",
            "value": null,
            "value_bytes": Value::Bytes(vec![0, 255]),
            "encoding": "binary",
        });
        let exact = serde_json::to_vec(&output).unwrap().len();
        assert_eq!(
            enforce_kv_output_budget(output.clone(), exact, "KV CLI test").unwrap(),
            output
        );
        assert!(matches!(
            enforce_kv_output_budget(output, exact - 1, "KV CLI test"),
            Err(Error::ReadBudgetExceeded { unit: "bytes", .. })
        ));
        assert!(ReadBudget::new(0, dbtool_core::model::DEFAULT_READ_BYTES).is_err());
        assert!(ReadBudget::new(usize::MAX, dbtool_core::model::DEFAULT_READ_BYTES).is_err());
    }
}
