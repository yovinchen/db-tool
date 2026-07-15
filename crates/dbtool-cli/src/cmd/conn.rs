use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{
    config::{
        env::discover_env_connections,
        file::{ConnectionConfig, ConnectionEntry},
    },
    dsn::{redact_dsn, Dsn},
    service::{ConnectionResolver, SafetyGuard},
    Error, Result,
};
use std::path::Path;

#[derive(Args)]
#[command(
    about = "Inspect and edit configured named connections.",
    long_about = "Connection commands list DBTOOL_CONN_* environment names and atomically edit only the default connections.toml file. Commands redact DSNs in every output. Add and remove require --allow-write; replacing or removing an entry additionally requires a target- and content-bound --confirm token."
)]
pub struct ConnCmd {
    #[command(subcommand)]
    pub action: ConnAction,
}

#[derive(Subcommand)]
pub enum ConnAction {
    /// List environment-managed and file-managed connection names
    List,
    /// Add one named connection to the default connections.toml
    Add(AddConnectionArgs),
    /// Remove one file-managed connection after confirmation
    Remove(RemoveConnectionArgs),
}

#[derive(Args)]
pub struct AddConnectionArgs {
    /// Portable connection name: lowercase letters, digits, and interior '-'
    pub name: String,
    /// DSN or DSN template; ${ENV_NAME} placeholders are stored verbatim
    pub dsn: String,
    /// Mark the named connection as read-only for data commands
    #[arg(long)]
    pub readonly: bool,
    /// Replace an existing file entry after target/content-bound confirmation
    #[arg(long)]
    pub replace: bool,
}

#[derive(Args)]
pub struct RemoveConnectionArgs {
    /// File-managed connection name to remove
    pub name: String,
}

pub async fn run(ctx: &Context, cmd: ConnCmd) -> Result<String> {
    match cmd.action {
        ConnAction::List => list(ctx),
        ConnAction::Add(args) => add(ctx, args),
        ConnAction::Remove(args) => remove(ctx, args),
    }
}

fn list(ctx: &Context) -> Result<String> {
    let schemes = ctx.registry.supported_schemes();
    let config_path = ConnectionConfig::default_path();
    let config = ConnectionConfig::load(&config_path)?;
    let mut env_connections: Vec<_> = discover_env_connections().keys().cloned().collect();
    env_connections.sort();

    let mut file_connections: Vec<_> = config
        .connections
        .iter()
        .map(|(name, entry)| {
            serde_json::json!({
                "name": name,
                "dsn": redact_dsn(&entry.dsn),
                "readonly": entry.readonly.unwrap_or(false),
            })
        })
        .collect();
    file_connections.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));

    Ok(ctx.render_success(
        "registry",
        serde_json::json!({
            "supported_schemes": schemes,
            "config_path": config_path,
            "env_connections": env_connections,
            "file_connections": file_connections,
        }),
        0,
        false,
    ))
}

fn add(ctx: &Context, args: AddConnectionArgs) -> Result<String> {
    require_config_write(ctx)?;
    validate_name(&args.name)?;
    ensure_not_environment_managed(&args.name)?;
    validate_dsn_template(ctx, &args.dsn)?;

    let config_path = ConnectionConfig::default_path();
    let mut config = ConnectionConfig::load(&config_path)?;
    let existing = config.connections.get(&args.name).cloned();
    if existing.is_some() && !args.replace {
        return Err(Error::Config(format!(
            "connection '{}' already exists; use --replace to request an overwrite",
            args.name
        )));
    }

    let entry = ConnectionEntry {
        dsn: args.dsn,
        readonly: args.readonly.then_some(true),
        // `conn add` intentionally does not expose limit editing. A replace
        // therefore carries the prior per-connection policy forward.
        limits: existing.as_ref().and_then(|entry| entry.limits.clone()),
    };
    if let Some(existing) = existing.as_ref() {
        let scope = SafetyGuard::confirmation_scope_digest(&(
            "replace-connection-v1",
            config_path.to_string_lossy().as_ref(),
            args.name.as_str(),
            existing,
            &entry,
        ))?;
        SafetyGuard::check_destructive_operation_with_scope(
            "REPLACE_CONNECTION",
            &args.name,
            &config_target(&config_path),
            &scope,
            ctx.allow_write,
            ctx.confirm.as_deref(),
        )?;
    }

    let redacted = redact_dsn(&entry.dsn);
    config.connections.insert(args.name.clone(), entry);
    config.save_atomic(&config_path)?;

    Ok(ctx.render_success(
        "connection",
        serde_json::json!({
            "action": if existing.is_some() { "replaced" } else { "added" },
            "name": args.name,
            "dsn": redacted,
            "readonly": args.readonly,
            "config_path": config_path,
            "serialization": serialization_boundary(),
        }),
        0,
        false,
    ))
}

fn remove(ctx: &Context, args: RemoveConnectionArgs) -> Result<String> {
    require_config_write(ctx)?;
    validate_name(&args.name)?;
    ensure_not_environment_managed(&args.name)?;

    let config_path = ConnectionConfig::default_path();
    let mut config = ConnectionConfig::load(&config_path)?;
    let existing = config
        .connections
        .get(&args.name)
        .cloned()
        .ok_or_else(|| Error::Config(format!("connection '{}' does not exist", args.name)))?;
    let scope = SafetyGuard::confirmation_scope_digest(&(
        "remove-connection-v1",
        config_path.to_string_lossy().as_ref(),
        args.name.as_str(),
        &existing,
    ))?;
    SafetyGuard::check_destructive_operation_with_scope(
        "REMOVE_CONNECTION",
        &args.name,
        &config_target(&config_path),
        &scope,
        ctx.allow_write,
        ctx.confirm.as_deref(),
    )?;

    config.connections.remove(&args.name);
    config.save_atomic(&config_path)?;
    Ok(ctx.render_success(
        "connection",
        serde_json::json!({
            "action": "removed",
            "name": args.name,
            "dsn": redact_dsn(&existing.dsn),
            "readonly": existing.readonly.unwrap_or(false),
            "config_path": config_path,
            "serialization": serialization_boundary(),
        }),
        0,
        false,
    ))
}

fn require_config_write(ctx: &Context) -> Result<()> {
    if ctx.allow_write {
        Ok(())
    } else {
        Err(Error::WriteNotAllowed)
    }
}

fn validate_name(name: &str) -> Result<()> {
    let bytes = name.as_bytes();
    let valid = (1..=64).contains(&bytes.len())
        && bytes[0].is_ascii_lowercase()
        && bytes[bytes.len() - 1].is_ascii_alphanumeric()
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-');
    if valid {
        Ok(())
    } else {
        Err(Error::Config(
            "connection name must be 1-64 lowercase ASCII characters, start with a letter, end with a letter or digit, and contain only letters, digits, or '-'".into(),
        ))
    }
}

fn ensure_not_environment_managed(name: &str) -> Result<()> {
    let env_key = ConnectionResolver::env_key(name);
    if std::env::var_os(&env_key).is_some() {
        return Err(Error::Config(format!(
            "connection '{name}' is managed by {env_key}; conn add/remove only edits connections.toml"
        )));
    }
    Ok(())
}

fn validate_dsn_template(ctx: &Context, raw: &str) -> Result<()> {
    let sanitized = sanitize_placeholders(raw)?;
    let parsed = Dsn::parse(&sanitized)
        .map_err(|_| Error::Dsn("connection DSN template is not a valid URL".into()))?;
    if ctx.registry.supported_schemes().contains(&parsed.scheme()) {
        Ok(())
    } else {
        Err(Error::UnsupportedScheme(parsed.scheme().to_owned()))
    }
}

fn sanitize_placeholders(raw: &str) -> Result<String> {
    if raw.is_empty() {
        return Err(Error::Dsn(
            "connection DSN template must not be empty".into(),
        ));
    }

    let mut sanitized = String::with_capacity(raw.len());
    let mut cursor = 0;
    while let Some(relative_start) = raw[cursor..].find("${") {
        let start = cursor + relative_start;
        sanitized.push_str(&raw[cursor..start]);
        let Some(relative_end) = raw[start + 2..].find('}') else {
            return Err(Error::Dsn(
                "connection DSN template has an unterminated environment placeholder".into(),
            ));
        };
        let end = start + 2 + relative_end;
        let variable = &raw[start + 2..end];
        let mut characters = variable.bytes();
        let valid = characters
            .next()
            .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
            && characters.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
        if !valid {
            return Err(Error::Dsn(
                "connection DSN template contains an invalid environment placeholder".into(),
            ));
        }
        // A numeric sentinel remains valid in credentials, host names, paths,
        // query values, and port positions. The original template—not this
        // validation-only string—is persisted.
        sanitized.push('1');
        cursor = end + 1;
    }
    sanitized.push_str(&raw[cursor..]);
    Ok(sanitized)
}

fn config_target(path: &Path) -> String {
    format!("config:{}", path.display())
}

fn serialization_boundary() -> serde_json::Value {
    serde_json::json!({
        "atomic": true,
        "mode": "0600-on-unix",
        "comments_preserved": false,
        "modeled_defaults_and_limits_preserved": true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn portable_names_are_strict_and_unambiguous() {
        for valid in ["a", "local", "prod-1"] {
            validate_name(valid).unwrap();
        }
        for invalid in ["", "A", "prod_1", "-prod", "prod-", "prod space"] {
            assert!(validate_name(invalid).is_err(), "accepted {invalid:?}");
        }
    }

    #[test]
    fn template_validation_never_expands_environment_values() {
        std::env::set_var("DBTOOL_CONN_TEMPLATE_TEST_SECRET", "do-not-persist");
        let sanitized = sanitize_placeholders(
            "postgres://user:${DBTOOL_CONN_TEMPLATE_TEST_SECRET}@host:${PORT}/db",
        )
        .unwrap();
        assert_eq!(sanitized, "postgres://user:1@host:1/db");
        assert!(!sanitized.contains("do-not-persist"));
    }

    #[test]
    fn malformed_template_placeholders_fail_without_echoing_input() {
        let error = sanitize_placeholders("postgres://user:${bad-name}@host/db").unwrap_err();
        assert!(!error.to_string().contains("bad-name"));
        assert!(sanitize_placeholders("postgres://${MISSING@host/db").is_err());
    }
}
