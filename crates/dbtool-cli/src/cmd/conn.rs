use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{
    config::{
        env::discover_env_connections_bounded,
        file::{ConnectionConfig, ConnectionEntry, CONNECTION_CONFIG_MAX_DSN_BYTES},
    },
    dsn::Dsn,
    service::{ConnectionResolver, Format, SafetyGuard},
    Error, Result,
};
use std::path::Path;

const CONNECTION_LIST_MAX_ITEMS: usize = 512;
const CONNECTION_LIST_MAX_BYTES: usize = 256 * 1024;
const CONNECTION_LIST_TABLE_MAX_CELL_BYTES: usize = 60 * 1024;

enum ConnectionListItem {
    Scheme(String),
    Environment(String),
    File {
        name: String,
        dsn: String,
        readonly: bool,
    },
}

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
    let caller_budget = ctx.read_budget()?;
    let max_items = caller_budget.max_items.min(CONNECTION_LIST_MAX_ITEMS);
    let max_bytes = caller_budget.max_bytes.min(CONNECTION_LIST_MAX_BYTES);

    let config_path = absolute_lexical_path(ConnectionConfig::default_path())?;
    let config = ConnectionConfig::load(&config_path)?;
    let mut schemes = ctx.registry.supported_schemes();
    schemes.sort();
    let mut env_connections: Vec<_> = discover_env_connections_bounded()?.into_keys().collect();
    env_connections.sort();
    let mut file_connections = config
        .connections
        .iter()
        .map(|(name, entry)| ConnectionListItem::File {
            name: name.clone(),
            dsn: redact_connection_dsn(&entry.dsn),
            readonly: entry.readonly.unwrap_or(false),
        })
        .collect::<Vec<_>>();
    file_connections.sort_by(|left, right| match (left, right) {
        (
            ConnectionListItem::File {
                name: left_name, ..
            },
            ConnectionListItem::File {
                name: right_name, ..
            },
        ) => left_name.cmp(right_name),
        _ => unreachable!("file connection catalog contains only file entries"),
    });

    let mut items =
        Vec::with_capacity(schemes.len() + env_connections.len() + file_connections.len());
    items.extend(
        schemes
            .into_iter()
            .map(|scheme| ConnectionListItem::Scheme(scheme.to_owned())),
    );
    items.extend(
        env_connections
            .into_iter()
            .map(ConnectionListItem::Environment),
    );
    items.extend(file_connections);
    render_bounded_connection_list(ctx, &config_path, &items, max_items, max_bytes)
}

fn render_bounded_connection_list(
    ctx: &Context,
    config_path: &Path,
    items: &[ConnectionListItem],
    max_items: usize,
    max_bytes: usize,
) -> Result<String> {
    let total = items.len();
    let empty = render_connection_list_prefix(ctx, config_path, items, 0, total > 0);
    let fixed_bytes = connection_list_output_bytes(&empty)?;
    if fixed_bytes > max_bytes {
        return Err(Error::ReadBudgetExceeded {
            subject: "connection list fixed metadata".into(),
            unit: "bytes",
            limit: max_bytes,
        });
    }

    if total <= max_items
        && (ctx.format != Format::Table || connection_list_table_full_render_safe(items)?)
    {
        let full = render_connection_list_prefix(ctx, config_path, items, total, false);
        if connection_list_output_bytes(&full)? <= max_bytes {
            return Ok(full);
        }
    }

    let item_ceiling =
        connection_list_preflight_ceiling(ctx.format, items, max_bytes - fixed_bytes)?
            .min(max_items);

    let mut lower = 0_usize;
    let mut upper = item_ceiling.min(total.saturating_sub(1));
    while lower < upper {
        let midpoint = lower + (upper - lower).div_ceil(2);
        let rendered = render_connection_list_prefix(ctx, config_path, items, midpoint, true);
        if connection_list_output_bytes(&rendered)? <= max_bytes {
            lower = midpoint;
        } else {
            upper = midpoint - 1;
        }
    }
    Ok(render_connection_list_prefix(
        ctx,
        config_path,
        items,
        lower,
        lower < total,
    ))
}

fn connection_list_table_full_render_safe(items: &[ConnectionListItem]) -> Result<bool> {
    let mut category_bytes = [0_usize; 3];
    for item in items {
        let (category, item_bytes) = connection_list_item_json_bytes(item)?;
        let item_charge = item_bytes
            .checked_mul(2)
            .ok_or_else(|| Error::Config("connection list size accounting overflow".into()))?;
        category_bytes[category] = category_bytes[category]
            .checked_add(item_charge)
            .ok_or_else(|| Error::Config("connection list size accounting overflow".into()))?;
        if category_bytes[category] > CONNECTION_LIST_TABLE_MAX_CELL_BYTES {
            return Ok(false);
        }
    }
    Ok(true)
}

fn connection_list_preflight_ceiling(
    format: Format,
    items: &[ConnectionListItem],
    remaining_bytes: usize,
) -> Result<usize> {
    let mut retained = 0_usize;
    let mut charged = 0_usize;
    let mut table_category_bytes = [0_usize; 3];
    for item in items {
        let (category, item_bytes) = connection_list_item_json_bytes(item)?;
        let item_charge = if format == Format::Table {
            // Generic table cells contain JSON arrays. Account once for JSON
            // and once more for table escaping/padding, then stay below the
            // formatter's dynamic-width boundary.
            item_bytes.checked_mul(2)
        } else {
            item_bytes.checked_add(2)
        }
        .ok_or_else(|| Error::Config("connection list size accounting overflow".into()))?;
        let next_charged = charged
            .checked_add(item_charge)
            .ok_or_else(|| Error::Config("connection list size accounting overflow".into()))?;
        let next_category = table_category_bytes[category]
            .checked_add(item_charge)
            .ok_or_else(|| Error::Config("connection list size accounting overflow".into()))?;
        if next_charged > remaining_bytes
            || (format == Format::Table && next_category > CONNECTION_LIST_TABLE_MAX_CELL_BYTES)
        {
            break;
        }
        charged = next_charged;
        table_category_bytes[category] = next_category;
        retained += 1;
    }
    Ok(retained)
}

fn connection_list_item_json_bytes(item: &ConnectionListItem) -> Result<(usize, usize)> {
    let (category, encoded) = match item {
        ConnectionListItem::Scheme(scheme) => (0, serde_json::to_vec(scheme)),
        ConnectionListItem::Environment(name) => (1, serde_json::to_vec(name)),
        ConnectionListItem::File {
            name,
            dsn,
            readonly,
        } => (
            2,
            serde_json::to_vec(&serde_json::json!({
                "name": name,
                "dsn": dsn,
                "readonly": readonly,
            })),
        ),
    };
    encoded
        .map(|bytes| (category, bytes.len()))
        .map_err(|error| Error::Serialization(error.to_string()))
}

fn render_connection_list_prefix(
    ctx: &Context,
    config_path: &Path,
    items: &[ConnectionListItem],
    retained: usize,
    truncated: bool,
) -> String {
    let mut schemes = Vec::new();
    let mut env_connections = Vec::new();
    let mut file_connections = Vec::new();
    for item in &items[..retained] {
        match item {
            ConnectionListItem::Scheme(scheme) => schemes.push(scheme),
            ConnectionListItem::Environment(name) => env_connections.push(name),
            ConnectionListItem::File {
                name,
                dsn,
                readonly,
            } => file_connections.push(serde_json::json!({
                "name": name,
                "dsn": dsn,
                "readonly": readonly,
            })),
        }
    }
    ctx.render_success(
        "registry",
        serde_json::json!({
            "supported_schemes": schemes,
            "config_path": rendered_config_path(config_path),
            "env_connections": env_connections,
            "file_connections": file_connections,
        }),
        0,
        truncated,
    )
}

fn connection_list_output_bytes(rendered: &str) -> Result<usize> {
    rendered
        .len()
        .checked_add(1)
        .ok_or_else(|| Error::Config("connection list output size overflow".into()))
}

fn add(ctx: &Context, args: AddConnectionArgs) -> Result<String> {
    require_config_write(ctx)?;
    validate_name(&args.name)?;
    ensure_not_environment_managed(&args.name)?;
    validate_dsn_template(ctx, &args.dsn)?;

    let config_path = absolute_lexical_path(ConnectionConfig::default_path())?;
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
        let config_identity = config_path_identity(&config_path);
        let scope = SafetyGuard::confirmation_scope_digest(&(
            "replace-connection-v1",
            config_identity.as_str(),
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

    let redacted = redact_connection_dsn(&entry.dsn);
    config.connections.insert(args.name.clone(), entry);
    config.save_atomic(&config_path)?;

    Ok(ctx.render_success(
        "connection",
        serde_json::json!({
            "action": if existing.is_some() { "replaced" } else { "added" },
            "name": args.name,
            "dsn": redacted,
            "readonly": args.readonly,
            "config_path": rendered_config_path(&config_path),
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

    let config_path = absolute_lexical_path(ConnectionConfig::default_path())?;
    let mut config = ConnectionConfig::load(&config_path)?;
    let existing = config
        .connections
        .get(&args.name)
        .cloned()
        .ok_or_else(|| Error::Config(format!("connection '{}' does not exist", args.name)))?;
    let config_identity = config_path_identity(&config_path);
    let scope = SafetyGuard::confirmation_scope_digest(&(
        "remove-connection-v1",
        config_identity.as_str(),
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
            "dsn": redact_connection_dsn(&existing.dsn),
            "readonly": existing.readonly.unwrap_or(false),
            "config_path": rendered_config_path(&config_path),
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
    if raw.len() > CONNECTION_CONFIG_MAX_DSN_BYTES {
        return Err(Error::Dsn(format!(
            "connection DSN template exceeds the {CONNECTION_CONFIG_MAX_DSN_BYTES}-byte limit"
        )));
    }
    let sanitized = sanitize_placeholders(raw)?;
    let parsed = Dsn::parse(&sanitized)
        .map_err(|_| Error::Dsn("connection DSN template is not a valid URL".into()))?;
    if ctx.registry.supported_schemes().contains(&parsed.scheme()) {
        Ok(())
    } else {
        Err(Error::UnsupportedScheme(parsed.scheme().to_owned()))
    }
}

fn redact_connection_dsn(raw: &str) -> String {
    let Ok(parsed) = sanitize_placeholders(raw).and_then(|sanitized| {
        Dsn::parse(&sanitized).map_err(|_| Error::Dsn("invalid configured DSN".into()))
    }) else {
        return "<invalid-dsn>".to_owned();
    };

    let (without_fragment, had_fragment) = raw
        .split_once('#')
        .map_or((raw, false), |(prefix, _)| (prefix, true));
    let (base, had_query) = without_fragment
        .split_once('?')
        .map_or((without_fragment, false), |(prefix, _)| (prefix, true));

    let mut redacted = if let Some(scheme_end) = base.find("://") {
        let authority_start = scheme_end + 3;
        let authority_end = base[authority_start..]
            .find('/')
            .map_or(base.len(), |offset| authority_start + offset);
        let authority = &base[authority_start..authority_end];
        if let Some(at) = authority.rfind('@') {
            let mut output = String::with_capacity(base.len());
            output.push_str(&base[..authority_start]);
            output.push_str("***@");
            output.push_str(&base[authority_start + at + 1..]);
            output
        } else {
            base.to_owned()
        }
    } else if parsed.scheme() == "sqlite" {
        base.to_owned()
    } else {
        format!("{}:<redacted>", parsed.scheme())
    };
    if had_query {
        redacted.push_str("?<redacted>");
    }
    if had_fragment {
        redacted.push_str("#<redacted>");
    }
    redacted
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
    format!("config:{}", rendered_config_path(path))
}

fn absolute_lexical_path(path: std::path::PathBuf) -> Result<std::path::PathBuf> {
    let resolved = std::path::absolute(path).map_err(|_| {
        Error::Config("cannot resolve the connection config working directory".into())
    })?;
    if !resolved.is_absolute() {
        return Err(Error::Config(
            "connection config path did not resolve to an absolute path".into(),
        ));
    }
    Ok(resolved)
}

fn rendered_config_path(path: &Path) -> String {
    let rendered = path.to_string_lossy();
    if !rendered.chars().any(char::is_control) {
        return rendered.into_owned();
    }

    let mut escaped = String::with_capacity(rendered.len());
    for character in rendered.chars() {
        if character.is_control() {
            escaped.extend(character.escape_default());
        } else {
            escaped.push(character);
        }
    }
    escaped
}

fn config_path_identity(path: &Path) -> String {
    if let Some(rendered) = path.to_str() {
        let mut encoded = String::with_capacity(rendered.len() + 5);
        encoded.push_str("utf8:");
        for character in rendered.chars() {
            if character == '\\' {
                encoded.push_str("\\\\");
            } else if character.is_control() {
                encoded.extend(character.escape_default());
            } else {
                encoded.push(character);
            }
        }
        return encoded;
    }

    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        format!("bytes:{}", hex_bytes(path.as_os_str().as_bytes()))
    }

    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        let mut encoded = String::from("wide:");
        for unit in path.as_os_str().encode_wide() {
            push_hex_u16(&mut encoded, unit);
        }
        encoded
    }

    #[cfg(not(any(unix, windows)))]
    "opaque:non-unicode-path".to_owned()
}

#[cfg(unix)]
fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[cfg(windows)]
fn push_hex_u16(encoded: &mut String, unit: u16) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for shift in [12, 8, 4, 0] {
        encoded.push(char::from(HEX[usize::from((unit >> shift) & 0x0f)]));
    }
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

    #[test]
    fn list_redaction_never_echoes_unparseable_configured_values() {
        let marker = "UNPARSEABLE_CONFIG_SECRET_MARKER";
        assert_eq!(redact_connection_dsn(marker), "<invalid-dsn>");
        assert!(!redact_connection_dsn(marker).contains(marker));
        let redacted = redact_connection_dsn("postgres://user:password@localhost/app");
        assert!(!redacted.contains("password"));

        let token = "NATS_USERNAME_TOKEN_MARKER";
        let redacted = redact_connection_dsn(&format!("nats://{token}@localhost:4222"));
        assert!(!redacted.contains(token));
        assert!(redacted.contains("***@localhost:4222"));

        let query = "UNKNOWN_AUTH_QUERY_MARKER";
        let redacted = redact_connection_dsn(&format!(
            "postgres://localhost/app?auth={query}&mode=readonly"
        ));
        assert!(!redacted.contains(query));
        assert!(redacted.ends_with("?<redacted>"));
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_config_paths_have_a_non_panicking_rendering() {
        use std::{ffi::OsString, os::unix::ffi::OsStringExt, path::PathBuf};

        let path = PathBuf::from(OsString::from_vec(b"config-\xff.toml".to_vec()));
        let rendered = rendered_config_path(&path);
        assert_eq!(rendered, "config-\u{fffd}.toml");
        assert_eq!(
            config_path_identity(&path),
            "bytes:636f6e6669672dff2e746f6d6c"
        );
    }

    #[test]
    fn config_path_identities_distinguish_controls_literals_and_non_utf8() {
        let control = Path::new("config-\u{1b}.toml");
        let literal = Path::new(r"config-\u{1b}.toml");
        assert_ne!(config_path_identity(control), config_path_identity(literal));
        assert!(!rendered_config_path(control).contains('\u{1b}'));

        #[cfg(unix)]
        {
            use std::{ffi::OsString, os::unix::ffi::OsStringExt, path::PathBuf};

            let raw = PathBuf::from(OsString::from_vec(b"config-\xff.toml".to_vec()));
            let replacement = Path::new("config-\u{fffd}.toml");
            assert_ne!(
                config_path_identity(&raw),
                config_path_identity(replacement)
            );
            assert!(config_path_identity(&raw).starts_with("bytes:"));
            assert!(config_path_identity(replacement).starts_with("utf8:"));
        }
    }

    #[test]
    fn ordinary_config_path_rendering_preserves_the_machine_readable_path() {
        let path = Path::new("/tmp/dbtool/connections.toml");
        assert_eq!(rendered_config_path(path), path.to_string_lossy());
        assert_eq!(config_target(path), "config:/tmp/dbtool/connections.toml");
    }

    #[test]
    fn relative_config_paths_resolve_to_absolute_lexical_paths() {
        let resolved = absolute_lexical_path("relative/dbtool/connections.toml".into()).unwrap();
        assert!(resolved.is_absolute());
    }

    #[cfg(windows)]
    #[test]
    fn windows_drive_relative_and_rooted_config_paths_resolve_absolutely() {
        for path in [r"C:dbtool\connections.toml", r"\dbtool\connections.toml"] {
            let resolved = absolute_lexical_path(path.into()).unwrap();
            assert!(resolved.is_absolute(), "did not resolve {path:?}");
        }
    }
}
