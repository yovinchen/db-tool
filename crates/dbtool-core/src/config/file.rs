use crate::{
    service::{write_file_atomically, Rate, ThrottleConfig},
    Error, Result,
};
use serde::{
    de::{MapAccess, Visitor},
    Deserialize, Deserializer, Serialize,
};
use std::collections::HashMap;
use std::fmt;
use std::io::Read;
use std::time::Duration;

pub const CONNECTION_CONFIG_MAX_BYTES: usize = 1024 * 1024;
pub const CONNECTION_CONFIG_MAX_ENTRIES: usize = 1024;
pub const CONNECTION_CONFIG_MAX_NAME_BYTES: usize = 256;
pub const CONNECTION_CONFIG_MAX_DSN_BYTES: usize = crate::dsn::MAX_DSN_BYTES;
pub const CONNECTION_CONFIG_MAX_SETTING_BYTES: usize = 256;
const CONNECTION_CONFIG_MAX_CONCURRENCY: usize = 1_000_000;
const CONNECTION_CONFIG_MAX_RETRIES: u32 = 1_000_000;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ConnectionConfig {
    #[serde(default)]
    pub defaults: Option<Defaults>,
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_connections_bounded")]
    pub connections: HashMap<String, ConnectionEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Defaults {
    pub limits: Option<LimitsConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct LimitsConfig {
    pub max_concurrency: Option<usize>,
    pub rate: Option<String>,
    pub acquire_timeout: Option<String>,
    pub request_timeout: Option<String>,
    pub overall_deadline: Option<String>,
    pub max_retries: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionEntry {
    pub dsn: String,
    #[serde(default, alias = "read_only")]
    pub readonly: Option<bool>,
    pub limits: Option<LimitsConfig>,
}

fn deserialize_connections_bounded<'de, D>(
    deserializer: D,
) -> std::result::Result<HashMap<String, ConnectionEntry>, D::Error>
where
    D: Deserializer<'de>,
{
    struct ConnectionsVisitor;

    impl<'de> Visitor<'de> for ConnectionsVisitor {
        type Value = HashMap<String, ConnectionEntry>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a bounded connection entry table")
        }

        fn visit_map<M>(self, mut map: M) -> std::result::Result<Self::Value, M::Error>
        where
            M: MapAccess<'de>,
        {
            let capacity = map
                .size_hint()
                .unwrap_or_default()
                .min(CONNECTION_CONFIG_MAX_ENTRIES);
            let mut connections = HashMap::with_capacity(capacity);
            while let Some(name) = map.next_key::<String>()? {
                if connections.len() == CONNECTION_CONFIG_MAX_ENTRIES {
                    return Err(serde::de::Error::custom(
                        "connection entry count exceeds the hard limit",
                    ));
                }
                if name.is_empty()
                    || name.len() > CONNECTION_CONFIG_MAX_NAME_BYTES
                    || name.chars().any(char::is_control)
                {
                    return Err(serde::de::Error::custom(
                        "connection entry name exceeds the hard field limit",
                    ));
                }
                let entry = map.next_value::<ConnectionEntry>()?;
                if entry.dsn.len() > CONNECTION_CONFIG_MAX_DSN_BYTES {
                    return Err(serde::de::Error::custom(
                        "connection entry DSN exceeds the hard field limit",
                    ));
                }
                if let Some(limits) = &entry.limits {
                    limits.validate_bounds().map_err(|_| {
                        serde::de::Error::custom("connection limits exceed a hard field limit")
                    })?;
                }
                if connections.insert(name, entry).is_some() {
                    return Err(serde::de::Error::custom(
                        "connection entry name is duplicated",
                    ));
                }
            }
            Ok(connections)
        }
    }

    deserializer.deserialize_map(ConnectionsVisitor)
}

impl ConnectionConfig {
    pub fn load(path: &std::path::Path) -> Result<Self> {
        let Some(bytes) = read_config_file_with(path, || {})? else {
            return Ok(Self::default());
        };
        let content = String::from_utf8(bytes)
            .map_err(|_| Error::Config("connection config must contain valid UTF-8 TOML".into()))?;
        // TOML parser diagnostics can quote the source line. A connection
        // entry commonly contains credentials, so never forward that
        // diagnostic across the public error boundary.
        let config: Self = toml::from_str(&content).map_err(|_| {
            crate::Error::Config(
                "connection config is invalid TOML or contains unsupported fields".into(),
            )
        })?;
        config.validate_bounds()?;
        Ok(config)
    }

    pub fn default_path() -> std::path::PathBuf {
        default_config_dir().join("dbtool").join("connections.toml")
    }

    /// Persist the complete configuration through a same-directory temporary
    /// file. The final rename is atomic on the supported local filesystems,
    /// and the temporary file is never treated as a usable configuration.
    ///
    /// TOML comments are not represented by this typed model and therefore
    /// cannot be retained by a successful write. All modeled defaults,
    /// connection limits, and unknown-field validation remain intact.
    pub fn save_atomic(&self, path: &std::path::Path) -> Result<()> {
        self.validate_bounds()?;
        let serialized_upper_bound = self.serialized_size_upper_bound()?;
        if serialized_upper_bound > CONNECTION_CONFIG_MAX_BYTES {
            return Err(serialized_config_size_error());
        }
        let mut content = String::with_capacity(serialized_upper_bound);
        self.serialize(toml::Serializer::pretty(&mut content))
            .map_err(|e| crate::Error::Serialization(e.to_string()))?;
        if content.len() > CONNECTION_CONFIG_MAX_BYTES {
            return Err(serialized_config_size_error());
        }
        write_file_atomically(path, content.as_bytes()).map_err(config_io_error)
    }

    fn serialized_size_upper_bound(&self) -> Result<usize> {
        // The fixed allowance covers table headers, field names, numeric
        // values, separators, and whitespace emitted by the pretty serializer.
        // String components are then charged at their worst TOML basic-string
        // representation before the serializer allocates its output buffer.
        const DOCUMENT_OVERHEAD: usize = 256;
        const ENTRY_OVERHEAD: usize = 256;
        const DEFAULTS_OVERHEAD: usize = 256;

        let mut total = DOCUMENT_OVERHEAD;
        if let Some(defaults) = &self.defaults {
            total = checked_serialized_add(total, DEFAULTS_OVERHEAD)?;
            if let Some(limits) = &defaults.limits {
                total = checked_serialized_add(total, limits.serialized_strings_upper_bound()?)?;
            }
        }
        for (name, entry) in &self.connections {
            total = checked_serialized_add(total, ENTRY_OVERHEAD)?;
            let encoded_name = toml_basic_string_upper_bound(name)?;
            total = checked_serialized_add(total, encoded_name)?;
            if entry.limits.is_some() {
                total = checked_serialized_add(total, encoded_name)?;
            }
            total = checked_serialized_add(total, toml_basic_string_upper_bound(&entry.dsn)?)?;
            if let Some(limits) = &entry.limits {
                total = checked_serialized_add(total, limits.serialized_strings_upper_bound()?)?;
            }
        }
        Ok(total)
    }

    pub fn validate_bounds(&self) -> Result<()> {
        if self.connections.len() > CONNECTION_CONFIG_MAX_ENTRIES {
            return Err(Error::Config(format!(
                "connection config exceeds the {CONNECTION_CONFIG_MAX_ENTRIES} connection entries limit"
            )));
        }

        let mut payload_bytes = 0_usize;
        if let Some(limits) = self
            .defaults
            .as_ref()
            .and_then(|defaults| defaults.limits.as_ref())
        {
            limits.validate_bounds()?;
            payload_bytes = add_config_payload_bytes(payload_bytes, limits.text_bytes()?)?;
        }
        for (name, entry) in &self.connections {
            if name.is_empty()
                || name.len() > CONNECTION_CONFIG_MAX_NAME_BYTES
                || name.chars().any(char::is_control)
            {
                return Err(Error::Config(format!(
                    "connection config contains a name outside the 1..={CONNECTION_CONFIG_MAX_NAME_BYTES}-byte non-control limit"
                )));
            }
            if entry.dsn.len() > CONNECTION_CONFIG_MAX_DSN_BYTES {
                return Err(Error::Config(format!(
                    "connection config contains a DSN exceeding the {CONNECTION_CONFIG_MAX_DSN_BYTES}-byte limit"
                )));
            }
            payload_bytes = add_config_payload_bytes(payload_bytes, name.len())?;
            payload_bytes = add_config_payload_bytes(payload_bytes, entry.dsn.len())?;
            if let Some(limits) = &entry.limits {
                limits.validate_bounds()?;
                payload_bytes = add_config_payload_bytes(payload_bytes, limits.text_bytes()?)?;
            }
        }
        Ok(())
    }

    pub fn throttle_config_for(&self, connection: Option<&str>) -> Result<ThrottleConfig> {
        self.validate_bounds()?;
        let mut config = ThrottleConfig::default();

        if let Some(limits) = self
            .defaults
            .as_ref()
            .and_then(|defaults| defaults.limits.as_ref())
        {
            limits.apply_to_throttle(&mut config, "defaults.limits")?;
        }

        if let Some(connection) = connection {
            if let Some(limits) = self
                .connections
                .get(connection)
                .and_then(|entry| entry.limits.as_ref())
            {
                limits
                    .apply_to_throttle(&mut config, &format!("connections.{connection}.limits"))?;
            }
        }

        Ok(config)
    }
}

fn read_config_file_with<F>(path: &std::path::Path, before_read: F) -> Result<Option<Vec<u8>>>
where
    F: FnOnce(),
{
    read_config_file_with_hooks(path, || {}, before_read)
}

fn read_config_file_with_hooks<B, R>(
    path: &std::path::Path,
    before_open: B,
    before_read: R,
) -> Result<Option<Vec<u8>>>
where
    B: FnOnce(),
    R: FnOnce(),
{
    let path_metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(config_read_error(error)),
    };
    if !path_metadata.file_type().is_file() {
        return Err(Error::Config(
            "connection config must be a regular file, not a directory, symlink, or device".into(),
        ));
    }
    ensure_config_file_size(path_metadata.len())?;

    before_open();
    let mut file = open_config_for_read(path)?;
    let opened_metadata = file.metadata().map_err(config_read_error)?;
    if !opened_metadata.file_type().is_file() {
        return Err(Error::Config(
            "connection config must remain a regular file while it is read".into(),
        ));
    }
    #[cfg(unix)]
    if !same_file_identity(&path_metadata, &opened_metadata) {
        return Err(Error::Config(
            "connection config changed before it could be opened; retry after writers finish"
                .into(),
        ));
    }
    let reopened_path_metadata = std::fs::symlink_metadata(path).map_err(config_read_error)?;
    if !reopened_path_metadata.file_type().is_file() {
        return Err(Error::Config(
            "connection config changed while it was opened; retry after writers finish".into(),
        ));
    }
    #[cfg(unix)]
    if !same_file_identity(&opened_metadata, &reopened_path_metadata) {
        return Err(Error::Config(
            "connection config changed while it was opened; retry after writers finish".into(),
        ));
    }
    ensure_config_file_size(opened_metadata.len())?;
    let expected_len = opened_metadata.len();
    before_read();

    let mut bytes = Vec::new();
    file.by_ref()
        .take((CONNECTION_CONFIG_MAX_BYTES as u64) + 1)
        .read_to_end(&mut bytes)
        .map_err(config_read_error)?;
    if bytes.len() > CONNECTION_CONFIG_MAX_BYTES {
        return Err(config_size_error());
    }
    let final_len = file.metadata().map_err(config_read_error)?.len();
    if final_len != expected_len || u64::try_from(bytes.len()).ok() != Some(expected_len) {
        return Err(Error::Config(
            "connection config changed while being read; retry after writers finish".into(),
        ));
    }
    Ok(Some(bytes))
}

fn open_config_for_read(path: &std::path::Path) -> Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);

    // Avoid following a path that is swapped to a symlink and avoid blocking
    // forever if it is swapped to a FIFO between metadata and open.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        use std::os::unix::fs::OpenOptionsExt;
        const O_NOFOLLOW: i32 = 0o400000;
        const O_NONBLOCK: i32 = 0o4000;
        options.custom_flags(O_NOFOLLOW | O_NONBLOCK);
    }
    #[cfg(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    ))]
    {
        use std::os::unix::fs::OpenOptionsExt;
        const O_NOFOLLOW: i32 = 0x100;
        const O_NONBLOCK: i32 = 0x4;
        options.custom_flags(O_NOFOLLOW | O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }

    options.open(path).map_err(config_read_error)
}

#[cfg(unix)]
fn same_file_identity(left: &std::fs::Metadata, right: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    left.dev() == right.dev() && left.ino() == right.ino()
}

fn ensure_config_file_size(bytes: u64) -> Result<()> {
    if bytes > CONNECTION_CONFIG_MAX_BYTES as u64 {
        Err(config_size_error())
    } else {
        Ok(())
    }
}

fn config_size_error() -> Error {
    Error::Config(format!(
        "connection config exceeds the {CONNECTION_CONFIG_MAX_BYTES}-byte size limit"
    ))
}

fn serialized_config_size_error() -> Error {
    Error::Config(format!(
        "serialized connection config exceeds the {CONNECTION_CONFIG_MAX_BYTES}-byte size limit"
    ))
}

fn checked_serialized_add(total: usize, bytes: usize) -> Result<usize> {
    total
        .checked_add(bytes)
        .ok_or_else(|| Error::Config("serialized connection config size overflow".into()))
}

fn toml_basic_string_upper_bound(value: &str) -> Result<usize> {
    value.chars().try_fold(2_usize, |total, character| {
        let encoded = if character.is_control() {
            // TOML can represent any control scalar with at most `\U` plus
            // eight hexadecimal digits. Common controls use shorter escapes.
            10
        } else if matches!(character, '"' | '\\') {
            2
        } else {
            character.len_utf8()
        };
        checked_serialized_add(total, encoded)
    })
}

fn config_read_error(error: std::io::Error) -> Error {
    Error::Config(format!("unable to read connection config: {error}"))
}

fn add_config_payload_bytes(total: usize, bytes: usize) -> Result<usize> {
    let total = total
        .checked_add(bytes)
        .ok_or_else(|| Error::Config("connection config payload size overflow".into()))?;
    if total > CONNECTION_CONFIG_MAX_BYTES {
        return Err(config_size_error());
    }
    Ok(total)
}

fn config_io_error(error: std::io::Error) -> Error {
    Error::Config(format!("unable to persist connection config: {error}"))
}

impl LimitsConfig {
    fn validate_bounds(&self) -> Result<()> {
        if self
            .max_concurrency
            .is_some_and(|value| value > CONNECTION_CONFIG_MAX_CONCURRENCY)
            || self
                .max_retries
                .is_some_and(|value| value > CONNECTION_CONFIG_MAX_RETRIES)
        {
            return Err(Error::Config(
                "connection config contains a numeric limit setting above the hard maximum".into(),
            ));
        }
        for value in [
            self.rate.as_deref(),
            self.acquire_timeout.as_deref(),
            self.request_timeout.as_deref(),
            self.overall_deadline.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            if value.len() > CONNECTION_CONFIG_MAX_SETTING_BYTES {
                return Err(Error::Config(format!(
                    "connection config contains a limit setting exceeding the {CONNECTION_CONFIG_MAX_SETTING_BYTES}-byte limit"
                )));
            }
        }
        Ok(())
    }

    fn text_bytes(&self) -> Result<usize> {
        [
            self.rate.as_deref(),
            self.acquire_timeout.as_deref(),
            self.request_timeout.as_deref(),
            self.overall_deadline.as_deref(),
        ]
        .into_iter()
        .flatten()
        .try_fold(0_usize, |total, value| {
            total
                .checked_add(value.len())
                .ok_or_else(|| Error::Config("connection limit setting size overflow".into()))
        })
    }

    fn serialized_strings_upper_bound(&self) -> Result<usize> {
        [
            self.rate.as_deref(),
            self.acquire_timeout.as_deref(),
            self.request_timeout.as_deref(),
            self.overall_deadline.as_deref(),
        ]
        .into_iter()
        .flatten()
        .try_fold(0_usize, |total, value| {
            checked_serialized_add(total, toml_basic_string_upper_bound(value)?)
        })
    }

    pub fn apply_to_throttle(&self, config: &mut ThrottleConfig, scope: &str) -> Result<()> {
        self.validate_bounds()?;
        if let Some(max_concurrency) = self.max_concurrency {
            if max_concurrency == 0 {
                return Err(Error::Config(format!(
                    "{scope}.max_concurrency must be greater than 0"
                )));
            }
            config.max_concurrency = max_concurrency;
        }

        if let Some(rate) = &self.rate {
            config.rate = Some(parse_rate(rate, &format!("{scope}.rate"))?);
        }

        if let Some(acquire_timeout) = &self.acquire_timeout {
            config.acquire_timeout =
                parse_positive_duration(acquire_timeout, &format!("{scope}.acquire_timeout"))?;
        }

        if let Some(request_timeout) = &self.request_timeout {
            config.request_timeout =
                parse_positive_duration(request_timeout, &format!("{scope}.request_timeout"))?;
        }

        if let Some(overall_deadline) = &self.overall_deadline {
            config.overall_deadline =
                parse_optional_duration(overall_deadline, &format!("{scope}.overall_deadline"))?;
        }

        if let Some(max_retries) = self.max_retries {
            config.max_retries = max_retries;
        }

        Ok(())
    }
}

fn parse_positive_duration(value: &str, field: &str) -> Result<Duration> {
    let duration = parse_duration(value, field)?;
    if duration.is_zero() {
        return Err(Error::Config(format!("{field} must be greater than 0")));
    }
    Ok(duration)
}

fn parse_optional_duration(value: &str, field: &str) -> Result<Option<Duration>> {
    let trimmed = value.trim().to_ascii_lowercase();
    if matches!(trimmed.as_str(), "none" | "off" | "disabled") {
        return Ok(None);
    }
    Ok(Some(parse_positive_duration(value, field)?))
}

fn parse_duration(value: &str, field: &str) -> Result<Duration> {
    let trimmed = value.trim();
    let split_at = trimmed
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (amount, unit) = trimmed.split_at(split_at);
    if amount.is_empty() {
        return Err(Error::Config(format!(
            "{field} must be a duration like 500ms, 2s, 5m, or 1h"
        )));
    }

    let amount = amount
        .parse::<u64>()
        .map_err(|_| Error::Config(format!("{field} has invalid duration amount")))?;
    let duration = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "s" | "sec" | "secs" | "second" | "seconds" => Duration::from_secs(amount),
        "ms" | "millisecond" | "milliseconds" => Duration::from_millis(amount),
        "m" | "min" | "mins" | "minute" | "minutes" => Duration::from_secs(
            amount
                .checked_mul(60)
                .ok_or_else(|| Error::Config(format!("{field} duration is too large")))?,
        ),
        "h" | "hr" | "hrs" | "hour" | "hours" => Duration::from_secs(
            amount
                .checked_mul(60 * 60)
                .ok_or_else(|| Error::Config(format!("{field} duration is too large")))?,
        ),
        _ => Err(Error::Config(format!(
            "{field} must use one of: ms, s, m, h"
        )))?,
    };

    if std::time::Instant::now().checked_add(duration).is_none() {
        return Err(Error::Config(format!(
            "{field} duration exceeds the platform timer range"
        )));
    }
    Ok(duration)
}

fn parse_rate(value: &str, field: &str) -> Result<Rate> {
    let (count, unit) = value
        .trim()
        .split_once('/')
        .ok_or_else(|| Error::Config(format!("{field} must be a rate like 50/s or 120/m")))?;
    let count = count
        .trim()
        .parse::<u32>()
        .map_err(|_| Error::Config(format!("{field} has invalid rate amount")))?;
    if count == 0 {
        return Err(Error::Config(format!("{field} must be greater than 0")));
    }

    match unit.trim().to_ascii_lowercase().as_str() {
        "s" | "sec" | "secs" | "second" | "seconds" => Rate::per_second(count)
            .ok_or_else(|| Error::Config(format!("{field} must be greater than 0"))),
        "m" | "min" | "mins" | "minute" | "minutes" => Rate::per_minute(count)
            .ok_or_else(|| Error::Config(format!("{field} must be greater than 0"))),
        _ => Err(Error::Config(format!(
            "{field} must use /s, /sec, /m, or /min"
        ))),
    }
}

fn default_config_dir() -> std::path::PathBuf {
    if let Some(path) = std::env::var_os("XDG_CONFIG_HOME") {
        return std::path::PathBuf::from(path);
    }

    #[cfg(target_os = "windows")]
    {
        if let Some(path) = std::env::var_os("APPDATA") {
            return std::path::PathBuf::from(path);
        }
    }

    if let Some(home) = std::env::var_os("HOME") {
        return std::path::PathBuf::from(home).join(".config");
    }

    std::path::PathBuf::from(".")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static CONFIG_TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn missing_config_loads_as_empty() {
        let path = std::env::temp_dir().join("dbtool-missing-config-for-test.toml");
        let config = ConnectionConfig::load(&path).unwrap();

        assert!(config.connections.is_empty());
    }

    #[test]
    fn parses_named_connections() {
        let path = std::env::temp_dir().join(format!("dbtool-config-{}.toml", std::process::id()));
        std::fs::write(
            &path,
            r#"
[connections.local]
dsn = "sqlite::memory:"
readonly = true
"#,
        )
        .unwrap();

        let config = ConnectionConfig::load(&path).unwrap();
        std::fs::remove_file(&path).ok();

        let local = config.connections.get("local").unwrap();
        assert_eq!(local.dsn, "sqlite::memory:");
        assert_eq!(local.readonly, Some(true));
    }

    #[test]
    fn merges_default_and_connection_limits() {
        let config: ConnectionConfig = toml::from_str(
            r#"
[defaults.limits]
max_concurrency = 8
rate = "50/s"
acquire_timeout = "2s"
request_timeout = "10s"
overall_deadline = "15s"
max_retries = 3

[connections.prod]
dsn = "postgres://example.invalid/app"

[connections.prod.limits]
max_concurrency = 2
rate = "120/min"
request_timeout = "5s"
overall_deadline = "none"
"#,
        )
        .unwrap();

        let throttle = config.throttle_config_for(Some("prod")).unwrap();

        assert_eq!(throttle.max_concurrency, 2);
        assert_eq!(throttle.rate, Rate::per_minute(120));
        assert_eq!(throttle.acquire_timeout, Duration::from_secs(2));
        assert_eq!(throttle.request_timeout, Duration::from_secs(5));
        assert_eq!(throttle.overall_deadline, None);
        assert_eq!(throttle.max_retries, 3);
    }

    #[test]
    fn default_limits_apply_without_connection_override() {
        let config: ConnectionConfig = toml::from_str(
            r#"
[defaults.limits]
rate = "25/s"
request_timeout = "750ms"

[connections.local]
dsn = "sqlite::memory:"
"#,
        )
        .unwrap();

        let throttle = config.throttle_config_for(Some("local")).unwrap();

        assert_eq!(throttle.rate, Rate::per_second(25));
        assert_eq!(throttle.request_timeout, Duration::from_millis(750));
        assert_eq!(
            throttle.max_concurrency,
            ThrottleConfig::default().max_concurrency
        );
    }

    #[test]
    fn invalid_limits_return_config_errors() {
        let config: ConnectionConfig = toml::from_str(
            r#"
[connections.local]
dsn = "sqlite::memory:"

[connections.local.limits]
max_concurrency = 0
"#,
        )
        .unwrap();

        let err = config.throttle_config_for(Some("local")).unwrap_err();

        assert!(matches!(err, Error::Config(message) if message.contains("max_concurrency")));
    }

    #[test]
    fn invalid_rate_unit_returns_config_error() {
        let config: ConnectionConfig = toml::from_str(
            r#"
[defaults.limits]
rate = "10/hour"
"#,
        )
        .unwrap();

        let err = config.throttle_config_for(None).unwrap_err();

        assert!(matches!(err, Error::Config(message) if message.contains("rate")));
    }

    #[test]
    fn read_only_remains_a_compatible_readonly_alias() {
        let config: ConnectionConfig = toml::from_str(
            r#"
[connections.archive]
dsn = "sqlite::memory:"
read_only = true
"#,
        )
        .unwrap();

        assert_eq!(config.connections["archive"].readonly, Some(true));
    }

    #[test]
    fn unknown_fields_are_rejected_at_every_config_scope() {
        let cases = [
            (
                "root_option",
                r#"
root_option = true
"#,
            ),
            (
                "limtis",
                r#"
[defaults]
limtis = true
"#,
            ),
            (
                "readonli",
                r#"
[connections.prod]
dsn = "postgres://127.0.0.1:1/app"
readonli = true
"#,
            ),
            (
                "request_timout",
                r#"
[connections.prod]
dsn = "postgres://127.0.0.1:1/app"

[connections.prod.limits]
request_timout = "1s"
"#,
            ),
        ];

        for (unknown_field, toml) in cases {
            let error = toml::from_str::<ConnectionConfig>(toml).unwrap_err();
            assert!(
                error.to_string().contains(unknown_field),
                "expected {unknown_field:?} in error: {error}"
            );
        }
    }

    #[test]
    fn duration_overflow_is_a_config_error_instead_of_a_panic() {
        for value in [format!("{}m", u64::MAX), format!("{}h", u64::MAX)] {
            let error = parse_duration(&value, "limits.request_timeout").unwrap_err();
            assert!(matches!(error, Error::Config(message) if message.contains("too large")));
        }

        let error =
            parse_duration(&format!("{}s", u64::MAX), "limits.request_timeout").unwrap_err();
        assert!(matches!(
            error,
            Error::Config(message) if message.contains("timer range")
        ));
    }

    #[test]
    fn atomic_save_preserves_modeled_defaults_and_connection_limits() {
        let root = unique_test_dir("atomic-save");
        let path = root.join("nested").join("connections.toml");
        let config: ConnectionConfig = toml::from_str(
            r#"
[defaults.limits]
max_concurrency = 7
request_timeout = "3s"

[connections.prod]
dsn = "postgres://user:secret@example.invalid/app"
readonly = true

[connections.prod.limits]
max_concurrency = 2
"#,
        )
        .unwrap();

        config.save_atomic(&path).unwrap();
        let reloaded = ConnectionConfig::load(&path).unwrap();
        assert_eq!(
            reloaded
                .defaults
                .as_ref()
                .and_then(|defaults| defaults.limits.as_ref())
                .and_then(|limits| limits.max_concurrency),
            Some(7)
        );
        assert_eq!(reloaded.connections["prod"].readonly, Some(true));
        assert_eq!(
            reloaded.connections["prod"]
                .limits
                .as_ref()
                .and_then(|limits| limits.max_concurrency),
            Some(2)
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn failure_before_replace_keeps_the_previous_config_and_removes_temp_file() {
        let root = unique_test_dir("atomic-failure");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("connections.toml");
        let original = b"[connections.old]\ndsn = \"sqlite::memory:\"\n";
        std::fs::write(&path, original).unwrap();

        let error = crate::service::atomic_file::write_file_atomically_with(
            &path,
            b"replacement",
            |_, _| Err(std::io::Error::other("injected before replace")),
        )
        .map_err(config_io_error)
        .unwrap_err();

        assert!(error.to_string().contains("unable to persist"));
        assert_eq!(std::fs::read(&path).unwrap(), original);
        let remaining: Vec<_> = std::fs::read_dir(&root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(remaining, [std::ffi::OsString::from("connections.toml")]);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn malformed_config_errors_do_not_quote_secret_source_lines() {
        let root = unique_test_dir("parse-redaction");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("connections.toml");
        std::fs::write(
            &path,
            "[connections.prod]\ndsn = \"postgres://user:credential-value@host/db\"\nunknown = true\n",
        )
        .unwrap();

        let error = ConnectionConfig::load(&path).unwrap_err();
        assert_eq!(error.code(), "CONFIG_ERROR");
        assert!(!error.to_string().contains("credential-value"));
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn config_load_rejects_oversized_growing_and_non_regular_files() {
        let root = unique_test_dir("bounded-load");
        std::fs::create_dir_all(&root).unwrap();
        let exact = root.join("exact.toml");
        std::fs::write(&exact, vec![b' '; CONNECTION_CONFIG_MAX_BYTES]).unwrap();
        assert!(ConnectionConfig::load(&exact)
            .unwrap()
            .connections
            .is_empty());

        let oversized = root.join("oversized.toml");
        std::fs::write(&oversized, vec![b' '; CONNECTION_CONFIG_MAX_BYTES + 1]).unwrap();
        let error = ConnectionConfig::load(&oversized).unwrap_err();
        assert!(matches!(error, Error::Config(message) if message.contains("size limit")));

        let directory = root.join("directory.toml");
        std::fs::create_dir(&directory).unwrap();
        let error = ConnectionConfig::load(&directory).unwrap_err();
        assert!(matches!(error, Error::Config(message) if message.contains("regular file")));

        let growing = root.join("growing.toml");
        std::fs::write(
            &growing,
            b"[connections.local]\ndsn = \"sqlite::memory:\"\n",
        )
        .unwrap();
        let error = read_config_file_with(&growing, || {
            use std::io::Write;
            std::fs::OpenOptions::new()
                .append(true)
                .open(&growing)
                .unwrap()
                .write_all(b"# grew")
                .unwrap();
        })
        .unwrap_err();
        assert!(
            matches!(error, Error::Config(message) if message.contains("changed while being read"))
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let swapped = root.join("swapped.toml");
            let replacement = root.join("replacement.toml");
            std::fs::write(&swapped, b"").unwrap();
            std::fs::write(&replacement, b"# replacement").unwrap();
            let error = read_config_file_with_hooks(
                &swapped,
                || {
                    std::fs::remove_file(&swapped).unwrap();
                    std::fs::rename(&replacement, &swapped).unwrap();
                },
                || {},
            )
            .unwrap_err();
            assert!(matches!(error, Error::Config(message) if message.contains("changed before")));

            let target = root.join("target.toml");
            let link = root.join("link.toml");
            std::fs::write(&target, b"").unwrap();
            symlink(&target, &link).unwrap();
            assert!(matches!(
                ConnectionConfig::load(&link),
                Err(Error::Config(message)) if message.contains("regular file")
            ));

            let raced = root.join("raced-link.toml");
            let raced_target = root.join("raced-target.toml");
            std::fs::write(&raced, b"").unwrap();
            std::fs::write(
                &raced_target,
                b"[connections.secret]\ndsn = \"postgres://user:RACE_SECRET@localhost/app\"\n",
            )
            .unwrap();
            let error = read_config_file_with_hooks(
                &raced,
                || {
                    std::fs::remove_file(&raced).unwrap();
                    symlink(&raced_target, &raced).unwrap();
                },
                || {},
            )
            .unwrap_err();
            assert_eq!(error.code(), "CONFIG_ERROR");
            assert!(!error.to_string().contains("RACE_SECRET"));
        }
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn config_model_bounds_cover_entries_names_dsns_and_limit_settings() {
        let entry = || ConnectionEntry {
            dsn: "sqlite::memory:".into(),
            readonly: None,
            limits: None,
        };
        let exact_entries = ConnectionConfig {
            defaults: None,
            connections: (0..CONNECTION_CONFIG_MAX_ENTRIES)
                .map(|index| (format!("exact-{index}"), entry()))
                .collect(),
        };
        exact_entries.validate_bounds().unwrap();

        let mut too_many = ConnectionConfig::default();
        for index in 0..=CONNECTION_CONFIG_MAX_ENTRIES {
            too_many
                .connections
                .insert(format!("connection-{index}"), entry());
        }
        assert!(matches!(
            too_many.validate_bounds(),
            Err(Error::Config(message)) if message.contains("connection entries")
        ));

        let root = unique_test_dir("bounded-entry-deserialize");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("connections.toml");
        let mut encoded = String::new();
        for index in 0..=CONNECTION_CONFIG_MAX_ENTRIES {
            encoded.push_str(&format!(
                "[connections.entry-{index}]\ndsn = \"sqlite::memory:\"\n"
            ));
        }
        std::fs::write(&path, encoded).unwrap();
        let error = ConnectionConfig::load(&path).unwrap_err();
        assert_eq!(error.code(), "CONFIG_ERROR");
        assert!(!error.to_string().contains("sqlite::memory:"));
        std::fs::remove_dir_all(root).ok();

        let mut oversized_name = ConnectionConfig::default();
        let mut exact_name = ConnectionConfig::default();
        exact_name
            .connections
            .insert("n".repeat(CONNECTION_CONFIG_MAX_NAME_BYTES), entry());
        exact_name.validate_bounds().unwrap();
        oversized_name
            .connections
            .insert("n".repeat(CONNECTION_CONFIG_MAX_NAME_BYTES + 1), entry());
        assert!(matches!(
            oversized_name.validate_bounds(),
            Err(Error::Config(message)) if message.contains("name")
        ));

        let mut oversized_dsn = ConnectionConfig::default();
        let mut exact_dsn = ConnectionConfig::default();
        exact_dsn.connections.insert(
            "local".into(),
            ConnectionEntry {
                dsn: "s".repeat(CONNECTION_CONFIG_MAX_DSN_BYTES),
                ..entry()
            },
        );
        exact_dsn.validate_bounds().unwrap();
        oversized_dsn.connections.insert(
            "local".into(),
            ConnectionEntry {
                dsn: "s".repeat(CONNECTION_CONFIG_MAX_DSN_BYTES + 1),
                ..entry()
            },
        );
        assert!(matches!(
            oversized_dsn.validate_bounds(),
            Err(Error::Config(message)) if message.contains("DSN")
        ));

        let mut oversized_setting = ConnectionConfig::default();
        let mut exact_setting = ConnectionConfig::default();
        exact_setting.connections.insert(
            "local".into(),
            ConnectionEntry {
                limits: Some(LimitsConfig {
                    rate: Some("r".repeat(CONNECTION_CONFIG_MAX_SETTING_BYTES)),
                    ..Default::default()
                }),
                ..entry()
            },
        );
        exact_setting.validate_bounds().unwrap();
        oversized_setting.connections.insert(
            "local".into(),
            ConnectionEntry {
                limits: Some(LimitsConfig {
                    rate: Some("r".repeat(CONNECTION_CONFIG_MAX_SETTING_BYTES + 1)),
                    ..Default::default()
                }),
                ..entry()
            },
        );
        assert!(matches!(
            oversized_setting.validate_bounds(),
            Err(Error::Config(message)) if message.contains("limit setting")
        ));
    }

    #[test]
    fn oversized_save_fails_before_atomic_publication_and_keeps_old_file() {
        let root = unique_test_dir("bounded-save");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("connections.toml");
        let original = b"[connections.old]\ndsn = \"sqlite::memory:\"\n";
        std::fs::write(&path, original).unwrap();
        let config = ConnectionConfig {
            defaults: None,
            connections: HashMap::from([(
                "new".into(),
                ConnectionEntry {
                    dsn: "x".repeat(CONNECTION_CONFIG_MAX_DSN_BYTES + 1),
                    readonly: None,
                    limits: None,
                },
            )]),
        };

        let error = config.save_atomic(&path).unwrap_err();
        assert!(matches!(error, Error::Config(message) if message.contains("DSN")));
        assert_eq!(std::fs::read(&path).unwrap(), original);
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn escaped_serialization_is_preflight_bounded_and_keeps_the_old_file() {
        let root = unique_test_dir("bounded-escaped-save");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("connections.toml");
        let original = b"[connections.old]\ndsn = \"sqlite::memory:\"\n";
        std::fs::write(&path, original).unwrap();

        let escaped_dsn = "\u{1}".repeat(CONNECTION_CONFIG_MAX_DSN_BYTES);
        let mut config = ConnectionConfig::default();
        for index in 0..11 {
            config.connections.insert(
                format!("escaped-{index}"),
                ConnectionEntry {
                    dsn: escaped_dsn.clone(),
                    readonly: Some(index % 2 == 0),
                    limits: None,
                },
            );
        }
        assert!(toml::to_string_pretty(&config).unwrap().len() > CONNECTION_CONFIG_MAX_BYTES);
        assert!(config.serialized_size_upper_bound().unwrap() > CONNECTION_CONFIG_MAX_BYTES);

        let error = config.save_atomic(&path).unwrap_err();
        assert!(matches!(error, Error::Config(message) if message.contains("serialized")));
        assert_eq!(std::fs::read(&path).unwrap(), original);
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn serialization_preflight_is_an_upper_bound_for_modeled_fields() {
        let limits = LimitsConfig {
            max_concurrency: Some(CONNECTION_CONFIG_MAX_CONCURRENCY),
            rate: Some("quoted-\"-slash-\\-control-\u{7f}-unicode-雪".into()),
            acquire_timeout: Some("999ms".into()),
            request_timeout: Some("8s".into()),
            overall_deadline: Some("9m".into()),
            max_retries: Some(CONNECTION_CONFIG_MAX_RETRIES),
        };
        let config = ConnectionConfig {
            defaults: Some(Defaults {
                limits: Some(limits.clone()),
            }),
            connections: HashMap::from([(
                "quoted.\".雪".into(),
                ConnectionEntry {
                    dsn: "postgres://user:\u{1}@localhost/雪?mode=read-only".into(),
                    readonly: Some(true),
                    limits: Some(limits),
                },
            )]),
        };
        config.validate_bounds().unwrap();
        let actual = toml::to_string_pretty(&config).unwrap().len();
        assert!(actual <= config.serialized_size_upper_bound().unwrap());
    }

    fn unique_test_dir(label: &str) -> std::path::PathBuf {
        let sequence = CONFIG_TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "dbtool-config-{label}-{}-{sequence}",
            std::process::id()
        ))
    }
}
