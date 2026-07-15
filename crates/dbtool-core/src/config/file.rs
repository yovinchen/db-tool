use crate::{
    service::{Rate, ThrottleConfig},
    Error, Result,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static CONFIG_TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ConnectionConfig {
    #[serde(default)]
    pub defaults: Option<Defaults>,
    #[serde(default)]
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

impl ConnectionConfig {
    pub fn load(path: &std::path::Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let content =
            std::fs::read_to_string(path).map_err(|e| crate::Error::Config(e.to_string()))?;
        // TOML parser diagnostics can quote the source line. A connection
        // entry commonly contains credentials, so never forward that
        // diagnostic across the public error boundary.
        toml::from_str(&content).map_err(|_| {
            crate::Error::Config(
                "connection config is invalid TOML or contains unsupported fields".into(),
            )
        })
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
        let content =
            toml::to_string_pretty(self).map_err(|e| crate::Error::Serialization(e.to_string()))?;
        write_atomic_config(path, content.as_bytes(), || Ok(()))
    }

    pub fn throttle_config_for(&self, connection: Option<&str>) -> Result<ThrottleConfig> {
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

fn write_atomic_config<F>(path: &std::path::Path, content: &[u8], before_rename: F) -> Result<()>
where
    F: FnOnce() -> std::io::Result<()>,
{
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    std::fs::create_dir_all(parent).map_err(config_io_error)?;

    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("connections.toml");
    let mut temporary = None;
    let mut file = None;
    for _ in 0..128 {
        let sequence = CONFIG_TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let candidate = parent.join(format!(
            ".{file_name}.tmp-{}-{sequence}",
            std::process::id()
        ));
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&candidate) {
            Ok(opened) => {
                temporary = Some(candidate);
                file = Some(opened);
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(config_io_error(error)),
        }
    }

    let temporary = temporary.ok_or_else(|| {
        Error::Config("unable to allocate a temporary connection config file".into())
    })?;
    let mut file = file.expect("temporary path and file are created together");
    let mut renamed = false;
    let result = (|| -> std::io::Result<()> {
        file.write_all(content)?;
        file.sync_all()?;
        drop(file);
        before_rename()?;
        replace_config_file(&temporary, path)?;
        renamed = true;
        sync_config_parent(parent)?;
        Ok(())
    })();

    if !renamed {
        let _ = std::fs::remove_file(&temporary);
    }
    result.map_err(config_io_error)
}

#[cfg(not(windows))]
fn replace_config_file(source: &std::path::Path, target: &std::path::Path) -> std::io::Result<()> {
    std::fs::rename(source, target)
}

#[cfg(windows)]
fn replace_config_file(source: &std::path::Path, target: &std::path::Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    const MOVEFILE_REPLACE_EXISTING: u32 = 0x0000_0001;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x0000_0008;

    #[link(name = "Kernel32")]
    extern "system" {
        fn MoveFileExW(
            existing_file_name: *const u16,
            new_file_name: *const u16,
            flags: u32,
        ) -> i32;
    }

    let source = source
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let target = target
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    // SAFETY: Both paths are live, NUL-terminated UTF-16 buffers for the
    // duration of the call. No buffer is mutated by MoveFileExW.
    let moved = unsafe {
        MoveFileExW(
            source.as_ptr(),
            target.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if moved == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn sync_config_parent(parent: &std::path::Path) -> std::io::Result<()> {
    std::fs::File::open(parent)?.sync_all()
}

#[cfg(not(unix))]
fn sync_config_parent(_parent: &std::path::Path) -> std::io::Result<()> {
    // The Windows replacement uses MOVEFILE_WRITE_THROUGH. Opening a
    // directory as a regular File is not portable on Windows.
    Ok(())
}

fn config_io_error(error: std::io::Error) -> Error {
    Error::Config(format!("unable to persist connection config: {error}"))
}

impl LimitsConfig {
    pub fn apply_to_throttle(&self, config: &mut ThrottleConfig, scope: &str) -> Result<()> {
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
    fn failure_before_rename_keeps_the_previous_config_and_removes_temp_file() {
        let root = unique_test_dir("atomic-failure");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("connections.toml");
        let original = b"[connections.old]\ndsn = \"sqlite::memory:\"\n";
        std::fs::write(&path, original).unwrap();

        let error = write_atomic_config(&path, b"replacement", || {
            Err(std::io::Error::other("injected before rename"))
        })
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

    fn unique_test_dir(label: &str) -> std::path::PathBuf {
        let sequence = CONFIG_TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "dbtool-config-{label}-{}-{sequence}",
            std::process::id()
        ))
    }
}
