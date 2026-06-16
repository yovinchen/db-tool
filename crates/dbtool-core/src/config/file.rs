use crate::{
    service::{Rate, ThrottleConfig},
    Error, Result,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ConnectionConfig {
    #[serde(default)]
    pub defaults: Option<Defaults>,
    #[serde(default)]
    pub connections: HashMap<String, ConnectionEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Defaults {
    pub limits: Option<LimitsConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LimitsConfig {
    pub max_concurrency: Option<usize>,
    pub rate: Option<String>,
    pub acquire_timeout: Option<String>,
    pub request_timeout: Option<String>,
    pub overall_deadline: Option<String>,
    pub max_retries: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionEntry {
    pub dsn: String,
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
        toml::from_str(&content).map_err(|e| crate::Error::Config(e.to_string()))
    }

    pub fn default_path() -> std::path::PathBuf {
        default_config_dir().join("dbtool").join("connections.toml")
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
    match unit.trim().to_ascii_lowercase().as_str() {
        "" | "s" | "sec" | "secs" | "second" | "seconds" => Ok(Duration::from_secs(amount)),
        "ms" | "millisecond" | "milliseconds" => Ok(Duration::from_millis(amount)),
        "m" | "min" | "mins" | "minute" | "minutes" => Ok(Duration::from_secs(amount * 60)),
        "h" | "hr" | "hrs" | "hour" | "hours" => Ok(Duration::from_secs(amount * 60 * 60)),
        _ => Err(Error::Config(format!(
            "{field} must use one of: ms, s, m, h"
        ))),
    }
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
}
