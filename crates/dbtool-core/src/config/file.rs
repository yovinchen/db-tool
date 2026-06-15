use crate::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ConnectionConfig {
    pub defaults: Option<Defaults>,
    pub connections: HashMap<String, ConnectionEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Defaults {
    pub limits: Option<LimitsConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
}
