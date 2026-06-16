use crate::{config::file::ConnectionConfig, dsn::Dsn, Error, Result};

/// Resolves a connection name or raw DSN into a `Dsn`, following priority order:
/// `--dsn` literal > env `DBTOOL_CONN_<NAME>` > `connections.toml` named entry.
pub struct ConnectionResolver {
    config: ConnectionConfig,
}

impl ConnectionResolver {
    pub fn new(config: ConnectionConfig) -> Self {
        Self { config }
    }

    pub fn env_key(name: &str) -> String {
        format!("DBTOOL_CONN_{}", name.to_uppercase().replace('-', "_"))
    }

    pub fn resolve(&self, name_or_dsn: &str) -> Result<Dsn> {
        // 1. Treat as a raw DSN if it contains "://".
        if name_or_dsn.contains("://") {
            return Dsn::parse(name_or_dsn);
        }

        // 2. Check env var DBTOOL_CONN_<UPPER_NAME>.
        let env_key = Self::env_key(name_or_dsn);
        if let Ok(dsn_str) = std::env::var(&env_key) {
            return Dsn::parse(&dsn_str);
        }

        // 3. Look up in connections.toml.
        if let Some(entry) = self.config.connections.get(name_or_dsn) {
            return Dsn::parse(&entry.dsn);
        }

        Err(Error::Config(format!(
            "connection '{}' not found",
            name_or_dsn
        )))
    }
}
