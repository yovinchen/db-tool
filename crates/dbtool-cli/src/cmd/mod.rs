pub mod caps;
pub mod conn;
pub mod cql;
pub mod db2;
pub mod doc;
pub mod kv;
pub mod mq;
pub mod ping;
pub mod search;
pub mod sql;
pub mod transfer;
pub mod ts;

use dbtool_core::service::{formatter::Format, ThrottleConfig};
use dbtool_core::{
    config::{ConnectionConfig, LimitsConfig},
    error::Error,
    service::ConnectionResolver,
};
use serde::Serialize;

pub struct Context {
    pub registry: dbtool_core::registry::Registry,
    pub conn: Option<String>,
    pub dsn: Option<String>,
    pub format: Format,
    pub limit: usize,
    pub throttle_overrides: LimitsConfig,
    pub allow_write: bool,
    pub confirm: Option<String>,
}

impl Context {
    pub fn ensure_positive_limit(&self) -> dbtool_core::Result<()> {
        if self.limit == 0 {
            return Err(Error::Config(
                "global --limit must be greater than zero".into(),
            ));
        }
        Ok(())
    }

    pub fn ensure_write_allowed(&self) -> dbtool_core::Result<()> {
        if !self.allow_write {
            return Err(Error::WriteNotAllowed);
        }

        let Some(name) = self.conn.as_deref() else {
            return Ok(());
        };
        if self.dsn.is_some() || std::env::var_os(ConnectionResolver::env_key(name)).is_some() {
            return Ok(());
        }

        let config = ConnectionConfig::load(&ConnectionConfig::default_path())?;
        if config
            .connections
            .get(name)
            .and_then(|entry| entry.readonly)
            .unwrap_or(false)
        {
            return Err(Error::ReadOnly);
        }

        Ok(())
    }

    /// Resolve the connection name/DSN from this context.
    pub fn resolve_dsn(&self) -> dbtool_core::Result<String> {
        if let Some(raw) = &self.dsn {
            return Ok(raw.clone());
        }
        if let Some(name) = &self.conn {
            let config = ConnectionConfig::load(&ConnectionConfig::default_path())?;
            return ConnectionResolver::new(config)
                .resolve(name)
                .map(|dsn| dsn.raw);
        }
        Err(dbtool_core::Error::Config("provide --conn or --dsn".into()))
    }

    pub fn throttle_config(&self) -> dbtool_core::Result<ThrottleConfig> {
        let config = ConnectionConfig::load(&ConnectionConfig::default_path())?;

        if self.dsn.is_some() {
            let mut throttle = config.throttle_config_for(None)?;
            self.throttle_overrides
                .apply_to_throttle(&mut throttle, "cli.limits")?;
            return Ok(throttle);
        }

        let connection = self.conn.as_deref().and_then(|name| {
            if std::env::var_os(ConnectionResolver::env_key(name)).is_some() {
                None
            } else {
                Some(name)
            }
        });

        let mut throttle = config.throttle_config_for(connection)?;
        self.throttle_overrides
            .apply_to_throttle(&mut throttle, "cli.limits")?;
        Ok(throttle)
    }

    pub fn safety_target(&self, resolved_dsn: &str) -> String {
        if let Some(name) = &self.conn {
            return format!("conn:{name}");
        }

        dbtool_core::dsn::Dsn::parse(resolved_dsn)
            .map(|dsn| format!("dsn:{}", dsn.redacted()))
            .unwrap_or_else(|_| "dsn:<unparsed>".to_owned())
    }

    pub fn render_success<T: Serialize>(
        &self,
        kind: &str,
        data: T,
        elapsed_ms: u64,
        truncated: bool,
    ) -> String {
        dbtool_core::service::Formatter::success_as(self.format, kind, data, elapsed_ms, truncated)
    }

    pub fn render_error(&self, err: &Error) -> String {
        dbtool_core::service::Formatter::error(err)
    }
}
