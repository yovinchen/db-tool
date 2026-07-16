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
    config::{file::CONNECTION_CONFIG_MAX_NAME_BYTES, ConnectionConfig, LimitsConfig},
    dsn::Dsn,
    error::Error,
    model::{MetadataBudget, ReadBudget, MAX_READ_BYTES},
    service::ConnectionResolver,
};
use serde::Serialize;

pub struct Context {
    pub registry: dbtool_core::registry::Registry,
    pub conn: Option<String>,
    pub dsn: Option<String>,
    pub format: Format,
    pub limit: usize,
    pub max_bytes: usize,
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

    /// Validate the global caller byte ceiling for every data command, even
    /// when the selected action does not yet consume a structured read budget.
    pub fn ensure_read_byte_budget(&self) -> dbtool_core::Result<()> {
        if self.max_bytes == 0 {
            return Err(Error::Config(
                "global --max-bytes must be greater than zero".to_owned(),
            ));
        }
        if self.max_bytes > MAX_READ_BYTES {
            return Err(Error::Config(format!(
                "global --max-bytes exceeds the hard {MAX_READ_BYTES}-byte ceiling"
            )));
        }
        Ok(())
    }

    /// Build the caller-owned envelope used by row, document, KV, and
    /// time-series reads. Validation happens before a connector is opened.
    pub fn read_budget(&self) -> dbtool_core::Result<ReadBudget> {
        ReadBudget::new(self.limit, self.max_bytes)
    }

    /// Build the caller-owned envelope used by complete schema and messaging
    /// administration responses. Validation happens before backend access.
    pub fn metadata_budget(&self) -> dbtool_core::Result<MetadataBudget> {
        MetadataBudget::new(self.limit, self.max_bytes)
    }

    pub fn ensure_write_allowed(&self) -> dbtool_core::Result<()> {
        if !self.allow_write {
            return Err(Error::WriteNotAllowed);
        }

        if self.dsn.is_some() {
            return Ok(());
        }
        let Some(name) = self.conn.as_deref() else {
            return Ok(());
        };
        if validate_connection_reference(name)? == ConnectionReferenceKind::RawDsn {
            return Ok(());
        }
        if std::env::var_os(ConnectionResolver::env_key(name)).is_some() {
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
            // Dsn::parse checks the hard raw-size ceiling before cloning and
            // also bounds every environment-expansion pass. Returning its
            // owned raw value prevents oversized CLI input from reaching the
            // registry/connect path.
            return Dsn::parse(raw).map(|dsn| dsn.raw);
        }
        if let Some(name) = &self.conn {
            if validate_connection_reference(name)? == ConnectionReferenceKind::RawDsn {
                return Dsn::parse(name).map(|dsn| dsn.raw);
            }
            let config = ConnectionConfig::load(&ConnectionConfig::default_path())?;
            return ConnectionResolver::new(config)
                .resolve(name)
                .map(|dsn| dsn.raw);
        }
        Err(dbtool_core::Error::Config("provide --conn or --dsn".into()))
    }

    pub fn throttle_config(&self) -> dbtool_core::Result<ThrottleConfig> {
        let connection_kind = if self.dsn.is_none() {
            self.conn
                .as_deref()
                .map(validate_connection_reference)
                .transpose()?
        } else {
            None
        };
        let config = ConnectionConfig::load(&ConnectionConfig::default_path())?;

        if self.dsn.is_some() {
            let mut throttle = config.throttle_config_for(None)?;
            self.throttle_overrides
                .apply_to_throttle(&mut throttle, "cli.limits")?;
            return Ok(throttle);
        }

        let connection = match (connection_kind, self.conn.as_deref()) {
            (Some(ConnectionReferenceKind::Name), Some(name))
                if std::env::var_os(ConnectionResolver::env_key(name)).is_none() =>
            {
                Some(name)
            }
            _ => None,
        };

        let mut throttle = config.throttle_config_for(connection)?;
        self.throttle_overrides
            .apply_to_throttle(&mut throttle, "cli.limits")?;
        Ok(throttle)
    }

    pub fn safety_target(&self, resolved_dsn: &str) -> String {
        if let Some(name) = &self.conn {
            if matches!(
                validate_connection_reference(name),
                Ok(ConnectionReferenceKind::Name)
            ) {
                return format!("conn:{name}");
            }
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum ConnectionReferenceKind {
    Name,
    RawDsn,
}

fn validate_connection_reference(value: &str) -> dbtool_core::Result<ConnectionReferenceKind> {
    if has_uri_scheme(value) {
        Dsn::parse(value)?;
        return Ok(ConnectionReferenceKind::RawDsn);
    }

    if value.is_empty()
        || value.len() > CONNECTION_CONFIG_MAX_NAME_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(Error::Config(
            "connection name is outside the supported field limit".into(),
        ));
    }
    Ok(ConnectionReferenceKind::Name)
}

fn has_uri_scheme(value: &str) -> bool {
    let Some((scheme, _)) = value.split_once(':') else {
        return false;
    };
    let mut characters = scheme.chars();
    characters
        .next()
        .is_some_and(|character| character.is_ascii_alphabetic())
        && characters.all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '+' | '-' | '.')
        })
}
