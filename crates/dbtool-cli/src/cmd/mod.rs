pub mod caps;
pub mod conn;
pub mod doc;
pub mod kv;
pub mod mq;
pub mod ping;
pub mod search;
pub mod sql;
pub mod ts;

use dbtool_core::service::formatter::Format;
use dbtool_core::{config::ConnectionConfig, service::ConnectionResolver};

pub struct Context {
    pub registry: dbtool_core::registry::Registry,
    pub conn: Option<String>,
    pub dsn: Option<String>,
    pub _format: Format,
    pub limit: usize,
    pub allow_write: bool,
    pub confirm: Option<String>,
}

impl Context {
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

    pub fn safety_target(&self, resolved_dsn: &str) -> String {
        if let Some(name) = &self.conn {
            return format!("conn:{name}");
        }

        dbtool_core::dsn::Dsn::parse(resolved_dsn)
            .map(|dsn| format!("dsn:{}", dsn.redacted()))
            .unwrap_or_else(|_| "dsn:<unparsed>".to_owned())
    }
}
