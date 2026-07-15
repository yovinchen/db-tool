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
        // 1. Treat any RFC-style URI scheme as a raw DSN. SQLite's canonical
        // in-memory form is `sqlite::memory:` and intentionally has no `//`.
        if has_uri_scheme(name_or_dsn) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_raw_sqlite_dsn_forms_without_treating_them_as_names() {
        let resolver = ConnectionResolver::new(ConnectionConfig::default());

        assert_eq!(
            resolver.resolve("sqlite::memory:").unwrap().scheme,
            "sqlite"
        );
        assert_eq!(
            resolver.resolve("sqlite:relative.db").unwrap().raw,
            "sqlite:relative.db"
        );
    }

    #[test]
    fn uri_scheme_detection_is_strict_and_named_connections_still_resolve() {
        assert!(has_uri_scheme("postgres://localhost/app"));
        assert!(has_uri_scheme("mongodb+srv://cluster/app"));
        assert!(!has_uri_scheme("local"));
        assert!(!has_uri_scheme("1invalid:value"));
        assert!(!has_uri_scheme("bad_scheme:value"));

        let config: ConnectionConfig = toml::from_str(
            r#"
[connections.local]
dsn = "sqlite::memory:"
"#,
        )
        .unwrap();
        assert_eq!(
            ConnectionResolver::new(config)
                .resolve("local")
                .unwrap()
                .scheme,
            "sqlite"
        );
    }
}
