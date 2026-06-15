use crate::{Error, Result};
use std::collections::HashMap;
use url::Url;

/// Parsed DSN with ownership — passed by value into Factory to satisfy 'static.
#[derive(Debug, Clone)]
pub struct Dsn {
    /// Original (raw) string, with password intact (never log this).
    pub raw: String,
    pub scheme: String,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub database: Option<String>,
    pub params: HashMap<String, String>,
}

impl Dsn {
    pub fn parse(s: &str) -> Result<Self> {
        let expanded = expand_env(s);
        let url = Url::parse(&expanded).map_err(|e| Error::Dsn(format!("invalid URL: {e}")))?;

        let scheme = url.scheme().to_lowercase();
        let host = url.host_str().map(str::to_owned);
        let port = url.port();
        let username = if url.username().is_empty() {
            None
        } else {
            Some(url.username().to_owned())
        };
        let password = url.password().map(str::to_owned);
        let database = url.path().trim_start_matches('/').to_owned();
        let database = if database.is_empty() {
            None
        } else {
            Some(database)
        };

        let params: HashMap<String, String> = url
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();

        Ok(Dsn {
            raw: expanded,
            scheme,
            host,
            port,
            username,
            password,
            database,
            params,
        })
    }

    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    pub fn redacted(&self) -> String {
        super::redact::redact_dsn(&self.raw)
    }
}

fn expand_env(s: &str) -> String {
    // Replace ${VAR} with environment variable value, leaving unknown vars as-is.
    let mut result = s.to_owned();
    while let Some(start) = result.find("${") {
        let end = match result[start..].find('}') {
            Some(i) => start + i,
            None => break,
        };
        let var_name = &result[start + 2..end];
        if let Ok(val) = std::env::var(var_name) {
            result.replace_range(start..=end, &val);
        } else {
            break;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_basic_url_parts() {
        let dsn = Dsn::parse("postgres://user:pass@localhost:5432/app?sslmode=require").unwrap();

        assert_eq!(dsn.scheme, "postgres");
        assert_eq!(dsn.host.as_deref(), Some("localhost"));
        assert_eq!(dsn.port, Some(5432));
        assert_eq!(dsn.username.as_deref(), Some("user"));
        assert_eq!(dsn.password.as_deref(), Some("pass"));
        assert_eq!(dsn.database.as_deref(), Some("app"));
        assert_eq!(
            dsn.params.get("sslmode").map(String::as_str),
            Some("require")
        );
    }

    #[test]
    fn stores_expanded_raw_url_for_connectors() {
        std::env::set_var("DBTOOL_PARSE_TEST_PASSWORD", "secret-pass");
        let dsn = Dsn::parse("mysql://root:${DBTOOL_PARSE_TEST_PASSWORD}@localhost/app").unwrap();

        assert!(dsn.raw.contains("secret-pass"));
        assert!(!dsn.raw.contains("${DBTOOL_PARSE_TEST_PASSWORD}"));
        assert_eq!(dsn.password.as_deref(), Some("secret-pass"));
        assert!(!dsn.redacted().contains("secret-pass"));
    }
}
