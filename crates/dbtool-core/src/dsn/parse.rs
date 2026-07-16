use crate::{Error, Result};
use std::{collections::HashMap, fmt};
use url::Url;

/// Hard ceiling for raw and environment-expanded DSNs. Keeping this boundary
/// at the parser prevents callers from bypassing config or CLI-specific caps.
pub const MAX_DSN_BYTES: usize = 16 * 1024;

/// Parsed DSN with ownership — passed by value into Factory to satisfy 'static.
#[derive(Clone)]
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

/// Never expose the raw DSN, password, or secret query parameters through
/// diagnostic formatting. Connector errors commonly include `Debug` values,
/// so this boundary must be safe by construction rather than relying on every
/// caller to remember to redact first.
impl fmt::Debug for Dsn {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Dsn")
            .field("redacted", &self.redacted())
            .field("scheme", &self.scheme)
            .field("host", &self.host)
            .field("port", &self.port)
            .finish_non_exhaustive()
    }
}

impl Dsn {
    pub fn parse(s: &str) -> Result<Self> {
        ensure_dsn_size(s)?;
        let expanded = expand_env(s)?;
        let url = Url::parse(&expanded).map_err(|e| Error::Dsn(format!("invalid URL: {e}")))?;

        let scheme = url.scheme().to_lowercase();
        let host = url.host_str().map(str::to_owned);
        let port = url.port();
        let username = if url.username().is_empty() {
            None
        } else {
            Some(percent_decode(url.username())?)
        };
        let password = url.password().map(percent_decode).transpose()?;
        let database = percent_decode(url.path().trim_start_matches('/'))?;
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

    pub fn raw_with_scheme(&self, scheme: &str) -> Result<String> {
        let mut url = Url::parse(&self.raw).map_err(|e| Error::Dsn(format!("invalid URL: {e}")))?;
        url.set_scheme(scheme)
            .map_err(|_| Error::Dsn(format!("invalid URL scheme: {scheme}")))?;
        Ok(url.to_string())
    }
}

fn expand_env(s: &str) -> Result<String> {
    const MAX_EXPANSION_PASSES: usize = 32;

    // Multiple passes support variables whose values reference other
    // variables. Unknown variables remain literal, but do not prevent known
    // variables later in the DSN from being expanded.
    let mut current = s.to_owned();
    let mut seen = std::collections::HashSet::new();
    for _ in 0..MAX_EXPANSION_PASSES {
        if !seen.insert(current.clone()) {
            return Err(Error::Dsn(
                "cyclic environment-variable expansion in DSN".into(),
            ));
        }

        let (next, replaced_known) = expand_env_once(&current)?;
        if !replaced_known {
            return Ok(current);
        }
        if next == current {
            return Err(Error::Dsn(
                "cyclic environment-variable expansion in DSN".into(),
            ));
        }
        current = next;
    }

    Err(Error::Dsn(format!(
        "environment-variable expansion exceeds {MAX_EXPANSION_PASSES} passes"
    )))
}

fn expand_env_once(input: &str) -> Result<(String, bool)> {
    let mut output = String::with_capacity(input.len());
    let mut cursor = 0;
    let mut replaced_known = false;

    while let Some(relative_start) = input[cursor..].find("${") {
        let start = cursor + relative_start;
        push_dsn_fragment(&mut output, &input[cursor..start])?;
        let Some(relative_end) = input[start + 2..].find('}') else {
            push_dsn_fragment(&mut output, &input[start..])?;
            return Ok((output, replaced_known));
        };
        let end = start + 2 + relative_end;
        let variable = &input[start + 2..end];
        match std::env::var_os(variable) {
            Some(value) if value.len() > MAX_DSN_BYTES.saturating_sub(output.len()) => {
                return Err(dsn_size_error());
            }
            Some(value) => match value.into_string() {
                Ok(value) => {
                    push_dsn_fragment(&mut output, &value)?;
                    replaced_known = true;
                }
                Err(_) => push_dsn_fragment(&mut output, &input[start..=end])?,
            },
            None => push_dsn_fragment(&mut output, &input[start..=end])?,
        }
        cursor = end + 1;
    }
    push_dsn_fragment(&mut output, &input[cursor..])?;
    Ok((output, replaced_known))
}

fn push_dsn_fragment(output: &mut String, fragment: &str) -> Result<()> {
    let next_len = output
        .len()
        .checked_add(fragment.len())
        .ok_or_else(dsn_size_error)?;
    if next_len > MAX_DSN_BYTES {
        return Err(dsn_size_error());
    }
    output.push_str(fragment);
    Ok(())
}

fn ensure_dsn_size(value: &str) -> Result<()> {
    if value.len() > MAX_DSN_BYTES {
        Err(dsn_size_error())
    } else {
        Ok(())
    }
}

fn dsn_size_error() -> Error {
    Error::Dsn(format!(
        "DSN exceeds the {MAX_DSN_BYTES}-byte expanded size limit"
    ))
}

fn percent_decode(input: &str) -> Result<String> {
    let bytes = input.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let encoded = bytes
                .get(index + 1..index + 3)
                .ok_or_else(|| Error::Dsn("invalid percent escape in DSN component".into()))?;
            let encoded = std::str::from_utf8(encoded)
                .map_err(|_| Error::Dsn("invalid percent escape in DSN component".into()))?;
            let byte = u8::from_str_radix(encoded, 16)
                .map_err(|_| Error::Dsn("invalid percent escape in DSN component".into()))?;
            decoded.push(byte);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }

    String::from_utf8(decoded)
        .map_err(|_| Error::Dsn("invalid UTF-8 in percent-encoded DSN component".into()))
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
    fn percent_decodes_credentials_and_database() {
        let dsn =
            Dsn::parse("postgres://user%40tenant:p%40ss%3Aword@localhost/team%20database").unwrap();

        assert_eq!(dsn.username.as_deref(), Some("user@tenant"));
        assert_eq!(dsn.password.as_deref(), Some("p@ss:word"));
        assert_eq!(dsn.database.as_deref(), Some("team database"));
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

    #[test]
    fn can_rewrite_alias_scheme_for_driver_urls() {
        let dsn = Dsn::parse("mariadb://user:pass@localhost:3306/app?ssl-mode=disabled").unwrap();

        assert_eq!(
            dsn.raw_with_scheme("mysql").unwrap(),
            "mysql://user:pass@localhost:3306/app?ssl-mode=disabled"
        );
    }

    #[test]
    fn unknown_environment_variable_does_not_block_later_known_variable() {
        std::env::set_var("DBTOOL_PARSE_LATER_KNOWN", "expanded");
        let dsn = Dsn::parse(
            "postgres://localhost/db?unknown=${DBTOOL_PARSE_MISSING}&known=${DBTOOL_PARSE_LATER_KNOWN}",
        )
        .unwrap();

        assert_eq!(
            dsn.params.get("unknown").map(String::as_str),
            Some("${DBTOOL_PARSE_MISSING}")
        );
        assert_eq!(
            dsn.params.get("known").map(String::as_str),
            Some("expanded")
        );
    }

    #[test]
    fn cyclic_environment_variable_fails_instead_of_looping() {
        std::env::set_var(
            "DBTOOL_PARSE_SELF_REFERENCE",
            "${DBTOOL_PARSE_SELF_REFERENCE}",
        );
        let error =
            Dsn::parse("postgres://localhost/db?token=${DBTOOL_PARSE_SELF_REFERENCE}").unwrap_err();

        assert_eq!(error.code(), "INVALID_DSN");
        assert!(error.to_string().contains("cyclic"));
    }

    #[test]
    fn raw_and_expanded_dsns_enforce_the_same_hard_byte_limit() {
        let prefix = "postgres://localhost/";
        let exact = format!("{prefix}{}", "x".repeat(MAX_DSN_BYTES - prefix.len()));
        assert_eq!(exact.len(), MAX_DSN_BYTES);
        assert!(Dsn::parse(&exact).is_ok());

        let oversized = format!("{exact}x");
        let error = Dsn::parse(&oversized).unwrap_err();
        assert!(error.to_string().contains("expanded size limit"));
        assert!(!error.to_string().contains(&oversized));

        let variable = "DBTOOL_PARSE_BOUNDED_EXPANSION";
        let template = format!("{prefix}${{{variable}}}");
        std::env::set_var(variable, "y".repeat(MAX_DSN_BYTES - prefix.len()));
        assert_eq!(Dsn::parse(&template).unwrap().raw.len(), MAX_DSN_BYTES);

        let marker = "EXPANSION_SECRET_MARKER";
        std::env::set_var(variable, format!("{marker}{}", "z".repeat(MAX_DSN_BYTES)));
        let error = Dsn::parse(&template).unwrap_err();
        assert!(error.to_string().contains("expanded size limit"));
        assert!(!error.to_string().contains(marker));
        std::env::remove_var(variable);
    }

    #[test]
    fn debug_output_never_contains_password_or_secret_query_value() {
        let dsn = Dsn::parse(
            "postgres://user:plain-secret@localhost/db?token=query-secret&sslmode=require",
        )
        .unwrap();
        let debug = format!("{dsn:?}");

        assert!(!debug.contains("plain-secret"));
        assert!(!debug.contains("query-secret"));
        assert!(debug.contains("***"));
    }
}
