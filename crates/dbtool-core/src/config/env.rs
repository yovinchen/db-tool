use crate::{Error, Result};
use std::{collections::HashMap, ffi::OsString};

use super::file::{CONNECTION_CONFIG_MAX_DSN_BYTES, CONNECTION_CONFIG_MAX_NAME_BYTES};

pub const ENV_CONNECTION_MAX_ENTRIES: usize = 256;
pub const ENV_CONNECTION_MAX_DSN_BYTES: usize = CONNECTION_CONFIG_MAX_DSN_BYTES;
const ENV_CONNECTION_MAX_NAME_BYTES: usize = CONNECTION_CONFIG_MAX_NAME_BYTES;
const ENV_CONNECTION_MAX_TOTAL_BYTES: usize = 512 * 1024;

/// Read every `DBTOOL_CONN_<NAME>` environment entry inside explicit count
/// and byte limits. Errors describe only the violated boundary and never echo
/// an environment key or DSN value.
pub fn discover_env_connections_bounded() -> Result<HashMap<String, String>> {
    collect_env_connections(std::env::vars_os())
}

/// Compatibility surface used by older UI callers that cannot return a
/// configuration error. The collection itself is still bounded; invalid or
/// oversized catalogs fail closed as an empty map.
pub fn discover_env_connections() -> HashMap<String, String> {
    discover_env_connections_bounded().unwrap_or_default()
}

fn collect_env_connections<I>(entries: I) -> Result<HashMap<String, String>>
where
    I: IntoIterator<Item = (OsString, OsString)>,
{
    let mut connections = HashMap::new();
    let mut total_bytes = 0_usize;
    for (raw_key, raw_value) in entries {
        let key_lossy = raw_key.to_string_lossy();
        let Some(raw_name_lossy) = key_lossy.strip_prefix("DBTOOL_CONN_") else {
            continue;
        };
        if raw_key.to_str().is_none() {
            return Err(env_catalog_error("contains a non-UTF-8 connection key"));
        }
        if connections.len() == ENV_CONNECTION_MAX_ENTRIES {
            return Err(env_catalog_error("exceeds the environment entry limit"));
        }
        if raw_name_lossy.is_empty()
            || raw_name_lossy.len() > ENV_CONNECTION_MAX_NAME_BYTES
            || raw_name_lossy.chars().any(char::is_control)
        {
            return Err(env_catalog_error("contains a name outside the field limit"));
        }
        // Preserve the normalization semantics of the original environment
        // discovery API: Unicode names are lowercased, not only ASCII names.
        let name = raw_name_lossy.to_lowercase().replace('_', "-");
        if name.len() > ENV_CONNECTION_MAX_NAME_BYTES {
            return Err(env_catalog_error("contains a name outside the field limit"));
        }
        let dsn = raw_value
            .into_string()
            .map_err(|_| env_catalog_error("contains a non-UTF-8 DSN value"))?;
        if dsn.len() > ENV_CONNECTION_MAX_DSN_BYTES {
            return Err(env_catalog_error("contains a DSN outside the field limit"));
        }
        total_bytes = total_bytes
            .checked_add(name.len())
            .and_then(|total| total.checked_add(dsn.len()))
            .ok_or_else(|| env_catalog_error("size accounting overflowed"))?;
        if total_bytes > ENV_CONNECTION_MAX_TOTAL_BYTES {
            return Err(env_catalog_error("exceeds the cumulative byte limit"));
        }
        if connections.insert(name, dsn).is_some() {
            return Err(env_catalog_error(
                "contains entries with the same normalized name",
            ));
        }
    }
    Ok(connections)
}

fn env_catalog_error(reason: &str) -> Error {
    Error::Config(format!("environment connection catalog {reason}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_env_collection_normalizes_reasonable_entries() {
        let connections = collect_env_connections([
            (
                OsString::from("DBTOOL_CONN_LOCAL_DB"),
                OsString::from("postgres://user:secret@localhost/app"),
            ),
            (OsString::from("UNRELATED"), OsString::from("ignored")),
        ])
        .unwrap();
        assert_eq!(
            connections.get("local-db").map(String::as_str),
            Some("postgres://user:secret@localhost/app")
        );

        let unicode = collect_env_connections([(
            OsString::from("DBTOOL_CONN_ÄPFEL"),
            OsString::from("sqlite::memory:"),
        )])
        .unwrap();
        assert!(unicode.contains_key("äpfel"));
    }

    #[test]
    fn bounded_env_collection_rejects_count_size_and_normalized_collisions() {
        let exact_count = (0..ENV_CONNECTION_MAX_ENTRIES).map(|index| {
            (
                OsString::from(format!("DBTOOL_CONN_EXACT_{index}")),
                OsString::from("sqlite::memory:"),
            )
        });
        assert_eq!(
            collect_env_connections(exact_count).unwrap().len(),
            ENV_CONNECTION_MAX_ENTRIES
        );

        let too_many = (0..=ENV_CONNECTION_MAX_ENTRIES).map(|index| {
            (
                OsString::from(format!("DBTOOL_CONN_TEST_{index}")),
                OsString::from("sqlite::memory:"),
            )
        });
        assert!(matches!(
            collect_env_connections(too_many),
            Err(crate::Error::Config(message)) if message.contains("entry limit")
        ));

        let secret = "ENV_SECRET_MARKER".repeat(ENV_CONNECTION_MAX_DSN_BYTES + 1);
        let error = collect_env_connections([(
            OsString::from("DBTOOL_CONN_TOO_LARGE"),
            OsString::from(&secret),
        )])
        .unwrap_err();
        assert!(
            matches!(error, crate::Error::Config(ref message) if message.contains("field limit"))
        );
        assert!(!error.to_string().contains("ENV_SECRET_MARKER"));

        let collision = collect_env_connections([
            (
                OsString::from("DBTOOL_CONN_SAME_NAME"),
                OsString::from("sqlite::memory:"),
            ),
            (
                OsString::from("DBTOOL_CONN_SAME-NAME"),
                OsString::from("redis://localhost/0"),
            ),
        ])
        .unwrap_err();
        assert!(
            matches!(collision, crate::Error::Config(message) if message.contains("same normalized name"))
        );

        assert!(matches!(
            collect_env_connections([(
                OsString::from("DBTOOL_CONN_BAD\nNAME"),
                OsString::from("sqlite::memory:"),
            )]),
            Err(crate::Error::Config(message)) if message.contains("field limit")
        ));
    }

    #[test]
    fn bounded_env_collection_distinguishes_exact_cumulative_bytes_from_n_plus_one() {
        let names = (0..32).map(|index| format!("t{index}")).collect::<Vec<_>>();
        let name_bytes = names.iter().map(String::len).sum::<usize>();
        let mut remaining_dsn_bytes = ENV_CONNECTION_MAX_TOTAL_BYTES - name_bytes;
        let mut exact = Vec::with_capacity(names.len());
        for name in names {
            let dsn_bytes = remaining_dsn_bytes.min(ENV_CONNECTION_MAX_DSN_BYTES);
            remaining_dsn_bytes -= dsn_bytes;
            exact.push((
                OsString::from(format!("DBTOOL_CONN_{}", name.to_uppercase())),
                OsString::from("x".repeat(dsn_bytes)),
            ));
        }
        assert_eq!(remaining_dsn_bytes, 0);
        assert_eq!(collect_env_connections(exact.clone()).unwrap().len(), 32);

        let mut plus_one = exact;
        plus_one.push((
            OsString::from("DBTOOL_CONN_OVER_BUDGET"),
            OsString::from("x"),
        ));
        assert!(matches!(
            collect_env_connections(plus_one),
            Err(crate::Error::Config(message)) if message.contains("cumulative byte limit")
        ));
    }
}
