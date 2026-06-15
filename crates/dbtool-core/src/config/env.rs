/// Reads `DBTOOL_CONN_<NAME>` environment variables and returns them as
/// a map of lowercase name → DSN string.
pub fn discover_env_connections() -> std::collections::HashMap<String, String> {
    std::env::vars()
        .filter_map(|(k, v)| {
            k.strip_prefix("DBTOOL_CONN_")
                .map(|name| (name.to_lowercase().replace('_', "-"), v))
        })
        .collect()
}
