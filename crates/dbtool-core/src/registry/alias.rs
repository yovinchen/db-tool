/// Protocol family alias table — single source of truth (see §3.8 / §6.3).
/// (canonical_scheme, &[aliases])
pub const PROTOCOL_ALIASES: &[(&str, &[&str])] = &[
    ("mysql", &["mariadb", "tidb"]),
    (
        "postgres",
        &["postgresql", "cockroach", "timescale", "redshift"],
    ),
    ("redis", &["valkey", "keydb", "dragonfly"]),
    ("kafka", &["automq", "redpanda", "warpstream", "confluent"]),
    ("mongodb", &[]),
    ("opensearch", &["elasticsearch"]),
    ("amqp", &["amqps"]),
    ("nats", &["nats+tls"]),
];

pub fn canonical_scheme(scheme: &str) -> &str {
    for (canonical, aliases) in PROTOCOL_ALIASES {
        if *canonical == scheme || aliases.contains(&scheme) {
            return canonical;
        }
    }
    scheme
}

pub fn protocol_family(canonical: &str) -> Option<(&'static str, &'static [&'static str])> {
    PROTOCOL_ALIASES
        .iter()
        .copied()
        .find(|(scheme, _)| *scheme == canonical)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonicalizes_known_aliases() {
        assert_eq!(canonical_scheme("postgresql"), "postgres");
        assert_eq!(canonical_scheme("cockroach"), "postgres");
        assert_eq!(canonical_scheme("valkey"), "redis");
        assert_eq!(canonical_scheme("redpanda"), "kafka");
        assert_eq!(canonical_scheme("elasticsearch"), "opensearch");
    }

    #[test]
    fn leaves_unknown_scheme_unchanged() {
        assert_eq!(canonical_scheme("custom"), "custom");
    }
}
