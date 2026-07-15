use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// The lifetime attached to one key/value snapshot.
///
/// Expiring keys use an absolute Unix deadline so copying a snapshot never
/// restarts its source TTL. Adapters must derive and apply this value against
/// the database server's clock, not the caller's local clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "unix_ms", rename_all = "kebab-case")]
pub enum KeyExpiry {
    Persistent,
    ExpiresAtUnixMs(i64),
}

/// An exact key value paired with the lifetime observed in the same backend
/// operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyValueSnapshot {
    pub value: Bytes,
    pub expiry: KeyExpiry,
}

/// Result of atomically restoring one key/value snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum KeyValueRestoreOutcome {
    Stored,
    ConditionNotMet,
    Expired,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expiry_and_restore_outcomes_have_stable_wire_names() {
        assert_eq!(
            serde_json::to_value(KeyExpiry::Persistent).unwrap(),
            serde_json::json!({ "kind": "persistent" })
        );
        assert_eq!(
            serde_json::to_value(KeyExpiry::ExpiresAtUnixMs(1_710_000_000_123)).unwrap(),
            serde_json::json!({
                "kind": "expires-at-unix-ms",
                "unix_ms": 1_710_000_000_123_i64
            })
        );
        assert_eq!(
            serde_json::to_value(KeyValueRestoreOutcome::ConditionNotMet).unwrap(),
            serde_json::json!("condition-not-met")
        );
    }

    #[test]
    fn snapshots_preserve_empty_and_binary_values() {
        for value in [Bytes::new(), Bytes::from_static(&[0, 0xff, b'Z'])] {
            let snapshot = KeyValueSnapshot {
                value,
                expiry: KeyExpiry::Persistent,
            };
            let encoded = serde_json::to_vec(&snapshot).unwrap();
            let decoded: KeyValueSnapshot = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(decoded, snapshot);
        }
    }
}
