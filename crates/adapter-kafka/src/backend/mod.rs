use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::Message,
    port::connector::Connector,
};
use futures::future::BoxFuture;

// Mutually exclusive backend selection (§12.2).
#[cfg(feature = "backend-native")]
mod rdkafka_backend;
#[cfg(not(feature = "backend-native"))]
mod rskafka_backend;

pub fn connect(dsn: Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>> {
    #[cfg(feature = "backend-native")]
    {
        rdkafka_backend::connect(dsn)
    }
    #[cfg(not(feature = "backend-native"))]
    {
        rskafka_backend::connect(dsn)
    }
}

/// Validate producer-only fields before the adapter creates a topic or sends
/// any records. Kafka assigns offsets after a successful append, so accepting
/// a caller-provided offset would silently misrepresent what was persisted.
fn validate_produce_message(message: &Message) -> Result<()> {
    if message.offset.is_some() {
        return Err(Error::Config(
            "Kafka producer messages cannot set offset; the broker assigns it".to_owned(),
        ));
    }

    if message.partition.is_some_and(|partition| partition < 0) {
        return Err(Error::Config(
            "Kafka partition must be greater than or equal to zero".to_owned(),
        ));
    }

    Ok(())
}

fn validate_consume_position(partition: Option<i32>, offset: Option<i64>) -> Result<()> {
    if partition.is_some_and(|partition| partition < 0) {
        return Err(Error::Config(
            "Kafka partition must be greater than or equal to zero".to_owned(),
        ));
    }
    if offset.is_some_and(|offset| offset < 0) {
        return Err(Error::Config(
            "Kafka offset must be greater than or equal to zero".to_owned(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;

    fn message(partition: Option<i32>, offset: Option<i64>) -> Message {
        Message {
            key: None,
            payload: Bytes::from_static(b"payload"),
            headers: HashMap::new(),
            partition,
            offset,
            timestamp: None,
        }
    }

    #[test]
    fn producer_rejects_broker_assigned_offset() {
        let error = validate_produce_message(&message(Some(0), Some(42))).unwrap_err();

        assert!(matches!(error, Error::Config(_)));
        assert!(error.to_string().contains("broker assigns"));
    }

    #[test]
    fn producer_and_consumer_reject_negative_partitions() {
        let produce_error = validate_produce_message(&message(Some(-1), None)).unwrap_err();
        let consume_error = validate_consume_position(Some(-1), None).unwrap_err();

        assert!(matches!(produce_error, Error::Config(_)));
        assert!(matches!(consume_error, Error::Config(_)));

        let offset_error = validate_consume_position(Some(0), Some(-1)).unwrap_err();
        assert!(matches!(offset_error, Error::Config(_)));
        assert!(offset_error.to_string().contains("offset"));
    }

    #[test]
    fn absent_or_non_negative_partitions_are_valid() {
        validate_produce_message(&message(None, None)).unwrap();
        validate_produce_message(&message(Some(0), None)).unwrap();
        validate_consume_position(None, None).unwrap();
        validate_consume_position(Some(3), Some(42)).unwrap();
    }
}
