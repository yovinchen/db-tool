use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{
        BoundedList, ConsumeCursor, ConsumeOptions, DeleteResourceOptions, Message,
        MessageResource, MessageResourceKind, PartitionWatermark, ReadBudget, TopicInfo,
    },
    port::connector::Connector,
    service::limiter::ReadLimiter,
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

fn budgeted_topic_catalog_plan(budget: ReadBudget) -> Result<(ReadLimiter, usize)> {
    let limiter = ReadLimiter::new(budget, "Kafka topic catalog response")?;
    let probe_items = limiter.probe_items()?;
    Ok((limiter, probe_items))
}

fn finish_budgeted_topic_catalog<I>(
    mut limiter: ReadLimiter,
    probe_items: usize,
    topics: I,
) -> Result<BoundedList<TopicInfo>>
where
    I: IntoIterator<Item = TopicInfo>,
{
    let mut retained = Vec::with_capacity(probe_items.saturating_sub(1).min(256));
    for topic in topics.into_iter().take(probe_items) {
        limiter.retain_item(topic, &mut retained)?;
    }
    retained.sort_by(|left, right| left.name.cmp(&right.name));
    limiter.finish(retained)
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

    if message.cursor.is_some() || message.metadata.is_some() {
        return Err(Error::Config(
            "Kafka producer messages cannot set consumer cursor or delivery metadata".to_owned(),
        ));
    }

    Ok(())
}

fn resolve_consume_position(options: &ConsumeOptions) -> Result<(Option<i32>, Option<i64>)> {
    let (partition, offset) = match &options.cursor {
        None => (options.partition, options.offset),
        Some(ConsumeCursor::Kafka { partition, offset }) => {
            if options.partition.is_some() || options.offset.is_some() {
                return Err(Error::Config(
                    "Kafka exact cursor cannot be combined with legacy partition or offset fields"
                        .to_owned(),
                ));
            }
            (Some(*partition), Some(*offset))
        }
        Some(cursor) => {
            return Err(Error::Config(format!(
                "Kafka consumer cannot use {cursor:?} cursor"
            )))
        }
    };

    if offset.is_some() && partition.is_none() {
        return Err(Error::Config(
            "Kafka consume offset requires an explicit partition".to_owned(),
        ));
    }
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

    Ok((partition, offset))
}

fn validate_kafka_consume_options(options: &ConsumeOptions) -> Result<()> {
    options
        .validate()
        .map_err(|message| Error::Config(format!("Kafka consume: {message}")))
}

#[cfg(test)]
fn validate_consume_position(partition: Option<i32>, offset: Option<i64>) -> Result<()> {
    resolve_consume_position(&ConsumeOptions {
        partition,
        offset,
        ..Default::default()
    })
    .map(|_| ())
}

fn validate_kafka_delete_request(
    resource: &MessageResource,
    options: DeleteResourceOptions,
) -> Result<()> {
    if resource.kind != MessageResourceKind::KafkaTopic {
        return Err(Error::Config(format!(
            "Kafka adapters can only delete kafka-topic resources, not {}",
            resource.kind.as_str()
        )));
    }
    if options.if_empty || options.if_unused {
        return Err(Error::Config(
            "Kafka topic deletion does not support the AMQP-only if_empty or if_unused options"
                .to_owned(),
        ));
    }
    Ok(())
}

fn kafka_messages_before(watermarks: &[PartitionWatermark]) -> Option<u64> {
    watermarks.iter().try_fold(0u64, |total, watermark| {
        let partition_messages = watermark.high.checked_sub(watermark.low)?;
        let partition_messages = u64::try_from(partition_messages).ok()?;
        total.checked_add(partition_messages)
    })
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
            cursor: None,
            metadata: None,
        }
    }

    fn topic(name: &str) -> TopicInfo {
        TopicInfo {
            name: name.to_owned(),
            partitions: 1,
            replicas: 1,
        }
    }

    fn finish_topic_fixture(
        names: &[&str],
        max_items: usize,
        max_bytes: usize,
    ) -> Result<BoundedList<TopicInfo>> {
        let (limiter, probe_items) =
            budgeted_topic_catalog_plan(ReadBudget::new(max_items, max_bytes)?)?;
        finish_budgeted_topic_catalog(limiter, probe_items, names.iter().map(|name| topic(name)))
    }

    #[test]
    fn kafka_budgeted_topic_plan_rejects_zero_and_overflow() {
        for budget in [
            ReadBudget {
                max_items: 0,
                max_bytes: 1,
            },
            ReadBudget {
                max_items: usize::MAX,
                max_bytes: 1,
            },
        ] {
            assert!(matches!(
                budgeted_topic_catalog_plan(budget),
                Err(Error::Config(_))
            ));
        }
        let (_, probe_items) =
            budgeted_topic_catalog_plan(ReadBudget::new(2, 1024).unwrap()).unwrap();
        assert_eq!(probe_items, 3);
    }

    #[test]
    fn kafka_budgeted_topic_catalog_distinguishes_n_and_n_plus_one() {
        let exact = finish_topic_fixture(&["beta", "alpha"], 2, 4096).unwrap();
        assert!(!exact.truncated);
        assert_eq!(
            exact
                .items
                .iter()
                .map(|topic| topic.name.as_str())
                .collect::<Vec<_>>(),
            ["alpha", "beta"]
        );

        let probed = finish_topic_fixture(&["beta", "alpha", "gamma", "ignored"], 2, 4096).unwrap();
        assert!(probed.truncated);
        assert_eq!(
            probed
                .items
                .iter()
                .map(|topic| topic.name.as_str())
                .collect::<Vec<_>>(),
            ["alpha", "beta"]
        );
    }

    #[test]
    fn kafka_budgeted_topic_catalog_enforces_exact_complete_envelope_bytes() {
        let expected = BoundedList::complete(vec![topic("alpha"), topic("beta")]);
        let exact_bytes = serde_json::to_vec(&expected).unwrap().len();
        let exact = finish_topic_fixture(&["alpha", "beta"], 2, exact_bytes).unwrap();
        assert!(!exact.truncated);
        assert_eq!(serde_json::to_vec(&exact).unwrap().len(), exact_bytes);
        assert!(matches!(
            finish_topic_fixture(&["alpha", "beta"], 2, exact_bytes - 1),
            Err(Error::ReadBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            }) if limit == exact_bytes - 1
        ));
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

    #[test]
    fn kafka_consume_rejects_invalid_byte_envelopes_before_client_access() {
        for options in [
            ConsumeOptions {
                max_message_bytes: 0,
                ..Default::default()
            },
            ConsumeOptions {
                max_batch_bytes: dbtool_core::model::MAX_READ_BYTES + 1,
                ..Default::default()
            },
        ] {
            let error = validate_kafka_consume_options(&options).unwrap_err();
            assert!(matches!(error, Error::Config(message) if message.contains("Kafka consume")));
        }
    }

    #[test]
    fn consumer_offset_requires_explicit_partition() {
        let error = validate_consume_position(None, Some(42)).unwrap_err();

        assert!(matches!(error, Error::Config(_)));
        assert!(error.to_string().contains("explicit partition"));
    }

    #[test]
    fn exact_kafka_cursor_resolves_without_legacy_position_loss() {
        assert_eq!(
            resolve_consume_position(&ConsumeOptions {
                cursor: Some(ConsumeCursor::Kafka {
                    partition: 7,
                    offset: 99,
                }),
                ..Default::default()
            })
            .unwrap(),
            (Some(7), Some(99))
        );

        let conflict = resolve_consume_position(&ConsumeOptions {
            partition: Some(7),
            cursor: Some(ConsumeCursor::Kafka {
                partition: 7,
                offset: 99,
            }),
            ..Default::default()
        })
        .unwrap_err();
        assert!(conflict.to_string().contains("cannot be combined"));

        let wrong_protocol = resolve_consume_position(&ConsumeOptions {
            cursor: Some(ConsumeCursor::RedisStream {
                id: "1-0".to_owned(),
            }),
            ..Default::default()
        })
        .unwrap_err();
        assert!(wrong_protocol.to_string().contains("cannot use"));
    }

    #[test]
    fn kafka_delete_rejects_other_resource_kinds_and_amqp_options() {
        let wrong_kind = MessageResource {
            kind: MessageResourceKind::RedisStream,
            name: "events".to_owned(),
        };
        let error = validate_kafka_delete_request(&wrong_kind, DeleteResourceOptions::default())
            .unwrap_err();
        assert!(matches!(error, Error::Config(_)));
        assert!(error.to_string().contains("redis-stream"));

        let topic = MessageResource {
            kind: MessageResourceKind::KafkaTopic,
            name: "events".to_owned(),
        };
        let error = validate_kafka_delete_request(
            &topic,
            DeleteResourceOptions {
                if_empty: true,
                if_unused: false,
            },
        )
        .unwrap_err();
        assert!(matches!(error, Error::Config(_)));
        assert!(error.to_string().contains("AMQP-only"));
    }

    #[test]
    fn kafka_message_count_sums_exact_partition_watermarks() {
        let watermarks = [
            PartitionWatermark {
                partition: 0,
                low: 2,
                high: 7,
            },
            PartitionWatermark {
                partition: 1,
                low: 10,
                high: 13,
            },
        ];

        assert_eq!(kafka_messages_before(&watermarks), Some(8));
        assert_eq!(
            kafka_messages_before(&[PartitionWatermark {
                partition: 0,
                low: 5,
                high: 4,
            }]),
            None
        );
    }
}
