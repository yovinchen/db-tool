#![cfg(not(feature = "backend-native"))]

use adapter_kafka::factory;
use dbtool_core::{
    dsn::Dsn,
    error::Error,
    model::{
        ConsumeOptions, DeleteResourceOptions, Message, MessageResource, MessageResourceKind,
        MetadataBudget,
    },
    port::CapabilityOperation,
};
use rskafka::client::ClientBuilder;
use std::{
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[tokio::test]
async fn pure_kafka_detail_and_delete_stay_on_capped_admin_clients() {
    if std::env::var("DBTOOL_RUN_MQ_INTEGRATION").as_deref() != Ok("1") {
        return;
    }
    let raw_dsn = std::env::var("DBTOOL_IT_KAFKA_DSN")
        .expect("DBTOOL_IT_KAFKA_DSN is required for the live test");
    let dsn = Dsn::parse(&raw_dsn).expect("Kafka DSN should parse");
    let broker = format!(
        "{}:{}",
        dsn.host.as_deref().unwrap_or("127.0.0.1"),
        dsn.port.unwrap_or(9092)
    );
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after the Unix epoch")
        .as_nanos();
    let topic = format!("dbtool_it_pure_bounded_{}_{}", std::process::id(), unique);

    let fixture = ClientBuilder::new(vec![broker])
        .client_id("dbtool-bounded-live-fixture")
        .build()
        .await
        .expect("fixture Kafka client should connect");
    fixture
        .controller_client()
        .expect("fixture controller should resolve")
        .create_topic(&topic, 2, 1, 5_000)
        .await
        .expect("two-partition fixture topic should be created");

    let connector = factory(dsn).await.expect("Kafka adapter should connect");
    assert!(connector
        .operations()
        .contains(&CapabilityOperation::MessageAdminTopicDetailBounded));
    assert!(!connector
        .operations()
        .contains(&CapabilityOperation::MessageAdminConsumerLagBounded));
    let admin = connector
        .as_admin()
        .expect("Kafka adapter should expose admin inspection");
    assert!(matches!(
        admin
            .topic_detail_bounded(
                &topic,
                MetadataBudget::with_default_bytes(1).expect("budget should be valid"),
            )
            .await,
        Err(Error::MetadataBudgetExceeded {
            unit: "items",
            limit: 1,
            ..
        })
    ));
    let detail = admin
        .topic_detail_bounded(
            &topic,
            MetadataBudget::with_default_bytes(2).expect("budget should be valid"),
        )
        .await
        .expect("two-partition detail should fit its exact item budget");
    assert_eq!(detail.info.partitions, 2);
    assert_eq!(detail.watermarks.len(), 2);

    connector
        .as_producer()
        .expect("Kafka adapter should expose production")
        .produce(
            &topic,
            vec![Message {
                key: Some(b"key".to_vec().into()),
                payload: b"budgeted-pure-kafka-message".to_vec().into(),
                headers: HashMap::from([("trace".to_owned(), "pure".to_owned())]),
                partition: Some(0),
                offset: None,
                timestamp: None,
                cursor: None,
                metadata: None,
            }],
        )
        .await
        .expect("pure Kafka budget fixture should publish");
    let consumer = connector
        .as_consumer()
        .expect("Kafka adapter should expose consumption");
    assert!(matches!(
        consumer
            .consume(
                &topic,
                ConsumeOptions {
                    max: 1,
                    timeout: Duration::from_secs(5),
                    partition: Some(0),
                    max_message_bytes: 1,
                    ..Default::default()
                },
            )
            .await,
        Err(Error::ReadBudgetExceeded {
            unit: "bytes",
            limit: 1,
            ..
        })
    ));
    let consumed = consumer
        .consume(
            &topic,
            ConsumeOptions {
                max: 1,
                timeout: Duration::from_secs(5),
                partition: Some(0),
                ..Default::default()
            },
        )
        .await
        .expect("pure Kafka message should remain readable with a sufficient budget");
    assert_eq!(consumed.len(), 1);
    assert_eq!(consumed[0].payload.as_ref(), b"budgeted-pure-kafka-message");

    let deleted = connector
        .as_admin_mutate()
        .expect("Kafka adapter should expose topic deletion")
        .delete_resource(
            MessageResource {
                kind: MessageResourceKind::KafkaTopic,
                name: topic,
            },
            DeleteResourceOptions::default(),
        )
        .await
        .expect("delete should use the hard-bounded private detail and absence path");
    assert!(deleted.acknowledged);
    assert!(deleted.verified_absent);
    assert_eq!(deleted.messages_before, Some(1));
}
