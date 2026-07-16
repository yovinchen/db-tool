#![cfg(not(feature = "backend-native"))]

use adapter_kafka::factory;
use dbtool_core::{
    dsn::Dsn,
    error::Error,
    model::{DeleteResourceOptions, MessageResource, MessageResourceKind, MetadataBudget},
    port::CapabilityOperation,
};
use rskafka::client::ClientBuilder;
use std::time::{SystemTime, UNIX_EPOCH};

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
    assert_eq!(deleted.messages_before, Some(0));
}
