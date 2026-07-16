use adapter_amqp::{factory, management_factory};
use dbtool_core::{
    dsn::Dsn,
    error::Error,
    model::{AckMode, ConsumeOptions, Message, MetadataBudget},
    port::CapabilityOperation,
};
use lapin::{
    options::{
        BasicAckOptions, BasicGetOptions, BasicPublishOptions, ConfirmSelectOptions,
        QueueDeclareOptions, QueueDeleteOptions,
    },
    publisher_confirm::Confirmation,
    types::{AMQPValue, FieldTable},
    BasicProperties, Connection, ConnectionProperties,
};
use std::{collections::HashMap, time::Duration};

fn integration_dsn() -> Option<String> {
    if std::env::var("DBTOOL_RUN_MQ_INTEGRATION").ok().as_deref() != Some("1") {
        return None;
    }
    std::env::var("DBTOOL_IT_AMQP_DSN").ok()
}

fn unique_queue() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after Unix epoch")
        .as_nanos();
    format!("dbtool_it_amqp_atomic_ack_{}_{}", std::process::id(), nanos)
}

#[tokio::test]
async fn rabbit_management_detail_has_transport_and_complete_object_bounds() {
    let Some(amqp_dsn) = integration_dsn() else {
        return;
    };
    let management_dsn = std::env::var("DBTOOL_IT_RABBITMQ_MANAGEMENT_DSN")
        .expect("RabbitMQ management DSN should be configured");
    let queue = unique_queue();
    let fixture = Connection::connect(&amqp_dsn, ConnectionProperties::default())
        .await
        .expect("fixture connection should open");
    let channel = fixture
        .create_channel()
        .await
        .expect("fixture channel should open");
    channel
        .queue_declare(
            &queue,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("fixture queue should be declared");

    let connector = management_factory(
        Dsn::parse(&management_dsn).expect("RabbitMQ management DSN should parse"),
    )
    .await
    .expect("RabbitMQ management adapter should connect");
    assert!(connector
        .operations()
        .contains(&CapabilityOperation::MessageAdminTopicDetailBounded));
    let admin = connector
        .as_admin()
        .expect("management adapter should expose admin inspection");
    // RabbitMQ's management statistics are populated asynchronously after an
    // AMQP declaration. Retry only that documented transient shape; every
    // other adapter error remains an immediate failure.
    let mut detail = None;
    for _ in 0..50 {
        match admin
            .topic_detail_bounded(
                &queue,
                MetadataBudget::with_default_bytes(100).expect("budget should be valid"),
            )
            .await
        {
            Ok(value) => {
                detail = Some(value);
                break;
            }
            Err(Error::Serialization(message)) if message.contains("messages_ready") => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => panic!("management queue detail failed: {error}"),
        }
    }
    let detail = detail.expect("RabbitMQ management statistics should become available");
    assert_eq!(detail.info.name, queue);
    assert_eq!(detail.watermarks.len(), 1);
    assert!(matches!(
        admin
            .topic_detail_bounded(
                &queue,
                MetadataBudget::with_default_bytes(1).expect("budget should be valid"),
            )
            .await,
        Err(Error::MetadataBudgetExceeded {
            unit: "items",
            limit: 1,
            ..
        })
    ));

    channel
        .queue_delete(&queue, QueueDeleteOptions::default())
        .await
        .expect("fixture queue should delete cleanly");
    fixture
        .close(200, "fixture complete")
        .await
        .expect("fixture connection should close");
}

#[tokio::test]
async fn failed_batch_conversion_requeues_every_delivery_before_ack() {
    let Some(dsn) = integration_dsn() else {
        return;
    };
    let queue = unique_queue();
    let connector = factory(Dsn::parse(&dsn).expect("integration DSN should parse"))
        .await
        .expect("AMQP adapter should connect");
    let producer = connector
        .as_producer()
        .expect("AMQP adapter should expose a producer");
    producer
        .produce(
            &queue,
            vec![Message {
                key: None,
                payload: b"valid-before-malformed".to_vec().into(),
                headers: HashMap::from([("trace".to_owned(), "valid".to_owned())]),
                partition: None,
                offset: None,
                timestamp: None,
                cursor: None,
                metadata: None,
            }],
        )
        .await
        .expect("valid fixture message should publish");

    let admin = connector
        .as_admin()
        .expect("AMQP adapter should expose queue detail");
    let detail = admin
        .topic_detail_bounded(
            &queue,
            MetadataBudget::with_default_bytes(2).expect("exact queue budget should be valid"),
        )
        .await
        .expect("fixed-shape passive queue detail should fit two config entries");
    assert_eq!(detail.info.name, queue);
    assert!(matches!(
        admin
            .topic_detail_bounded(&queue, MetadataBudget::with_default_bytes(1).unwrap(),)
            .await,
        Err(Error::MetadataBudgetExceeded {
            unit: "items",
            limit: 1,
            ..
        })
    ));

    let fixture = Connection::connect(&dsn, ConnectionProperties::default())
        .await
        .expect("fixture connection should open");
    let fixture_channel = fixture
        .create_channel()
        .await
        .expect("fixture channel should open");
    fixture_channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .expect("fixture publisher confirms should enable");
    let mut invalid_headers = FieldTable::default();
    invalid_headers.insert("attempt".into(), AMQPValue::LongInt(424_242));
    let confirmation = fixture_channel
        .basic_publish(
            "",
            &queue,
            BasicPublishOptions::default(),
            b"malformed-header-message",
            BasicProperties::default().with_headers(invalid_headers),
        )
        .await
        .expect("malformed fixture publish should be accepted")
        .await
        .expect("malformed fixture confirmation should resolve");
    assert!(matches!(confirmation, Confirmation::Ack(_)));

    let error = connector
        .as_consumer()
        .expect("AMQP adapter should expose a consumer")
        .consume(
            &queue,
            ConsumeOptions {
                max: 2,
                timeout: Duration::from_secs(5),
                ack: AckMode::OnSuccess,
                ..Default::default()
            },
        )
        .await
        .expect_err("the non-string header must fail portable conversion");
    let rendered_error = error.to_string();
    assert!(rendered_error.contains("unsupported non-string type"));
    assert!(!rendered_error.contains("424242"));
    assert!(!rendered_error.contains("valid-before-malformed"));

    let mut ready = 0;
    for _ in 0..50 {
        ready = fixture_channel
            .queue_declare(
                &queue,
                QueueDeclareOptions {
                    passive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .expect("fixture queue should remain available")
            .message_count();
        if ready == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        ready, 2,
        "conversion failure must requeue the complete batch"
    );

    let first = fixture_channel
        .basic_get(&queue, BasicGetOptions::default())
        .await
        .expect("first requeued delivery should be readable")
        .expect("first requeued delivery should exist");
    let second = fixture_channel
        .basic_get(&queue, BasicGetOptions::default())
        .await
        .expect("second requeued delivery should be readable")
        .expect("second requeued delivery should exist");
    assert!(first.delivery.redelivered);
    assert!(second.delivery.redelivered);
    let mut payloads = vec![first.delivery.data.clone(), second.delivery.data.clone()];
    payloads.sort();
    assert_eq!(
        payloads,
        vec![
            b"malformed-header-message".to_vec(),
            b"valid-before-malformed".to_vec(),
        ]
    );
    fixture_channel
        .basic_ack(
            second.delivery.delivery_tag,
            BasicAckOptions { multiple: true },
        )
        .await
        .expect("fixture cleanup should acknowledge both deliveries");
    fixture_channel
        .queue_delete(&queue, QueueDeleteOptions::default())
        .await
        .expect("fixture queue should delete cleanly");
    fixture
        .close(200, "fixture complete")
        .await
        .expect("fixture connection should close");
    connector
        .close()
        .await
        .expect("adapter connection should close");
}
