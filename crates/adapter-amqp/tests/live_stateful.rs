use adapter_amqp::factory;
use dbtool_core::{
    dsn::Dsn,
    model::{AckMode, ConsumeOptions, Message},
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
