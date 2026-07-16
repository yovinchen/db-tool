use adapter_amqp::{factory, management_factory};
use dbtool_core::{
    dsn::Dsn,
    error::Error,
    model::{AckMode, ConsumeOptions, Message, MetadataBudget, ProduceBudget, ReadBudget},
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

fn produce_message(payload: &'static [u8]) -> Message {
    Message {
        key: None,
        payload: payload.to_vec().into(),
        headers: HashMap::from([("trace".to_owned(), "budgeted".to_owned())]),
        partition: None,
        offset: None,
        timestamp: None,
        cursor: None,
        metadata: None,
    }
}

#[tokio::test]
async fn budgeted_amqp_produce_rejects_before_queue_creation_then_round_trips() {
    let Some(dsn) = integration_dsn() else {
        return;
    };
    let queue = unique_queue();
    let connector = factory(Dsn::parse(&dsn).expect("integration DSN should parse"))
        .await
        .expect("AMQP adapter should connect");
    assert!(connector
        .operations()
        .contains(&CapabilityOperation::MessageProduceBudgeted));
    let producer = connector
        .as_producer()
        .expect("AMQP adapter should expose a producer");
    let candidate = produce_message(b"budgeted-amqp-round-trip");

    assert!(matches!(
        producer
            .produce_budgeted(
                &queue,
                vec![candidate.clone()],
                ProduceBudget::new(1, 1, 4096).unwrap(),
            )
            .await,
        Err(Error::InputBudgetExceeded {
            unit: "bytes",
            limit: 1,
            ..
        })
    ));

    let fixture = Connection::connect(&dsn, ConnectionProperties::default())
        .await
        .expect("fixture connection should open");
    let passive_channel = fixture
        .create_channel()
        .await
        .expect("passive fixture channel should open");
    assert!(passive_channel
        .queue_declare(
            &queue,
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .is_err());

    let message_bytes = serde_json::to_vec(&candidate).unwrap().len();
    let batch_bytes = serde_json::to_vec(&vec![candidate.clone()]).unwrap().len();
    let outcome = producer
        .produce_budgeted(
            &queue,
            vec![candidate.clone()],
            ProduceBudget::new(1, message_bytes, batch_bytes).unwrap(),
        )
        .await
        .expect("exact budgeted publish should succeed");
    assert_eq!(outcome.produced, 1);

    let channel = fixture
        .create_channel()
        .await
        .expect("readback fixture channel should open");
    let delivery = channel
        .basic_get(&queue, BasicGetOptions::default())
        .await
        .expect("budgeted publish should be readable")
        .expect("one budgeted delivery should exist");
    assert_eq!(delivery.delivery.data, candidate.payload);
    channel
        .basic_ack(delivery.delivery.delivery_tag, BasicAckOptions::default())
        .await
        .expect("fixture delivery should acknowledge");
    channel
        .queue_delete(&queue, QueueDeleteOptions::default())
        .await
        .expect("budgeted queue should delete cleanly");
    fixture
        .close(200, "fixture complete")
        .await
        .expect("fixture connection should close");
    connector
        .close()
        .await
        .expect("adapter connection should close");
}

#[tokio::test]
async fn rabbit_management_detail_has_transport_and_complete_object_bounds() {
    let Some(amqp_dsn) = integration_dsn() else {
        return;
    };
    let management_dsn = std::env::var("DBTOOL_IT_RABBITMQ_MANAGEMENT_DSN")
        .expect("RabbitMQ management DSN should be configured");
    let queue = unique_queue();
    let auxiliary_queue = format!("{queue}_catalog_probe");
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
    channel
        .queue_declare(
            &auxiliary_queue,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("catalog probe queue should be declared");

    let connector = management_factory(
        Dsn::parse(&management_dsn).expect("RabbitMQ management DSN should parse"),
    )
    .await
    .expect("RabbitMQ management adapter should connect");
    assert!(connector
        .operations()
        .contains(&CapabilityOperation::MessageAdminTopicDetailBounded));
    assert!(connector
        .operations()
        .contains(&CapabilityOperation::MessageAdminListTopicsBudgeted));
    let admin = connector
        .as_admin()
        .expect("management adapter should expose admin inspection");
    let truncated = admin
        .list_topics_budgeted(ReadBudget::with_default_bytes(1).unwrap())
        .await
        .expect("two fixture queues should provide an N+1 probe");
    assert!(truncated.truncated);
    assert_eq!(truncated.items.len(), 1);
    assert!(matches!(
        admin
            .list_topics_budgeted(ReadBudget::new(1, 1).unwrap())
            .await,
        Err(Error::ReadBudgetExceeded {
            unit: "bytes",
            limit: 1,
            ..
        })
    ));
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
        .queue_delete(&auxiliary_queue, QueueDeleteOptions::default())
        .await
        .expect("catalog probe queue should delete cleanly");
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

#[tokio::test]
async fn consume_byte_budget_failure_requeues_before_batch_ack() {
    let Some(dsn) = integration_dsn() else {
        return;
    };
    let queue = unique_queue();
    let connector = factory(Dsn::parse(&dsn).expect("integration DSN should parse"))
        .await
        .expect("AMQP adapter should connect");
    connector
        .as_producer()
        .expect("AMQP adapter should expose a producer")
        .produce(
            &queue,
            vec![Message {
                key: None,
                payload: b"budget-must-fail-before-ack".to_vec().into(),
                headers: HashMap::new(),
                partition: None,
                offset: None,
                timestamp: None,
                cursor: None,
                metadata: None,
            }],
        )
        .await
        .expect("budget fixture should publish");

    let error = connector
        .as_consumer()
        .expect("AMQP adapter should expose a consumer")
        .consume(
            &queue,
            ConsumeOptions {
                max: 1,
                timeout: Duration::from_secs(5),
                max_batch_bytes: 1,
                ack: AckMode::OnSuccess,
                ..Default::default()
            },
        )
        .await
        .expect_err("complete batch must exceed the one-byte budget");
    assert!(matches!(
        error,
        Error::ReadBudgetExceeded {
            unit: "bytes",
            limit: 1,
            ..
        }
    ));

    let fixture = Connection::connect(&dsn, ConnectionProperties::default())
        .await
        .expect("fixture connection should open");
    let channel = fixture
        .create_channel()
        .await
        .expect("fixture channel should open");
    let mut ready = 0;
    for _ in 0..50 {
        ready = channel
            .queue_declare(
                &queue,
                QueueDeclareOptions {
                    passive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .expect("budget fixture queue should remain available")
            .message_count();
        if ready == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(ready, 1, "budget failure must requeue before basic.ack");
    let delivery = channel
        .basic_get(&queue, BasicGetOptions::default())
        .await
        .expect("requeued budget fixture should be readable")
        .expect("requeued budget fixture should exist");
    assert!(delivery.delivery.redelivered);
    channel
        .basic_ack(delivery.delivery.delivery_tag, BasicAckOptions::default())
        .await
        .expect("fixture cleanup should acknowledge the delivery");
    channel
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
