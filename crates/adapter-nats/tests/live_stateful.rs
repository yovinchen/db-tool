use adapter_nats::factory;
use bytes::Bytes;
use dbtool_core::{
    dsn::Dsn,
    error::Error,
    model::{
        AckMode, ConsumeOptions, ConsumerIdentity, DeleteResourceOptions, MessageCursor,
        MessageResource, MessageResourceKind, MetadataBudget,
    },
};
use futures::TryStreamExt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn live_dsn() -> Option<String> {
    std::env::var("DBTOOL_IT_NATS_DSN").ok()
}

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after the Unix epoch")
        .as_nanos();
    format!("{}_{}", std::process::id(), nanos)
}

fn durable_options(name: &str, max: usize, ack: AckMode) -> ConsumeOptions {
    ConsumeOptions {
        max,
        timeout: Duration::from_secs(3),
        identity: ConsumerIdentity::Durable {
            name: name.to_owned(),
        },
        ack,
        ..Default::default()
    }
}

async fn create_stream_and_consumer(
    jetstream: &async_nats::jetstream::Context,
    stream_name: &str,
    subject: &str,
    durable_name: &str,
    ack_wait: Duration,
) {
    use async_nats::jetstream::consumer::{pull, AckPolicy, DeliverPolicy, ReplayPolicy};

    let stream = jetstream
        .create_stream(async_nats::jetstream::stream::Config {
            name: stream_name.to_owned(),
            subjects: vec![subject.to_owned()],
            max_messages: 100,
            ..Default::default()
        })
        .await
        .expect("isolated JetStream should be created");
    stream
        .create_consumer_strict(pull::Config {
            durable_name: Some(durable_name.to_owned()),
            deliver_policy: DeliverPolicy::All,
            ack_policy: AckPolicy::Explicit,
            ack_wait,
            filter_subject: subject.to_owned(),
            replay_policy: ReplayPolicy::Instant,
            ..Default::default()
        })
        .await
        .expect("compatible durable should be created");
}

async fn delete_stream(connector: &dyn dbtool_core::port::connector::Connector, stream_name: &str) {
    let outcome = connector
        .as_admin_mutate()
        .expect("NATS adapter should expose destructive admin")
        .delete_resource(
            MessageResource {
                kind: MessageResourceKind::NatsJetstream,
                name: stream_name.to_owned(),
            },
            DeleteResourceOptions::default(),
        )
        .await
        .expect("JetStream cleanup should succeed");
    assert!(outcome.acknowledged);
    assert!(outcome.verified_absent);
}

#[tokio::test]
async fn core_queue_groups_and_jetstream_durables_are_stateful_only_where_supported() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let suffix = unique_suffix();
    let connector = factory(Dsn::parse(&dsn).expect("NATS DSN should parse"))
        .await
        .expect("NATS adapter should connect");
    let direct = async_nats::connect(dsn.clone())
        .await
        .expect("direct NATS fixture client should connect");

    let core_subject = format!("dbtool.it.nats.group.{suffix}");
    let core_consume = connector
        .as_consumer()
        .expect("NATS adapter should expose consume")
        .consume(
            &core_subject,
            ConsumeOptions {
                max: 1,
                timeout: Duration::from_secs(3),
                identity: ConsumerIdentity::Group {
                    group: format!("dbtool-workers-{suffix}"),
                    member: None,
                },
                ack: AckMode::None,
                ..Default::default()
            },
        );
    let core_publish = async {
        tokio::time::sleep(Duration::from_millis(150)).await;
        direct
            .publish(core_subject.clone(), Bytes::from_static(b"\0core\xff"))
            .await
            .expect("core fixture publish should enqueue");
        direct.flush().await.expect("core fixture should flush");
    };
    let (core_messages, ()) = tokio::join!(core_consume, core_publish);
    let core_messages = core_messages.expect("queue group should receive its delivery");
    assert_eq!(core_messages.len(), 1);
    assert_eq!(core_messages[0].payload, Bytes::from_static(b"\0core\xff"));
    assert!(core_messages[0].cursor.is_none());
    assert!(core_messages[0].metadata.is_none());

    let member_error = connector
        .as_consumer()
        .expect("NATS adapter should expose consume")
        .consume(
            &core_subject,
            ConsumeOptions {
                max: 1,
                timeout: Duration::from_millis(50),
                identity: ConsumerIdentity::Group {
                    group: "workers".to_owned(),
                    member: Some("invented-member".to_owned()),
                },
                ack: AckMode::None,
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(
        member_error,
        Err(Error::Config(message)) if message.contains("stable member")
    ));

    let stream_name = format!("DBTOOL_NATS_STATEFUL_{suffix}").to_ascii_uppercase();
    let subject = format!("dbtool.it.nats.jetstream.{suffix}");
    let durable_name = format!("DBTOOL_DURABLE_{suffix}").to_ascii_uppercase();
    let jetstream = async_nats::jetstream::new(direct.clone());
    create_stream_and_consumer(
        &jetstream,
        &stream_name,
        &subject,
        &durable_name,
        Duration::from_millis(250),
    )
    .await;
    let sparse_stream_name = format!("DBTOOL_NATS_SPARSE_{suffix}").to_ascii_uppercase();
    jetstream
        .create_stream(async_nats::jetstream::stream::Config {
            name: sparse_stream_name.clone(),
            subjects: vec![format!("dbtool.it.nats.sparse.{suffix}")],
            max_messages: 1,
            ..Default::default()
        })
        .await
        .expect("unrelated sparse JetStream should be created");

    let admin = connector
        .as_admin()
        .expect("NATS adapter should expose admin");
    let detail = admin
        .topic_detail_bounded(
            &stream_name,
            MetadataBudget::with_default_bytes(11).expect("exact detail budget should be valid"),
        )
        .await
        .expect("bounded stream detail should fit its exact nested-item budget");
    assert_eq!(detail.info.name, stream_name);
    assert_eq!(detail.config["subjects"], subject);
    assert!(matches!(
        admin
            .topic_detail_bounded(
                &stream_name,
                MetadataBudget::with_default_bytes(10).unwrap(),
            )
            .await,
        Err(Error::MetadataBudgetExceeded {
            unit: "items",
            limit: 10,
            ..
        })
    ));

    for payload in [
        Bytes::from_static(b"\0first\xff"),
        Bytes::new(),
        Bytes::from_static(b"third"),
    ] {
        jetstream
            .publish(subject.clone(), payload)
            .await
            .expect("JetStream publish should start")
            .await
            .expect("JetStream publish should be acknowledged");
    }

    let consumer = connector
        .as_consumer()
        .expect("NATS adapter should expose consume");
    let first = consumer
        .consume(&subject, durable_options(&durable_name, 2, AckMode::None))
        .await
        .expect("ack-none durable read should succeed");
    assert_eq!(first.len(), 2);
    assert_eq!(first[0].payload, Bytes::from_static(b"\0first\xff"));
    assert!(first[1].payload.is_empty());
    let first_cursors = first
        .iter()
        .map(|message| {
            message
                .cursor
                .clone()
                .expect("JetStream cursor is required")
        })
        .collect::<Vec<_>>();

    let lag = admin
        .consumer_lag(&durable_name)
        .await
        .expect("JetStream lag should load");
    let lag = lag
        .iter()
        .find(|item| item.topic == stream_name)
        .expect("durable lag should include the fixture stream");
    assert_eq!((lag.committed, lag.latest, lag.lag), (0, 3, 3));
    // Count the live catalog immediately before the bounded calls. Stale
    // streams from earlier runs are therefore included in N, while the
    // unrelated stream above guarantees N >= 2 for the N-1 assertion.
    let stream_count = jetstream
        .stream_names()
        .try_collect::<Vec<_>>()
        .await
        .expect("live stream names should enumerate")
        .len();
    assert!(stream_count >= 2);
    let bounded_lag = admin
        .consumer_lag_bounded(
            &durable_name,
            MetadataBudget::with_default_bytes(stream_count).unwrap(),
        )
        .await
        .expect("bounded lag should query the named consumer without listing all consumers");
    let bounded_lag = bounded_lag
        .iter()
        .find(|item| item.topic == stream_name)
        .expect("bounded durable lag should include the fixture stream");
    assert_eq!(
        (bounded_lag.committed, bounded_lag.latest, bounded_lag.lag,),
        (0, 3, 3)
    );
    assert!(matches!(
        admin
            .consumer_lag_bounded(
                &durable_name,
                MetadataBudget::with_default_bytes(stream_count - 1).unwrap(),
            )
            .await,
        Err(Error::MetadataBudgetExceeded {
            unit: "items",
            limit,
            ..
        }) if limit == stream_count - 1
    ));

    tokio::time::sleep(Duration::from_millis(400)).await;
    let mut budget_failure = durable_options(&durable_name, 2, AckMode::OnSuccess);
    budget_failure.max_message_bytes = 1;
    assert!(matches!(
        consumer.consume(&subject, budget_failure).await,
        Err(Error::ReadBudgetExceeded {
            unit: "bytes",
            limit: 1,
            ..
        })
    ));
    let durable_info = jetstream
        .get_stream(&stream_name)
        .await
        .expect("stream should exist after a consume budget failure")
        .consumer_info(&durable_name)
        .await
        .expect("durable should remain inspectable after a budget failure");
    assert_eq!(
        durable_info.ack_floor.stream_sequence, 0,
        "budget failure must occur before the first double-ACK"
    );

    tokio::time::sleep(Duration::from_millis(400)).await;
    let replayed = consumer
        .consume(&subject, durable_options(&durable_name, 2, AckMode::None))
        .await
        .expect("expired unacknowledged deliveries should replay");
    assert_eq!(replayed.len(), 2);
    assert_eq!(
        replayed
            .iter()
            .map(|message| message
                .cursor
                .clone()
                .expect("JetStream cursor is required"))
            .collect::<Vec<_>>(),
        first_cursors
    );
    assert!(replayed
        .iter()
        .all(|message| matches!(message.cursor, Some(MessageCursor::NatsJetstream { .. }))));

    tokio::time::sleep(Duration::from_millis(400)).await;
    let acknowledged = consumer
        .consume(
            &subject,
            durable_options(&durable_name, 2, AckMode::OnSuccess),
        )
        .await
        .expect("complete replayed batch should be double-ACKed");
    assert_eq!(acknowledged.len(), 2);

    let lag = connector
        .as_admin()
        .expect("NATS adapter should expose admin")
        .consumer_lag(&durable_name)
        .await
        .expect("post-ACK lag should load");
    let lag = lag
        .iter()
        .find(|item| item.topic == stream_name)
        .expect("durable lag should include the fixture stream");
    assert_eq!((lag.committed, lag.latest, lag.lag), (2, 3, 1));

    let last = consumer
        .consume(
            &subject,
            durable_options(&durable_name, 1, AckMode::OnSuccess),
        )
        .await
        .expect("remaining message should be double-ACKed");
    assert_eq!(last.len(), 1);
    assert_eq!(last[0].payload, Bytes::from_static(b"third"));

    let lag = connector
        .as_admin()
        .expect("NATS adapter should expose admin")
        .consumer_lag(&durable_name)
        .await
        .expect("settled lag should load");
    let lag = lag
        .iter()
        .find(|item| item.topic == stream_name)
        .expect("durable lag should include the fixture stream");
    assert_eq!((lag.committed, lag.latest, lag.lag), (3, 3, 0));

    let auto_name = format!("DBTOOL_AUTO_{suffix}").to_ascii_uppercase();
    let auto = consumer
        .consume(
            &subject,
            durable_options(&auto_name, 10, AckMode::OnSuccess),
        )
        .await
        .expect("partially filled auto-created durable batch should retain ACK budget");
    assert_eq!(auto.len(), 3);
    let auto_info = jetstream
        .get_stream(&stream_name)
        .await
        .expect("stream should exist")
        .consumer_info(&auto_name)
        .await
        .expect("auto-created durable should exist");
    assert_eq!(
        auto_info.config.durable_name.as_deref(),
        Some(auto_name.as_str())
    );
    assert_eq!(auto_info.config.filter_subject, subject);
    assert_eq!(
        auto_info.config.deliver_policy,
        async_nats::jetstream::consumer::DeliverPolicy::All
    );
    assert_eq!(
        auto_info.config.ack_policy,
        async_nats::jetstream::consumer::AckPolicy::Explicit
    );
    assert_eq!(auto_info.ack_floor.stream_sequence, 3);
    assert_eq!(auto_info.num_ack_pending, 0);
    assert_eq!(auto_info.num_pending, 0);

    let incompatible_name = format!("DBTOOL_INCOMPAT_{suffix}").to_ascii_uppercase();
    jetstream
        .get_stream(&stream_name)
        .await
        .expect("stream should exist")
        .create_consumer_strict(async_nats::jetstream::consumer::pull::Config {
            durable_name: Some(incompatible_name.clone()),
            deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::New,
            ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
            filter_subject: subject.clone(),
            replay_policy: async_nats::jetstream::consumer::ReplayPolicy::Instant,
            ..Default::default()
        })
        .await
        .expect("incompatible fixture durable should be created");
    let incompatible = consumer
        .consume(
            &subject,
            durable_options(&incompatible_name, 1, AckMode::None),
        )
        .await;
    assert!(matches!(
        incompatible,
        Err(Error::Config(message))
            if message.contains("incompatible") && message.contains("not modified")
    ));
    let incompatible_info = jetstream
        .get_stream(&stream_name)
        .await
        .expect("stream should exist")
        .consumer_info(&incompatible_name)
        .await
        .expect("incompatible durable should remain inspectable");
    assert_eq!(
        incompatible_info.config.deliver_policy,
        async_nats::jetstream::consumer::DeliverPolicy::New
    );
    // A deliver-new consumer records the stream tail at creation even before
    // delivering anything. Its consumer sequence and ACK floor remain zero.
    assert_eq!(incompatible_info.delivered.consumer_sequence, 0);
    assert_eq!(incompatible_info.ack_floor.consumer_sequence, 0);
    assert_eq!(incompatible_info.ack_floor.stream_sequence, 0);

    delete_stream(connector.as_ref(), &stream_name).await;
    delete_stream(connector.as_ref(), &sparse_stream_name).await;
}

#[tokio::test]
async fn malformed_jetstream_metadata_is_not_acknowledged() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let suffix = unique_suffix();
    let connector = factory(Dsn::parse(&dsn).expect("NATS DSN should parse"))
        .await
        .expect("NATS adapter should connect");
    let direct = async_nats::connect(dsn)
        .await
        .expect("direct NATS fixture client should connect");
    let jetstream = async_nats::jetstream::new(direct);
    let stream_name = format!("DBTOOL_NATS_MALFORMED_{suffix}").to_ascii_uppercase();
    let subject = format!("dbtool.it.nats.malformed.{suffix}");
    let durable_name = format!("DBTOOL_BAD_{suffix}").to_ascii_uppercase();
    create_stream_and_consumer(
        &jetstream,
        &stream_name,
        &subject,
        &durable_name,
        Duration::from_millis(250),
    )
    .await;

    let mut headers = async_nats::HeaderMap::new();
    headers.append("trace", "one");
    headers.append("trace", "two");
    jetstream
        .publish_with_headers(subject.clone(), headers, Bytes::from_static(b"payload"))
        .await
        .expect("malformed fixture publish should start")
        .await
        .expect("malformed fixture publish should be stored");

    let outcome = connector
        .as_consumer()
        .expect("NATS adapter should expose consume")
        .consume(
            &subject,
            durable_options(&durable_name, 1, AckMode::OnSuccess),
        )
        .await;
    assert!(matches!(
        outcome,
        Err(Error::Serialization(message)) if message.contains("2 values")
    ));

    let info = jetstream
        .get_stream(&stream_name)
        .await
        .expect("stream should exist")
        .consumer_info(&durable_name)
        .await
        .expect("durable should exist");
    assert_eq!(info.ack_floor.stream_sequence, 0);
    assert_eq!(info.num_ack_pending, 1);
    assert_eq!(info.num_pending, 0);

    tokio::time::sleep(Duration::from_millis(400)).await;
    let replay = connector
        .as_consumer()
        .expect("NATS adapter should expose consume")
        .consume(
            &subject,
            durable_options(&durable_name, 1, AckMode::OnSuccess),
        )
        .await;
    assert!(matches!(
        replay,
        Err(Error::Serialization(message)) if message.contains("2 values")
    ));
    let info = jetstream
        .get_stream(&stream_name)
        .await
        .expect("stream should exist")
        .consumer_info(&durable_name)
        .await
        .expect("durable should exist");
    assert_eq!(info.ack_floor.stream_sequence, 0);
    assert_eq!(info.num_ack_pending, 1);
    assert!(info.num_redelivered >= 1);

    delete_stream(connector.as_ref(), &stream_name).await;
}
