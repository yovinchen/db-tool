use std::{
    env,
    path::PathBuf,
    process::{Command, Output, Stdio},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use dbtool_core::dsn::Dsn;
use serde_json::Value;

fn integration_enabled() -> bool {
    env::var("DBTOOL_RUN_MQ_INTEGRATION").as_deref() == Ok("1")
}

fn tls_integration_enabled() -> bool {
    env::var("DBTOOL_RUN_MQ_TLS_INTEGRATION").as_deref() == Ok("1")
}

fn vendor_kafka_integration_enabled() -> bool {
    env::var("DBTOOL_RUN_VENDOR_KAFKA_INTEGRATION").as_deref() == Ok("1")
}

fn dbtool(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_dbtool"))
        .args(args)
        .output()
        .expect("dbtool command should run")
}

fn stdout_json(output: Output) -> Value {
    assert!(
        output.status.success(),
        "expected success\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("stdout should be JSON")
}

fn stdout_json_retry(args: &[&str]) -> Value {
    let mut last_output = None;
    for _ in 0..5 {
        let output = dbtool(args);
        if output.status.success() {
            return stdout_json(output);
        }
        last_output = Some(output);
        thread::sleep(Duration::from_secs(1));
    }

    stdout_json(last_output.expect("command should have been attempted"))
}

fn stdout_json_retry_until(args: &[&str], matches: impl Fn(&Value) -> bool) -> Value {
    let mut last_value = None;
    for _ in 0..10 {
        let value = stdout_json_retry(args);
        if matches(&value) {
            return value;
        }
        last_value = Some(value);
        thread::sleep(Duration::from_secs(1));
    }

    let last_value = last_value.expect("command should have produced JSON");
    panic!("retry condition was not met for args {args:?}; last JSON response: {last_value}");
}

fn stderr_json(output: Output) -> Value {
    assert!(
        !output.status.success(),
        "expected failure\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stderr).expect("stderr should be JSON")
}

fn dsn(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
}

fn unique_name(prefix: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_millis();
    format!("{prefix}_{}_{}", std::process::id(), millis)
}

fn unique_subject(prefix: &str) -> String {
    unique_name(prefix).replace('_', ".")
}

fn payload_text(message: &Value) -> String {
    match &message["payload"] {
        Value::String(value) => value.clone(),
        Value::Array(bytes) => {
            let bytes = bytes
                .iter()
                .map(|value| value.as_u64().expect("payload byte should be numeric") as u8)
                .collect::<Vec<_>>();
            String::from_utf8(bytes).expect("payload should be UTF-8")
        }
        other => panic!("unexpected payload JSON: {other:?}"),
    }
}

async fn nats_client_for_test(raw_dsn: &str) -> async_nats::Client {
    let dsn = Dsn::parse(raw_dsn).expect("NATS test DSN should parse");
    let driver_url = match dsn.scheme.as_str() {
        "nats" => dsn.raw.clone(),
        "nats+tls" => dsn
            .raw_with_scheme("tls")
            .expect("nats+tls should rewrite to async-nats tls scheme"),
        scheme => panic!("unexpected NATS test DSN scheme: {scheme}"),
    };
    let mut options = async_nats::ConnectOptions::new();
    if dsn.scheme == "nats+tls" {
        options = options.require_tls(true);
    }
    if let Some(path) = dsn
        .params
        .get("tls-ca")
        .or_else(|| dsn.params.get("ssl-ca"))
    {
        options = options.add_root_certificates(PathBuf::from(path));
    }

    options
        .connect(driver_url)
        .await
        .expect("NATS client should connect")
}

#[test]
fn redis_live_stream_produce_detail_and_consume() {
    if !integration_enabled() {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_REDIS_DSN") else {
        return;
    };
    let stream = unique_name("dbtool_it_redis_stream");

    let blocked = stderr_json(dbtool(&[
        "--dsn", &dsn, "mq", "produce", &stream, "blocked",
    ]));
    assert_eq!(blocked["error"]["code"], "WRITE_NOT_ALLOWED");

    let produced = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "mq",
        "produce",
        &stream,
        "redis-stream-payload",
    ]));
    assert_eq!(produced["data"]["produced"], 1);

    let topics = stdout_json(dbtool(&["--dsn", &dsn, "mq", "topics"]));
    assert!(topics["data"]
        .as_array()
        .expect("topics should be an array")
        .iter()
        .any(|item| item["name"] == stream));

    let detail = stdout_json(dbtool(&["--dsn", &dsn, "mq", "detail", &stream]));
    assert_eq!(detail["data"]["info"]["name"], stream);
    assert_eq!(detail["data"]["config"]["kind"], "stream");
    assert_eq!(detail["data"]["config"]["length"], "1");

    let consumed = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "mq",
        "consume",
        &stream,
        "--max",
        "1",
        "--timeout",
        "5",
    ]));
    assert_eq!(payload_text(&consumed["data"][0]), "redis-stream-payload");
    assert!(consumed["data"][0]["headers"]["redis_stream_id"]
        .as_str()
        .expect("stream id header should be present")
        .contains('-'));

    // Create a consumer group at offset 0 so it sees the already-produced message as undelivered.
    let group = unique_name("dbtool_it_redis_group");
    stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "raw",
        "XGROUP",
        "CREATE",
        &stream,
        &group,
        "0",
    ]));

    let lag = stdout_json(dbtool(&["--dsn", &dsn, "mq", "lag", &group]));
    let lag_items = lag["data"]
        .as_array()
        .expect("mq lag should return an array");
    assert!(
        lag_items
            .iter()
            .any(|item| item["topic"] == stream && item["lag"].as_i64().unwrap_or_default() >= 1),
        "expected lag >= 1 for group {group} on stream {stream}; output: {lag}"
    );

    let group_destroyed = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "raw",
        "XGROUP",
        "DESTROY",
        &stream,
        &group,
    ]));
    assert_eq!(group_destroyed["data"], 1);

    let deleted = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "del",
        &stream,
    ]));
    assert_eq!(deleted["data"]["deleted"], 1);

    let missing = stdout_json(dbtool(&["--dsn", &dsn, "kv", "get", &stream]));
    assert_eq!(missing["data"]["value"], Value::Null);

    let topics_after_cleanup = stdout_json(dbtool(&["--dsn", &dsn, "mq", "topics"]));
    assert!(!topics_after_cleanup["data"]
        .as_array()
        .expect("topics should be an array")
        .iter()
        .any(|item| item["name"] == stream));
}

#[test]
fn redis_live_pubsub_publish_and_subscribe_round_trip() {
    if !integration_enabled() {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_REDIS_DSN") else {
        return;
    };
    let channel = format!("pubsub:{}", unique_name("dbtool_it_redis_channel"));

    let consumer = Command::new(env!("CARGO_BIN_EXE_dbtool"))
        .args([
            "--dsn",
            &dsn,
            "mq",
            "consume",
            &channel,
            "--max",
            "1",
            "--timeout",
            "5",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Redis PubSub consume command should start");

    let mut subscriber_seen = false;
    for _ in 0..20 {
        thread::sleep(Duration::from_millis(250));
        let detail = stdout_json(dbtool(&["--dsn", &dsn, "mq", "detail", &channel]));
        if detail["data"]["config"]["subscribers"] == "1" {
            subscriber_seen = true;
            break;
        }
    }
    assert!(
        subscriber_seen,
        "Redis PubSub subscriber should be registered"
    );

    let produced = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "mq",
        "produce",
        &channel,
        "redis-pubsub-payload",
    ]));
    assert_eq!(produced["data"]["produced"], 1);

    let output = consumer
        .wait_with_output()
        .expect("Redis PubSub consume command should finish");
    let consumed = stdout_json(output);
    assert_eq!(payload_text(&consumed["data"][0]), "redis-pubsub-payload");
}

#[test]
fn kafka_live_topic_produce_detail_and_consume() {
    if !integration_enabled() {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_KAFKA_DSN") else {
        return;
    };
    run_kafka_smoke(&dsn, "kafka", "dbtool_it_kafka_topic", "kafka-payload");
}

#[test]
fn vendor_kafka_compatible_smoke_profiles() {
    if !vendor_kafka_integration_enabled() {
        return;
    }

    let vendors = [
        (
            "DBTOOL_IT_AUTOMQ_DSN",
            "automq",
            "dbtool_it_automq_topic",
            "automq-payload",
        ),
        (
            "DBTOOL_IT_WARPSTREAM_DSN",
            "warpstream",
            "dbtool_it_warpstream_topic",
            "warpstream-payload",
        ),
        (
            "DBTOOL_IT_CONFLUENT_DSN",
            "confluent",
            "dbtool_it_confluent_topic",
            "confluent-payload",
        ),
    ];

    let mut tested = 0;
    for (env_name, expected_kind, prefix, payload) in vendors {
        if let Some(dsn) = dsn(env_name) {
            run_kafka_smoke(&dsn, expected_kind, prefix, payload);
            tested += 1;
        }
    }

    assert!(tested > 0, "set at least one vendor Kafka DSN env var");
}

fn run_kafka_smoke(dsn: &str, expected_kind: &str, prefix: &str, payload: &str) {
    let ping = stdout_json_retry(&["--dsn", dsn, "ping"]);
    assert_eq!(ping["kind"], expected_kind);

    let topic = unique_name(prefix);
    let blocked = stderr_json(dbtool(&["--dsn", dsn, "mq", "produce", &topic, "blocked"]));
    assert_eq!(blocked["error"]["code"], "WRITE_NOT_ALLOWED");

    let produced = stdout_json_retry(&[
        "--dsn",
        dsn,
        "--allow-write",
        "mq",
        "produce",
        &topic,
        payload,
    ]);
    assert_eq!(produced["data"]["produced"], 1);

    let topics = stdout_json(dbtool(&["--dsn", dsn, "mq", "topics"]));
    assert!(topics["data"]
        .as_array()
        .expect("topics should be an array")
        .iter()
        .any(|item| item["name"] == topic));

    let detail = stdout_json(dbtool(&["--dsn", dsn, "mq", "detail", &topic]));
    assert_eq!(detail["data"]["info"]["name"], topic);
    assert_eq!(detail["data"]["watermarks"][0]["low"], 0);
    assert!(detail["data"]["watermarks"][0]["high"].as_i64().unwrap() >= 1);

    let consumed = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "mq",
        "consume",
        &topic,
        "--max",
        "1",
        "--timeout",
        "5",
    ]));
    assert_eq!(payload_text(&consumed["data"][0]), payload);

    // The public MQ API has no topic-delete operation. Keep that boundary explicit instead of
    // claiming cleanup; local integration environments remove the broker volume on teardown.
    let lag = stderr_json(dbtool(&["--dsn", dsn, "mq", "lag", "dbtool"]));
    assert_eq!(lag["error"]["code"], "UNSUPPORTED_CAPABILITY");
}

#[test]
fn amqp_live_queue_produce_detail_and_consume() {
    if !integration_enabled() {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_AMQP_DSN") else {
        return;
    };
    let queue = unique_name("dbtool_it_amqp_queue");

    let blocked = stderr_json(dbtool(&["--dsn", &dsn, "mq", "produce", &queue, "blocked"]));
    assert_eq!(blocked["error"]["code"], "WRITE_NOT_ALLOWED");

    let produced = stdout_json_retry(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "mq",
        "produce",
        &queue,
        "amqp-payload",
    ]);
    assert_eq!(produced["data"]["produced"], 1);

    let detail = stdout_json_retry(&["--dsn", &dsn, "mq", "detail", &queue]);
    assert_eq!(detail["data"]["info"]["name"], queue);
    assert_eq!(detail["data"]["config"]["message_count"], "1");

    let topics = stderr_json(dbtool(&["--dsn", &dsn, "mq", "topics"]));
    assert_eq!(topics["error"]["code"], "UNSUPPORTED_CAPABILITY");
    let lag = stderr_json(dbtool(&["--dsn", &dsn, "mq", "lag", &queue]));
    assert_eq!(lag["error"]["code"], "UNSUPPORTED_CAPABILITY");

    let consumed = stdout_json_retry(&[
        "--dsn",
        &dsn,
        "mq",
        "consume",
        &queue,
        "--max",
        "1",
        "--timeout",
        "5",
    ]);
    assert_eq!(payload_text(&consumed["data"][0]), "amqp-payload");

    let drained = stdout_json_retry_until(&["--dsn", &dsn, "mq", "detail", &queue], |value| {
        value["data"]["config"]["message_count"] == "0"
    });
    assert_eq!(drained["data"]["config"]["message_count"], "0");
}

#[test]
fn rabbitmq_management_live_lists_detail_and_queue_lag() {
    if !integration_enabled() {
        return;
    }
    let Some(amqp_dsn) = dsn("DBTOOL_IT_AMQP_DSN") else {
        return;
    };
    let Some(management_dsn) = dsn("DBTOOL_IT_RABBITMQ_MANAGEMENT_DSN") else {
        return;
    };
    let queue = unique_name("dbtool_it_rabbitmq_mgmt_queue");

    let produced = stdout_json_retry(&[
        "--dsn",
        &amqp_dsn,
        "--allow-write",
        "mq",
        "produce",
        &queue,
        "rabbitmq-management-payload",
    ]);
    assert_eq!(produced["data"]["produced"], 1);

    let ping = stdout_json_retry(&["--dsn", &management_dsn, "ping"]);
    assert_eq!(ping["kind"], "rabbitmq+http");
    assert_eq!(ping["ok"], true);

    let caps = stdout_json_retry(&["--dsn", &management_dsn, "caps"]);
    assert_eq!(caps["data"]["admin"], true);
    assert_eq!(caps["data"]["producer"], false);

    let topics = stdout_json_retry(&["--dsn", &management_dsn, "mq", "topics"]);
    assert!(topics["data"]
        .as_array()
        .expect("topics should be an array")
        .iter()
        .any(|item| item["name"] == queue));

    let detail = stdout_json_retry_until(
        &["--dsn", &management_dsn, "mq", "detail", &queue],
        |value| value["data"]["config"]["message_count"] == "1",
    );
    assert_eq!(detail["data"]["info"]["name"], queue);
    assert_eq!(detail["data"]["config"]["message_count"], "1");
    assert_eq!(detail["data"]["watermarks"][0]["high"], 1);

    let lag = stdout_json_retry_until(&["--dsn", &management_dsn, "mq", "lag", &queue], |value| {
        value["data"][0]["latest"] == 1 && value["data"][0]["lag"] == 1
    });
    assert_eq!(lag["data"][0]["topic"], queue);
    assert_eq!(lag["data"][0]["latest"], 1);
    assert_eq!(lag["data"][0]["lag"], 1);

    let unsupported = stderr_json(dbtool(&[
        "--dsn",
        &management_dsn,
        "--allow-write",
        "mq",
        "produce",
        &queue,
        "unsupported",
    ]));
    assert_eq!(unsupported["error"]["code"], "UNSUPPORTED_CAPABILITY");

    let consumed = stdout_json_retry(&[
        "--dsn",
        &amqp_dsn,
        "mq",
        "consume",
        &queue,
        "--max",
        "1",
        "--timeout",
        "5",
    ]);
    assert_eq!(
        payload_text(&consumed["data"][0]),
        "rabbitmq-management-payload"
    );

    let drained_detail = stdout_json_retry_until(
        &["--dsn", &management_dsn, "mq", "detail", &queue],
        |value| value["data"]["config"]["message_count"] == "0",
    );
    assert_eq!(drained_detail["data"]["config"]["message_count"], "0");

    let drained_lag =
        stdout_json_retry_until(&["--dsn", &management_dsn, "mq", "lag", &queue], |value| {
            value["data"][0]["latest"] == 0 && value["data"][0]["lag"] == 0
        });
    assert_eq!(drained_lag["data"][0]["latest"], 0);
    assert_eq!(drained_lag["data"][0]["lag"], 0);
}

#[test]
fn amqps_mq_tls_live_queue_produce_detail_and_consume() {
    if !tls_integration_enabled() {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_AMQPS_DSN") else {
        return;
    };
    let queue = unique_name("dbtool_it_amqps_queue");

    let ping = stdout_json_retry(&["--dsn", &dsn, "ping"]);
    assert_eq!(ping["kind"], "amqps");
    assert_eq!(ping["ok"], true);

    let blocked = stderr_json(dbtool(&["--dsn", &dsn, "mq", "produce", &queue, "blocked"]));
    assert_eq!(blocked["error"]["code"], "WRITE_NOT_ALLOWED");

    let produced = stdout_json_retry(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "mq",
        "produce",
        &queue,
        "amqps-payload",
    ]);
    assert_eq!(produced["data"]["produced"], 1);

    let detail = stdout_json_retry(&["--dsn", &dsn, "mq", "detail", &queue]);
    assert_eq!(detail["data"]["info"]["name"], queue);
    assert_eq!(detail["data"]["config"]["message_count"], "1");

    let topics = stderr_json(dbtool(&["--dsn", &dsn, "mq", "topics"]));
    assert_eq!(topics["error"]["code"], "UNSUPPORTED_CAPABILITY");
    let lag = stderr_json(dbtool(&["--dsn", &dsn, "mq", "lag", &queue]));
    assert_eq!(lag["error"]["code"], "UNSUPPORTED_CAPABILITY");

    let consumed = stdout_json_retry(&[
        "--dsn",
        &dsn,
        "mq",
        "consume",
        &queue,
        "--max",
        "1",
        "--timeout",
        "5",
    ]);
    assert_eq!(payload_text(&consumed["data"][0]), "amqps-payload");

    let drained = stdout_json_retry_until(&["--dsn", &dsn, "mq", "detail", &queue], |value| {
        value["data"]["config"]["message_count"] == "0"
    });
    assert_eq!(drained["data"]["config"]["message_count"], "0");
}

#[test]
fn nats_live_publish_and_subscribe_round_trip() {
    if !integration_enabled() {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_NATS_DSN") else {
        return;
    };
    let subject = unique_subject("dbtool_it_nats_subject");

    let blocked = stderr_json(dbtool(&[
        "--dsn", &dsn, "mq", "produce", &subject, "blocked",
    ]));
    assert_eq!(blocked["error"]["code"], "WRITE_NOT_ALLOWED");

    let consumer = Command::new(env!("CARGO_BIN_EXE_dbtool"))
        .args([
            "--dsn",
            &dsn,
            "mq",
            "consume",
            &subject,
            "--max",
            "1",
            "--timeout",
            "5",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("NATS consume command should start");
    thread::sleep(Duration::from_millis(500));

    let produced = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "mq",
        "produce",
        &subject,
        "nats-payload",
    ]));
    assert_eq!(produced["data"]["produced"], 1);

    let output = consumer
        .wait_with_output()
        .expect("NATS consume command should finish");
    let consumed = stdout_json(output);
    assert_eq!(payload_text(&consumed["data"][0]), "nats-payload");
}

#[test]
fn nats_live_jetstream_admin_lists_detail_and_lag() {
    if !integration_enabled() {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_NATS_DSN") else {
        return;
    };
    let stream = unique_name("DBTOOL_IT_NATS_STREAM").to_ascii_uppercase();
    let subject = format!("{}.events", stream.to_ascii_lowercase());
    let consumer = "DBTOOLCONSUMER";

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime should start");
    rt.block_on(async {
        let client = async_nats::connect(dsn.clone())
            .await
            .expect("NATS client should connect");
        let jetstream = async_nats::jetstream::new(client);
        let stream_handle = jetstream
            .get_or_create_stream(async_nats::jetstream::stream::Config {
                name: stream.clone(),
                subjects: vec![subject.clone()],
                max_messages: 100,
                ..Default::default()
            })
            .await
            .expect("JetStream stream should be created");
        jetstream
            .publish(subject.clone(), "nats-jetstream-payload".into())
            .await
            .expect("JetStream publish should start")
            .await
            .expect("JetStream publish should be acknowledged");
        stream_handle
            .get_or_create_consumer(
                consumer,
                async_nats::jetstream::consumer::pull::Config {
                    durable_name: Some(consumer.to_owned()),
                    ..Default::default()
                },
            )
            .await
            .expect("JetStream consumer should be created");
    });

    let topics = stdout_json(dbtool(&["--dsn", &dsn, "mq", "topics"]));
    assert!(topics["data"]
        .as_array()
        .expect("topics should be an array")
        .iter()
        .any(|item| item["name"] == stream));

    let detail = stdout_json(dbtool(&["--dsn", &dsn, "mq", "detail", &stream]));
    assert_eq!(detail["data"]["info"]["name"], stream);
    assert_eq!(detail["data"]["config"]["kind"], "jetstream");
    assert_eq!(detail["data"]["config"]["messages"], "1");
    assert_eq!(detail["data"]["config"]["consumer_count"], "1");

    let lag = stdout_json(dbtool(&["--dsn", &dsn, "mq", "lag", consumer]));
    assert!(lag["data"]
        .as_array()
        .expect("lag should be an array")
        .iter()
        .any(|item| item["topic"] == stream && item["group"] == consumer && item["lag"] == 1));

    rt.block_on(async {
        let client = async_nats::connect(dsn.clone())
            .await
            .expect("NATS client should reconnect for cleanup");
        let jetstream = async_nats::jetstream::new(client);
        jetstream
            .delete_stream(stream.clone())
            .await
            .expect("JetStream stream cleanup should succeed");
    });

    let topics_after_cleanup = stdout_json_retry_until(&["--dsn", &dsn, "mq", "topics"], |value| {
        !value["data"]
            .as_array()
            .expect("topics should be an array")
            .iter()
            .any(|item| item["name"] == stream)
    });
    assert!(!topics_after_cleanup["data"]
        .as_array()
        .expect("topics should be an array")
        .iter()
        .any(|item| item["name"] == stream));
}

#[test]
fn nats_mq_tls_live_publish_subscribe_and_jetstream_admin() {
    if !tls_integration_enabled() {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_NATS_TLS_DSN") else {
        return;
    };
    let subject = unique_subject("dbtool_it_nats_tls_subject");

    let ping = stdout_json_retry(&["--dsn", &dsn, "ping"]);
    assert_eq!(ping["kind"], "nats+tls");
    assert_eq!(ping["ok"], true);

    let blocked = stderr_json(dbtool(&[
        "--dsn", &dsn, "mq", "produce", &subject, "blocked",
    ]));
    assert_eq!(blocked["error"]["code"], "WRITE_NOT_ALLOWED");

    let consumer = Command::new(env!("CARGO_BIN_EXE_dbtool"))
        .args([
            "--dsn",
            &dsn,
            "mq",
            "consume",
            &subject,
            "--max",
            "1",
            "--timeout",
            "5",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("NATS TLS consume command should start");
    thread::sleep(Duration::from_millis(500));

    let produced = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "mq",
        "produce",
        &subject,
        "nats-tls-payload",
    ]));
    assert_eq!(produced["data"]["produced"], 1);

    let output = consumer
        .wait_with_output()
        .expect("NATS TLS consume command should finish");
    let consumed = stdout_json(output);
    assert_eq!(payload_text(&consumed["data"][0]), "nats-tls-payload");

    let stream = unique_name("DBTOOL_IT_NATS_TLS_STREAM").to_ascii_uppercase();
    let stream_subject = format!("{}.events", stream.to_ascii_lowercase());
    let jetstream_consumer = "DBTOOLTLSCONSUMER";

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime should start");
    rt.block_on(async {
        let client = nats_client_for_test(&dsn).await;
        let jetstream = async_nats::jetstream::new(client);
        let stream_handle = jetstream
            .get_or_create_stream(async_nats::jetstream::stream::Config {
                name: stream.clone(),
                subjects: vec![stream_subject.clone()],
                max_messages: 100,
                ..Default::default()
            })
            .await
            .expect("NATS TLS JetStream stream should be created");
        jetstream
            .publish(stream_subject.clone(), "nats-tls-jetstream-payload".into())
            .await
            .expect("NATS TLS JetStream publish should start")
            .await
            .expect("NATS TLS JetStream publish should be acknowledged");
        stream_handle
            .get_or_create_consumer(
                jetstream_consumer,
                async_nats::jetstream::consumer::pull::Config {
                    durable_name: Some(jetstream_consumer.to_owned()),
                    ..Default::default()
                },
            )
            .await
            .expect("NATS TLS JetStream consumer should be created");
    });

    let topics = stdout_json(dbtool(&["--dsn", &dsn, "mq", "topics"]));
    assert!(topics["data"]
        .as_array()
        .expect("topics should be an array")
        .iter()
        .any(|item| item["name"] == stream));

    let detail = stdout_json(dbtool(&["--dsn", &dsn, "mq", "detail", &stream]));
    assert_eq!(detail["data"]["info"]["name"], stream);
    assert_eq!(detail["data"]["config"]["kind"], "jetstream");
    assert_eq!(detail["data"]["config"]["messages"], "1");
    assert_eq!(detail["data"]["config"]["consumer_count"], "1");

    let lag = stdout_json(dbtool(&["--dsn", &dsn, "mq", "lag", jetstream_consumer]));
    assert!(lag["data"]
        .as_array()
        .expect("lag should be an array")
        .iter()
        .any(|item| item["topic"] == stream
            && item["group"] == jetstream_consumer
            && item["lag"] == 1));

    rt.block_on(async {
        let client = nats_client_for_test(&dsn).await;
        let jetstream = async_nats::jetstream::new(client);
        jetstream
            .delete_stream(stream.clone())
            .await
            .expect("NATS TLS JetStream stream cleanup should succeed");
    });

    let topics_after_cleanup = stdout_json_retry_until(&["--dsn", &dsn, "mq", "topics"], |value| {
        !value["data"]
            .as_array()
            .expect("topics should be an array")
            .iter()
            .any(|item| item["name"] == stream)
    });
    assert!(!topics_after_cleanup["data"]
        .as_array()
        .expect("topics should be an array")
        .iter()
        .any(|item| item["name"] == stream));
}
