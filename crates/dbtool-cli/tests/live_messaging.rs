use std::{
    env,
    process::{Command, Output, Stdio},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde_json::Value;

fn integration_enabled() -> bool {
    env::var("DBTOOL_RUN_MQ_INTEGRATION").as_deref() == Ok("1")
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

#[test]
fn redis_live_stream_produce_detail_and_consume() {
    if !integration_enabled() {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_REDIS_DSN") else {
        return;
    };
    let stream = unique_name("dbtool_redis_stream");

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
}

#[test]
fn redis_live_pubsub_publish_and_subscribe_round_trip() {
    if !integration_enabled() {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_REDIS_DSN") else {
        return;
    };
    let channel = format!("pubsub:{}", unique_name("dbtool_redis_channel"));

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
    let topic = unique_name("dbtool_kafka_topic");

    let blocked = stderr_json(dbtool(&["--dsn", &dsn, "mq", "produce", &topic, "blocked"]));
    assert_eq!(blocked["error"]["code"], "WRITE_NOT_ALLOWED");

    let produced = stdout_json_retry(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "mq",
        "produce",
        &topic,
        "kafka-payload",
    ]);
    assert_eq!(produced["data"]["produced"], 1);

    let topics = stdout_json(dbtool(&["--dsn", &dsn, "mq", "topics"]));
    assert!(topics["data"]
        .as_array()
        .expect("topics should be an array")
        .iter()
        .any(|item| item["name"] == topic));

    let detail = stdout_json(dbtool(&["--dsn", &dsn, "mq", "detail", &topic]));
    assert_eq!(detail["data"]["info"]["name"], topic);
    assert_eq!(detail["data"]["watermarks"][0]["low"], 0);
    assert!(detail["data"]["watermarks"][0]["high"].as_i64().unwrap() >= 1);

    let consumed = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "mq",
        "consume",
        &topic,
        "--max",
        "1",
        "--timeout",
        "5",
    ]));
    assert_eq!(payload_text(&consumed["data"][0]), "kafka-payload");
}

#[test]
fn amqp_live_queue_produce_detail_and_consume() {
    if !integration_enabled() {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_AMQP_DSN") else {
        return;
    };
    let queue = unique_name("dbtool_amqp_queue");

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
}

#[test]
fn nats_live_publish_and_subscribe_round_trip() {
    if !integration_enabled() {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_NATS_DSN") else {
        return;
    };
    let subject = unique_subject("dbtool_nats_subject");

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
