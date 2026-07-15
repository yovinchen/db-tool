use dbtool_core::dsn::Dsn;
use serde_json::Value;
use std::{
    env,
    io::{BufRead, BufReader, Write},
    net::TcpStream,
    panic::{catch_unwind, AssertUnwindSafe},
    process::{Command, Output},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const CATALOG_CEILING: usize = 4096;

struct CleanupGuard {
    cleanup: Option<Box<dyn FnOnce()>>,
}

impl CleanupGuard {
    fn new(cleanup: impl FnOnce() + 'static) -> Self {
        Self {
            cleanup: Some(Box::new(cleanup)),
        }
    }

    fn run(mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            cleanup();
        }
    }
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            // Cleanup must not cause a second panic while preserving the
            // original test failure. Individual resource deletion attempts
            // are still all executed by resource_cleanup_guard.
            let _ = catch_unwind(AssertUnwindSafe(cleanup));
        }
    }
}

fn integration_enabled() -> bool {
    env::var("DBTOOL_RUN_MQ_INTEGRATION").as_deref() == Ok("1")
}

fn dsn(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
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

fn unique_name(prefix: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after Unix epoch")
        .as_millis();
    format!("{prefix}_{}_{}", std::process::id(), millis)
}

fn catalog(dsn: &str, limit: usize) -> Value {
    stdout_json(dbtool(&[
        "--limit",
        &limit.to_string(),
        "--dsn",
        dsn,
        "mq",
        "topics",
    ]))
}

fn catalog_count(value: &Value) -> usize {
    value["data"]
        .as_array()
        .expect("message catalog should be an array")
        .len()
}

fn complete_catalog_count(dsn: &str) -> usize {
    let value = catalog(dsn, CATALOG_CEILING);
    assert_eq!(
        value["meta"]["truncated"], false,
        "integration fixture exceeds catalog ceiling: {value}"
    );
    catalog_count(&value)
}

fn wait_for_catalog_count(dsn: &str, expected: usize) -> Value {
    let mut last = None;
    for _ in 0..20 {
        let value = catalog(dsn, CATALOG_CEILING);
        if value["meta"]["truncated"] == false && catalog_count(&value) == expected {
            return value;
        }
        last = Some(value);
        thread::sleep(Duration::from_millis(250));
    }
    panic!(
        "catalog count did not become {expected}; last response: {}",
        last.expect("catalog should have been read")
    );
}

fn assert_bounded_capability(dsn: &str) {
    let caps = stdout_json(dbtool(&["--dsn", dsn, "caps"]));
    assert!(caps["data"]["operations"]
        .as_array()
        .expect("operations should be an array")
        .iter()
        .any(|operation| operation == "message.admin.list_topics_bounded"));
}

fn try_catalog_contains(dsn: &str, name: &str) -> Result<bool, String> {
    let output = dbtool(&[
        "--limit",
        &CATALOG_CEILING.to_string(),
        "--dsn",
        dsn,
        "mq",
        "topics",
    ]);
    if !output.status.success() {
        return Err(format!(
            "catalog read failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let value: Value = serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("catalog response was not JSON: {error}"))?;
    if value["meta"]["truncated"] != false {
        return Err("catalog was truncated, so absence cannot be proven".to_owned());
    }
    let items = value["data"]
        .as_array()
        .ok_or_else(|| "catalog response data was not an array".to_owned())?;
    Ok(items.iter().any(|item| item["name"].as_str() == Some(name)))
}

fn try_confirmed_delete(dsn: &str, kind: &str, name: &str) -> Result<(), String> {
    let first = dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "mq",
        "delete",
        "--kind",
        kind,
        name,
    ]);
    if first.status.success() {
        return Err("delete unexpectedly succeeded without confirmation".to_owned());
    }
    let first_json: Value = serde_json::from_slice(&first.stderr)
        .map_err(|error| format!("delete challenge was not JSON: {error}"))?;
    if first_json["error"]["code"] != "CONFIRM_REQUIRED" {
        return Err(format!("delete challenge failed: {first_json}"));
    }
    let token = first_json["error"]["confirm_token"]
        .as_str()
        .ok_or_else(|| "delete challenge omitted confirmation token".to_owned())?;
    let second = dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "--confirm",
        token,
        "mq",
        "delete",
        "--kind",
        kind,
        name,
    ]);
    if !second.status.success() {
        return Err(format!(
            "confirmed delete failed: {}",
            String::from_utf8_lossy(&second.stderr)
        ));
    }
    serde_json::from_slice::<Value>(&second.stdout)
        .map_err(|error| format!("confirmed delete response was not JSON: {error}"))?;
    Ok(())
}

fn resource_cleanup_guard(dsn: &str, kind: &str, names: &[String]) -> CleanupGuard {
    let dsn = dsn.to_owned();
    let kind = kind.to_owned();
    let names = names.to_vec();
    CleanupGuard::new(move || {
        let mut failed = Vec::new();
        for name in names {
            let deadline = Instant::now() + Duration::from_secs(10);
            let absence_grace = Duration::from_secs(2);
            let mut absent_since = None;
            let mut failure = None;
            loop {
                let error = match try_catalog_contains(&dsn, &name) {
                    Ok(false) => {
                        let first_absent = absent_since.get_or_insert_with(Instant::now);
                        if first_absent.elapsed() >= absence_grace {
                            break;
                        }
                        "resource is absent during the propagation grace window".to_owned()
                    }
                    Ok(true) => match try_confirmed_delete(&dsn, &kind, &name) {
                        Ok(()) => break,
                        Err(error) => {
                            absent_since = None;
                            error
                        }
                    },
                    Err(error) => {
                        absent_since = None;
                        error
                    }
                };
                if Instant::now() >= deadline {
                    failure = Some(error);
                    break;
                }
                thread::sleep(Duration::from_millis(250));
            }
            if let Some(error) = failure {
                failed.push(format!("{name}: {error}"));
            }
        }
        assert!(
            failed.is_empty(),
            "failed to clean message resources: {failed:?}"
        );
    })
}

fn produce(dsn: &str, target: &str) {
    let produced = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "mq",
        "produce",
        target,
        "bounded-catalog-fixture",
    ]));
    assert_eq!(produced["data"]["produced"], 1);
}

fn wait_for_queue_counts(management_dsn: &str, queue: &str) {
    let mut last = "queue detail was not requested".to_owned();
    for _ in 0..20 {
        let output = dbtool(&["--dsn", management_dsn, "mq", "detail", queue]);
        if output.status.success() {
            return;
        }
        last = String::from_utf8_lossy(&output.stderr).into_owned();
        thread::sleep(Duration::from_millis(250));
    }
    panic!("RabbitMQ queue counts did not become complete for {queue:?}: {last}");
}

fn redis_command(raw_dsn: &str, arguments: &[&str]) {
    let dsn = Dsn::parse(raw_dsn).expect("Redis fixture DSN should parse");
    let host = dsn
        .host
        .as_deref()
        .expect("Redis DSN should include a host");
    let port = dsn.port.unwrap_or(6379);
    let stream = TcpStream::connect((host, port)).expect("Redis fixture should connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("read timeout should configure");
    let mut connection = BufReader::new(stream);
    let mut request = format!("*{}\r\n", arguments.len()).into_bytes();
    for argument in arguments {
        request.extend_from_slice(format!("${}\r\n", argument.len()).as_bytes());
        request.extend_from_slice(argument.as_bytes());
        request.extend_from_slice(b"\r\n");
    }
    connection
        .get_mut()
        .write_all(&request)
        .expect("Redis fixture command should write");
    connection
        .get_mut()
        .flush()
        .expect("Redis fixture command should flush");
    let mut response = String::new();
    connection
        .read_line(&mut response)
        .expect("Redis fixture response should read");
    assert!(
        !response.starts_with('-'),
        "Redis command failed: {response}"
    );
}

#[test]
fn redis_catalog_distinguishes_n_from_n_plus_one_and_cleans_up() {
    if !integration_enabled() {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_REDIS_DSN") else {
        return;
    };
    let first = unique_name("dbtool_it_bounded_redis_a");
    let second = unique_name("dbtool_it_bounded_redis_b");
    let cleanup = resource_cleanup_guard(&dsn, "redis-stream", &[first.clone(), second.clone()]);
    let baseline = complete_catalog_count(&dsn);
    assert_bounded_capability(&dsn);

    redis_command(&dsn, &["XADD", &first, "*", "payload", "one"]);
    wait_for_catalog_count(&dsn, baseline + 1);
    let exact = catalog(&dsn, baseline + 1);
    assert_eq!(catalog_count(&exact), baseline + 1);
    assert_eq!(exact["meta"]["truncated"], false);

    redis_command(&dsn, &["XADD", &second, "*", "payload", "two"]);
    wait_for_catalog_count(&dsn, baseline + 2);
    let probed = catalog(&dsn, baseline + 1);
    assert_eq!(catalog_count(&probed), baseline + 1);
    assert_eq!(probed["meta"]["truncated"], true);

    cleanup.run();
    wait_for_catalog_count(&dsn, baseline);
}

#[test]
fn kafka_catalog_distinguishes_n_from_n_plus_one_and_cleans_up() {
    if !integration_enabled() {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_KAFKA_DSN") else {
        return;
    };
    let first = unique_name("dbtool_it_bounded_kafka_a");
    let second = unique_name("dbtool_it_bounded_kafka_b");
    let cleanup = resource_cleanup_guard(&dsn, "kafka-topic", &[first.clone(), second.clone()]);
    let baseline = complete_catalog_count(&dsn);
    assert_bounded_capability(&dsn);

    produce(&dsn, &first);
    wait_for_catalog_count(&dsn, baseline + 1);
    let exact = catalog(&dsn, baseline + 1);
    assert_eq!(catalog_count(&exact), baseline + 1);
    assert_eq!(exact["meta"]["truncated"], false);

    produce(&dsn, &second);
    wait_for_catalog_count(&dsn, baseline + 2);
    let probed = catalog(&dsn, baseline + 1);
    assert_eq!(catalog_count(&probed), baseline + 1);
    assert_eq!(probed["meta"]["truncated"], true);

    cleanup.run();
    wait_for_catalog_count(&dsn, baseline);
}

#[test]
fn rabbit_catalog_distinguishes_n_from_n_plus_one_and_cleans_up() {
    if !integration_enabled() {
        return;
    }
    let (Some(amqp_dsn), Some(management_dsn)) = (
        dsn("DBTOOL_IT_AMQP_DSN"),
        dsn("DBTOOL_IT_RABBITMQ_MANAGEMENT_DSN"),
    ) else {
        return;
    };
    let first = unique_name("dbtool_it_bounded_rabbit_a");
    let second = unique_name("dbtool_it_bounded_rabbit_b");
    let cleanup = resource_cleanup_guard(
        &management_dsn,
        "amqp-queue",
        &[first.clone(), second.clone()],
    );
    let baseline = complete_catalog_count(&management_dsn);
    assert_bounded_capability(&management_dsn);

    produce(&amqp_dsn, &first);
    wait_for_catalog_count(&management_dsn, baseline + 1);
    let exact = catalog(&management_dsn, baseline + 1);
    assert_eq!(catalog_count(&exact), baseline + 1);
    assert_eq!(exact["meta"]["truncated"], false);

    produce(&amqp_dsn, &second);
    wait_for_catalog_count(&management_dsn, baseline + 2);
    let probed = catalog(&management_dsn, baseline + 1);
    assert_eq!(catalog_count(&probed), baseline + 1);
    assert_eq!(probed["meta"]["truncated"], true);

    // A newly declared queue can briefly be listed before management has
    // published exact ready/unacknowledged counts. Deletion correctly refuses
    // that incomplete preflight, so wait for the complete snapshot first.
    wait_for_queue_counts(&management_dsn, &first);
    wait_for_queue_counts(&management_dsn, &second);
    cleanup.run();
    wait_for_catalog_count(&management_dsn, baseline);
}

#[test]
fn nats_catalog_distinguishes_n_from_n_plus_one_and_cleans_up() {
    if !integration_enabled() {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_NATS_DSN") else {
        return;
    };
    let first = unique_name("DBTOOL_IT_BOUNDED_NATS_A").to_ascii_uppercase();
    let second = unique_name("DBTOOL_IT_BOUNDED_NATS_B").to_ascii_uppercase();
    let cleanup = resource_cleanup_guard(&dsn, "nats-jetstream", &[first.clone(), second.clone()]);
    let baseline = complete_catalog_count(&dsn);
    assert_bounded_capability(&dsn);

    let runtime = tokio::runtime::Runtime::new().expect("Tokio runtime should start");
    runtime.block_on(async {
        let client = async_nats::connect(dsn.clone())
            .await
            .expect("NATS fixture should connect");
        let jetstream = async_nats::jetstream::new(client);
        jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: first.clone(),
                subjects: vec![format!("{}.events", first.to_ascii_lowercase())],
                ..Default::default()
            })
            .await
            .expect("first stream should be created");
    });
    wait_for_catalog_count(&dsn, baseline + 1);
    let exact = catalog(&dsn, baseline + 1);
    assert_eq!(catalog_count(&exact), baseline + 1);
    assert_eq!(exact["meta"]["truncated"], false);

    runtime.block_on(async {
        let client = async_nats::connect(dsn.clone())
            .await
            .expect("NATS fixture should connect");
        let jetstream = async_nats::jetstream::new(client);
        jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: second.clone(),
                subjects: vec![format!("{}.events", second.to_ascii_lowercase())],
                ..Default::default()
            })
            .await
            .expect("second stream should be created");
    });
    wait_for_catalog_count(&dsn, baseline + 2);
    let probed = catalog(&dsn, baseline + 1);
    assert_eq!(catalog_count(&probed), baseline + 1);
    assert_eq!(probed["meta"]["truncated"], true);

    cleanup.run();
    wait_for_catalog_count(&dsn, baseline);
}
