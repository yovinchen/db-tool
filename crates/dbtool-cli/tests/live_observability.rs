use std::{
    env,
    process::{Command, Output},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde_json::Value;

fn integration_enabled() -> bool {
    env::var("DBTOOL_RUN_OBSERVABILITY_INTEGRATION").as_deref() == Ok("1")
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

fn stdout_json_retry(args: &[&str], matches: impl Fn(&Value) -> bool) -> Value {
    let mut last = None;
    for _ in 0..12 {
        let value = stdout_json(dbtool(args));
        if matches(&value) {
            return value;
        }
        last = Some(value);
        thread::sleep(Duration::from_secs(1));
    }

    last.expect("command should have produced JSON")
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

fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} should be set for this integration test"))
}

fn unique_name(prefix: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_millis();
    format!("{prefix}_{}_{}", std::process::id(), millis)
}

#[test]
fn opensearch_live_index_search_and_list() {
    if !integration_enabled() {
        return;
    }

    let dsn = required_env("DBTOOL_IT_OPENSEARCH_DSN");
    run_search_lifecycle(&dsn, "opensearch", "dbtool_search");
}

#[test]
fn opensearch_tls_live_index_search_and_list() {
    if !integration_enabled() {
        return;
    }

    let dsn = required_env("DBTOOL_IT_OPENSEARCH_TLS_DSN");
    assert_tls_seed_fixture(&dsn);
    run_search_lifecycle(&dsn, "opensearch+https", "dbtool_search_tls");
}

fn assert_tls_seed_fixture(dsn: &str) {
    let indices = stdout_json_retry(&["--dsn", dsn, "search", "indices"], |value| {
        value["data"]
            .as_array()
            .is_some_and(|items| items.iter().any(|item| item["name"] == "dbtool_seed"))
    });
    assert!(indices["data"].as_array().is_some());

    let hits = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--limit",
        "10",
        "search",
        "search",
        "dbtool_seed",
        "--q",
        r#"{"match_all":{}}"#,
    ]));
    assert_eq!(hits["data"]["total"], 2);
    assert!(hits["data"]["hits"].as_array().is_some_and(|items| {
        items
            .iter()
            .any(|item| item["_source"]["source"] == "dockerfile-fixture")
    }));
}

fn run_search_lifecycle(dsn: &str, expected_kind: &str, prefix: &str) {
    let index = unique_name(prefix);

    let ping = stdout_json(dbtool(&["--dsn", dsn, "ping"]));
    assert_eq!(ping["kind"], expected_kind);

    let blocked = stderr_json(dbtool(&[
        "--dsn",
        dsn,
        "search",
        "index",
        &index,
        r#"{"name":"alice","role":"search"}"#,
    ]));
    assert_eq!(blocked["error"]["code"], "WRITE_NOT_ALLOWED");

    let indexed = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "search",
        "index",
        &index,
        r#"{"name":"alice","role":"search"}"#,
    ]));
    assert_eq!(indexed["data"]["indexed"], true);

    let hits = stdout_json_retry(
        &[
            "--dsn",
            dsn,
            "--limit",
            "10",
            "search",
            "search",
            &index,
            "--q",
            r#"{"match_all":{}}"#,
        ],
        |value| value["data"]["total"].as_u64().unwrap_or_default() >= 1,
    );
    assert!(hits["data"]["hits"][0]["_source"]["name"]
        .as_str()
        .is_some());

    let indices = stdout_json_retry(&["--dsn", dsn, "search", "indices"], |value| {
        value["data"]
            .as_array()
            .is_some_and(|items| items.iter().any(|item| item["name"] == index))
    });
    assert!(indices["data"].as_array().is_some());
}

#[test]
fn prometheus_live_measurements_and_query() {
    if !integration_enabled() {
        return;
    }

    let dsn = required_env("DBTOOL_IT_PROMETHEUS_DSN");
    let ping = stdout_json(dbtool(&["--dsn", &dsn, "ping"]));
    assert_eq!(ping["kind"], "prometheus");

    let measurements = stdout_json_retry(&["--dsn", &dsn, "ts", "measurements"], |value| {
        value["data"]
            .as_array()
            .is_some_and(|items| items.iter().any(|item| item == "up"))
    });
    assert!(measurements["data"].as_array().is_some());

    let query = stdout_json_retry(
        &["--dsn", &dsn, "ts", "query", "up", "--last-minutes", "10"],
        |value| {
            value["data"]["series"]
                .as_array()
                .is_some_and(|series| !series.is_empty())
        },
    );
    assert_eq!(query["data"]["truncated"], false);
}
