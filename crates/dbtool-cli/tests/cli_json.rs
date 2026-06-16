use std::{
    fs,
    path::Path,
    process::{Command, Output},
    sync::atomic::{AtomicU64, Ordering},
};

use serde_json::Value;

static CONFIG_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn dbtool(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_dbtool"))
        .args(args)
        .output()
        .expect("dbtool command should run")
}

fn dbtool_with_config(args: &[&str], config: &str) -> Output {
    let root = std::env::temp_dir().join(format!(
        "dbtool-cli-config-{}-{}",
        std::process::id(),
        unique_suffix()
    ));
    let config_dir = root.join("dbtool");
    fs::create_dir_all(&config_dir).expect("config directory should be created");
    fs::write(config_dir.join("connections.toml"), config).expect("config should be written");

    let output = Command::new(env!("CARGO_BIN_EXE_dbtool"))
        .env("XDG_CONFIG_HOME", &root)
        .env_remove("DBTOOL_CONN_LIMIT_TEST")
        .args(args)
        .output()
        .expect("dbtool command should run");

    cleanup_dir(&root);
    output
}

fn unique_suffix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos();
    let counter = CONFIG_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{nanos}-{counter}")
}

fn cleanup_dir(path: &Path) {
    fs::remove_dir_all(path).ok();
}

fn stdout_json(output: &Output) -> Value {
    assert!(
        output.status.success(),
        "expected success\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("stdout should be JSON")
}

fn stderr_json(output: &Output) -> Value {
    assert!(
        !output.status.success(),
        "expected failure\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stderr).expect("stderr should be JSON")
}

fn stdout_text(output: &Output) -> String {
    assert!(
        output.status.success(),
        "expected success\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout.clone()).expect("stdout should be valid UTF-8")
}

#[test]
fn cli_help_documents_kv_and_doc_subcommands() {
    let kv_help = stdout_text(&dbtool(&["kv", "--help"]));
    assert!(kv_help.contains("Read one key"));
    assert!(kv_help.contains("Write one string value"));
    assert!(kv_help.contains("Scan keys matching a pattern"));
    assert!(kv_help.contains("Delete one or more keys"));

    let doc_help = stdout_text(&dbtool(&["doc", "--help"]));
    assert!(doc_help.contains("List document collections"));
    assert!(doc_help.contains("Find documents with a JSON filter"));
    assert!(doc_help.contains("Insert one JSON document"));
    assert!(doc_help.contains("Run a JSON aggregation pipeline"));
}

#[test]
fn ping_and_caps_emit_success_envelopes() {
    let ping = stdout_json(&dbtool(&["--dsn", "sqlite::memory:", "ping"]));
    assert_eq!(ping["ok"], true);
    assert_eq!(ping["kind"], "sqlite");
    assert_eq!(ping["data"]["status"], "ok");

    let caps = stdout_json(&dbtool(&["--dsn", "sqlite::memory:", "caps"]));
    assert_eq!(caps["ok"], true);
    assert_eq!(caps["data"]["sql"], true);
    assert_eq!(caps["data"]["key_value"], false);
}

#[test]
fn sql_query_returns_typed_json_rows() {
    let value = stdout_json(&dbtool(&[
        "--dsn",
        "sqlite::memory:",
        "sql",
        "query",
        "select 1 as id, 'alice' as name",
    ]));

    assert_eq!(value["ok"], true);
    assert_eq!(value["data"]["columns"][0]["name"], "id");
    assert_eq!(value["data"]["rows"][0][0], 1);
    assert_eq!(value["data"]["rows"][0][1], "alice");
}

#[test]
fn sql_query_supports_table_format() {
    let output = dbtool(&[
        "--dsn",
        "sqlite::memory:",
        "--format",
        "table",
        "sql",
        "query",
        "select 1 as id, 'alice' as name",
    ]);

    assert!(
        output.status.success(),
        "expected success\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("| id | name  |"));
    assert!(stdout.contains("| 1  | alice |"));
}

#[test]
fn sql_query_supports_ndjson_format() {
    let output = dbtool(&[
        "--dsn",
        "sqlite::memory:",
        "--format",
        "ndjson",
        "sql",
        "query",
        "select 1 as id, 'alice' as name union all select 2, 'bob'",
    ]);

    assert!(
        output.status.success(),
        "expected success\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines = stdout.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 2);
    assert_eq!(
        serde_json::from_str::<Value>(lines[0]).unwrap()["name"],
        "alice"
    );
    assert_eq!(serde_json::from_str::<Value>(lines[1]).unwrap()["id"], 2);
}

#[test]
fn destructive_sql_uses_two_step_confirm_token() {
    let first = stderr_json(&dbtool(&[
        "--dsn",
        "sqlite::memory:",
        "--allow-write",
        "sql",
        "exec",
        "create table users (id integer)",
    ]));

    assert_eq!(first["error"]["code"], "CONFIRM_REQUIRED");
    assert_eq!(first["error"]["impact"]["op"], "CREATE");
    assert_eq!(first["error"]["impact"]["target"], "dsn:sqlite::memory:");
    let token = first["error"]["confirm_token"]
        .as_str()
        .expect("confirm token should be a string");

    let second = stdout_json(&dbtool(&[
        "--dsn",
        "sqlite::memory:",
        "--allow-write",
        "--confirm",
        token,
        "sql",
        "exec",
        "create table users (id integer)",
    ]));

    assert_eq!(second["ok"], true);
    assert_eq!(second["kind"], "sqlite");
    assert_eq!(second["data"]["rows_affected"], 0);
}

#[test]
fn errors_stay_json_when_table_format_is_requested() {
    let first = stderr_json(&dbtool(&[
        "--dsn",
        "sqlite::memory:",
        "--format",
        "table",
        "--allow-write",
        "sql",
        "exec",
        "create table users (id integer)",
    ]));

    assert_eq!(first["error"]["code"], "CONFIRM_REQUIRED");
    assert!(first["error"]["confirm_token"].as_str().is_some());
}

#[test]
fn search_index_requires_write_flag_before_connecting() {
    let err = stderr_json(&dbtool(&[
        "--dsn",
        "opensearch://127.0.0.1:1",
        "search",
        "index",
        "users",
        r#"{"name":"alice"}"#,
    ]));

    assert_eq!(err["error"]["code"], "WRITE_NOT_ALLOWED");
}

#[test]
fn ts_write_requires_write_flag_before_connecting() {
    let err = stderr_json(&dbtool(&[
        "--dsn",
        "prometheus://127.0.0.1:1",
        "ts",
        "write",
        "dbtool_sample",
        "1",
    ]));

    assert_eq!(err["error"]["code"], "WRITE_NOT_ALLOWED");
}

#[test]
fn named_connection_limits_are_loaded_for_data_commands() {
    let output = dbtool_with_config(
        &["--conn", "limit-test", "ping"],
        r#"
[connections.limit-test]
dsn = "sqlite::memory:"

[connections.limit-test.limits]
request_timeout = "not-a-duration"
"#,
    );

    let err = stderr_json(&output);

    assert_eq!(err["error"]["code"], "CONFIG_ERROR");
    assert!(err["error"]["message"]
        .as_str()
        .unwrap()
        .contains("request_timeout"));
}

#[test]
fn configured_request_timeout_aborts_slow_data_commands() {
    let output = dbtool_with_config(
        &[
            "--conn",
            "timeout-test",
            "sql",
            "query",
            "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt LIMIT 100000000) SELECT sum(x) FROM cnt",
        ],
        r#"
[connections.timeout-test]
dsn = "sqlite::memory:"

[connections.timeout-test.limits]
request_timeout = "1ms"
overall_deadline = "20ms"
max_retries = 0
"#,
    );

    let err = stderr_json(&output);

    assert_eq!(err["error"]["code"], "TIMEOUT");
}

#[test]
fn request_timeout_flag_aborts_slow_data_commands() {
    let output = dbtool(&[
        "--dsn",
        "sqlite::memory:",
        "--request-timeout",
        "1ms",
        "--deadline",
        "20ms",
        "sql",
        "query",
        "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt LIMIT 100000000) SELECT sum(x) FROM cnt",
    ]);

    let err = stderr_json(&output);

    assert_eq!(err["error"]["code"], "TIMEOUT");
}

#[test]
fn invalid_rate_flag_is_a_json_config_error() {
    let output = dbtool(&["--dsn", "sqlite::memory:", "--rate", "10/hour", "ping"]);

    let err = stderr_json(&output);

    assert_eq!(err["error"]["code"], "CONFIG_ERROR");
    assert!(err["error"]["message"]
        .as_str()
        .unwrap()
        .contains("cli.limits.rate"));
}
