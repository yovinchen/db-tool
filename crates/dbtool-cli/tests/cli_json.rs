use std::process::{Command, Output};

use serde_json::Value;

fn dbtool(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_dbtool"))
        .args(args)
        .output()
        .expect("dbtool command should run")
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
