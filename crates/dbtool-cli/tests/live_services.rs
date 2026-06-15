use std::{
    env,
    process::{Command, Output},
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::Value;

fn integration_enabled() -> bool {
    env::var("DBTOOL_RUN_INTEGRATION").as_deref() == Ok("1")
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

fn stderr_json(output: Output) -> Value {
    assert!(
        !output.status.success(),
        "expected failure\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stderr).expect("stderr should be JSON")
}

fn unique_name(prefix: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_millis();
    format!("{prefix}_{}_{}", std::process::id(), millis)
}

fn dsn(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
}

fn confirmed_sql_exec(dsn: &str, sql: &str) -> Value {
    let first = stderr_json(dbtool(&["--dsn", dsn, "--allow-write", "sql", "exec", sql]));
    assert_eq!(first["error"]["code"], "CONFIRM_REQUIRED");
    let token = first["error"]["confirm_token"]
        .as_str()
        .expect("confirm token should be a string");

    stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "--confirm",
        token,
        "sql",
        "exec",
        sql,
    ]))
}

fn sql_lifecycle(dsn: &str, table: &str, create_sql: String, drop_sql: String) {
    let ping = stdout_json(dbtool(&["--dsn", dsn, "ping"]));
    assert_eq!(ping["ok"], true);

    confirmed_sql_exec(dsn, &create_sql);

    stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "sql",
        "exec",
        &format!("insert into {table} (id, name) values (1, 'alice')"),
    ]));

    let rows = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "sql",
        "query",
        &format!("select id, name from {table} where id = 1"),
    ]));
    assert_eq!(rows["data"]["rows"][0][0], 1);
    assert_eq!(rows["data"]["rows"][0][1], "alice");

    let schema = stdout_json(dbtool(&["--dsn", dsn, "sql", "schema", table]));
    assert_eq!(schema["ok"], true);
    assert_eq!(schema["data"]["name"], table);

    confirmed_sql_exec(dsn, &drop_sql);
}

#[test]
fn postgres_live_sql_lifecycle() {
    if !integration_enabled() {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_POSTGRES_DSN") else {
        return;
    };
    let table = unique_name("dbtool_pg_users");

    sql_lifecycle(
        &dsn,
        &table,
        format!("create table {table} (id integer primary key, name text not null)"),
        format!("drop table {table}"),
    );
}

#[test]
fn mysql_live_sql_lifecycle() {
    if !integration_enabled() {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_MYSQL_DSN") else {
        return;
    };
    let table = unique_name("dbtool_mysql_users");

    sql_lifecycle(
        &dsn,
        &table,
        format!("create table {table} (id integer primary key, name varchar(64) not null)"),
        format!("drop table {table}"),
    );
}

#[test]
fn redis_live_kv_lifecycle_and_raw_safety() {
    if !integration_enabled() {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_REDIS_DSN") else {
        return;
    };
    let key = unique_name("dbtool_redis_key");

    let blocked_set = stderr_json(dbtool(&["--dsn", &dsn, "kv", "set", &key, "alice"]));
    assert_eq!(blocked_set["error"]["code"], "WRITE_NOT_ALLOWED");

    stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "set",
        &key,
        "alice",
    ]));

    let value = stdout_json(dbtool(&["--dsn", &dsn, "kv", "get", &key]));
    assert_eq!(value["data"]["value"], "alice");

    let keys = stdout_json(dbtool(&["--dsn", &dsn, "kv", "scan", &format!("{key}*")]));
    assert_eq!(keys["data"][0], key);

    let pong = stdout_json(dbtool(&["--dsn", &dsn, "kv", "raw", "PING"]));
    assert_eq!(pong["data"], "PONG");

    let blocked = stderr_json(dbtool(&["--dsn", &dsn, "kv", "raw", "FLUSHALL"]));
    assert_eq!(blocked["error"]["code"], "WRITE_NOT_ALLOWED");

    stdout_json(dbtool(&["--dsn", &dsn, "--allow-write", "kv", "del", &key]));
}

#[test]
fn mongo_live_document_lifecycle() {
    if !integration_enabled() {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_MONGO_DSN") else {
        return;
    };
    let collection = unique_name("dbtool_mongo_users");

    let ping = stdout_json(dbtool(&["--dsn", &dsn, "ping"]));
    assert_eq!(ping["ok"], true);

    let blocked_insert = stderr_json(dbtool(&[
        "--dsn",
        &dsn,
        "doc",
        "insert",
        &collection,
        r#"{"name":"alice","visits":1}"#,
    ]));
    assert_eq!(blocked_insert["error"]["code"], "WRITE_NOT_ALLOWED");

    let inserted = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "doc",
        "insert",
        &collection,
        r#"{"name":"alice","visits":1}"#,
    ]));
    assert_eq!(inserted["data"]["inserted"], 1);

    let found = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "doc",
        "find",
        &collection,
        "--filter",
        r#"{"name":"alice"}"#,
    ]));
    assert_eq!(found["data"][0]["name"], "alice");

    let updated = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "doc",
        "update",
        &collection,
        "--filter",
        r#"{"name":"alice"}"#,
        "--update",
        r#"{"visits":2}"#,
    ]));
    assert_eq!(updated["data"]["matched"], 1);

    let aggregated = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "doc",
        "aggregate",
        &collection,
        r#"[{"$match":{"name":"alice"}},{"$project":{"_id":0,"name":1,"visits":1}}]"#,
    ]));
    assert_eq!(aggregated["data"][0]["visits"], 2);

    let deleted = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "doc",
        "delete",
        &collection,
        "--filter",
        r#"{"name":"alice"}"#,
    ]));
    assert_eq!(deleted["data"]["deleted"], 1);
}
