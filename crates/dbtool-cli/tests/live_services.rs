use std::{
    env,
    process::{Command, Output},
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::Value;

fn integration_enabled() -> bool {
    env::var("DBTOOL_RUN_INTEGRATION").as_deref() == Ok("1")
}

fn compat_enabled(flag: &str) -> bool {
    env::var("DBTOOL_RUN_COMPAT_INTEGRATION").as_deref() == Ok("1")
        && env::var(flag).as_deref() == Ok("1")
}

fn pg_compat_enabled(flag: &str) -> bool {
    env::var("DBTOOL_RUN_PG_COMPAT_INTEGRATION").as_deref() == Ok("1")
        && env::var(flag).as_deref() == Ok("1")
}

fn tidb_enabled() -> bool {
    env::var("DBTOOL_RUN_TIDB_INTEGRATION").as_deref() == Ok("1")
}

fn tidb_secure_enabled() -> bool {
    env::var("DBTOOL_RUN_TIDB_SECURE_INTEGRATION").as_deref() == Ok("1")
}

fn sqlserver_enabled() -> bool {
    env::var("DBTOOL_RUN_SQLSERVER_INTEGRATION").as_deref() == Ok("1")
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

fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} should be set for this integration test"))
}

fn dsn_with_scheme(dsn: &str, scheme: &str) -> String {
    let (_, rest) = dsn
        .split_once("://")
        .expect("integration DSN should include a URL scheme");
    format!("{scheme}://{rest}")
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

fn setup_sql_exec(dsn: &str, sql: &str) -> Value {
    let first = dbtool(&["--dsn", dsn, "--allow-write", "sql", "exec", sql]);
    if first.status.success() {
        return stdout_json(first);
    }

    let first = stderr_json(first);
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

    let (schema, expected_table_name) = table
        .split_once('.')
        .map_or((None, table), |(schema, name)| (Some(schema), name));
    let tables = if let Some(schema) = schema {
        stdout_json(dbtool(&["--dsn", dsn, "sql", "tables", "--schema", schema]))
    } else {
        stdout_json(dbtool(&["--dsn", dsn, "sql", "tables"]))
    };
    let table_found = tables["data"]
        .as_array()
        .expect("tables output should be an array")
        .iter()
        .any(|entry| entry["name"].as_str() == Some(expected_table_name));
    assert!(
        table_found,
        "expected sql tables to include {expected_table_name}; output: {tables}"
    );

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
    assert_eq!(schema["data"]["name"], expected_table_name);

    confirmed_sql_exec(dsn, &drop_sql);
}

fn mysql_family_typed_probe(dsn: &str, expected_kind: &str) {
    let ping = stdout_json(dbtool(&["--dsn", dsn, "ping"]));
    assert_eq!(ping["kind"], expected_kind);
    assert_eq!(ping["ok"], true);

    let caps = stdout_json(dbtool(&["--dsn", dsn, "caps"]));
    assert_eq!(caps["kind"], expected_kind);
    assert_eq!(caps["data"]["sql"], true);

    let typed = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "sql",
        "query",
        "select cast(42 as signed) as int_value, cast(3.5 as double) as float_value, cast(X'6869' as binary) as bytes_value, cast(null as char) as null_value",
    ]));
    let row = &typed["data"]["rows"][0];
    assert_eq!(row[0], 42);
    assert_eq!(row[1].as_f64().expect("float should decode"), 3.5);
    assert_eq!(row[2], serde_json::json!([104, 105]));
    assert_eq!(row[3], Value::Null);

    let limited = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--limit",
        "2",
        "sql",
        "query",
        "select 1 as n union all select 2 union all select 3",
    ]));
    assert_eq!(limited["data"]["rows"].as_array().unwrap().len(), 2);
    assert_eq!(limited["meta"]["truncated"], true);
}

fn postgres_family_typed_probe(dsn: &str, expected_kind: &str) {
    let ping = stdout_json(dbtool(&["--dsn", dsn, "ping"]));
    assert_eq!(ping["kind"], expected_kind);
    assert_eq!(ping["ok"], true);

    let caps = stdout_json(dbtool(&["--dsn", dsn, "caps"]));
    assert_eq!(caps["kind"], expected_kind);
    assert_eq!(caps["data"]["sql"], true);

    let typed = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "sql",
        "query",
        "select cast(42 as integer) as int_value, cast(3.5 as double precision) as float_value, cast(true as boolean) as bool_value, cast('hi' as text) as text_value, cast(null as text) as null_value",
    ]));
    let row = &typed["data"]["rows"][0];
    assert_eq!(row[0], 42);
    assert_eq!(row[1].as_f64().expect("float should decode"), 3.5);
    assert_eq!(row[2], true);
    assert_eq!(row[3], "hi");
    assert_eq!(row[4], Value::Null);

    let limited = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--limit",
        "2",
        "sql",
        "query",
        "select 1 as n union all select 2 union all select 3",
    ]));
    assert_eq!(limited["data"]["rows"].as_array().unwrap().len(), 2);
    assert_eq!(limited["meta"]["truncated"], true);
}

fn sqlserver_typed_probe(dsn: &str, expected_kind: &str) {
    let ping = stdout_json(dbtool(&["--dsn", dsn, "ping"]));
    assert_eq!(ping["kind"], expected_kind);
    assert_eq!(ping["ok"], true);

    let caps = stdout_json(dbtool(&["--dsn", dsn, "caps"]));
    assert_eq!(caps["kind"], expected_kind);
    assert_eq!(caps["data"]["sql"], true);

    let typed = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "sql",
        "query",
        "select cast(42 as int) as int_value, cast(3.5 as float) as float_value, cast(1 as bit) as bool_value, cast('hi' as nvarchar(32)) as text_value, cast(null as nvarchar(32)) as null_value",
    ]));
    let row = &typed["data"]["rows"][0];
    assert_eq!(row[0], 42);
    assert_eq!(row[1].as_f64().expect("float should decode"), 3.5);
    assert_eq!(row[2], true);
    assert_eq!(row[3], "hi");
    assert_eq!(row[4], Value::Null);

    let limited = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--limit",
        "2",
        "sql",
        "query",
        "select 1 as n union all select 2 union all select 3",
    ]));
    assert_eq!(limited["data"]["rows"].as_array().unwrap().len(), 2);
    assert_eq!(limited["meta"]["truncated"], true);
}

fn assert_mysql_tls_connection(dsn: &str) {
    let status = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "sql",
        "query",
        "show status like 'Ssl_cipher'",
    ]));
    let rows = status["data"]["rows"]
        .as_array()
        .expect("TLS status should return rows");
    let cipher = rows
        .first()
        .and_then(|row| row.as_array())
        .and_then(|row| row.get(1))
        .and_then(Value::as_str)
        .unwrap_or_default();
    assert!(
        !cipher.is_empty(),
        "expected TLS connection with non-empty Ssl_cipher; output: {status}"
    );
}

fn assert_connection_rejected(dsn: &str) {
    let rejected = stderr_json(dbtool(&["--dsn", dsn, "ping"]));
    let code = rejected["error"]["code"].as_str().unwrap_or_default();
    assert!(
        matches!(code, "CONNECTION_ERROR" | "AUTH_ERROR"),
        "expected connection/auth rejection, got: {rejected}"
    );
}

fn assert_mysql_identifier(value: &str, label: &str) {
    assert!(
        !value.is_empty()
            && value
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_'),
        "{label} must be a simple MySQL identifier for integration SQL, got {value:?}"
    );
}

fn mysql_account(user: &str) -> String {
    assert_mysql_identifier(user, "user");
    format!("'{user}'@'%'")
}

fn mysql_password(password: &str) -> String {
    assert!(
        !password.contains('\'') && !password.contains('\\'),
        "password must not need escaping in integration SQL"
    );
    format!("'{password}'")
}

fn redis_family_kv_probe(dsn: &str, expected_kind: &str, prefix: &str) {
    let key = unique_name(&format!("dbtool_{prefix}_key"));
    let ttl_key = unique_name(&format!("dbtool_{prefix}_ttl"));
    let raw_key = unique_name(&format!("dbtool_{prefix}_raw"));

    let ping = stdout_json(dbtool(&["--dsn", dsn, "ping"]));
    assert_eq!(ping["kind"], expected_kind);
    assert_eq!(ping["ok"], true);

    let caps = stdout_json(dbtool(&["--dsn", dsn, "caps"]));
    assert_eq!(caps["kind"], expected_kind);
    assert_eq!(caps["data"]["key_value"], true);

    stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "kv",
        "set",
        &key,
        expected_kind,
    ]));
    let value = stdout_json(dbtool(&["--dsn", dsn, "kv", "get", &key]));
    assert_eq!(value["data"]["value"], expected_kind);

    let blocked_raw_set = stderr_json(dbtool(&[
        "--dsn",
        dsn,
        "kv",
        "raw",
        "SET",
        &raw_key,
        "raw-value",
    ]));
    assert_eq!(blocked_raw_set["error"]["code"], "WRITE_NOT_ALLOWED");

    let raw_set = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "kv",
        "raw",
        "SET",
        &raw_key,
        "raw-value",
    ]));
    assert_eq!(raw_set["data"], "OK");

    stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "kv",
        "set",
        &ttl_key,
        "short-lived",
        "--ttl",
        "30",
    ]));
    let ttl = stdout_json(dbtool(&["--dsn", dsn, "kv", "raw", "TTL", &ttl_key]));
    let ttl = ttl["data"].as_i64().expect("TTL should be numeric");
    assert!((1..=30).contains(&ttl), "unexpected TTL value: {ttl}");

    let deleted = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "kv",
        "del",
        &key,
        &ttl_key,
        &raw_key,
    ]));
    assert_eq!(deleted["data"]["deleted"], 3);
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
fn mysql_live_protocol_aliases_and_typed_values() {
    if !integration_enabled() {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_MYSQL_DSN") else {
        return;
    };

    for alias in ["mariadb", "tidb"] {
        let alias_dsn = dsn_with_scheme(&dsn, alias);

        mysql_family_typed_probe(&alias_dsn, alias);
    }
}

#[test]
fn mariadb_compat_live_sql_lifecycle_and_typed_values() {
    if !compat_enabled("DBTOOL_RUN_MARIADB_COMPAT") {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_MARIADB_DSN") else {
        return;
    };
    let table = unique_name("dbtool_mariadb_users");

    mysql_family_typed_probe(&dsn, "mariadb");
    sql_lifecycle(
        &dsn,
        &table,
        format!("create table {table} (id integer primary key, name varchar(64) not null)"),
        format!("drop table {table}"),
    );
}

#[test]
fn cockroach_pg_compat_live_sql_lifecycle_and_typed_values() {
    if !pg_compat_enabled("DBTOOL_RUN_COCKROACH_COMPAT") {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_COCKROACH_DSN") else {
        return;
    };
    let table = unique_name("dbtool_cockroach_users");

    postgres_family_typed_probe(&dsn, "cockroach");
    sql_lifecycle(
        &dsn,
        &table,
        format!("create table {table} (id integer primary key, name text not null)"),
        format!("drop table {table}"),
    );
}

#[test]
fn timescale_pg_compat_live_sql_lifecycle_and_typed_values() {
    if !pg_compat_enabled("DBTOOL_RUN_TIMESCALE_COMPAT") {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_TIMESCALE_DSN") else {
        return;
    };
    let table = unique_name("dbtool_timescale_users");

    postgres_family_typed_probe(&dsn, "timescale");
    sql_lifecycle(
        &dsn,
        &table,
        format!("create table {table} (id integer primary key, name text not null)"),
        format!("drop table {table}"),
    );
}

#[test]
fn tidb_compat_live_sql_lifecycle_and_typed_values() {
    if !tidb_enabled() {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_TIDB_DSN") else {
        return;
    };
    let database = env::var("DBTOOL_IT_TIDB_DB").unwrap_or_else(|_| "dbtool_it_tidb".to_owned());
    let table = unique_name("dbtool_tidb_users");
    let qualified_table = format!("{database}.{table}");

    mysql_family_typed_probe(&dsn, "tidb");
    setup_sql_exec(&dsn, &format!("create database if not exists {database}"));
    sql_lifecycle(
        &dsn,
        &qualified_table,
        format!(
            "create table {qualified_table} (id integer primary key, name varchar(64) not null)"
        ),
        format!("drop table {qualified_table}"),
    );
}

#[test]
fn tidb_secure_auth_tls_and_ha_live_sql_lifecycle() {
    if !tidb_secure_enabled() {
        return;
    }

    let root_dsn_1 = required_env("DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1");
    let root_dsn_2 = required_env("DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2");
    let secure_dsn_1 = required_env("DBTOOL_IT_TIDB_SECURE_DSN_1");
    let secure_dsn_2 = required_env("DBTOOL_IT_TIDB_SECURE_DSN_2");
    let disabled_dsn = required_env("DBTOOL_IT_TIDB_SECURE_DISABLED_DSN");
    let x509_dsn = required_env("DBTOOL_IT_TIDB_SECURE_X509_DSN");
    let x509_no_cert_dsn = required_env("DBTOOL_IT_TIDB_SECURE_X509_NO_CERT_DSN");
    let database = required_env("DBTOOL_IT_TIDB_SECURE_DB");
    let ssl_user = required_env("DBTOOL_IT_TIDB_SECURE_USER");
    let ssl_password = required_env("DBTOOL_IT_TIDB_SECURE_PASSWORD");
    let x509_user = required_env("DBTOOL_IT_TIDB_SECURE_X509_USER");
    let x509_password = required_env("DBTOOL_IT_TIDB_SECURE_X509_PASSWORD");

    assert_mysql_identifier(&database, "database");

    mysql_family_typed_probe(&root_dsn_1, "tidb");
    assert_mysql_tls_connection(&root_dsn_1);
    assert_mysql_tls_connection(&root_dsn_2);

    setup_sql_exec(
        &root_dsn_1,
        &format!("create database if not exists {database}"),
    );
    setup_sql_exec(
        &root_dsn_1,
        &format!("drop user if exists {}", mysql_account(&ssl_user)),
    );
    setup_sql_exec(
        &root_dsn_1,
        &format!("drop user if exists {}", mysql_account(&x509_user)),
    );
    setup_sql_exec(
        &root_dsn_1,
        &format!(
            "create user {} identified by {} require ssl",
            mysql_account(&ssl_user),
            mysql_password(&ssl_password)
        ),
    );
    setup_sql_exec(
        &root_dsn_1,
        &format!(
            "create user {} identified by {} require x509",
            mysql_account(&x509_user),
            mysql_password(&x509_password)
        ),
    );
    setup_sql_exec(
        &root_dsn_1,
        &format!(
            "grant all privileges on {database}.* to {}",
            mysql_account(&ssl_user)
        ),
    );
    setup_sql_exec(
        &root_dsn_1,
        &format!(
            "grant all privileges on {database}.* to {}",
            mysql_account(&x509_user)
        ),
    );

    assert_connection_rejected(&disabled_dsn);
    assert_connection_rejected(&x509_no_cert_dsn);

    mysql_family_typed_probe(&secure_dsn_1, "tidb");
    assert_mysql_tls_connection(&secure_dsn_1);
    let table_1 = format!("{}.{}", database, unique_name("dbtool_tidb_secure_node1"));
    sql_lifecycle(
        &secure_dsn_1,
        &table_1,
        format!("create table {table_1} (id integer primary key, name varchar(64) not null)"),
        format!("drop table {table_1}"),
    );

    mysql_family_typed_probe(&secure_dsn_2, "tidb");
    assert_mysql_tls_connection(&secure_dsn_2);
    let table_2 = format!("{}.{}", database, unique_name("dbtool_tidb_secure_node2"));
    sql_lifecycle(
        &secure_dsn_2,
        &table_2,
        format!("create table {table_2} (id integer primary key, name varchar(64) not null)"),
        format!("drop table {table_2}"),
    );

    mysql_family_typed_probe(&x509_dsn, "tidb");
    assert_mysql_tls_connection(&x509_dsn);
    let table_x509 = format!("{}.{}", database, unique_name("dbtool_tidb_x509"));
    sql_lifecycle(
        &x509_dsn,
        &table_x509,
        format!("create table {table_x509} (id integer primary key, name varchar(64) not null)"),
        format!("drop table {table_x509}"),
    );
}

#[test]
fn sqlserver_live_sql_lifecycle_and_typed_values() {
    if !sqlserver_enabled() {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_SQLSERVER_DSN") else {
        return;
    };
    let table = unique_name("dbtool_sqlserver_users");
    let qualified_table = format!("dbo.{table}");

    sqlserver_typed_probe(&dsn, "sqlserver");
    sql_lifecycle(
        &dsn,
        &qualified_table,
        format!("create table {qualified_table} (id int primary key, name nvarchar(64) not null)"),
        format!("drop table {qualified_table}"),
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
    let ttl_key = unique_name("dbtool_redis_ttl");
    let raw_key = unique_name("dbtool_redis_raw");
    let counter_key = unique_name("dbtool_redis_counter");
    let scan_prefix = unique_name("dbtool_redis_scan");
    let scan_keys = [
        format!("{scan_prefix}:1"),
        format!("{scan_prefix}:2"),
        format!("{scan_prefix}:3"),
    ];

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

    let blocked_raw_set = stderr_json(dbtool(&[
        "--dsn",
        &dsn,
        "kv",
        "raw",
        "SET",
        &raw_key,
        "raw-value",
    ]));
    assert_eq!(blocked_raw_set["error"]["code"], "WRITE_NOT_ALLOWED");

    let raw_set = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "raw",
        "SET",
        &raw_key,
        "raw-value",
    ]));
    assert_eq!(raw_set["data"], "OK");

    let incr = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "raw",
        "INCR",
        &counter_key,
    ]));
    assert_eq!(incr["data"], 1);

    let multi_get = stdout_json(dbtool(&[
        "--dsn", &dsn, "kv", "raw", "MGET", &key, &raw_key,
    ]));
    assert_eq!(multi_get["data"][0], "alice");
    assert_eq!(multi_get["data"][1], "raw-value");

    stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "set",
        &ttl_key,
        "short-lived",
        "--ttl",
        "30",
    ]));
    let ttl = stdout_json(dbtool(&["--dsn", &dsn, "kv", "raw", "TTL", &ttl_key]));
    let ttl = ttl["data"].as_i64().expect("TTL should be numeric");
    assert!((1..=30).contains(&ttl), "unexpected TTL value: {ttl}");

    for scan_key in &scan_keys {
        stdout_json(dbtool(&[
            "--dsn",
            &dsn,
            "--allow-write",
            "kv",
            "set",
            scan_key,
            "scan-value",
        ]));
    }
    let scan = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        "2",
        "kv",
        "scan",
        &format!("{scan_prefix}:*"),
    ]));
    assert_eq!(scan["data"].as_array().unwrap().len(), 2);
    assert_eq!(scan["meta"]["truncated"], true);

    let blocked = stderr_json(dbtool(&["--dsn", &dsn, "kv", "raw", "FLUSHALL"]));
    assert_eq!(blocked["error"]["code"], "WRITE_NOT_ALLOWED");

    let mut delete_args = vec![
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "del",
        &key,
        &ttl_key,
        &raw_key,
        &counter_key,
    ];
    for scan_key in &scan_keys {
        delete_args.push(scan_key);
    }
    let deleted = stdout_json(dbtool(&delete_args));
    assert_eq!(deleted["data"]["deleted"], 7);
}

#[test]
fn redis_live_protocol_aliases_resolve_to_same_adapter() {
    if !integration_enabled() {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_REDIS_DSN") else {
        return;
    };

    for alias in ["valkey", "keydb", "dragonfly"] {
        let alias_dsn = dsn_with_scheme(&dsn, alias);
        let key = unique_name(&format!("dbtool_{alias}_key"));

        let ping = stdout_json(dbtool(&["--dsn", &alias_dsn, "ping"]));
        assert_eq!(ping["kind"], alias);
        assert_eq!(ping["ok"], true);

        let caps = stdout_json(dbtool(&["--dsn", &alias_dsn, "caps"]));
        assert_eq!(caps["kind"], alias);
        assert_eq!(caps["data"]["key_value"], true);

        stdout_json(dbtool(&[
            "--dsn",
            &alias_dsn,
            "--allow-write",
            "kv",
            "set",
            &key,
            alias,
        ]));
        let value = stdout_json(dbtool(&["--dsn", &alias_dsn, "kv", "get", &key]));
        assert_eq!(value["data"]["value"], alias);
        stdout_json(dbtool(&[
            "--dsn",
            &alias_dsn,
            "--allow-write",
            "kv",
            "del",
            &key,
        ]));
    }
}

#[test]
fn valkey_compat_live_kv_lifecycle_and_raw_safety() {
    if !compat_enabled("DBTOOL_RUN_VALKEY_COMPAT") {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_VALKEY_DSN") else {
        return;
    };

    redis_family_kv_probe(&dsn, "valkey", "valkey");
}

#[test]
fn keydb_compat_live_kv_lifecycle_and_raw_safety() {
    if !compat_enabled("DBTOOL_RUN_KEYDB_COMPAT") {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_KEYDB_DSN") else {
        return;
    };

    redis_family_kv_probe(&dsn, "keydb", "keydb");
}

#[test]
fn dragonfly_compat_live_kv_lifecycle_and_raw_safety() {
    if !compat_enabled("DBTOOL_RUN_DRAGONFLY_COMPAT") {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_DRAGONFLY_DSN") else {
        return;
    };

    redis_family_kv_probe(&dsn, "dragonfly", "dragonfly");
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
