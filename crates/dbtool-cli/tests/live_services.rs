use std::{
    env,
    process::{Command, Output},
    time::{SystemTime, UNIX_EPOCH},
};

use dbtool_core::model::Value as CoreValue;
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

fn cassandra_enabled() -> bool {
    env::var("DBTOOL_RUN_CASSANDRA_INTEGRATION").as_deref() == Ok("1")
}

fn redshift_enabled() -> bool {
    env::var("DBTOOL_RUN_REDSHIFT_INTEGRATION").as_deref() == Ok("1")
}

fn db2_enabled() -> bool {
    env::var("DBTOOL_RUN_DB2_INTEGRATION").as_deref() == Ok("1")
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

fn cql_exec(dsn: &str, cql: &str) -> Value {
    let first = dbtool(&["--dsn", dsn, "--allow-write", "cql", "exec", cql]);
    if first.status.success() {
        return stdout_json(first);
    }

    let first = stderr_json(first);
    assert_eq!(first["error"]["code"], "CONFIRM_REQUIRED");
    let token = first["error"]["confirm_token"]
        .as_str()
        .expect("CQL confirm token should be a string");

    stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "--confirm",
        token,
        "cql",
        "exec",
        cql,
    ]))
}

fn sql_lifecycle(dsn: &str, table: &str, create_sql: String, drop_sql: String) {
    eprintln!("dbtool test resource: sql table={table}");

    let ping = stdout_json(dbtool(&["--dsn", dsn, "ping"]));
    assert_eq!(ping["ok"], true);

    let caps = stdout_json(dbtool(&["--dsn", dsn, "caps"]));
    assert_eq!(caps["data"]["sql"], true);

    let schemas = stdout_json(dbtool(&["--dsn", dsn, "sql", "schemas"]));
    assert!(
        schemas["data"].as_array().is_some(),
        "sql schemas should return an array; output: {schemas}"
    );

    confirmed_sql_exec(dsn, &create_sql);

    let (table_schema, expected_table_name) = table
        .split_once('.')
        .map_or((None, table), |(schema, name)| (Some(schema), name));
    let tables = if let Some(schema) = table_schema {
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

    let blocked_insert = stderr_json(dbtool(&[
        "--dsn",
        dsn,
        "sql",
        "exec",
        &format!("insert into {table} (id, name) values (99, 'blocked')"),
    ]));
    assert_eq!(blocked_insert["error"]["code"], "WRITE_NOT_ALLOWED");

    stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "sql",
        "exec",
        &format!("insert into {table} (id, name) values (1, 'alice')"),
    ]));
    stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "sql",
        "exec",
        &format!("insert into {table} (id, name) values (2, 'bob')"),
    ]));

    let rows = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "sql",
        "query",
        &format!("select id, name from {table}"),
    ]));
    let mut all_rows = rows["data"]["rows"]
        .as_array()
        .expect("SQL rows should be an array")
        .clone();
    all_rows.sort_by_key(|row| row[0].as_i64().unwrap_or_default());
    assert_eq!(
        Value::Array(all_rows),
        serde_json::json!([[1, "alice"], [2, "bob"]]),
        "all fixture rows must be read back exactly"
    );

    stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "sql",
        "exec",
        &format!("update {table} set name = 'alice-updated' where id = 1"),
    ]));
    let updated = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "sql",
        "query",
        &format!("select id, name from {table} where id = 1"),
    ]));
    assert_eq!(updated["data"]["rows"][0][0], 1);
    assert_eq!(updated["data"]["rows"][0][1], "alice-updated");

    let unbounded_delete = stderr_json(dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "sql",
        "exec",
        &format!("delete from {table}"),
    ]));
    assert_eq!(unbounded_delete["error"]["code"], "CONFIRM_REQUIRED");

    stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "sql",
        "exec",
        &format!("delete from {table} where id = 2"),
    ]));
    let deleted = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "sql",
        "query",
        &format!("select id, name from {table} where id = 2"),
    ]));
    assert_eq!(deleted["data"]["rows"], serde_json::json!([]));

    let remaining = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "sql",
        "query",
        &format!("select id, name from {table}"),
    ]));
    assert_eq!(
        remaining["data"]["rows"],
        serde_json::json!([[1, "alice-updated"]]),
        "targeted delete must leave only the updated fixture row"
    );

    let schema = stdout_json(dbtool(&["--dsn", dsn, "sql", "schema", table]));
    assert_eq!(schema["ok"], true);
    assert_eq!(schema["data"]["name"], expected_table_name);
    let columns = schema["data"]["columns"]
        .as_array()
        .expect("schema should have a columns array");
    assert!(
        !columns.is_empty(),
        "schema columns should not be empty; output: {schema}"
    );
    let id_col = columns
        .iter()
        .find(|c| c["name"] == "id")
        .expect("id column should appear in schema");
    assert_eq!(
        id_col["nullable"], false,
        "id column should not be nullable"
    );

    if create_sql.to_lowercase().contains("primary key") {
        assert_eq!(
            id_col["primary_key"], true,
            "id should be detected as primary key; output: {schema}"
        );
        let has_pk_index = schema["data"]["indexes"]
            .as_array()
            .map(|idxs| idxs.iter().any(|i| i["primary"] == true))
            .unwrap_or(false);
        assert!(
            has_pk_index,
            "schema should include a primary-key index; output: {schema}"
        );
    }

    confirmed_sql_exec(dsn, &drop_sql);

    let tables_after_drop = if let Some(schema) = table_schema {
        stdout_json(dbtool(&["--dsn", dsn, "sql", "tables", "--schema", schema]))
    } else {
        stdout_json(dbtool(&["--dsn", dsn, "sql", "tables"]))
    };
    let table_still_present = tables_after_drop["data"]
        .as_array()
        .expect("tables output should be an array")
        .iter()
        .any(|entry| entry["name"].as_str() == Some(expected_table_name));
    assert!(
        !table_still_present,
        "dropped table must be absent; output: {tables_after_drop}"
    );
}

fn cql_lifecycle(dsn: &str, keyspace: &str, table: &str) {
    let qualified_table = format!("{keyspace}.{table}");
    eprintln!("dbtool test resource: cql table={qualified_table}");
    cql_exec(
        dsn,
        &format!("create table {qualified_table} (id int primary key, name text)"),
    );

    let tables = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "cql",
        "tables",
        "--keyspace",
        keyspace,
    ]));
    let table_found = tables["data"]
        .as_array()
        .expect("CQL tables output should be an array")
        .iter()
        .any(|entry| entry["name"].as_str() == Some(table));
    assert!(
        table_found,
        "expected cql tables to include {table}; output: {tables}"
    );

    let blocked_exec = stderr_json(dbtool(&[
        "--dsn",
        dsn,
        "cql",
        "exec",
        &format!("insert into {qualified_table} (id, name) values (99, 'blocked')"),
    ]));
    assert_eq!(blocked_exec["error"]["code"], "WRITE_NOT_ALLOWED");

    let blocked_query = stderr_json(dbtool(&[
        "--dsn",
        dsn,
        "cql",
        "query",
        &format!("insert into {qualified_table} (id, name) values (99, 'blocked')"),
    ]));
    assert_eq!(blocked_query["error"]["code"], "WRITE_NOT_ALLOWED");

    cql_exec(
        dsn,
        &format!("insert into {qualified_table} (id, name) values (1, 'alice')"),
    );
    cql_exec(
        dsn,
        &format!("insert into {qualified_table} (id, name) values (2, 'bob')"),
    );

    let all_rows = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "cql",
        "query",
        &format!("select id, name from {qualified_table}"),
    ]));
    let mut exact_rows = all_rows["data"]["rows"]
        .as_array()
        .expect("CQL rows should be an array")
        .clone();
    exact_rows.sort_by_key(|row| row[0].as_i64().unwrap_or_default());
    assert_eq!(
        Value::Array(exact_rows),
        serde_json::json!([[1, "alice"], [2, "bob"]]),
        "CQL must read the complete fixture"
    );

    let limited = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--limit",
        "1",
        "cql",
        "query",
        &format!("select id, name from {qualified_table}"),
    ]));
    assert_eq!(limited["data"]["rows"].as_array().unwrap().len(), 1);
    assert_eq!(limited["meta"]["truncated"], true);

    let rows = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "cql",
        "query",
        &format!("select id, name from {qualified_table} where id = 1"),
    ]));
    assert_eq!(rows["data"]["rows"][0][0], 1);
    assert_eq!(rows["data"]["rows"][0][1], "alice");

    cql_exec(
        dsn,
        &format!("update {qualified_table} set name = 'alice-updated' where id = 1"),
    );
    let updated = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "cql",
        "query",
        &format!("select id, name from {qualified_table} where id = 1"),
    ]));
    assert_eq!(updated["data"]["rows"][0][0], 1);
    assert_eq!(updated["data"]["rows"][0][1], "alice-updated");

    let unbounded_delete = stderr_json(dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "cql",
        "exec",
        &format!("delete from {qualified_table}"),
    ]));
    assert_eq!(unbounded_delete["error"]["code"], "CONFIRM_REQUIRED");

    cql_exec(dsn, &format!("delete from {qualified_table} where id = 2"));
    let deleted = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "cql",
        "query",
        &format!("select id, name from {qualified_table} where id = 2"),
    ]));
    assert_eq!(deleted["data"]["rows"], serde_json::json!([]));

    let schema = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "cql",
        "schema",
        table,
        "--keyspace",
        keyspace,
    ]));
    assert_eq!(schema["ok"], true);
    assert_eq!(schema["data"]["name"], table);
    let cql_columns = schema["data"]["columns"]
        .as_array()
        .expect("CQL schema should have columns");
    let id_col = cql_columns
        .iter()
        .find(|c| c["name"] == "id")
        .expect("id column should appear in CQL schema");
    assert_eq!(
        id_col["primary_key"], true,
        "CQL id column should be detected as primary key; output: {schema}"
    );
    assert!(
        schema["data"]["indexes"]
            .as_array()
            .is_some_and(|indexes| indexes.iter().any(|index| index["primary"] == true)),
        "CQL schema should expose a primary index; output: {schema}"
    );

    cql_exec(dsn, &format!("drop table {qualified_table}"));

    let tables_after_drop = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "cql",
        "tables",
        "--keyspace",
        keyspace,
    ]));
    assert!(
        tables_after_drop["data"]
            .as_array()
            .expect("CQL tables output should be an array")
            .iter()
            .all(|entry| entry["name"].as_str() != Some(table)),
        "dropped CQL table must be absent; output: {tables_after_drop}"
    );
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
    assert_eq!(
        serde_json::from_value::<CoreValue>(row[2].clone()).unwrap(),
        CoreValue::Bytes(vec![104, 105])
    );
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

fn postgres_lossless_non_null_probe(dsn: &str) {
    let typed = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "sql",
        "query",
        "select '550e8400-e29b-41d4-a716-446655440000'::uuid as uuid_value, \
                date '2026-07-16' as date_value, \
                time '12:34:56.123456' as time_value, \
                12345678901234567890.123456789::numeric as numeric_value, \
                array[1,2,3]::int[] as int_array, \
                array['alpha','beta']::text[] as text_array",
    ]));
    let row = &typed["data"]["rows"][0];
    assert_eq!(row[0], "550e8400-e29b-41d4-a716-446655440000");
    assert_eq!(row[1], "2026-07-16");
    assert_eq!(row[2], "12:34:56.123456");
    assert_eq!(row[3], "12345678901234567890.123456789");
    assert_eq!(
        serde_json::from_value::<CoreValue>(row[4].clone()).unwrap(),
        CoreValue::Array(vec![
            CoreValue::Int(1),
            CoreValue::Int(2),
            CoreValue::Int(3)
        ])
    );
    assert_eq!(
        serde_json::from_value::<CoreValue>(row[5].clone()).unwrap(),
        CoreValue::Array(vec![
            CoreValue::Text("alpha".into()),
            CoreValue::Text("beta".into())
        ])
    );
}

fn mysql_lossless_non_null_probe(dsn: &str) {
    let typed = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "sql",
        "query",
        "select cast(18446744073709551615 as unsigned) as unsigned_value, \
                cast(12345678901234567890.123456789 as decimal(40,9)) as decimal_value, \
                cast('2026-07-16' as date) as date_value, \
                cast('12:34:56.123456' as time(6)) as time_value",
    ]));
    let row = &typed["data"]["rows"][0];
    assert_eq!(row[0], "18446744073709551615");
    assert_eq!(row[1], "12345678901234567890.123456789");
    assert_eq!(row[2], "2026-07-16");
    assert_eq!(row[3], "12:34:56.123456");
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

fn cassandra_cql_probe(dsn: &str, expected_kind: &str) {
    let ping = stdout_json(dbtool(&["--dsn", dsn, "ping"]));
    assert_eq!(ping["kind"], expected_kind);
    assert_eq!(ping["ok"], true);

    let caps = stdout_json(dbtool(&["--dsn", dsn, "caps"]));
    assert_eq!(caps["kind"], expected_kind);
    assert_eq!(caps["data"]["sql"], true);
    assert_eq!(caps["data"]["cql"], true);

    let local = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "cql",
        "query",
        "select release_version from system.local",
    ]));
    assert!(local["data"]["rows"][0][0].as_str().is_some());

    let keyspaces = stdout_json(dbtool(&["--dsn", dsn, "cql", "keyspaces"]));
    assert!(keyspaces["data"]
        .as_array()
        .expect("keyspaces should be an array")
        .iter()
        .any(|entry| entry.as_str() == Some("system")));
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
    let resource_prefix = unique_name(&format!("dbtool_it_{prefix}"));
    let key = format!("{resource_prefix}:value");
    let ttl_key = format!("{resource_prefix}:ttl");
    let nx_key = format!("{resource_prefix}:nx");
    let raw_key = format!("{resource_prefix}:raw");
    eprintln!("dbtool test resources: {expected_kind} keys={key},{ttl_key},{nx_key},{raw_key}");

    let ping = stdout_json(dbtool(&["--dsn", dsn, "ping"]));
    assert_eq!(ping["kind"], expected_kind);
    assert_eq!(ping["ok"], true);

    let caps = stdout_json(dbtool(&["--dsn", dsn, "caps"]));
    assert_eq!(caps["kind"], expected_kind);
    assert_eq!(caps["data"]["key_value"], true);

    let blocked_set = stderr_json(dbtool(&["--dsn", dsn, "kv", "set", &key, "blocked"]));
    assert_eq!(blocked_set["error"]["code"], "WRITE_NOT_ALLOWED");

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

    let updated_value = format!("{expected_kind}-updated");
    stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "kv",
        "set",
        &key,
        &updated_value,
    ]));
    let overwritten = stdout_json(dbtool(&["--dsn", dsn, "kv", "get", &key]));
    assert_eq!(overwritten["data"]["value"], updated_value);

    let pong = stdout_json(dbtool(&["--dsn", dsn, "kv", "raw", "PING"]));
    assert_eq!(pong["data"], "PONG");

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

    stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "kv",
        "set",
        &nx_key,
        "created-once",
        "--ttl",
        "30",
        "--nx",
    ]));
    let nx_conflict = stderr_json(dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "kv",
        "set",
        &nx_key,
        "must-not-overwrite",
        "--nx",
    ]));
    assert_eq!(nx_conflict["error"]["code"], "QUERY_ERROR");
    assert!(nx_conflict["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("NX condition not met")));
    let nx_value = stdout_json(dbtool(&["--dsn", dsn, "kv", "get", &nx_key]));
    assert_eq!(nx_value["data"]["value"], "created-once");
    let nx_ttl = stdout_json(dbtool(&["--dsn", dsn, "kv", "raw", "TTL", &nx_key]));
    let nx_ttl = nx_ttl["data"].as_i64().expect("NX TTL should be numeric");
    assert!((1..=30).contains(&nx_ttl), "unexpected NX TTL: {nx_ttl}");

    let scan = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--limit",
        "2",
        "kv",
        "scan",
        &format!("{resource_prefix}:*"),
    ]));
    assert_eq!(scan["data"].as_array().unwrap().len(), 2);
    assert_eq!(scan["meta"]["truncated"], true);

    let fixture_values = [
        (&key, updated_value.as_str()),
        (&ttl_key, "short-lived"),
        (&nx_key, "created-once"),
        (&raw_key, "raw-value"),
    ];
    for (fixture_key, expected) in fixture_values {
        let value = stdout_json(dbtool(&["--dsn", dsn, "kv", "get", fixture_key]));
        assert_eq!(value["data"]["value"], expected);
    }

    let blocked_flush = stderr_json(dbtool(&["--dsn", dsn, "kv", "raw", "FLUSHALL"]));
    assert_eq!(blocked_flush["error"]["code"], "WRITE_NOT_ALLOWED");

    let deleted = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "kv",
        "del",
        &key,
        &ttl_key,
        &nx_key,
        &raw_key,
    ]));
    assert_eq!(deleted["data"]["deleted"], 4);

    for (fixture_key, _) in fixture_values {
        let missing = stdout_json(dbtool(&["--dsn", dsn, "kv", "get", fixture_key]));
        assert_eq!(missing["data"]["value"], Value::Null);
    }
    let empty_scan = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "kv",
        "scan",
        &format!("{resource_prefix}:*"),
    ]));
    assert_eq!(empty_scan["data"], serde_json::json!([]));
}

#[test]
fn postgres_live_sql_lifecycle() {
    if !integration_enabled() {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_POSTGRES_DSN") else {
        return;
    };
    let table = unique_name("dbtool_it_postgres_users");

    postgres_family_typed_probe(&dsn, "postgres");
    postgres_lossless_non_null_probe(&dsn);
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
    let table = unique_name("dbtool_it_mysql_users");

    mysql_family_typed_probe(&dsn, "mysql");
    mysql_lossless_non_null_probe(&dsn);
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
    let table = unique_name("dbtool_it_mariadb_users");

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
    let table = unique_name("dbtool_it_cockroach_users");

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
    let table = unique_name("dbtool_it_timescale_users");

    postgres_family_typed_probe(&dsn, "timescale");
    sql_lifecycle(
        &dsn,
        &table,
        format!("create table {table} (id integer primary key, name text not null)"),
        format!("drop table {table}"),
    );
}

#[test]
fn redshift_external_sql_lifecycle_and_typed_values() {
    if !redshift_enabled() {
        return;
    }
    let dsn = required_env("DBTOOL_IT_REDSHIFT_DSN");
    let table = unique_name("dbtool_it_redshift_users");

    postgres_family_typed_probe(&dsn, "redshift");
    sql_lifecycle(
        &dsn,
        &table,
        format!("create table {table} (id integer, name varchar(64) not null)"),
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
    let table = unique_name("dbtool_it_tidb_users");
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
    let table_1 = format!(
        "{}.{}",
        database,
        unique_name("dbtool_it_tidb_secure_node1")
    );
    sql_lifecycle(
        &secure_dsn_1,
        &table_1,
        format!("create table {table_1} (id integer primary key, name varchar(64) not null)"),
        format!("drop table {table_1}"),
    );

    mysql_family_typed_probe(&secure_dsn_2, "tidb");
    assert_mysql_tls_connection(&secure_dsn_2);
    let table_2 = format!(
        "{}.{}",
        database,
        unique_name("dbtool_it_tidb_secure_node2")
    );
    sql_lifecycle(
        &secure_dsn_2,
        &table_2,
        format!("create table {table_2} (id integer primary key, name varchar(64) not null)"),
        format!("drop table {table_2}"),
    );

    mysql_family_typed_probe(&x509_dsn, "tidb");
    assert_mysql_tls_connection(&x509_dsn);
    let table_x509 = format!("{}.{}", database, unique_name("dbtool_it_tidb_x509"));
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
    let table = unique_name("dbtool_it_sqlserver_users");
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
fn cassandra_live_cql_lifecycle_and_typed_values() {
    if !cassandra_enabled() {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_CASSANDRA_DSN") else {
        return;
    };
    let keyspace = env::var("DBTOOL_IT_CASSANDRA_KEYSPACE")
        .unwrap_or_else(|_| "dbtool_it_cassandra".to_owned());
    let table = unique_name("dbtool_it_cassandra_users");
    let qualified_table = format!("{keyspace}.{table}");

    cassandra_cql_probe(&dsn, "cassandra");
    cassandra_cql_probe(&dsn_with_scheme(&dsn, "scylla"), "scylla");

    setup_sql_exec(
        &dsn,
        &format!(
            "create keyspace if not exists {keyspace} \
             with replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
        ),
    );
    sql_lifecycle(
        &dsn,
        &qualified_table,
        format!("create table {qualified_table} (id int primary key, name text)"),
        format!("drop table {qualified_table}"),
    );
    cql_lifecycle(
        &dsn,
        &keyspace,
        &unique_name("dbtool_it_cassandra_cql_users"),
    );

    let typed_table_name = unique_name("dbtool_it_cassandra_typed");
    let typed_table = format!("{keyspace}.{typed_table_name}");
    eprintln!("dbtool test resource: cql typed table={typed_table}");
    cql_exec(
        &dsn,
        &format!(
            "create table {typed_table} \
             (id int primary key, name text, score double, active boolean, \
              tags list<text>, labels set<text>, attrs map<text, int>, \
              pair tuple<int, text>, payload blob, external_id uuid, \
              occurred_at timestamp, optional text)"
        ),
    );
    cql_exec(
        &dsn,
        &format!(
            "insert into {typed_table} \
             (id, name, score, active, tags, labels, attrs, pair, payload, external_id, occurred_at) \
             values (1, 'alice', 3.5, true, ['core', 'cql'], {{'green', 'blue'}}, \
                     {{'attempts': 2, 'level': 7}}, (9, 'nine'), 0x6869, \
                     550e8400-e29b-41d4-a716-446655440000, '2026-07-14T00:00:00Z')"
        ),
    );

    let typed = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "cql",
        "query",
        &format!(
            "select id, name, score, active, tags, labels, attrs, pair, payload, \
             external_id, occurred_at, optional from {typed_table} where id = 1"
        ),
    ]));
    let row = &typed["data"]["rows"][0];
    assert_eq!(row[0], 1);
    assert_eq!(row[1], "alice");
    assert_eq!(row[2].as_f64().expect("score should decode"), 3.5);
    assert_eq!(row[3], true);
    assert_eq!(
        serde_json::from_value::<CoreValue>(row[4].clone()).unwrap(),
        CoreValue::Array(vec![
            CoreValue::Text("core".into()),
            CoreValue::Text("cql".into())
        ])
    );
    let CoreValue::Array(mut labels) = serde_json::from_value::<CoreValue>(row[5].clone()).unwrap()
    else {
        panic!("set should decode as a typed array");
    };
    labels.sort_by_key(|value| value.as_str().unwrap_or_default().to_owned());
    assert_eq!(
        labels,
        vec![
            CoreValue::Text("blue".into()),
            CoreValue::Text("green".into())
        ]
    );
    assert_eq!(
        serde_json::from_value::<CoreValue>(row[6].clone()).unwrap(),
        CoreValue::Map(std::collections::BTreeMap::from([
            ("attempts".into(), CoreValue::Int(2)),
            ("level".into(), CoreValue::Int(7)),
        ]))
    );
    assert_eq!(
        serde_json::from_value::<CoreValue>(row[7].clone()).unwrap(),
        CoreValue::Array(vec![CoreValue::Int(9), CoreValue::Text("nine".into())])
    );
    assert_eq!(
        serde_json::from_value::<CoreValue>(row[8].clone()).unwrap(),
        CoreValue::Bytes(vec![104, 105])
    );
    assert_eq!(row[9], "550e8400-e29b-41d4-a716-446655440000");
    assert_eq!(
        serde_json::from_value::<CoreValue>(row[10].clone()).unwrap(),
        CoreValue::Timestamp(1_783_987_200_000_i64)
    );
    assert_eq!(row[11], Value::Null);

    let typed_schema = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "cql",
        "schema",
        &typed_table_name,
        "--keyspace",
        &keyspace,
    ]));
    let typed_columns = typed_schema["data"]["columns"]
        .as_array()
        .expect("typed CQL schema should have columns");
    for expected in [
        "id",
        "name",
        "score",
        "active",
        "tags",
        "labels",
        "attrs",
        "pair",
        "payload",
        "external_id",
        "occurred_at",
        "optional",
    ] {
        assert!(
            typed_columns
                .iter()
                .any(|column| column["name"] == expected),
            "typed schema is missing {expected}; output: {typed_schema}"
        );
    }

    cql_exec(&dsn, &format!("drop table {typed_table}"));
    let typed_tables_after_drop = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "cql",
        "tables",
        "--keyspace",
        &keyspace,
    ]));
    assert!(
        typed_tables_after_drop["data"]
            .as_array()
            .expect("CQL tables output should be an array")
            .iter()
            .all(|entry| entry["name"].as_str() != Some(&typed_table_name)),
        "typed CQL table must be absent after drop; output: {typed_tables_after_drop}"
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
    let key = unique_name("dbtool_it_redis_key");
    let ttl_key = unique_name("dbtool_it_redis_ttl");
    let nx_key = unique_name("dbtool_it_redis_nx");
    let raw_key = unique_name("dbtool_it_redis_raw");
    let counter_key = unique_name("dbtool_it_redis_counter");
    let scan_prefix = unique_name("dbtool_it_redis_scan");
    let scan_keys = [
        format!("{scan_prefix}:1"),
        format!("{scan_prefix}:2"),
        format!("{scan_prefix}:3"),
    ];
    eprintln!(
        "dbtool test resources: redis keys={}",
        [
            key.as_str(),
            ttl_key.as_str(),
            nx_key.as_str(),
            raw_key.as_str(),
            counter_key.as_str(),
            scan_keys[0].as_str(),
            scan_keys[1].as_str(),
            scan_keys[2].as_str(),
        ]
        .join(",")
    );

    let ping = stdout_json(dbtool(&["--dsn", &dsn, "ping"]));
    assert_eq!(ping["ok"], true);

    let caps = stdout_json(dbtool(&["--dsn", &dsn, "caps"]));
    assert_eq!(caps["data"]["key_value"], true);

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

    stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "set",
        &key,
        "alice-updated",
    ]));
    let overwritten = stdout_json(dbtool(&["--dsn", &dsn, "kv", "get", &key]));
    assert_eq!(overwritten["data"]["value"], "alice-updated");

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
    assert_eq!(
        serde_json::from_value::<CoreValue>(multi_get["data"].clone()).unwrap(),
        CoreValue::Array(vec![
            CoreValue::Text("alice-updated".into()),
            CoreValue::Text("raw-value".into()),
        ])
    );

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

    stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "set",
        &nx_key,
        "created-once",
        "--ttl",
        "30",
        "--nx",
    ]));
    let nx_conflict = stderr_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "set",
        &nx_key,
        "must-not-overwrite",
        "--nx",
    ]));
    assert_eq!(nx_conflict["error"]["code"], "QUERY_ERROR");
    assert!(nx_conflict["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("NX condition not met")));
    let nx_value = stdout_json(dbtool(&["--dsn", &dsn, "kv", "get", &nx_key]));
    assert_eq!(nx_value["data"]["value"], "created-once");
    let nx_ttl = stdout_json(dbtool(&["--dsn", &dsn, "kv", "raw", "TTL", &nx_key]));
    let nx_ttl = nx_ttl["data"].as_i64().expect("NX TTL should be numeric");
    assert!((1..=30).contains(&nx_ttl), "unexpected NX TTL: {nx_ttl}");

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

    let fixture_values = [
        (&key, "alice-updated"),
        (&ttl_key, "short-lived"),
        (&nx_key, "created-once"),
        (&raw_key, "raw-value"),
        (&counter_key, "1"),
        (&scan_keys[0], "scan-value"),
        (&scan_keys[1], "scan-value"),
        (&scan_keys[2], "scan-value"),
    ];
    for (fixture_key, expected) in fixture_values {
        let value = stdout_json(dbtool(&["--dsn", &dsn, "kv", "get", fixture_key]));
        assert_eq!(
            value["data"]["value"], expected,
            "fixture key {fixture_key} must retain its exact value"
        );
    }

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
        &nx_key,
        &raw_key,
        &counter_key,
    ];
    for scan_key in &scan_keys {
        delete_args.push(scan_key);
    }
    let deleted = stdout_json(dbtool(&delete_args));
    assert_eq!(deleted["data"]["deleted"], 8);

    for (fixture_key, _) in fixture_values {
        let missing = stdout_json(dbtool(&["--dsn", &dsn, "kv", "get", fixture_key]));
        assert_eq!(
            missing["data"]["value"],
            Value::Null,
            "deleted fixture key {fixture_key} must be absent"
        );
    }
    let empty_scan = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "kv",
        "scan",
        &format!("{scan_prefix}:*"),
    ]));
    assert_eq!(empty_scan["data"], serde_json::json!([]));
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
        let key = unique_name(&format!("dbtool_it_{alias}_key"));

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
    let collection = unique_name("dbtool_it_mongo_users");
    eprintln!("dbtool test resource: mongodb collection={collection}");

    let ping = stdout_json(dbtool(&["--dsn", &dsn, "ping"]));
    assert_eq!(ping["ok"], true);

    let caps = stdout_json(dbtool(&["--dsn", &dsn, "caps"]));
    assert_eq!(caps["data"]["document"], true);

    let blocked_insert = stderr_json(dbtool(&[
        "--dsn",
        &dsn,
        "doc",
        "insert",
        &collection,
        r#"{"_id":"alice","name":"alice","visits":1}"#,
    ]));
    assert_eq!(blocked_insert["error"]["code"], "WRITE_NOT_ALLOWED");

    let inserted = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "doc",
        "insert",
        &collection,
        r#"{"_id":"alice","name":"alice","visits":1}"#,
    ]));
    assert_eq!(inserted["data"]["inserted"], 1);
    let inserted_bob = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "doc",
        "insert",
        &collection,
        r#"{"_id":"bob","name":"bob","visits":3}"#,
    ]));
    assert_eq!(inserted_bob["data"]["inserted"], 1);

    let collections = stdout_json(dbtool(&["--dsn", &dsn, "doc", "collections"]));
    assert!(
        collections["data"]
            .as_array()
            .expect("doc collections should be an array")
            .iter()
            .any(|item| item.as_str() == Some(collection.as_str())),
        "expected doc collections to include {collection}; output: {collections}"
    );

    let all = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "doc",
        "find",
        &collection,
        "--filter",
        "{}",
    ]));
    let mut all_docs = all["data"]
        .as_array()
        .expect("document find should return an array")
        .clone();
    all_docs.sort_by(|left, right| left["name"].as_str().cmp(&right["name"].as_str()));
    assert_eq!(all_docs.len(), 2);
    assert_eq!(all_docs[0]["_id"], "alice");
    assert_eq!(all_docs[0]["visits"], 1);
    assert_eq!(all_docs[1]["_id"], "bob");
    assert_eq!(all_docs[1]["visits"], 3);

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
    assert_eq!(updated["data"]["modified"], 1);

    let updated_doc = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "doc",
        "find",
        &collection,
        "--filter",
        r#"{"_id":"alice"}"#,
    ]));
    assert_eq!(updated_doc["data"][0]["visits"], 2);

    let archive_collection = format!("{collection}_archive");
    let out_pipeline = format!(r#"[{{"$out":"{archive_collection}"}}]"#);
    let blocked_out = stderr_json(dbtool(&[
        "--dsn",
        &dsn,
        "doc",
        "aggregate",
        &collection,
        &out_pipeline,
    ]));
    assert_eq!(blocked_out["error"]["code"], "WRITE_NOT_ALLOWED");

    let out_confirmation = stderr_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "doc",
        "aggregate",
        &collection,
        &out_pipeline,
    ]));
    assert_eq!(out_confirmation["error"]["code"], "CONFIRM_REQUIRED");
    assert!(out_confirmation["error"]["impact"]["resource"]
        .as_str()
        .is_some_and(|resource| resource.ends_with(&format!(".{archive_collection}"))));
    let out_token = out_confirmation["error"]["confirm_token"]
        .as_str()
        .expect("$out should return a confirmation token");
    let out_result = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "--confirm",
        out_token,
        "doc",
        "aggregate",
        &collection,
        &out_pipeline,
    ]));
    assert_eq!(out_result["data"], serde_json::json!([]));

    let archived = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "doc",
        "find",
        &archive_collection,
        "--filter",
        "{}",
    ]));
    assert_eq!(archived["data"].as_array().map(Vec::len), Some(2));

    let archive_drop_confirmation = stderr_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "doc",
        "drop",
        &archive_collection,
    ]));
    let archive_drop_token = archive_drop_confirmation["error"]["confirm_token"]
        .as_str()
        .expect("archive drop should return a confirmation token");
    let archive_dropped = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "--confirm",
        archive_drop_token,
        "doc",
        "drop",
        &archive_collection,
    ]));
    assert_eq!(archive_dropped["data"]["dropped"], true);

    let aggregated = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "doc",
        "aggregate",
        &collection,
        r#"[{"$match":{"name":"alice"}},{"$project":{"_id":0,"name":1,"visits":1}}]"#,
    ]));
    assert_eq!(aggregated["data"][0]["visits"], 2);

    let unbounded_delete = stderr_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "doc",
        "delete",
        &collection,
        "--filter",
        "{}",
    ]));
    assert_eq!(unbounded_delete["error"]["code"], "QUERY_ERROR");

    let deleted_bob = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "doc",
        "delete",
        &collection,
        "--filter",
        r#"{"name":"bob"}"#,
    ]));
    assert_eq!(deleted_bob["data"]["deleted"], 1);

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

    let inserted_typed = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "doc",
        "insert",
        &collection,
        r#"{"name":"typed","decimal":{"$numberDecimal":"1234567890.0123456789"},"regex":{"$regularExpression":{"pattern":"^dbtool","options":"im"}},"bson_timestamp":{"$timestamp":{"t":1700000000,"i":42}}}"#,
    ]));
    assert_eq!(inserted_typed["data"]["inserted"], 1);
    let generated_id = inserted_typed["data"]["ids"][0]
        .as_str()
        .expect("MongoDB-generated ObjectId should be returned as reusable hex");
    assert_eq!(generated_id.len(), 24);
    assert!(generated_id
        .chars()
        .all(|character| character.is_ascii_hexdigit()));
    let generated_id_filter = format!(r#"{{"_id":{{"$oid":"{generated_id}"}}}}"#);

    let found_typed = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "doc",
        "find",
        &collection,
        "--filter",
        &generated_id_filter,
    ]));
    assert_eq!(found_typed["data"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        found_typed["data"][0]["_id"]["$dbtool"]["value"]["$oid"],
        generated_id
    );
    assert_eq!(
        found_typed["data"][0]["decimal"]["$dbtool"]["value"]["$numberDecimal"],
        "1234567890.0123456789"
    );
    assert_eq!(
        found_typed["data"][0]["regex"]["$dbtool"]["value"]["$regularExpression"]["pattern"],
        "^dbtool"
    );
    assert_eq!(
        found_typed["data"][0]["bson_timestamp"]["$dbtool"]["value"]["$timestamp"]["i"],
        42
    );

    let updated_typed = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "doc",
        "update",
        &collection,
        "--filter",
        &generated_id_filter,
        "--update",
        r#"{"roundtrip":true}"#,
    ]));
    assert_eq!(updated_typed["data"]["matched"], 1);
    assert_eq!(updated_typed["data"]["modified"], 1);

    let deleted_typed = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "doc",
        "delete",
        &collection,
        "--filter",
        &generated_id_filter,
    ]));
    assert_eq!(deleted_typed["data"]["deleted"], 1);

    let empty = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "doc",
        "find",
        &collection,
        "--filter",
        "{}",
    ]));
    assert_eq!(empty["data"], serde_json::json!([]));
}

// ── IBM Db2 live tests ────────────────────────────────────────────────────────

#[test]
fn db2_live_sql_lifecycle_and_schema_inspection() {
    if !db2_enabled() {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_DB2_DSN") else {
        return;
    };

    // ping
    let ping = stdout_json(dbtool(&["--dsn", &dsn, "ping"]));
    assert_eq!(ping["ok"], true, "DB2 ping failed: {ping}");

    // caps — must include sql
    let caps = stdout_json(dbtool(&["--dsn", &dsn, "caps"]));
    assert_eq!(caps["data"]["sql"], true, "DB2 must expose sql capability");

    // ibmdb2 alias resolves to the same adapter
    let alias_dsn = dsn_with_scheme(&dsn, "ibmdb2");
    let alias_ping = stdout_json(dbtool(&["--dsn", &alias_dsn, "ping"]));
    assert_eq!(alias_ping["ok"], true, "ibmdb2:// alias ping failed");

    // list schemas — expect at least one non-system schema
    let schemas = stdout_json(dbtool(&["--dsn", &dsn, "sql", "tables"]));
    assert_eq!(schemas["ok"], true, "sql tables failed: {schemas}");

    // write guard — exec must be blocked without --allow-write
    let table = format!(
        "DB2INST1.{}",
        unique_name("DBTOOL_DB2_TEST").to_ascii_uppercase()
    );
    let create_sql = format!("CREATE TABLE {table} (id INTEGER NOT NULL, name VARCHAR(64))");
    let blocked = stderr_json(dbtool(&["--dsn", &dsn, "sql", "exec", &create_sql]));
    assert_eq!(
        blocked["error"]["code"], "CONFIRM_REQUIRED",
        "destructive DDL must require a confirmation token"
    );

    // create table
    let created = confirmed_sql_exec(&dsn, &create_sql);
    assert_eq!(created["ok"], true, "CREATE TABLE failed: {created}");

    // insert row
    let insert_sql = format!("INSERT INTO {table} VALUES (1, 'alice')");
    stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "sql",
        "exec",
        &insert_sql,
    ]));

    // query row back
    let rows = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "sql",
        "query",
        &format!("SELECT id, name FROM {table}"),
    ]));
    assert_eq!(rows["ok"], true, "SELECT failed: {rows}");
    assert_eq!(rows["data"]["rows"][0][0], 1);
    assert_eq!(rows["data"]["rows"][0][1], "alice");

    // schema inspection
    let schema_out = stdout_json(dbtool(&["--dsn", &dsn, "sql", "schema", &table]));
    assert_eq!(schema_out["ok"], true, "sql schema failed: {schema_out}");
    let cols = &schema_out["data"]["columns"];
    assert!(
        cols.as_array().is_some_and(|c| !c.is_empty()),
        "schema must return columns"
    );

    // drop table
    let drop_sql = format!("DROP TABLE {table}");
    let dropped = confirmed_sql_exec(&dsn, &drop_sql);
    assert_eq!(dropped["ok"], true, "DROP TABLE failed: {dropped}");
}

#[test]
fn db2_live_db2_subcommand_schema_inspection() {
    if !db2_enabled() {
        return;
    }
    let Some(dsn) = dsn("DBTOOL_IT_DB2_DSN") else {
        return;
    };

    // ── db2 schemas ──────────────────────────────────────────────────────────
    let schemas = stdout_json(dbtool(&["--dsn", &dsn, "db2", "schemas"]));
    assert_eq!(schemas["ok"], true, "db2 schemas failed: {schemas}");

    // ── db2 tables ───────────────────────────────────────────────────────────
    let tables = stdout_json(dbtool(&[
        "--dsn", &dsn, "db2", "tables", "--schema", "DB2INST1",
    ]));
    assert_eq!(tables["ok"], true, "db2 tables failed: {tables}");

    // ── db2 tablespaces ──────────────────────────────────────────────────────
    let tsp = stdout_json(dbtool(&["--dsn", &dsn, "db2", "tablespaces"]));
    assert_eq!(tsp["ok"], true, "db2 tablespaces failed: {tsp}");
    assert!(
        tsp["data"].as_array().is_some_and(|a| !a.is_empty()),
        "every Db2 database has at least one tablespace"
    );

    // ── db2 sequences ────────────────────────────────────────────────────────
    let seqs = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "db2",
        "sequences",
        "--schema",
        "DB2INST1",
    ]));
    assert_eq!(seqs["ok"], true, "db2 sequences failed: {seqs}");

    // ── db2 routines ─────────────────────────────────────────────────────────
    let rts = stdout_json(dbtool(&[
        "--dsn", &dsn, "db2", "routines", "--schema", "DB2INST1",
    ]));
    assert_eq!(rts["ok"], true, "db2 routines failed: {rts}");

    // ── Set up a test table with a PK + FK for schema / fk / ddl tests ──────
    let base = unique_name("DBTOOL_DB2_INSP").to_ascii_uppercase();
    let parent = format!("DB2INST1.{base}_PARENT");
    let child = format!("DB2INST1.{base}_CHILD");

    confirmed_sql_exec(
        &dsn,
        &format!("CREATE TABLE {parent} (id INTEGER NOT NULL, PRIMARY KEY (id))"),
    );
    confirmed_sql_exec(
        &dsn,
        &format!("CREATE TABLE {child} (id INTEGER NOT NULL, parent_id INTEGER, PRIMARY KEY (id), FOREIGN KEY (parent_id) REFERENCES {parent}(id))"),
    );

    // db2 schema
    let schema_out = stdout_json(dbtool(&["--dsn", &dsn, "db2", "schema", &child]));
    assert_eq!(schema_out["ok"], true, "db2 schema failed: {schema_out}");
    let cols = &schema_out["data"]["columns"];
    assert!(
        cols.as_array().is_some_and(|c| !c.is_empty()),
        "schema must return columns"
    );

    // db2 foreign-keys
    let fks = stdout_json(dbtool(&["--dsn", &dsn, "db2", "foreign-keys", &child]));
    assert_eq!(fks["ok"], true, "db2 foreign-keys failed: {fks}");
    assert!(
        fks["data"].as_array().is_some_and(|a| !a.is_empty()),
        "child table must have at least one FK"
    );

    // db2 ddl
    let ddl_out = stdout_json(dbtool(&["--dsn", &dsn, "db2", "ddl", &parent]));
    assert_eq!(ddl_out["ok"], true, "db2 ddl failed: {ddl_out}");
    let ddl_str = ddl_out["data"].as_str().unwrap_or("");
    assert!(
        ddl_str.contains("CREATE TABLE"),
        "DDL must start with CREATE TABLE"
    );

    // cleanup
    confirmed_sql_exec(&dsn, &format!("DROP TABLE {child}"));
    confirmed_sql_exec(&dsn, &format!("DROP TABLE {parent}"));
}
