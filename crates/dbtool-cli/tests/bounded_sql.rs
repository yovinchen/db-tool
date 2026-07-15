use serde_json::Value;
use std::{
    env, fs,
    path::PathBuf,
    process::{Command, Output},
    time::{SystemTime, UNIX_EPOCH},
};

fn integration_enabled() -> bool {
    env::var("DBTOOL_RUN_INTEGRATION").as_deref() == Ok("1")
}

fn cassandra_integration_enabled() -> bool {
    env::var("DBTOOL_RUN_CASSANDRA_INTEGRATION").as_deref() == Ok("1")
}

fn integration_dsn(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
}

fn dbtool(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_dbtool"))
        .args(args)
        .output()
        .expect("dbtool should run")
}

fn stdout_json(output: Output) -> Value {
    assert!(
        output.status.success(),
        "dbtool failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("stdout should be JSON")
}

fn stderr_json(output: Output) -> Value {
    assert!(
        !output.status.success(),
        "dbtool unexpectedly succeeded: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    serde_json::from_slice(&output.stderr).expect("stderr should be JSON")
}

fn temp_path(name: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after the Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "dbtool-bounded-{name}-{}-{suffix}",
        std::process::id()
    ))
}

#[test]
fn sqlite_cli_streams_large_results_and_distinguishes_exact_limit() {
    let limited = stdout_json(dbtool(&[
        "--dsn",
        "sqlite::memory:",
        "--limit",
        "3",
        "sql",
        "query",
        "with recursive numbers(value) as (select 1 union all select value + 1 from numbers where value < 10000) select value from numbers",
    ]));
    assert_eq!(limited["data"]["rows"], serde_json::json!([[1], [2], [3]]));
    assert_eq!(limited["data"]["truncated"], true);
    assert_eq!(limited["meta"]["truncated"], true);

    let exact = stdout_json(dbtool(&[
        "--dsn",
        "sqlite::memory:",
        "--limit",
        "3",
        "sql",
        "query",
        "select 1 as value union all select 2 union all select 3",
    ]));
    assert_eq!(exact["data"]["rows"], serde_json::json!([[1], [2], [3]]));
    assert_eq!(exact["data"]["truncated"], false);
    assert_eq!(exact["meta"]["truncated"], false);
}

fn assert_live_sql_bounding(dsn: &str, large_query: &str) {
    let limited = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--limit",
        "3",
        "sql",
        "query",
        large_query,
    ]));
    assert_eq!(
        limited["data"]["rows"]
            .as_array()
            .expect("bounded rows should be an array")
            .len(),
        3
    );
    assert_eq!(limited["data"]["truncated"], true);
    assert_eq!(limited["meta"]["truncated"], true);

    let exact = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--limit",
        "3",
        "sql",
        "query",
        "select 1 as value union all select 2 union all select 3",
    ]));
    assert_eq!(
        exact["data"]["rows"]
            .as_array()
            .expect("exact rows should be an array")
            .len(),
        3
    );
    assert_eq!(exact["data"]["truncated"], false);
    assert_eq!(exact["meta"]["truncated"], false);
}

#[test]
fn postgres_live_streams_one_probe_row_for_large_results() {
    if !integration_enabled() {
        return;
    }
    let Some(dsn) = integration_dsn("DBTOOL_IT_POSTGRES_DSN") else {
        return;
    };

    assert_live_sql_bounding(&dsn, "select value from generate_series(1, 10000) as value");
}

#[test]
fn mysql_live_streams_one_probe_row_for_large_results() {
    if !integration_enabled() {
        return;
    }
    let Some(dsn) = integration_dsn("DBTOOL_IT_MYSQL_DSN") else {
        return;
    };

    assert_live_sql_bounding(
        &dsn,
        "with recursive numbers(value) as (select 1 union all select value + 1 from numbers where value < 1000) select value from numbers",
    );
}

#[test]
fn cassandra_live_streams_one_probe_row_for_paged_results() {
    if !cassandra_integration_enabled() {
        return;
    }
    let Some(dsn) = integration_dsn("DBTOOL_IT_CASSANDRA_DSN") else {
        return;
    };

    let limited = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        "3",
        "cql",
        "query",
        "select keyspace_name, table_name, column_name from system_schema.columns",
    ]));
    assert_eq!(
        limited["data"]["rows"]
            .as_array()
            .expect("bounded CQL rows should be an array")
            .len(),
        3
    );
    assert_eq!(limited["data"]["truncated"], true);
    assert_eq!(limited["meta"]["truncated"], true);

    let exact = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        "3",
        "cql",
        "query",
        "select keyspace_name from system_schema.keyspaces limit 3",
    ]));
    assert_eq!(
        exact["data"]["rows"]
            .as_array()
            .expect("exact CQL rows should be an array")
            .len(),
        3
    );
    assert_eq!(exact["data"]["truncated"], false);
    assert_eq!(exact["meta"]["truncated"], false);
}

#[test]
fn sql_query_rejects_probe_overflow_before_connecting() {
    let max = usize::MAX.to_string();
    let error = stderr_json(dbtool(&[
        "--dsn",
        "postgres://127.0.0.1:1/unreachable",
        "--limit",
        &max,
        "sql",
        "query",
        "select 1",
    ]));

    assert_eq!(error["error"]["code"], "CONFIG_ERROR");
    assert!(error["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("truncation probe row")));
}

#[test]
fn sql_export_rejects_writes_and_ddl_before_connecting() {
    let out = temp_path("blocked-export.json");
    let out_arg = out.to_string_lossy().to_string();

    for query in ["delete from users where id = 1", "drop table users"] {
        let error = stderr_json(dbtool(&[
            "--dsn",
            "postgres://127.0.0.1:1/unreachable",
            "export",
            "sql",
            "--query",
            query,
            "--out",
            &out_arg,
        ]));
        assert_eq!(error["error"]["code"], "WRITE_NOT_ALLOWED");
    }
    assert!(!out.exists());
}

#[test]
fn sql_query_rejects_writes_even_when_allow_write_is_present() {
    for query in ["delete from users where id = 1", "drop table users"] {
        let error = stderr_json(dbtool(&[
            "--dsn",
            "postgres://127.0.0.1:1/unreachable",
            "--allow-write",
            "sql",
            "query",
            query,
        ]));
        assert_eq!(error["error"]["code"], "WRITE_NOT_ALLOWED");
    }
}

#[test]
fn sql_export_records_truncation_and_import_refuses_partial_artifact() {
    let root = temp_path("artifact");
    fs::create_dir_all(&root).expect("temporary directory should be created");
    let artifact = root.join("rows.json");
    let artifact_arg = artifact.to_string_lossy().to_string();

    let exported = stdout_json(dbtool(&[
        "--dsn",
        "sqlite::memory:",
        "--limit",
        "2",
        "export",
        "sql",
        "--query",
        "select 1 as id union all select 2 union all select 3",
        "--out",
        &artifact_arg,
    ]));
    assert_eq!(exported["data"]["rows"], 2);
    assert_eq!(exported["meta"]["truncated"], true);

    let artifact_json: Value = serde_json::from_slice(
        &fs::read(&artifact).expect("bounded SQL export artifact should exist"),
    )
    .expect("bounded SQL export artifact should be JSON");
    assert_eq!(artifact_json["truncated"], true);
    assert_eq!(artifact_json["rows"], serde_json::json!([[1], [2]]));

    let rejected = stderr_json(dbtool(&[
        "--dsn",
        "sqlite::memory:",
        "--allow-write",
        "import",
        "sql",
        "--table",
        "partial_rows",
        "--input",
        &artifact_arg,
    ]));
    assert_eq!(rejected["error"]["code"], "SERIALIZATION_ERROR");
    assert!(rejected["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("truncated sql-rows artifact")));

    fs::remove_dir_all(root).expect("temporary directory should be removed");
}

#[test]
fn sql_import_rejects_unmarked_legacy_artifact_by_default() {
    let root = temp_path("legacy-artifact");
    fs::create_dir_all(&root).expect("temporary directory should be created");
    let artifact = root.join("rows-v1.json");
    fs::write(
        &artifact,
        br#"{
          "kind": "sql-rows",
          "version": 1,
          "columns": ["id"],
          "rows": [[1]]
        }"#,
    )
    .expect("legacy artifact should be written");
    let artifact_arg = artifact.to_string_lossy().to_string();

    let rejected = stderr_json(dbtool(&[
        "--dsn",
        "sqlite::memory:",
        "--allow-write",
        "import",
        "sql",
        "--table",
        "legacy_rows",
        "--input",
        &artifact_arg,
    ]));
    assert_eq!(rejected["error"]["code"], "SERIALIZATION_ERROR");
    assert!(rejected["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("--accept-legacy-unmarked")));

    fs::remove_dir_all(root).expect("temporary directory should be removed");
}
