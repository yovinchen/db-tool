use std::{
    fs,
    path::Path,
    process::{Command, Output},
    sync::atomic::{AtomicU64, Ordering},
};

use dbtool_core::model::Value as CoreValue;
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

fn confirmed_sql_exec(dsn: &str, sql: &str) {
    let first = dbtool(&["--dsn", dsn, "--allow-write", "sql", "exec", sql]);
    if first.status.success() {
        return;
    }

    let first = stderr_json(&first);
    assert_eq!(first["error"]["code"], "CONFIRM_REQUIRED");
    let token = first["error"]["confirm_token"]
        .as_str()
        .expect("confirm token should be present");
    let second = stdout_json(&dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "--confirm",
        token,
        "sql",
        "exec",
        sql,
    ]));
    assert_eq!(second["ok"], true);
}

#[test]
fn cli_help_documents_core_command_families() {
    let root_help = stdout_text(&dbtool(&["--help"]));
    assert!(root_help.contains("JSON-first CLI"));
    assert!(root_help.contains("CQL"));
    assert!(root_help.contains("Examples:"));

    let sql_help = stdout_text(&dbtool(&["sql", "--help"]));
    assert!(sql_help.contains("shared safety path"));
    assert!(sql_help.contains("query"));
    assert!(sql_help.contains("schema"));
    let sql_query_help = stdout_text(&dbtool(&["sql", "query", "--help"]));
    assert!(sql_query_help.contains("--params"));
    assert!(sql_query_help.contains("$bytes"));
    assert!(sql_query_help.contains("$timestamp"));
    assert!(sql_query_help.contains("$json"));

    let cql_help = stdout_text(&dbtool(&["cql", "--help"]));
    assert!(cql_help.contains("Cassandra-specific keyspace"));
    assert!(cql_help.contains("require --allow-write"));

    let kv_help = stdout_text(&dbtool(&["kv", "--help"]));
    assert!(kv_help.contains("Read one key"));
    assert!(kv_help.contains("Write one text or canonical-base64 value"));
    assert!(kv_help.contains("Scan keys matching a pattern"));
    assert!(kv_help.contains("Delete one or more keys"));
    assert!(kv_help.contains("allowlisted, bounded raw Redis command"));
    let kv_set_help = stdout_text(&dbtool(&["kv", "set", "--help"]));
    assert!(kv_set_help.contains("--nx"));
    assert!(kv_set_help.contains("--value-base64"));
    assert!(kv_set_help.contains("Canonical RFC 4648 base64"));

    let doc_help = stdout_text(&dbtool(&["doc", "--help"]));
    assert!(doc_help.contains("List document collections"));
    assert!(doc_help.contains("Find documents with a JSON filter"));
    assert!(doc_help.contains("Insert one JSON document"));
    assert!(doc_help.contains("Update one document by default"));
    assert!(doc_help.contains("Delete one document by default"));
    assert!(doc_help.contains("Run a bounded JSON aggregation pipeline"));

    let search_help = stdout_text(&dbtool(&["search", "--help"]));
    assert!(search_help.contains("OpenSearch/Elasticsearch-compatible"));
    assert!(search_help.contains("requires --allow-write"));
    for action in ["index", "put", "get", "update", "delete", "delete-index"] {
        assert!(
            search_help.contains(action),
            "missing search action {action}"
        );
    }

    let ts_help = stdout_text(&dbtool(&["ts", "--help"]));
    assert!(ts_help.contains("Prometheus-compatible"));
    assert!(ts_help.contains("remote write"));

    let mq_help = stdout_text(&dbtool(&["mq", "--help"]));
    assert!(mq_help.contains("Kafka-compatible"));
    assert!(mq_help.contains("Consume messages (always bounded)"));

    let conn_help = stdout_text(&dbtool(&["conn", "--help"]));
    assert!(conn_help.contains("DBTOOL_CONN_*"));
    assert!(conn_help.contains("redact DSNs"));
}

#[test]
fn sql_import_help_documents_the_atomic_bound_parameter_contract() {
    let help = stdout_text(&dbtool(&["import", "sql", "--help"]));
    assert!(help.contains("sql.insert_rows_atomic"));
    assert!(help.contains("bound parameter"));
    assert!(help.contains("one transaction"));
}

#[test]
fn document_mutation_help_exposes_safe_one_and_many_modes() {
    let update = stdout_text(&dbtool(&["doc", "update", "--help"]));
    assert!(update.contains("--many"));
    assert!(update.contains("Update every matching document"));

    let delete = stdout_text(&dbtool(&["doc", "delete", "--help"]));
    assert!(delete.contains("--many"));
    assert!(delete.contains("Delete every matching document"));
}

#[test]
fn document_many_mutations_confirm_before_connecting_and_reject_token_reuse() {
    let dsn = "mongodb://dbtool:secret@127.0.0.1:1/app";
    let blocked = stderr_json(&dbtool(&[
        "--dsn",
        dsn,
        "doc",
        "update",
        "users",
        "--filter",
        r#"{"tenant":"one"}"#,
        "--update",
        r#"{"active":true}"#,
        "--many",
    ]));
    assert_eq!(blocked["error"]["code"], "WRITE_NOT_ALLOWED");

    let confirmation = stderr_json(&dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "doc",
        "update",
        "users",
        "--filter",
        r#"{"tenant":"one"}"#,
        "--update",
        r#"{"active":true}"#,
        "--many",
    ]));
    assert_eq!(confirmation["error"]["code"], "CONFIRM_REQUIRED");
    let token = confirmation["error"]["confirm_token"]
        .as_str()
        .expect("update many should expose a confirmation token");
    assert!(!token.contains("mongodb://"));
    assert!(!token.contains("secret"));

    let reused = stderr_json(&dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "--confirm",
        token,
        "doc",
        "delete",
        "users",
        "--filter",
        r#"{"tenant":"one"}"#,
        "--many",
    ]));
    assert_eq!(reused["error"]["code"], "INTERNAL_ERROR");
    assert!(reused["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("mismatch")));

    let empty = stderr_json(&dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "doc",
        "delete",
        "users",
        "--filter",
        "{}",
    ]));
    assert_eq!(empty["error"]["code"], "CONFIG_ERROR");
}

#[test]
fn sql_query_help_omits_the_dead_schema_option() {
    let help = stdout_text(&dbtool(&["sql", "query", "--help"]));
    assert!(help.contains("--params"));
    assert!(
        !help.contains("--schema"),
        "query must not advertise a schema option without cross-dialect semantics"
    );
}

#[test]
fn global_limit_rejects_zero_before_connecting() {
    let output = dbtool(&[
        "--dsn",
        "postgres://127.0.0.1:1/unreachable",
        "--limit",
        "0",
        "sql",
        "query",
        "select 1",
    ]);

    let error = stderr_json(&output);
    assert_eq!(error["error"]["code"], "CONFIG_ERROR");
    assert!(error["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("global --limit must be greater than zero")));
}

#[test]
fn cli_generate_artifacts_writes_completion_and_manpage_files() {
    let root = std::env::temp_dir().join(format!(
        "dbtool-cli-artifacts-{}-{}",
        std::process::id(),
        unique_suffix()
    ));
    let out_dir = root.join("artifacts");
    let out_dir_arg = out_dir.to_string_lossy().to_string();

    let output = stdout_text(&dbtool(&["generate-artifacts", "--out-dir", &out_dir_arg]));
    assert!(output.contains("completions/dbtool.bash"));
    assert!(output.contains("man/dbtool.1"));

    let bash = fs::read_to_string(out_dir.join("completions/dbtool.bash"))
        .expect("bash completion should be written");
    assert!(bash.contains("complete -F _dbtool dbtool"));
    assert!(bash.contains("sql"));
    assert!(bash.contains("cql"));
    assert!(!bash.contains("generate-artifacts"));

    let zsh = fs::read_to_string(out_dir.join("completions/dbtool.zsh"))
        .expect("zsh completion should be written");
    assert!(zsh.contains("#compdef dbtool"));

    let fish = fs::read_to_string(out_dir.join("completions/dbtool.fish"))
        .expect("fish completion should be written");
    assert!(fish.contains("complete -c dbtool"));
    assert!(fish.contains("mq"));

    let man = fs::read_to_string(out_dir.join("man/dbtool.1")).expect("manpage should be written");
    assert!(man.contains(".TH DBTOOL 1"));
    assert!(man.contains("dbtool sql query"));
    assert!(man.contains("dbtool cql keyspaces"));
    assert!(man.contains("dbtool export sql"));
    assert!(man.contains("dbtool import sql"));

    cleanup_dir(&root);
}

#[test]
fn export_import_sql_round_trips_sqlite_rows() {
    let root = std::env::temp_dir().join(format!(
        "dbtool-cli-transfer-{}-{}",
        std::process::id(),
        unique_suffix()
    ));
    fs::create_dir_all(&root).expect("transfer temp dir should be created");
    let db_file = root.join("roundtrip.db");
    fs::File::create(&db_file).expect("sqlite db file should be created");
    let export_file = root.join("people.json");
    let dsn = format!("sqlite://{}", db_file.display());
    let export_arg = export_file.to_string_lossy().to_string();

    confirmed_sql_exec(
        &dsn,
        "create table people (id integer primary key, name text not null, active boolean not null)",
    );
    stdout_json(&dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "sql",
        "exec",
        "insert into people (id, name, active) values (1, 'alice', true), (2, 'bob', false)",
    ]));
    confirmed_sql_exec(
        &dsn,
        "create table people_copy (id integer primary key, name text not null, active boolean not null)",
    );

    let export = stdout_json(&dbtool(&[
        "--dsn",
        &dsn,
        "export",
        "sql",
        "--query",
        "select id, name, active from people order by id",
        "--out",
        &export_arg,
    ]));
    assert_eq!(export["data"]["kind"], "sql-rows");
    assert_eq!(export["data"]["rows"], 2);

    let artifact: Value =
        serde_json::from_slice(&fs::read(&export_file).expect("export artifact should exist"))
            .expect("export artifact should be JSON");
    assert_eq!(artifact["kind"], "sql-rows");
    assert_eq!(
        artifact["columns"],
        serde_json::json!(["id", "name", "active"])
    );

    let blocked = stderr_json(&dbtool(&[
        "--dsn",
        &dsn,
        "import",
        "sql",
        "--table",
        "people_copy",
        "--input",
        &export_arg,
    ]));
    assert_eq!(blocked["error"]["code"], "WRITE_NOT_ALLOWED");

    let imported = stdout_json(&dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "import",
        "sql",
        "--table",
        "people_copy",
        "--input",
        &export_arg,
    ]));
    assert_eq!(imported["data"]["inserted"], 2);
    assert_eq!(imported["data"]["atomic"], true);

    let rows = stdout_json(&dbtool(&[
        "--dsn",
        &dsn,
        "sql",
        "query",
        "select name, active from people_copy order by id",
    ]));
    assert_eq!(rows["data"]["rows"][0][0], "alice");
    assert_eq!(rows["data"]["rows"][1][0], "bob");

    cleanup_dir(&root);
}

#[test]
fn sql_import_rolls_back_the_complete_sqlite_artifact_on_late_constraint_error() {
    let root = std::env::temp_dir().join(format!(
        "dbtool-cli-atomic-import-{}-{}",
        std::process::id(),
        unique_suffix()
    ));
    fs::create_dir_all(&root).expect("temporary directory should be created");
    let db_file = root.join("atomic.db");
    fs::File::create(&db_file).expect("sqlite db file should be created");
    let artifact = root.join("rows.json");
    let dsn = format!("sqlite://{}", db_file.display());
    let injection = "O'Reilly'); drop table atomic_rows; --";

    confirmed_sql_exec(
        &dsn,
        "create table atomic_rows (id integer primary key, note text not null)",
    );
    fs::write(
        &artifact,
        serde_json::to_vec_pretty(&serde_json::json!({
            "kind": "sql-rows",
            "version": 3,
            "columns": ["id", "note"],
            "rows": [[1, injection], [1, "duplicate"]],
            "truncated": false
        }))
        .unwrap(),
    )
    .unwrap();

    let rejected = stderr_json(&dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "import",
        "sql",
        "--table",
        "atomic_rows",
        "--input",
        &artifact.to_string_lossy(),
    ]));
    assert_eq!(rejected["error"]["code"], "QUERY_ERROR");

    let count = stdout_json(&dbtool(&[
        "--dsn",
        &dsn,
        "sql",
        "query",
        "select count(*) as total from atomic_rows",
    ]));
    assert_eq!(count["data"]["rows"][0][0], 0);

    cleanup_dir(&root);
}

#[test]
fn sql_schemas_returns_list_for_sqlite() {
    let value = stdout_json(&dbtool(&["--dsn", "sqlite::memory:", "sql", "schemas"]));

    assert_eq!(value["ok"], true);
    let schemas = value["data"]
        .as_array()
        .expect("sql schemas should return an array");
    assert!(
        schemas.iter().any(|s| s == "main"),
        "sqlite should report the 'main' schema; got: {schemas:?}"
    );
}

#[test]
fn sql_metadata_lists_respect_limit_and_expose_effective_schema() {
    let root = std::env::temp_dir().join(format!(
        "dbtool-cli-metadata-{}-{}",
        std::process::id(),
        unique_suffix()
    ));
    fs::create_dir_all(&root).expect("temporary directory should be created");
    let db_file = root.join("metadata.db");
    fs::File::create(&db_file).expect("sqlite database file should be created");
    let dsn = format!("sqlite://{}", db_file.display());

    confirmed_sql_exec(&dsn, "create table alpha (id integer primary key)");
    confirmed_sql_exec(&dsn, "create table beta (id integer primary key)");
    confirmed_sql_exec(&dsn, "create view alpha_ids as select id from alpha");

    let limited = stdout_json(&dbtool(&[
        "--dsn", &dsn, "--limit", "2", "sql", "tables", "--schema", "main",
    ]));
    assert_eq!(limited["data"].as_array().map(Vec::len), Some(2));
    assert_eq!(limited["meta"]["truncated"], true);
    assert!(limited["data"]
        .as_array()
        .unwrap()
        .iter()
        .all(|table| table["schema"] == "main"));

    let complete = stdout_json(&dbtool(&[
        "--dsn", &dsn, "--limit", "3", "sql", "tables", "--schema", "main",
    ]));
    assert_eq!(complete["data"].as_array().map(Vec::len), Some(3));
    assert_eq!(complete["meta"]["truncated"], false);
    assert!(complete["data"]
        .as_array()
        .unwrap()
        .iter()
        .any(|table| table["name"] == "alpha_ids" && table["kind"] == "view"));

    cleanup_dir(&root);
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
fn sql_cli_binds_json_parameters_to_sqlite_without_interpolation() {
    let root = std::env::temp_dir().join(format!(
        "dbtool-cli-sql-params-{}-{}",
        std::process::id(),
        unique_suffix()
    ));
    fs::create_dir_all(&root).expect("SQL parameter temp dir should be created");
    let db_file = root.join("params.db");
    fs::File::create(&db_file).expect("SQLite database file should be created");
    let dsn = format!("sqlite://{}", db_file.display());

    confirmed_sql_exec(
        &dsn,
        "create table bound_values (
            id integer primary key,
            note text not null,
            score real not null,
            enabled boolean not null,
            payload blob not null,
            optional text
        )",
    );

    let injection = "O'Reilly'); drop table bound_values; --";
    let params = serde_json::json!([
        7,
        injection,
        12.75,
        true,
        {"$bytes": [0, 127, 255]},
        null
    ])
    .to_string();
    let inserted = stdout_json(&dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "sql",
        "exec",
        "insert into bound_values (id, note, score, enabled, payload, optional) values (?, ?, ?, ?, ?, ?)",
        "--params",
        &params,
    ]));
    assert_eq!(inserted["data"]["rows_affected"], 1);

    let query_params = serde_json::json!([7, injection]).to_string();
    let queried = stdout_json(&dbtool(&[
        "--dsn",
        &dsn,
        "sql",
        "query",
        "select id, note, score, enabled, payload, optional from bound_values where id = ? and note = ?",
        "--params",
        &query_params,
    ]));

    assert_eq!(queried["data"]["rows"][0][0], 7);
    assert_eq!(queried["data"]["rows"][0][1], injection);
    assert_eq!(queried["data"]["rows"][0][2], 12.75);
    assert_eq!(queried["data"]["rows"][0][3], true);
    assert_eq!(
        serde_json::from_value::<CoreValue>(queried["data"]["rows"][0][4].clone()).unwrap(),
        CoreValue::Bytes(vec![0, 127, 255])
    );
    assert_eq!(queried["data"]["rows"][0][5], Value::Null);

    let tagged = stdout_json(&dbtool(&[
        "--dsn",
        &dsn,
        "sql",
        "query",
        "select CAST(strftime('%s', ?) AS INTEGER) * 1000 as timestamp_ms, json_extract(?, '$.source') as source",
        "--params",
        r#"[{"$timestamp":1700000000123},{"$json":{"source":"cli"}}]"#,
    ]));
    assert_eq!(tagged["data"]["rows"][0][0], 1_700_000_000_000_i64);
    assert_eq!(tagged["data"]["rows"][0][1], "cli");

    let table_survived = stdout_json(&dbtool(&[
        "--dsn",
        &dsn,
        "sql",
        "query",
        "select count(*) as total from bound_values",
    ]));
    assert_eq!(table_survived["data"]["rows"][0][0], 1);

    cleanup_dir(&root);
}

#[test]
fn sql_params_validation_happens_before_connecting() {
    let error = stderr_json(&dbtool(&[
        "--dsn",
        "postgres://127.0.0.1:1/unreachable",
        "sql",
        "query",
        "select $1",
        "--params",
        r#"{"not":"an-array"}"#,
    ]));

    assert_eq!(error["error"]["code"], "CONFIG_ERROR");
    assert!(error["error"]["message"]
        .as_str()
        .unwrap_or_default()
        .contains("must be a JSON array"));
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
    let records = stdout
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 3);
    for record in &records {
        assert_eq!(record["ok"], true);
        assert_eq!(record["kind"], "sqlite");
        assert!(record["meta"]["elapsed_ms"].is_number());
        assert_eq!(record["meta"]["truncated"], false);
    }
    assert_eq!(records[0]["record"], "schema");
    assert_eq!(records[0]["data"]["columns"][0]["name"], "id");
    assert_eq!(records[1]["record"], "row");
    assert_eq!(records[1]["data"], serde_json::json!([1, "alice"]));
    assert_eq!(records[2]["data"], serde_json::json!([2, "bob"]));
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
fn all_search_document_mutations_require_write_flag_before_connecting() {
    let cases = [
        vec!["search", "put", "users", "user-1", r#"{"name":"alice"}"#],
        vec!["search", "update", "users", "user-1", r#"{"name":"bob"}"#],
        vec!["search", "delete", "users", "user-1"],
    ];

    for case in cases {
        let mut args = vec!["--dsn", "opensearch://127.0.0.1:1"];
        args.extend(case);
        let err = stderr_json(&dbtool(&args));
        assert_eq!(err["error"]["code"], "WRITE_NOT_ALLOWED");
    }
}

#[test]
fn search_delete_index_requires_target_bound_confirmation_before_connecting() {
    let dsn = "opensearch://127.0.0.1:1";
    let blocked = stderr_json(&dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "search",
        "delete-index",
        "users",
    ]));

    assert_eq!(blocked["error"]["code"], "CONFIRM_REQUIRED");
    assert_eq!(blocked["error"]["impact"]["op"], "DELETE_SEARCH_INDEX");
    assert_eq!(blocked["error"]["impact"]["resource"], "users");
    assert_eq!(
        blocked["error"]["impact"]["target"],
        "dsn:opensearch://127.0.0.1:1"
    );
    assert!(blocked["error"]["confirm_token"].as_str().is_some());
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
fn cql_exec_requires_write_flag_before_connecting() {
    let err = stderr_json(&dbtool(&[
        "--dsn",
        "cassandra://127.0.0.1:1/app",
        "cql",
        "exec",
        "create table app.users (id int primary key)",
    ]));

    assert_eq!(err["error"]["code"], "WRITE_NOT_ALLOWED");
}

#[test]
fn cql_query_rejects_writes_before_connecting() {
    let err = stderr_json(&dbtool(&[
        "--dsn",
        "cassandra://127.0.0.1:1/app",
        "cql",
        "query",
        "insert into app.users (id) values (1)",
    ]));

    assert_eq!(err["error"]["code"], "WRITE_NOT_ALLOWED");
}

#[test]
fn document_aggregate_write_stage_requires_write_flag_before_connecting() {
    let err = stderr_json(&dbtool(&[
        "--dsn",
        "mongodb://127.0.0.1:1/app",
        "doc",
        "aggregate",
        "users",
        r#"[{"$match":{}},{"$out":"users_archive"}]"#,
    ]));

    assert_eq!(err["error"]["code"], "WRITE_NOT_ALLOWED");
}

#[test]
fn readonly_named_connection_blocks_sql_kv_and_cql_writes() {
    let config = r#"
[connections.locked]
dsn = "sqlite::memory:"
readonly = true
"#;

    let sql = stderr_json(&dbtool_with_config(
        &[
            "--conn",
            "locked",
            "--allow-write",
            "sql",
            "exec",
            "insert into users (id) values (1)",
        ],
        config,
    ));
    assert_eq!(sql["error"]["code"], "READ_ONLY");

    let kv = stderr_json(&dbtool_with_config(
        &[
            "--conn",
            "locked",
            "--allow-write",
            "kv",
            "set",
            "user:1",
            "alice",
        ],
        config,
    ));
    assert_eq!(kv["error"]["code"], "READ_ONLY");

    let cql = stderr_json(&dbtool_with_config(
        &[
            "--conn",
            "locked",
            "--allow-write",
            "cql",
            "exec",
            "insert into users (id) values (1)",
        ],
        config,
    ));
    assert_eq!(cql["error"]["code"], "READ_ONLY");
}

#[test]
fn read_only_alias_remains_fail_closed_before_connecting() {
    let err = stderr_json(&dbtool_with_config(
        &[
            "--conn",
            "locked",
            "--allow-write",
            "sql",
            "exec",
            "insert into users (id) values (1)",
        ],
        r#"
[connections.locked]
dsn = "postgres://127.0.0.1:1/app"
read_only = true
"#,
    ));

    assert_eq!(err["error"]["code"], "READ_ONLY");
}

#[test]
fn misspelled_connection_fields_fail_closed_without_echoing_config_source() {
    let cases = [
        r#"
[connections.typo]
dsn = "postgres://user:config-source-secret@127.0.0.1:1/app"
readonli = true
"#,
        r#"
[connections.typo]
dsn = "postgres://user:config-source-secret@127.0.0.1:1/app"

[connections.typo.limits]
request_timout = "1s"
"#,
        r#"
[defaults.limits]
max_concurency = 1

[connections.typo]
dsn = "postgres://user:config-source-secret@127.0.0.1:1/app"
"#,
    ];

    for config in cases {
        let err = stderr_json(&dbtool_with_config(&["--conn", "typo", "ping"], config));
        assert_eq!(err["error"]["code"], "CONFIG_ERROR");
        assert_eq!(
            err["error"]["message"],
            "config error: connection config is invalid TOML or contains unsupported fields"
        );
        assert!(!err.to_string().contains("config-source-secret"));
    }
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
