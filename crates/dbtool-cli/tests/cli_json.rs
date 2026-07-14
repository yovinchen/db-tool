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

    let cql_help = stdout_text(&dbtool(&["cql", "--help"]));
    assert!(cql_help.contains("Cassandra-specific keyspace"));
    assert!(cql_help.contains("require --allow-write"));

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

    let search_help = stdout_text(&dbtool(&["search", "--help"]));
    assert!(search_help.contains("OpenSearch/Elasticsearch-compatible"));
    assert!(search_help.contains("requires --allow-write"));

    let ts_help = stdout_text(&dbtool(&["ts", "--help"]));
    assert!(ts_help.contains("Prometheus-compatible"));
    assert!(ts_help.contains("remote write"));

    let mq_help = stdout_text(&dbtool(&["mq", "--help"]));
    assert!(mq_help.contains("Kafka-compatible"));
    assert!(mq_help.contains("consume is always bounded"));

    let conn_help = stdout_text(&dbtool(&["conn", "--help"]));
    assert!(conn_help.contains("DBTOOL_CONN_*"));
    assert!(conn_help.contains("redact DSNs"));
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
