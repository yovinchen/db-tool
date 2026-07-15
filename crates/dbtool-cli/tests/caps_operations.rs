use serde_json::Value;
use std::process::{Command, Output};

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

#[test]
fn caps_preserves_legacy_booleans_and_adds_sorted_sql_operations() {
    let value = stdout_json(dbtool(&["--dsn", "sqlite::memory:", "caps"]));

    assert_eq!(value["data"]["sql"], true);
    assert_eq!(value["data"]["key_value"], false);
    assert_eq!(value["data"]["admin"], false);
    assert_eq!(
        value["data"]["operations"],
        serde_json::json!([
            "sql.describe_table",
            "sql.execute",
            "sql.list_schemas",
            "sql.list_tables",
            "sql.query",
            "sql.query_bounded"
        ])
    );
}
