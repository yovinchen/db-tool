use serde_json::Value;
use std::{
    path::PathBuf,
    process::{Command, Output},
    time::{SystemTime, UNIX_EPOCH},
};

const UNREACHABLE_DSN: &str = "postgres://127.0.0.1:1/unreachable";

fn dbtool(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_dbtool"))
        .args(args)
        .output()
        .expect("dbtool command should run")
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

fn assert_write_blocked(output: Output) {
    let error = stderr_json(output);
    assert_eq!(error["error"]["code"], "WRITE_NOT_ALLOWED", "{error}");
}

fn unused_output_path() -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "dbtool-blocked-readonly-{}-{suffix}.json",
        std::process::id()
    ))
}

#[test]
fn sql_query_rejects_row_returning_writes_before_connecting() {
    for sql in [
        "with deleted as (delete from users returning *) select * from deleted",
        "select 1 as id into created_table",
        "select * from users for update",
    ] {
        assert_write_blocked(dbtool(&[
            "--dsn",
            UNREACHABLE_DSN,
            "--allow-write",
            "sql",
            "query",
            sql,
        ]));
    }
}

#[test]
fn sql_export_rejects_row_returning_writes_before_connecting_or_writing() {
    let output_path = unused_output_path();
    let output_arg = output_path.to_string_lossy().to_string();

    for sql in [
        "with deleted as (delete from users returning *) select * from deleted",
        "select 1 as id into created_table",
        "select * from users for update",
    ] {
        assert_write_blocked(dbtool(&[
            "--dsn",
            UNREACHABLE_DSN,
            "export",
            "sql",
            "--query",
            sql,
            "--out",
            &output_arg,
        ]));
    }

    assert!(!output_path.exists());
}

#[test]
fn destructive_sql_requires_allow_write_before_a_confirmation_token_is_issued() {
    assert_write_blocked(dbtool(&[
        "--dsn",
        UNREACHABLE_DSN,
        "sql",
        "exec",
        "drop table users",
    ]));
}
