use std::{
    env, fs,
    panic::{self, AssertUnwindSafe},
    path::Path,
    process::{Command, Output},
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::Value;

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

fn gated_dsn(gates: &[&str], dsn_name: &str) -> Option<String> {
    if !gates
        .iter()
        .all(|gate| env::var(gate).as_deref() == Ok("1"))
    {
        return None;
    }

    let gate_contract = gates
        .iter()
        .map(|gate| format!("{gate}=1"))
        .collect::<Vec<_>>()
        .join(" and ");
    Some(
        env::var(dsn_name)
            .unwrap_or_else(|_| panic!("{dsn_name} must be set when {gate_contract}"))
            .trim()
            .to_owned(),
    )
    .filter(|dsn| !dsn.is_empty())
    .or_else(|| panic!("{dsn_name} must not be empty when {gate_contract}"))
}

fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after the Unix epoch")
        .as_nanos();
    format!("{prefix}_{}_{}", std::process::id(), nanos)
}

fn sql_exec_with_confirmation(dsn: &str, sql: &str) -> Value {
    let first = dbtool(&["--dsn", dsn, "--allow-write", "sql", "exec", sql]);
    if first.status.success() {
        return stdout_json(first);
    }

    let first = stderr_json(first);
    assert_eq!(first["error"]["code"], "CONFIRM_REQUIRED");
    let token = first["error"]["confirm_token"]
        .as_str()
        .expect("confirmation token should be a string");
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

fn write_sql_artifact(path: &Path, rows: Value) {
    fs::write(
        path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "kind": "sql-rows",
            "version": 3,
            "columns": ["id", "note"],
            "rows": rows,
            "truncated": false
        }))
        .expect("artifact should serialize"),
    )
    .expect("artifact should be written");
}

fn import_sql_artifact(dsn: &str, table: &str, artifact: &Path) -> Output {
    dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "import",
        "sql",
        "--table",
        table,
        "--input",
        artifact
            .to_str()
            .expect("temporary artifact path should be UTF-8"),
    ])
}

fn assert_named_product_atomic_import(dsn: &str, table: &str, engine_clause: &str) {
    let temp_dir = env::temp_dir().join(unique_name("dbtool_named_sql_atomic"));
    fs::create_dir_all(&temp_dir).expect("temporary artifact directory should be created");
    let success_artifact = temp_dir.join("success.json");
    let rejected_artifact = temp_dir.join("late-constraint.json");
    let injection = format!("O'Reilly'); DROP TABLE {table}; --");
    eprintln!("dbtool named-product atomic resource: table={table}");

    sql_exec_with_confirmation(
        dsn,
        &format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY, note TEXT NOT NULL){engine_clause}"),
    );

    let exercise = panic::catch_unwind(AssertUnwindSafe(|| {
        write_sql_artifact(
            &success_artifact,
            serde_json::json!([[1, injection], [2, "second bound value"]]),
        );
        let imported = stdout_json(import_sql_artifact(dsn, table, &success_artifact));
        assert_eq!(imported["data"]["kind"], "sql-rows");
        assert_eq!(imported["data"]["table"], table);
        assert_eq!(imported["data"]["inserted"], 2);
        assert_eq!(imported["data"]["atomic"], true);

        let stored = stdout_json(dbtool(&[
            "--dsn",
            dsn,
            "sql",
            "query",
            &format!("SELECT id, note FROM {table} ORDER BY id"),
        ]));
        assert_eq!(stored["data"]["rows"][0], serde_json::json!([1, injection]));
        assert_eq!(
            stored["data"]["rows"][1],
            serde_json::json!([2, "second bound value"])
        );

        write_sql_artifact(
            &rejected_artifact,
            serde_json::json!([[3, "must roll back"], [1, "late duplicate key"]]),
        );
        let rejected = stderr_json(import_sql_artifact(dsn, table, &rejected_artifact));
        assert_eq!(rejected["error"]["code"], "QUERY_ERROR");

        let unchanged = stdout_json(dbtool(&[
            "--dsn",
            dsn,
            "sql",
            "query",
            &format!("SELECT id, note FROM {table} ORDER BY id"),
        ]));
        assert_eq!(unchanged["data"]["rows"].as_array().unwrap().len(), 2);
        assert_eq!(
            unchanged["data"]["rows"][0],
            serde_json::json!([1, injection])
        );
        assert_eq!(
            unchanged["data"]["rows"][1],
            serde_json::json!([2, "second bound value"])
        );
    }));

    let cleanup = panic::catch_unwind(AssertUnwindSafe(|| {
        sql_exec_with_confirmation(dsn, &format!("DROP TABLE {table}"));
    }));
    fs::remove_dir_all(&temp_dir).expect("temporary artifact directory should be removed");

    if let Err(payload) = exercise {
        panic::resume_unwind(payload);
    }
    if let Err(payload) = cleanup {
        panic::resume_unwind(payload);
    }
}

#[test]
fn mariadb_named_product_cli_atomic_import_and_late_constraint_rollback() {
    let Some(dsn) = gated_dsn(
        &["DBTOOL_RUN_COMPAT_INTEGRATION", "DBTOOL_RUN_MARIADB_COMPAT"],
        "DBTOOL_IT_MARIADB_DSN",
    ) else {
        return;
    };
    let table = unique_name("dbtool_it_mariadb_atomic");

    assert_named_product_atomic_import(&dsn, &table, " ENGINE=InnoDB");
}

#[test]
fn pg_compat_named_product_cockroachdb_cli_atomic_import_and_late_constraint_rollback() {
    let Some(dsn) = gated_dsn(
        &[
            "DBTOOL_RUN_PG_COMPAT_INTEGRATION",
            "DBTOOL_RUN_COCKROACH_COMPAT",
        ],
        "DBTOOL_IT_COCKROACH_DSN",
    ) else {
        return;
    };
    let table = unique_name("dbtool_it_cockroach_atomic");

    assert_named_product_atomic_import(&dsn, &table, "");
}

#[test]
fn pg_compat_named_product_timescaledb_cli_atomic_import_and_late_constraint_rollback() {
    let Some(dsn) = gated_dsn(
        &[
            "DBTOOL_RUN_PG_COMPAT_INTEGRATION",
            "DBTOOL_RUN_TIMESCALE_COMPAT",
        ],
        "DBTOOL_IT_TIMESCALE_DSN",
    ) else {
        return;
    };
    let table = unique_name("dbtool_it_timescale_atomic");

    assert_named_product_atomic_import(&dsn, &table, "");
}

#[test]
fn tidb_named_product_cli_atomic_import_and_late_constraint_rollback() {
    let Some(dsn) = gated_dsn(&["DBTOOL_RUN_TIDB_INTEGRATION"], "DBTOOL_IT_TIDB_DSN") else {
        return;
    };
    let database = env::var("DBTOOL_IT_TIDB_DB")
        .unwrap_or_else(|_| "dbtool_it_tidb".to_owned())
        .trim()
        .to_owned();
    assert!(!database.is_empty(), "DBTOOL_IT_TIDB_DB must not be empty");
    sql_exec_with_confirmation(&dsn, &format!("CREATE DATABASE IF NOT EXISTS {database}"));
    let table = format!("{database}.{}", unique_name("dbtool_it_tidb_atomic"));

    assert_named_product_atomic_import(&dsn, &table, " ENGINE=InnoDB");
}
