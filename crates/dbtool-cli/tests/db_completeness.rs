use std::{
    fs,
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

fn confirmed_sql_exec(dsn: &str, sql: &str) -> Value {
    let first = dbtool(&["--dsn", dsn, "--allow-write", "sql", "exec", sql]);
    if first.status.success() {
        return stdout_json(first);
    }

    let first = stderr_json(first);
    assert_eq!(first["error"]["code"], "CONFIRM_REQUIRED");
    let token = first["error"]["confirm_token"]
        .as_str()
        .expect("confirm token should be present");
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

fn cleanup_dir(path: &Path) {
    fs::remove_dir_all(path).ok();
}

#[test]
fn sqlite_full_crud() {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "dbtool-it-sqlite-completeness-{}-{timestamp}",
        std::process::id()
    ));
    fs::create_dir_all(&root).expect("temporary directory should be created");
    let db_path = root.join("dbtool_it_sqlite.db");
    fs::File::create(&db_path).expect("SQLite file should be created");
    let dsn = format!("sqlite://{}", db_path.display());
    let export_path = root.join("dbtool_it_sqlite_export.json");
    let export_arg = export_path.to_string_lossy().to_string();
    let table = "dbtool_it_sqlite_records";
    let restored = "dbtool_it_sqlite_records_restored";

    let ping = stdout_json(dbtool(&["--dsn", &dsn, "ping"]));
    assert_eq!(ping["ok"], true);
    let caps = stdout_json(dbtool(&["--dsn", &dsn, "caps"]));
    assert_eq!(caps["data"]["sql"], true);

    let create = format!(
        "create table {table} (\
         id integer primary key, \
         name text not null, \
         active boolean not null default true, \
         score real, \
         payload blob)"
    );
    let blocked = stderr_json(dbtool(&["--dsn", &dsn, "sql", "exec", &create]));
    assert_eq!(blocked["error"]["code"], "WRITE_NOT_ALLOWED");
    assert_eq!(confirmed_sql_exec(&dsn, &create)["ok"], true);
    assert_eq!(
        confirmed_sql_exec(
            &dsn,
            &format!("create unique index dbtool_it_sqlite_records_name_uq on {table}(name)")
        )["ok"],
        true
    );

    let insert = format!(
        "insert into {table} (id, name, active, score, payload) values \
         (1, 'alice', true, 3.5, X'6869'), \
         (2, 'bob', false, null, X'00FF'), \
         (3, 'carol', true, -2.25, null)"
    );
    assert_eq!(
        stdout_json(dbtool(&[
            "--dsn",
            &dsn,
            "--allow-write",
            "sql",
            "exec",
            &insert,
        ]))["ok"],
        true
    );

    let all_rows = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "sql",
        "query",
        &format!("select id, name, active, score, payload from {table} order by id"),
    ]));
    assert_eq!(all_rows["data"]["rows"].as_array().unwrap().len(), 3);
    assert_eq!(
        all_rows["data"]["rows"][0],
        serde_json::json!([1, "alice", true, 3.5, [104, 105]])
    );
    assert_eq!(
        all_rows["data"]["rows"][1],
        serde_json::json!([2, "bob", false, null, [0, 255]])
    );
    assert_eq!(
        all_rows["data"]["rows"][2],
        serde_json::json!([3, "carol", true, -2.25, null])
    );

    let limited = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        "2",
        "sql",
        "query",
        &format!("select id from {table} order by id"),
    ]));
    assert_eq!(limited["data"]["rows"].as_array().unwrap().len(), 2);
    assert_eq!(limited["meta"]["truncated"], true);

    let schema = stdout_json(dbtool(&["--dsn", &dsn, "sql", "schema", table]));
    assert_eq!(schema["data"]["columns"].as_array().unwrap().len(), 5);
    assert_eq!(schema["data"]["columns"][0]["name"], "id");
    assert_eq!(schema["data"]["columns"][0]["primary_key"], true);
    assert!(schema["data"]["indexes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|index| index["primary"] == true));
    assert!(schema["data"]["indexes"]
        .as_array()
        .unwrap()
        .iter()
        .any(
            |index| index["name"] == "dbtool_it_sqlite_records_name_uq" && index["unique"] == true
        ));

    let update = format!("update {table} set name = 'bob-updated', score = 4.25 where id = 2");
    stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "sql",
        "exec",
        &update,
    ]));
    let updated = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "sql",
        "query",
        &format!("select id, name, score from {table} where id = 2"),
    ]));
    assert_eq!(
        updated["data"]["rows"][0],
        serde_json::json!([2, "bob-updated", 4.25])
    );

    let unbounded_delete = stderr_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "sql",
        "exec",
        &format!("delete from {table}"),
    ]));
    assert_eq!(unbounded_delete["error"]["code"], "CONFIRM_REQUIRED");

    stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "sql",
        "exec",
        &format!("delete from {table} where id = 3"),
    ]));
    let remaining = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "sql",
        "query",
        &format!("select id, name from {table} order by id"),
    ]));
    assert_eq!(
        remaining["data"]["rows"],
        serde_json::json!([[1, "alice"], [2, "bob-updated"]])
    );

    stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "export",
        "sql",
        "--query",
        &format!("select id, name, active, score, payload from {table} order by id"),
        "--out",
        &export_arg,
    ]));
    confirmed_sql_exec(
        &dsn,
        &format!(
            "create table {restored} (id integer primary key, name text not null, active boolean not null, score real, payload blob)"
        ),
    );
    stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "import",
        "sql",
        "--table",
        restored,
        "--input",
        &export_arg,
    ]));
    let restored_rows = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "sql",
        "query",
        &format!("select id, name from {restored} order by id"),
    ]));
    assert_eq!(restored_rows["data"]["rows"], remaining["data"]["rows"]);

    confirmed_sql_exec(&dsn, &format!("drop table {restored}"));
    confirmed_sql_exec(&dsn, &format!("drop table {table}"));
    let tables = stdout_json(dbtool(&["--dsn", &dsn, "sql", "tables"]));
    assert!(!tables["data"]
        .as_array()
        .unwrap()
        .iter()
        .any(|entry| entry["name"] == table || entry["name"] == restored));

    cleanup_dir(&root);
}
