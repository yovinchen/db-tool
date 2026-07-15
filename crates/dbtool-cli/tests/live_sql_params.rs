use dbtool_core::model::Value as CoreValue;
use serde_json::Value;
use std::{
    env,
    process::{Command, Output},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Copy)]
enum Backend {
    Postgres,
    MySql,
}

impl Backend {
    fn env_name(self) -> &'static str {
        match self {
            Self::Postgres => "DBTOOL_IT_POSTGRES_DSN",
            Self::MySql => "DBTOOL_IT_MYSQL_DSN",
        }
    }

    fn prefix(self) -> &'static str {
        match self {
            Self::Postgres => "pg",
            Self::MySql => "mysql",
        }
    }
}

fn integration_enabled() -> bool {
    env::var("DBTOOL_RUN_SQL_PARAM_INTEGRATION").as_deref() == Ok("1")
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
        "command failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("stdout should contain JSON")
}

fn stderr_json(output: Output) -> Value {
    assert!(!output.status.success(), "command should fail");
    serde_json::from_slice(&output.stderr).expect("stderr should contain JSON")
}

fn confirmed_exec(dsn: &str, sql: &str) -> Value {
    let first = dbtool(&["--dsn", dsn, "--allow-write", "sql", "exec", sql]);
    if first.status.success() {
        return stdout_json(first);
    }

    let confirmation = stderr_json(first);
    assert_eq!(confirmation["error"]["code"], "CONFIRM_REQUIRED");
    let token = confirmation["error"]["confirm_token"]
        .as_str()
        .expect("destructive SQL should return a confirmation token");
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

fn unique_table(backend: Backend) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_nanos();
    format!(
        "dbtool_it_{}_params_{}_{}",
        backend.prefix(),
        std::process::id(),
        nanos
    )
}

fn run_parameter_lifecycle(backend: Backend) {
    let dsn = env::var(backend.env_name())
        .unwrap_or_else(|_| panic!("{} is required", backend.env_name()));
    let table = unique_table(backend);
    let create = match backend {
        Backend::Postgres => format!(
            "create table {table} (\
             id bigint primary key, note text not null, score double precision not null, \
             enabled boolean not null, payload bytea not null, optional text, \
             occurred_at timestamptz not null, metadata jsonb not null)"
        ),
        Backend::MySql => format!(
            "create table {table} (\
             id bigint primary key, note text not null, score double not null, \
             enabled boolean not null, payload blob not null, optional text, \
             occurred_at datetime(3) not null, metadata json not null)"
        ),
    };
    confirmed_exec(&dsn, &create);

    let injection = "O'Reilly'); drop table protected_data; --";
    let timestamp = 1_700_000_000_123_i64;
    let params = serde_json::json!([
        7,
        injection,
        12.75,
        true,
        {"$bytes": [0, 127, 255]},
        null,
        {"$timestamp": timestamp},
        {"$json": {"source": backend.prefix(), "tags": ["bound", "safe"]}}
    ])
    .to_string();
    let insert = match backend {
        Backend::Postgres => format!(
            "insert into {table} \
             (id,note,score,enabled,payload,optional,occurred_at,metadata) \
             values ($1,$2,$3,$4,$5,$6,$7,$8)"
        ),
        Backend::MySql => format!(
            "insert into {table} \
             (id,note,score,enabled,payload,optional,occurred_at,metadata) \
             values (?,?,?,?,?,?,?,?)"
        ),
    };
    let inserted = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "sql",
        "exec",
        &insert,
        "--params",
        &params,
    ]));
    assert_eq!(inserted["data"]["rows_affected"], 1);

    let (query, query_params) = match backend {
        Backend::Postgres => (
            format!(
                "select id,note,score,enabled,payload,optional,occurred_at,metadata \
                 from {table} where id=$1 and note=$2"
            ),
            serde_json::json!([7, injection]).to_string(),
        ),
        Backend::MySql => (
            format!(
                "select id,note,score,enabled,payload,optional,occurred_at,metadata \
                 from {table} where id=? and note=?"
            ),
            serde_json::json!([7, injection]).to_string(),
        ),
    };
    let queried = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "sql",
        "query",
        &query,
        "--params",
        &query_params,
    ]));
    let row = &queried["data"]["rows"][0];
    assert_eq!(row[0], 7);
    assert_eq!(row[1], injection);
    assert_eq!(row[2], 12.75);
    assert_eq!(row[3], true);
    assert_eq!(
        serde_json::from_value::<CoreValue>(row[4].clone()).unwrap(),
        CoreValue::Bytes(vec![0, 127, 255])
    );
    assert_eq!(row[5], Value::Null);
    assert_eq!(
        serde_json::from_value::<CoreValue>(row[6].clone()).unwrap(),
        CoreValue::Timestamp(timestamp)
    );
    assert_eq!(
        serde_json::from_value::<CoreValue>(row[7].clone()).unwrap(),
        CoreValue::Json(serde_json::json!({"source": backend.prefix(), "tags": ["bound", "safe"]}))
    );

    let count = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "sql",
        "query",
        &format!("select count(*) as total from {table}"),
    ]));
    assert_eq!(count["data"]["rows"][0][0], 1);

    confirmed_exec(&dsn, &format!("drop table {table}"));
}

#[test]
fn postgres_live_binds_every_sql_parameter_type() {
    if integration_enabled() {
        run_parameter_lifecycle(Backend::Postgres);
    }
}

#[test]
fn mysql_live_binds_every_sql_parameter_type() {
    if integration_enabled() {
        run_parameter_lifecycle(Backend::MySql);
    }
}
