use serde_json::Value;
use std::{env, process::Command, time::SystemTime};

fn integration_dsn() -> Option<String> {
    if env::var("DBTOOL_RUN_SCYLLA_INTEGRATION").as_deref() == Ok("1") {
        return env::var("DBTOOL_IT_SCYLLA_DSN")
            .ok()
            .filter(|value| !value.is_empty());
    }
    if env::var("DBTOOL_RUN_CASSANDRA_INTEGRATION").as_deref() == Ok("1") {
        return env::var("DBTOOL_IT_CASSANDRA_DSN")
            .ok()
            .filter(|value| !value.is_empty());
    }
    None
}

fn dbtool(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_dbtool"))
        .args(args)
        .output()
        .expect("dbtool should run")
}

fn stdout_json(output: std::process::Output) -> Value {
    assert!(
        output.status.success(),
        "expected success\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("stdout should be JSON")
}

fn stderr_json(output: std::process::Output) -> Value {
    assert!(
        !output.status.success(),
        "expected failure\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stderr).expect("stderr should be JSON")
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
        .expect("destructive CQL should return a confirmation token");
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

fn unique_suffix() -> String {
    let millis = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock should be after Unix epoch")
        .as_millis();
    format!("{}_{}", std::process::id(), millis % 100_000_000)
}

#[test]
fn cql_catalogs_are_bounded_before_cli_rendering() {
    let Some(dsn) = integration_dsn() else {
        return;
    };
    let suffix = unique_suffix();
    let keyspace = format!("dbtool_it_bnd_{suffix}");
    let tables = [
        format!("a_{suffix}"),
        format!("b_{suffix}"),
        format!("c_{suffix}"),
    ];

    let zero = stderr_json(dbtool(&[
        "--dsn",
        "cassandra://127.0.0.1:1",
        "--limit",
        "0",
        "cql",
        "keyspaces",
    ]));
    assert_eq!(zero["error"]["code"], "CONFIG_ERROR");
    let overflow = stderr_json(dbtool(&[
        "--dsn",
        "cassandra://127.0.0.1:1",
        "--limit",
        &usize::MAX.to_string(),
        "cql",
        "tables",
    ]));
    assert_eq!(overflow["error"]["code"], "CONFIG_ERROR");

    cql_exec(
        &dsn,
        &format!(
            "create keyspace {keyspace} with replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
        ),
    );
    for table in &tables {
        cql_exec(
            &dsn,
            &format!("create table {keyspace}.{table} (id int primary key)"),
        );
    }

    let caps = stdout_json(dbtool(&["--dsn", &dsn, "caps"]));
    let operations = caps["data"]["operations"]
        .as_array()
        .expect("capability operations should be an array");
    for expected in [
        "cql.list_keyspaces_bounded",
        "cql.list_tables_bounded",
        "sql.list_schemas_bounded",
        "sql.list_tables_bounded",
    ] {
        assert!(operations.iter().any(|operation| operation == expected));
    }

    let exact_tables = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        "3",
        "cql",
        "tables",
        "--keyspace",
        &keyspace,
    ]));
    assert_eq!(exact_tables["data"].as_array().unwrap().len(), 3);
    assert_eq!(exact_tables["meta"]["truncated"], false);

    let probed_tables = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        "2",
        "cql",
        "tables",
        "--keyspace",
        &keyspace,
    ]));
    assert_eq!(probed_tables["data"].as_array().unwrap().len(), 2);
    assert_eq!(probed_tables["meta"]["truncated"], true);

    let all_keyspaces = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        "1000",
        "cql",
        "keyspaces",
    ]));
    assert_eq!(all_keyspaces["meta"]["truncated"], false);
    let keyspace_count = all_keyspaces["data"].as_array().unwrap().len();
    assert!(keyspace_count > 1);
    let exact_keyspace_limit = keyspace_count.to_string();
    let exact_keyspaces = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        &exact_keyspace_limit,
        "cql",
        "keyspaces",
    ]));
    assert_eq!(exact_keyspaces["meta"]["truncated"], false);
    let probe_keyspace_limit = (keyspace_count - 1).to_string();
    let probed_keyspaces = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        &probe_keyspace_limit,
        "cql",
        "keyspaces",
    ]));
    assert_eq!(probed_keyspaces["meta"]["truncated"], true);

    cql_exec(&dsn, &format!("drop keyspace {keyspace}"));
    let after_cleanup = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        "1000",
        "cql",
        "keyspaces",
    ]));
    assert!(after_cleanup["data"]
        .as_array()
        .unwrap()
        .iter()
        .all(|item| item != &keyspace));
}
