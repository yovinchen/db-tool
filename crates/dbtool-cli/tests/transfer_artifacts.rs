use serde_json::Value;
use std::{
    fs,
    path::PathBuf,
    process::{Command, Output},
    time::{SystemTime, UNIX_EPOCH},
};

fn dbtool(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_dbtool"))
        .args(args)
        .output()
        .expect("dbtool should run")
}

fn stderr_json(output: Output) -> Value {
    assert!(
        !output.status.success(),
        "dbtool unexpectedly succeeded: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    serde_json::from_slice(&output.stderr).expect("stderr should be JSON")
}

fn stdout_json(output: Output) -> Value {
    assert!(
        output.status.success(),
        "dbtool failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("stdout should be JSON")
}

fn integration_dsn(name: &str) -> Option<String> {
    if std::env::var("DBTOOL_RUN_INTEGRATION").as_deref() != Ok("1") {
        return None;
    }
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn drop_document_collection(dsn: &str, collection: &str) {
    let required = stderr_json(dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "doc",
        "drop",
        collection,
    ]));
    assert_eq!(required["error"]["code"], "CONFIRM_REQUIRED");
    let token = required["error"]["confirm_token"]
        .as_str()
        .expect("drop confirmation token")
        .to_owned();
    stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "--confirm",
        &token,
        "doc",
        "drop",
        collection,
    ]));
}

fn temp_path(name: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after the Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "dbtool-transfer-{name}-{}-{suffix}.json",
        std::process::id()
    ))
}

#[test]
fn incomplete_kv_artifact_is_rejected_before_network_access() {
    let artifact = temp_path("incomplete-kv");
    fs::write(
        &artifact,
        serde_json::to_vec_pretty(&serde_json::json!({
            "kind": "kv-pairs",
            "version": 2,
            "source": {
                "connector": "redis",
                "connection": "conn:source",
                "resource": "key-pattern",
                "selector": "fixture:*"
            },
            "integrity": {
                "value_codec": "dbtool-value-v2",
                "complete": false,
                "truncated": true,
                "source_changed": false,
                "exported_items": 1,
                "selected_items": 1,
                "limit": 1,
                "consistency": "best-effort"
            },
            "entries": [{
                "key": "fixture:1",
                "value": {
                    "$dbtool": {
                        "codec": "dbtool-value-v2",
                        "type": "bytes",
                        "value": "AP8="
                    }
                }
            }]
        }))
        .expect("artifact should serialize"),
    )
    .expect("artifact should be written");
    let artifact_arg = artifact.to_string_lossy().to_string();

    let rejected = stderr_json(dbtool(&[
        "--dsn",
        "redis://127.0.0.1:1",
        "--allow-write",
        "import",
        "kv",
        "--input",
        &artifact_arg,
    ]));
    assert_eq!(rejected["error"]["code"], "SERIALIZATION_ERROR");
    assert!(rejected["error"]["message"]
        .as_str()
        .is_some_and(
            |message| message.contains("incomplete kv-pairs") && message.contains("hit its limit")
        ));

    fs::remove_file(artifact).expect("artifact should be removed");
}

#[test]
fn import_write_gate_runs_before_artifact_and_connection_access() {
    let missing = temp_path("missing");
    let missing_arg = missing.to_string_lossy().to_string();
    let rejected = stderr_json(dbtool(&[
        "--dsn",
        "redis://127.0.0.1:1",
        "import",
        "kv",
        "--input",
        &missing_arg,
    ]));
    assert_eq!(rejected["error"]["code"], "WRITE_NOT_ALLOWED");
}

#[test]
fn action_specific_import_preflight_runs_before_network_access() {
    let sql = temp_path("invalid-sql-shape");
    fs::write(
        &sql,
        serde_json::to_vec_pretty(&serde_json::json!({
            "kind": "sql-rows",
            "version": 3,
            "columns": ["id", "ID"],
            "rows": [[1, 2]],
            "truncated": false
        }))
        .unwrap(),
    )
    .unwrap();
    let rejected = stderr_json(dbtool(&[
        "--dsn",
        "postgres://127.0.0.1:1/unreachable",
        "--allow-write",
        "import",
        "sql",
        "--table",
        "target",
        "--input",
        &sql.to_string_lossy(),
    ]));
    assert_eq!(rejected["error"]["code"], "SERIALIZATION_ERROR");
    assert!(rejected["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("duplicate SQL artifact column")));

    let kv = temp_path("invalid-kv-transform");
    fs::write(
        &kv,
        serde_json::to_vec_pretty(&serde_json::json!({
            "kind": "kv-pairs",
            "version": 2,
            "source": {
                "connector": "redis",
                "connection": "conn:source",
                "resource": "key-pattern",
                "selector": "source:*"
            },
            "integrity": {
                "value_codec": "dbtool-value-v2",
                "complete": true,
                "truncated": false,
                "source_changed": false,
                "exported_items": 1,
                "selected_items": 1,
                "limit": 1,
                "consistency": "best-effort"
            },
            "entries": [{
                "key": "source:a",
                "value": {"$dbtool": {
                    "codec": "dbtool-value-v2",
                    "type": "bytes",
                    "value": "AP8="
                }}
            }]
        }))
        .unwrap(),
    )
    .unwrap();
    let rejected = stderr_json(dbtool(&[
        "--dsn",
        "redis://127.0.0.1:1",
        "--allow-write",
        "import",
        "kv",
        "--input",
        &kv.to_string_lossy(),
        "--strip-prefix",
        "missing:",
    ]));
    assert_eq!(rejected["error"]["code"], "SERIALIZATION_ERROR");
    assert!(rejected["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("does not start with strip prefix")));

    let documents = temp_path("duplicate-document-id");
    fs::write(
        &documents,
        serde_json::to_vec_pretty(&serde_json::json!({
            "kind": "documents",
            "version": 3,
            "source": {
                "connector": "mongo",
                "connection": "conn:source",
                "resource": "source",
                "selector": {"$dbtool": {
                    "codec": "dbtool-value-v2",
                    "type": "json",
                    "value": {}
                }}
            },
            "integrity": {
                "value_codec": "dbtool-value-v2",
                "complete": true,
                "truncated": false,
                "source_changed": false,
                "exported_items": 2,
                "selected_items": 2,
                "limit": 2,
                "consistency": "best-effort"
            },
            "collection": "source",
            "documents": [
                {"_id": 7},
                {"_id": {"$dbtool": {
                    "codec": "dbtool-value-v2",
                    "type": "json",
                    "value": 7
                }}}
            ]
        }))
        .unwrap(),
    )
    .unwrap();
    let rejected = stderr_json(dbtool(&[
        "--dsn",
        "mongodb://127.0.0.1:1/unreachable",
        "--allow-write",
        "import",
        "doc",
        "target",
        "--input",
        &documents.to_string_lossy(),
    ]));
    assert_eq!(rejected["error"]["code"], "SERIALIZATION_ERROR");
    assert!(rejected["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("duplicate _id")));

    fs::remove_file(sql).unwrap();
    fs::remove_file(kv).unwrap();
    fs::remove_file(documents).unwrap();
}

#[test]
fn import_item_budget_is_enforced_before_network_access() {
    let artifact = temp_path("over-item-budget");
    fs::write(
        &artifact,
        serde_json::to_vec_pretty(&serde_json::json!({
            "kind": "sql-rows",
            "version": 3,
            "columns": ["id"],
            "rows": [[1], [2]],
            "truncated": false
        }))
        .unwrap(),
    )
    .unwrap();
    let rejected = stderr_json(dbtool(&[
        "--dsn",
        "postgres://127.0.0.1:1/unreachable",
        "--limit",
        "1",
        "--allow-write",
        "import",
        "sql",
        "--table",
        "target",
        "--input",
        &artifact.to_string_lossy(),
    ]));
    assert_eq!(rejected["error"]["code"], "CONFIG_ERROR");
    assert!(rejected["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("exceeding the import --limit 1")));
    fs::remove_file(artifact).unwrap();
}

#[test]
fn kv_import_help_documents_safe_replace_and_non_atomic_boundary() {
    let output = dbtool(&["import", "kv", "--help"]);
    assert!(output.status.success());
    let help = String::from_utf8_lossy(&output.stdout);
    assert!(help.contains("--replace-existing"));
    assert!(help.contains("complete transformed key/value set, and TTL"));
    assert!(help.contains("atomic=false"));
    assert!(help.contains("256 MiB"));
    assert!(help.contains("global --limit item budget"));
}

#[test]
fn redis_artifact_roundtrip_proves_completeness_and_replace_confirmation() {
    let Some(dsn) = integration_dsn("DBTOOL_IT_REDIS_DSN") else {
        return;
    };
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let source_prefix = format!("dbtool_it_artifact_{suffix}:source:");
    let target_prefix = format!("dbtool_it_artifact_{suffix}:target:");
    let source_one = format!("{source_prefix}one");
    let source_two = format!("{source_prefix}two");
    let target_one = format!("{target_prefix}one");
    let target_two = format!("{target_prefix}two");
    let pattern = format!("{source_prefix}*");
    let complete_path = temp_path("redis-complete");
    let partial_path = temp_path("redis-partial");
    let complete_arg = complete_path.to_string_lossy().to_string();
    let partial_arg = partial_path.to_string_lossy().to_string();

    stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "set",
        &source_one,
        "alpha",
    ]));
    stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "set",
        &source_two,
        "beta",
    ]));

    let partial = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        "1",
        "export",
        "kv",
        "--pattern",
        &pattern,
        "--out",
        &partial_arg,
    ]));
    assert_eq!(partial["data"]["complete"], false);
    assert_eq!(partial["meta"]["truncated"], true);
    let rejected = stderr_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "import",
        "kv",
        "--input",
        &partial_arg,
        "--strip-prefix",
        &source_prefix,
        "--key-prefix",
        &target_prefix,
    ]));
    assert_eq!(rejected["error"]["code"], "SERIALIZATION_ERROR");

    let complete = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        "10",
        "export",
        "kv",
        "--pattern",
        &pattern,
        "--out",
        &complete_arg,
    ]));
    assert_eq!(complete["data"]["complete"], true);
    assert_eq!(complete["meta"]["truncated"], false);
    let artifact: Value = serde_json::from_slice(&fs::read(&complete_path).unwrap()).unwrap();
    assert_eq!(artifact["version"], 2);
    assert_eq!(artifact["integrity"]["complete"], true);
    assert!(artifact["entries"]
        .as_array()
        .unwrap()
        .iter()
        .all(|entry| entry["value"]["$dbtool"]["type"] == "bytes"));

    let imported = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "import",
        "kv",
        "--input",
        &complete_arg,
        "--strip-prefix",
        &source_prefix,
        "--key-prefix",
        &target_prefix,
    ]));
    assert_eq!(imported["data"]["restored"], 2);
    assert_eq!(imported["data"]["atomic"], false);

    stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "set",
        &target_one,
        "changed",
    ]));
    let default_rejected = stderr_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "import",
        "kv",
        "--input",
        &complete_arg,
        "--strip-prefix",
        &source_prefix,
        "--key-prefix",
        &target_prefix,
    ]));
    assert_eq!(default_rejected["error"]["code"], "CONFIG_ERROR");

    let confirm_required = stderr_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "import",
        "kv",
        "--input",
        &complete_arg,
        "--strip-prefix",
        &source_prefix,
        "--key-prefix",
        &target_prefix,
        "--replace-existing",
    ]));
    assert_eq!(confirm_required["error"]["code"], "CONFIRM_REQUIRED");
    let token = confirm_required["error"]["confirm_token"]
        .as_str()
        .expect("confirmation token")
        .to_owned();
    let replaced = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "--confirm",
        &token,
        "import",
        "kv",
        "--input",
        &complete_arg,
        "--strip-prefix",
        &source_prefix,
        "--key-prefix",
        &target_prefix,
        "--replace-existing",
    ]));
    assert_eq!(replaced["data"]["replaced"], 2);
    let values = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "kv",
        "raw",
        "MGET",
        &target_one,
        &target_two,
    ]));
    assert_eq!(
        values["data"]["$dbtool"]["value"],
        serde_json::json!(["alpha", "beta"])
    );

    let _ = dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "del",
        &source_one,
        &source_two,
        &target_one,
        &target_two,
    ]);
    fs::remove_file(complete_path).unwrap();
    fs::remove_file(partial_path).unwrap();
}

#[test]
fn mongo_artifact_roundtrip_proves_bounded_completeness_and_typed_values() {
    let Some(dsn) = integration_dsn("DBTOOL_IT_MONGO_DSN") else {
        return;
    };
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let source = format!("dbtool_it_artifact_source_{suffix}");
    let target = format!("dbtool_it_artifact_target_{suffix}");
    let marker = format!("artifact-{suffix}");
    let filter = serde_json::json!({"marker": marker}).to_string();
    let complete_path = temp_path("mongo-complete");
    let partial_path = temp_path("mongo-partial");
    let complete_arg = complete_path.to_string_lossy().to_string();
    let partial_arg = partial_path.to_string_lossy().to_string();
    let first = serde_json::json!({"marker": marker, "name": "alice", "items": [1, 2]}).to_string();
    let second = serde_json::json!({"marker": marker, "name": "bob", "active": true}).to_string();

    stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "doc",
        "insert",
        &source,
        &first,
    ]));
    stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "doc",
        "insert",
        &source,
        &second,
    ]));
    let partial = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        "1",
        "export",
        "doc",
        &source,
        "--filter",
        &filter,
        "--out",
        &partial_arg,
    ]));
    assert_eq!(partial["data"]["complete"], false);
    assert_eq!(partial["meta"]["truncated"], true);
    let rejected = stderr_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "import",
        "doc",
        &target,
        "--input",
        &partial_arg,
        "--drop-id",
    ]));
    assert_eq!(rejected["error"]["code"], "SERIALIZATION_ERROR");

    stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        "10",
        "export",
        "doc",
        &source,
        "--filter",
        &filter,
        "--out",
        &complete_arg,
    ]));
    let encoded = fs::read_to_string(&complete_path).unwrap();
    assert!(!encoded.contains("dbtool:dbtool@"));
    let artifact: Value = serde_json::from_str(&encoded).unwrap();
    assert_eq!(artifact["version"], 3);
    assert_eq!(artifact["integrity"]["complete"], true);
    assert_eq!(artifact["documents"].as_array().unwrap().len(), 2);

    let imported = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "import",
        "doc",
        &target,
        "--input",
        &complete_arg,
        "--drop-id",
    ]));
    assert_eq!(imported["data"]["inserted"], 2);
    assert_eq!(imported["data"]["atomic"], false);
    let found = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        "10",
        "doc",
        "find",
        "--filter",
        &filter,
        "--sort",
        "{\"name\":1}",
        &target,
    ]));
    assert_eq!(found["data"][0]["name"], "alice");
    assert_eq!(
        found["data"][0]["items"]["$dbtool"]["value"],
        serde_json::json!([1, 2])
    );
    assert_eq!(found["data"][1]["name"], "bob");
    assert_eq!(found["data"][1]["active"], true);

    drop_document_collection(&dsn, &source);
    drop_document_collection(&dsn, &target);
    fs::remove_file(complete_path).unwrap();
    fs::remove_file(partial_path).unwrap();
}

#[test]
fn mongo_typed_ids_are_detected_before_a_second_import() {
    let Some(dsn) = integration_dsn("DBTOOL_IT_MONGO_DSN") else {
        return;
    };
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let target = format!("dbtool_it_artifact_typed_ids_{suffix}");
    let artifact = temp_path("mongo-typed-ids");
    fs::write(
        &artifact,
        serde_json::to_vec_pretty(&serde_json::json!({
            "kind": "documents",
            "version": 3,
            "source": {
                "connector": "mongo",
                "connection": "conn:source",
                "resource": "typed_ids",
                "selector": {"$dbtool": {
                    "codec": "dbtool-value-v2",
                    "type": "json",
                    "value": {}
                }}
            },
            "integrity": {
                "value_codec": "dbtool-value-v2",
                "complete": true,
                "truncated": false,
                "source_changed": false,
                "exported_items": 2,
                "selected_items": 2,
                "limit": 2,
                "consistency": "best-effort"
            },
            "collection": "typed_ids",
            "documents": [
                {
                    "_id": {"$dbtool": {
                        "codec": "dbtool-value-v2",
                        "type": "bytes",
                        "value": "AP8="
                    }},
                    "kind": "binary"
                },
                {
                    "_id": {"$dbtool": {
                        "codec": "dbtool-value-v2",
                        "type": "timestamp",
                        "value": 1700000000123_i64
                    }},
                    "kind": "datetime"
                }
            ]
        }))
        .unwrap(),
    )
    .unwrap();
    let artifact_arg = artifact.to_string_lossy().to_string();

    let first = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "import",
        "doc",
        &target,
        "--input",
        &artifact_arg,
    ]));
    assert_eq!(first["data"]["inserted"], 2);

    let second = stderr_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "import",
        "doc",
        &target,
        "--input",
        &artifact_arg,
    ]));
    assert_eq!(second["error"]["code"], "CONFIG_ERROR");
    assert!(second["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("already contains an exported _id")));

    drop_document_collection(&dsn, &target);
    fs::remove_file(artifact).unwrap();
}
