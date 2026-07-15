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

fn redis_server_time_ms(dsn: &str) -> i64 {
    let response = stdout_json(dbtool(&["--dsn", dsn, "kv", "raw", "TIME"]));
    let parts = response["data"]["$dbtool"]["value"]
        .as_array()
        .expect("Redis TIME should return a typed two-item array");
    assert_eq!(
        parts.len(),
        2,
        "Redis TIME should contain seconds and microseconds"
    );
    let integer = |value: &Value, label: &str| {
        value.as_i64().unwrap_or_else(|| {
            value
                .as_str()
                .unwrap_or_else(|| panic!("Redis TIME {label} should be integer or decimal text"))
                .parse::<i64>()
                .unwrap_or_else(|_| panic!("Redis TIME {label} should be an integer"))
        })
    };
    let seconds = integer(&parts[0], "seconds");
    let microseconds = integer(&parts[1], "microseconds");
    seconds
        .checked_mul(1_000)
        .and_then(|value| value.checked_add(microseconds / 1_000))
        .expect("Redis TIME should fit in signed milliseconds")
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
            "version": 3,
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
                },
                "expiry": {"kind": "persistent"}
            }]
        }))
        .expect("artifact should serialize"),
    )
    .expect("artifact should be written");
    let artifact_arg = artifact.to_string_lossy().to_string();

    let rejected = stderr_json(dbtool(&[
        "--dsn",
        "redis://[",
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
fn legacy_kv_v2_is_rejected_before_network_access_with_lossless_reexport_guidance() {
    let artifact = temp_path("legacy-kv-v2");
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
                "complete": true,
                "truncated": false,
                "source_changed": false,
                "exported_items": 1,
                "selected_items": 1,
                "limit": 1,
                "consistency": "best-effort"
            },
            "entries": [{
                "key": "fixture:1",
                "value": {"$dbtool": {
                    "codec": "dbtool-value-v2",
                    "type": "bytes",
                    "value": "dmFsdWU="
                }}
            }]
        }))
        .unwrap(),
    )
    .unwrap();

    let rejected = stderr_json(dbtool(&[
        "--dsn",
        "redis://[",
        "--allow-write",
        "import",
        "kv",
        "--input",
        &artifact.to_string_lossy(),
    ]));
    assert_eq!(rejected["error"]["code"], "SERIALIZATION_ERROR");
    assert!(rejected["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("kv-pairs v2")
            && message.contains("per-key expiry")
            && message.contains("re-export")));

    fs::remove_file(artifact).unwrap();
}

#[test]
fn kv_v3_missing_expiry_is_rejected_before_dsn_parsing() {
    let artifact = temp_path("kv-v3-missing-expiry");
    fs::write(
        &artifact,
        serde_json::to_vec_pretty(&serde_json::json!({
            "kind": "kv-pairs",
            "version": 3,
            "source": {
                "connector": "redis",
                "connection": "conn:source",
                "resource": "key-pattern",
                "selector": "fixture:*"
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
                "key": "fixture:1",
                "value": {"$dbtool": {
                    "codec": "dbtool-value-v2",
                    "type": "bytes",
                    "value": "dmFsdWU="
                }}
            }]
        }))
        .unwrap(),
    )
    .unwrap();

    let rejected = stderr_json(dbtool(&[
        "--dsn",
        "redis://[",
        "--allow-write",
        "import",
        "kv",
        "--input",
        &artifact.to_string_lossy(),
    ]));
    assert_eq!(rejected["error"]["code"], "SERIALIZATION_ERROR");
    assert!(rejected["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("missing field `expiry`")));

    fs::remove_file(artifact).unwrap();
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
            "version": 3,
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
                }},
                "expiry": {"kind": "persistent"}
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
    assert!(!help.contains("--ttl"));
    assert!(help.contains("exact value, and absolute expiry"));
    assert!(help.contains("atomic=false"));
    assert!(help.contains("per_entry_atomic=true"));
    assert!(help.contains("256 MiB"));
    assert!(help.contains("global --limit item budget"));
}

#[test]
fn redis_artifact_v3_preserves_lifetimes_skips_expired_and_binds_replacement() {
    let Some(dsn) = integration_dsn("DBTOOL_IT_REDIS_DSN") else {
        return;
    };
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let source_prefix = format!("dbtool_it_artifact_{suffix}:source:");
    let target_prefix = format!("dbtool_it_artifact_{suffix}:target:");
    let source_persistent = format!("{source_prefix}persistent");
    let source_binary = format!("{source_prefix}binary");
    let source_empty = format!("{source_prefix}empty");
    let source_long = format!("{source_prefix}long");
    let source_short = format!("{source_prefix}short");
    let target_persistent = format!("{target_prefix}persistent");
    let target_binary = format!("{target_prefix}binary");
    let target_empty = format!("{target_prefix}empty");
    let target_long = format!("{target_prefix}long");
    let target_short = format!("{target_prefix}short");
    let pattern = format!("{source_prefix}*");
    let complete_path = temp_path("redis-complete");
    let partial_path = temp_path("redis-partial");
    let mutated_path = temp_path("redis-mutated-expiry");
    let complete_arg = complete_path.to_string_lossy().to_string();
    let partial_arg = partial_path.to_string_lossy().to_string();
    let mutated_arg = mutated_path.to_string_lossy().to_string();

    stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "set",
        &source_persistent,
        "persistent-text",
    ]));
    stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "set",
        &source_binary,
        "--value-base64",
        "AP8=",
    ]));
    stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "set",
        &source_empty,
        "--value-base64",
        "",
    ]));
    stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "set",
        &source_long,
        "long-lived",
        "--ttl",
        "120",
    ]));
    stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "set",
        &source_short,
        "short-lived",
        "--ttl",
        "30",
    ]));

    let partial = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        "4",
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

    // Reset the short key immediately before the complete export so the
    // artifact captures it successfully, then let its absolute deadline pass.
    stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "set",
        &source_short,
        "short-lived",
        "--ttl",
        "1",
    ]));
    let long_pttl_before_export =
        stdout_json(dbtool(&["--dsn", &dsn, "kv", "raw", "PTTL", &source_long]))["data"]
            .as_i64()
            .expect("PTTL should be an integer");
    let complete = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        "5",
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
    assert_eq!(artifact["version"], 3);
    assert_eq!(artifact["integrity"]["complete"], true);
    let entries = artifact["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 5);
    assert!(entries.iter().all(|entry| {
        entry["value"]["$dbtool"]["type"] == "bytes" && entry.get("expiry").is_some()
    }));
    let entry = |key: &str| {
        entries
            .iter()
            .find(|entry| entry["key"] == key)
            .expect("artifact entry")
    };
    assert_eq!(entry(&source_persistent)["expiry"]["kind"], "persistent");
    assert_eq!(entry(&source_binary)["value"]["$dbtool"]["value"], "AP8=");
    assert_eq!(entry(&source_empty)["value"]["$dbtool"]["value"], "");
    assert_eq!(entry(&source_long)["expiry"]["kind"], "expires-at-unix-ms");
    assert_eq!(entry(&source_short)["expiry"]["kind"], "expires-at-unix-ms");
    let long_deadline = entry(&source_long)["expiry"]["unix_ms"]
        .as_i64()
        .expect("long expiry deadline");

    std::thread::sleep(std::time::Duration::from_millis(1_300));

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
    assert_eq!(imported["data"]["restored"], 4);
    assert_eq!(imported["data"]["expired_skipped"], 1);
    assert_eq!(imported["data"]["replaced"], 0);
    assert_eq!(imported["data"]["atomic"], false);
    assert_eq!(imported["data"]["per_entry_atomic"], true);
    assert_eq!(imported["data"]["expiry_preserved"], true);

    let persistent_pttl = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "kv",
        "raw",
        "PTTL",
        &target_persistent,
    ]))["data"]
        .as_i64()
        .unwrap();
    assert_eq!(persistent_pttl, -1);
    let redis_now_before_pttl = redis_server_time_ms(&dsn);
    let long_pttl_after_import =
        stdout_json(dbtool(&["--dsn", &dsn, "kv", "raw", "PTTL", &target_long]))["data"]
            .as_i64()
            .unwrap();
    assert!(long_pttl_after_import > 0);
    assert!(long_pttl_after_import <= long_pttl_before_export - 1_000);
    let reconstructed_deadline = redis_now_before_pttl + long_pttl_after_import;
    assert!(reconstructed_deadline <= long_deadline + 2);
    assert!(reconstructed_deadline >= long_deadline - 2_000);

    let binary = stdout_json(dbtool(&["--dsn", &dsn, "kv", "get", &target_binary]));
    assert_eq!(binary["data"]["value_bytes"]["$dbtool"]["value"], "AP8=");
    let empty = stdout_json(dbtool(&["--dsn", &dsn, "kv", "get", &target_empty]));
    assert_eq!(empty["data"]["value_bytes"]["$dbtool"]["value"], "");
    let short = stdout_json(dbtool(&["--dsn", &dsn, "kv", "get", &target_short]));
    assert!(short["data"]["value_bytes"].is_null());

    stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "set",
        &target_persistent,
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
    assert!(default_rejected["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("already exists or changed after preflight")));

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

    let mut mutated = artifact.clone();
    let long_entry = mutated["entries"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|entry| entry["key"] == source_long)
        .unwrap();
    long_entry["expiry"]["unix_ms"] = serde_json::json!(long_deadline + 1);
    fs::write(&mutated_path, serde_json::to_vec_pretty(&mutated).unwrap()).unwrap();
    let stale_token = stderr_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "--confirm",
        &token,
        "import",
        "kv",
        "--input",
        &mutated_arg,
        "--strip-prefix",
        &source_prefix,
        "--key-prefix",
        &target_prefix,
        "--replace-existing",
    ]));
    assert_eq!(stale_token["error"]["code"], "INTERNAL_ERROR");
    assert!(stale_token["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("confirm token mismatch")));
    let changed_confirmation = stderr_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "import",
        "kv",
        "--input",
        &mutated_arg,
        "--strip-prefix",
        &source_prefix,
        "--key-prefix",
        &target_prefix,
        "--replace-existing",
    ]));
    assert_eq!(changed_confirmation["error"]["code"], "CONFIRM_REQUIRED");
    assert_ne!(changed_confirmation["error"]["confirm_token"], token);

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
    assert_eq!(replaced["data"]["restored"], 4);
    assert_eq!(replaced["data"]["expired_skipped"], 1);
    assert_eq!(replaced["data"]["replaced"], 4);
    let restored = stdout_json(dbtool(&["--dsn", &dsn, "kv", "get", &target_persistent]));
    assert_eq!(
        restored["data"]["value_bytes"]["$dbtool"]["value"],
        "cGVyc2lzdGVudC10ZXh0"
    );

    let _ = dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "del",
        &source_persistent,
        &source_binary,
        &source_empty,
        &source_long,
        &source_short,
        &target_persistent,
        &target_binary,
        &target_empty,
        &target_long,
        &target_short,
    ]);
    let source_after = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        "20",
        "kv",
        "scan",
        &format!("{source_prefix}*"),
    ]));
    let target_after = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        "20",
        "kv",
        "scan",
        &format!("{target_prefix}*"),
    ]));
    assert_eq!(source_after["data"], serde_json::json!([]));
    assert_eq!(target_after["data"], serde_json::json!([]));
    fs::remove_file(complete_path).unwrap();
    fs::remove_file(partial_path).unwrap();
    fs::remove_file(mutated_path).unwrap();
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
