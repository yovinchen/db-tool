use serde_json::Value;
use std::{
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

fn integration_enabled() -> bool {
    std::env::var("DBTOOL_RUN_INTEGRATION").as_deref() == Ok("1")
}

fn mongo_dsn() -> String {
    std::env::var("DBTOOL_IT_MONGO_DSN").expect("DBTOOL_IT_MONGO_DSN is required")
}

fn unique_collection() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after epoch")
        .as_nanos();
    format!("dbtool_it_document_surface_{nanos}")
}

fn dbtool(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_dbtool"))
        .args(args)
        .output()
        .expect("dbtool command should run")
}

fn stdout_json(output: std::process::Output) -> Value {
    assert!(
        output.status.success(),
        "command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("stdout should contain JSON")
}

fn stderr_json(output: std::process::Output) -> Value {
    assert!(!output.status.success(), "command should fail");
    serde_json::from_slice(&output.stderr).expect("stderr should contain JSON")
}

struct CollectionCleanup {
    dsn: String,
    collection: String,
}

impl Drop for CollectionCleanup {
    fn drop(&mut self) {
        let confirmation = dbtool(&[
            "--dsn",
            &self.dsn,
            "--allow-write",
            "doc",
            "drop",
            &self.collection,
        ]);
        let Ok(error) = serde_json::from_slice::<Value>(&confirmation.stderr) else {
            return;
        };
        let Some(token) = error["error"]["confirm_token"].as_str() else {
            return;
        };
        let _ = dbtool(&[
            "--dsn",
            &self.dsn,
            "--allow-write",
            "--confirm",
            token,
            "doc",
            "drop",
            &self.collection,
        ]);
    }
}

#[test]
fn mongo_live_full_find_options_bounded_aggregate_and_drop() {
    if !integration_enabled() {
        return;
    }

    let dsn = mongo_dsn();
    let collection = unique_collection();
    let _cleanup = CollectionCleanup {
        dsn: dsn.clone(),
        collection: collection.clone(),
    };
    let caps = stdout_json(dbtool(&["--dsn", &dsn, "caps"]));
    let operations = caps["data"]["operations"]
        .as_array()
        .expect("MongoDB caps operations should be an array");
    for operation in [
        "document.update_one",
        "document.update_many",
        "document.delete_one",
        "document.delete_many",
    ] {
        assert!(operations
            .iter()
            .any(|value| value.as_str() == Some(operation)));
    }
    for (name, rank, active) in [("alice", 1, true), ("bob", 2, true), ("carol", 3, false)] {
        let document = serde_json::json!({
            "name": name,
            "rank": rank,
            "active": active,
            "batch": "all",
            "secret": format!("secret-{name}"),
        })
        .to_string();
        let inserted = stdout_json(dbtool(&[
            "--dsn",
            &dsn,
            "--allow-write",
            "doc",
            "insert",
            &collection,
            &document,
        ]));
        assert_eq!(inserted["data"]["inserted"], 1);
    }

    let page = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        "1",
        "doc",
        "find",
        &collection,
        "--filter",
        r#"{"active":true}"#,
        "--skip",
        "1",
        "--sort",
        r#"{"rank":1}"#,
        "--projection",
        r#"{"_id":0,"name":1,"rank":1}"#,
    ]));
    assert_eq!(page["data"].as_array().map(Vec::len), Some(1));
    assert_eq!(page["data"][0]["name"], "bob");
    assert!(page["data"][0].get("secret").is_none());
    assert_eq!(page["meta"]["truncated"], false);

    let exact_page = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        "2",
        "doc",
        "find",
        &collection,
        "--filter",
        r#"{"active":true}"#,
        "--sort",
        r#"{"rank":1}"#,
    ]));
    assert_eq!(exact_page["data"].as_array().map(Vec::len), Some(2));
    assert_eq!(exact_page["meta"]["truncated"], false);

    let aggregate = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        "2",
        "doc",
        "aggregate",
        &collection,
        r#"[{"$sort":{"rank":1}}]"#,
    ]));
    assert_eq!(aggregate["data"].as_array().map(Vec::len), Some(2));
    assert_eq!(aggregate["meta"]["truncated"], true);

    let single_update = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "doc",
        "update",
        &collection,
        "--filter",
        r#"{"batch":"all"}"#,
        "--update",
        r#"{"single_updated":true}"#,
    ]));
    assert_eq!(single_update["data"]["matched"], 1);
    assert_eq!(single_update["data"]["modified"], 1);
    let single_updated = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "doc",
        "find",
        &collection,
        "--filter",
        r#"{"single_updated":true}"#,
    ]));
    assert_eq!(single_updated["data"].as_array().map(Vec::len), Some(1));

    let many_update_confirmation = stderr_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "doc",
        "update",
        &collection,
        "--filter",
        r#"{"batch":"all"}"#,
        "--update",
        r#"{"many_updated":true}"#,
        "--many",
    ]));
    assert_eq!(
        many_update_confirmation["error"]["code"],
        "CONFIRM_REQUIRED"
    );
    assert_eq!(
        many_update_confirmation["error"]["impact"]["op"],
        "DOCUMENT_UPDATE_MANY"
    );
    let many_update_token = many_update_confirmation["error"]["confirm_token"]
        .as_str()
        .expect("update many should return a confirmation token");

    let changed_update = stderr_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "--confirm",
        many_update_token,
        "doc",
        "update",
        &collection,
        "--filter",
        r#"{"batch":"all"}"#,
        "--update",
        r#"{"many_updated":"different"}"#,
        "--many",
    ]));
    assert_eq!(changed_update["error"]["code"], "INTERNAL_ERROR");
    assert!(changed_update["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("mismatch")));

    let cross_operation = stderr_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "--confirm",
        many_update_token,
        "doc",
        "delete",
        &collection,
        "--filter",
        r#"{"batch":"all"}"#,
        "--many",
    ]));
    assert_eq!(cross_operation["error"]["code"], "INTERNAL_ERROR");

    let many_update = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "--confirm",
        many_update_token,
        "doc",
        "update",
        &collection,
        "--filter",
        r#" { "batch": "all" } "#,
        "--update",
        r#" { "many_updated": true } "#,
        "--many",
    ]));
    assert_eq!(many_update["data"]["matched"], 3);
    assert_eq!(many_update["data"]["modified"], 3);
    let many_updated = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "doc",
        "find",
        &collection,
        "--filter",
        r#"{"many_updated":true}"#,
    ]));
    assert_eq!(many_updated["data"].as_array().map(Vec::len), Some(3));

    let single_delete = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "doc",
        "delete",
        &collection,
        "--filter",
        r#"{"batch":"all"}"#,
    ]));
    assert_eq!(single_delete["data"]["deleted"], 1);
    let after_single_delete = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "doc",
        "find",
        &collection,
        "--filter",
        r#"{"batch":"all"}"#,
    ]));
    assert_eq!(
        after_single_delete["data"].as_array().map(Vec::len),
        Some(2)
    );

    let many_delete_confirmation = stderr_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "doc",
        "delete",
        &collection,
        "--filter",
        r#"{"batch":"all"}"#,
        "--many",
    ]));
    assert_eq!(
        many_delete_confirmation["error"]["code"],
        "CONFIRM_REQUIRED"
    );
    let many_delete_token = many_delete_confirmation["error"]["confirm_token"]
        .as_str()
        .expect("delete many should return a confirmation token");
    let many_delete = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "--confirm",
        many_delete_token,
        "doc",
        "delete",
        &collection,
        "--filter",
        r#" { "batch": "all" } "#,
        "--many",
    ]));
    assert_eq!(many_delete["data"]["deleted"], 2);
    let after_many_delete = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "doc",
        "find",
        &collection,
        "--filter",
        r#"{"batch":"all"}"#,
    ]));
    assert_eq!(after_many_delete["data"].as_array().map(Vec::len), Some(0));

    let empty_update = stderr_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "doc",
        "update",
        &collection,
        "--filter",
        "{}",
        "--update",
        r#"{"active":false}"#,
    ]));
    assert_eq!(empty_update["error"]["code"], "CONFIG_ERROR");
    assert!(empty_update["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("non-empty JSON object")));

    let blocked = stderr_json(dbtool(&["--dsn", &dsn, "doc", "drop", &collection]));
    assert_eq!(blocked["error"]["code"], "WRITE_NOT_ALLOWED");

    let confirmation = stderr_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "doc",
        "drop",
        &collection,
    ]));
    assert_eq!(confirmation["error"]["code"], "CONFIRM_REQUIRED");
    assert_eq!(confirmation["error"]["impact"]["op"], "DROP_COLLECTION");
    assert_eq!(confirmation["error"]["impact"]["resource"], collection);
    let token = confirmation["error"]["confirm_token"]
        .as_str()
        .expect("drop should return a confirmation token");

    let dropped = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "--confirm",
        token,
        "doc",
        "drop",
        &collection,
    ]));
    assert_eq!(dropped["data"]["dropped"], true);
    assert_eq!(dropped["data"]["collection"], collection);

    let collections = stdout_json(dbtool(&["--dsn", &dsn, "doc", "collections"]));
    assert!(!collections["data"]
        .as_array()
        .expect("collections should be an array")
        .iter()
        .any(|value| value == &collection));
}
