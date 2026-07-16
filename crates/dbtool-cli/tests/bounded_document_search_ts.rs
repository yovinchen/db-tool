use serde_json::Value;
use std::{
    env,
    process::{Command, Output},
    time::{SystemTime, UNIX_EPOCH},
};

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
    serde_json::from_slice(&output.stdout).expect("stdout should contain JSON")
}

fn stderr_json(output: Output) -> Value {
    assert!(
        !output.status.success(),
        "expected failure\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stderr).expect("stderr should contain JSON")
}

fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after the Unix epoch")
        .as_nanos();
    format!("{prefix}_{}_{}", std::process::id(), nanos)
}

fn assert_operation(dsn: &str, expected: &str) {
    let caps = stdout_json(dbtool(&["--dsn", dsn, "caps"]));
    assert!(
        caps["data"]["operations"]
            .as_array()
            .is_some_and(|operations| operations.iter().any(|value| value == expected)),
        "missing operation {expected}: {caps}"
    );
}

fn mongo_database_dsn(base: &str, database: &str) -> String {
    let (without_query, query) = base
        .split_once('?')
        .map_or((base, None), |(head, query)| (head, Some(query)));
    let authority_start = without_query
        .find("://")
        .map(|offset| offset + 3)
        .expect("MongoDB DSN should contain an authority");
    let authority_end = without_query[authority_start..]
        .find('/')
        .map(|offset| authority_start + offset)
        .unwrap_or(without_query.len());
    let mut isolated = format!("{}/{database}", &without_query[..authority_end]);
    if let Some(query) = query {
        isolated.push('?');
        isolated.push_str(query);
    }
    isolated
}

fn drop_mongo_collection(dsn: &str, collection: &str) {
    let challenge = dbtool(&["--dsn", dsn, "--allow-write", "doc", "drop", collection]);
    let Ok(challenge) = serde_json::from_slice::<Value>(&challenge.stderr) else {
        return;
    };
    let Some(token) = challenge["error"]["confirm_token"].as_str() else {
        return;
    };
    let _ = dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "--confirm",
        token,
        "doc",
        "drop",
        collection,
    ]);
}

fn delete_search_index(dsn: &str, index: &str) {
    let challenge = dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "search",
        "delete-index",
        index,
    ]);
    let Ok(challenge) = serde_json::from_slice::<Value>(&challenge.stderr) else {
        return;
    };
    let Some(token) = challenge["error"]["confirm_token"].as_str() else {
        return;
    };
    let _ = dbtool(&[
        "--dsn",
        dsn,
        "--allow-write",
        "--confirm",
        token,
        "search",
        "delete-index",
        index,
    ]);
}

struct MongoCleanup {
    dsn: String,
    collections: Vec<String>,
}

impl Drop for MongoCleanup {
    fn drop(&mut self) {
        for collection in &self.collections {
            drop_mongo_collection(&self.dsn, collection);
        }
    }
}

struct SearchCleanup {
    dsn: String,
    indices: Vec<String>,
}

impl Drop for SearchCleanup {
    fn drop(&mut self) {
        for index in &self.indices {
            delete_search_index(&self.dsn, index);
        }
    }
}

#[test]
fn invalid_catalog_limits_fail_before_connection_for_all_three_surfaces() {
    for args in [
        vec![
            "--dsn",
            "mongodb://127.0.0.1:1/app",
            "--limit",
            "0",
            "doc",
            "collections",
        ],
        vec![
            "--dsn",
            "opensearch://127.0.0.1:1",
            "--limit",
            "0",
            "search",
            "indices",
        ],
        vec![
            "--dsn",
            "prometheus://127.0.0.1:1",
            "--limit",
            "18446744073709551615",
            "ts",
            "measurements",
        ],
    ] {
        let error = stderr_json(dbtool(&args));
        assert_eq!(error["error"]["code"], "CONFIG_ERROR");
        assert!(error["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("limit") || message.contains("budget")));
    }
}

#[test]
fn mongo_live_collection_catalog_distinguishes_n_from_n_plus_one_and_cleans_up() {
    if env::var("DBTOOL_RUN_INTEGRATION").as_deref() != Ok("1") {
        return;
    }

    let base = env::var("DBTOOL_IT_MONGO_DSN").expect("DBTOOL_IT_MONGO_DSN is required");
    let dsn = mongo_database_dsn(&base, &unique_name("dbtool_it_bounded_catalog"));
    let collections = ["alpha", "beta", "gamma"];
    let _cleanup = MongoCleanup {
        dsn: dsn.clone(),
        collections: collections.iter().map(ToString::to_string).collect(),
    };
    for collection in collections {
        let inserted = stdout_json(dbtool(&[
            "--dsn",
            &dsn,
            "--allow-write",
            "doc",
            "insert",
            collection,
            r#"{"kind":"bounded-catalog"}"#,
        ]));
        assert_eq!(inserted["data"]["inserted"], 1);
    }

    let exact = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        "3",
        "doc",
        "collections",
    ]));
    assert_eq!(exact["data"].as_array().map(Vec::len), Some(3));
    assert_eq!(exact["meta"]["truncated"], false);

    let probed = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        "2",
        "doc",
        "collections",
    ]));
    assert_eq!(probed["data"].as_array().map(Vec::len), Some(2));
    assert_eq!(probed["meta"]["truncated"], true);
    assert_operation(&dsn, "document.list_collections_bounded");

    for collection in collections {
        drop_mongo_collection(&dsn, collection);
    }
    let cleaned = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        "1",
        "doc",
        "collections",
    ]));
    assert_eq!(cleaned["data"], serde_json::json!([]));
    assert_eq!(cleaned["meta"]["truncated"], false);
}

#[test]
fn mongo_live_find_and_aggregate_use_cumulative_document_budgets() {
    if env::var("DBTOOL_RUN_INTEGRATION").as_deref() != Ok("1") {
        return;
    }

    let base = env::var("DBTOOL_IT_MONGO_DSN").expect("DBTOOL_IT_MONGO_DSN is required");
    let dsn = mongo_database_dsn(&base, &unique_name("dbtool_it_document_budget"));
    let collection = "events";
    let _cleanup = MongoCleanup {
        dsn: dsn.clone(),
        collections: vec![collection.to_owned()],
    };
    for sequence in [1, 2] {
        let document = format!(
            r#"{{"sequence":{sequence},"payload":"{}"}}"#,
            "x".repeat(64)
        );
        let inserted = stdout_json(dbtool(&[
            "--dsn",
            &dsn,
            "--allow-write",
            "doc",
            "insert",
            collection,
            &document,
        ]));
        assert_eq!(inserted["data"]["inserted"], 1);
    }

    for action in [
        vec!["doc", "find", collection, "--filter", "{}"],
        vec!["doc", "aggregate", collection, "[]"],
    ] {
        let mut args = vec!["--dsn", dsn.as_str(), "--limit", "1"];
        args.extend(action);
        let bounded = stdout_json(dbtool(&args));
        assert_eq!(bounded["data"].as_array().map(Vec::len), Some(1));
        assert_eq!(bounded["meta"]["truncated"], true);
    }

    let too_small = stderr_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        "2",
        "--max-bytes",
        "1",
        "doc",
        "find",
        collection,
        "--filter",
        "{}",
    ]));
    assert_eq!(too_small["error"]["code"], "READ_BUDGET_EXCEEDED");
    assert_operation(&dsn, "document.find_budgeted");
    assert_operation(&dsn, "document.aggregate_budgeted");
}

#[test]
fn opensearch_live_index_catalog_distinguishes_n_from_n_plus_one_and_cleans_up() {
    if env::var("DBTOOL_RUN_OBSERVABILITY_INTEGRATION").as_deref() != Ok("1") {
        return;
    }

    let dsn = env::var("DBTOOL_IT_OPENSEARCH_DSN").expect("DBTOOL_IT_OPENSEARCH_DSN is required");
    let baseline = stdout_json(dbtool(&[
        "--dsn", &dsn, "--limit", "10000", "search", "indices",
    ]));
    assert_eq!(baseline["meta"]["truncated"], false);
    let baseline_count = baseline["data"]
        .as_array()
        .expect("indices should be an array")
        .len();
    let indices = [
        unique_name("dbtool_it_bounded_search_a"),
        unique_name("dbtool_it_bounded_search_b"),
        unique_name("dbtool_it_bounded_search_c"),
    ];
    let _cleanup = SearchCleanup {
        dsn: dsn.clone(),
        indices: indices.to_vec(),
    };
    for index in &indices {
        let written = stdout_json(dbtool(&[
            "--dsn",
            &dsn,
            "--allow-write",
            "search",
            "put",
            index,
            "probe",
            r#"{"kind":"bounded-catalog"}"#,
        ]));
        assert_eq!(written["data"]["id"], "probe");
    }

    let exact_limit = (baseline_count + indices.len()).to_string();
    let exact = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        &exact_limit,
        "search",
        "indices",
    ]));
    assert_eq!(
        exact["data"].as_array().map(Vec::len),
        Some(baseline_count + indices.len())
    );
    assert_eq!(exact["meta"]["truncated"], false);

    let probe_limit = (baseline_count + indices.len() - 1).to_string();
    let probed = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        &probe_limit,
        "search",
        "indices",
    ]));
    assert_eq!(
        probed["data"].as_array().map(Vec::len),
        Some(baseline_count + indices.len() - 1)
    );
    assert_eq!(probed["meta"]["truncated"], true);
    assert_operation(&dsn, "search.list_indices_bounded");

    for index in &indices {
        delete_search_index(&dsn, index);
    }
    let cleaned = stdout_json(dbtool(&[
        "--dsn", &dsn, "--limit", "10000", "search", "indices",
    ]));
    assert!(cleaned["data"]
        .as_array()
        .is_some_and(|items| items.iter().all(|item| indices
            .iter()
            .all(|index| item["name"].as_str() != Some(index)))));
}

#[test]
fn prometheus_live_measurement_catalog_distinguishes_n_from_n_plus_one_without_writes() {
    if env::var("DBTOOL_RUN_OBSERVABILITY_INTEGRATION").as_deref() != Ok("1") {
        return;
    }

    let dsn = env::var("DBTOOL_IT_PROMETHEUS_DSN").expect("DBTOOL_IT_PROMETHEUS_DSN is required");
    let all = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        "10000",
        "ts",
        "measurements",
    ]));
    assert_eq!(all["meta"]["truncated"], false);
    let count = all["data"]
        .as_array()
        .expect("measurements should be an array")
        .len();
    assert!(
        count > 1,
        "Prometheus fixture should expose multiple metrics"
    );

    let exact_limit = count.to_string();
    let exact = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        &exact_limit,
        "ts",
        "measurements",
    ]));
    assert_eq!(exact["data"].as_array().map(Vec::len), Some(count));
    assert_eq!(exact["meta"]["truncated"], false);

    let probe_limit = (count - 1).to_string();
    let probed = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        &probe_limit,
        "ts",
        "measurements",
    ]));
    assert_eq!(probed["data"].as_array().map(Vec::len), Some(count - 1));
    assert_eq!(probed["meta"]["truncated"], true);
    assert_operation(&dsn, "time_series.list_measurements_bounded");
}
