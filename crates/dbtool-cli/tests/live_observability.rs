use std::{
    env,
    process::{Command, Output},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde_json::Value;

fn integration_enabled() -> bool {
    env::var("DBTOOL_RUN_OBSERVABILITY_INTEGRATION").as_deref() == Ok("1")
}

fn elasticsearch_integration_enabled() -> bool {
    env::var("DBTOOL_RUN_ELASTICSEARCH_INTEGRATION").as_deref() == Ok("1")
}

fn opensearch_security_integration_enabled() -> bool {
    env::var("DBTOOL_RUN_OPENSEARCH_SECURITY_INTEGRATION").as_deref() == Ok("1")
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
        "expected success\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("stdout should be JSON")
}

fn stdout_json_retry(args: &[&str], matches: impl Fn(&Value) -> bool) -> Value {
    let mut last = None;
    for _ in 0..12 {
        let value = stdout_json(dbtool(args));
        if matches(&value) {
            return value;
        }
        last = Some(value);
        thread::sleep(Duration::from_secs(1));
    }

    panic!(
        "retry condition was not satisfied after 12 successful responses; last response: {}",
        last.expect("command should have produced JSON")
    )
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

fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} should be set for this integration test"))
}

fn unique_name(prefix: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_millis();
    format!("{prefix}_{}_{}", std::process::id(), millis)
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_millis() as i64
}

fn search_fixture_rows(value: &Value) -> Vec<(String, String, String)> {
    let mut rows = value["data"]["hits"]
        .as_array()
        .expect("search hits should be an array")
        .iter()
        .map(|hit| {
            let source = &hit["_source"];
            (
                source["name"]
                    .as_str()
                    .expect("fixture name should be a string")
                    .to_owned(),
                source["role"]
                    .as_str()
                    .expect("fixture role should be a string")
                    .to_owned(),
                source["source"]
                    .as_str()
                    .expect("fixture source should be a string")
                    .to_owned(),
            )
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

#[test]
fn opensearch_live_index_search_and_list() {
    if !integration_enabled() {
        return;
    }

    let dsn = required_env("DBTOOL_IT_OPENSEARCH_DSN");
    run_search_lifecycle(&dsn, "opensearch", "dbtool_it_search", true);
}

#[test]
fn opensearch_tls_live_index_search_and_list() {
    if !integration_enabled() {
        return;
    }

    let dsn = required_env("DBTOOL_IT_OPENSEARCH_TLS_DSN");
    assert_tls_seed_fixture(&dsn);
    run_search_lifecycle(&dsn, "opensearch+https", "dbtool_it_search_tls", false);
}

#[test]
fn elasticsearch_native_live_index_search_and_list() {
    if !elasticsearch_integration_enabled() {
        return;
    }

    let dsn = required_env("DBTOOL_IT_ELASTICSEARCH_DSN");
    run_search_lifecycle(&dsn, "elasticsearch", "dbtool_it_elasticsearch", true);
}

#[test]
fn opensearch_security_tls_live_index_search_and_list() {
    if !opensearch_security_integration_enabled() {
        return;
    }

    let dsn = required_env("DBTOOL_IT_OPENSEARCH_SECURITY_DSN");
    assert_opensearch_security_rejects_bad_credentials_and_missing_ca();
    run_search_lifecycle(&dsn, "opensearch+https", "dbtool_it_search_security", true);
}

fn assert_opensearch_security_rejects_bad_credentials_and_missing_ca() {
    let port = required_env("DBTOOL_IT_OPENSEARCH_SECURITY_PORT");
    let password = required_env("DBTOOL_IT_OPENSEARCH_SECURITY_ADMIN_PASSWORD");
    let ca = required_env("DBTOOL_IT_OPENSEARCH_SECURITY_CA");

    let wrong_password =
        format!("opensearch+https://admin:Wrong9!Credential@127.0.0.1:{port}?tls-ca={ca}");
    let rejected_auth = stderr_json(dbtool(&["--dsn", &wrong_password, "ping"]));
    assert_eq!(rejected_auth["error"]["code"], "QUERY_ERROR");
    assert!(rejected_auth["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("HTTP 401")));

    let missing_ca = format!("opensearch+https://admin:{password}@127.0.0.1:{port}");
    let rejected_trust = stderr_json(dbtool(&["--dsn", &missing_ca, "ping"]));
    assert_eq!(rejected_trust["error"]["code"], "CONNECTION_ERROR");
    assert!(rejected_trust["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("certificate")));
}

fn assert_tls_seed_fixture(dsn: &str) {
    let indices = stdout_json_retry(&["--dsn", dsn, "search", "indices"], |value| {
        value["data"]
            .as_array()
            .is_some_and(|items| items.iter().any(|item| item["name"] == "dbtool_it_seed"))
    });
    let seed_index = indices["data"]
        .as_array()
        .expect("indices should be an array")
        .iter()
        .find(|item| item["name"] == "dbtool_it_seed")
        .expect("dbtool_it_seed should be listed");
    assert_eq!(
        seed_index,
        &serde_json::json!({
            "name": "dbtool_it_seed",
            "columns": [],
            "unique": false,
            "primary": false
        })
    );

    let hits = stdout_json_retry(
        &[
            "--dsn",
            dsn,
            "--limit",
            "10",
            "search",
            "search",
            "dbtool_it_seed",
            "--q",
            r#"{"match_all":{}}"#,
            "--source",
        ],
        |value| value["data"]["total"] == 2,
    );
    assert_eq!(hits["data"]["total"], 2);
    assert_eq!(hits["meta"]["truncated"], false);
    assert_eq!(
        search_fixture_rows(&hits),
        vec![
            (
                "seed-alice".to_owned(),
                "search".to_owned(),
                "dockerfile-fixture".to_owned()
            ),
            (
                "seed-bob".to_owned(),
                "observability".to_owned(),
                "dockerfile-fixture".to_owned()
            ),
        ]
    );
}

fn run_search_lifecycle(dsn: &str, expected_kind: &str, prefix: &str, full_crud: bool) {
    let index = unique_name(prefix);

    let ping = stdout_json(dbtool(&["--dsn", dsn, "ping"]));
    assert_eq!(ping["kind"], expected_kind);

    let caps = stdout_json(dbtool(&["--dsn", dsn, "caps"]));
    assert_eq!(caps["kind"], expected_kind);
    assert_eq!(caps["data"]["search"], true);

    let blocked = stderr_json(dbtool(&[
        "--dsn",
        dsn,
        "search",
        "index",
        &index,
        r#"{"name":"alice","role":"search"}"#,
    ]));
    assert_eq!(blocked["error"]["code"], "WRITE_NOT_ALLOWED");

    if full_crud {
        let auto = stdout_json(dbtool(&[
            "--dsn",
            dsn,
            "--allow-write",
            "search",
            "index",
            &index,
            r#"{"name":"temporary","role":"search-temporary","source":"dbtool-live-fixture"}"#,
        ]));
        assert_eq!(auto["data"]["index"], index);
        assert_eq!(auto["data"]["result"], "created");
        let generated_id = auto["data"]["id"]
            .as_str()
            .expect("auto indexed document must return its generated ID")
            .to_owned();
        assert!(!generated_id.is_empty());

        let deleted = stdout_json(dbtool(&[
            "--dsn",
            dsn,
            "--allow-write",
            "search",
            "delete",
            &index,
            &generated_id,
        ]));
        assert_eq!(deleted["data"]["id"], generated_id);
        assert_eq!(deleted["data"]["result"], "deleted");

        let fixture_docs = [
            (
                "alice",
                r#"{"name":"alice","role":"search-reader","source":"dbtool-live-fixture"}"#,
            ),
            (
                "bob",
                r#"{"name":"bob","role":"search-writer","source":"dbtool-live-fixture"}"#,
            ),
            (
                "carol",
                r#"{"name":"carol","role":"search-reviewer","source":"dbtool-live-fixture"}"#,
            ),
        ];
        for (id, doc) in fixture_docs {
            let put = stdout_json(dbtool(&[
                "--dsn",
                dsn,
                "--allow-write",
                "search",
                "put",
                &index,
                id,
                doc,
            ]));
            assert_eq!(put["data"]["index"], index);
            assert_eq!(put["data"]["id"], id);
            assert_eq!(put["data"]["result"], "created");
            assert_eq!(put["data"]["version"], 1);
        }

        let alice = stdout_json(dbtool(&["--dsn", dsn, "search", "get", &index, "alice"]));
        assert_eq!(alice["data"]["index"], index);
        assert_eq!(alice["data"]["id"], "alice");
        assert_eq!(alice["data"]["found"], true);
        assert_eq!(alice["data"]["source"]["name"], "alice");
        assert_eq!(alice["data"]["source"]["role"], "search-reader");

        let missing = stdout_json(dbtool(&["--dsn", dsn, "search", "get", &index, "missing"]));
        assert!(missing["data"].is_null());

        let updated = stdout_json(dbtool(&[
            "--dsn",
            dsn,
            "--allow-write",
            "search",
            "update",
            &index,
            "bob",
            r#"{"role":"search-editor","revision":2}"#,
        ]));
        assert_eq!(updated["data"]["id"], "bob");
        assert_eq!(updated["data"]["result"], "updated");
        assert_eq!(updated["data"]["version"], 2);

        let bob = stdout_json(dbtool(&["--dsn", dsn, "search", "get", &index, "bob"]));
        assert_eq!(bob["data"]["source"]["role"], "search-editor");
        assert_eq!(bob["data"]["source"]["revision"], 2);
    } else {
        let fixture_docs = [
            r#"{"name":"alice","role":"search-reader","source":"dbtool-live-fixture"}"#,
            r#"{"name":"bob","role":"search-writer","source":"dbtool-live-fixture"}"#,
            r#"{"name":"carol","role":"search-reviewer","source":"dbtool-live-fixture"}"#,
        ];
        for doc in fixture_docs {
            let indexed = stdout_json(dbtool(&[
                "--dsn",
                dsn,
                "--allow-write",
                "search",
                "index",
                &index,
                doc,
            ]));
            assert_eq!(indexed["data"]["index"], index);
            assert_eq!(indexed["data"]["result"], "created");
            assert!(indexed["data"]["id"].as_str().is_some());
        }
    }

    let hits = stdout_json_retry(
        &[
            "--dsn",
            dsn,
            "--limit",
            "10",
            "search",
            "search",
            &index,
            "--q",
            r#"{"query":{"match_all":{}},"aggs":{"roles":{"terms":{"field":"role.keyword"}}}}"#,
            "--source",
        ],
        |value| {
            value["data"]["total"] == 3
                && value["data"]["hits"]
                    .as_array()
                    .is_some_and(|hits| hits.len() == 3)
        },
    );
    assert_eq!(hits["data"]["total"], 3);
    assert_eq!(hits["data"]["total_relation"], "eq");
    assert_eq!(hits["data"]["timed_out"], false);
    assert!(hits["data"]["took_ms"].is_u64());
    assert_eq!(hits["meta"]["truncated"], false);
    if full_crud {
        assert_eq!(
            hits["data"]["aggregations"]["roles"]["buckets"]
                .as_array()
                .expect("role aggregation buckets must be returned")
                .iter()
                .map(|bucket| bucket["doc_count"].as_u64().unwrap())
                .sum::<u64>(),
            3
        );
    }
    assert_eq!(
        search_fixture_rows(&hits),
        vec![
            (
                "alice".to_owned(),
                "search-reader".to_owned(),
                "dbtool-live-fixture".to_owned()
            ),
            (
                "bob".to_owned(),
                if full_crud {
                    "search-editor".to_owned()
                } else {
                    "search-writer".to_owned()
                },
                "dbtool-live-fixture".to_owned()
            ),
            (
                "carol".to_owned(),
                "search-reviewer".to_owned(),
                "dbtool-live-fixture".to_owned()
            ),
        ]
    );

    let oversized_body = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--limit",
        "1",
        "search",
        "search",
        &index,
        "--q",
        r#"{"query":{"match_all":{}},"sort":[{"name.keyword":"asc"}],"size":1000}"#,
        "--source",
    ]));
    assert_eq!(oversized_body["data"]["total"], 3);
    assert_eq!(oversized_body["data"]["hits"].as_array().unwrap().len(), 1);
    assert_eq!(
        oversized_body["data"]["hits"][0]["_source"]["name"],
        "alice"
    );
    assert_eq!(oversized_body["meta"]["truncated"], true);

    let body_offset = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--limit",
        "1",
        "search",
        "search",
        &index,
        "--q",
        r#"{"query":{"match_all":{}},"sort":[{"name.keyword":"asc"}],"size":1000,"from":1}"#,
        "--source",
    ]));
    assert_eq!(body_offset["data"]["total"], 3);
    assert_eq!(body_offset["data"]["hits"].as_array().unwrap().len(), 1);
    assert_eq!(body_offset["data"]["hits"][0]["_source"]["name"], "bob");
    assert_eq!(body_offset["meta"]["truncated"], true);

    let final_page = stdout_json(dbtool(&[
        "--dsn",
        dsn,
        "--limit",
        "1",
        "search",
        "search",
        &index,
        "--q",
        r#"{"query":{"match_all":{}},"sort":[{"name.keyword":"asc"}],"size":1000,"from":0}"#,
        "--from",
        "2",
        "--source",
    ]));
    assert_eq!(final_page["data"]["total"], 3);
    assert_eq!(final_page["data"]["hits"].as_array().unwrap().len(), 1);
    assert_eq!(final_page["data"]["hits"][0]["_source"]["name"], "carol");
    assert_eq!(final_page["meta"]["truncated"], false);

    let indices = stdout_json_retry(&["--dsn", dsn, "search", "indices"], |value| {
        value["data"]
            .as_array()
            .is_some_and(|items| items.iter().any(|item| item["name"] == index))
    });
    let listed = indices["data"]
        .as_array()
        .expect("indices should be an array")
        .iter()
        .find(|item| item["name"] == index)
        .expect("created index should be listed");
    assert_eq!(
        listed,
        &serde_json::json!({
            "name": index,
            "columns": [],
            "unique": false,
            "primary": false
        })
    );

    if full_crud {
        let deleted = stdout_json(dbtool(&[
            "--dsn",
            dsn,
            "--allow-write",
            "search",
            "delete",
            &index,
            "carol",
        ]));
        assert_eq!(deleted["data"]["id"], "carol");
        assert_eq!(deleted["data"]["result"], "deleted");
        let missing = stdout_json(dbtool(&["--dsn", dsn, "search", "get", &index, "carol"]));
        assert!(missing["data"].is_null());

        let confirmation = stderr_json(dbtool(&[
            "--dsn",
            dsn,
            "--allow-write",
            "search",
            "delete-index",
            &index,
        ]));
        assert_eq!(confirmation["error"]["code"], "CONFIRM_REQUIRED");
        assert_eq!(confirmation["error"]["impact"]["resource"], index);
        let token = confirmation["error"]["confirm_token"]
            .as_str()
            .expect("delete-index must provide a confirmation token")
            .to_owned();

        let wrong_target = stderr_json(dbtool(&[
            "--dsn",
            dsn,
            "--allow-write",
            "--confirm",
            &token,
            "search",
            "delete-index",
            "another-index",
        ]));
        assert_eq!(wrong_target["error"]["code"], "INTERNAL_ERROR");

        let removed = stdout_json(dbtool(&[
            "--dsn",
            dsn,
            "--allow-write",
            "--confirm",
            &token,
            "search",
            "delete-index",
            &index,
        ]));
        assert_eq!(removed["data"]["acknowledged"], true);

        let indices = stdout_json_retry(&["--dsn", dsn, "search", "indices"], |value| {
            value["data"]
                .as_array()
                .is_some_and(|items| items.iter().all(|item| item["name"] != index))
        });
        assert!(indices["data"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["name"] != index));
    }
}

#[test]
fn prometheus_rejects_zero_last_minutes_before_connecting() {
    let rejected = stderr_json(dbtool(&[
        "--dsn",
        "prometheus://127.0.0.1:1",
        "ts",
        "query",
        "up",
        "--last-minutes",
        "0",
    ]));
    assert_eq!(rejected["error"]["code"], "CONFIG_ERROR");
    assert!(rejected["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("greater than zero")));
}

#[test]
fn prometheus_live_measurements_and_query() {
    if !integration_enabled() {
        return;
    }

    let dsn = required_env("DBTOOL_IT_PROMETHEUS_DSN");
    let ping = stdout_json(dbtool(&["--dsn", &dsn, "ping"]));
    assert_eq!(ping["kind"], "prometheus");

    let caps = stdout_json(dbtool(&["--dsn", &dsn, "caps"]));
    assert_eq!(caps["kind"], "prometheus");
    assert_eq!(caps["data"]["time_series"], true);

    let blocked = stderr_json(dbtool(&[
        "--dsn",
        &dsn,
        "ts",
        "write",
        "dbtool_integration_probe",
        "1.0",
    ]));
    assert_eq!(blocked["error"]["code"], "WRITE_NOT_ALLOWED");

    let metric = unique_name("dbtool_it_probe");
    let anchor = now_millis().div_euclid(1000) * 1000;
    let first_timestamp = anchor - 8_000;
    let second_timestamp = anchor - 4_000;
    let first_timestamp_arg = first_timestamp.to_string();
    let second_timestamp_arg = second_timestamp.to_string();

    let first_written = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "ts",
        "write",
        &metric,
        "41.25",
        "--tag",
        "sample=first",
        "--tag",
        "source=dbtool_integration",
        "--timestamp-ms",
        &first_timestamp_arg,
    ]));
    assert_eq!(first_written["data"]["written_points"], 1);
    assert_eq!(first_written["data"]["written_samples"], 1);

    let second_written = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "ts",
        "write",
        &metric,
        "84.5",
        "--tag",
        "sample=second",
        "--tag",
        "source=dbtool_integration",
        "--timestamp-ms",
        &second_timestamp_arg,
    ]));
    assert_eq!(second_written["data"]["written_points"], 1);
    assert_eq!(second_written["data"]["written_samples"], 1);

    let measurements = stdout_json_retry(&["--dsn", &dsn, "ts", "measurements"], |value| {
        value["data"]
            .as_array()
            .is_some_and(|items| items.iter().any(|item| item == &metric))
    });
    assert!(measurements["data"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == &metric));

    let probed = stdout_json_retry(
        &["--dsn", &dsn, "ts", "query", &metric, "--last-minutes", "1"],
        |value| {
            value["data"]["series"].as_array().is_some_and(|series| {
                series.len() == 2
                    && series.iter().all(|item| {
                        item["values"]
                            .as_array()
                            .is_some_and(|values| !values.is_empty())
                    })
            })
        },
    );
    assert_eq!(probed["data"]["truncated"], false);
    assert_eq!(probed["meta"]["truncated"], false);
    assert_prometheus_samples(&probed, &metric, first_timestamp, second_timestamp, false);

    // Use a one-second evaluation step so the short explicit range exercises
    // both source timestamps without extending the query into the future.
    let explicit_dsn = if dsn.contains('?') {
        format!("{dsn}&step=1s")
    } else {
        format!("{dsn}?step=1s")
    };
    let explicit_start = first_timestamp.to_string();
    let explicit_end = (second_timestamp + 1_000).to_string();
    let explicit = stdout_json_retry(
        &[
            "--dsn",
            &explicit_dsn,
            "ts",
            "query",
            &metric,
            "--start-ms",
            &explicit_start,
            "--end-ms",
            &explicit_end,
        ],
        |value| {
            value["data"]["series"].as_array().is_some_and(|series| {
                series.len() == 2
                    && series.iter().all(|item| {
                        item["values"]
                            .as_array()
                            .is_some_and(|values| !values.is_empty())
                    })
            })
        },
    );
    assert_eq!(explicit["data"]["truncated"], false);
    assert_eq!(explicit["meta"]["truncated"], false);
    assert_prometheus_samples(&explicit, &metric, first_timestamp, second_timestamp, false);

    let timestamp_query = format!("timestamp({metric})");
    let timestamps = stdout_json_retry(
        &[
            "--dsn",
            &dsn,
            "ts",
            "query",
            &timestamp_query,
            "--last-minutes",
            "1",
        ],
        |value| {
            value["data"]["series"]
                .as_array()
                .is_some_and(|series| series.len() == 2)
        },
    );
    assert_prometheus_source_timestamps(&timestamps, first_timestamp, second_timestamp);

    let limited = stdout_json_retry(
        &[
            "--dsn",
            &dsn,
            "--limit",
            "1",
            "ts",
            "query",
            &metric,
            "--last-minutes",
            "1",
        ],
        |value| value["data"]["truncated"] == true,
    );
    assert_eq!(limited["data"]["truncated"], true);
    assert_eq!(limited["meta"]["truncated"], true);
    assert_prometheus_samples(&limited, &metric, first_timestamp, second_timestamp, true);

    // TimeSeriesStore intentionally exposes no public delete operation. The
    // unique metric and disposable Prometheus volume bound test data cleanup.
}

fn expected_prometheus_sample(
    sample: &str,
    first_timestamp: i64,
    second_timestamp: i64,
) -> (f64, i64) {
    match sample {
        "first" => (41.25, first_timestamp),
        "second" => (84.5, second_timestamp),
        other => panic!("unexpected sample label: {other}"),
    }
}

fn assert_prometheus_samples(
    value: &Value,
    metric: &str,
    first_timestamp: i64,
    second_timestamp: i64,
    limited: bool,
) {
    let series = value["data"]["series"]
        .as_array()
        .expect("time-series result should contain a series array");
    let total_rows = series
        .iter()
        .map(|item| item["values"].as_array().unwrap().len())
        .sum::<usize>();
    if limited {
        assert_eq!(total_rows, 1);
    } else {
        assert_eq!(series.len(), 2);
        assert!(total_rows >= 2);
    }

    let mut seen_samples = Vec::new();
    for item in series {
        assert_eq!(item["name"], metric);
        assert_eq!(
            item["columns"],
            serde_json::json!(["timestamp", "value", "sample", "source"])
        );
        let values = item["values"].as_array().unwrap();
        let series_sample = values[0][2]
            .as_str()
            .expect("sample tag should be a string");
        seen_samples.push(series_sample);
        for row in values {
            let row = row.as_array().expect("sample row should be an array");
            assert_eq!(row.len(), 4);
            let sample = row[2].as_str().expect("sample tag should be a string");
            assert_eq!(sample, series_sample);
            let (expected_value, source_timestamp) =
                expected_prometheus_sample(sample, first_timestamp, second_timestamp);
            assert_eq!(row[1].as_f64(), Some(expected_value));
            assert_eq!(row[3], "dbtool_integration");

            let evaluation_timestamp = row[0]
                .as_i64()
                .expect("query timestamp should be epoch milliseconds");
            assert!(evaluation_timestamp >= source_timestamp);
            assert!(evaluation_timestamp <= now_millis() + 1_000);
        }
    }
    seen_samples.sort();
    if limited {
        assert!(matches!(seen_samples.as_slice(), ["first"] | ["second"]));
    } else {
        assert_eq!(seen_samples, vec!["first", "second"]);
    }
}

fn assert_prometheus_source_timestamps(value: &Value, first_timestamp: i64, second_timestamp: i64) {
    let series = value["data"]["series"]
        .as_array()
        .expect("timestamp query should contain a series array");
    assert_eq!(series.len(), 2);

    let mut seen_samples = Vec::new();
    for item in series {
        assert_eq!(
            item["columns"],
            serde_json::json!(["timestamp", "value", "sample", "source"])
        );
        let values = item["values"].as_array().unwrap();
        assert!(!values.is_empty());
        let series_sample = values[0][2]
            .as_str()
            .expect("sample tag should be a string");
        seen_samples.push(series_sample);
        for row in values {
            let row = row.as_array().expect("timestamp row should be an array");
            let sample = row[2].as_str().expect("sample tag should be a string");
            assert_eq!(sample, series_sample);
            let (_, expected_timestamp) =
                expected_prometheus_sample(sample, first_timestamp, second_timestamp);
            assert_eq!(row[1].as_f64(), Some(expected_timestamp as f64 / 1000.0));
            assert_eq!(row[3], "dbtool_integration");
        }
    }
    seen_samples.sort();
    assert_eq!(seen_samples, vec!["first", "second"]);
}
