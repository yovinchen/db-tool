use std::{
    env,
    process::{Command, Output},
    time::{SystemTime, UNIX_EPOCH},
};

use dbtool_core::model::Value as CoreValue;
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

fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_nanos();
    format!("{prefix}_{}_{}", std::process::id(), nanos)
}

fn confirmed_raw(dsn: &str, args: &[&str]) -> Value {
    let mut first = vec!["--dsn", dsn, "--allow-write", "kv", "raw"];
    first.extend_from_slice(args);
    let confirmation = stderr_json(dbtool(&first));
    assert_eq!(confirmation["error"]["code"], "CONFIRM_REQUIRED");
    let token = confirmation["error"]["confirm_token"]
        .as_str()
        .expect("raw mutation should expose a confirmation token");

    let mut second = vec![
        "--dsn",
        dsn,
        "--allow-write",
        "--confirm",
        token,
        "kv",
        "raw",
    ];
    second.extend_from_slice(args);
    stdout_json(dbtool(&second))
}

#[test]
fn base64_input_errors_are_config_errors_before_connection() {
    let dsn = "redis://127.0.0.1:1/0";
    for args in [
        vec![
            "--dsn",
            dsn,
            "--allow-write",
            "kv",
            "set",
            "key",
            "--value-base64",
            "AB==",
        ],
        vec![
            "--dsn",
            dsn,
            "--allow-write",
            "kv",
            "set",
            "key",
            "text",
            "--value-base64",
            "dGV4dA==",
        ],
        vec!["--dsn", dsn, "--allow-write", "kv", "set", "key"],
    ] {
        let error = stderr_json(dbtool(&args));
        assert_eq!(error["error"]["code"], "CONFIG_ERROR", "{error}");
        assert_ne!(error["error"]["code"], "CONNECTION_ERROR");
    }
}

#[test]
fn redis_live_binary_values_and_raw_policy_are_exact() {
    if env::var("DBTOOL_RUN_INTEGRATION").as_deref() != Ok("1") {
        return;
    }
    let Ok(dsn) = env::var("DBTOOL_IT_REDIS_DSN") else {
        return;
    };

    let prefix = unique_name("dbtool_it_kv_binary_raw");
    let binary_key = format!("{prefix}:binary");
    let empty_key = format!("{prefix}:empty");
    let text_key = format!("{prefix}:text");
    let raw_key = format!("{prefix}:raw");
    let raw_other_key = format!("{prefix}:raw-other");

    let binary_set = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "set",
        &binary_key,
        "--value-base64",
        "AP9oZWxsbw==",
    ]));
    assert_eq!(binary_set["data"], serde_json::json!({"ok": true}));
    assert!(!binary_set.to_string().contains("AP9oZWxsbw=="));

    stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "set",
        &empty_key,
        "--value-base64",
        "",
    ]));
    stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "set",
        &text_key,
        "compatible-text",
    ]));

    let binary = stdout_json(dbtool(&["--dsn", &dsn, "kv", "get", &binary_key]));
    assert_eq!(binary["data"]["value"], Value::Null);
    assert_eq!(binary["data"]["encoding"], "binary");
    assert_eq!(
        serde_json::from_value::<CoreValue>(binary["data"]["value_bytes"].clone()).unwrap(),
        CoreValue::Bytes(vec![0, 255, b'h', b'e', b'l', b'l', b'o'])
    );

    let empty = stdout_json(dbtool(&["--dsn", &dsn, "kv", "get", &empty_key]));
    assert_eq!(empty["data"]["value"], "");
    assert_eq!(empty["data"]["encoding"], "utf8");
    assert_eq!(
        serde_json::from_value::<CoreValue>(empty["data"]["value_bytes"].clone()).unwrap(),
        CoreValue::Bytes(vec![])
    );

    let text = stdout_json(dbtool(&["--dsn", &dsn, "kv", "get", &text_key]));
    assert_eq!(text["data"]["value"], "compatible-text");
    assert_eq!(text["data"]["encoding"], "utf8");
    assert_eq!(
        serde_json::from_value::<CoreValue>(text["data"]["value_bytes"].clone()).unwrap(),
        CoreValue::Bytes(b"compatible-text".to_vec())
    );

    let raw_binary = stdout_json(dbtool(&["--dsn", &dsn, "kv", "raw", "GET", &binary_key]));
    assert_eq!(
        serde_json::from_value::<CoreValue>(raw_binary["data"].clone()).unwrap(),
        CoreValue::Bytes(vec![0, 255, b'h', b'e', b'l', b'l', b'o'])
    );

    let blocked = stderr_json(dbtool(&[
        "--dsn", &dsn, "kv", "raw", "SET", &raw_key, "first",
    ]));
    assert_eq!(blocked["error"]["code"], "WRITE_NOT_ALLOWED");

    let confirmation = stderr_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "raw",
        "SET",
        &raw_key,
        "first",
    ]));
    assert_eq!(confirmation["error"]["code"], "CONFIRM_REQUIRED");
    let token = confirmation["error"]["confirm_token"]
        .as_str()
        .expect("raw SET should expose a confirmation token");
    assert!(!token.contains("first"));
    assert!(!token.contains(&dsn));

    let changed_value = stderr_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "--confirm",
        token,
        "kv",
        "raw",
        "SET",
        &raw_key,
        "second",
    ]));
    assert_eq!(changed_value["error"]["code"], "INTERNAL_ERROR");

    let changed_target = stderr_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "--confirm",
        token,
        "kv",
        "raw",
        "SET",
        &raw_other_key,
        "first",
    ]));
    assert_eq!(changed_target["error"]["code"], "INTERNAL_ERROR");

    let raw_set = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "--confirm",
        token,
        "kv",
        "raw",
        "SET",
        &raw_key,
        "first",
    ]));
    assert_eq!(raw_set["data"], "OK");

    for args in [
        vec!["FLUSHALL"],
        vec!["SELECT", "1"],
        vec!["EVAL", "return 1", "0"],
        vec!["FUNCTION", "LIST"],
        vec!["TOTALLY_UNKNOWN"],
        vec!["KEYS", "*"],
        vec!["HGETALL", &raw_key],
    ] {
        let mut command = vec!["--dsn", dsn.as_str(), "kv", "raw"];
        command.extend_from_slice(&args);
        let error = stderr_json(dbtool(&command));
        assert_eq!(error["error"]["code"], "CONFIG_ERROR", "{error}");
    }

    let deleted = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "del",
        &binary_key,
        &empty_key,
        &text_key,
        &raw_key,
        &raw_other_key,
    ]));
    assert_eq!(deleted["data"]["deleted"], 4);

    for key in [&binary_key, &empty_key, &text_key, &raw_key, &raw_other_key] {
        let missing = stdout_json(dbtool(&["--dsn", &dsn, "kv", "get", key]));
        assert_eq!(missing["data"]["value"], Value::Null);
        assert_eq!(missing["data"]["value_bytes"], Value::Null);
        assert_eq!(missing["data"]["encoding"], Value::Null);
    }

    // Exercise the helper after the explicit token-binding assertions too.
    let cleanup_probe = format!("{prefix}:cleanup-probe");
    assert_eq!(
        confirmed_raw(&dsn, &["SET", &cleanup_probe, "probe"])["data"],
        "OK"
    );
    let cleanup = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--allow-write",
        "kv",
        "del",
        &cleanup_probe,
    ]));
    assert_eq!(cleanup["data"]["deleted"], 1);
}
