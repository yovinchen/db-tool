use serde_json::Value;
use std::process::{Command, Output};

const UNREACHABLE_DSN: &str = "kafka://127.0.0.1:1";

fn dbtool(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_dbtool"))
        .args(args)
        .output()
        .expect("dbtool command should run")
}

fn stdout_text(output: Output) -> String {
    assert!(
        output.status.success(),
        "expected success\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("stdout should be UTF-8")
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

fn assert_error(args: &[&str], code: &str, expected_message: &str) {
    let error = stderr_json(dbtool(args));
    assert_eq!(error["ok"], false);
    assert_eq!(error["error"]["code"], code);
    assert!(
        error["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains(expected_message)),
        "unexpected error envelope: {error}"
    );
}

fn assert_config_error(args: &[&str], expected_message: &str) {
    assert_error(args, "CONFIG_ERROR", expected_message);
}

#[test]
fn messaging_help_documents_raw_payload_and_existing_model_fields() {
    let produce_help = stdout_text(dbtool(&["mq", "produce", "--help"]));
    assert!(produce_help.contains("Raw UTF-8 payload"));
    assert!(!produce_help.contains("JSON payload"));
    assert!(produce_help.contains("--key <TEXT>"));
    assert!(produce_help.contains("--header <KEY=VALUE>"));
    assert!(produce_help.contains("--partition <PARTITION>"));
    assert!(produce_help.contains("--timestamp-ms <EPOCH_MILLIS>"));

    let consume_help = stdout_text(dbtool(&["mq", "consume", "--help"]));
    assert!(consume_help.contains("reached --max"));
    assert!(consume_help.contains("does not prove that another message exists"));
    assert!(consume_help.contains("--max <MAX>"));
    assert!(consume_help.contains("--timeout <TIMEOUT>"));
    assert!(consume_help.contains("--partition <PARTITION>"));
    assert!(consume_help.contains("--offset <OFFSET>"));
}

#[test]
fn produce_still_requires_write_permission_before_connecting() {
    assert_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "mq",
            "produce",
            "events",
            "raw payload",
        ],
        "WRITE_NOT_ALLOWED",
        "require --allow-write",
    );
}

#[test]
fn consume_bounds_and_positions_fail_as_json_before_connecting() {
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "mq",
            "consume",
            "events",
            "--max",
            "0",
        ],
        "--max must be greater than zero",
    );
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "mq",
            "consume",
            "events",
            "--timeout",
            "0",
        ],
        "--timeout must be greater than zero",
    );
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "mq",
            "consume",
            "events",
            "--partition=-1",
        ],
        "--partition must be greater than or equal to zero",
    );
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "mq",
            "consume",
            "events",
            "--offset=-1",
        ],
        "--offset must be greater than or equal to zero",
    );
}

#[test]
fn invalid_producer_metadata_fails_as_json_before_connecting() {
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "--allow-write",
            "mq",
            "produce",
            "events",
            "payload",
            "--partition=-1",
        ],
        "--partition must be greater than or equal to zero",
    );
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "--allow-write",
            "mq",
            "produce",
            "events",
            "payload",
            "--header",
            "missing-separator",
        ],
        "expected KEY=VALUE",
    );
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "--allow-write",
            "mq",
            "produce",
            "events",
            "payload",
            "--header",
            "=value",
        ],
        "header key must not be empty",
    );
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "--allow-write",
            "mq",
            "produce",
            "events",
            "payload",
            "--header",
            "trace=first",
            "--header",
            "trace=second",
        ],
        "duplicate message header key",
    );
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "--allow-write",
            "mq",
            "produce",
            "events",
            "payload",
            "--header",
            " trace=value",
        ],
        "must not have leading or trailing whitespace",
    );
}
