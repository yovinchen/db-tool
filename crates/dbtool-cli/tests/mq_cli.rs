use serde_json::Value;
use std::process::{Command, Output};

const UNREACHABLE_DSN: &str = "kafka://127.0.0.1:1";
const UNREACHABLE_AMQP_DSN: &str = "amqp://127.0.0.1:1/%2f";

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
    assert!(produce_help.contains("--max-message-bytes <MAX_MESSAGE_BYTES>"));
    assert!(produce_help.contains("default: 8388608"));
    assert!(produce_help.contains("complete message input"));

    let consume_help = stdout_text(dbtool(&["mq", "consume", "--help"]));
    assert!(consume_help.contains("AMQP/AMQPS rejects group/durable identities"));
    assert!(consume_help.contains("requires explicit --ack on-success"));
    assert!(consume_help.contains("--allow-write"));
    assert!(consume_help.contains("reached --max"));
    assert!(consume_help.contains("does not prove that another message exists"));
    assert!(consume_help.contains("--max <MAX>"));
    assert!(consume_help.contains("--timeout <TIMEOUT>"));
    assert!(consume_help.contains("--max-message-bytes <MAX_MESSAGE_BYTES>"));
    assert!(consume_help.contains("--max-bytes <MAX_BYTES>"));
    assert!(consume_help.contains("default: 8388608"));
    assert!(consume_help.contains("payload, key, headers, cursor"));
    assert!(consume_help.contains("JetStream double-ACK"));
    assert!(consume_help.contains("Core NATS and Redis Pub/Sub"));
    assert!(consume_help.contains("--partition <PARTITION>"));
    assert!(consume_help.contains("--offset <OFFSET>"));
    assert!(consume_help.contains("--cursor <CURSOR>"));
    assert!(consume_help.contains("redis-stream:M-S"));
    assert!(consume_help.contains("--group <GROUP>"));
    assert!(consume_help.contains("--consumer <MEMBER>"));
    assert!(consume_help.contains("--durable <NAME>"));
    assert!(consume_help.contains("--ack <MODE>"));
    assert!(consume_help.contains("none"));
    assert!(consume_help.contains("on-success"));

    let delete_help = stdout_text(dbtool(&["mq", "delete", "--help"]));
    assert!(delete_help.contains("--kind <KIND>"));
    for kind in [
        "kafka-topic",
        "amqp-queue",
        "redis-stream",
        "nats-jetstream",
    ] {
        assert!(delete_help.contains(kind), "missing resource kind {kind}");
    }
    assert!(delete_help.contains("--if-empty"));
    assert!(delete_help.contains("--if-unused"));
}

#[test]
fn produce_input_budgets_fail_before_connecting_with_stable_errors() {
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "--allow-write",
            "mq",
            "produce",
            "events",
            "payload",
            "--max-message-bytes",
            "0",
        ],
        "per-message byte budget must be greater than zero",
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
            "--max-message-bytes",
            "16777217",
        ],
        "per-message byte budget exceeds the hard",
    );
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "--allow-write",
            "--max-bytes",
            "0",
            "mq",
            "produce",
            "events",
            "payload",
        ],
        "--max-bytes must be greater than zero",
    );

    assert_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "--allow-write",
            "mq",
            "produce",
            "events",
            "payload",
            "--max-message-bytes",
            "1",
        ],
        "INPUT_BUDGET_EXCEEDED",
        "input bytes budget of 1",
    );
    assert_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "--allow-write",
            "--max-bytes",
            "1",
            "mq",
            "produce",
            "events",
            "payload",
        ],
        "INPUT_BUDGET_EXCEEDED",
        "input bytes budget of 1",
    );
}

#[test]
fn amqp_consume_requires_explicit_success_ack_and_write_before_connecting() {
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_AMQP_DSN,
            "mq",
            "consume",
            "events",
            "--max",
            "1",
            "--timeout",
            "1",
        ],
        "explicit --ack on-success",
    );
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_AMQP_DSN,
            "mq",
            "consume",
            "events",
            "--ack",
            "none",
        ],
        "explicit --ack on-success",
    );
    assert_error(
        &[
            "--dsn",
            UNREACHABLE_AMQP_DSN,
            "mq",
            "consume",
            "events",
            "--ack",
            "on-success",
        ],
        "WRITE_NOT_ALLOWED",
        "require --allow-write",
    );
}

#[test]
fn stateful_consume_contract_fails_closed_before_connecting() {
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "mq",
            "consume",
            "events",
            "--group",
            "orders",
        ],
        "explicit --ack",
    );
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "mq",
            "consume",
            "events",
            "--consumer",
            "worker-1",
            "--ack",
            "none",
        ],
        "--consumer requires --group",
    );
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "mq",
            "consume",
            "events",
            "--group",
            "orders",
            "--durable",
            "billing",
            "--ack",
            "none",
        ],
        "mutually exclusive",
    );
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "mq",
            "consume",
            "events",
            "--group",
            " orders ",
            "--ack",
            "none",
        ],
        "leading or trailing whitespace",
    );
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "mq",
            "consume",
            "events",
            "--durable",
            "   ",
            "--ack",
            "none",
        ],
        "durable consumer",
    );
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "mq",
            "consume",
            "events",
            "--group",
            "orders",
            "--ack",
            "none",
            "--cursor",
            "kafka:0:1",
        ],
        "stateful consume identity",
    );
    assert_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "mq",
            "consume",
            "events",
            "--group",
            "orders",
            "--ack",
            "none",
        ],
        "WRITE_NOT_ALLOWED",
        "require --allow-write",
    );
    assert_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "mq",
            "consume",
            "events",
            "--ack",
            "on-success",
        ],
        "WRITE_NOT_ALLOWED",
        "require --allow-write",
    );
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_AMQP_DSN,
            "--allow-write",
            "mq",
            "consume",
            "events",
            "--group",
            "orders",
            "--ack",
            "on-success",
        ],
        "does not support --group or --durable",
    );
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
fn topic_catalog_limit_is_rejected_before_connecting() {
    assert_config_error(
        &["--limit", "0", "--dsn", UNREACHABLE_DSN, "mq", "topics"],
        "global --limit must be greater than zero",
    );

    let overflow = usize::MAX.to_string();
    assert_config_error(
        &[
            "--limit",
            &overflow,
            "--dsn",
            UNREACHABLE_DSN,
            "mq",
            "topics",
        ],
        "read item budget is too large to reserve a probe item",
    );
}

#[test]
fn resource_delete_requires_write_and_target_bound_confirmation_before_connecting() {
    assert_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "mq",
            "delete",
            "--kind",
            "kafka-topic",
            "events",
        ],
        "WRITE_NOT_ALLOWED",
        "require --allow-write",
    );

    let first = stderr_json(dbtool(&[
        "--dsn",
        UNREACHABLE_DSN,
        "--allow-write",
        "mq",
        "delete",
        "--kind",
        "kafka-topic",
        "events",
    ]));
    assert_eq!(first["error"]["code"], "CONFIRM_REQUIRED");
    assert_eq!(
        first["error"]["impact"]["resource"],
        "kafka-topic:\"events\""
    );
    assert_eq!(first["error"]["impact"]["op"], "DELETE_MESSAGE_RESOURCE");
}

#[test]
fn resource_delete_rejects_amqp_only_conditions_before_connecting() {
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "--allow-write",
            "mq",
            "delete",
            "--kind",
            "kafka-topic",
            "events",
            "--if-empty",
        ],
        "only to --kind amqp-queue",
    );
}

#[test]
fn resource_delete_confirmation_cannot_be_reused_with_different_conditions() {
    let first = stderr_json(dbtool(&[
        "--dsn",
        UNREACHABLE_AMQP_DSN,
        "--allow-write",
        "mq",
        "delete",
        "--kind",
        "amqp-queue",
        "events",
        "--if-empty",
    ]));
    assert_eq!(first["error"]["code"], "CONFIRM_REQUIRED");
    let token = first["error"]["confirm_token"]
        .as_str()
        .expect("delete confirmation should include a token")
        .to_owned();

    for changed_conditions in [
        Vec::<&str>::new(),
        vec!["--if-unused"],
        vec!["--if-empty", "--if-unused"],
    ] {
        let mut args = vec![
            "--dsn",
            UNREACHABLE_AMQP_DSN,
            "--allow-write",
            "--confirm",
            token.as_str(),
            "mq",
            "delete",
            "--kind",
            "amqp-queue",
            "events",
        ];
        args.extend(changed_conditions);
        assert_error(&args, "INTERNAL_ERROR", "confirm token mismatch");
    }
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
    for (option, value, expected_message) in [
        (
            "--max-message-bytes",
            "0",
            "--max-message-bytes must be greater than zero",
        ),
        ("--max-bytes", "0", "--max-bytes must be greater than zero"),
        (
            "--max-message-bytes",
            "16777217",
            "--max-message-bytes exceeds the hard",
        ),
        ("--max-bytes", "16777217", "--max-bytes exceeds the hard"),
    ] {
        assert_config_error(
            &[
                "--dsn",
                UNREACHABLE_DSN,
                "mq",
                "consume",
                "events",
                option,
                value,
            ],
            expected_message,
        );
    }

    let exact_ceiling = stderr_json(dbtool(&[
        "--dsn",
        UNREACHABLE_DSN,
        "--max-bytes",
        "16777216",
        "mq",
        "consume",
        "events",
        "--max-message-bytes",
        "16777216",
    ]));
    assert_ne!(exact_ceiling["error"]["code"], "CONFIG_ERROR");
    assert!(!exact_ceiling["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("byte ceiling")));
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
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "mq",
            "consume",
            "events",
            "--cursor",
            "redis-stream:1710000000000",
        ],
        "full <milliseconds>-<sequence>",
    );
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "mq",
            "consume",
            "events",
            "--partition",
            "0",
            "--cursor",
            "kafka:0:1",
        ],
        "cannot be combined",
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
