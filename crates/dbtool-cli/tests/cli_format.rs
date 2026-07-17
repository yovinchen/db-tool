use std::process::Command;

use serde_json::Value;

fn dbtool(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_dbtool"))
        .args(args)
        .output()
        .expect("dbtool command should run")
}

#[test]
fn cli_rejects_unknown_output_format() {
    let output = dbtool(&[
        "--format",
        "definitely-invalid",
        "--dsn",
        "sqlite::memory:",
        "ping",
    ]);

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).expect("clap error should be valid UTF-8");
    assert!(stderr.contains("invalid value 'definitely-invalid'"));
    assert!(stderr.contains("possible values: json, table, ndjson"));
}

#[test]
fn explicit_json_errors_wrap_clap_failures_in_the_machine_envelope() {
    let output = dbtool(&[
        "--json-errors",
        "--format",
        "definitely-invalid",
        "--dsn",
        "sqlite::memory:",
        "ping",
    ]);

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let error: Value =
        serde_json::from_slice(&output.stderr).expect("argument failure should be one JSON object");
    assert_eq!(error["ok"], false);
    assert_eq!(error["error"]["code"], "CLI_ARGUMENT_ERROR");
    assert!(error["error"]["message"]
        .as_str()
        .is_some_and(
            |message| message.contains("invalid value 'definitely-invalid'")
                && message.contains("possible values: json, table, ndjson")
        ));
}

#[test]
fn json_errors_is_global_and_handles_unknown_arguments_after_a_subcommand() {
    let output = dbtool(&[
        "--dsn",
        "sqlite::memory:",
        "ping",
        "--json-errors",
        "--not-a-dbtool-option",
    ]);

    assert_eq!(output.status.code(), Some(2));
    let error: Value =
        serde_json::from_slice(&output.stderr).expect("argument failure should be JSON");
    assert_eq!(error["error"]["code"], "CLI_ARGUMENT_ERROR");
    assert!(error["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("--not-a-dbtool-option")));
}

#[test]
fn json_errors_keeps_help_and_version_on_the_normal_clap_contract() {
    for args in [["--json-errors", "--help"], ["--json-errors", "--version"]] {
        let output = dbtool(&args);
        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        assert!(!output.stdout.is_empty());
        assert!(serde_json::from_slice::<Value>(&output.stdout).is_err());
    }
}
