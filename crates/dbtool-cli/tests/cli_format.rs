use std::process::Command;

#[test]
fn cli_rejects_unknown_output_format() {
    let output = Command::new(env!("CARGO_BIN_EXE_dbtool"))
        .args([
            "--format",
            "definitely-invalid",
            "--dsn",
            "sqlite::memory:",
            "ping",
        ])
        .output()
        .expect("dbtool command should run");

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).expect("clap error should be valid UTF-8");
    assert!(stderr.contains("invalid value 'definitely-invalid'"));
    assert!(stderr.contains("possible values: json, table, ndjson"));
}
