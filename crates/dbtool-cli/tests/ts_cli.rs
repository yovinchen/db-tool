use serde_json::Value;
use std::{
    io::{Read, Write},
    net::TcpListener,
    process::{Command, Output},
    sync::mpsc,
    thread,
    time::Duration,
};

const UNREACHABLE_DSN: &str = "prometheus://127.0.0.1:1";

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

fn stdout_json(output: Output) -> Value {
    let stdout = stdout_text(output);
    serde_json::from_str(&stdout).expect("stdout should be JSON")
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

fn assert_config_error(args: &[&str], expected_message: &str) {
    let error = stderr_json(dbtool(args));
    assert_eq!(error["ok"], false);
    assert_eq!(error["error"]["code"], "CONFIG_ERROR");
    assert!(
        error["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains(expected_message)),
        "unexpected error envelope: {error}"
    );
}

fn mock_prometheus_once() -> (String, mpsc::Receiver<String>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("mock listener should bind");
    let address = listener.local_addr().expect("mock address should resolve");
    let (request_tx, request_rx) = mpsc::channel();

    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("query request should arrive");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("read timeout should be set");

        let mut request = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            let read = stream
                .read(&mut buffer)
                .expect("request should be readable");
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        request_tx
            .send(String::from_utf8(request).expect("request should be UTF-8"))
            .expect("request should be observed");

        let body = r#"{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"up","instance":"mock:9090"},"values":[[1710000000,"1"],[1710000060,"0"]]}]}}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream
            .write_all(response.as_bytes())
            .expect("response should be writable");
    });

    (format!("prometheus://{address}"), request_rx, handle)
}

#[test]
fn query_help_documents_range_modes_units_and_sample_budget() {
    let help = stdout_text(dbtool(&["ts", "query", "--help"]));

    assert!(help.contains("--last-minutes <MINUTES>"));
    assert!(help.contains("--start-ms <EPOCH_MILLIS>"));
    assert!(help.contains("--end-ms <EPOCH_MILLIS>"));
    assert!(help.contains("--max-series <COUNT>"));
    assert!(help.contains("Unix epoch milliseconds"));
    assert!(help.contains("60 minutes by default"));
    assert!(help.contains("1 and 1,000,000 cumulative samples"));
    assert!(help.contains("complete portable response"));
}

#[test]
fn explicit_epoch_millis_range_reaches_backend_and_preserves_json_contract() {
    let (dsn, request_rx, server) = mock_prometheus_once();
    let output = stdout_json(dbtool(&[
        "--dsn",
        &dsn,
        "--limit",
        "1",
        "ts",
        "query",
        "up",
        "--start-ms",
        "1710000000000",
        "--end-ms",
        "1710000060000",
    ]));

    let request = request_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("backend request should be captured");
    server.join().expect("mock server should exit cleanly");
    assert!(request.starts_with("GET /api/v1/query_range?"));
    assert!(request.contains("query=up"));
    assert!(request.contains("start=1710000000"));
    assert!(request.contains("end=1710000060"));

    assert_eq!(output["ok"], true);
    assert_eq!(output["kind"], "prometheus");
    assert_eq!(output["data"]["series"].as_array().unwrap().len(), 1);
    assert_eq!(
        output["data"]["series"][0]["values"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(output["data"]["truncated"], true);
    assert_eq!(output["meta"]["truncated"], true);
    assert!(output["meta"]["elapsed_ms"].is_u64());
}

#[test]
fn invalid_range_and_limit_values_fail_before_connecting() {
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "ts",
            "query",
            "up",
            "--start-ms",
            "20",
            "--end-ms",
            "10",
        ],
        "less than or equal",
    );
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "ts",
            "query",
            "up",
            "--start-ms",
            "10",
        ],
        "provided together",
    );
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "ts",
            "query",
            "up",
            "--last-minutes",
            "5",
            "--start-ms",
            "10",
            "--end-ms",
            "20",
        ],
        "cannot be combined",
    );
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "--limit",
            "0",
            "ts",
            "query",
            "up",
        ],
        "greater than zero",
    );
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "--limit",
            "1000001",
            "ts",
            "query",
            "up",
        ],
        "must not exceed 1000000",
    );
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "ts",
            "query",
            "up",
            "--max-series",
            "0",
        ],
        "--max-series must be greater than zero",
    );
    assert_config_error(
        &[
            "--dsn",
            UNREACHABLE_DSN,
            "--max-bytes",
            "0",
            "ts",
            "query",
            "up",
        ],
        "global --max-bytes must be greater than zero",
    );
}
