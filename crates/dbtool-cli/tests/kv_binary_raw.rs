use std::{
    env,
    io::{BufRead, BufReader, Read, Write},
    net::TcpStream,
    process::{Command, Output},
    time::{Duration, SystemTime, UNIX_EPOCH},
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

#[derive(Debug, PartialEq, Eq)]
enum RespValue {
    Simple(Vec<u8>),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
}

fn redis_compat_endpoint(dsn: &str) -> (&str, u16) {
    let (_, remainder) = dsn
        .split_once("://")
        .expect("compatibility DSN should include a scheme");
    let authority = remainder
        .split(['/', '?'])
        .next()
        .expect("compatibility DSN should include an authority");
    assert!(
        !authority.contains('@'),
        "direct RESP fixture setup expects the unauthenticated local compatibility profile"
    );
    let (host, port) = authority
        .rsplit_once(':')
        .expect("compatibility DSN should include an explicit port");
    let port = port
        .parse::<u16>()
        .expect("compatibility DSN port should be a valid u16");
    (host, port)
}

fn resp_line(reader: &mut BufReader<TcpStream>) -> Vec<u8> {
    let mut line = Vec::new();
    reader
        .read_until(b'\n', &mut line)
        .expect("RESP line should be readable");
    assert!(line.ends_with(b"\r\n"), "RESP line should end in CRLF");
    line.truncate(line.len() - 2);
    line
}

fn read_resp(reader: &mut BufReader<TcpStream>) -> RespValue {
    let mut marker = [0_u8; 1];
    reader
        .read_exact(&mut marker)
        .expect("RESP type marker should be readable");
    match marker[0] {
        b'+' => RespValue::Simple(resp_line(reader)),
        b'-' => panic!(
            "direct RESP fixture command failed: {}",
            String::from_utf8_lossy(&resp_line(reader))
        ),
        b':' => RespValue::Integer(
            String::from_utf8(resp_line(reader))
                .expect("RESP integer should be UTF-8")
                .parse()
                .expect("RESP integer should be numeric"),
        ),
        b'$' => {
            let length = String::from_utf8(resp_line(reader))
                .expect("RESP bulk length should be UTF-8")
                .parse::<isize>()
                .expect("RESP bulk length should be numeric");
            if length == -1 {
                return RespValue::Bulk(None);
            }
            let length = usize::try_from(length).expect("RESP bulk length should be non-negative");
            let mut bytes = vec![0_u8; length];
            reader
                .read_exact(&mut bytes)
                .expect("RESP bulk payload should be readable");
            let mut crlf = [0_u8; 2];
            reader
                .read_exact(&mut crlf)
                .expect("RESP bulk terminator should be readable");
            assert_eq!(&crlf, b"\r\n");
            RespValue::Bulk(Some(bytes))
        }
        b'*' => {
            let length = String::from_utf8(resp_line(reader))
                .expect("RESP array length should be UTF-8")
                .parse::<isize>()
                .expect("RESP array length should be numeric");
            if length == -1 {
                return RespValue::Array(None);
            }
            let length = usize::try_from(length).expect("RESP array length should be non-negative");
            RespValue::Array(Some((0..length).map(|_| read_resp(reader)).collect()))
        }
        other => panic!("unsupported RESP fixture response marker: {other:#x}"),
    }
}

fn direct_resp(dsn: &str, args: &[&[u8]]) -> RespValue {
    let endpoint = redis_compat_endpoint(dsn);
    let mut stream = TcpStream::connect(endpoint).expect("compatibility server should accept RESP");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("RESP read timeout should be configurable");
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .expect("RESP write timeout should be configurable");

    write!(stream, "*{}\r\n", args.len()).expect("RESP array header should write");
    for arg in args {
        write!(stream, "${}\r\n", arg.len()).expect("RESP bulk header should write");
        stream
            .write_all(arg)
            .expect("RESP bulk payload should write");
        stream.write_all(b"\r\n").expect("RESP CRLF should write");
    }
    stream.flush().expect("RESP fixture command should flush");

    read_resp(&mut BufReader::new(stream))
}

fn assert_recursive_xrange(value: CoreValue, expected_payloads: &[Vec<u8>]) {
    let CoreValue::Array(entries) = value else {
        panic!("XRANGE should return an array");
    };
    assert_eq!(entries.len(), expected_payloads.len());
    for (entry, expected_payload) in entries.into_iter().zip(expected_payloads) {
        let CoreValue::Array(parts) = entry else {
            panic!("XRANGE entry should be a nested array");
        };
        assert_eq!(parts.len(), 2);
        assert!(matches!(&parts[0], CoreValue::Text(id) if id.contains('-')));
        let CoreValue::Array(fields) = &parts[1] else {
            panic!("XRANGE fields should be a recursive array");
        };
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0], CoreValue::Text("payload".to_owned()));
        assert_eq!(fields[1], CoreValue::Bytes(expected_payload.clone()));
    }
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
fn value_returning_raw_mutations_are_rejected_before_connection() {
    let dsn = "redis://127.0.0.1:1/0";
    for args in [
        vec!["GETDEL", "key"],
        vec!["LPOP", "list"],
        vec!["RPOP", "list", "2"],
        vec!["SPOP", "set"],
        vec!["SET", "key", "value", "GET"],
    ] {
        let mut command = vec!["--dsn", dsn, "--allow-write", "kv", "raw"];
        command.extend(args);
        let error = stderr_json(dbtool(&command));
        assert_eq!(error["error"]["code"], "CONFIG_ERROR", "{error}");
        assert!(error["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("cannot be budgeted after mutation")));
    }
}

#[test]
fn redis_compatible_live_bounded_read_envelopes_and_cleanup() {
    if env::var("DBTOOL_RUN_INTEGRATION").as_deref() != Ok("1") {
        return;
    }

    for (dsn_variable, expected_kind) in [
        ("DBTOOL_IT_REDIS_DSN", "redis"),
        ("DBTOOL_IT_VALKEY_DSN", "valkey"),
        ("DBTOOL_IT_KEYDB_DSN", "keydb"),
        ("DBTOOL_IT_DRAGONFLY_DSN", "dragonfly"),
    ] {
        let Ok(dsn) = env::var(dsn_variable) else {
            continue;
        };
        let prefix = unique_name(&format!("dbtool_it_kv_envelope_{expected_kind}"));
        let first = format!("{prefix}:first");
        let second = format!("{prefix}:second");

        for (key, value) in [(&first, "first-value"), (&second, "second-value")] {
            stdout_json(dbtool(&[
                "--dsn",
                &dsn,
                "--allow-write",
                "kv",
                "set",
                key,
                value,
            ]));
        }

        let caps = stdout_json(dbtool(&["--dsn", &dsn, "caps"]));
        assert_eq!(caps["kind"], expected_kind);
        let operations = caps["data"]["operations"]
            .as_array()
            .expect("operations should be an array");
        for operation in [
            "kv.exists",
            "kv.get_bounded",
            "kv.get_with_expiry_bounded",
            "kv.scan_bounded",
            "kv.raw_command_bounded",
        ] {
            assert!(
                operations.iter().any(|actual| actual == operation),
                "{expected_kind} did not advertise {operation}: {caps}"
            );
        }

        let exact = stdout_json(dbtool(&["--dsn", &dsn, "kv", "get", &first]));
        assert_eq!(exact["data"]["value"], "first-value");

        let oversized = stderr_json(dbtool(&[
            "--dsn",
            &dsn,
            "--max-bytes",
            "1",
            "kv",
            "get",
            &first,
        ]));
        assert_eq!(oversized["error"]["code"], "READ_BUDGET_EXCEEDED");

        let scan = stdout_json(dbtool(&[
            "--dsn",
            &dsn,
            "--limit",
            "1",
            "kv",
            "scan",
            &format!("{prefix}:*"),
        ]));
        assert_eq!(scan["meta"]["truncated"], true, "{scan}");
        assert_eq!(scan["data"].as_array().map(Vec::len), Some(1));

        let raw = stdout_json(dbtool(&[
            "--dsn", &dsn, "--limit", "2", "kv", "raw", "MGET", &first, &second,
        ]));
        let decoded = serde_json::from_value::<CoreValue>(raw["data"].clone()).unwrap();
        assert_eq!(
            decoded,
            CoreValue::Array(vec![
                CoreValue::Text("first-value".to_owned()),
                CoreValue::Text("second-value".to_owned()),
            ])
        );

        let deleted = stdout_json(dbtool(&[
            "--dsn",
            &dsn,
            "--allow-write",
            "kv",
            "del",
            &first,
            &second,
        ]));
        assert_eq!(deleted["data"]["deleted"], 2);
        let cleanup = stdout_json(dbtool(&[
            "--dsn",
            &dsn,
            "kv",
            "scan",
            &format!("{prefix}:*"),
        ]));
        assert_eq!(cleanup["data"], serde_json::json!([]));
        assert_eq!(cleanup["meta"]["truncated"], false);
    }
}

#[test]
fn redis_compatible_products_enforce_strict_scan_and_raw_contracts() {
    if env::var("DBTOOL_RUN_COMPAT_INTEGRATION").as_deref() != Ok("1") {
        return;
    }

    for (enabled_variable, dsn_variable, expected_kind) in [
        ("DBTOOL_RUN_VALKEY_COMPAT", "DBTOOL_IT_VALKEY_DSN", "valkey"),
        ("DBTOOL_RUN_KEYDB_COMPAT", "DBTOOL_IT_KEYDB_DSN", "keydb"),
        (
            "DBTOOL_RUN_DRAGONFLY_COMPAT",
            "DBTOOL_IT_DRAGONFLY_DSN",
            "dragonfly",
        ),
    ] {
        if env::var(enabled_variable).as_deref() != Ok("1") {
            continue;
        }
        let dsn = env::var(dsn_variable)
            .unwrap_or_else(|_| panic!("{dsn_variable} should be set by the compatibility runner"));
        let prefix = unique_name(&format!("dbtool_it_kv_strict_{expected_kind}"));
        let scan_keys = (1..=25)
            .map(|index| format!("{prefix}:scan:{index:02}"))
            .collect::<Vec<_>>();
        let stream_key = format!("{prefix}:stream");

        let caps = stdout_json(dbtool(&["--dsn", &dsn, "caps"]));
        assert_eq!(caps["kind"], expected_kind);
        for key in &scan_keys {
            stdout_json(dbtool(&[
                "--dsn",
                &dsn,
                "--allow-write",
                "kv",
                "set",
                key,
                "scan-value",
            ]));
        }

        let complete_scan = stdout_json(dbtool(&[
            "--dsn",
            &dsn,
            "--limit",
            "30",
            "kv",
            "scan",
            &format!("{prefix}:scan:*"),
        ]));
        assert_eq!(complete_scan["data"].as_array().map(Vec::len), Some(25));
        assert_eq!(complete_scan["meta"]["truncated"], false);
        let actual = complete_scan["data"]
            .as_array()
            .expect("SCAN data should be an array")
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .expect("portable SCAN key should be UTF-8")
                    .to_owned()
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            actual,
            scan_keys
                .iter()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>()
        );
        let truncated_scan = stdout_json(dbtool(&[
            "--dsn",
            &dsn,
            "--limit",
            "24",
            "kv",
            "scan",
            &format!("{prefix}:scan:*"),
        ]));
        assert_eq!(truncated_scan["data"].as_array().map(Vec::len), Some(24));
        assert_eq!(truncated_scan["meta"]["truncated"], true);

        let mut binary_key = format!("{prefix}:binary:").into_bytes();
        binary_key.push(0xff);
        assert_eq!(
            direct_resp(&dsn, &[b"SET", &binary_key, b"value"]),
            RespValue::Simple(b"OK".to_vec())
        );
        let non_utf8_scan = stderr_json(dbtool(&[
            "--dsn",
            &dsn,
            "kv",
            "scan",
            &format!("{prefix}:binary:*"),
        ]));
        assert_eq!(non_utf8_scan["error"]["code"], "SERIALIZATION_ERROR");
        assert!(non_utf8_scan["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("non-UTF-8 key")));
        assert_eq!(
            direct_resp(&dsn, &[b"DEL", &binary_key]),
            RespValue::Integer(1)
        );

        for args in [
            vec!["TOTALLY_UNKNOWN"],
            vec!["KEYS", "*"],
            vec!["SCAN", "0", "COUNT", "10"],
            vec!["HGETALL", stream_key.as_str()],
        ] {
            let mut command = vec!["--dsn", dsn.as_str(), "kv", "raw"];
            command.extend_from_slice(&args);
            let rejected = stderr_json(dbtool(&command));
            assert_eq!(rejected["error"]["code"], "CONFIG_ERROR", "{rejected}");
        }

        let payloads = [vec![0_u8, 255, 1], vec![0_u8, 255, 2]];
        for (id, payload) in [(b"1-0".as_slice(), &payloads[0]), (b"2-0", &payloads[1])] {
            assert_eq!(
                direct_resp(
                    &dsn,
                    &[b"XADD", stream_key.as_bytes(), id, b"payload", payload]
                ),
                RespValue::Bulk(Some(id.to_vec()))
            );
        }
        let xrange = stdout_json(dbtool(&[
            "--dsn",
            &dsn,
            "--limit",
            "2",
            "kv",
            "raw",
            "XRANGE",
            &stream_key,
            "-",
            "+",
            "COUNT",
            "2",
        ]));
        assert_recursive_xrange(
            serde_json::from_value::<CoreValue>(xrange["data"].clone())
                .expect("typed XRANGE output should deserialize"),
            &payloads,
        );
        let bounded = stderr_json(dbtool(&[
            "--dsn",
            &dsn,
            "--limit",
            "1",
            "kv",
            "raw",
            "XRANGE",
            &stream_key,
            "-",
            "+",
            "COUNT",
            "2",
        ]));
        assert_eq!(bounded["error"]["code"], "CONFIG_ERROR");
        assert!(bounded["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("exceeding global --limit 1")));

        let mut delete_args = vec!["--dsn", &dsn, "--allow-write", "kv", "del"];
        delete_args.extend(scan_keys.iter().map(String::as_str));
        delete_args.push(&stream_key);
        let deleted = stdout_json(dbtool(&delete_args));
        assert_eq!(deleted["data"]["deleted"], 26);
        let cleanup = stdout_json(dbtool(&[
            "--dsn",
            &dsn,
            "kv",
            "scan",
            &format!("{prefix}:*"),
        ]));
        assert_eq!(cleanup["data"], serde_json::json!([]));
        assert_eq!(cleanup["meta"]["truncated"], false);
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
