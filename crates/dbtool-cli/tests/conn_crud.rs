use dbtool_core::config::ConnectionConfig;
use serde_json::Value;
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::atomic::{AtomicU64, Ordering},
};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn root(label: &str) -> PathBuf {
    let sequence = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "dbtool-conn-crud-{label}-{}-{sequence}",
        std::process::id()
    ))
}

fn run(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_dbtool"))
        .env("XDG_CONFIG_HOME", root)
        .env_remove("DBTOOL_CONN_LOCAL")
        .env_remove("DBTOOL_CONN_OTHER")
        .env_remove("DBTOOL_CONN_ARCHIVE")
        .env_remove("DBTOOL_CONN_ENV_ONLY")
        .args(args)
        .output()
        .expect("dbtool command should run")
}

fn run_with_env(root: &Path, args: &[&str], key: &str, value: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_dbtool"))
        .env("XDG_CONFIG_HOME", root)
        .env_remove("DBTOOL_CONN_LOCAL")
        .env_remove("DBTOOL_CONN_OTHER")
        .env_remove("DBTOOL_CONN_ARCHIVE")
        .env_remove("DBTOOL_CONN_ENV_ONLY")
        .env(key, value)
        .args(args)
        .output()
        .expect("dbtool command should run")
}

fn clean_command(root: &Path, args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_dbtool"));
    command.env_clear().env("XDG_CONFIG_HOME", root).args(args);
    command
}

fn relative_xdg_command(working_directory: &Path, args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_dbtool"));
    command
        .env_clear()
        .env("XDG_CONFIG_HOME", "relative-config")
        .current_dir(working_directory)
        .args(args);
    command
}

fn relative_config_path(working_directory: &Path) -> PathBuf {
    working_directory
        .join("relative-config")
        .join("dbtool")
        .join("connections.toml")
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

fn config_path(root: &Path) -> PathBuf {
    root.join("dbtool").join("connections.toml")
}

fn confirmation_token(value: &Value) -> String {
    assert_eq!(value["error"]["code"], "CONFIRM_REQUIRED");
    value["error"]["confirm_token"]
        .as_str()
        .expect("confirmation token should be present")
        .to_owned()
}

#[test]
fn add_list_and_remove_are_local_atomic_and_secret_safe() {
    let root = root("lifecycle");
    let path = config_path(&root);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(
        &path,
        r#"# this comment is intentionally outside the typed model
[defaults.limits]
max_concurrency = 9

[connections.local]
dsn = "sqlite::memory:"

[connections.local.limits]
request_timeout = "4s"
"#,
    )
    .unwrap();

    let denied = stderr_json(run(
        &root,
        &[
            "conn",
            "add",
            "archive",
            "postgres://user:${DBTOOL_CONN_CRUD_SECRET}@127.0.0.1:1/db",
        ],
    ));
    assert_eq!(denied["error"]["code"], "WRITE_NOT_ALLOWED");

    let added_output = run_with_env(
        &root,
        &[
            "--allow-write",
            "conn",
            "add",
            "archive",
            "postgres://user:${DBTOOL_CONN_CRUD_SECRET}@127.0.0.1:1/db",
            "--readonly",
        ],
        "DBTOOL_CONN_CRUD_SECRET",
        "expanded-credential",
    );
    let added_raw = String::from_utf8_lossy(&added_output.stdout);
    assert!(!added_raw.contains("expanded-credential"));
    assert!(!added_raw.contains("DBTOOL_CONN_CRUD_SECRET"));
    let added = stdout_json(added_output);
    assert_eq!(added["data"]["action"], "added");
    assert_eq!(added["data"]["readonly"], true);
    assert_eq!(
        added["data"]["config_path"],
        path.to_string_lossy().as_ref()
    );
    assert_eq!(added["data"]["serialization"]["comments_preserved"], false);

    let persisted = fs::read_to_string(&path).unwrap();
    assert!(persisted.contains("${DBTOOL_CONN_CRUD_SECRET}"));
    assert!(!persisted.contains("expanded-credential"));
    assert!(!persisted.contains("this comment"));
    let config = ConnectionConfig::load(&path).unwrap();
    assert_eq!(config.connections["archive"].readonly, Some(true));
    assert_eq!(
        config
            .defaults
            .as_ref()
            .and_then(|defaults| defaults.limits.as_ref())
            .and_then(|limits| limits.max_concurrency),
        Some(9)
    );
    assert_eq!(
        config.connections["local"]
            .limits
            .as_ref()
            .and_then(|limits| limits.request_timeout.as_deref()),
        Some("4s")
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    let listed_output = run(&root, &["conn", "list"]);
    let listed_raw = String::from_utf8_lossy(&listed_output.stdout);
    assert!(!listed_raw.contains("expanded-credential"));
    assert!(!listed_raw.contains("DBTOOL_CONN_CRUD_SECRET"));
    let listed = stdout_json(listed_output);
    assert_eq!(
        listed["data"]["config_path"],
        path.to_string_lossy().as_ref()
    );
    assert_eq!(listed["data"]["file_connections"][0]["name"], "archive");
    assert_eq!(listed["data"]["file_connections"][1]["name"], "local");

    let remove_first = stderr_json(run(&root, &["--allow-write", "conn", "remove", "archive"]));
    let token = confirmation_token(&remove_first);
    let removed = stdout_json(run(
        &root,
        &[
            "--allow-write",
            "--confirm",
            &token,
            "conn",
            "remove",
            "archive",
        ],
    ));
    assert_eq!(removed["data"]["action"], "removed");
    assert!(!removed.to_string().contains("DBTOOL_CONN_CRUD_SECRET"));
    assert!(!ConnectionConfig::load(&path)
        .unwrap()
        .connections
        .contains_key("archive"));
    fs::remove_dir_all(root).ok();
}

#[test]
fn replace_and_remove_tokens_are_bound_to_action_target_and_content() {
    let root = root("confirmation-binding");
    let path = config_path(&root);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(
        &path,
        r#"[connections.local]
dsn = "postgres://user:old-credential@127.0.0.1:1/db"

[connections.local.limits]
max_concurrency = 3

[connections.other]
dsn = "sqlite::memory:"
"#,
    )
    .unwrap();

    let duplicate = run(
        &root,
        &[
            "--allow-write",
            "conn",
            "add",
            "local",
            "postgres://user:new-credential@127.0.0.1:1/db",
        ],
    );
    assert!(!String::from_utf8_lossy(&duplicate.stderr).contains("old-credential"));
    assert!(!String::from_utf8_lossy(&duplicate.stderr).contains("new-credential"));
    assert_eq!(stderr_json(duplicate)["error"]["code"], "CONFIG_ERROR");

    let replacement = stderr_json(run(
        &root,
        &[
            "--allow-write",
            "conn",
            "add",
            "local",
            "postgres://user:new-credential@127.0.0.1:1/db",
            "--replace",
        ],
    ));
    let replace_token = confirmation_token(&replacement);
    let wrong_content = run(
        &root,
        &[
            "--allow-write",
            "--confirm",
            &replace_token,
            "conn",
            "add",
            "local",
            "postgres://user:different-credential@127.0.0.1:1/db",
            "--replace",
        ],
    );
    let wrong_text = String::from_utf8_lossy(&wrong_content.stderr);
    assert!(!wrong_text.contains("different-credential"));
    assert_eq!(
        stderr_json(wrong_content)["error"]["code"],
        "INTERNAL_ERROR"
    );

    let replaced = stdout_json(run(
        &root,
        &[
            "--allow-write",
            "--confirm",
            &replace_token,
            "conn",
            "add",
            "local",
            "postgres://user:new-credential@127.0.0.1:1/db",
            "--replace",
        ],
    ));
    assert_eq!(replaced["data"]["action"], "replaced");
    let config = ConnectionConfig::load(&path).unwrap();
    assert_eq!(
        config.connections["local"]
            .limits
            .as_ref()
            .and_then(|limits| limits.max_concurrency),
        Some(3),
        "replace must preserve an existing connection limit policy"
    );

    let local_remove = stderr_json(run(&root, &["--allow-write", "conn", "remove", "local"]));
    let local_remove_token = confirmation_token(&local_remove);
    let wrong_target = stderr_json(run(
        &root,
        &[
            "--allow-write",
            "--confirm",
            &local_remove_token,
            "conn",
            "remove",
            "other",
        ],
    ));
    assert_eq!(wrong_target["error"]["code"], "INTERNAL_ERROR");
    assert!(ConnectionConfig::load(&path)
        .unwrap()
        .connections
        .contains_key("other"));

    let removed = stdout_json(run(
        &root,
        &[
            "--allow-write",
            "--confirm",
            &local_remove_token,
            "conn",
            "remove",
            "local",
        ],
    ));
    assert_eq!(removed["data"]["name"], "local");
    assert!(!removed.to_string().contains("new-credential"));
    fs::remove_dir_all(root).ok();
}

#[test]
fn relative_xdg_remove_tokens_are_bound_to_the_absolute_working_directory() {
    let root = root("relative-xdg-remove-binding");
    let first_working_directory = root.join("first-cwd");
    let second_working_directory = root.join("second-cwd");
    for working_directory in [&first_working_directory, &second_working_directory] {
        let path = relative_config_path(working_directory);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, "[connections.local]\ndsn = \"sqlite::memory:\"\n").unwrap();
    }

    let first = stderr_json(
        relative_xdg_command(
            &first_working_directory,
            &["--allow-write", "conn", "remove", "local"],
        )
        .output()
        .unwrap(),
    );
    let second = stderr_json(
        relative_xdg_command(
            &second_working_directory,
            &["--allow-write", "conn", "remove", "local"],
        )
        .output()
        .unwrap(),
    );
    let first_token = confirmation_token(&first);
    let second_token = confirmation_token(&second);
    assert_ne!(first_token, second_token);
    assert_ne!(
        first["error"]["impact"]["target"],
        second["error"]["impact"]["target"]
    );
    assert!(first["error"]["impact"]["target"]
        .as_str()
        .unwrap()
        .starts_with("config:"));

    let cross_directory = stderr_json(
        relative_xdg_command(
            &second_working_directory,
            &[
                "--allow-write",
                "--confirm",
                &first_token,
                "conn",
                "remove",
                "local",
            ],
        )
        .output()
        .unwrap(),
    );
    assert_eq!(cross_directory["error"]["code"], "INTERNAL_ERROR");
    assert!(relative_config_path(&second_working_directory).exists());

    let removed = stdout_json(
        relative_xdg_command(
            &second_working_directory,
            &[
                "--allow-write",
                "--confirm",
                &second_token,
                "conn",
                "remove",
                "local",
            ],
        )
        .output()
        .unwrap(),
    );
    assert_eq!(removed["data"]["action"], "removed");
    assert_eq!(
        removed["data"]["config_path"],
        second["error"]["impact"]["target"]
            .as_str()
            .unwrap()
            .strip_prefix("config:")
            .unwrap()
    );
    let rendered_path = removed["data"]["config_path"].as_str().unwrap();
    #[cfg(unix)]
    assert!(rendered_path.starts_with('/'));
    #[cfg(windows)]
    assert_eq!(rendered_path.as_bytes().get(1), Some(&b':'));
    fs::remove_dir_all(root).ok();
}

#[test]
fn relative_xdg_replace_tokens_are_bound_to_the_absolute_working_directory() {
    let root = root("relative-xdg-replace-binding");
    let first_working_directory = root.join("first-cwd");
    let second_working_directory = root.join("second-cwd");
    for working_directory in [&first_working_directory, &second_working_directory] {
        let path = relative_config_path(working_directory);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, "[connections.local]\ndsn = \"sqlite::memory:\"\n").unwrap();
    }
    let replace_args = [
        "--allow-write",
        "conn",
        "add",
        "local",
        "sqlite:replacement.db",
        "--replace",
    ];
    let first = stderr_json(
        relative_xdg_command(&first_working_directory, &replace_args)
            .output()
            .unwrap(),
    );
    let second = stderr_json(
        relative_xdg_command(&second_working_directory, &replace_args)
            .output()
            .unwrap(),
    );
    let first_token = confirmation_token(&first);
    let second_token = confirmation_token(&second);
    assert_ne!(first_token, second_token);
    assert_ne!(
        first["error"]["impact"]["target"],
        second["error"]["impact"]["target"]
    );

    let cross_directory = stderr_json(
        relative_xdg_command(
            &second_working_directory,
            &[
                "--allow-write",
                "--confirm",
                &first_token,
                "conn",
                "add",
                "local",
                "sqlite:replacement.db",
                "--replace",
            ],
        )
        .output()
        .unwrap(),
    );
    assert_eq!(cross_directory["error"]["code"], "INTERNAL_ERROR");
    assert_eq!(
        ConnectionConfig::load(&relative_config_path(&second_working_directory))
            .unwrap()
            .connections["local"]
            .dsn,
        "sqlite::memory:"
    );

    let replaced = stdout_json(
        relative_xdg_command(
            &second_working_directory,
            &[
                "--allow-write",
                "--confirm",
                &second_token,
                "conn",
                "add",
                "local",
                "sqlite:replacement.db",
                "--replace",
            ],
        )
        .output()
        .unwrap(),
    );
    assert_eq!(replaced["data"]["action"], "replaced");
    assert_eq!(
        ConnectionConfig::load(&relative_config_path(&second_working_directory))
            .unwrap()
            .connections["local"]
            .dsn,
        "sqlite:replacement.db"
    );
    assert_eq!(
        ConnectionConfig::load(&relative_config_path(&first_working_directory))
            .unwrap()
            .connections["local"]
            .dsn,
        "sqlite::memory:"
    );
    fs::remove_dir_all(root).ok();
}

#[test]
fn environment_managed_names_and_unregistered_schemes_are_never_persisted() {
    let root = root("env-boundary");
    let path = config_path(&root);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, "[connections.env-only]\ndsn = \"sqlite::memory:\"\n").unwrap();

    for args in [
        vec![
            "--allow-write",
            "conn",
            "add",
            "env-only",
            "sqlite::memory:",
            "--replace",
        ],
        vec!["--allow-write", "conn", "remove", "env-only"],
    ] {
        let error = stderr_json(run_with_env(
            &root,
            &args,
            "DBTOOL_CONN_ENV_ONLY",
            "postgres://user:environment-secret@127.0.0.1:1/db",
        ));
        assert_eq!(error["error"]["code"], "CONFIG_ERROR");
        assert!(!error.to_string().contains("environment-secret"));
    }
    assert!(ConnectionConfig::load(&path)
        .unwrap()
        .connections
        .contains_key("env-only"));

    let unsupported = stderr_json(run(
        &root,
        &[
            "--allow-write",
            "conn",
            "add",
            "unknown",
            "unknown-protocol://user:secret@127.0.0.1/db",
        ],
    ));
    assert_eq!(unsupported["error"]["code"], "UNSUPPORTED_SCHEME");
    assert!(!unsupported.to_string().contains("secret"));

    let invalid_name = stderr_json(run(
        &root,
        &[
            "--allow-write",
            "conn",
            "add",
            "Bad_Name",
            "sqlite::memory:",
        ],
    ));
    assert_eq!(invalid_name["error"]["code"], "CONFIG_ERROR");
    fs::remove_dir_all(root).ok();
}

#[test]
fn connection_list_applies_independent_item_and_byte_envelopes_without_secrets() {
    let item_root = root("list-item-envelope");
    let item_path = config_path(&item_root);
    fs::create_dir_all(item_path.parent().unwrap()).unwrap();
    let mut item_config = String::new();
    for index in 0..600 {
        let dsn = match index {
            0 => "nats://NATS_USERNAME_TOKEN_MARKER@localhost:4222".to_owned(),
            1 => "postgres://localhost/app?auth=UNKNOWN_AUTH_QUERY_MARKER".to_owned(),
            _ => format!("postgres://user:item-secret-{index}@localhost/app"),
        };
        item_config.push_str(&format!("[connections.c{index:04}]\ndsn = \"{dsn}\"\n"));
    }
    fs::write(&item_path, item_config).unwrap();
    let item_output = clean_command(&item_root, &["conn", "list"])
        .output()
        .unwrap();
    let item_raw = String::from_utf8_lossy(&item_output.stdout);
    assert!(!item_raw.contains("item-secret"));
    assert!(!item_raw.contains("NATS_USERNAME_TOKEN_MARKER"));
    assert!(!item_raw.contains("UNKNOWN_AUTH_QUERY_MARKER"));
    let item_json = stdout_json(item_output);
    assert_eq!(item_json["meta"]["truncated"], true);
    let retained = item_json["data"]["file_connections"]
        .as_array()
        .unwrap()
        .len();
    assert!(retained > 0 && retained < 600);

    let byte_root = root("list-byte-envelope");
    let byte_path = config_path(&byte_root);
    fs::create_dir_all(byte_path.parent().unwrap()).unwrap();
    let long_path = "p".repeat(12_000);
    let mut byte_config = String::new();
    for index in 0..50 {
        byte_config.push_str(&format!(
            "[connections.long{index:02}]\ndsn = \"postgres://user:byte-secret@localhost/{long_path}{index}\"\n"
        ));
    }
    fs::write(&byte_path, byte_config).unwrap();
    let byte_output = clean_command(&byte_root, &["conn", "list"])
        .output()
        .unwrap();
    assert!(byte_output.stdout.len() <= 256 * 1024);
    assert!(!String::from_utf8_lossy(&byte_output.stdout).contains("byte-secret"));
    let byte_json = stdout_json(byte_output);
    assert_eq!(byte_json["meta"]["truncated"], true);
    assert!(
        byte_json["data"]["file_connections"]
            .as_array()
            .unwrap()
            .len()
            < 50
    );
    for format in ["table", "ndjson"] {
        let output = clean_command(&byte_root, &["--format", format, "conn", "list"])
            .output()
            .unwrap();
        assert!(output.status.success(), "{format} output should succeed");
        assert!(output.stdout.len() <= 256 * 1024);
        let raw = String::from_utf8_lossy(&output.stdout);
        assert!(!raw.contains("byte-secret"));
        if format == "table" {
            assert!(raw.contains("# truncated"));
        } else {
            assert!(raw.contains("\"truncated\":true"));
        }
    }

    fs::remove_dir_all(item_root).ok();
    fs::remove_dir_all(byte_root).ok();
}

#[test]
fn connection_list_item_limit_distinguishes_exact_n_from_n_plus_one() {
    const MAX_ITEMS: usize = 512;

    let root = root("list-exact-items");
    let baseline = stdout_json(
        clean_command(&root, &["--limit", "512", "conn", "list"])
            .output()
            .unwrap(),
    );
    let scheme_count = baseline["data"]["supported_schemes"]
        .as_array()
        .unwrap()
        .len();
    assert!(scheme_count < MAX_ITEMS);

    let path = config_path(&root);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let file_count = MAX_ITEMS - scheme_count;
    let encode = |count: usize| {
        (0..count)
            .map(|index| format!("[connections.exact{index:04}]\ndsn = \"sqlite::memory:\"\n"))
            .collect::<String>()
    };
    fs::write(&path, encode(file_count)).unwrap();
    let exact = stdout_json(
        clean_command(&root, &["--limit", "512", "conn", "list"])
            .output()
            .unwrap(),
    );
    assert_eq!(exact["meta"]["truncated"], false);
    assert_eq!(
        exact["data"]["file_connections"].as_array().unwrap().len(),
        file_count
    );

    fs::write(&path, encode(file_count + 1)).unwrap();
    let plus_one = stdout_json(
        clean_command(&root, &["--limit", "512", "conn", "list"])
            .output()
            .unwrap(),
    );
    assert_eq!(plus_one["meta"]["truncated"], true);
    assert_eq!(
        plus_one["data"]["file_connections"]
            .as_array()
            .unwrap()
            .len(),
        file_count
    );
    fs::remove_dir_all(root).ok();
}

#[test]
fn connection_list_honors_small_caller_item_budget_at_n_and_n_plus_one() {
    let root = root("list-caller-items");
    let baseline = stdout_json(
        clean_command(&root, &["--limit", "512", "conn", "list"])
            .output()
            .unwrap(),
    );
    let scheme_count = baseline["data"]["supported_schemes"]
        .as_array()
        .unwrap()
        .len();
    let caller_limit = scheme_count + 2;
    assert!(caller_limit < 512);
    let caller_limit = caller_limit.to_string();

    let path = config_path(&root);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let encode = |count: usize| {
        (0..count)
            .map(|index| format!("[connections.small{index}]\ndsn = \"sqlite::memory:\"\n"))
            .collect::<String>()
    };
    fs::write(&path, encode(2)).unwrap();
    let exact = stdout_json(
        clean_command(&root, &["--limit", &caller_limit, "conn", "list"])
            .output()
            .unwrap(),
    );
    assert_eq!(exact["meta"]["truncated"], false);
    assert_eq!(
        exact["data"]["file_connections"].as_array().unwrap().len(),
        2
    );

    fs::write(&path, encode(3)).unwrap();
    let plus_one = stdout_json(
        clean_command(&root, &["--limit", &caller_limit, "conn", "list"])
            .output()
            .unwrap(),
    );
    assert_eq!(plus_one["meta"]["truncated"], true);
    assert_eq!(
        plus_one["data"]["file_connections"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    fs::remove_dir_all(root).ok();
}

#[test]
fn connection_list_honors_caller_byte_budget_at_n_and_n_minus_one_for_all_formats() {
    let root = root("list-caller-bytes");
    let path = config_path(&root);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(
        &path,
        "[connections.a]\ndsn = \"sqlite::memory:\"\n\
         [connections.b]\ndsn = \"postgres://user:BYTE_SECRET_MARKER@localhost/app\"\n",
    )
    .unwrap();

    for format in ["json", "table", "ndjson"] {
        let high = clean_command(
            &root,
            &[
                "--limit",
                "512",
                "--max-bytes",
                "262144",
                "--format",
                format,
                "conn",
                "list",
            ],
        )
        .output()
        .unwrap();
        assert!(high.status.success());
        let exact_bytes = high.stdout.len();
        assert!(exact_bytes > 1 && exact_bytes < 262_144);
        assert!(!String::from_utf8_lossy(&high.stdout).contains("BYTE_SECRET_MARKER"));

        let exact_budget = exact_bytes.to_string();
        let exact = clean_command(
            &root,
            &[
                "--limit",
                "512",
                "--max-bytes",
                &exact_budget,
                "--format",
                format,
                "conn",
                "list",
            ],
        )
        .output()
        .unwrap();
        assert!(exact.status.success(), "{format} exact budget should fit");
        assert_eq!(exact.stdout.len(), exact_bytes);
        let exact_raw = String::from_utf8_lossy(&exact.stdout);
        match format {
            "json" => assert_eq!(
                serde_json::from_slice::<Value>(&exact.stdout).unwrap()["meta"]["truncated"],
                false
            ),
            "table" => assert!(!exact_raw.contains("# truncated")),
            "ndjson" => assert!(exact_raw.contains("\"truncated\":false")),
            _ => unreachable!(),
        }

        let below_budget = (exact_bytes - 1).to_string();
        let below = clean_command(
            &root,
            &[
                "--limit",
                "512",
                "--max-bytes",
                &below_budget,
                "--format",
                format,
                "conn",
                "list",
            ],
        )
        .output()
        .unwrap();
        assert!(below.status.success(), "{format} N-1 should truncate");
        assert!(below.stdout.len() < exact_bytes);
        let below_raw = String::from_utf8_lossy(&below.stdout);
        assert!(!below_raw.contains("BYTE_SECRET_MARKER"));
        match format {
            "json" => assert_eq!(
                serde_json::from_slice::<Value>(&below.stdout).unwrap()["meta"]["truncated"],
                true
            ),
            "table" => assert!(below_raw.contains("# truncated")),
            "ndjson" => assert!(below_raw.contains("\"truncated\":true")),
            _ => unreachable!(),
        }
    }
    fs::remove_dir_all(root).ok();
}

#[test]
fn connection_list_rejects_invalid_or_too_small_caller_budgets_before_success_output() {
    let root = root("list-invalid-budget");
    let path = config_path(&root);
    fs::create_dir_all(&path).unwrap();

    let maximum_item_value = usize::MAX.to_string();
    for args in [
        ["--limit", "0", "conn", "list"],
        ["--limit", &maximum_item_value, "conn", "list"],
        ["--max-bytes", "0", "conn", "list"],
        ["--max-bytes", "16777217", "conn", "list"],
    ] {
        let output = clean_command(&root, &args).output().unwrap();
        let error = stderr_json(output);
        assert_eq!(error["error"]["code"], "CONFIG_ERROR");
        assert!(error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("budget"));
    }
    fs::remove_dir_all(&root).unwrap();

    let output = clean_command(&root, &["--max-bytes", "1", "conn", "list"])
        .output()
        .unwrap();
    assert!(output.stdout.is_empty());
    let error = stderr_json(output);
    assert_eq!(error["error"]["code"], "READ_BUDGET_EXCEEDED");
    assert_eq!(
        error["error"]["message"],
        "connection list fixed metadata exceeds the read bytes budget of 1"
    );
}

#[cfg(unix)]
#[test]
fn connection_list_escapes_config_path_control_sequences_in_all_formats() {
    let control_marker = "\u{1b}]0;DBTOOL_PATH_CONTROL_MARKER\u{7}\t\r";
    let mut root = root("path-control").into_os_string();
    root.push(control_marker);
    let root = PathBuf::from(root);

    for format in ["json", "table", "ndjson"] {
        let output = clean_command(&root, &["--format", format, "conn", "list"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{format} path rendering should succeed"
        );
        let raw = String::from_utf8(output.stdout).unwrap();
        assert!(!raw.contains(control_marker));
        for control in ['\u{1b}', '\u{7}', '\t', '\r'] {
            assert!(!raw.contains(control), "{format} leaked {control:?}");
        }
        assert!(raw.contains("DBTOOL_PATH_CONTROL_MARKER"));
        assert!(raw.contains("\\u{1b}"));
    }

    let added = clean_command(
        &root,
        &["--allow-write", "conn", "add", "local", "sqlite::memory:"],
    )
    .output()
    .unwrap();
    assert!(added.status.success());
    let added_raw = String::from_utf8(added.stdout).unwrap();
    assert!(!added_raw.contains(control_marker));
    for control in ['\u{1b}', '\u{7}', '\t', '\r'] {
        assert!(!added_raw.contains(control));
    }

    let confirmation = clean_command(
        &root,
        &[
            "--allow-write",
            "conn",
            "add",
            "local",
            "sqlite:replacement.db",
            "--replace",
        ],
    )
    .output()
    .unwrap();
    assert!(confirmation.stdout.is_empty());
    let confirmation_raw = String::from_utf8_lossy(&confirmation.stderr);
    assert!(!confirmation_raw.contains(control_marker));
    for control in ['\u{1b}', '\u{7}', '\t', '\r'] {
        assert!(!confirmation_raw.contains(control));
    }
    let error = stderr_json(confirmation);
    assert_eq!(error["error"]["code"], "CONFIRM_REQUIRED");
    let target = error["error"]["impact"]["target"].as_str().unwrap();
    assert!(target.contains("\\u{1b}"));
    fs::remove_dir_all(root).ok();
}

#[cfg(target_os = "linux")]
#[test]
fn connection_list_handles_non_utf8_config_paths_without_panicking() {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt};

    let mut bytes = root("non-utf8-path").into_os_string().into_vec();
    bytes.extend_from_slice(b"-\xff");
    let root = PathBuf::from(OsString::from_vec(bytes));
    let path = config_path(&root);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, b"").unwrap();

    let output = clean_command(&root, &["conn", "list"]).output().unwrap();
    let json = stdout_json(output);
    assert!(json["data"]["config_path"].as_str().is_some());
    fs::remove_dir_all(root).ok();
}

#[test]
fn connection_list_rejects_oversized_environment_catalog_without_echoing_values() {
    let root = root("env-list-envelope");
    let mut too_many = clean_command(&root, &["conn", "list"]);
    for index in 0..257 {
        too_many.env(
            format!("DBTOOL_CONN_ENV_{index}"),
            format!("redis://localhost/{index}"),
        );
    }
    let error = stderr_json(too_many.output().unwrap());
    assert_eq!(error["error"]["code"], "CONFIG_ERROR");
    assert!(!error.to_string().contains("redis://"));

    let marker = "ENV_VALUE_SECRET_MARKER";
    let oversized = format!("redis://user:{marker}@localhost/{}", "x".repeat(20_000));
    let mut oversized_command = clean_command(&root, &["conn", "list"]);
    oversized_command.env("DBTOOL_CONN_OVERSIZED", &oversized);
    let output = oversized_command.output().unwrap();
    let raw_error = String::from_utf8_lossy(&output.stderr);
    assert!(!raw_error.contains(marker));
    assert_eq!(stderr_json(output)["error"]["code"], "CONFIG_ERROR");
    fs::remove_dir_all(root).ok();
}

#[test]
fn raw_cli_dsn_is_bounded_and_redacted_before_connection_dispatch() {
    let root = root("oversized-raw-dsn");
    let marker = "RAW_CLI_DSN_SECRET_MARKER";
    let oversized = format!(
        "postgres://user:{marker}@localhost/{}",
        "x".repeat(dbtool_core::dsn::MAX_DSN_BYTES)
    );
    let output = clean_command(&root, &["--dsn", &oversized, "ping"])
        .output()
        .unwrap();
    let raw_error = String::from_utf8_lossy(&output.stderr);
    assert!(!raw_error.contains(marker));
    let error = stderr_json(output);
    assert_eq!(error["error"]["code"], "INVALID_DSN");
    assert!(error["error"]["message"]
        .as_str()
        .unwrap()
        .contains("size limit"));
}

#[test]
fn oversized_or_control_connection_references_fail_before_dispatch_without_echoing_input() {
    let root = root("invalid-connection-reference");
    let marker = "CONNECTION_REFERENCE_SECRET_MARKER";
    let oversized_name = format!("{marker}{}", "n".repeat(256));
    let control_name = format!("{marker}\nname");
    for value in [&oversized_name, &control_name] {
        let output = clean_command(&root, &["--conn", value, "ping"])
            .output()
            .unwrap();
        assert!(output.stdout.is_empty());
        assert!(!String::from_utf8_lossy(&output.stderr).contains(marker));
        assert_eq!(stderr_json(output)["error"]["code"], "CONFIG_ERROR");
    }

    let oversized_raw = format!(
        "nats://{marker}@localhost/{}",
        "x".repeat(dbtool_core::dsn::MAX_DSN_BYTES)
    );
    let output = clean_command(&root, &["--conn", &oversized_raw, "ping"])
        .output()
        .unwrap();
    assert!(output.stdout.is_empty());
    assert!(!String::from_utf8_lossy(&output.stderr).contains(marker));
    assert_eq!(stderr_json(output)["error"]["code"], "INVALID_DSN");
}

#[test]
fn destructive_safety_targets_redact_userinfo_query_and_fragment_for_all_input_modes() {
    let root = root("safety-target-redaction");
    let token_marker = "NATS_SAFETY_TARGET_TOKEN_MARKER";
    let query_marker = "NATS_SAFETY_TARGET_QUERY_MARKER";
    let cases = [
        (token_marker, format!("nats://{token_marker}@127.0.0.1:1")),
        (
            query_marker,
            format!("nats://127.0.0.1:1?auth={query_marker}#fragment-{query_marker}"),
        ),
    ];

    for (marker, dsn) in cases {
        for selector in ["--dsn", "--conn"] {
            let output = clean_command(
                &root,
                &[
                    selector,
                    &dsn,
                    "--allow-write",
                    "mq",
                    "delete",
                    "--kind",
                    "nats-jetstream",
                    "review-topic",
                ],
            )
            .output()
            .unwrap();
            assert!(output.stdout.is_empty());
            assert!(!String::from_utf8_lossy(&output.stderr).contains(marker));
            let error = stderr_json(output);
            assert_eq!(error["error"]["code"], "CONFIRM_REQUIRED");
            let target = error["error"]["impact"]["target"].as_str().unwrap();
            assert!(target.starts_with("dsn:nats://"));
            assert!(target.contains("***"));
            assert!(!target.contains(marker));
        }
    }
}
