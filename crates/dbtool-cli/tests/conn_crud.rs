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
