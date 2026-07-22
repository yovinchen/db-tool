# Transfer Artifact Atomic Publication Evidence

Result: PASS

Run at (UTC): 2026-07-17

Environment: macOS arm64 workspace plus GitHub Actions `windows-latest`

## Contract

Connection configuration and transfer artifacts call the same
`dbtool_core::service::write_file_atomically` primitive. It creates one
same-directory exclusive temporary file, writes and syncs it, then publishes it:

- Unix uses rename followed by parent-directory sync.
- Windows uses `MoveFileExW` with `REPLACE_EXISTING | WRITE_THROUGH`.
- Failures before replacement remove the temporary file and preserve an existing target.
- A Unix parent-sync failure occurs after rename and therefore reports an error after the new
  target has already been published; this boundary is intentionally not described as rollback.

## Local verification

| Check | Result |
| --- | --- |
| `cargo test -p dbtool-core --lib service::atomic_file::tests` | PASS, 3/3 |
| `cargo test -p dbtool-cli --bin dbtool cmd::transfer::tests::artifact_publish_replaces_existing_target_and_leaves_no_temp_file -- --exact` | PASS, 1/1 |
| `cargo check -p dbtool-cli --no-default-features --features portable` | PASS |
| `.github/workflows/ci.yml` YAML parse | PASS |
| `git diff --check` | PASS |

The tests cover replacement of an existing regular file, cleanup of the same-directory temporary
file, preservation before an injected publish failure, and the transfer caller's replacement
wiring. Power-loss durability is not simulated locally.

## Windows PR gate

Every pull request now runs the core atomic tests and the exact transfer replacement bin test on
`windows-latest`. A separate matrix performs a portable release build/link for:

- `x86_64-pc-windows-msvc`;
- `aarch64-pc-windows-msvc`.

The gate passed for current implementation commit
`9529b518804b9363b5fdea094cfe400dfa2c7594` in
[workflow run 29599157939](https://github.com/yovinchen/db-tool/actions/runs/29599157939):

- [Windows tests](https://github.com/yovinchen/db-tool/actions/runs/29599157939/job/87946815132)
  ran the core atomic and exact transfer replacement tests successfully.
- [Windows x64 portable gate](https://github.com/yovinchen/db-tool/actions/runs/29599157939/job/87946814998)
  compiled and linked the release binary, then executed the portable SQLite core
  smoke and expected rejected-write probe successfully.
- [Windows ARM64 portable gate](https://github.com/yovinchen/db-tool/actions/runs/29599157939/job/87946815000)
  compiled and linked the release binary. Runtime was correctly omitted because
  GitHub's hosted Windows runner is x64.

The x64 run provides runtime evidence; ARM64 is compile/link-only. This proves
the checked-in Windows publication path without claiming ARM64 execution on an
incompatible runner.
