# Transfer Artifact Atomic Publication Evidence

Result: IMPLEMENTATION_PASS_WINDOWS_RUN_PENDING

Run at (UTC): 2026-07-16

Environment: macOS arm64 workspace; Windows semantics delegated to the checked-in PR gate

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

This file does not claim those commands ran for the current unpushed SHA. Before changing IF-T68
from pending evidence to fully verified, record the Windows Actions run URL and commit SHA here;
x64 supplies runtime evidence, while arm64 is compile/link-only on the x64 runner.
