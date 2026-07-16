# Release Feature And Packaging Evidence

Result: LOCAL_HOST_FINAL_PASS_WITH_STRICT_CROSS_TARGET_BOUNDARY

Run at (UTC): 2026-07-16T13:46:46Z

Host: macOS arm64; workspace version `0.1.0`; verified source commit
`687967f` (the following evidence-only commit changes no Rust or packaging code)

## Final local gate

| Gate | Result |
| --- | --- |
| `./scripts/verify.sh` | PASS: fmt, workspace check, strict workspace Clippy, all workspace unit/integration/doctests, SQLite core smoke, 27-task DB manifest, eight TiDB HA drill manifests, final-goal validator |
| `RUSTFLAGS='-D deprecated' cargo check --workspace --lib --bins` | PASS: no unapproved first-party production legacy API call |
| `./scripts/validate-feature-matrix.sh` | PASS: minimal/default/portable/full/full-native CLI and TUI composition plus pure/native Kafka exclusivity |
| `cargo build --release -p dbtool-cli --no-default-features --features portable` | PASS in 1m15s |
| archive + npm + Python package generation and archive smoke | PASS for the selected native `aarch64-apple-darwin` target |
| isolated npm offline install and Python `--no-index` venv install | both returned exact `dbtool 0.1.0` |
| `./scripts/smoke-docker-image.sh` | PASS: cold Docker build, image `sha256:88e54b25e32f25ae44f0bbf9b14a4641792cf6d6450b7508f9d5f1860fcea6b6`, non-root runtime SQLite core smoke |

The first sandboxed `verify.sh` attempt could not bind the AMQP fixture's local
temporary listener (`Operation not permitted`). The complete script was rerun
with host loopback permission and passed; this was a sandbox boundary, not a
suppressed test failure.

## Feature composition boundary

The feature matrix was rerun after IF-T75 and IF-T78 closed.

| Build | Last verified result | Verified boundary |
| --- | --- | --- |
| CLI `--no-default-features` | PASS | no adapter dependency and zero registered schemes |
| default CLI/TUI | PASS | existing default composition still compiles |
| `portable` CLI | PASS | 34 registered schemes; includes SQL/SQL Server/CQL/KV/Document/Search/Timeseries/Kafka/AMQP/NATS; excludes `db2`, `ibmdb2`, `as400` and `adapter-db2` |
| `full` CLI/TUI | PASS | pure Kafka backend only; includes Db2 ODBC |
| `full-native` CLI/TUI | PASS | native Kafka backend only; same 37 schemes as `full` |

## Version and CLI strictness

| Check | Result |
| --- | --- |
| `validate-release-version.sh v0.1.0` | PASS |
| `validate-release-version.sh v0.1.1` | expected rejection PASS |
| `dbtool --format invalid ...` | Clap non-zero rejection before connection PASS |
| release `dbtool --version` | exact `dbtool 0.1.0` PASS |

## Target identity and failure paths

All four entry points now default to all six release targets and resolve only
one of these target-specific layouts:

```text
artifacts/dbtool-bin-<target>/dbtool[.exe]
artifacts/<target>/dbtool[.exe]
```

The former generic `artifacts/dbtool[.exe]` fallback is not accepted for package
payloads. Before removing or creating output, npm/Python packaging resolves every
selected binary; release archive packaging likewise preflights the complete
selection.

| Negative check | Result |
| --- | --- |
| Unknown target in archive/npm selector | expected non-zero PASS |
| Duplicate target in archive-smoke/Python selector | expected non-zero PASS |
| Default six-target archive packaging with only host artifact | rejected before archive generation PASS |
| Default six-target npm/Python packaging with only host artifact | rejected before output reset/generation PASS |
| Default six-target archive smoke with only host archive | rejected PASS |

`DBTOOL_PACKAGE_TARGETS` is an explicit local-test filter. Unknown targets, empty
list entries, and duplicates are rejected; the GitHub release workflow does not
set it and therefore continues to require all six targets.

## Host release build and package installation

Command: `cargo build --release -p dbtool-cli --no-default-features --features portable`

| Artifact path | Generate | Permission/content | Install and execute |
| --- | --- | --- | --- |
| macOS arm64 release binary | PASS | 34 portable schemes; version exact PASS | direct `--version` PASS |
| npm macOS arm64 platform package + main wrapper | PASS | copied target-specific binary is mode 0755; completions/manpage present | both local `.tgz` files installed offline with isolated npm cache; wrapper returned exact `dbtool 0.1.0` |
| Python macOS arm64 wheel | PASS | target-specific binary embedded as mode 0755; completions/manpage present | installed into a fresh venv with `--no-index`; console entry returned exact `dbtool 0.1.0` |
| release workflow assets | YAML PASS | archives, `.tgz`, and `.whl` all feed GitHub Release; npm and musllinux jobs contain install/run gates | executable only on a tag runner |

Cross-target boundary: this host built, packaged, installed, and executed only
the native macOS arm64 binary. No package or manifest was generated for the
other five targets. Their real binaries and install gates remain the
responsibility of the six-target GitHub Actions matrix and must not be reported
as locally executed.
