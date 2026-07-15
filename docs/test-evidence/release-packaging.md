# Release Feature And Packaging Evidence

Result: LOCAL_HOST_PASS_WITH_STRICT_CROSS_TARGET_BOUNDARY

Run at (UTC): 2026-07-15T20:32:52Z

Host: macOS arm64; workspace version `0.1.0`

## Feature composition boundary

The feature matrix is unchanged by this packaging-only slice. Its final rerun is
tracked by IF-T51 after all interface work is complete.

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
