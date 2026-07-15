# Release Feature And Packaging Evidence

Result: LOCAL_PASS_WITH_CROSS_TARGET_BOUNDARY

Run at (UTC): 2026-07-15T16:54:43Z

Host: macOS arm64; workspace version `0.1.0`

## Feature composition

Command: `./scripts/validate-feature-matrix.sh`

| Build | Result | Verified boundary |
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

## Release build and package installation

Command: `cargo build --release -p dbtool-cli --no-default-features --features portable`

| Artifact path | Generate | Permission/content | Install and execute |
| --- | --- | --- | --- |
| macOS arm64 release binary | PASS | 34 portable schemes; version exact PASS | direct `--version` PASS |
| npm platform packages (six manifests) | PASS | four Unix tarballs contain `package/bin/dbtool` mode 0755 PASS | real macOS arm64 platform tarball + main wrapper installed with isolated npm cache; `dbtool --version` PASS |
| Python wheels (six manifests) | PASS | embedded binary mode 0755 PASS | real macOS arm64 wheel installed into a fresh venv with `--no-index`; `dbtool --version` PASS |
| release workflow assets | YAML PASS | archives, `.tgz`, and `.whl` all feed GitHub Release; npm and musllinux jobs contain install/run gates | executable only on a tag runner |

Cross-target boundary: this host built and executed only the native macOS arm64
binary. The other five package manifests and Unix permission bits were tested
using disposable copies of the host binary; their real target binaries remain
the responsibility of the six-target GitHub Actions matrix and must not be
reported as locally executed.
