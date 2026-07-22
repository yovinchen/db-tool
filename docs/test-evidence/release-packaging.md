# Release Feature And Packaging Evidence

Result: MACOS_ARM64_SINGLE_ASSET_PASS

Run at: 2026-07-17 Asia/Shanghai

Host: macOS arm64; workspace version `1.0.0`; this historical evidence belongs
to tag `v1.0.0` and source commit
`193d32e5b38a4dfd6c11342809497d7df79d52fd`. Later main-branch hardening is not
part of that tagged artifact and must use a new version/tag if released.

## Final local gate

| Gate | Result |
| --- | --- |
| `./scripts/verify.sh` | PASS: fmt, workspace check, strict workspace Clippy, all workspace unit/integration/doctests, SQLite core smoke, 27-task DB manifest, eight TiDB HA drill manifests, final-goal validator |
| `RUSTFLAGS='-D deprecated' cargo check --workspace --lib --bins` | PASS: no unapproved first-party production legacy API call |
| `./scripts/validate-feature-matrix.sh` | PASS: minimal/default/portable/full/full-native CLI and TUI composition plus pure/native Kafka exclusivity |
| `./scripts/package-macos-arm64.sh` | PASS in 2m16s target build plus packaging/smoke |
| Mach-O identity | `file` and `lipo -archs` both proved 64-bit `arm64` only |
| archive content/runtime | binary, bash/zsh/fish completions and manpage present; extracted binary returned exact `dbtool 1.0.0` and passed SQLite core flow |
| checksum | generated `.tar.gz.sha256`; `shasum -a 256 -c` PASS |
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
| `validate-release-version.sh v1.0.0` | PASS |
| `validate-release-version.sh v1.0.1` | expected rejection PASS |
| `dbtool --format invalid ...` | Clap non-zero rejection before connection PASS |
| release `dbtool --version` | exact `dbtool 1.0.0` PASS |

## Selected target and identity boundary

The official release scope is exactly `aarch64-apple-darwin`. Both
`.github/workflows/release.yml` and `scripts/package-macos-arm64.sh` set the
target explicitly and resolve only one of these target-specific layouts:

```text
artifacts/dbtool-bin-<target>/dbtool[.exe]
artifacts/<target>/dbtool[.exe]
```

The former generic `artifacts/dbtool[.exe]` fallback is not accepted for package
payloads. The release archive preflights the selected native binary before it
writes output. Generic archive/npm/Python tools retain their strict multi-target
support for manual use but are not attached by the official release workflow.

| Negative check | Result |
| --- | --- |
| Unknown target in archive/npm selector | expected non-zero PASS |
| Duplicate target in archive-smoke/Python selector | expected non-zero PASS |
| Default six-target archive packaging with only host artifact | rejected before archive generation PASS |
| Default six-target npm/Python packaging with only host artifact | rejected before output reset/generation PASS |
| Default six-target archive smoke with only host archive | rejected PASS |

`DBTOOL_PACKAGE_TARGETS` still rejects unknown targets, empty list entries, and
duplicates. The official workflow now fixes it to `aarch64-apple-darwin`, so a
tag creates one archive instead of waiting for five unwanted platform binaries.

## Host release build and package installation

Command: `./scripts/package-macos-arm64.sh`

| Artifact path | Generate | Permission/content | Install and execute |
| --- | --- | --- | --- |
| macOS arm64 release binary | PASS | 34 portable schemes; version exact PASS | direct `--version` PASS |
| `dbtool-v1.0.0-aarch64-apple-darwin.tar.gz` | PASS, 11,117,865 bytes | executable + completions + manpage | extracted runtime and SQLite core smoke PASS |
| checksum sidecar | PASS | SHA-256 `e70dd45a6465a5ce3dad9c60ca1ab2594c4dbff9267ebe74b9c9f9bbe3b27447` | independent `shasum -a 256 -c` PASS |
| official release workflow | PASS ([run 29556274392](https://github.com/yovinchen/db-tool/actions/runs/29556274392)) | native `macos-latest` ARM64 build, one archive and its SHA-256 sidecar | published as GitHub Pre-release `v1.0.0` |

Release boundary: the current product decision intentionally publishes only
Apple Silicon macOS. Linux, Windows, Intel Mac, npm, and Python generators remain
available as optional tooling, but their absence from the official Release is
intentional and no longer an incomplete release claim.

## Published pre-release verification

GitHub Pre-release [`v1.0.0`](https://github.com/yovinchen/db-tool/releases/tag/v1.0.0)
was published from source commit `193d32e5b38a4dfd6c11342809497d7df79d52fd`
after CI run `29556079528` completed all 11 required jobs successfully. Release
run `29556274392` passed tag validation, native ARM64 build/runtime smoke,
archive packaging/runtime smoke, checksum generation, and publication.

The published assets were downloaded again from GitHub and verified independently:

| Published asset check | Result |
| --- | --- |
| attached asset set | exactly one `.tar.gz` and one `.sha256`; no other platform or package asset |
| archive size/digest | 10,912,066 bytes; SHA-256 `cc6831e788c518f7af5701478cc11698dc3d0c199daeeed3eb9a42ed3b45f44f` |
| sidecar verification | `shasum -a 256 -c` PASS against the downloaded archive |
| executable identity | Mach-O 64-bit `arm64` only; `lipo -archs` returned `arm64` |
| executable version | exact `dbtool 1.0.0` PASS |
| downloaded runtime | extracted archive passed the SQLite core flow |

## Current v1.0.1 local release candidate

Result: LOCAL_CANDIDATE_PASS

Run at: 2026-07-22 Asia/Shanghai

Source: version/packaging commit `8121487`; the following `e1f845b` changes only
the hosted Db2 workflow and does not alter the packaged Rust source. This local
candidate is not a claim that tag `v1.0.1` or its GitHub prerelease already
exists.

| Candidate check | Result |
| --- | --- |
| locked build/package | `./scripts/package-macos-arm64.sh v1.0.1` PASS |
| archive | `release-dist/macos-arm64/dbtool-v1.0.1-aarch64-apple-darwin.tar.gz`, 11,119,794 bytes |
| SHA-256 | `4832835e0d0a6255722ab6bc05a44c5dab8ae8d33574b19390151bb2516b35a8`; sidecar verification PASS |
| payload | binary, bash/zsh/fish completions, and `dbtool.1` manpage PASS |
| executable identity | Mach-O 64-bit thin `arm64`; `lipo -archs` returned `arm64` |
| executable version/runtime | exact `dbtool 1.0.1`; packaged SQLite core smoke PASS |

The candidate is linker-signed ad hoc (`Signature=adhoc`, no TeamIdentifier),
not Developer ID signed, notarized, or stapled. It is technically usable for
manual/test distribution on Apple Silicon, but those Apple trust-chain steps
remain required before describing it as a polished public macOS download.
