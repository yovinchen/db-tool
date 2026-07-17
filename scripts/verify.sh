#!/usr/bin/env bash
set -euo pipefail

cargo fmt --all -- --check
cargo check --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
./scripts/smoke-core-flow.sh
./scripts/validate-db-completeness.sh
./scripts/validate-db-completeness-test.sh
./scripts/integration-external-requirement-test.sh
./scripts/validate-tidb-ha-drills.sh
./scripts/validate-final-goal.sh
