#!/usr/bin/env bash
set -euo pipefail

cargo fmt --all -- --check
cargo check --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
./scripts/smoke-core-flow.sh
node ./scripts/package-npm-test.mjs
./scripts/validate-container-image-pins.sh
./scripts/validate-container-image-pins-test.sh
./scripts/validate-db-completeness.sh
./scripts/validate-db-completeness-test.sh
./scripts/integration-external-requirement-test.sh
./scripts/validate-tidb-ha-drills.sh
./scripts/validate-final-goal.sh
