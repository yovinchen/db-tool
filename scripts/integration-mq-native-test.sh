#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

"$ROOT/scripts/integration-mq-up.sh"

if [[ "${DBTOOL_IT_KEEP_SERVICES:-0}" != "1" ]]; then
  trap '"$ROOT/scripts/integration-down.sh"' EXIT
fi

export DBTOOL_RUN_MQ_INTEGRATION=1

cargo test -p dbtool-cli --no-default-features --features full-native --test live_messaging -- --nocapture
cargo test -p dbtool-cli --no-default-features --features full-native --test bounded_messaging -- --test-threads=1 --nocapture
