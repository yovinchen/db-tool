#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

if [[ "${DBTOOL_IT_KEEP_SERVICES:-0}" != "1" ]]; then
  trap '"$ROOT/scripts/integration-down.sh"' EXIT
fi

"$ROOT/scripts/integration-tidb-up.sh"

export DBTOOL_RUN_TIDB_INTEGRATION=1

cargo test -p dbtool-cli --test live_services tidb_compat -- --nocapture
cargo test -p dbtool-cli --test named_sql_atomic tidb_named_product -- --nocapture
