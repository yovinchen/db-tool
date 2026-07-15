#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

"$ROOT/scripts/integration-cassandra-up.sh"

if [[ "${DBTOOL_IT_KEEP_SERVICES:-0}" != "1" ]]; then
  trap '"$ROOT/scripts/integration-down.sh"' EXIT
fi

export DBTOOL_RUN_CASSANDRA_INTEGRATION=1

cargo test -p dbtool-cli --features full --test live_services cassandra_live -- --nocapture
cargo test -p dbtool-cli --features full --test bounded_sql cassandra_live_streams_one_probe_row_for_paged_results -- --exact --nocapture
