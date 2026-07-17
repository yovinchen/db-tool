#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

"$ROOT/scripts/integration-scylla-up.sh"

if [[ "${DBTOOL_IT_KEEP_SERVICES:-0}" != "1" ]]; then
  trap '"$ROOT/scripts/integration-down.sh"' EXIT
fi

export DBTOOL_RUN_SCYLLA_INTEGRATION=1

cargo test -p dbtool-cli --features full --test live_services cql_live -- --nocapture
cargo test -p dbtool-cli --features full --test bounded_sql cql_live_streams_one_probe_row_for_paged_results -- --exact --nocapture
cargo test -p dbtool-cli --features full --test live_bounded_cql -- --nocapture
DBTOOL_IT_CASSANDRA_DSN="$DBTOOL_IT_SCYLLA_DSN" \
  cargo test -p adapter-cassandra --lib cassandra_live_budgeted_cql_rejects_before_write_and_cleans_keyspace -- --nocapture
