#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

"$ROOT/scripts/integration-sqlserver-up.sh"

if [[ "${DBTOOL_IT_KEEP_SERVICES:-0}" != "1" ]]; then
  trap '"$ROOT/scripts/integration-down.sh"' EXIT
fi

export DBTOOL_RUN_SQLSERVER_INTEGRATION=1

printf '%s\n' "==> sqlserver adapter budgets, catalog bounds, and protocol mapping (service-free)"
cargo test -p adapter-sqlserver --lib -- --nocapture

printf '%s\n' "==> sqlserver product lifecycle, typed values, bounded reads, and catalog metadata (live)"
cargo test -p dbtool-cli --features full --test live_services sqlserver_live -- --nocapture
