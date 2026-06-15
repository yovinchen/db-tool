#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

"$ROOT/scripts/integration-compat-up.sh"

if [[ "${DBTOOL_IT_KEEP_SERVICES:-0}" != "1" ]]; then
  trap '"$ROOT/scripts/integration-down.sh"' EXIT
fi

export DBTOOL_RUN_COMPAT_INTEGRATION=1
export DBTOOL_RUN_MARIADB_COMPAT=1
export DBTOOL_RUN_VALKEY_COMPAT=1

if [[ "${DBTOOL_IT_COMPAT_EXTRA:-0}" == "1" ]]; then
  export DBTOOL_RUN_KEYDB_COMPAT=1
  export DBTOOL_RUN_DRAGONFLY_COMPAT=1
fi

cargo test -p dbtool-cli --test live_services compat_live -- --nocapture
