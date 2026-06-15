#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-tidb-secure-prepare.sh"

if [[ "${DBTOOL_IT_KEEP_SERVICES:-0}" != "1" ]]; then
  trap '"$ROOT/scripts/integration-down.sh"' EXIT
fi

"$ROOT/scripts/integration-tidb-secure-up.sh"

export DBTOOL_RUN_TIDB_SECURE_INTEGRATION=1

cargo test -p dbtool-cli --test live_services tidb_secure -- --nocapture
