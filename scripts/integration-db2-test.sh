#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

"$ROOT/scripts/integration-db2-up.sh"

if [[ "${DBTOOL_IT_KEEP_SERVICES:-0}" != "1" ]]; then
  trap '"$ROOT/scripts/integration-down.sh"' EXIT
fi

export DBTOOL_RUN_DB2_INTEGRATION=1

# The IBM Data Server Driver for ODBC and CLI must be installed and the driver
# must be registered in /etc/odbcinst.ini (Linux) or the system registry
# (Windows) before running this test.
cargo test -p dbtool-cli --features full --test live_services db2_live -- --nocapture
