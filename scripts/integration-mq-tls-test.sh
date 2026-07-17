#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-mq-tls-prepare.sh"

"$ROOT/scripts/integration-mq-tls-up.sh"

if [[ "${DBTOOL_IT_KEEP_SERVICES:-0}" != "1" ]]; then
  trap '"$ROOT/scripts/integration-down.sh"' EXIT
fi

export DBTOOL_RUN_MQ_TLS_INTEGRATION=1

cargo test -p dbtool-cli --no-default-features --features messaging --test live_messaging mq_tls_live -- --nocapture
