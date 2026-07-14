#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-opensearch-security-prepare.sh"

"$ROOT/scripts/integration-opensearch-security-up.sh"

if [[ "${DBTOOL_IT_KEEP_SERVICES:-0}" != "1" ]]; then
  trap '"$ROOT/scripts/integration-down.sh"' EXIT
fi

export DBTOOL_RUN_OPENSEARCH_SECURITY_INTEGRATION=1

cargo test -p dbtool-cli --test live_observability opensearch_security_tls_live_index_search_and_list -- --nocapture
