#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

if [[ -z "$DBTOOL_IT_REDSHIFT_DSN" ]]; then
  if [[ "${DBTOOL_IT_REQUIRE_EXTERNAL:-0}" == "1" ]]; then
    echo "dbtool Redshift smoke failed: DBTOOL_IT_REDSHIFT_DSN is required when DBTOOL_IT_REQUIRE_EXTERNAL=1." >&2
    exit 2
  fi

  cat <<'EOF'
dbtool Redshift smoke SKIP: DBTOOL_IT_REDSHIFT_DSN is not set.

Set DBTOOL_IT_REDSHIFT_DSN to run it, for example:
  DBTOOL_IT_REDSHIFT_DSN='redshift://user:pass@host:5439/dev?sslmode=require'

No Redshift credentials are committed by this script.
EOF
  exit 0
fi

export DBTOOL_RUN_REDSHIFT_INTEGRATION=1

cargo test -p dbtool-cli --test live_services redshift_external_sql_lifecycle_and_typed_values -- --nocapture
