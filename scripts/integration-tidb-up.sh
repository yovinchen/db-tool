#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

docker compose \
  -f "$ROOT/docker-compose.integration.yml" \
  -p "$DBTOOL_IT_PROJECT" \
  --profile tidb \
  up -d --wait --wait-timeout "${DBTOOL_IT_WAIT_TIMEOUT:-360}" \
  tidb-pd tidb-tikv tidb

docker compose \
  -f "$ROOT/docker-compose.integration.yml" \
  -p "$DBTOOL_IT_PROJECT" \
  --profile tidb \
  ps tidb-pd tidb-tikv tidb
