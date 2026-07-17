#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

docker compose \
  -f "$ROOT/docker-compose.integration.yml" \
  -p "$DBTOOL_IT_PROJECT" \
  --profile scylla \
  up -d --wait --wait-timeout "${DBTOOL_IT_SCYLLA_WAIT_TIMEOUT:-600}" \
  scylla

docker compose \
  -f "$ROOT/docker-compose.integration.yml" \
  -p "$DBTOOL_IT_PROJECT" \
  --profile scylla \
  ps
