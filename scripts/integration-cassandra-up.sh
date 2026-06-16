#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

docker compose \
  -f "$ROOT/docker-compose.integration.yml" \
  -p "$DBTOOL_IT_PROJECT" \
  --profile cassandra \
  up -d --wait --wait-timeout "${DBTOOL_IT_CASSANDRA_WAIT_TIMEOUT:-600}" \
  cassandra

docker compose \
  -f "$ROOT/docker-compose.integration.yml" \
  -p "$DBTOOL_IT_PROJECT" \
  --profile cassandra \
  ps
