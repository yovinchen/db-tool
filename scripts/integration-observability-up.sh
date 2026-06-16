#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"
source "$ROOT/scripts/integration-observability-tls-prepare.sh"

docker compose \
  -f "$ROOT/docker-compose.integration.yml" \
  -p "$DBTOOL_IT_PROJECT" \
  --profile observability \
  up -d --wait --wait-timeout "${DBTOOL_IT_WAIT_TIMEOUT:-300}" \
  opensearch opensearch-tls prometheus

docker compose \
  -f "$ROOT/docker-compose.integration.yml" \
  -p "$DBTOOL_IT_PROJECT" \
  --profile observability \
  ps opensearch opensearch-tls prometheus
