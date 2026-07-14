#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-opensearch-security-prepare.sh"

docker compose \
  -f "$ROOT/docker-compose.integration.yml" \
  -p "$DBTOOL_IT_PROJECT" \
  --profile opensearch-security \
  up -d --wait --wait-timeout "${DBTOOL_IT_WAIT_TIMEOUT:-300}" \
  opensearch-security

docker compose \
  -f "$ROOT/docker-compose.integration.yml" \
  -p "$DBTOOL_IT_PROJECT" \
  --profile opensearch-security \
  ps opensearch-security
