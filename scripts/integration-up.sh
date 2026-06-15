#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

docker compose \
  -f "$ROOT/docker-compose.integration.yml" \
  -p "$DBTOOL_IT_PROJECT" \
  up -d --wait --wait-timeout "${DBTOOL_IT_WAIT_TIMEOUT:-180}" \
  postgres mysql redis mongo

docker compose \
  -f "$ROOT/docker-compose.integration.yml" \
  -p "$DBTOOL_IT_PROJECT" \
  ps
