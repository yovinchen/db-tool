#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

docker compose \
  -f "$ROOT/docker-compose.integration.yml" \
  -p "$DBTOOL_IT_PROJECT" \
  --profile messaging \
  up -d --wait --wait-timeout "${DBTOOL_IT_WAIT_TIMEOUT:-240}" \
  redis kafka rabbitmq nats

docker compose \
  -f "$ROOT/docker-compose.integration.yml" \
  -p "$DBTOOL_IT_PROJECT" \
  --profile messaging \
  ps redis kafka rabbitmq nats
