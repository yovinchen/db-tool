#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-mq-tls-prepare.sh"

docker compose \
  -f "$ROOT/docker-compose.integration.yml" \
  -p "$DBTOOL_IT_PROJECT" \
  --profile messaging-tls \
  up -d --wait --wait-timeout "${DBTOOL_IT_WAIT_TIMEOUT:-240}" \
  rabbitmq-tls nats-tls

docker compose \
  -f "$ROOT/docker-compose.integration.yml" \
  -p "$DBTOOL_IT_PROJECT" \
  --profile messaging-tls \
  ps rabbitmq-tls nats-tls
