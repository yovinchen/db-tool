#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

profiles=(--profile compat)
services=(mariadb valkey)

if [[ "${DBTOOL_IT_COMPAT_EXTRA:-0}" == "1" ]]; then
  profiles+=(--profile compat-extra)
  services+=(keydb dragonfly)
fi

docker compose \
  -f "$ROOT/docker-compose.integration.yml" \
  -p "$DBTOOL_IT_PROJECT" \
  "${profiles[@]}" \
  up -d --wait --wait-timeout "${DBTOOL_IT_WAIT_TIMEOUT:-240}" \
  "${services[@]}"

docker compose \
  -f "$ROOT/docker-compose.integration.yml" \
  -p "$DBTOOL_IT_PROJECT" \
  "${profiles[@]}" \
  ps "${services[@]}"
