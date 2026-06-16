#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

if [[ "$(uname -m)" != "x86_64" && "${DBTOOL_IT_SQLSERVER_ALLOW_UNSUPPORTED_ARCH:-0}" != "1" ]]; then
  printf '%s\n' "SQL Server Linux containers are officially supported on x86_64 hosts; set DBTOOL_IT_SQLSERVER_ALLOW_UNSUPPORTED_ARCH=1 to force this run." >&2
  exit 78
fi

docker compose \
  -f "$ROOT/docker-compose.integration.yml" \
  -p "$DBTOOL_IT_PROJECT" \
  --profile sqlserver \
  up -d --wait --wait-timeout "${DBTOOL_IT_WAIT_TIMEOUT:-420}" \
  sqlserver

docker compose \
  -f "$ROOT/docker-compose.integration.yml" \
  -p "$DBTOOL_IT_PROJECT" \
  --profile sqlserver \
  ps
