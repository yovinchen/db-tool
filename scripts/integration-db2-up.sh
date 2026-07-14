#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

# IBM Db2 Community Edition takes 2-10 minutes to initialise the database on
# first start. The healthcheck polls every 15 s for up to 15 min (60 retries).
docker compose \
  -f "$ROOT/docker-compose.integration.yml" \
  -p "$DBTOOL_IT_PROJECT" \
  --profile db2 \
  up -d --wait --wait-timeout "${DBTOOL_IT_DB2_WAIT_TIMEOUT:-900}" \
  db2

docker compose \
  -f "$ROOT/docker-compose.integration.yml" \
  -p "$DBTOOL_IT_PROJECT" \
  --profile db2 \
  ps
