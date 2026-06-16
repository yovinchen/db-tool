#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

docker compose \
  -f "$ROOT/docker-compose.integration.yml" \
  -p "$DBTOOL_IT_PROJECT" \
  --profile fixture-images \
  --profile messaging \
  --profile messaging-tls \
  --profile compat \
  --profile compat-extra \
  --profile pg-compat \
  --profile sqlserver \
  --profile cassandra \
  --profile tidb \
  --profile tidb-secure \
  --profile tidb-tiproxy \
  --profile observability \
  down -v --remove-orphans
