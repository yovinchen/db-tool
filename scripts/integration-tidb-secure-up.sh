#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-tidb-secure-prepare.sh"

services=(
  tidb-secure-pd-1
  tidb-secure-pd-2
  tidb-secure-pd-3
  tidb-secure-tikv-1
  tidb-secure-tikv-2
  tidb-secure-1
  tidb-secure-2
)

docker compose \
  -f "$ROOT/docker-compose.integration.yml" \
  -p "$DBTOOL_IT_PROJECT" \
  --profile tidb-secure \
  up -d --wait --wait-timeout "${DBTOOL_IT_WAIT_TIMEOUT:-360}" \
  "${services[@]}"

docker compose \
  -f "$ROOT/docker-compose.integration.yml" \
  -p "$DBTOOL_IT_PROJECT" \
  --profile tidb-secure \
  ps "${services[@]}"

ping_with_timeout() {
  local dsn="$1"
  local timeout_secs="${DBTOOL_IT_TIDB_SECURE_PING_TIMEOUT:-10}"
  local elapsed=0
  local pid

  cargo run -q -p dbtool-cli -- --dsn "$dsn" ping >/dev/null 2>&1 &
  pid="$!"

  while kill -0 "$pid" 2>/dev/null; do
    if ((elapsed >= timeout_secs)); then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
      return 1
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done

  wait "$pid"
}

deadline="${DBTOOL_IT_TIDB_SECURE_READY_ATTEMPTS:-90}"
for attempt in $(seq 1 "$deadline"); do
  if ping_with_timeout "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" &&
    ping_with_timeout "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2"; then
    exit 0
  fi
  sleep 2
done

docker compose \
  -f "$ROOT/docker-compose.integration.yml" \
  -p "$DBTOOL_IT_PROJECT" \
  --profile tidb-secure \
  logs --tail 120 "${services[@]}"

echo "TiDB secure HA cluster did not become SQL-ready in time" >&2
exit 1
