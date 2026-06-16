#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-tidb-secure-prepare.sh"

pd_services=(
  tidb-secure-pd-1
  tidb-secure-pd-2
  tidb-secure-pd-3
)

tikv_services=(
  tidb-secure-tikv-1
  tidb-secure-tikv-2
)

tidb_services=(
  tidb-secure-1
  tidb-secure-2
)

services=("${pd_services[@]}" "${tikv_services[@]}" "${tidb_services[@]}")

compose() {
  docker compose \
    -f "$ROOT/docker-compose.integration.yml" \
    -p "$DBTOOL_IT_PROJECT" \
    --profile tidb-secure \
    "$@"
}

phase_sleep() {
  local label="$1"
  local seconds="$2"

  if ((seconds <= 0)); then
    return 0
  fi

  echo "TiDB secure HA: waiting ${seconds}s for $label"
  sleep "$seconds"
}

start_phase() {
  local label="$1"
  shift

  echo "TiDB secure HA: starting $label"
  if ! compose up -d --wait --wait-timeout "${DBTOOL_IT_WAIT_TIMEOUT:-360}" "$@"; then
    echo "TiDB secure HA: $label startup failed" >&2
    compose ps "${services[@]}" >&2 || true
    compose logs --tail "${DBTOOL_IT_TIDB_SECURE_FAILURE_LOG_LINES:-160}" "${services[@]}" >&2 || true
    return 1
  fi
  compose ps "$@"
}

start_phase "PD" "${pd_services[@]}"
phase_sleep "PD peer election" "${DBTOOL_IT_TIDB_SECURE_PD_BOOTSTRAP_DELAY:-8}"

start_phase "TiKV" "${tikv_services[@]}"
phase_sleep "TiKV cluster bootstrap" "${DBTOOL_IT_TIDB_SECURE_TIKV_BOOTSTRAP_DELAY:-30}"

start_phase "TiDB SQL" "${tidb_services[@]}"
compose ps "${services[@]}"

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

compose logs --tail 120 "${services[@]}"

echo "TiDB secure HA cluster did not become SQL-ready in time" >&2
exit 1
