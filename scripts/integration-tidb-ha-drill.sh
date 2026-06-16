#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-tidb-secure-prepare.sh"

compose() {
  docker compose \
    -f "$ROOT/docker-compose.integration.yml" \
    -p "$DBTOOL_IT_PROJECT" \
    --profile tidb-secure \
    "$@"
}

stopped_services=()

restore_stopped_services() {
  if ((${#stopped_services[@]} == 0)); then
    return 0
  fi

  compose start "${stopped_services[@]}" >/dev/null || true
}

cleanup() {
  if [[ "${DBTOOL_IT_KEEP_SERVICES:-0}" == "1" ]]; then
    restore_stopped_services
  else
    "$ROOT/scripts/integration-down.sh"
  fi
}

trap cleanup EXIT

run_with_timeout() {
  local timeout_secs="$1"
  shift
  local elapsed=0
  local pid

  "$@" &
  pid="$!"

  while kill -0 "$pid" 2>/dev/null; do
    if ((elapsed >= timeout_secs)); then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
      return 124
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done

  wait "$pid"
}

dbtool_cli() {
  cargo run -q -p dbtool-cli -- "$@"
}

ping_dsn() {
  local dsn="$1"
  local timeout_secs="${DBTOOL_IT_TIDB_SECURE_PING_TIMEOUT:-10}"

  run_with_timeout "$timeout_secs" dbtool_cli --dsn "$dsn" ping >/dev/null 2>&1
}

wait_for_ping() {
  local name="$1"
  local dsn="$2"
  local attempts="${DBTOOL_IT_TIDB_HA_DRILL_READY_ATTEMPTS:-60}"

  for attempt in $(seq 1 "$attempts"); do
    if ping_dsn "$dsn"; then
      return 0
    fi
    sleep 2
  done

  echo "TiDB HA drill: $name did not become reachable in time" >&2
  compose logs --tail 120 tidb-secure-1 tidb-secure-2 >&2 || true
  return 1
}

expect_ping_failure() {
  local name="$1"
  local dsn="$2"

  if ping_dsn "$dsn"; then
    echo "TiDB HA drill: $name was expected to be unavailable but still accepted ping" >&2
    return 1
  fi
}

sql_exec() {
  local dsn="$1"
  local sql="$2"
  local output
  local status
  local token

  set +e
  output="$(dbtool_cli --dsn "$dsn" --allow-write sql exec "$sql" 2>&1)"
  status=$?
  set -e

  if ((status == 0)); then
    return 0
  fi

  if [[ "$output" =~ \"confirm_token\":\"([^\"]+)\" ]]; then
    token="${BASH_REMATCH[1]}"
    dbtool_cli --dsn "$dsn" --allow-write --confirm "$token" sql exec "$sql" >/dev/null
    return 0
  fi

  echo "$output" >&2
  return "$status"
}

assert_query_contains() {
  local name="$1"
  local dsn="$2"
  local sql="$3"
  local expected="$4"
  local output

  output="$(dbtool_cli --dsn "$dsn" --format table sql query "$sql")"
  if ! grep -Fq "$expected" <<<"$output"; then
    echo "TiDB HA drill: $name did not contain expected value: $expected" >&2
    echo "$output" >&2
    return 1
  fi
}

assert_identifier() {
  local value="$1"
  local label="$2"

  if [[ ! "$value" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    echo "TiDB HA drill: invalid $label identifier: $value" >&2
    return 1
  fi
}

stop_service() {
  local service="$1"

  compose stop "$service"
  stopped_services+=("$service")
}

start_service() {
  local service="$1"

  compose start "$service"
}

"$ROOT/scripts/integration-tidb-secure-up.sh"

database="$DBTOOL_IT_TIDB_SECURE_DB"
assert_identifier "$database" "database"

table="dbtool_tidb_ha_drill_$(date +%s)_$$"
qualified_table="$database.$table"

echo "TiDB HA drill: preparing $qualified_table through SQL node 1"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "create database if not exists $database"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "create table $qualified_table (id bigint primary key, note varchar(64) not null)"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "insert into $qualified_table (id, note) values (1, 'node1-before-stop')"
assert_query_contains "node 2 baseline read" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2" "select note from $qualified_table where id = 1" "node1-before-stop"

echo "TiDB HA drill: stopping SQL node 1 and validating SQL node 2"
stop_service tidb-secure-1
expect_ping_failure "SQL node 1" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1"
wait_for_ping "SQL node 2" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2" "insert into $qualified_table (id, note) values (2, 'node2-while-node1-down')"
assert_query_contains "node 2 write while node 1 is down" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2" "select note from $qualified_table where id = 2" "node2-while-node1-down"

echo "TiDB HA drill: restarting SQL node 1"
start_service tidb-secure-1
wait_for_ping "SQL node 1 after restart" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1"
assert_query_contains "node 1 read after restart" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "select note from $qualified_table where id = 2" "node2-while-node1-down"

echo "TiDB HA drill: stopping SQL node 2 and validating SQL node 1"
stop_service tidb-secure-2
expect_ping_failure "SQL node 2" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2"
wait_for_ping "SQL node 1" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "insert into $qualified_table (id, note) values (3, 'node1-while-node2-down')"
assert_query_contains "node 1 write while node 2 is down" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "select note from $qualified_table where id = 3" "node1-while-node2-down"

echo "TiDB HA drill: restarting SQL node 2"
start_service tidb-secure-2
wait_for_ping "SQL node 2 after restart" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2"
assert_query_contains "node 2 read after restart" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2" "select note from $qualified_table where id = 3" "node1-while-node2-down"

echo "TiDB secure HA failover drill passed"
