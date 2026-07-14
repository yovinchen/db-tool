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

pd_services=(
  tidb-secure-pd-1
  tidb-secure-pd-2
  tidb-secure-pd-3
)

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
  cargo run -q -p dbtool-cli -- \
    --request-timeout "${DBTOOL_IT_TIDB_PD_DRILL_REQUEST_TIMEOUT:-20s}" \
    --deadline "${DBTOOL_IT_TIDB_PD_DRILL_DEADLINE:-30s}" \
    "$@"
}

ping_dsn() {
  local dsn="$1"
  local timeout_secs="${DBTOOL_IT_TIDB_SECURE_PING_TIMEOUT:-10}"

  run_with_timeout "$timeout_secs" dbtool_cli --dsn "$dsn" ping >/dev/null 2>&1
}

wait_for_ping() {
  local name="$1"
  local dsn="$2"
  local attempts="${DBTOOL_IT_TIDB_PD_DRILL_READY_ATTEMPTS:-90}"

  for attempt in $(seq 1 "$attempts"); do
    if ping_dsn "$dsn"; then
      return 0
    fi
    sleep 2
  done

  echo "TiDB PD drill: $name did not become reachable in time" >&2
  compose logs --tail 160 tidb-secure-1 tidb-secure-2 "${pd_services[@]}" >&2 || true
  return 1
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
    echo "TiDB PD drill: $name did not contain expected value: $expected" >&2
    echo "$output" >&2
    return 1
  fi
}

assert_complete_fixture() {
  local dsn="$1"
  local table="$2"
  local output

  output="$(dbtool_cli --dsn "$dsn" sql query "select id, note from $table order by id")"
  printf '%s' "$output" | python3 -c '
import json,sys
data=json.load(sys.stdin)
assert data["data"]["rows"] == [
    [1, "pd-baseline"],
    [2, "write-through-node1-while-tidb-secure-pd-1-down"],
    [3, "write-through-node2-while-tidb-secure-pd-1-down"],
    [4, "write-through-node1-while-tidb-secure-pd-2-down"],
    [5, "write-through-node2-while-tidb-secure-pd-2-down"],
    [6, "write-through-node1-while-tidb-secure-pd-3-down"],
    [7, "write-through-node2-while-tidb-secure-pd-3-down"],
], data
'
}

assert_identifier() {
  local value="$1"
  local label="$2"

  if [[ ! "$value" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    echo "TiDB PD drill: invalid $label identifier: $value" >&2
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
  compose up -d --wait --wait-timeout "${DBTOOL_IT_WAIT_TIMEOUT:-360}" "$service"
}

"$ROOT/scripts/integration-tidb-secure-up.sh"

database="$DBTOOL_IT_TIDB_SECURE_DB"
assert_identifier "$database" "database"

table="dbtool_it_tidb_pd_drill_$(date +%s)_$$"
qualified_table="$database.$table"
row_id=1
echo "TiDB PD N-1 resource: table=$qualified_table"

echo "TiDB PD drill: preparing $qualified_table through SQL node 1"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "create database if not exists $database"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "create table $qualified_table (id bigint primary key, note varchar(96) not null)"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "insert into $qualified_table (id, note) values ($row_id, 'pd-baseline')"
assert_query_contains "node 2 baseline read" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2" "select note from $qualified_table where id = $row_id" "pd-baseline"

for pd_service in "${pd_services[@]}"; do
  echo "TiDB PD drill: stopping $pd_service and validating SQL continuity"
  stop_service "$pd_service"

  wait_for_ping "SQL node 1 while $pd_service is down" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1"
  wait_for_ping "SQL node 2 while $pd_service is down" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2"

  row_id=$((row_id + 1))
  note="write-through-node1-while-${pd_service}-down"
  sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "insert into $qualified_table (id, note) values ($row_id, '$note')"
  assert_query_contains "node 2 read while $pd_service is down" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2" "select note from $qualified_table where id = $row_id" "$note"

  row_id=$((row_id + 1))
  note="write-through-node2-while-${pd_service}-down"
  sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2" "insert into $qualified_table (id, note) values ($row_id, '$note')"
  assert_query_contains "node 1 read while $pd_service is down" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "select note from $qualified_table where id = $row_id" "$note"

  echo "TiDB PD drill: restarting $pd_service"
  start_service "$pd_service"
  wait_for_ping "SQL node 1 after $pd_service restart" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1"
  wait_for_ping "SQL node 2 after $pd_service restart" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2"
done

assert_complete_fixture "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2" "$qualified_table"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "drop table $qualified_table"

echo "TiDB PD N-1 continuity drill passed"
