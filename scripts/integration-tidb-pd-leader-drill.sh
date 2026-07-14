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
    --request-timeout "${DBTOOL_IT_TIDB_PD_LEADER_DRILL_REQUEST_TIMEOUT:-20s}" \
    --deadline "${DBTOOL_IT_TIDB_PD_LEADER_DRILL_DEADLINE:-30s}" \
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
  local attempts="${DBTOOL_IT_TIDB_PD_LEADER_DRILL_READY_ATTEMPTS:-90}"

  for attempt in $(seq 1 "$attempts"); do
    if ping_dsn "$dsn"; then
      return 0
    fi
    sleep 2
  done

  echo "TiDB PD leader drill: $name did not become reachable in time" >&2
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
    echo "TiDB PD leader drill: $name did not contain expected value: $expected" >&2
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
    [1, "leader-baseline"],
    [2, "node1-while-pd-leader-down"],
    [3, "node2-while-pd-leader-down"],
], data
'
}

assert_identifier() {
  local value="$1"
  local label="$2"

  if [[ ! "$value" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    echo "TiDB PD leader drill: invalid $label identifier: $value" >&2
    return 1
  fi
}

pd_members_json() {
  local service

  for service in "${pd_services[@]}"; do
    if compose ps --status running --services "$service" | grep -Fxq "$service"; then
      if compose exec -T "$service" \
        curl -fsS \
          --cacert /tidb-secure/certs/ca.pem \
          --cert /tidb-secure/certs/server.pem \
          --key /tidb-secure/certs/server-key.pem \
          https://127.0.0.1:2379/pd/api/v1/members; then
        return 0
      fi
    fi
  done

  return 1
}

pd_leader_name_from_json() {
  python3 -c 'import json,sys
data=json.load(sys.stdin)
leader=data.get("leader") or {}
name=leader.get("name")
if not name:
    raise SystemExit("missing PD leader name")
print(name)'
}

pd_service_for_name() {
  local name="$1"

  case "$name" in
    pd-1) printf '%s\n' tidb-secure-pd-1 ;;
    pd-2) printf '%s\n' tidb-secure-pd-2 ;;
    pd-3) printf '%s\n' tidb-secure-pd-3 ;;
    *)
      echo "TiDB PD leader drill: unknown PD leader name: $name" >&2
      return 1
      ;;
  esac
}

current_pd_leader_service() {
  local leader_name

  leader_name="$(pd_members_json | pd_leader_name_from_json)"
  pd_service_for_name "$leader_name"
}

wait_for_new_pd_leader() {
  local stopped_service="$1"
  local attempts="${DBTOOL_IT_TIDB_PD_LEADER_DRILL_READY_ATTEMPTS:-90}"
  local leader_service

  for attempt in $(seq 1 "$attempts"); do
    if leader_service="$(current_pd_leader_service 2>/dev/null)" &&
      [[ "$leader_service" != "$stopped_service" ]]; then
      printf '%s\n' "$leader_service"
      return 0
    fi
    sleep 2
  done

  echo "TiDB PD leader drill: no replacement PD leader elected after stopping $stopped_service" >&2
  compose logs --tail 160 "${pd_services[@]}" >&2 || true
  return 1
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

table="dbtool_it_tidb_pd_leader_drill_$(date +%s)_$$"
qualified_table="$database.$table"
echo "TiDB PD leader resource: table=$qualified_table"

echo "TiDB PD leader drill: preparing $qualified_table through SQL node 1"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "create database if not exists $database"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "create table $qualified_table (id bigint primary key, note varchar(96) not null)"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "insert into $qualified_table (id, note) values (1, 'leader-baseline')"
assert_query_contains "node 2 baseline read" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2" "select note from $qualified_table where id = 1" "leader-baseline"

leader_service="$(current_pd_leader_service)"
echo "TiDB PD leader drill: stopping current PD leader $leader_service"
stop_service "$leader_service"

replacement_leader="$(wait_for_new_pd_leader "$leader_service")"
echo "TiDB PD leader drill: replacement PD leader is $replacement_leader"

wait_for_ping "SQL node 1 while PD leader is down" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1"
wait_for_ping "SQL node 2 while PD leader is down" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2"

sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "insert into $qualified_table (id, note) values (2, 'node1-while-pd-leader-down')"
assert_query_contains "node 2 read node 1 write while leader is down" \
  "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2" \
  "select note from $qualified_table where id = 2" \
  "node1-while-pd-leader-down"

sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2" "insert into $qualified_table (id, note) values (3, 'node2-while-pd-leader-down')"
assert_query_contains "node 1 read node 2 write while leader is down" \
  "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" \
  "select note from $qualified_table where id = 3" \
  "node2-while-pd-leader-down"

echo "TiDB PD leader drill: restarting stopped PD leader $leader_service"
start_service "$leader_service"
wait_for_ping "SQL node 1 after PD leader restart" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1"
wait_for_ping "SQL node 2 after PD leader restart" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2"
restored_leader="$(current_pd_leader_service)"
echo "TiDB PD leader drill: healthy cluster leader after recovery is $restored_leader"
assert_complete_fixture "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2" "$qualified_table"

sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "drop table $qualified_table"

echo "TiDB PD leader failover drill passed"
