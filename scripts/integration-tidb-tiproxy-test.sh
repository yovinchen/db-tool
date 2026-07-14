#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-tidb-secure-prepare.sh"

compose() {
  docker compose \
    -f "$ROOT/docker-compose.integration.yml" \
    -p "$DBTOOL_IT_PROJECT" \
    --profile tidb-secure \
    --profile tidb-tiproxy \
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
  local attempts="${DBTOOL_IT_TIDB_TIPROXY_READY_ATTEMPTS:-90}"

  for attempt in $(seq 1 "$attempts"); do
    if ping_dsn "$dsn"; then
      return 0
    fi
    sleep 2
  done

  echo "TiDB TiProxy drill: $name did not become reachable in time" >&2
  compose logs --tail 120 tidb-secure-tiproxy tidb-secure-1 tidb-secure-2 >&2 || true
  return 1
}

expect_ping_failure() {
  local name="$1"
  local dsn="$2"

  if ping_dsn "$dsn"; then
    echo "TiDB TiProxy drill: $name was expected to be unavailable but still accepted ping" >&2
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
    echo "TiDB TiProxy drill: $name did not contain expected value: $expected" >&2
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
    [1, "proxy-before-stop"],
    [2, "proxy-while-node1-down"],
    [3, "proxy-while-node2-down"],
], data
'
}

assert_identifier() {
  local value="$1"
  local label="$2"

  if [[ ! "$value" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    echo "TiDB TiProxy drill: invalid $label identifier: $value" >&2
    return 1
  fi
}

mysql_account() {
  local user="$1"

  assert_identifier "$user" "user"
  printf "'%s'@'%%'" "$user"
}

mysql_password() {
  local password="$1"

  if [[ "$password" == *"'"* || "$password" == *"\\"* ]]; then
    echo "TiDB TiProxy drill: password must not need escaping" >&2
    return 1
  fi
  printf "'%s'" "$password"
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

echo "TiDB TiProxy drill: starting secure HA topology"
"$ROOT/scripts/integration-tidb-secure-up.sh"

echo "TiDB TiProxy drill: starting TiProxy"
compose up -d --wait --wait-timeout "${DBTOOL_IT_WAIT_TIMEOUT:-360}" tidb-secure-tiproxy
compose ps tidb-secure-tiproxy

database="$DBTOOL_IT_TIDB_SECURE_DB"
assert_identifier "$database" "database"

table="dbtool_it_tidb_tiproxy_$(date +%s)_$$"
qualified_table="$database.$table"
proxy_user="dbtool_it_proxy_ssl_$$"
proxy_password="dbtool_proxy_ssl"
proxy_user_account="$(mysql_account "$proxy_user")"
proxy_user_password="$(mysql_password "$proxy_password")"
proxy_user_dsn="tidb://$proxy_user:$proxy_password@127.0.0.1:${DBTOOL_IT_TIDB_TIPROXY_PORT}/${database}?ssl-mode=VERIFY_CA&ssl-ca=$DBTOOL_IT_TIDB_SECURE_CA"
echo "TiDB TiProxy resources: table=$qualified_table user=$proxy_user"

echo "TiDB TiProxy drill: preparing $qualified_table through direct secure SQL"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "create database if not exists $database"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "create table $qualified_table (id bigint primary key, note varchar(64) not null)"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "insert into $qualified_table (id, note) values (1, 'proxy-before-stop')"

echo "TiDB TiProxy drill: creating REQUIRE SSL proxy user through direct secure SQL"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "create user if not exists $proxy_user_account identified by $proxy_user_password require ssl"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "grant all privileges on $database.* to $proxy_user_account"

wait_for_ping "TiProxy REQUIRE SSL user DSN" "$proxy_user_dsn"
assert_query_contains "REQUIRE SSL user through TiProxy" "$proxy_user_dsn" "select note from $qualified_table where id = 1" "proxy-before-stop"

echo "TiDB TiProxy drill: stopping SQL node 1 and validating TiProxy routes new connections"
stop_service tidb-secure-1
expect_ping_failure "SQL node 1" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1"
wait_for_ping "TiProxy while SQL node 1 is down" "$proxy_user_dsn"
sql_exec "$proxy_user_dsn" "insert into $qualified_table (id, note) values (2, 'proxy-while-node1-down')"
assert_query_contains "proxy write while node 1 is down" "$proxy_user_dsn" "select note from $qualified_table where id = 2" "proxy-while-node1-down"

echo "TiDB TiProxy drill: restarting SQL node 1"
start_service tidb-secure-1
wait_for_ping "SQL node 1 after restart" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1"
wait_for_ping "TiProxy after SQL node 1 restart" "$proxy_user_dsn"

echo "TiDB TiProxy drill: stopping SQL node 2 and validating TiProxy routes new connections"
stop_service tidb-secure-2
expect_ping_failure "SQL node 2" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2"
wait_for_ping "TiProxy while SQL node 2 is down" "$proxy_user_dsn"
sql_exec "$proxy_user_dsn" "insert into $qualified_table (id, note) values (3, 'proxy-while-node2-down')"
assert_query_contains "proxy write while node 2 is down" "$proxy_user_dsn" "select note from $qualified_table where id = 3" "proxy-while-node2-down"
assert_query_contains "proxy user read after failover" "$proxy_user_dsn" "select note from $qualified_table where id = 3" "proxy-while-node2-down"

echo "TiDB TiProxy drill: restarting SQL node 2"
start_service tidb-secure-2
wait_for_ping "SQL node 2 after restart" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2"
wait_for_ping "TiProxy after SQL node 2 restart" "$proxy_user_dsn"
assert_complete_fixture "$proxy_user_dsn" "$qualified_table"

sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "drop table $qualified_table"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "drop user $proxy_user_account"

echo "TiDB TiProxy failover drill passed"
