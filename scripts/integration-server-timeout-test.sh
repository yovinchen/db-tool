#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

compose() {
  docker compose -f "$ROOT/docker-compose.integration.yml" -p "$DBTOOL_IT_PROJECT" "$@"
}

compose \
  up -d --wait --wait-timeout "${DBTOOL_IT_WAIT_TIMEOUT:-180}" \
  postgres mysql

compose ps

pg_timeout_set=0
pg_idle_timeout_set=0
pg_lock_timeout_set=0
pg_lock_pid=""
pg_lock_table=""
mysql_timeout_set=0
mysql_lock_pid=""
mysql_table=""

run_dbtool() {
  cargo run -q -p dbtool-cli -- "$@"
}

json_field() {
  python3 -c 'import json,sys
text=sys.stdin.read()
decoder=json.JSONDecoder()
data=None
for index, char in enumerate(text):
    if char not in "{[":
        continue
    try:
        data, _ = decoder.raw_decode(text[index:])
        break
    except json.JSONDecodeError:
        continue
if data is None:
    raise SystemExit(f"no JSON payload found in output: {text!r}")
cur=data
for part in sys.argv[1].split("."):
    cur = cur[int(part)] if isinstance(cur, list) else cur[part]
print(cur)' "$1"
}

assert_json_field() {
  local json="$1"
  local path="$2"
  local expected="$3"
  local actual

  actual="$(printf '%s' "$json" | json_field "$path")"
  if [[ "$actual" != "$expected" ]]; then
    echo "expected $path to be $expected, got $actual" >&2
    echo "$json" >&2
    exit 1
  fi
}

assert_contains() {
  local text="$1"
  local expected="$2"

  if [[ "$text" != *"$expected"* ]]; then
    echo "expected output to contain: $expected" >&2
    echo "$text" >&2
    exit 1
  fi
}

expect_error_code() {
  local expected="$1"
  shift
  local output
  local status

  set +e
  output="$(run_dbtool "$@" 2>&1 >/dev/null)"
  status=$?
  set -e

  if [[ "$status" -eq 0 ]]; then
    echo "expected failure with $expected, command succeeded: $*" >&2
    exit 1
  fi

  assert_json_field "$output" "error.code" "$expected"
  printf '%s' "$output"
}

sql_exec() {
  local dsn="$1"
  local sql="$2"
  local output
  local status
  local token

  set +e
  output="$(run_dbtool --dsn "$dsn" --allow-write sql exec "$sql" 2>&1)"
  status=$?
  set -e

  if [[ "$status" -eq 0 ]]; then
    return 0
  fi

  if [[ "$(printf '%s' "$output" | json_field "error.code")" == "CONFIRM_REQUIRED" ]]; then
    token="$(printf '%s' "$output" | json_field "error.confirm_token")"
    run_dbtool --dsn "$dsn" --allow-write --confirm "$token" sql exec "$sql" >/dev/null
    return 0
  fi

  echo "$output" >&2
  return "$status"
}

postgres_user_exec() {
  compose exec -T \
    -e PGPASSWORD="$DBTOOL_IT_POSTGRES_PASSWORD" \
    postgres \
    psql \
    -h 127.0.0.1 \
    -U "$DBTOOL_IT_POSTGRES_USER" \
    -d "$DBTOOL_IT_POSTGRES_DB" \
    -v ON_ERROR_STOP=1 \
    -c "$1"
}

postgres_user_stdin() {
  compose exec -T \
    -e PGPASSWORD="$DBTOOL_IT_POSTGRES_PASSWORD" \
    postgres \
    psql \
    -h 127.0.0.1 \
    -U "$DBTOOL_IT_POSTGRES_USER" \
    -d "$DBTOOL_IT_POSTGRES_DB" \
    -v ON_ERROR_STOP=1
}

mysql_admin_exec() {
  compose exec -T mysql \
    mysql \
    -uroot \
    -p"$DBTOOL_IT_MYSQL_ROOT_PASSWORD" \
    "$DBTOOL_IT_MYSQL_DB" \
    -e "$1"
}

mysql_user_exec() {
  compose exec -T mysql \
    mysql \
    -u"$DBTOOL_IT_MYSQL_USER" \
    -p"$DBTOOL_IT_MYSQL_PASSWORD" \
    "$DBTOOL_IT_MYSQL_DB" \
    -e "$1"
}

cleanup() {
  set +e
  if [[ -n "$pg_lock_pid" ]]; then
    wait "$pg_lock_pid" >/dev/null 2>&1
  fi
  if [[ -n "$mysql_lock_pid" ]]; then
    wait "$mysql_lock_pid" >/dev/null 2>&1
  fi
  if [[ "$pg_lock_timeout_set" == "1" ]]; then
    sql_exec "$DBTOOL_IT_POSTGRES_DSN" "alter role current_user reset lock_timeout" >/dev/null 2>&1
  fi
  if [[ "$pg_idle_timeout_set" == "1" ]]; then
    sql_exec "$DBTOOL_IT_POSTGRES_DSN" "alter role current_user reset idle_in_transaction_session_timeout" >/dev/null 2>&1
  fi
  if [[ -n "$pg_lock_table" ]]; then
    sql_exec "$DBTOOL_IT_POSTGRES_DSN" "drop table if exists $pg_lock_table" >/dev/null 2>&1
  fi
  if [[ "$mysql_timeout_set" == "1" ]]; then
    mysql_admin_exec "set global innodb_lock_wait_timeout = ${DBTOOL_IT_SERVER_TIMEOUT_MYSQL_LOCK_WAIT_RESET_SECONDS:-50}" >/dev/null 2>&1
  fi
  if [[ -n "$mysql_table" ]]; then
    sql_exec "$DBTOOL_IT_MYSQL_DSN" "drop table if exists $mysql_table" >/dev/null 2>&1
  fi
  if [[ "$pg_timeout_set" == "1" ]]; then
    sql_exec "$DBTOOL_IT_POSTGRES_DSN" "alter role current_user reset statement_timeout" >/dev/null 2>&1
  fi
  if [[ "${DBTOOL_IT_KEEP_SERVICES:-0}" != "1" ]]; then
    "$ROOT/scripts/integration-down.sh"
  fi
}
trap cleanup EXIT

run_dbtool --dsn "$DBTOOL_IT_POSTGRES_DSN" ping >/dev/null
run_dbtool --dsn "$DBTOOL_IT_MYSQL_DSN" ping >/dev/null

pg_statement_timeout="${DBTOOL_IT_SERVER_TIMEOUT_POSTGRES_STATEMENT_TIMEOUT:-100ms}"
pg_sleep_seconds="${DBTOOL_IT_SERVER_TIMEOUT_POSTGRES_SLEEP_SECONDS:-1}"
pg_idle_timeout="${DBTOOL_IT_SERVER_TIMEOUT_POSTGRES_IDLE_TIMEOUT:-100ms}"
pg_idle_hold_seconds="${DBTOOL_IT_SERVER_TIMEOUT_POSTGRES_IDLE_HOLD_SECONDS:-1}"
pg_lock_timeout="${DBTOOL_IT_SERVER_TIMEOUT_POSTGRES_LOCK_TIMEOUT:-100ms}"
pg_lock_hold_seconds="${DBTOOL_IT_SERVER_TIMEOUT_POSTGRES_LOCK_HOLD_SECONDS:-5}"
pg_lock_ready_sleep="${DBTOOL_IT_SERVER_TIMEOUT_POSTGRES_LOCK_READY_SLEEP:-1}"
mysql_lock_wait_seconds="${DBTOOL_IT_SERVER_TIMEOUT_MYSQL_LOCK_WAIT_SECONDS:-1}"
mysql_lock_hold_seconds="${DBTOOL_IT_SERVER_TIMEOUT_MYSQL_LOCK_HOLD_SECONDS:-5}"
mysql_lock_ready_sleep="${DBTOOL_IT_SERVER_TIMEOUT_MYSQL_LOCK_READY_SLEEP:-1}"
suffix="$(date +%s)_$$"
pg_lock_table="dbtool_pg_lock_timeout_${suffix}"
mysql_table="dbtool_server_timeout_${suffix}"

echo "dbtool server timeout smoke: verifying PostgreSQL statement_timeout"
sql_exec \
  "$DBTOOL_IT_POSTGRES_DSN" \
  "alter role current_user set statement_timeout = '$pg_statement_timeout'"
pg_timeout_set=1

pg_timeout_error="$(
  expect_error_code \
    QUERY_ERROR \
    --dsn "$DBTOOL_IT_POSTGRES_DSN" \
    --request-timeout "${DBTOOL_IT_SERVER_TIMEOUT_CLIENT_REQUEST_TIMEOUT:-5s}" \
    --deadline "${DBTOOL_IT_SERVER_TIMEOUT_CLIENT_DEADLINE:-10s}" \
    sql query "select pg_sleep($pg_sleep_seconds)"
)"
assert_contains "$pg_timeout_error" "statement timeout"

sql_exec "$DBTOOL_IT_POSTGRES_DSN" "alter role current_user reset statement_timeout"
pg_timeout_set=0
assert_json_field \
  "$(run_dbtool --dsn "$DBTOOL_IT_POSTGRES_DSN" sql query "select 1")" \
  "data.rows.0.0" \
  "1"

echo "dbtool server timeout smoke: verifying PostgreSQL idle_in_transaction_session_timeout"
sql_exec \
  "$DBTOOL_IT_POSTGRES_DSN" \
  "alter role current_user set idle_in_transaction_session_timeout = '$pg_idle_timeout'"
pg_idle_timeout_set=1

set +e
pg_idle_error="$(
  {
    printf 'begin;\n'
    sleep "$pg_idle_hold_seconds"
    printf 'commit;\n'
  } | postgres_user_stdin 2>&1
)"
pg_idle_status=$?
set -e

if [[ "$pg_idle_status" -eq 0 ]]; then
  echo "expected PostgreSQL idle transaction session to be terminated" >&2
  echo "$pg_idle_error" >&2
  exit 1
fi
assert_contains "$pg_idle_error" "idle-in-transaction timeout"

sql_exec "$DBTOOL_IT_POSTGRES_DSN" "alter role current_user reset idle_in_transaction_session_timeout"
pg_idle_timeout_set=0
assert_json_field \
  "$(run_dbtool --dsn "$DBTOOL_IT_POSTGRES_DSN" sql query "select 1")" \
  "data.rows.0.0" \
  "1"

echo "dbtool server timeout smoke: verifying PostgreSQL lock_timeout"
sql_exec "$DBTOOL_IT_POSTGRES_DSN" "drop table if exists $pg_lock_table"
sql_exec "$DBTOOL_IT_POSTGRES_DSN" "create table $pg_lock_table (id integer primary key, note text not null)"
sql_exec "$DBTOOL_IT_POSTGRES_DSN" "insert into $pg_lock_table (id, note) values (1, 'initial')"
sql_exec \
  "$DBTOOL_IT_POSTGRES_DSN" \
  "alter role current_user set lock_timeout = '$pg_lock_timeout'"
pg_lock_timeout_set=1

postgres_user_exec \
  "begin; update $pg_lock_table set note = 'held' where id = 1; select pg_sleep($pg_lock_hold_seconds); commit;" \
  >/dev/null 2>&1 &
pg_lock_pid=$!
sleep "$pg_lock_ready_sleep"

pg_lock_error="$(
  expect_error_code \
    QUERY_ERROR \
    --dsn "$DBTOOL_IT_POSTGRES_DSN" \
    --request-timeout "${DBTOOL_IT_SERVER_TIMEOUT_CLIENT_REQUEST_TIMEOUT:-5s}" \
    --deadline "${DBTOOL_IT_SERVER_TIMEOUT_CLIENT_DEADLINE:-10s}" \
    --allow-write \
    sql exec "update $pg_lock_table set note = 'dbtool' where id = 1"
)"
assert_contains "$pg_lock_error" "lock timeout"

wait "$pg_lock_pid"
pg_lock_pid=""
sql_exec "$DBTOOL_IT_POSTGRES_DSN" "alter role current_user reset lock_timeout"
pg_lock_timeout_set=0
assert_json_field \
  "$(run_dbtool --dsn "$DBTOOL_IT_POSTGRES_DSN" sql query "select note from $pg_lock_table where id = 1")" \
  "data.rows.0.0" \
  "held"
sql_exec "$DBTOOL_IT_POSTGRES_DSN" "drop table if exists $pg_lock_table"
pg_lock_table=""

echo "dbtool server timeout smoke: verifying MySQL innodb_lock_wait_timeout"
sql_exec "$DBTOOL_IT_MYSQL_DSN" "drop table if exists $mysql_table"
sql_exec "$DBTOOL_IT_MYSQL_DSN" "create table $mysql_table (id integer primary key, note varchar(64) not null) engine=InnoDB"
sql_exec "$DBTOOL_IT_MYSQL_DSN" "insert into $mysql_table (id, note) values (1, 'initial')"
mysql_admin_exec "set global innodb_lock_wait_timeout = $mysql_lock_wait_seconds" >/dev/null 2>&1
mysql_timeout_set=1

mysql_user_exec \
  "start transaction; update $mysql_table set note = 'held' where id = 1; select sleep($mysql_lock_hold_seconds); commit;" \
  >/dev/null 2>&1 &
mysql_lock_pid=$!
sleep "$mysql_lock_ready_sleep"

mysql_timeout_error="$(
  expect_error_code \
    QUERY_ERROR \
    --dsn "$DBTOOL_IT_MYSQL_DSN" \
    --request-timeout "${DBTOOL_IT_SERVER_TIMEOUT_CLIENT_REQUEST_TIMEOUT:-5s}" \
    --deadline "${DBTOOL_IT_SERVER_TIMEOUT_CLIENT_DEADLINE:-10s}" \
    --allow-write \
    sql exec "update $mysql_table set note = 'dbtool' where id = 1"
)"
assert_contains "$mysql_timeout_error" "Lock wait timeout"

wait "$mysql_lock_pid"
mysql_lock_pid=""
mysql_admin_exec "set global innodb_lock_wait_timeout = ${DBTOOL_IT_SERVER_TIMEOUT_MYSQL_LOCK_WAIT_RESET_SECONDS:-50}" >/dev/null 2>&1
mysql_timeout_set=0

assert_json_field \
  "$(run_dbtool --dsn "$DBTOOL_IT_MYSQL_DSN" sql query "select note from $mysql_table where id = 1")" \
  "data.rows.0.0" \
  "held"
sql_exec "$DBTOOL_IT_MYSQL_DSN" "drop table if exists $mysql_table"
mysql_table=""

echo "dbtool server timeout smoke passed"
