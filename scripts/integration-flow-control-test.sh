#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

"$ROOT/scripts/integration-up.sh"

if [[ "${DBTOOL_IT_KEEP_SERVICES:-0}" != "1" ]]; then
  trap '"$ROOT/scripts/integration-down.sh"' EXIT
fi

run_dbtool() {
  cargo run -q -p dbtool-cli -- "$@"
}

json_field() {
  python3 -c 'import json,sys; data=json.load(sys.stdin); cur=data
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

assert_json_predicate() {
  local json="$1"
  local expression="$2"
  printf '%s' "$json" | python3 -c "import json,sys; data=json.load(sys.stdin); assert $expression, data"
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

run_dbtool --dsn "$DBTOOL_IT_POSTGRES_DSN" ping >/dev/null
run_dbtool --dsn "$DBTOOL_IT_MYSQL_DSN" ping >/dev/null
run_dbtool --dsn "$DBTOOL_IT_REDIS_DSN" ping >/dev/null
run_dbtool --dsn "$DBTOOL_IT_MONGO_DSN" ping >/dev/null

suffix="$(date +%s)_$$"
pg_table="dbtool_flow_${suffix}"
mongo_collection="dbtool_flow_${suffix}"
redis_prefix="dbtool:flow:${suffix}"

echo "dbtool live flow-control smoke: verifying PostgreSQL SQL limits"
sql_exec "$DBTOOL_IT_POSTGRES_DSN" "drop table if exists $pg_table"
sql_exec "$DBTOOL_IT_POSTGRES_DSN" "create table $pg_table (id integer primary key, note text not null)"
sql_exec "$DBTOOL_IT_POSTGRES_DSN" "insert into $pg_table (id, note) values (1, 'one'), (2, 'two'), (3, 'three')"

pg_limited="$(
  run_dbtool \
    --dsn "$DBTOOL_IT_POSTGRES_DSN" \
    --limit 2 \
    sql query "select id, note from $pg_table order by id"
)"
assert_json_field "$pg_limited" "meta.truncated" "True"
assert_json_predicate "$pg_limited" 'len(data["data"]["rows"]) == 2 and data["data"]["rows"][1][1] == "two"'

echo "dbtool live flow-control smoke: verifying PostgreSQL request timeout"
expect_error_code \
  TIMEOUT \
  --dsn "$DBTOOL_IT_POSTGRES_DSN" \
  --request-timeout "${DBTOOL_IT_FLOW_CONTROL_TIMEOUT_REQUEST:-50ms}" \
  --deadline "${DBTOOL_IT_FLOW_CONTROL_TIMEOUT_DEADLINE:-200ms}" \
  sql query "select pg_sleep(1)" >/dev/null

echo "dbtool live flow-control smoke: verifying SQL rate/admission flags on MySQL"
mysql_limited="$(
  run_dbtool \
    --dsn "$DBTOOL_IT_MYSQL_DSN" \
    --rate "${DBTOOL_IT_FLOW_CONTROL_RATE:-10/s}" \
    --acquire-timeout "${DBTOOL_IT_FLOW_CONTROL_ACQUIRE_TIMEOUT:-500ms}" \
    --request-timeout "${DBTOOL_IT_FLOW_CONTROL_REQUEST_TIMEOUT:-2s}" \
    --deadline "${DBTOOL_IT_FLOW_CONTROL_DEADLINE:-5s}" \
    --limit 2 \
    sql query "select 1 as n union all select 2 union all select 3"
)"
assert_json_field "$mysql_limited" "meta.truncated" "True"
assert_json_predicate "$mysql_limited" 'len(data["data"]["rows"]) == 2'

echo "dbtool live flow-control smoke: verifying Redis scan limit"
run_dbtool --dsn "$DBTOOL_IT_REDIS_DSN" --allow-write kv set --ttl 120 "${redis_prefix}:1" one >/dev/null
run_dbtool --dsn "$DBTOOL_IT_REDIS_DSN" --allow-write kv set --ttl 120 "${redis_prefix}:2" two >/dev/null
run_dbtool --dsn "$DBTOOL_IT_REDIS_DSN" --allow-write kv set --ttl 120 "${redis_prefix}:3" three >/dev/null

redis_limited="$(
  run_dbtool \
    --dsn "$DBTOOL_IT_REDIS_DSN" \
    --limit 2 \
    kv scan "${redis_prefix}:*"
)"
assert_json_field "$redis_limited" "meta.truncated" "True"
assert_json_predicate "$redis_limited" 'len(data["data"]) == 2'

echo "dbtool live flow-control smoke: verifying MongoDB find limit"
run_dbtool --dsn "$DBTOOL_IT_MONGO_DSN" --allow-write doc insert "$mongo_collection" '{"kind":"flow","n":1}' >/dev/null
run_dbtool --dsn "$DBTOOL_IT_MONGO_DSN" --allow-write doc insert "$mongo_collection" '{"kind":"flow","n":2}' >/dev/null
run_dbtool --dsn "$DBTOOL_IT_MONGO_DSN" --allow-write doc insert "$mongo_collection" '{"kind":"flow","n":3}' >/dev/null

mongo_limited="$(
  run_dbtool \
    --dsn "$DBTOOL_IT_MONGO_DSN" \
    --limit 2 \
    doc find --filter '{"kind":"flow"}' "$mongo_collection"
)"
assert_json_field "$mongo_limited" "meta.truncated" "True"
assert_json_predicate "$mongo_limited" 'len(data["data"]) == 2'

sql_exec "$DBTOOL_IT_POSTGRES_DSN" "drop table if exists $pg_table" || true
run_dbtool --dsn "$DBTOOL_IT_REDIS_DSN" --allow-write kv del "${redis_prefix}:1" "${redis_prefix}:2" "${redis_prefix}:3" >/dev/null || true
run_dbtool --dsn "$DBTOOL_IT_MONGO_DSN" --allow-write doc delete --filter '{"kind":"flow"}' "$mongo_collection" >/dev/null || true

echo "dbtool live flow-control smoke passed"
