#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

tmp="$(mktemp -d "${TMPDIR:-/tmp}/dbtool-live-config.XXXXXX")"
config_home="$tmp/config"
mkdir -p "$config_home/dbtool"

cleanup() {
  rm -rf "$tmp"

  if [[ "${DBTOOL_IT_KEEP_SERVICES:-0}" != "1" ]]; then
    "$ROOT/scripts/integration-down.sh"
  fi
}

trap cleanup EXIT

cat >"$config_home/dbtool/connections.toml" <<EOF
[defaults.limits]
max_concurrency = 2
rate = "100/s"
acquire_timeout = "1s"
request_timeout = "5s"
overall_deadline = "10s"
max_retries = 0

[connections.live-postgres]
dsn = "$DBTOOL_IT_POSTGRES_DSN"
readonly = false

[connections.live-postgres-timeout]
dsn = "$DBTOOL_IT_POSTGRES_DSN"
readonly = true

[connections.live-postgres-timeout.limits]
request_timeout = "${DBTOOL_IT_CONFIG_TIMEOUT_REQUEST:-50ms}"
overall_deadline = "${DBTOOL_IT_CONFIG_TIMEOUT_DEADLINE:-200ms}"
max_retries = 0

[connections.live-mysql]
dsn = "$DBTOOL_IT_MYSQL_DSN"
readonly = false

[connections.live-redis]
dsn = "$DBTOOL_IT_REDIS_DSN"
readonly = false

[connections.live-mongo]
dsn = "$DBTOOL_IT_MONGO_DSN"
readonly = false
EOF

"$ROOT/scripts/integration-up.sh"

run_dbtool() {
  XDG_CONFIG_HOME="$config_home" cargo run -q -p dbtool-cli -- "$@"
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
  local conn="$1"
  local sql="$2"
  local output
  local status
  local token

  set +e
  output="$(run_dbtool --conn "$conn" --allow-write sql exec "$sql" 2>&1)"
  status=$?
  set -e

  if [[ "$status" -eq 0 ]]; then
    return 0
  fi

  if [[ "$(printf '%s' "$output" | json_field "error.code")" == "CONFIRM_REQUIRED" ]]; then
    token="$(printf '%s' "$output" | json_field "error.confirm_token")"
    run_dbtool --conn "$conn" --allow-write --confirm "$token" sql exec "$sql" >/dev/null
    return 0
  fi

  echo "$output" >&2
  return "$status"
}

suffix="$(date +%s)_$$"
pg_table="dbtool_config_${suffix}"
mysql_table="dbtool_config_${suffix}"
redis_key="dbtool:config:${suffix}"
mongo_collection="dbtool_config_${suffix}"

echo "dbtool live config smoke: verifying configured connection discovery"
connections="$(run_dbtool conn list)"
assert_json_predicate "$connections" 'any(item["name"] == "live-postgres" for item in data["data"]["file_connections"])'
assert_json_predicate "$connections" 'any(item["name"] == "live-mysql" for item in data["data"]["file_connections"])'
assert_json_predicate "$connections" 'any(item["name"] == "live-redis" for item in data["data"]["file_connections"])'
assert_json_predicate "$connections" 'any(item["name"] == "live-mongo" for item in data["data"]["file_connections"])'

echo "dbtool live config smoke: verifying named connection pings"
assert_json_field "$(run_dbtool --conn live-postgres ping)" "data.status" "ok"
assert_json_field "$(run_dbtool --conn live-mysql ping)" "data.status" "ok"
assert_json_field "$(run_dbtool --conn live-redis ping)" "data.status" "ok"
assert_json_field "$(run_dbtool --conn live-mongo ping)" "data.status" "ok"

echo "dbtool live config smoke: verifying PostgreSQL SQL through --conn"
sql_exec live-postgres "drop table if exists $pg_table"
sql_exec live-postgres "create table $pg_table (id integer primary key, note text not null)"
sql_exec live-postgres "insert into $pg_table (id, note) values (1, 'configured-postgres')"
pg_query="$(run_dbtool --conn live-postgres sql query "select note from $pg_table where id = 1")"
assert_json_field "$pg_query" "data.rows.0.0" "configured-postgres"

echo "dbtool live config smoke: verifying MySQL SQL through --conn"
sql_exec live-mysql "drop table if exists $mysql_table"
sql_exec live-mysql "create table $mysql_table (id integer primary key, note varchar(64) not null)"
sql_exec live-mysql "insert into $mysql_table (id, note) values (1, 'configured-mysql')"
mysql_query="$(run_dbtool --conn live-mysql sql query "select note from $mysql_table where id = 1")"
assert_json_field "$mysql_query" "data.rows.0.0" "configured-mysql"

echo "dbtool live config smoke: verifying Redis KV through --conn"
run_dbtool --conn live-redis --allow-write kv set --ttl 120 "$redis_key" configured-redis >/dev/null
redis_get="$(run_dbtool --conn live-redis kv get "$redis_key")"
assert_json_field "$redis_get" "data.value" "configured-redis"

echo "dbtool live config smoke: verifying MongoDB document commands through --conn"
run_dbtool --conn live-mongo --allow-write doc insert "$mongo_collection" '{"kind":"configured","name":"mongo"}' >/dev/null
mongo_find="$(run_dbtool --conn live-mongo doc find --filter '{"kind":"configured"}' "$mongo_collection")"
assert_json_predicate "$mongo_find" 'len(data["data"]) == 1 and data["data"][0]["name"] == "mongo"'

echo "dbtool live config smoke: verifying named connection request timeout"
expect_error_code TIMEOUT --conn live-postgres-timeout sql query "select pg_sleep(1)" >/dev/null

sql_exec live-postgres "drop table if exists $pg_table" || true
sql_exec live-mysql "drop table if exists $mysql_table" || true
run_dbtool --conn live-redis --allow-write kv del "$redis_key" >/dev/null || true
run_dbtool --conn live-mongo --allow-write doc delete --filter '{"kind":"configured"}' "$mongo_collection" >/dev/null || true

echo "dbtool live config smoke passed"
