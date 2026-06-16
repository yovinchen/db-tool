#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export DBTOOL_IT_PROJECT="${DBTOOL_IT_PROJECT:-dbtool-it-custom-env}"

export DBTOOL_IT_POSTGRES_DB="${DBTOOL_IT_POSTGRES_DB:-dbtool_custom_pg}"
export DBTOOL_IT_POSTGRES_USER="${DBTOOL_IT_POSTGRES_USER:-dbtool_custom_pg_user}"
export DBTOOL_IT_POSTGRES_PASSWORD="${DBTOOL_IT_POSTGRES_PASSWORD:-dbtool_custom_pg_pass}"
export DBTOOL_IT_POSTGRES_PORT="${DBTOOL_IT_POSTGRES_PORT:-58432}"

export DBTOOL_IT_MYSQL_DB="${DBTOOL_IT_MYSQL_DB:-dbtool_custom_mysql}"
export DBTOOL_IT_MYSQL_USER="${DBTOOL_IT_MYSQL_USER:-dbtool_custom_mysql_user}"
export DBTOOL_IT_MYSQL_PASSWORD="${DBTOOL_IT_MYSQL_PASSWORD:-dbtool_custom_mysql_pass}"
export DBTOOL_IT_MYSQL_ROOT_PASSWORD="${DBTOOL_IT_MYSQL_ROOT_PASSWORD:-dbtool_custom_mysql_root}"
export DBTOOL_IT_MYSQL_PORT="${DBTOOL_IT_MYSQL_PORT:-58406}"

export DBTOOL_IT_REDIS_PORT="${DBTOOL_IT_REDIS_PORT:-58479}"

export DBTOOL_IT_MONGO_DB="${DBTOOL_IT_MONGO_DB:-dbtool_custom_mongo}"
export DBTOOL_IT_MONGO_USER="${DBTOOL_IT_MONGO_USER:-dbtool_custom_mongo_user}"
export DBTOOL_IT_MONGO_PASSWORD="${DBTOOL_IT_MONGO_PASSWORD:-dbtool_custom_mongo_pass}"
export DBTOOL_IT_MONGO_PORT="${DBTOOL_IT_MONGO_PORT:-58417}"

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

suffix="$(date +%s)_$$"
pg_table="dbtool_custom_env_${suffix}"
mysql_table="dbtool_custom_env_${suffix}"
redis_key="dbtool:custom-env:${suffix}"
mongo_collection="dbtool_custom_env_${suffix}"

echo "dbtool custom env smoke: verifying generated custom DSNs"
case "$DBTOOL_IT_POSTGRES_DSN" in
  *":${DBTOOL_IT_POSTGRES_PORT}/${DBTOOL_IT_POSTGRES_DB}") ;;
  *)
    echo "unexpected PostgreSQL DSN: $DBTOOL_IT_POSTGRES_DSN" >&2
    exit 1
    ;;
esac
case "$DBTOOL_IT_MYSQL_DSN" in
  *":${DBTOOL_IT_MYSQL_PORT}/${DBTOOL_IT_MYSQL_DB}") ;;
  *)
    echo "unexpected MySQL DSN: $DBTOOL_IT_MYSQL_DSN" >&2
    exit 1
    ;;
esac
case "$DBTOOL_IT_REDIS_DSN" in
  *":${DBTOOL_IT_REDIS_PORT}/0") ;;
  *)
    echo "unexpected Redis DSN: $DBTOOL_IT_REDIS_DSN" >&2
    exit 1
    ;;
esac
case "$DBTOOL_IT_MONGO_DSN" in
  *":${DBTOOL_IT_MONGO_PORT}/${DBTOOL_IT_MONGO_DB}?authSource=admin") ;;
  *)
    echo "unexpected MongoDB DSN: $DBTOOL_IT_MONGO_DSN" >&2
    exit 1
    ;;
esac

echo "dbtool custom env smoke: verifying custom service pings"
assert_json_field "$(run_dbtool --dsn "$DBTOOL_IT_POSTGRES_DSN" ping)" "data.status" "ok"
assert_json_field "$(run_dbtool --dsn "$DBTOOL_IT_MYSQL_DSN" ping)" "data.status" "ok"
assert_json_field "$(run_dbtool --dsn "$DBTOOL_IT_REDIS_DSN" ping)" "data.status" "ok"
assert_json_field "$(run_dbtool --dsn "$DBTOOL_IT_MONGO_DSN" ping)" "data.status" "ok"

echo "dbtool custom env smoke: verifying PostgreSQL custom database and user"
pg_identity="$(
  run_dbtool \
    --dsn "$DBTOOL_IT_POSTGRES_DSN" \
    sql query "select current_database(), current_user"
)"
assert_json_field "$pg_identity" "data.rows.0.0" "$DBTOOL_IT_POSTGRES_DB"
assert_json_field "$pg_identity" "data.rows.0.1" "$DBTOOL_IT_POSTGRES_USER"
sql_exec "$DBTOOL_IT_POSTGRES_DSN" "drop table if exists $pg_table"
sql_exec "$DBTOOL_IT_POSTGRES_DSN" "create table $pg_table (id integer primary key, note text not null)"
sql_exec "$DBTOOL_IT_POSTGRES_DSN" "insert into $pg_table (id, note) values (1, 'custom-postgres')"
assert_json_field \
  "$(run_dbtool --dsn "$DBTOOL_IT_POSTGRES_DSN" sql query "select note from $pg_table where id = 1")" \
  "data.rows.0.0" \
  "custom-postgres"

echo "dbtool custom env smoke: verifying MySQL custom database"
mysql_identity="$(
  run_dbtool \
    --dsn "$DBTOOL_IT_MYSQL_DSN" \
    sql query "select database(), substring_index(current_user(), '@', 1)"
)"
assert_json_field "$mysql_identity" "data.rows.0.0" "$DBTOOL_IT_MYSQL_DB"
assert_json_field "$mysql_identity" "data.rows.0.1" "$DBTOOL_IT_MYSQL_USER"
sql_exec "$DBTOOL_IT_MYSQL_DSN" "drop table if exists $mysql_table"
sql_exec "$DBTOOL_IT_MYSQL_DSN" "create table $mysql_table (id integer primary key, note varchar(64) not null)"
sql_exec "$DBTOOL_IT_MYSQL_DSN" "insert into $mysql_table (id, note) values (1, 'custom-mysql')"
assert_json_field \
  "$(run_dbtool --dsn "$DBTOOL_IT_MYSQL_DSN" sql query "select note from $mysql_table where id = 1")" \
  "data.rows.0.0" \
  "custom-mysql"

echo "dbtool custom env smoke: verifying Redis custom port"
run_dbtool --dsn "$DBTOOL_IT_REDIS_DSN" --allow-write kv set --ttl 120 "$redis_key" custom-redis >/dev/null
assert_json_field \
  "$(run_dbtool --dsn "$DBTOOL_IT_REDIS_DSN" kv get "$redis_key")" \
  "data.value" \
  "custom-redis"

echo "dbtool custom env smoke: verifying MongoDB custom database and credentials"
run_dbtool --dsn "$DBTOOL_IT_MONGO_DSN" --allow-write doc insert "$mongo_collection" '{"kind":"custom-env","name":"mongo"}' >/dev/null
mongo_find="$(run_dbtool --dsn "$DBTOOL_IT_MONGO_DSN" doc find --filter '{"kind":"custom-env"}' "$mongo_collection")"
assert_json_predicate "$mongo_find" 'len(data["data"]) == 1 and data["data"][0]["name"] == "mongo"'
collections="$(run_dbtool --dsn "$DBTOOL_IT_MONGO_DSN" doc collections)"
assert_json_predicate "$collections" "any(item == '$mongo_collection' for item in data['data'])"

sql_exec "$DBTOOL_IT_POSTGRES_DSN" "drop table if exists $pg_table" || true
sql_exec "$DBTOOL_IT_MYSQL_DSN" "drop table if exists $mysql_table" || true
run_dbtool --dsn "$DBTOOL_IT_REDIS_DSN" --allow-write kv del "$redis_key" >/dev/null || true
run_dbtool --dsn "$DBTOOL_IT_MONGO_DSN" --allow-write doc delete --filter '{"kind":"custom-env"}' "$mongo_collection" >/dev/null || true

echo "dbtool custom env smoke passed"
