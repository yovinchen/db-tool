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

seed_sql_file() {
  local dsn="$1"
  local file="$2"
  local statement

  while IFS= read -r statement || [[ -n "$statement" ]]; do
    [[ -z "$statement" || "$statement" == \#* ]] && continue
    sql_exec "$dsn" "$statement"
  done <"$file"
}

seed_redis_commands() {
  local file="$1"
  local command

  run_dbtool --dsn "$DBTOOL_IT_REDIS_DSN" --allow-write kv del \
    dbtool:fixture:user:1 \
    dbtool:fixture:user:2 \
    dbtool:fixture:user:3 >/dev/null || true

  while IFS= read -r command || [[ -n "$command" ]]; do
    [[ -z "$command" || "$command" == \#* ]] && continue
    read -r -a args <<<"$command"
    run_dbtool --dsn "$DBTOOL_IT_REDIS_DSN" --allow-write kv raw "${args[@]}" >/dev/null
  done <"$file"
}

seed_mongo_ndjson() {
  local collection="$1"
  local file="$2"
  local doc

  run_dbtool --dsn "$DBTOOL_IT_MONGO_DSN" --allow-write doc delete \
    --filter '{"kind":"dbtool-fixture"}' \
    "$collection" >/dev/null || true

  while IFS= read -r doc || [[ -n "$doc" ]]; do
    [[ -z "$doc" || "$doc" == \#* ]] && continue
    run_dbtool --dsn "$DBTOOL_IT_MONGO_DSN" --allow-write doc insert "$collection" "$doc" >/dev/null
  done <"$file"
}

postgres_seed="$ROOT/testdata/base-postgres-seed.sql"
mysql_seed="$ROOT/testdata/base-mysql-seed.sql"
redis_seed="$ROOT/testdata/base-redis-seed.commands"
mongo_seed="$ROOT/testdata/base-mongo-seed.ndjson"
mongo_collection="dbtool_fixture_people"

echo "dbtool fixture smoke: seeding PostgreSQL from $postgres_seed"
seed_sql_file "$DBTOOL_IT_POSTGRES_DSN" "$postgres_seed"
postgres_people="$(
  run_dbtool \
    --dsn "$DBTOOL_IT_POSTGRES_DSN" \
    --limit 3 \
    sql query "select name, role from dbtool_fixture_people order by id"
)"
assert_json_field "$postgres_people" "data.rows.0.0" "alice"
assert_json_field "$postgres_people" "data.rows.2.1" "reviewer"

echo "dbtool fixture smoke: seeding MySQL from $mysql_seed"
seed_sql_file "$DBTOOL_IT_MYSQL_DSN" "$mysql_seed"
mysql_people="$(
  run_dbtool \
    --dsn "$DBTOOL_IT_MYSQL_DSN" \
    --limit 3 \
    sql query "select name, role from dbtool_fixture_people order by id"
)"
assert_json_field "$mysql_people" "data.rows.0.0" "alice"
assert_json_field "$mysql_people" "data.rows.2.1" "reviewer"

echo "dbtool fixture smoke: seeding Redis from $redis_seed"
seed_redis_commands "$redis_seed"
redis_user="$(run_dbtool --dsn "$DBTOOL_IT_REDIS_DSN" kv get dbtool:fixture:user:1)"
assert_json_field "$redis_user" "data.value" "alice"
redis_keys="$(run_dbtool --dsn "$DBTOOL_IT_REDIS_DSN" --limit 3 kv scan "dbtool:fixture:user:*")"
assert_json_predicate "$redis_keys" 'len(data["data"]) == 3'

echo "dbtool fixture smoke: seeding MongoDB from $mongo_seed"
seed_mongo_ndjson "$mongo_collection" "$mongo_seed"
mongo_people="$(
  run_dbtool \
    --dsn "$DBTOOL_IT_MONGO_DSN" \
    --limit 3 \
    doc find --filter '{"kind":"dbtool-fixture"}' "$mongo_collection"
)"
assert_json_predicate "$mongo_people" 'len(data["data"]) == 3'
assert_json_predicate "$mongo_people" 'any(doc.get("name") == "alice" and doc.get("role") == "reader" for doc in data["data"])'

echo "dbtool fixture smoke passed"
