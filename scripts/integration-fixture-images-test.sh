#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

services=(
  postgres-fixture
  mysql-fixture
  redis-fixture
  mongo-fixture
)

compose() {
  docker compose \
    -f "$ROOT/docker-compose.integration.yml" \
    -p "$DBTOOL_IT_PROJECT" \
    --profile fixture-images \
    "$@"
}

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

echo "dbtool fixture image smoke: building fixture database images"
compose build "${services[@]}"

echo "dbtool fixture image smoke: starting fixture database images"
compose up -d --wait --wait-timeout "${DBTOOL_IT_WAIT_TIMEOUT:-180}" "${services[@]}"
compose ps "${services[@]}"

echo "dbtool fixture image smoke: verifying PostgreSQL baked fixture"
postgres_people="$(
  run_dbtool \
    --dsn "$DBTOOL_IT_POSTGRES_FIXTURE_DSN" \
    --limit 3 \
    sql query "select name, role from dbtool_fixture_people order by id"
)"
assert_json_field "$postgres_people" "data.rows.0.0" "alice"
assert_json_field "$postgres_people" "data.rows.2.1" "reviewer"

echo "dbtool fixture image smoke: verifying MySQL baked fixture"
mysql_people="$(
  run_dbtool \
    --dsn "$DBTOOL_IT_MYSQL_FIXTURE_DSN" \
    --limit 3 \
    sql query "select name, role from dbtool_fixture_people order by id"
)"
assert_json_field "$mysql_people" "data.rows.0.0" "alice"
assert_json_field "$mysql_people" "data.rows.2.1" "reviewer"

echo "dbtool fixture image smoke: verifying Redis baked fixture"
redis_user="$(run_dbtool --dsn "$DBTOOL_IT_REDIS_FIXTURE_DSN" kv get dbtool:fixture:user:1)"
assert_json_field "$redis_user" "data.value" "alice"
redis_keys="$(run_dbtool --dsn "$DBTOOL_IT_REDIS_FIXTURE_DSN" --limit 3 kv scan "dbtool:fixture:user:*")"
assert_json_predicate "$redis_keys" 'len(data["data"]) == 3'

echo "dbtool fixture image smoke: verifying MongoDB baked fixture"
mongo_people="$(
  run_dbtool \
    --dsn "$DBTOOL_IT_MONGO_FIXTURE_DSN" \
    --limit 3 \
    doc find --filter '{"kind":"dbtool-fixture"}' dbtool_fixture_people
)"
assert_json_predicate "$mongo_people" 'len(data["data"]) == 3'
assert_json_predicate "$mongo_people" 'any(doc.get("name") == "alice" and doc.get("role") == "reader" for doc in data["data"])'

echo "dbtool fixture image smoke passed"
