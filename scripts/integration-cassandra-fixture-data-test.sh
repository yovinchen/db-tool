#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

"$ROOT/scripts/integration-cassandra-up.sh"

if [[ "${DBTOOL_IT_KEEP_SERVICES:-0}" != "1" ]]; then
  trap '"$ROOT/scripts/integration-down.sh"' EXIT
fi

run_dbtool() {
  cargo run -q -p dbtool-cli --features full -- "$@"
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

seed_cql_file() {
  local dsn="$1"
  local file="$2"
  local statement

  while IFS= read -r statement || [[ -n "$statement" ]]; do
    [[ -z "$statement" || "$statement" == \#* ]] && continue
    statement="${statement//__KEYSPACE__/$DBTOOL_IT_CASSANDRA_KEYSPACE}"
    statement="${statement%;}"
    sql_exec "$dsn" "$statement"
  done <"$file"
}

cassandra_seed="$ROOT/testdata/base-cassandra-seed.cql"
cassandra_table="$DBTOOL_IT_CASSANDRA_KEYSPACE.dbtool_fixture_people"

echo "dbtool Cassandra fixture smoke: seeding CQL from $cassandra_seed"
run_dbtool --dsn "$DBTOOL_IT_CASSANDRA_DSN" ping >/dev/null
seed_cql_file "$DBTOOL_IT_CASSANDRA_DSN" "$cassandra_seed"

echo "dbtool Cassandra fixture smoke: verifying seeded rows"
alice="$(
  run_dbtool \
    --dsn "$DBTOOL_IT_CASSANDRA_DSN" \
    sql query "select name, role, active, tags from $cassandra_table where id = 1"
)"
assert_json_field "$alice" "data.rows.0.0" "alice"
assert_json_field "$alice" "data.rows.0.1" "reader"
assert_json_field "$alice" "data.rows.0.2" "True"
assert_json_predicate "$alice" '"fixture" in data["data"]["rows"][0][3]'

carol="$(
  run_dbtool \
    --dsn "$DBTOOL_IT_CASSANDRA_DSN" \
    sql query "select name, role from $cassandra_table where id = 3"
)"
assert_json_field "$carol" "data.rows.0.0" "carol"
assert_json_field "$carol" "data.rows.0.1" "reviewer"

echo "dbtool Cassandra fixture smoke: verifying table listing and schema"
tables="$(run_dbtool --dsn "$DBTOOL_IT_CASSANDRA_DSN" sql tables --schema "$DBTOOL_IT_CASSANDRA_KEYSPACE")"
assert_json_predicate "$tables" 'any(item["name"] == "dbtool_fixture_people" for item in data["data"])'

schema="$(run_dbtool --dsn "$DBTOOL_IT_CASSANDRA_DSN" sql schema "$cassandra_table")"
assert_json_field "$schema" "data.name" "dbtool_fixture_people"
assert_json_predicate "$schema" 'any(column["name"] == "tags" and "list" in column["type_name"] for column in data["data"]["columns"])'

sql_exec "$DBTOOL_IT_CASSANDRA_DSN" "drop table if exists $cassandra_table" || true

echo "dbtool Cassandra fixture smoke passed"
