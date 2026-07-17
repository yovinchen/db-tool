#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

cassandra_seed="$ROOT/testdata/base-cassandra-seed.cql"
cassandra_table_name="dbtool_it_cassandra_fixture_people"
cassandra_table="$DBTOOL_IT_CASSANDRA_KEYSPACE.$cassandra_table_name"
fixture_touched=0

run_dbtool() {
  cargo run -q -p dbtool-cli --no-default-features --features cassandra -- "$@"
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

cql_exec() {
  local dsn="$1"
  local cql="$2"
  local output
  local status
  local token

  set +e
  output="$(run_dbtool --dsn "$dsn" --allow-write cql exec "$cql" 2>&1)"
  status=$?
  set -e

  if [[ "$status" -eq 0 ]]; then
    return 0
  fi

  if [[ "$(printf '%s' "$output" | json_field "error.code")" == "CONFIRM_REQUIRED" ]]; then
    token="$(printf '%s' "$output" | json_field "error.confirm_token")"
    run_dbtool --dsn "$dsn" --allow-write --confirm "$token" cql exec "$cql" >/dev/null
    return 0
  fi

  echo "$output" >&2
  return "$status"
}

cleanup() {
  local status=$?

  trap - EXIT
  set +e
  if [[ "$fixture_touched" == "1" ]]; then
    cql_exec "$DBTOOL_IT_CASSANDRA_DSN" "drop table if exists $cassandra_table" >/dev/null 2>&1
  fi
  if [[ "${DBTOOL_IT_KEEP_SERVICES:-0}" != "1" ]]; then
    "$ROOT/scripts/integration-down.sh" >/dev/null 2>&1
  fi
  return "$status"
}

trap cleanup EXIT

seed_cql_file() {
  local dsn="$1"
  local file="$2"
  local statement

  while IFS= read -r statement || [[ -n "$statement" ]]; do
    [[ -z "$statement" || "$statement" == \#* ]] && continue
    statement="${statement//__KEYSPACE__/$DBTOOL_IT_CASSANDRA_KEYSPACE}"
    statement="${statement%;}"
    cql_exec "$dsn" "$statement"
  done <"$file"
}

"$ROOT/scripts/integration-cassandra-up.sh"

echo "dbtool Cassandra fixture smoke: seeding CQL from $cassandra_seed"
run_dbtool --dsn "$DBTOOL_IT_CASSANDRA_DSN" ping >/dev/null
fixture_touched=1
seed_cql_file "$DBTOOL_IT_CASSANDRA_DSN" "$cassandra_seed"
echo "dbtool Cassandra fixture resource: table=$cassandra_table"

echo "dbtool Cassandra fixture smoke: verifying every field in every seeded row"
all_rows="$(
  run_dbtool \
    --dsn "$DBTOOL_IT_CASSANDRA_DSN" \
    cql query "select id, name, role, active, tags from $cassandra_table"
)"
printf '%s' "$all_rows" | python3 -c '
import json,sys
data=json.load(sys.stdin)
rows=sorted(data["data"]["rows"], key=lambda row: row[0])
assert rows == [
    [1, "alice", "reader", True, ["cql", "fixture"]],
    [2, "bob", "writer", False, ["cql", "seed"]],
    [3, "carol", "reviewer", True, ["cql", "verify"]],
], data
'

echo "dbtool Cassandra fixture smoke: verifying table listing and schema"
tables="$(run_dbtool --dsn "$DBTOOL_IT_CASSANDRA_DSN" cql tables --keyspace "$DBTOOL_IT_CASSANDRA_KEYSPACE")"
assert_json_predicate "$tables" 'any(item["name"] == "dbtool_it_cassandra_fixture_people" for item in data["data"])'

schema="$(run_dbtool --dsn "$DBTOOL_IT_CASSANDRA_DSN" cql schema "$cassandra_table_name" --keyspace "$DBTOOL_IT_CASSANDRA_KEYSPACE")"
assert_json_field "$schema" "data.name" "$cassandra_table_name"
printf '%s' "$schema" | python3 -c '
import json,sys
data=json.load(sys.stdin)["data"]
columns={column["name"]: column for column in data["columns"]}
assert set(columns) == {"id", "name", "role", "active", "tags"}, data
assert columns["id"]["type_name"] == "int" and columns["id"]["primary_key"] is True, data
assert columns["id"]["nullable"] is False, data
assert columns["name"]["type_name"] == "text", data
assert columns["role"]["type_name"] == "text", data
assert columns["active"]["type_name"] == "boolean", data
assert columns["tags"]["type_name"].replace(" ", "") == "list<text>", data
assert any(index["primary"] is True and index["unique"] is True and index["columns"] == ["id"] for index in data["indexes"]), data
'

cql_exec "$DBTOOL_IT_CASSANDRA_DSN" "drop table if exists $cassandra_table"
fixture_touched=0

tables_after_drop="$(run_dbtool --dsn "$DBTOOL_IT_CASSANDRA_DSN" cql tables --keyspace "$DBTOOL_IT_CASSANDRA_KEYSPACE")"
assert_json_predicate "$tables_after_drop" 'all(item["name"] != "dbtool_it_cassandra_fixture_people" for item in data["data"])'

echo "dbtool Cassandra fixture smoke passed"
