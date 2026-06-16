#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

"$ROOT/scripts/integration-up.sh"

suffix="$(date +%s)_$$"
export_dir="$ROOT/.tmp/dbtool-data-roundtrip-$suffix"
mkdir -p "$export_dir"

cleanup() {
  if [[ "${DBTOOL_IT_KEEP_EXPORTS:-0}" != "1" ]]; then
    rm -rf "$export_dir"
  else
    echo "dbtool data roundtrip: kept exports in $export_dir"
  fi

  if [[ "${DBTOOL_IT_KEEP_SERVICES:-0}" != "1" ]]; then
    "$ROOT/scripts/integration-down.sh"
  fi
}

trap cleanup EXIT

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
    dbtool:fixture:user:3 \
    dbtool:roundtrip:user:1 \
    dbtool:roundtrip:user:2 \
    dbtool:roundtrip:user:3 >/dev/null || true

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

restore_sql_export() {
  local dsn="$1"
  local export_file="$2"
  local table="$3"
  local statement

  sql_exec "$dsn" "drop table if exists $table"
  sql_exec "$dsn" "create table $table (id integer primary key, name varchar(32) not null, role varchar(32) not null, active boolean not null)"

  while IFS= read -r statement || [[ -n "$statement" ]]; do
    [[ -z "$statement" ]] && continue
    sql_exec "$dsn" "$statement"
  done < <(
    python3 -c 'import json,sys
def quote(value):
    return "'"'"'" + str(value).replace("'"'"'", "'"'"''"'"'") + "'"'"'"
data=json.load(open(sys.argv[1]))
table=sys.argv[2]
for row in data["data"]["rows"]:
    active = "true" if row[3] else "false"
    print(f"insert into {table} (id, name, role, active) values ({int(row[0])}, {quote(row[1])}, {quote(row[2])}, {active})")
' "$export_file" "$table"
  )
}

export_redis_roundtrip_commands() {
  local scan_file="$1"
  local export_file="$2"
  local key
  local value_json
  local value
  local suffix_part

  : >"$export_file"
  while IFS= read -r key || [[ -n "$key" ]]; do
    [[ -z "$key" ]] && continue
    value_json="$(run_dbtool --dsn "$DBTOOL_IT_REDIS_DSN" kv get "$key")"
    value="$(printf '%s' "$value_json" | json_field "data.value")"
    suffix_part="${key#dbtool:fixture:user:}"
    python3 -c 'import json,sys
print(json.dumps({"source_key": sys.argv[1], "restore_key": "dbtool:roundtrip:user:" + sys.argv[2], "value": sys.argv[3]}, separators=(",", ":")))
' "$key" "$suffix_part" "$value" >>"$export_file"
  done < <(
    python3 -c 'import json,sys
data=json.load(open(sys.argv[1]))
for key in sorted(data["data"]):
    print(key)
' "$scan_file"
  )
}

restore_redis_export() {
  local export_file="$1"
  local line
  local key
  local value

  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -z "$line" ]] && continue
    key="$(printf '%s' "$line" | python3 -c 'import json,sys; print(json.load(sys.stdin)["restore_key"])')"
    value="$(printf '%s' "$line" | python3 -c 'import json,sys; print(json.load(sys.stdin)["value"])')"
    run_dbtool --dsn "$DBTOOL_IT_REDIS_DSN" --allow-write kv set --ttl 120 "$key" "$value" >/dev/null
  done <"$export_file"
}

restore_mongo_export() {
  local export_file="$1"
  local collection="$2"
  local doc

  run_dbtool --dsn "$DBTOOL_IT_MONGO_DSN" --allow-write doc delete \
    --filter '{"kind":"dbtool-fixture"}' \
    "$collection" >/dev/null || true

  while IFS= read -r doc || [[ -n "$doc" ]]; do
    [[ -z "$doc" ]] && continue
    run_dbtool --dsn "$DBTOOL_IT_MONGO_DSN" --allow-write doc insert "$collection" "$doc" >/dev/null
  done < <(
    python3 -c 'import json,sys
data=json.load(open(sys.argv[1]))
for doc in data["data"]:
    doc.pop("_id", None)
    print(json.dumps(doc, separators=(",", ":")))
' "$export_file"
  )
}

postgres_seed="$ROOT/testdata/base-postgres-seed.sql"
mysql_seed="$ROOT/testdata/base-mysql-seed.sql"
redis_seed="$ROOT/testdata/base-redis-seed.commands"
mongo_seed="$ROOT/testdata/base-mongo-seed.ndjson"
mongo_collection="dbtool_fixture_people"

postgres_restore="dbtool_fixture_people_restore_$suffix"
mysql_restore="dbtool_fixture_people_restore_$suffix"
mongo_restore="dbtool_fixture_people_restore_$suffix"

echo "dbtool data roundtrip: seeding base fixtures"
seed_sql_file "$DBTOOL_IT_POSTGRES_DSN" "$postgres_seed"
seed_sql_file "$DBTOOL_IT_MYSQL_DSN" "$mysql_seed"
seed_redis_commands "$redis_seed"
seed_mongo_ndjson "$mongo_collection" "$mongo_seed"

echo "dbtool data roundtrip: exporting PostgreSQL fixture rows"
run_dbtool \
  --dsn "$DBTOOL_IT_POSTGRES_DSN" \
  --limit 10 \
  sql query "select id, name, role, active from dbtool_fixture_people order by id" \
  >"$export_dir/postgres-people.json"
restore_sql_export "$DBTOOL_IT_POSTGRES_DSN" "$export_dir/postgres-people.json" "$postgres_restore"
postgres_roundtrip="$(
  run_dbtool \
    --dsn "$DBTOOL_IT_POSTGRES_DSN" \
    --limit 3 \
    sql query "select name, role from $postgres_restore order by id"
)"
assert_json_field "$postgres_roundtrip" "data.rows.0.0" "alice"
assert_json_field "$postgres_roundtrip" "data.rows.2.1" "reviewer"

echo "dbtool data roundtrip: exporting MySQL fixture rows"
run_dbtool \
  --dsn "$DBTOOL_IT_MYSQL_DSN" \
  --limit 10 \
  sql query "select id, name, role, active from dbtool_fixture_people order by id" \
  >"$export_dir/mysql-people.json"
restore_sql_export "$DBTOOL_IT_MYSQL_DSN" "$export_dir/mysql-people.json" "$mysql_restore"
mysql_roundtrip="$(
  run_dbtool \
    --dsn "$DBTOOL_IT_MYSQL_DSN" \
    --limit 3 \
    sql query "select name, role from $mysql_restore order by id"
)"
assert_json_field "$mysql_roundtrip" "data.rows.0.0" "alice"
assert_json_field "$mysql_roundtrip" "data.rows.2.1" "reviewer"

echo "dbtool data roundtrip: exporting Redis fixture keys"
run_dbtool \
  --dsn "$DBTOOL_IT_REDIS_DSN" \
  --limit 10 \
  kv scan "dbtool:fixture:user:*" \
  >"$export_dir/redis-keys.json"
export_redis_roundtrip_commands "$export_dir/redis-keys.json" "$export_dir/redis-values.ndjson"
restore_redis_export "$export_dir/redis-values.ndjson"
redis_roundtrip="$(run_dbtool --dsn "$DBTOOL_IT_REDIS_DSN" kv get dbtool:roundtrip:user:1)"
assert_json_field "$redis_roundtrip" "data.value" "alice"
redis_roundtrip_keys="$(run_dbtool --dsn "$DBTOOL_IT_REDIS_DSN" --limit 3 kv scan "dbtool:roundtrip:user:*")"
assert_json_predicate "$redis_roundtrip_keys" 'len(data["data"]) == 3'

echo "dbtool data roundtrip: exporting MongoDB fixture documents"
run_dbtool \
  --dsn "$DBTOOL_IT_MONGO_DSN" \
  --limit 10 \
  doc find --filter '{"kind":"dbtool-fixture"}' "$mongo_collection" \
  >"$export_dir/mongo-people.json"
restore_mongo_export "$export_dir/mongo-people.json" "$mongo_restore"
mongo_roundtrip="$(
  run_dbtool \
    --dsn "$DBTOOL_IT_MONGO_DSN" \
    --limit 3 \
    doc find --filter '{"kind":"dbtool-fixture"}' "$mongo_restore"
)"
assert_json_predicate "$mongo_roundtrip" 'len(data["data"]) == 3'
assert_json_predicate "$mongo_roundtrip" 'any(doc.get("name") == "alice" and doc.get("role") == "reader" for doc in data["data"])'

sql_exec "$DBTOOL_IT_POSTGRES_DSN" "drop table if exists $postgres_restore" || true
sql_exec "$DBTOOL_IT_MYSQL_DSN" "drop table if exists $mysql_restore" || true
run_dbtool --dsn "$DBTOOL_IT_REDIS_DSN" --allow-write kv del \
  dbtool:roundtrip:user:1 \
  dbtool:roundtrip:user:2 \
  dbtool:roundtrip:user:3 >/dev/null || true
run_dbtool --dsn "$DBTOOL_IT_MONGO_DSN" --allow-write doc delete \
  --filter '{"kind":"dbtool-fixture"}' \
  "$mongo_restore" >/dev/null || true

echo "dbtool data roundtrip passed"
