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
    dbtool_it_fixture:user:1 \
    dbtool_it_fixture:user:2 \
    dbtool_it_fixture:user:3 \
    dbtool_it_roundtrip:user:1 \
    dbtool_it_roundtrip:user:2 \
    dbtool_it_roundtrip:user:3 >/dev/null || true

  while IFS= read -r command || [[ -n "$command" ]]; do
    [[ -z "$command" || "$command" == \#* ]] && continue
    read -r -a args <<<"$command"
    if [[ "${args[0]}" != "SET" || "${#args[@]}" -ne 3 ]]; then
      echo "unsupported Redis fixture command: $command" >&2
      return 1
    fi
    run_dbtool --dsn "$DBTOOL_IT_REDIS_DSN" --allow-write \
      kv set "${args[1]}" "${args[2]}" >/dev/null
  done <"$file"
}

seed_mongo_ndjson() {
  local collection="$1"
  local file="$2"
  local doc

  run_dbtool --dsn "$DBTOOL_IT_MONGO_DSN" --allow-write doc delete \
    --filter '{"kind":"dbtool-it-fixture"}' \
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
mongo_collection="dbtool_it_fixture_people"

postgres_restore="dbtool_it_fixture_people_restore_$suffix"
mysql_restore="dbtool_it_fixture_people_restore_$suffix"
mongo_restore="dbtool_it_fixture_people_restore_$suffix"

echo "dbtool data roundtrip resources: postgres=$postgres_restore mysql=$mysql_restore redis=dbtool_it_roundtrip:user:* mongo=$mongo_restore"

echo "dbtool data roundtrip: seeding base fixtures"
seed_sql_file "$DBTOOL_IT_POSTGRES_DSN" "$postgres_seed"
seed_sql_file "$DBTOOL_IT_MYSQL_DSN" "$mysql_seed"
seed_redis_commands "$redis_seed"
seed_mongo_ndjson "$mongo_collection" "$mongo_seed"

echo "dbtool data roundtrip: exporting PostgreSQL fixture rows"
sql_exec "$DBTOOL_IT_POSTGRES_DSN" "drop table if exists $postgres_restore"
sql_exec "$DBTOOL_IT_POSTGRES_DSN" "create table $postgres_restore (id integer primary key, name varchar(32) not null, role varchar(32) not null, active boolean not null)"
run_dbtool \
  --dsn "$DBTOOL_IT_POSTGRES_DSN" \
  --limit 10 \
  export sql \
  --query "select id, name, role, active from dbtool_it_fixture_people order by id" \
  --out "$export_dir/postgres-people.json" >/dev/null
run_dbtool \
  --dsn "$DBTOOL_IT_POSTGRES_DSN" \
  --allow-write \
  import sql \
  --table "$postgres_restore" \
  --input "$export_dir/postgres-people.json" >/dev/null
postgres_roundtrip="$(
  run_dbtool \
    --dsn "$DBTOOL_IT_POSTGRES_DSN" \
    --limit 3 \
    sql query "select id, name, role, active from $postgres_restore order by id"
)"
assert_json_predicate "$postgres_roundtrip" 'data["data"]["rows"] == [[1,"alice","reader",True],[2,"bob","writer",False],[3,"carol","reviewer",True]]'

echo "dbtool data roundtrip: exporting MySQL fixture rows"
sql_exec "$DBTOOL_IT_MYSQL_DSN" "drop table if exists $mysql_restore"
sql_exec "$DBTOOL_IT_MYSQL_DSN" "create table $mysql_restore (id integer primary key, name varchar(32) not null, role varchar(32) not null, active boolean not null)"
run_dbtool \
  --dsn "$DBTOOL_IT_MYSQL_DSN" \
  --limit 10 \
  export sql \
  --query "select id, name, role, active from dbtool_it_fixture_people order by id" \
  --out "$export_dir/mysql-people.json" >/dev/null
run_dbtool \
  --dsn "$DBTOOL_IT_MYSQL_DSN" \
  --allow-write \
  import sql \
  --table "$mysql_restore" \
  --input "$export_dir/mysql-people.json" >/dev/null
mysql_roundtrip="$(
  run_dbtool \
    --dsn "$DBTOOL_IT_MYSQL_DSN" \
    --limit 3 \
    sql query "select id, name, role, active from $mysql_restore order by id"
)"
assert_json_predicate "$mysql_roundtrip" 'data["data"]["rows"] == [[1,"alice","reader",True],[2,"bob","writer",False],[3,"carol","reviewer",True]]'

echo "dbtool data roundtrip: exporting Redis fixture keys"
run_dbtool \
  --dsn "$DBTOOL_IT_REDIS_DSN" \
  --limit 10 \
  export kv \
  --pattern "dbtool_it_fixture:user:*" \
  --out "$export_dir/redis-values.json" >/dev/null
redis_artifact="$(cat "$export_dir/redis-values.json")"
assert_json_predicate "$redis_artifact" 'data["version"] == 3 and all(entry["expiry"]["kind"] == "persistent" for entry in data["entries"])'
redis_import="$(run_dbtool \
  --dsn "$DBTOOL_IT_REDIS_DSN" \
  --allow-write \
  import kv \
  --input "$export_dir/redis-values.json" \
  --strip-prefix "dbtool_it_fixture:user:" \
  --key-prefix "dbtool_it_roundtrip:user:")"
assert_json_predicate "$redis_import" 'data["data"]["restored"] == 3 and data["data"]["expired_skipped"] == 0 and data["data"]["per_entry_atomic"] is True and data["data"]["expiry_preserved"] is True'
redis_roundtrip="$(run_dbtool --dsn "$DBTOOL_IT_REDIS_DSN" kv raw MGET dbtool_it_roundtrip:user:1 dbtool_it_roundtrip:user:2 dbtool_it_roundtrip:user:3)"
assert_json_predicate "$redis_roundtrip" 'data["data"] == ["alice","bob","carol"]'
redis_roundtrip_keys="$(run_dbtool --dsn "$DBTOOL_IT_REDIS_DSN" --limit 3 kv scan "dbtool_it_roundtrip:user:*")"
assert_json_predicate "$redis_roundtrip_keys" 'len(data["data"]) == 3'
for key in dbtool_it_roundtrip:user:1 dbtool_it_roundtrip:user:2 dbtool_it_roundtrip:user:3; do
  redis_ttl="$(run_dbtool --dsn "$DBTOOL_IT_REDIS_DSN" kv raw TTL "$key")"
  assert_json_predicate "$redis_ttl" 'data["data"] == -1'
done

echo "dbtool data roundtrip: exporting MongoDB fixture documents"
run_dbtool \
  --dsn "$DBTOOL_IT_MONGO_DSN" \
  --limit 10 \
  export doc \
  "$mongo_collection" \
  --filter '{"kind":"dbtool-it-fixture"}' \
  --out "$export_dir/mongo-people.json" >/dev/null
run_dbtool --dsn "$DBTOOL_IT_MONGO_DSN" --allow-write doc delete \
  --filter '{"kind":"dbtool-it-fixture"}' \
  "$mongo_restore" >/dev/null || true
run_dbtool \
  --dsn "$DBTOOL_IT_MONGO_DSN" \
  --allow-write \
  import doc \
  "$mongo_restore" \
  --input "$export_dir/mongo-people.json" \
  --drop-id >/dev/null
mongo_roundtrip="$(
  run_dbtool \
    --dsn "$DBTOOL_IT_MONGO_DSN" \
    --limit 3 \
    doc find --filter '{"kind":"dbtool-it-fixture"}' "$mongo_restore"
)"
assert_json_predicate "$mongo_roundtrip" 'sorted((doc["id"],doc["name"],doc["role"],doc["active"]) for doc in data["data"]) == [(1,"alice","reader",True),(2,"bob","writer",False),(3,"carol","reviewer",True)]'

sql_exec "$DBTOOL_IT_POSTGRES_DSN" "drop table if exists $postgres_restore" || true
sql_exec "$DBTOOL_IT_POSTGRES_DSN" "drop table if exists dbtool_it_fixture_people" || true
sql_exec "$DBTOOL_IT_MYSQL_DSN" "drop table if exists $mysql_restore" || true
sql_exec "$DBTOOL_IT_MYSQL_DSN" "drop table if exists dbtool_it_fixture_people" || true
run_dbtool --dsn "$DBTOOL_IT_REDIS_DSN" --allow-write kv del \
  dbtool_it_fixture:user:1 \
  dbtool_it_fixture:user:2 \
  dbtool_it_fixture:user:3 \
  dbtool_it_roundtrip:user:1 \
  dbtool_it_roundtrip:user:2 \
  dbtool_it_roundtrip:user:3 >/dev/null || true
run_dbtool --dsn "$DBTOOL_IT_MONGO_DSN" --allow-write doc delete \
  --filter '{"kind":"dbtool-it-fixture"}' \
  "$mongo_collection" >/dev/null || true
run_dbtool --dsn "$DBTOOL_IT_MONGO_DSN" --allow-write doc delete \
  --filter '{"kind":"dbtool-it-fixture"}' \
  "$mongo_restore" >/dev/null || true

echo "dbtool data roundtrip passed"
