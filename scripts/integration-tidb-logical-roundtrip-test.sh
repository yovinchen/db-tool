#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-tidb-secure-prepare.sh"

export_dir=""
source_table=""
restore_table=""

dbtool_cli() {
  cargo run -q -p dbtool-cli -- \
    --request-timeout "${DBTOOL_IT_TIDB_ROUNDTRIP_REQUEST_TIMEOUT:-20s}" \
    --deadline "${DBTOOL_IT_TIDB_ROUNDTRIP_DEADLINE:-30s}" \
    "$@"
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
  output="$(dbtool_cli --dsn "$dsn" --allow-write sql exec "$sql" 2>&1)"
  status=$?
  set -e

  if ((status == 0)); then
    return 0
  fi

  if [[ "$output" =~ \"confirm_token\":\"([^\"]+)\" ]]; then
    token="${BASH_REMATCH[1]}"
    dbtool_cli --dsn "$dsn" --allow-write --confirm "$token" sql exec "$sql" >/dev/null
    return 0
  fi

  echo "$output" >&2
  return "$status"
}

assert_identifier() {
  local value="$1"
  local label="$2"

  if [[ ! "$value" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    echo "TiDB logical roundtrip: invalid $label identifier: $value" >&2
    exit 1
  fi
}

prepare_restore_table() {
  local dsn="$1"
  local table="$2"

  sql_exec "$dsn" "drop table if exists $table"
  sql_exec "$dsn" "create table $table (id bigint primary key, note varchar(96) not null, priority integer not null)"
}

cleanup() {
  if [[ -n "$source_table" ]]; then
    sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "drop table if exists $source_table" >/dev/null 2>&1 || true
  fi

  if [[ -n "$restore_table" ]]; then
    sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2" "drop table if exists $restore_table" >/dev/null 2>&1 || true
  fi

  if [[ -n "$export_dir" ]]; then
    if [[ "${DBTOOL_IT_KEEP_EXPORTS:-0}" != "1" ]]; then
      rm -rf "$export_dir"
    else
      echo "TiDB logical roundtrip: kept exports in $export_dir"
    fi
  fi

  if [[ "${DBTOOL_IT_KEEP_SERVICES:-0}" != "1" ]]; then
    "$ROOT/scripts/integration-down.sh"
  fi
}

trap cleanup EXIT

"$ROOT/scripts/integration-tidb-secure-up.sh"

database="$DBTOOL_IT_TIDB_SECURE_DB"
assert_identifier "$database" "database"

suffix="$(date +%s)_$$"
export_dir="$ROOT/.tmp/dbtool-tidb-logical-roundtrip-$suffix"
mkdir -p "$export_dir"

source_table="$database.dbtool_it_tidb_roundtrip_src_$suffix"
restore_table="$database.dbtool_it_tidb_roundtrip_restore_$suffix"
echo "TiDB logical roundtrip resources: source=$source_table restore=$restore_table"

echo "TiDB logical roundtrip: preparing source table through SQL node 1"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "create database if not exists $database"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "drop table if exists $source_table"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "create table $source_table (id bigint primary key, note varchar(96) not null, priority integer not null)"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "insert into $source_table (id, note, priority) values (1, 'tls-node1-export', 10)"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "insert into $source_table (id, note, priority) values (2, 'restore-through-node2', 20)"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "insert into $source_table (id, note, priority) values (3, 'cross-node-readback', 30)"

echo "TiDB logical roundtrip: exporting source rows through SQL node 1"
dbtool_cli \
  --dsn "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" \
  --limit 10 \
  export sql \
  --query "select id, note, priority from $source_table order by id" \
  --out "$export_dir/tidb-source.json" >/dev/null

assert_json_predicate "$(cat "$export_dir/tidb-source.json")" 'data["kind"] == "sql-rows" and data["columns"] == ["id","note","priority"] and data["rows"] == [[1,"tls-node1-export",10],[2,"restore-through-node2",20],[3,"cross-node-readback",30]]'

echo "TiDB logical roundtrip: restoring rows through SQL node 2"
prepare_restore_table "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2" "$restore_table"
dbtool_cli \
  --dsn "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2" \
  --allow-write \
  import sql \
  --table "$restore_table" \
  --input "$export_dir/tidb-source.json" >/dev/null

node2_roundtrip="$(
  dbtool_cli \
    --dsn "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2" \
    --limit 3 \
    sql query "select id, note, priority from $restore_table order by id"
)"
assert_json_predicate "$node2_roundtrip" 'data["data"]["rows"] == [[1,"tls-node1-export",10],[2,"restore-through-node2",20],[3,"cross-node-readback",30]]'

node1_readback="$(
  dbtool_cli \
    --dsn "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" \
    --limit 3 \
    sql query "select id, note, priority from $restore_table order by id"
)"
assert_json_predicate "$node1_readback" 'data["data"]["rows"] == [[1,"tls-node1-export",10],[2,"restore-through-node2",20],[3,"cross-node-readback",30]]'

echo "TiDB logical roundtrip passed"
