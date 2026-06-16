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
    echo "TiDB logical roundtrip: expected $path to be $expected, got $actual" >&2
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

restore_sql_export() {
  local dsn="$1"
  local export_file="$2"
  local table="$3"

  sql_exec "$dsn" "drop table if exists $table"
  sql_exec "$dsn" "create table $table (id bigint primary key, note varchar(96) not null, priority integer not null)"

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
    print(f"insert into {table} (id, note, priority) values ({int(row[0])}, {quote(row[1])}, {int(row[2])})")
' "$export_file" "$table"
  )
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

source_table="$database.dbtool_tidb_roundtrip_src_$suffix"
restore_table="$database.dbtool_tidb_roundtrip_restore_$suffix"

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
  sql query "select id, note, priority from $source_table order by id" \
  >"$export_dir/tidb-source.json"

assert_json_predicate "$(cat "$export_dir/tidb-source.json")" 'len(data["data"]["rows"]) == 3'

echo "TiDB logical roundtrip: restoring rows through SQL node 2"
restore_sql_export "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2" "$export_dir/tidb-source.json" "$restore_table"

node2_roundtrip="$(
  dbtool_cli \
    --dsn "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2" \
    --limit 3 \
    sql query "select note, priority from $restore_table order by id"
)"
assert_json_field "$node2_roundtrip" "data.rows.0.0" "tls-node1-export"
assert_json_field "$node2_roundtrip" "data.rows.1.1" "20"
assert_json_field "$node2_roundtrip" "data.rows.2.0" "cross-node-readback"

node1_readback="$(
  dbtool_cli \
    --dsn "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" \
    --limit 3 \
    sql query "select note from $restore_table where id = 2"
)"
assert_json_field "$node1_readback" "data.rows.0.0" "restore-through-node2"

echo "TiDB logical roundtrip passed"
