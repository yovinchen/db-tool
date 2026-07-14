#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-tidb-secure-prepare.sh"

compose() {
  docker compose \
    -f "$ROOT/docker-compose.integration.yml" \
    -p "$DBTOOL_IT_PROJECT" \
    --profile tidb-secure \
    "$@"
}

stopped_services=()

restore_stopped_services() {
  if ((${#stopped_services[@]} == 0)); then
    return 0
  fi

  compose start "${stopped_services[@]}" >/dev/null || true
}

cleanup() {
  if [[ "${DBTOOL_IT_KEEP_SERVICES:-0}" == "1" ]]; then
    restore_stopped_services
  else
    "$ROOT/scripts/integration-down.sh"
  fi
}

trap cleanup EXIT

run_with_timeout() {
  local timeout_secs="$1"
  shift
  local elapsed=0
  local pid

  "$@" &
  pid="$!"

  while kill -0 "$pid" 2>/dev/null; do
    if ((elapsed >= timeout_secs)); then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
      return 124
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done

  wait "$pid"
}

dbtool_cli() {
  cargo run -q -p dbtool-cli -- \
    --request-timeout "${DBTOOL_IT_TIDB_TIKV_OUTAGE_REQUEST_TIMEOUT:-10s}" \
    --deadline "${DBTOOL_IT_TIDB_TIKV_OUTAGE_DEADLINE:-15s}" \
    "$@"
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

assert_query_contains() {
  local name="$1"
  local dsn="$2"
  local sql="$3"
  local expected="$4"
  local output

  output="$(dbtool_cli --dsn "$dsn" --format table sql query "$sql")"
  if ! grep -Fq "$expected" <<<"$output"; then
    echo "TiDB TiKV outage boundary: $name did not contain expected value: $expected" >&2
    echo "$output" >&2
    return 1
  fi
}

assert_recovered_fixture() {
  local dsn="$1"
  local table="$2"
  local outage_outcome="$3"
  local output

  output="$(dbtool_cli --dsn "$dsn" sql query "select id, note from $table order by id")"
  printf '%s' "$output" | python3 -c '
import json,sys
data=json.load(sys.stdin)
outcome=sys.argv[1]
rows=data["data"]["rows"]
without_outage_write = [
    [1, "tikv-baseline"],
    [3, "write-after-tikv-restart"],
]
with_outage_write = [
    [1, "tikv-baseline"],
    [2, "write-while-tikv-down"],
    [3, "write-after-tikv-restart"],
]
if outcome == "client-success":
    assert rows == with_outage_write, data
else:
    assert outcome == "bounded-storage-failure", outcome
    assert rows in (without_outage_write, with_outage_write), data
print("TiDB TiKV outage boundary: recovered fixture rows=%d outage_write=%s committed=%s" % (
    len(rows), outcome, "yes" if rows == with_outage_write else "no"
))
' "$outage_outcome"
}

assert_identifier() {
  local value="$1"
  local label="$2"

  if [[ ! "$value" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    echo "TiDB TiKV outage boundary: invalid $label identifier: $value" >&2
    return 1
  fi
}

stop_service() {
  local service="$1"

  compose stop "$service"
  stopped_services+=("$service")
}

start_service() {
  local service="$1"

  compose start "$service"
  compose up -d --wait --wait-timeout "${DBTOOL_IT_WAIT_TIMEOUT:-360}" "$service"
}

bounded_probe() {
  local label="$1"
  local timeout_secs="$2"
  shift 2
  local output
  local status

  set +e
  output="$(run_with_timeout "$timeout_secs" "$@" 2>&1)"
  status=$?
  set -e

  if ((status == 124)); then
    echo "TiDB TiKV outage boundary: $label exceeded ${timeout_secs}s hard timeout" >&2
    echo "$output" >&2
    return 1
  fi

  if ((status == 0)); then
    echo "TiDB TiKV outage boundary: $label succeeded within bounded window"
  else
    echo "TiDB TiKV outage boundary: $label returned bounded failure status $status"
    echo "$output" | sed -n '1,20p'
  fi

  return 0
}

bounded_baseline_read() {
  local hard_timeout="$1"
  local output
  local status

  set +e
  output="$(
    run_with_timeout "$hard_timeout" \
      dbtool_cli \
        --dsn "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" \
        sql query "select note from $qualified_table where id = 1" \
      2>&1
  )"
  status=$?
  set -e

  if ((status == 124)); then
    echo "TiDB TiKV outage boundary: baseline read exceeded ${hard_timeout}s hard timeout" >&2
    echo "$output" >&2
    return 1
  fi

  if ((status != 0)); then
    echo "TiDB TiKV outage boundary: baseline read returned bounded failure status $status"
    echo "$output" | sed -n '1,20p'
    return 0
  fi

  printf '%s' "$output" | python3 -c '
import json,sys
data=json.load(sys.stdin)
assert data["data"]["rows"] == [["tikv-baseline"]], data
'
  echo "TiDB TiKV outage boundary: baseline read succeeded with the exact value"
}

probe_write_with_one_tikv_down() {
  local hard_timeout="${DBTOOL_IT_TIDB_TIKV_OUTAGE_HARD_TIMEOUT:-45}"
  local output
  local status

  set +e
  output="$(
    run_with_timeout "$hard_timeout" \
      dbtool_cli \
        --dsn "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" \
        --allow-write \
        sql exec "insert into $qualified_table (id, note) values (2, 'write-while-tikv-down')" \
      2>&1
  )"
  status=$?
  set -e

  if ((status == 124)); then
    echo "TiDB TiKV outage boundary: write exceeded ${hard_timeout}s hard timeout" >&2
    echo "$output" >&2
    return 1
  fi

  if ((status == 0)); then
    outage_write_outcome="client-success"
    echo "TiDB TiKV outage boundary: write succeeded while one TiKV was stopped"
    assert_query_contains "cross-node read after TiKV outage write" \
      "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2" \
      "select note from $qualified_table where id = 2" \
      "write-while-tikv-down"
    return 0
  fi

  if ! grep -Eqi \
    'tikv|region|raft|store.*(unavailable|not|error|timeout)|unavailable|deadline exceeded|operation timed out|request timeout|context canceled|connection (closed|reset|refused)|broken pipe|server is busy|not leader|epoch.*not match' \
    <<<"$output"; then
    echo "TiDB TiKV outage boundary: write failed for an unexpected non-storage reason" >&2
    echo "$output" >&2
    return 1
  fi

  outage_write_outcome="bounded-storage-failure"
  echo "TiDB TiKV outage boundary: write returned bounded failure status $status"
  echo "$output" | sed -n '1,20p'
  return 0
}

"$ROOT/scripts/integration-tidb-secure-up.sh"

database="$DBTOOL_IT_TIDB_SECURE_DB"
assert_identifier "$database" "database"

table="dbtool_it_tidb_tikv_outage_$(date +%s)_$$"
qualified_table="$database.$table"
echo "TiDB TiKV outage boundary resource: table=$qualified_table"

echo "TiDB TiKV outage boundary: preparing $qualified_table through SQL node 1"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "create database if not exists $database"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "create table $qualified_table (id bigint primary key, note varchar(96) not null)"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "insert into $qualified_table (id, note) values (1, 'tikv-baseline')"
assert_query_contains "node 2 baseline read" "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2" "select note from $qualified_table where id = 1" "tikv-baseline"

stopped_tikv="${DBTOOL_IT_TIDB_TIKV_OUTAGE_SERVICE:-tidb-secure-tikv-1}"
case "$stopped_tikv" in
  tidb-secure-tikv-1 | tidb-secure-tikv-2) ;;
  *)
    echo "TiDB TiKV outage boundary: invalid TiKV service: $stopped_tikv" >&2
    exit 1
    ;;
esac

echo "TiDB TiKV outage boundary: stopping $stopped_tikv"
stop_service "$stopped_tikv"

hard_timeout="${DBTOOL_IT_TIDB_TIKV_OUTAGE_HARD_TIMEOUT:-45}"
bounded_probe \
  "SQL node 1 ping" \
  "$hard_timeout" \
  dbtool_cli --dsn "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" ping
bounded_probe \
  "SQL node 2 ping" \
  "$hard_timeout" \
  dbtool_cli --dsn "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2" ping
bounded_baseline_read "$hard_timeout"

outage_write_outcome="unknown"
probe_write_with_one_tikv_down

echo "TiDB TiKV outage boundary: restarting $stopped_tikv"
start_service "$stopped_tikv"

bounded_probe \
  "SQL node 1 ping after TiKV restart" \
  "$hard_timeout" \
  dbtool_cli --dsn "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" ping
bounded_probe \
  "SQL node 2 ping after TiKV restart" \
  "$hard_timeout" \
  dbtool_cli --dsn "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2" ping

assert_query_contains "baseline after TiKV restart" \
  "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2" \
  "select note from $qualified_table where id = 1" \
  "tikv-baseline"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" \
  "insert into $qualified_table (id, note) values (3, 'write-after-tikv-restart')"
assert_recovered_fixture \
  "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2" \
  "$qualified_table" \
  "$outage_write_outcome"

sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "drop table $qualified_table"

echo "TiDB TiKV outage boundary drill passed"
