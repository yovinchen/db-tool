#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${1:-}"

if [[ -n "$BIN" && ! -x "$BIN" ]]; then
  echo "binary is not executable: $BIN" >&2
  exit 1
fi

tmp="$(mktemp -d "${TMPDIR:-/tmp}/dbtool-smoke-core.XXXXXX")"
cleanup() {
  rm -rf "$tmp"
}
trap cleanup EXIT

db_file="$tmp/core-flow.db"
config_home="$tmp/config"
mkdir -p "$config_home/dbtool"
touch "$db_file"

cat >"$config_home/dbtool/connections.toml" <<EOF
[defaults.limits]
max_concurrency = 1
rate = "100/s"
acquire_timeout = "500ms"
request_timeout = "2s"
overall_deadline = "5s"
max_retries = 0

[connections.smoke-sqlite]
dsn = "sqlite://$db_file"
readonly = false

[connections.timeout-sqlite]
dsn = "sqlite://$db_file"
readonly = false

[connections.timeout-sqlite.limits]
request_timeout = "1ms"
overall_deadline = "20ms"
max_retries = 0
EOF

run_dbtool() {
  if [[ -n "$BIN" ]]; then
    XDG_CONFIG_HOME="$config_home" "$BIN" "$@"
  else
    XDG_CONFIG_HOME="$config_home" cargo run -q -p dbtool-cli -- "$@"
  fi
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

expect_error_code() {
  local expected="$1"
  shift
  local output
  set +e
  output="$(run_dbtool "$@" 2>&1 >/dev/null)"
  local status=$?
  set -e
  if [[ "$status" -eq 0 ]]; then
    echo "expected failure with $expected, command succeeded: $*" >&2
    exit 1
  fi
  assert_json_field "$output" "error.code" "$expected"
  printf '%s' "$output"
}

exec_write() {
  run_dbtool --conn smoke-sqlite --allow-write sql exec "$1" >/dev/null
}

exec_confirmed_write() {
  local sql="$1"
  local first
  first="$(expect_error_code CONFIRM_REQUIRED --conn smoke-sqlite --allow-write sql exec "$sql")"
  local token
  token="$(printf '%s' "$first" | json_field "error.confirm_token")"
  run_dbtool --conn smoke-sqlite --allow-write --confirm "$token" sql exec "$sql" >/dev/null
}

run_dbtool conn list >/dev/null
assert_json_field "$(run_dbtool --conn smoke-sqlite ping)" "data.status" "ok"

while IFS= read -r statement; do
  [[ -z "$statement" || "$statement" == \#* ]] && continue
  case "$statement" in
    CREATE*|DROP*|ALTER*|TRUNCATE*|DELETE*)
      exec_confirmed_write "$statement"
      ;;
    *)
      exec_write "$statement"
      ;;
  esac
done <"$ROOT/testdata/sqlite-core-flow.sql"

query="$(run_dbtool --conn smoke-sqlite sql query "SELECT id, name, role FROM people ORDER BY id")"
assert_json_field "$query" "data.rows.0.1" "alice"
assert_json_field "$query" "data.rows.1.2" "reader"

limited="$(run_dbtool --conn smoke-sqlite --limit 1 sql query "SELECT id, name FROM people ORDER BY id")"
assert_json_field "$limited" "meta.truncated" "True"
assert_json_predicate "$limited" 'len(data["data"]["rows"]) == 1'

tables="$(run_dbtool --conn smoke-sqlite sql tables)"
assert_json_predicate "$tables" 'any(item["name"] == "people" for item in data["data"])'

schema="$(run_dbtool --conn smoke-sqlite sql schema people)"
assert_json_predicate "$schema" 'any(column["name"] == "name" for column in data["data"]["columns"])'

expect_error_code WRITE_NOT_ALLOWED --conn smoke-sqlite sql exec "INSERT INTO people (id, name, role) VALUES (3, 'eve', 'writer')" >/dev/null

expect_error_code TIMEOUT --conn timeout-sqlite sql query "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt LIMIT 100000000) SELECT sum(x) FROM cnt" >/dev/null

echo "dbtool core smoke passed"
