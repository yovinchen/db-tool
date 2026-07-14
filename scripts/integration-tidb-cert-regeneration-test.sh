#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export DBTOOL_IT_PROJECT="${DBTOOL_IT_PROJECT:-dbtool-it-tidb-cert-regeneration}"
export DBTOOL_IT_TIDB_SECURE_DIR="${DBTOOL_IT_TIDB_SECURE_DIR:-.tmp/dbtool-it-tidb-cert-regeneration}"
source "$ROOT/scripts/integration-env.sh"

case "$DBTOOL_IT_TIDB_SECURE_DIR" in
  /*) ;;
  *) DBTOOL_IT_TIDB_SECURE_DIR="$ROOT/$DBTOOL_IT_TIDB_SECURE_DIR" ;;
esac
export DBTOOL_IT_TIDB_SECURE_DIR

CERT_DIR="$DBTOOL_IT_TIDB_SECURE_DIR/certs"
FIRST_CERT_DIR="$DBTOOL_IT_TIDB_SECURE_DIR/first-generation"

unset DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1
unset DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2
unset DBTOOL_IT_TIDB_SECURE_DSN_1
unset DBTOOL_IT_TIDB_SECURE_DSN_2
unset DBTOOL_IT_TIDB_SECURE_DISABLED_DSN
unset DBTOOL_IT_TIDB_SECURE_X509_DSN
unset DBTOOL_IT_TIDB_SECURE_X509_NO_CERT_DSN

cleanup() {
  if [[ "${DBTOOL_IT_KEEP_SERVICES:-0}" != "1" ]]; then
    "$ROOT/scripts/integration-down.sh"
  fi
}

trap cleanup EXIT

dbtool_cli() {
  cargo run -q -p dbtool-cli -- \
    --request-timeout "${DBTOOL_IT_TIDB_CERT_DRILL_REQUEST_TIMEOUT:-20s}" \
    --deadline "${DBTOOL_IT_TIDB_CERT_DRILL_DEADLINE:-30s}" \
    "$@"
}

cert_fingerprint() {
  local file="$1"
  local hash

  read -r hash _ < <(shasum -a 256 "$file")
  printf '%s\n' "$hash"
}

assert_changed() {
  local label="$1"
  local before="$2"
  local after="$3"

  if [[ "$before" == "$after" ]]; then
    echo "TiDB cert regeneration drill: $label fingerprint did not change" >&2
    return 1
  fi
}

assert_identifier() {
  local value="$1"
  local label="$2"

  if [[ ! "$value" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    echo "TiDB cert regeneration drill: invalid $label identifier: $value" >&2
    return 1
  fi
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

assert_generation_fixture() {
  local dsn="$1"
  local table="$2"
  local generation="$3"
  local note="$4"
  local output

  output="$(dbtool_cli --dsn "$dsn" sql query "select id, note from $table order by id")"
  printf '%s' "$output" | python3 -c '
import json,sys
data=json.load(sys.stdin)
generation=int(sys.argv[1])
note=sys.argv[2]
assert data["data"]["rows"] == [[generation, note]], data
' "$generation" "$note"
}

expect_tls_rejection() {
  local name="$1"
  local dsn="$2"
  local output
  local status

  set +e
  output="$(dbtool_cli --dsn "$dsn" ping 2>&1)"
  status=$?
  set -e

  if ((status == 0)); then
    echo "TiDB cert regeneration drill: $name unexpectedly trusted the regenerated cluster" >&2
    return 1
  fi

  if ! grep -Eqi 'certificate|tls|ssl|unknown.?issuer|unknown.?ca|invalid peer' <<<"$output"; then
    echo "TiDB cert regeneration drill: $name failed for an unexpected non-TLS reason" >&2
    echo "$output" >&2
    return 1
  fi

  echo "TiDB cert regeneration drill: $name rejected as expected"
}

prepare_generation() {
  export DBTOOL_IT_TIDB_SECURE_REGENERATE_CERTS=1
  source "$ROOT/scripts/integration-tidb-secure-prepare.sh"
  export DBTOOL_IT_TIDB_SECURE_REGENERATE_CERTS=0
}

start_secure_cluster() {
  "$ROOT/scripts/integration-tidb-secure-up.sh"
}

stop_secure_cluster() {
  "$ROOT/scripts/integration-down.sh"
}

verify_tls_sql() {
  local generation="$1"
  local note="$2"

  dbtool_cli --dsn "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" ping >/dev/null
  dbtool_cli --dsn "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2" ping >/dev/null

  sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "create database if not exists $database"
  sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "create table $qualified_table (id bigint primary key, note varchar(96) not null)"
  sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "insert into $qualified_table (id, note) values ($generation, '$note')"
  assert_generation_fixture \
    "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2" \
    "$qualified_table" \
    "$generation" \
    "$note"
}

database="$DBTOOL_IT_TIDB_SECURE_DB"
assert_identifier "$database" "database"

table="dbtool_it_tidb_cert_drill_$(date +%s)_$$"
assert_identifier "$table" "table"
qualified_table="$database.$table"

echo "TiDB cert regeneration drill: generating first certificate set"
prepare_generation
first_ca="$(cert_fingerprint "$CERT_DIR/ca.pem")"
first_server="$(cert_fingerprint "$CERT_DIR/server.pem")"
first_client="$(cert_fingerprint "$CERT_DIR/client.pem")"
mkdir -p "$FIRST_CERT_DIR"
cp "$CERT_DIR/ca.pem" "$FIRST_CERT_DIR/ca.pem"
echo "TiDB cert regeneration resource: generation=1 table=$qualified_table ca_sha256=$first_ca"

echo "TiDB cert regeneration drill: starting secure HA with first certificate set"
start_secure_cluster
verify_tls_sql 1 "first-cert-generation"

echo "TiDB cert regeneration drill: stopping secure HA before certificate regeneration"
stop_secure_cluster

echo "TiDB cert regeneration drill: generating second certificate set"
prepare_generation
second_ca="$(cert_fingerprint "$CERT_DIR/ca.pem")"
second_server="$(cert_fingerprint "$CERT_DIR/server.pem")"
second_client="$(cert_fingerprint "$CERT_DIR/client.pem")"

assert_changed "CA" "$first_ca" "$second_ca"
assert_changed "server certificate" "$first_server" "$second_server"
assert_changed "client certificate" "$first_client" "$second_client"
echo "TiDB cert regeneration resource: generation=2 table=$qualified_table ca_sha256=$second_ca"

echo "TiDB cert regeneration drill: starting secure HA with second certificate set"
start_secure_cluster
verify_tls_sql 2 "second-cert-generation"

stale_ca_dsn="tidb://${DBTOOL_IT_TIDB_USER}@127.0.0.1:${DBTOOL_IT_TIDB_SECURE_PORT_1}?ssl-mode=VERIFY_CA&ssl-ca=$FIRST_CERT_DIR/ca.pem"
expect_tls_rejection "first-generation CA" "$stale_ca_dsn"
sql_exec "$DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1" "drop table $qualified_table"

echo "TiDB certificate regeneration drill passed"
