#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp="$(mktemp -d "${TMPDIR:-/tmp}/dbtool-external-requirement.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT

mkdir -p "$tmp/bin"
cat >"$tmp/bin/cargo" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$DBTOOL_IT_FAKE_CARGO_LOG"
EOF
chmod +x "$tmp/bin/cargo"

run_case() {
  local name="$1"
  shift
  local output="$tmp/$name.output"
  local status

  set +e
  "$@" >"$output" 2>&1
  status=$?
  set -e
  printf '%s' "$status"
}

assert_status() {
  local name="$1"
  local actual="$2"
  local expected="$3"

  if [[ "$actual" != "$expected" ]]; then
    echo "external requirement gate: $name returned $actual, expected $expected" >&2
    cat "$tmp/$name.output" >&2
    exit 1
  fi
}

assert_output_contains() {
  local name="$1"
  local expected="$2"

  if ! grep -Fq "$expected" "$tmp/$name.output"; then
    echo "external requirement gate: $name output did not contain: $expected" >&2
    cat "$tmp/$name.output" >&2
    exit 1
  fi
}

assert_output_excludes() {
  local name="$1"
  local forbidden="$2"

  if grep -Fq "$forbidden" "$tmp/$name.output"; then
    echo "external requirement gate: $name output contained forbidden text" >&2
    exit 1
  fi
}

status="$(run_case redshift-skip env \
  DBTOOL_IT_REQUIRE_EXTERNAL=0 \
  DBTOOL_IT_REDSHIFT_DSN= \
  "$ROOT/scripts/integration-redshift-test.sh")"
assert_status redshift-skip "$status" 0
assert_output_contains redshift-skip "SKIP"
assert_output_contains redshift-skip "DBTOOL_IT_REDSHIFT_DSN"

status="$(run_case redshift-required env \
  DBTOOL_IT_REQUIRE_EXTERNAL=1 \
  DBTOOL_IT_REDSHIFT_DSN= \
  "$ROOT/scripts/integration-redshift-test.sh")"
assert_status redshift-required "$status" 2
assert_output_contains redshift-required "DBTOOL_IT_REDSHIFT_DSN is required"

status="$(run_case kafka-skip env \
  DBTOOL_IT_REQUIRE_EXTERNAL=0 \
  DBTOOL_IT_AUTOMQ_DSN= \
  DBTOOL_IT_WARPSTREAM_DSN= \
  DBTOOL_IT_CONFLUENT_DSN= \
  "$ROOT/scripts/integration-kafka-vendor-test.sh")"
assert_status kafka-skip "$status" 0
assert_output_contains kafka-skip "SKIP"
assert_output_contains kafka-skip "no vendor DSN is set"

status="$(run_case kafka-required env \
  DBTOOL_IT_REQUIRE_EXTERNAL=1 \
  DBTOOL_IT_AUTOMQ_DSN= \
  DBTOOL_IT_WARPSTREAM_DSN= \
  DBTOOL_IT_CONFLUENT_DSN= \
  "$ROOT/scripts/integration-kafka-vendor-test.sh")"
assert_status kafka-required "$status" 2
assert_output_contains kafka-required "at least one of DBTOOL_IT_AUTOMQ_DSN"

status="$(run_case suite-redshift-skip env \
  DBTOOL_IT_REQUIRE_EXTERNAL=0 \
  DBTOOL_IT_REDSHIFT_DSN= \
  DBTOOL_IT_DB_SUITE_PHASES=redshift \
  "$ROOT/scripts/integration-db-suite.sh")"
assert_status suite-redshift-skip "$status" 0
assert_output_contains suite-redshift-skip "skipped redshift"
assert_output_excludes suite-redshift-skip "passed redshift"

status="$(run_case suite-kafka-skip env \
  DBTOOL_IT_REQUIRE_EXTERNAL=0 \
  DBTOOL_IT_AUTOMQ_DSN= \
  DBTOOL_IT_WARPSTREAM_DSN= \
  DBTOOL_IT_CONFLUENT_DSN= \
  DBTOOL_IT_DB_SUITE_PHASES=kafka-vendors \
  "$ROOT/scripts/integration-db-suite.sh")"
assert_status suite-kafka-skip "$status" 0
assert_output_contains suite-kafka-skip "skipped kafka-vendors"
assert_output_excludes suite-kafka-skip "passed kafka-vendors"

status="$(run_case suite-redshift-required env \
  DBTOOL_IT_REQUIRE_EXTERNAL=1 \
  DBTOOL_IT_REDSHIFT_DSN= \
  DBTOOL_IT_DB_SUITE_PHASES=redshift \
  "$ROOT/scripts/integration-db-suite.sh")"
assert_status suite-redshift-required "$status" 2
assert_output_contains suite-redshift-required "DBTOOL_IT_REDSHIFT_DSN is required"

secret="dbtool-redshift-secret-sentinel"
fake_log="$tmp/cargo.log"
status="$(run_case redshift-present env \
  PATH="$tmp/bin:$PATH" \
  DBTOOL_IT_FAKE_CARGO_LOG="$fake_log" \
  DBTOOL_IT_REQUIRE_EXTERNAL=1 \
  DBTOOL_IT_REDSHIFT_DSN="redshift://user:${secret}@example.invalid:5439/dev" \
  "$ROOT/scripts/integration-redshift-test.sh")"
assert_status redshift-present "$status" 0
assert_output_excludes redshift-present "$secret"

secret="dbtool-kafka-secret-sentinel"
status="$(run_case kafka-present env \
  PATH="$tmp/bin:$PATH" \
  DBTOOL_IT_FAKE_CARGO_LOG="$fake_log" \
  DBTOOL_IT_REQUIRE_EXTERNAL=1 \
  DBTOOL_IT_AUTOMQ_DSN= \
  DBTOOL_IT_WARPSTREAM_DSN= \
  DBTOOL_IT_CONFLUENT_DSN="confluent://user:${secret}@example.invalid:9092" \
  "$ROOT/scripts/integration-kafka-vendor-test.sh")"
assert_status kafka-present "$status" 0
assert_output_excludes kafka-present "$secret"

grep -Fq "redshift_external_sql_lifecycle_and_typed_values" "$fake_log"
grep -Fq "vendor_kafka_compatible_smoke_profiles" "$fake_log"

echo "dbtool external requirement gate: ok"
