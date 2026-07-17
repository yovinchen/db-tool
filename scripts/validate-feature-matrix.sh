#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

fail() {
  echo "feature matrix validation failed: $*" >&2
  exit 1
}

cargo check -p dbtool-cli --no-default-features
cargo check -p dbtool-cli
cargo check -p dbtool-cli --no-default-features --features portable
cargo check -p dbtool-cli --no-default-features --features messaging
cargo check -p dbtool-cli --no-default-features --features messaging-native
cargo check -p dbtool-cli --no-default-features --features full
cargo check -p dbtool-cli --no-default-features --features full-native
cargo check -p dbtool-tui --no-default-features
cargo check -p dbtool-tui
cargo check -p dbtool-tui --no-default-features --features full
cargo check -p dbtool-tui --no-default-features --features full-native

minimal_tree="$(cargo tree -p dbtool-cli --no-default-features -e normal)"
if grep -Eq 'adapter-(sql|sqlserver|cassandra|db2|redis|mongo|search|timeseries|kafka|amqp|nats)' <<<"$minimal_tree"; then
  fail "CLI --no-default-features unexpectedly activates an adapter"
fi

pure_tree="$(cargo tree -p dbtool-cli --no-default-features --features full -e features -i adapter-kafka)"
grep -Fq 'adapter-kafka feature "backend-pure"' <<<"$pure_tree" \
  || fail "full does not activate the pure Kafka backend"
if grep -Fq 'adapter-kafka feature "backend-native"' <<<"$pure_tree"; then
  fail "full unexpectedly activates the native Kafka backend"
fi

native_tree="$(cargo tree -p dbtool-cli --no-default-features --features full-native -e features -i adapter-kafka)"
grep -Fq 'adapter-kafka feature "backend-native"' <<<"$native_tree" \
  || fail "full-native does not activate the native Kafka backend"
if grep -Fq 'adapter-kafka feature "backend-pure"' <<<"$native_tree"; then
  fail "full-native unexpectedly activates the pure Kafka backend"
fi

messaging_tree="$(cargo tree -p dbtool-cli --no-default-features --features messaging -e features -i adapter-kafka)"
grep -Fq 'adapter-kafka feature "backend-pure"' <<<"$messaging_tree" \
  || fail "messaging does not activate the pure Kafka backend"
messaging_normal_tree="$(cargo tree -p dbtool-cli --no-default-features --features messaging -e normal)"
if grep -Fq 'adapter-kafka feature "backend-native"' <<<"$messaging_tree" \
  || grep -Fq 'adapter-db2' <<<"$messaging_normal_tree"; then
  fail "messaging unexpectedly activates Db2 or native Kafka"
fi

messaging_native_tree="$(cargo tree -p dbtool-cli --no-default-features --features messaging-native -e features -i adapter-kafka)"
grep -Fq 'adapter-kafka feature "backend-native"' <<<"$messaging_native_tree" \
  || fail "messaging-native does not activate the native Kafka backend"
messaging_native_normal_tree="$(cargo tree -p dbtool-cli --no-default-features --features messaging-native -e normal)"
if grep -Fq 'adapter-kafka feature "backend-pure"' <<<"$messaging_native_tree" \
  || grep -Fq 'adapter-db2' <<<"$messaging_native_normal_tree"; then
  fail "messaging-native unexpectedly activates Db2 or pure Kafka"
fi

portable_tree="$(cargo tree -p dbtool-cli --no-default-features --features portable -e normal)"
grep -Fq 'adapter-kafka' <<<"$portable_tree" \
  || fail "portable does not activate the pure Kafka adapter"
if grep -Fq 'adapter-db2' <<<"$portable_tree"; then
  fail "portable unexpectedly activates the host-ODBC Db2 adapter"
fi

pure_schemes="$(
  cargo run --quiet -p dbtool-cli --no-default-features --features full -- conn list \
    | python3 -c 'import json, sys; print("\n".join(json.load(sys.stdin)["data"]["supported_schemes"]))'
)"
native_schemes="$(
  cargo run --quiet -p dbtool-cli --no-default-features --features full-native -- conn list \
    | python3 -c 'import json, sys; print("\n".join(json.load(sys.stdin)["data"]["supported_schemes"]))'
)"
portable_schemes="$(
  cargo run --quiet -p dbtool-cli --no-default-features --features portable -- conn list \
    | python3 -c 'import json, sys; print("\n".join(json.load(sys.stdin)["data"]["supported_schemes"]))'
)"
if [[ "$pure_schemes" != "$native_schemes" ]]; then
  fail "full and full-native do not register the same protocol schemes"
fi
for scheme in db2 ibmdb2 as400; do
  if grep -Fxq "$scheme" <<<"$portable_schemes"; then
    fail "portable unexpectedly registers host-ODBC scheme $scheme"
  fi
done
for scheme in postgres mysql sqlite mssql cassandra redis mongodb opensearch prometheus kafka amqp nats; do
  grep -Fxq "$scheme" <<<"$portable_schemes" \
    || fail "portable release bundle is missing scheme $scheme"
done

echo "feature matrix validation passed"
