#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VALIDATOR="$ROOT/scripts/validate-container-image-pins.sh"
PIN="sha256:16bc17c64a573ef34162af9298258d1aec548232985b33ed7b1eac33ba35c229"
PYTHON_PIN="sha256:dbb1970cc04ce7d381c65efe8309c0c03d463e5b35c88f14d721796ad24cfbfd"

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/dbtool-image-pins-test.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT

new_fixture() {
  local name="$1"
  local fixture="$work_dir/$name"
  mkdir -p "$fixture"
  cat >"$fixture/compose.yml" <<EOF
services:
  remote:
    image: postgres:\${TEST_POSTGRES_VERSION:-16.14-alpine@$PIN}
  local:
    image: \${TEST_FIXTURE_IMAGE:-dbtool-test-fixture:local}
    build:
      context: .
EOF
  cat >"$fixture/Dockerfile" <<EOF
ARG BASE_TAG=3.12-alpine@$PYTHON_PIN
FROM python:\${BASE_TAG}
EOF
  printf '%s\n' "$fixture"
}

replace_remote_image() {
  local fixture="$1"
  local replacement="$2"
  awk -v replacement="$replacement" '
    /^  remote:/ { in_remote = 1 }
    in_remote && /^    image:/ {
      print "    image: " replacement
      in_remote = 0
      next
    }
    { print }
  ' "$fixture/compose.yml" >"$fixture/compose.next"
  mv "$fixture/compose.next" "$fixture/compose.yml"
}

run_fixture() {
  local fixture="$1"
  "$VALIDATOR" "$fixture/compose.yml" "$fixture/Dockerfile"
}

expect_pass() {
  local name="$1"
  local fixture="$2"
  local log="$work_dir/$name.log"
  if ! run_fixture "$fixture" >"$log" 2>&1; then
    echo "image pin fixture failed unexpectedly: $name" >&2
    sed -n '1,80p' "$log" >&2
    exit 1
  fi
  echo "PASS: $name"
}

expect_fail() {
  local name="$1"
  local fixture="$2"
  local expected="$3"
  local log="$work_dir/$name.log"
  if run_fixture "$fixture" >"$log" 2>&1; then
    echo "image pin fixture passed unexpectedly: $name" >&2
    exit 1
  fi
  if ! grep -Fq "$expected" "$log"; then
    echo "image pin fixture returned the wrong failure: $name" >&2
    sed -n '1,80p' "$log" >&2
    exit 1
  fi
  echo "PASS: $name rejected"
}

baseline="$(new_fixture baseline)"
expect_pass baseline "$baseline"

unpinned="$(new_fixture unpinned-remote)"
replace_remote_image "$unpinned" "postgres:16.14-alpine"
expect_fail unpinned-remote "$unpinned" "remote image default is not pinned"

latest="$(new_fixture latest-without-digest)"
replace_remote_image "$latest" "postgres:latest"
expect_fail latest-without-digest "$latest" "uses latest without a sha256 digest"

malformed="$(new_fixture malformed-digest)"
replace_remote_image "$malformed" "postgres:16.14-alpine@sha256:1234"
expect_fail malformed-digest "$malformed" "has malformed image pin"

local_without_build="$(new_fixture local-without-build)"
awk '
  /^    build:/ { skipping = 1; next }
  skipping && /^      / { next }
  { skipping = 0; print }
' "$local_without_build/compose.yml" >"$local_without_build/compose.next"
mv "$local_without_build/compose.next" "$local_without_build/compose.yml"
expect_fail local-without-build "$local_without_build" "local image default requires build"

unpinned_from="$(new_fixture unpinned-from)"
cat >"$unpinned_from/Dockerfile" <<'EOF'
FROM python:3.12-alpine
EOF
expect_fail unpinned-from "$unpinned_from" "remote image default is not pinned"

unpinned_arg="$(new_fixture unpinned-arg-default)"
cat >"$unpinned_arg/Dockerfile" <<'EOF'
ARG BASE_TAG=3.12-alpine
FROM python:${BASE_TAG}
EOF
expect_fail unpinned-arg-default "$unpinned_arg" "remote image default is not pinned"

malformed_from="$(new_fixture malformed-from-digest)"
cat >"$malformed_from/Dockerfile" <<'EOF'
FROM python:3.12-alpine@sha256:abcd
EOF
expect_fail malformed-from-digest "$malformed_from" "has malformed image pin"

echo "container image pin validator fixture tests passed"
