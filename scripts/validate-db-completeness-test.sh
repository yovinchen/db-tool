#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VALIDATOR="$ROOT/scripts/validate-db-completeness.sh"
BASE_MANIFEST="$ROOT/testdata/db-completeness.manifest"
BASE_TASKS="$ROOT/docs/db-completeness-tasks.md"

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/dbtool-completeness-test.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT

new_fixture() {
  local name="$1"
  local fixture="$work_dir/$name"
  mkdir -p "$fixture"
  cp "$BASE_MANIFEST" "$fixture/manifest"
  cp "$BASE_TASKS" "$fixture/tasks.md"
  printf '%s\n' "$fixture"
}

run_fixture() {
  local fixture="$1"
  DBTOOL_COMPLETENESS_MANIFEST="$fixture/manifest" \
    DBTOOL_COMPLETENESS_TASKS="$fixture/tasks.md" \
    "$VALIDATOR"
}

expect_pass() {
  local name="$1"
  local fixture="$2"
  local log="$work_dir/$name.log"
  if ! run_fixture "$fixture" >"$log" 2>&1; then
    echo "validator fixture failed unexpectedly: $name" >&2
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
    echo "validator fixture passed unexpectedly: $name" >&2
    exit 1
  fi
  if ! grep -Fq "$expected" "$log"; then
    echo "validator fixture returned the wrong failure: $name" >&2
    sed -n '1,80p' "$log" >&2
    exit 1
  fi
  echo "PASS: $name rejected"
}

baseline="$(new_fixture baseline)"
expect_pass baseline "$baseline"

manifest_missing="$(new_fixture manifest-missing-id)"
awk '$0 !~ /^DB-SQLITE-001\|/' "$manifest_missing/manifest" >"$manifest_missing/manifest.next"
mv "$manifest_missing/manifest.next" "$manifest_missing/manifest"
expect_fail manifest-missing-id "$manifest_missing" "manifest must contain exactly 27 tasks, found 26"

ledger_missing="$(new_fixture ledger-missing-id)"
awk '$0 !~ /^\| DB-SQLITE-001 \|/' "$ledger_missing/tasks.md" >"$ledger_missing/tasks.next"
mv "$ledger_missing/tasks.next" "$ledger_missing/tasks.md"
expect_fail ledger-missing-id "$ledger_missing" "missing task id DB-SQLITE-001 from task ledger"

status_mismatch="$(new_fixture status-mismatch)"
awk -F'|' '
  BEGIN { OFS = "|" }
  /^\| DB-SQLITE-001 \|/ { $7 = " PARTIAL " }
  { print }
' "$status_mismatch/tasks.md" >"$status_mismatch/tasks.next"
mv "$status_mismatch/tasks.next" "$status_mismatch/tasks.md"
expect_fail status-mismatch "$status_mismatch" "status mismatch for DB-SQLITE-001: manifest=COMPLETE ledger=PARTIAL"

ledger_extra="$(new_fixture ledger-extra-id)"
awk '
  /^## Per-Resource Evidence Contract/ && !inserted {
    print "| DB-EXTRA-001 | SQL | Fixture `fixture:` | fixture | Ready | BLOCKED | - | test-only boundary |"
    inserted = 1
  }
  { print }
' "$ledger_extra/tasks.md" >"$ledger_extra/tasks.next"
mv "$ledger_extra/tasks.next" "$ledger_extra/tasks.md"
expect_fail ledger-extra-id "$ledger_extra" "extra task id DB-EXTRA-001 in task ledger"

ledger_duplicate="$(new_fixture ledger-duplicate-id)"
awk '
  { print }
  /^\| DB-SQLITE-001 \|/ { print }
' "$ledger_duplicate/tasks.md" >"$ledger_duplicate/tasks.next"
mv "$ledger_duplicate/tasks.next" "$ledger_duplicate/tasks.md"
expect_fail ledger-duplicate-id "$ledger_duplicate" "duplicate task id DB-SQLITE-001 in task ledger"

echo "db completeness validator fixture tests passed"
