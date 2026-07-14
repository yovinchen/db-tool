#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST="$ROOT/testdata/db-completeness.manifest"
TASKS="$ROOT/docs/db-completeness-tasks.md"

fail() {
  echo "db completeness validation failed: $*" >&2
  exit 1
}

[[ -f "$MANIFEST" ]] || fail "missing manifest"
[[ -f "$TASKS" ]] || fail "missing task ledger"

seen_ids="|"
count=0

while IFS='|' read -r id family product schemes feature environment runner status evidence boundary extra; do
  [[ -z "$id" || "$id" == \#* ]] && continue
  [[ -z "${extra:-}" ]] || fail "$id has more than 10 fields"

  for value in "$id" "$family" "$product" "$schemes" "$feature" "$environment" "$runner" "$status" "$evidence" "$boundary"; do
    [[ -n "$value" ]] || fail "$id contains an empty field"
  done

  case "$seen_ids" in
    *"|$id|"*) fail "duplicate task id $id" ;;
  esac
  seen_ids="${seen_ids}${id}|"
  count=$((count + 1))

  case "$family" in
    sql|cql|kv|document|search|timeseries|messaging) ;;
    *) fail "$id has unknown family $family" ;;
  esac

  case "$status" in
    NOT_RUN|HARNESS_READY|LIVE_PASS|COMPLETE|BLOCKED|EXTERNAL|PARTIAL) ;;
    *) fail "$id has unknown status $status" ;;
  esac

  grep -Fq "| $id |" "$TASKS" || fail "$id is missing from the task ledger"

  if [[ "$runner" == ./* ]]; then
    runner_path="${runner%% *}"
    [[ -x "$ROOT/${runner_path#./}" ]] || fail "$id runner is not executable: $runner_path"
  fi

  case "$status" in
    LIVE_PASS|COMPLETE)
      [[ "$evidence" != "-" ]] || fail "$id is $status without evidence"
      evidence_path="$ROOT/$evidence"
      [[ -f "$evidence_path" ]] || fail "$id evidence does not exist: $evidence"
      grep -Fq "Task ID: $id" "$evidence_path" || fail "$id evidence has wrong Task ID"
      grep -Fq "Result: LIVE_PASS" "$evidence_path" || fail "$id evidence is not LIVE_PASS"
      grep -Fq "Run at (UTC):" "$evidence_path" || fail "$id evidence lacks UTC time"
      grep -Fq "Command:" "$evidence_path" || fail "$id evidence lacks command"
      grep -Eq '^Cleanup: (PASS|UNSUPPORTED)' "$evidence_path" || fail "$id evidence lacks cleanup result"
      ;;
    BLOCKED|EXTERNAL|PARTIAL)
      [[ "$boundary" != "-" ]] || fail "$id $status requires a concrete boundary"
      ;;
  esac
done < "$MANIFEST"

((count >= 20)) || fail "manifest unexpectedly small: $count entries"

echo "db completeness manifest validation passed ($count tasks)"
