#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST="${DBTOOL_COMPLETENESS_MANIFEST:-$ROOT/testdata/db-completeness.manifest}"
TASKS="${DBTOOL_COMPLETENESS_TASKS:-$ROOT/docs/db-completeness-tasks.md}"
EXPECTED_TASK_COUNT=27

fail() {
  echo "db completeness validation failed: $*" >&2
  exit 1
}

[[ -f "$MANIFEST" ]] || fail "missing manifest: $MANIFEST"
[[ -f "$TASKS" ]] || fail "missing task ledger: $TASKS"
grep -Fq "this commit" "$TASKS" && fail "task ledger contains unresolved commit placeholders"

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/dbtool-completeness.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT
manifest_index="$work_dir/manifest-index"
ledger_index="$work_dir/ledger-index"
ledger_rows="$work_dir/ledger-rows"
ledger_commit_fields="$work_dir/ledger-commit-fields"
commit_refs="$work_dir/commit-refs"
: >"$manifest_index"
: >"$ledger_index"
: >"$ledger_rows"
: >"$ledger_commit_fields"
: >"$commit_refs"

validate_resource_operations() {
  local id="$1"
  local evidence_path="$2"

  awk -F'|' -v id="$id" '
    function trim(value) {
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      return value
    }

    /^Resource operations:/ && !done {
      marker = 1
      in_table = 1
      next
    }

    in_table && /^\|/ {
      first = trim($2)
      if (first == "Resource" || first == "Operation") {
        mode = tolower(first)
        header = 1
        last = ""
        for (i = NF; i >= 2; i--) {
          value = trim($i)
          if (value != "") {
            last = value
            break
          }
        }
        if ((mode == "resource" && last != "Cleanup") ||
            (mode == "operation" && last != "Result")) {
          printf "db completeness validation failed: %s evidence resource table has an invalid final column: %s\n", id, last > "/dev/stderr"
          bad = 1
        }
        next
      }
      if (first ~ /^-+$/) {
        next
      }
      if (header) {
        last = ""
        for (i = NF; i >= 2; i--) {
          value = trim($i)
          if (value != "") {
            last = value
            break
          }
        }
        rows++
        if (last !~ /PASS|UNSUPPORTED/) {
          printf "db completeness validation failed: %s evidence operation row %s lacks PASS/UNSUPPORTED cleanup or result\n", id, first > "/dev/stderr"
          bad = 1
        }
        if (mode == "operation" && first == "Cleanup") {
          cleanup_row = 1
        }
      }
      next
    }

    in_table && header && rows > 0 && !/^\|/ {
      in_table = 0
      done = 1
    }

    END {
      if (!marker) {
        printf "db completeness validation failed: %s evidence lacks Resource operations table\n", id > "/dev/stderr"
        bad = 1
      } else if (!header) {
        printf "db completeness validation failed: %s evidence lacks a Resource/Operation table header\n", id > "/dev/stderr"
        bad = 1
      } else if (rows < 1) {
        printf "db completeness validation failed: %s evidence resource table has no operation rows\n", id > "/dev/stderr"
        bad = 1
      } else if (mode == "operation" && !cleanup_row) {
        printf "db completeness validation failed: %s operation matrix lacks an explicit Cleanup row\n", id > "/dev/stderr"
        bad = 1
      }
      exit bad
    }
  ' "$evidence_path"
}

collect_explicit_evidence_commits() {
  local evidence_path="$1"

  grep -Ei '(^|[[:space:]])((implementation|verification|campaign)[[:space:]]+)?commits?:' "$evidence_path" \
    | grep -Eo '`[0-9a-f]{7,40}`' \
    | tr -d '`' >>"$commit_refs" || true
}

seen_ids="|"
count=0

while IFS='|' read -r id family product schemes feature environment runner status evidence boundary extra; do
  [[ -z "$id" || "$id" == \#* ]] && continue
  [[ -z "${extra:-}" ]] || fail "$id has more than 10 fields"
  [[ "$id" =~ ^DB-[A-Z0-9-]+-[0-9]{3}$ ]] || fail "invalid task id $id"

  for value in "$id" "$family" "$product" "$schemes" "$feature" "$environment" "$runner" "$status" "$evidence" "$boundary"; do
    [[ -n "$value" ]] || fail "$id contains an empty field"
  done

  case "$seen_ids" in
    *"|$id|"*) fail "duplicate task id $id in manifest" ;;
  esac
  seen_ids="${seen_ids}${id}|"
  count=$((count + 1))
  printf '%s|%s\n' "$id" "$status" >>"$manifest_index"

  case "$family" in
    sql|cql|kv|document|search|timeseries|messaging) ;;
    *) fail "$id has unknown family $family" ;;
  esac

  case "$status" in
    NOT_RUN|HARNESS_READY|LIVE_PASS|COMPLETE|BLOCKED|EXTERNAL|PARTIAL) ;;
    *) fail "$id has unknown status $status" ;;
  esac

  if [[ "$runner" == ./* ]]; then
    IFS=',' read -r -a runner_paths <<<"$runner"
    for runner_path in "${runner_paths[@]}"; do
      [[ "$runner_path" == ./* ]] || fail "$id has an invalid runner path: $runner_path"
      [[ -x "$ROOT/${runner_path#./}" ]] || fail "$id runner is not executable: $runner_path"
    done
  fi

  if [[ "$feature" != "default" && "$runner" == ./* ]]; then
    IFS=',' read -r -a feature_names <<<"$feature"
    for feature_name in "${feature_names[@]}"; do
      feature_found=0
      for runner_path in "${runner_paths[@]}"; do
        if grep -Fq -- "--features $feature_name" "$ROOT/${runner_path#./}"; then
          feature_found=1
          break
        fi
      done
      ((feature_found == 1)) \
        || fail "$id feature $feature_name is not used by its declared runner(s)"
    done
  fi

  case "$status" in
    LIVE_PASS|COMPLETE)
      [[ "$evidence" != "-" ]] || fail "$id is $status without evidence"
      evidence_path="$ROOT/$evidence"
      [[ -f "$evidence_path" ]] || fail "$id evidence does not exist: $evidence"
      grep -Fq "Task ID: $id" "$evidence_path" || fail "$id evidence has wrong Task ID"
      grep -Fq "Result: LIVE_PASS" "$evidence_path" || fail "$id evidence is not LIVE_PASS"
      grep -Eq '^Run at \(UTC\): .+' "$evidence_path" || fail "$id evidence lacks UTC time"
      grep -Eq '^Environment: .+' "$evidence_path" || fail "$id evidence lacks environment"
      grep -Eq '^Product version: .+' "$evidence_path" || fail "$id evidence lacks product version"
      grep -Eq '^Command: .+' "$evidence_path" || fail "$id evidence lacks command"
      validate_resource_operations "$id" "$evidence_path"
      grep -Eq '^Cleanup: (PASS|UNSUPPORTED)' "$evidence_path" || fail "$id evidence lacks cleanup result"
      grep -Fq "this commit" "$evidence_path" && fail "$id evidence contains an unresolved commit placeholder"
      collect_explicit_evidence_commits "$evidence_path"
      ;;
    BLOCKED|EXTERNAL|PARTIAL)
      [[ "$boundary" != "-" ]] || fail "$id $status requires a concrete boundary"
      ;;
  esac
done <"$MANIFEST"

((count == EXPECTED_TASK_COUNT)) \
  || fail "manifest must contain exactly $EXPECTED_TASK_COUNT tasks, found $count"

awk -F'|' -v rows="$ledger_rows" -v commits="$ledger_commit_fields" '
  function trim(value) {
    gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
    return value
  }

  /^## Execution Task Table[[:space:]]*$/ {
    section = 1
    found_section = 1
    next
  }
  section && /^## / {
    section = 0
  }
  section && /^\|/ {
    id = trim($2)
    if (id == "" || id == "Task" || id ~ /^-+$/) {
      next
    }
    status = trim($7)
    if (id !~ /^DB-[A-Z0-9-]+-[0-9][0-9][0-9]$/) {
      printf "db completeness validation failed: invalid task-ledger id %s\n", id > "/dev/stderr"
      bad = 1
      next
    }
    if (status !~ /^(NOT_RUN|HARNESS_READY|LIVE_PASS|COMPLETE|BLOCKED|EXTERNAL|PARTIAL)$/) {
      printf "db completeness validation failed: %s has unknown task-ledger status %s\n", id, status > "/dev/stderr"
      bad = 1
      next
    }
    print id "|" status >> rows
    print id "|" status "|" trim($9) >> commits
  }
  END {
    if (!found_section) {
      print "db completeness validation failed: task ledger lacks Execution Task Table" > "/dev/stderr"
      bad = 1
    }
    exit bad
  }
' "$TASKS"

cp "$ledger_rows" "$ledger_index"

awk -F'|' '
  NR == FNR {
    manifest[$1] = $2
    next
  }
  {
    if (++seen[$1] > 1) {
      printf "db completeness validation failed: duplicate task id %s in task ledger\n", $1 > "/dev/stderr"
      bad = 1
    }
    ledger[$1] = $2
  }
  END {
    for (id in manifest) {
      if (!(id in ledger)) {
        printf "db completeness validation failed: missing task id %s from task ledger\n", id > "/dev/stderr"
        bad = 1
      } else if (manifest[id] != ledger[id]) {
        printf "db completeness validation failed: status mismatch for %s: manifest=%s ledger=%s\n", id, manifest[id], ledger[id] > "/dev/stderr"
        bad = 1
      }
    }
    for (id in ledger) {
      if (!(id in manifest)) {
        printf "db completeness validation failed: extra task id %s in task ledger\n", id > "/dev/stderr"
        bad = 1
      }
    }
    exit bad
  }
' "$manifest_index" "$ledger_index"

ledger_count="$(wc -l <"$ledger_index" | tr -d '[:space:]')"
((ledger_count == EXPECTED_TASK_COUNT)) \
  || fail "task ledger must contain exactly $EXPECTED_TASK_COUNT tasks, found $ledger_count"

while IFS='|' read -r id status commit_field; do
  row_refs="$work_dir/row-commit-refs"
  : >"$row_refs"
  printf '%s\n' "$commit_field" \
    | grep -Eo '`[0-9a-f]{7,40}`' \
    | tr -d '`' >"$row_refs" || true
  if [[ "$status" == "COMPLETE" && ! -s "$row_refs" ]]; then
    fail "$id COMPLETE task-ledger row lacks an explicit commit SHA"
  fi
  cat "$row_refs" >>"$commit_refs"
done <"$ledger_commit_fields"

if git -C "$ROOT" rev-parse --git-dir >/dev/null 2>&1; then
  shallow="$(git -C "$ROOT" rev-parse --is-shallow-repository 2>/dev/null || echo unknown)"
  if [[ "$shallow" == "true" ]]; then
    echo "db completeness validation note: commit SHA ancestry checks skipped in a shallow clone; use checkout fetch-depth: 0 for full provenance validation" >&2
  else
    sort -u "$commit_refs" -o "$commit_refs"
    while IFS= read -r sha; do
      [[ -n "$sha" ]] || continue
      git -C "$ROOT" cat-file -e "${sha}^{commit}" 2>/dev/null \
        || fail "referenced commit $sha does not exist"
      git -C "$ROOT" merge-base --is-ancestor "$sha" HEAD \
        || fail "referenced commit $sha is not an ancestor of HEAD"
    done <"$commit_refs"
  fi
else
  echo "db completeness validation note: commit SHA ancestry checks skipped outside a Git worktree" >&2
fi

echo "db completeness manifest validation passed ($count tasks; manifest and ledger synchronized)"
