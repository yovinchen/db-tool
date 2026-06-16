#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST="${DBTOOL_TIDB_HA_DRILL_MANIFEST:-$ROOT/testdata/tidb-ha-drills.manifest}"
DOC="$ROOT/docs/tidb-compat-design.md"
SUITE="$ROOT/scripts/integration-db-suite.sh"

fail() {
  echo "TiDB HA drill manifest validation failed: $*" >&2
  exit 1
}

[[ -f "$MANIFEST" ]] || fail "missing manifest $MANIFEST"
[[ -f "$DOC" ]] || fail "missing documentation $DOC"
[[ -x "$SUITE" ]] || fail "suite entrypoint is not executable: $SUITE"

seen_ids=" "
seen_phases=" "
count=0

while IFS='|' read -r id phase script heading status extra; do
  [[ -z "${id:-}" || "${id:0:1}" == "#" ]] && continue
  [[ -z "${extra:-}" ]] || fail "too many fields for row '$id'"
  [[ -n "${id:-}" ]] || fail "empty id"
  [[ -n "${phase:-}" ]] || fail "empty phase for $id"
  [[ -n "${script:-}" ]] || fail "empty script for $id"
  [[ -n "${heading:-}" ]] || fail "empty documentation heading for $id"
  [[ -n "${status:-}" ]] || fail "empty status for $id"

  case "$status" in
    done|boundary) ;;
    *) fail "invalid status '$status' for $id" ;;
  esac

  case "$seen_ids" in
    *" $id "*) fail "duplicate id $id" ;;
  esac
  seen_ids="$seen_ids$id "

  [[ -x "$ROOT/$script" ]] || fail "$id script is missing or not executable: $script"
  grep -Fq "$heading" "$DOC" || fail "$id documentation heading is missing: $heading"

  case "$seen_phases" in
    *" $phase "*) ;;
    *)
      DBTOOL_IT_DB_SUITE_DRY_RUN=1 \
        DBTOOL_IT_DB_SUITE_PHASES="$phase" \
        "$SUITE" >/dev/null
      seen_phases="$seen_phases$phase "
      ;;
  esac

  count=$((count + 1))
done <"$MANIFEST"

((count > 0)) || fail "manifest has no drill rows"

for boundary in \
  "production TiKV failover" \
  "product-native" \
  "online certificate rotation" \
  "upgrade" \
  "existing-session migration"
do
  grep -Fq "$boundary" "$DOC" || fail "known boundary is not documented: $boundary"
done

echo "TiDB HA drill manifest validation passed ($count drills)"
