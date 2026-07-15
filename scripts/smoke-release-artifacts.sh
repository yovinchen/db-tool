#!/usr/bin/env bash
set -euo pipefail

ARCHIVE_DIR="${1:?archive directory is required}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

targets=(
  "x86_64-unknown-linux-musl:"
  "aarch64-unknown-linux-musl:"
  "x86_64-apple-darwin:"
  "aarch64-apple-darwin:"
  "x86_64-pc-windows-msvc:.exe"
  "aarch64-pc-windows-msvc:.exe"
)

validate_selected_targets() {
  local requested="${DBTOOL_PACKAGE_TARGETS:-}"
  local name entry found seen=","
  local requested_targets=()
  if [[ -z "$requested" ]]; then
    return 0
  fi

  case "$requested" in
    ,*|*,|*,,*)
      echo "DBTOOL_PACKAGE_TARGETS must be a comma-separated list without empty entries" >&2
      return 1
      ;;
  esac

  IFS=',' read -r -a requested_targets <<< "$requested"
  for name in "${requested_targets[@]}"; do
    if [[ "$seen" == *",$name,"* ]]; then
      echo "DBTOOL_PACKAGE_TARGETS must not contain duplicate target: $name" >&2
      return 1
    fi
    seen="${seen}${name},"
    found=0
    for entry in "${targets[@]}"; do
      if [[ "${entry%%:*}" == "$name" ]]; then
        found=1
        break
      fi
    done
    if [[ "$found" == "0" ]]; then
      echo "unsupported DBTOOL_PACKAGE_TARGETS entry: $name" >&2
      return 1
    fi
  done
}

target_selected() {
  [[ -z "${DBTOOL_PACKAGE_TARGETS:-}" ]] ||
    [[ ",${DBTOOL_PACKAGE_TARGETS}," == *",$1,"* ]]
}

validate_selected_targets
for entry in "${targets[@]}"; do
  target="${entry%%:*}"
  target_selected "$target" || continue
  suffix="${entry#*:}"
  archive="$(find "$ARCHIVE_DIR" -name "dbtool-*-$target.tar.gz" -type f -print -quit)"
  if [[ -z "$archive" ]]; then
    echo "missing release archive for $target" >&2
    exit 1
  fi

  tmp="$(mktemp -d)"
  tar -C "$tmp" -xzf "$archive"
  "$ROOT/scripts/smoke-binary.sh" "$target" "$tmp/dbtool$suffix"
  for artifact in \
    completions/dbtool.bash \
    completions/dbtool.zsh \
    completions/dbtool.fish \
    man/dbtool.1; do
    if [[ ! -f "$tmp/$artifact" ]]; then
      echo "missing release artifact $artifact in $archive" >&2
      exit 1
    fi
  done
  grep -Fq "complete -F _dbtool dbtool" "$tmp/completions/dbtool.bash"
  grep -Fq "#compdef dbtool" "$tmp/completions/dbtool.zsh"
  grep -Fq "complete -c dbtool" "$tmp/completions/dbtool.fish"
  grep -Fq ".TH DBTOOL 1" "$tmp/man/dbtool.1"
  rm -rf "$tmp"
done
