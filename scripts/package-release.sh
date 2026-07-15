#!/usr/bin/env bash
set -euo pipefail

ARTIFACT_ROOT="${1:?artifact root is required}"
OUT_DIR="${2:?output directory is required}"
REF_NAME="${3:?release ref name is required}"
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

find_binary() {
  local target="$1"
  local suffix="$2"
  local path

  for path in \
    "$ARTIFACT_ROOT/dbtool-bin-$target/dbtool$suffix" \
    "$ARTIFACT_ROOT/$target/dbtool$suffix"; do
    if [[ -f "$path" ]]; then
      printf '%s\n' "$path"
      return 0
    fi
  done

  find "$ARTIFACT_ROOT" -path "*/dbtool-bin-$target/dbtool$suffix" -type f -print -quit
}

preflight_selected_binaries() {
  local entry target suffix bin
  for entry in "${targets[@]}"; do
    target="${entry%%:*}"
    target_selected "$target" || continue
    suffix="${entry#*:}"
    bin="$(find_binary "$target" "$suffix")"
    if [[ -z "$bin" ]]; then
      echo "missing build artifact for $target" >&2
      return 1
    fi
  done
}

validate_selected_targets
preflight_selected_binaries
mkdir -p "$OUT_DIR"
cli_artifacts="$(mktemp -d)"
trap 'rm -rf "$cli_artifacts"' EXIT
"$ROOT/scripts/generate-cli-artifacts.sh" "$ARTIFACT_ROOT" "$cli_artifacts"

for entry in "${targets[@]}"; do
  target="${entry%%:*}"
  target_selected "$target" || continue
  suffix="${entry#*:}"
  bin="$(find_binary "$target" "$suffix")"

  tmp="$(mktemp -d)"
  cp "$bin" "$tmp/dbtool$suffix"
  cp -R "$cli_artifacts/completions" "$tmp/completions"
  cp -R "$cli_artifacts/man" "$tmp/man"
  chmod +x "$tmp/dbtool$suffix" 2>/dev/null || true

  archive="$OUT_DIR/dbtool-$REF_NAME-$target.tar.gz"
  tar -C "$tmp" -czf "$archive" "dbtool$suffix" "completions" "man"
  rm -rf "$tmp"
  echo "wrote $archive"
done
