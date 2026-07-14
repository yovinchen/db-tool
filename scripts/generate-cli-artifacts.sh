#!/usr/bin/env bash
set -euo pipefail

ARTIFACT_ROOT="${1:?artifact root is required}"
OUT_DIR="${2:?output directory is required}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

find_binary() {
  local target="$1"
  local suffix="$2"
  local path

  for path in \
    "$ARTIFACT_ROOT/dbtool-bin-$target/dbtool$suffix" \
    "$ARTIFACT_ROOT/$target/dbtool$suffix" \
    "$ARTIFACT_ROOT/dbtool$suffix"; do
    if [[ -f "$path" ]]; then
      printf '%s\n' "$path"
      return 0
    fi
  done

  find "$ARTIFACT_ROOT" -path "*/dbtool-bin-$target/dbtool$suffix" -type f -print -quit
}

host_target_entries() {
  case "$(uname -s):$(uname -m)" in
    Linux:x86_64) echo "x86_64-unknown-linux-musl:" ;;
    Linux:aarch64|Linux:arm64) echo "aarch64-unknown-linux-musl:" ;;
    Darwin:x86_64) echo "x86_64-apple-darwin:" ;;
    Darwin:arm64) echo "aarch64-apple-darwin:" ;;
  esac
}

mkdir -p "$OUT_DIR"

while IFS= read -r entry; do
  [[ -n "$entry" ]] || continue
  target="${entry%%:*}"
  suffix="${entry#*:}"
  bin="$(find_binary "$target" "$suffix")"
  if [[ -n "$bin" ]]; then
    chmod +x "$bin" 2>/dev/null || true
    "$bin" generate-artifacts --out-dir "$OUT_DIR"
    exit 0
  fi
done < <(host_target_entries)

if command -v cargo >/dev/null 2>&1; then
  cargo run --quiet -p dbtool-cli -- generate-artifacts --out-dir "$OUT_DIR"
  exit 0
fi

echo "could not find a runnable dbtool artifact and cargo is unavailable" >&2
exit 1
