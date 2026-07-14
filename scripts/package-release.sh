#!/usr/bin/env bash
set -euo pipefail

ARTIFACT_ROOT="${1:?artifact root is required}"
OUT_DIR="${2:?output directory is required}"
REF_NAME="${3:?release ref name is required}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

mkdir -p "$OUT_DIR"

targets=(
  "x86_64-unknown-linux-musl:"
  "aarch64-unknown-linux-musl:"
  "x86_64-apple-darwin:"
  "aarch64-apple-darwin:"
  "x86_64-pc-windows-msvc:.exe"
  "aarch64-pc-windows-msvc:.exe"
)

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

cli_artifacts="$(mktemp -d)"
"$ROOT/scripts/generate-cli-artifacts.sh" "$ARTIFACT_ROOT" "$cli_artifacts"

for entry in "${targets[@]}"; do
  target="${entry%%:*}"
  suffix="${entry#*:}"
  bin="$(find_binary "$target" "$suffix")"
  if [[ -z "$bin" ]]; then
    echo "missing build artifact for $target" >&2
    exit 1
  fi

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

rm -rf "$cli_artifacts"
