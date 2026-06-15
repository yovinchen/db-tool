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

for entry in "${targets[@]}"; do
  target="${entry%%:*}"
  suffix="${entry#*:}"
  archive="$(find "$ARCHIVE_DIR" -name "dbtool-*-$target.tar.gz" -type f -print -quit)"
  if [[ -z "$archive" ]]; then
    echo "missing release archive for $target" >&2
    exit 1
  fi

  tmp="$(mktemp -d)"
  tar -C "$tmp" -xzf "$archive"
  "$ROOT/scripts/smoke-binary.sh" "$target" "$tmp/dbtool$suffix"
  rm -rf "$tmp"
done
