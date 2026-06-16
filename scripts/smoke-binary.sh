#!/usr/bin/env bash
set -euo pipefail

TARGET="${1:?target triple is required}"
BIN="${2:?binary path is required}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ! -f "$BIN" ]]; then
  echo "missing binary: $BIN" >&2
  exit 1
fi

chmod +x "$BIN" 2>/dev/null || true

host_os="$(uname -s)"
host_arch="$(uname -m)"
can_run=0

case "$host_os:$host_arch:$TARGET" in
  Linux:x86_64:x86_64-unknown-linux-musl) can_run=1 ;;
  Linux:aarch64:aarch64-unknown-linux-musl) can_run=1 ;;
  Darwin:x86_64:x86_64-apple-darwin) can_run=1 ;;
  Darwin:arm64:aarch64-apple-darwin) can_run=1 ;;
  MINGW*:x86_64:x86_64-pc-windows-msvc) can_run=1 ;;
  MSYS*:x86_64:x86_64-pc-windows-msvc) can_run=1 ;;
  CYGWIN*:x86_64:x86_64-pc-windows-msvc) can_run=1 ;;
esac

if [[ "$can_run" == "1" ]]; then
  "$BIN" --version >/dev/null
  case "$TARGET" in
    *windows*)
      ;;
    *)
      "$ROOT/scripts/smoke-core-flow.sh" "$BIN" >/dev/null
      ;;
  esac
else
  echo "structural smoke only for $TARGET on $host_os/$host_arch"
fi
