#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="aarch64-apple-darwin"
VERSION="$(sed -nE 's/^version[[:space:]]*=[[:space:]]*"([^"]+)"/\1/p' "$ROOT/Cargo.toml" | head -n 1)"
REF_NAME="${1:-v$VERSION}"
OUT_DIR="${2:-$ROOT/release-dist/macos-arm64}"

if [[ "$(uname -s)" != "Darwin" || "$(uname -m)" != "arm64" ]]; then
  echo "macOS ARM64 packaging requires a Darwin arm64 host" >&2
  exit 1
fi

"$ROOT/scripts/validate-release-version.sh" "$REF_NAME"

cargo build --locked --release \
  --target "$TARGET" \
  -p dbtool-cli \
  --no-default-features \
  --features portable

artifact_root="$(mktemp -d)"
trap 'rm -rf "$artifact_root"' EXIT
mkdir -p "$artifact_root/dbtool-bin-$TARGET"
cp "$ROOT/target/$TARGET/release/dbtool" \
  "$artifact_root/dbtool-bin-$TARGET/dbtool"

DBTOOL_PACKAGE_TARGETS="$TARGET" \
  "$ROOT/scripts/package-release.sh" "$artifact_root" "$OUT_DIR" "$REF_NAME"
DBTOOL_PACKAGE_TARGETS="$TARGET" \
  "$ROOT/scripts/smoke-release-artifacts.sh" "$OUT_DIR"

archive_name="dbtool-$REF_NAME-$TARGET.tar.gz"
archive="$OUT_DIR/$archive_name"
(
  cd "$OUT_DIR"
  shasum -a 256 "$archive_name" > "$archive_name.sha256"
)
echo "macOS ARM64 release ready: $archive"
echo "checksum ready: $archive.sha256"
