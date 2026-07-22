#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REF_NAME="${1:-${GITHUB_REF_NAME:-}}"

if [[ -z "$REF_NAME" ]]; then
  echo "release version validation failed: release tag is required" >&2
  exit 1
fi

workspace_version="$({
  cd "$ROOT"
  cargo metadata --no-deps --format-version 1
} | python3 -c '
import json
import sys

metadata = json.load(sys.stdin)
versions = {
    package["version"]
    for package in metadata["packages"]
    if package["name"] == "dbtool-cli"
}
if len(versions) != 1:
    raise SystemExit("dbtool-cli workspace version was not uniquely resolved")
print(versions.pop())
')"
expected="v${workspace_version}"

if [[ "$REF_NAME" != "$expected" ]]; then
  echo "release version validation failed: tag $REF_NAME does not match workspace version $expected" >&2
  exit 1
fi

if tag_commit="$(git -C "$ROOT" rev-list -n 1 "$REF_NAME" 2>/dev/null)" \
  && [[ -n "$tag_commit" ]]; then
  head_commit="$(git -C "$ROOT" rev-parse HEAD)"
  if [[ "$tag_commit" != "$head_commit" ]]; then
    echo "release version validation failed: existing tag $REF_NAME points to $tag_commit, not current commit $head_commit" >&2
    exit 1
  fi
fi

echo "release version validation passed ($expected)"
