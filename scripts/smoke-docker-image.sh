#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${DBTOOL_DOCKER_IMAGE:-dbtool:smoke}"

docker build \
  -f "$ROOT/docker/dbtool/Dockerfile" \
  -t "$IMAGE" \
  "$ROOT"

"$ROOT/scripts/smoke-core-flow.sh" "docker://$IMAGE"
