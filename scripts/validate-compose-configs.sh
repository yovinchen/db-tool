#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${DBTOOL_COMPOSE_FILE:-$ROOT/docker-compose.integration.yml}"

run_config() {
  local name="$1"
  shift
  printf 'validating compose config: %s\n' "$name"
  docker compose -f "$COMPOSE_FILE" "$@" config >/dev/null
}

run_config "base"
run_config "fixture-images" --profile fixture-images
run_config "compat" --profile compat --profile compat-extra
run_config "pg-compat" --profile pg-compat
run_config "sqlserver" --profile sqlserver
run_config "cassandra" --profile cassandra
run_config "scylla" --profile scylla
run_config "db2" --profile db2
run_config "tidb" --profile tidb
run_config "tidb-secure" --profile tidb-secure
run_config "tidb-tiproxy" --profile tidb-secure --profile tidb-tiproxy
run_config "messaging" --profile messaging
run_config "messaging-tls" --profile messaging-tls
run_config "observability" --profile observability
run_config "opensearch-security" --profile opensearch-security
run_config "elasticsearch" --profile elasticsearch

echo "compose config validation passed"
