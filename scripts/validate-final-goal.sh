#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

fail() {
  echo "final goal validation failed: $*" >&2
  exit 1
}

require_file() {
  local path="$1"
  [[ -f "$ROOT/$path" ]] || fail "missing file: $path"
}

require_executable() {
  local path="$1"
  [[ -x "$ROOT/$path" ]] || fail "missing executable: $path"
}

require_pattern() {
  local path="$1"
  local pattern="$2"
  grep -Fq "$pattern" "$ROOT/$path" || fail "missing pattern '$pattern' in $path"
}

require_no_pattern() {
  local path="$1"
  local pattern="$2"
  if grep -Fq "$pattern" "$ROOT/$path"; then
    fail "stale pattern '$pattern' remains in $path"
  fi
}

targets=(
  x86_64-unknown-linux-musl
  aarch64-unknown-linux-musl
  x86_64-apple-darwin
  aarch64-apple-darwin
  x86_64-pc-windows-msvc
  aarch64-pc-windows-msvc
)

require_file "docs/final-goal-audit.md"
require_file "docs/implementation-status.md"
require_file "docs/tasks.md"
require_file "SKILL.md"
require_file ".github/workflows/release.yml"
require_file "dist/npm/package.json"
require_file "dist/npm/bin/dbtool.js"
require_file "dist/python/pyproject.toml"
require_file "dist/python/dbtool_bin/cli.py"
require_file "dist/mise/README.md"
require_file "crates/dbtool-registry/tests/embedded_library.rs"

require_executable "scripts/package-release.sh"
require_executable "scripts/smoke-binary.sh"
require_executable "scripts/smoke-release-artifacts.sh"
require_executable "scripts/validate-tidb-ha-drills.sh"

for target in "${targets[@]}"; do
  require_pattern ".github/workflows/release.yml" "$target"
  require_pattern "scripts/package-release.sh" "$target"
  require_pattern "scripts/package-npm.mjs" "$target"
  require_pattern "scripts/package-python-wheel.py" "$target"
  require_pattern "dist/mise/README.md" "$target"
  require_pattern "scripts/smoke-release-artifacts.sh" "$target"
done

require_pattern "dist/npm/bin/dbtool.js" "DBTOOL_BINARY"
require_pattern "dist/python/dbtool_bin/cli.py" "DBTOOL_BINARY"
require_pattern "dist/python/pyproject.toml" "dbtool = \"dbtool_bin.cli:main\""

for alias in \
  '"mysql"' '"mariadb"' '"tidb"' \
  '"postgres"' '"postgresql"' '"cockroach"' '"timescale"' '"redshift"' \
  '"redis"' '"valkey"' '"keydb"' '"dragonfly"' \
  '"kafka"' '"automq"' '"redpanda"' '"warpstream"' '"confluent"' \
  '"opensearch"' '"elasticsearch"'
do
  require_pattern "crates/dbtool-core/src/registry/alias.rs" "$alias"
done

for family in \
  "SQLite" "PostgreSQL" "MySQL" "MongoDB" "Redis" "Kafka" "AMQP" \
  "NATS" "OpenSearch" "Elasticsearch" "Prometheus" "SQL Server" \
  "Cassandra" "TiDB" "AutoMQ"
do
  require_pattern "docs/implementation-status.md" "$family"
done

require_pattern "crates/dbtool-core/src/config/env.rs" "DBTOOL_CONN_"
require_pattern "crates/dbtool-core/src/service/safety.rs" "ConfirmRequired"
require_pattern "crates/dbtool-core/src/service/safety.rs" "WriteNotAllowed"
require_pattern "crates/dbtool-core/src/service/throttle.rs" "max_retries"
require_pattern "crates/dbtool-core/src/service/throttle.rs" "request_timeout"
require_pattern "crates/dbtool-cli/tests/cli_json.rs" "destructive_sql_uses_two_step_confirm_token"
require_pattern "crates/dbtool-cli/tests/cli_json.rs" "search_index_requires_write_flag_before_connecting"
require_pattern "crates/dbtool-cli/tests/cli_json.rs" "ts_write_requires_write_flag_before_connecting"
require_pattern "crates/dbtool-registry/tests/embedded_library.rs" "ConnectionManager"
require_pattern "crates/dbtool-registry/tests/embedded_library.rs" "FlowControl"
require_pattern "crates/dbtool-tui/src/state.rs" "CommandFormState"
require_pattern "crates/dbtool-tui/src/app.rs" "pending_write"

if grep -Eq 'Pending|In progress|Deferred|\[ \]' "$ROOT/docs/tasks.md"; then
  fail "docs/tasks.md still contains an unfinished task marker"
fi

require_no_pattern "docs/implementation-status.md" "Prometheus remote write | Not supported"
require_no_pattern "docs/implementation-status.md" "TUI rich workflows"
require_no_pattern "docs/implementation-status.md" "Next Implementation Queue"

require_pattern "docs/final-goal-audit.md" "The repo satisfies the stated dbtool objective"
require_pattern "docs/final-goal-audit.md" "Product-specific production-readiness exercises remain explicit boundaries"

echo "final goal validation passed"
