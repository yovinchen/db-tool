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
require_executable "scripts/generate-cli-artifacts.sh"
require_executable "scripts/smoke-binary.sh"
require_executable "scripts/smoke-release-artifacts.sh"
require_executable "scripts/validate-tidb-ha-drills.sh"
require_executable "scripts/integration-elasticsearch-up.sh"
require_executable "scripts/integration-elasticsearch-test.sh"
require_executable "scripts/integration-opensearch-security-prepare.sh"
require_executable "scripts/integration-opensearch-security-up.sh"
require_executable "scripts/integration-opensearch-security-test.sh"
require_executable "scripts/integration-kafka-vendor-test.sh"
require_executable "scripts/integration-redshift-test.sh"

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
require_pattern "crates/dbtool-cli/src/main.rs" "generate-artifacts"
require_pattern "scripts/package-release.sh" "completions"
require_pattern "scripts/package-release.sh" "man"
require_pattern "scripts/package-npm.mjs" "copyCliArtifacts"
require_pattern "scripts/package-python-wheel.py" "generate_cli_artifacts"
require_pattern "scripts/smoke-release-artifacts.sh" "completions/dbtool.bash"
require_pattern "scripts/smoke-release-artifacts.sh" "man/dbtool.1"

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
  "NATS" "OpenSearch" "Elasticsearch" "Prometheus" "SQL Server" "CQL" \
  "Cassandra" "TiDB" "AutoMQ"
do
  require_pattern "docs/implementation-status.md" "$family"
done

require_pattern "crates/dbtool-core/src/config/env.rs" "DBTOOL_CONN_"
require_pattern "crates/dbtool-core/src/service/safety.rs" "ConfirmRequired"
require_pattern "crates/dbtool-core/src/service/safety.rs" "WriteNotAllowed"
require_pattern "crates/dbtool-core/src/service/throttle.rs" "max_retries"
require_pattern "crates/dbtool-core/src/service/throttle.rs" "request_timeout"
require_pattern "crates/dbtool-core/src/port/capability.rs" "trait CqlEngine"
require_pattern "crates/dbtool-core/src/port/connector.rs" "pub cql: bool"
require_pattern "crates/adapter-cassandra/src/lib.rs" "impl CqlEngine for CassandraAdapter"
require_pattern "crates/dbtool-cli/src/main.rs" "Cql(cmd::cql::CqlCmd)"
require_pattern "crates/dbtool-cli/src/main.rs" "Export(cmd::transfer::ExportCmd)"
require_pattern "crates/dbtool-cli/src/main.rs" "Import(cmd::transfer::ImportCmd)"
require_pattern "crates/dbtool-cli/src/cmd/cql.rs" "pub enum CqlAction"
require_pattern "crates/dbtool-cli/src/cmd/transfer.rs" "enum TransferArtifact"
require_pattern "crates/dbtool-cli/src/cmd/transfer.rs" "ensure_write_allowed(ctx)?"
require_pattern "crates/dbtool-cli/tests/cli_json.rs" "destructive_sql_uses_two_step_confirm_token"
require_pattern "crates/dbtool-cli/tests/cli_json.rs" "cli_help_documents_core_command_families"
require_pattern "crates/dbtool-cli/tests/cli_json.rs" "export_import_sql_round_trips_sqlite_rows"
require_pattern "crates/dbtool-cli/tests/cli_json.rs" "cql_exec_requires_write_flag_before_connecting"
require_pattern "crates/dbtool-cli/tests/cli_json.rs" "search_index_requires_write_flag_before_connecting"
require_pattern "crates/dbtool-cli/tests/cli_json.rs" "ts_write_requires_write_flag_before_connecting"
require_pattern "scripts/integration-data-roundtrip-test.sh" "export sql"
require_pattern "scripts/integration-data-roundtrip-test.sh" "import sql"
require_pattern "scripts/integration-data-roundtrip-test.sh" "export kv"
require_pattern "scripts/integration-data-roundtrip-test.sh" "import kv"
require_pattern "scripts/integration-data-roundtrip-test.sh" "export doc"
require_pattern "scripts/integration-data-roundtrip-test.sh" "import doc"
require_pattern "docker-compose.integration.yml" 'profiles: ["elasticsearch"]'
require_pattern "docker-compose.integration.yml" 'profiles: ["opensearch-security"]'
require_pattern "scripts/validate-compose-configs.sh" "elasticsearch"
require_pattern "scripts/validate-compose-configs.sh" "opensearch-security"
require_pattern "scripts/integration-db-suite.sh" "elasticsearch"
require_pattern "scripts/integration-db-suite.sh" "opensearch-security"
require_pattern ".github/workflows/ci.yml" "run_live_elasticsearch"
require_pattern ".github/workflows/ci.yml" "run_live_opensearch_security"
require_pattern "crates/dbtool-cli/tests/live_observability.rs" "elasticsearch_native_live_index_search_and_list"
require_pattern "crates/dbtool-cli/tests/live_observability.rs" "opensearch_security_tls_live_index_search_and_list"
require_pattern "scripts/integration-opensearch-security-prepare.sh" "DBTOOL_IT_OPENSEARCH_SECURITY_DSN"
require_pattern "crates/adapter-kafka/src/backend/rdkafka_backend.rs" "sasl.username"
require_pattern "crates/adapter-kafka/src/backend/rdkafka_backend.rs" "security.protocol"
require_pattern "scripts/integration-kafka-vendor-test.sh" "DBTOOL_IT_AUTOMQ_DSN"
require_pattern "scripts/integration-kafka-vendor-test.sh" "DBTOOL_IT_WARPSTREAM_DSN"
require_pattern "scripts/integration-kafka-vendor-test.sh" "DBTOOL_IT_CONFLUENT_DSN"
require_pattern "scripts/integration-db-suite.sh" "kafka-vendors"
require_pattern ".github/workflows/ci.yml" "run_live_kafka_vendors"
require_pattern "crates/dbtool-cli/tests/live_messaging.rs" "vendor_kafka_compatible_smoke_profiles"
require_pattern "scripts/integration-redshift-test.sh" "DBTOOL_IT_REDSHIFT_DSN"
require_pattern "scripts/integration-db-suite.sh" "redshift"
require_pattern ".github/workflows/ci.yml" "run_live_redshift"
require_pattern "crates/dbtool-cli/tests/live_services.rs" "redshift_external_sql_lifecycle_and_typed_values"
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
require_no_pattern "docs/implementation-status.md" "Shell completions and manpage artifacts | Makes local installation"
require_no_pattern "docs/implementation-status.md" "CLI discoverability polish | Keeps the tool"
require_no_pattern "docs/implementation-status.md" "Dedicated CQL command surface | Cassandra works today"
require_no_pattern "docs/implementation-status.md" "Generic export/import CLI | Integration scripts prove"
require_no_pattern "docs/implementation-status.md" "Product-native Elasticsearch profile | The shared"
require_no_pattern "docs/implementation-status.md" "Vendor Kafka-compatible smoke profiles | AutoMQ"
require_no_pattern "docs/implementation-status.md" "OpenSearch security-plugin TLS profile | Current HTTPS"
require_no_pattern "docs/implementation-status.md" "Real OpenSearch security-plugin TLS profile"
require_no_pattern "docs/implementation-status.md" "| Candidate | Why it helps"
require_no_pattern "docs/implementation-status.md" "Not live-tested against Redshift"
require_no_pattern "docs/implementation-status.md" "Cassandra trait split"
require_no_pattern "docs/extended-backends.md" 'A future `CqlEngine` can be added'

require_pattern "docs/final-goal-audit.md" "The repo satisfies the stated dbtool objective"
require_pattern "docs/final-goal-audit.md" "Product-specific production-readiness exercises remain explicit boundaries"
require_pattern "docs/implementation-status.md" "export sql"
require_pattern "docs/implementation-status.md" "integration-elasticsearch-test.sh"
require_pattern "docs/implementation-status.md" "integration-opensearch-security-test.sh"
require_pattern "docs/implementation-status.md" "integration-kafka-vendor-test.sh"
require_pattern "docs/implementation-status.md" "integration-redshift-test.sh"
require_pattern "docs/implementation-status.md" "No open recommended enhancement candidates remain"
require_pattern "docs/tasks.md" "T35 Generic export/import CLI"
require_pattern "docs/tasks.md" "T36 Product-native Elasticsearch profile"
require_pattern "docs/tasks.md" "T37 Vendor Kafka-compatible smoke profiles"
require_pattern "docs/tasks.md" "T38 OpenSearch security-plugin TLS profile"
require_pattern "docs/tasks.md" "T39 External Redshift compatibility smoke"

echo "final goal validation passed"
