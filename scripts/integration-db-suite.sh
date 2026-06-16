#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

DEFAULT_PHASES=(
  compose-config
  service-free
  base
  flow-control
  fixture-data
  fixture-images
  data-roundtrip
  compat
  pg-compat
  tidb
)

HEAVY_PHASES=(
  dbtool-image
  compat-extra
  sqlserver
  cassandra
  tidb-secure
  tidb-ha
  tidb-pd
  tidb-pd-leader
  tidb-tikv-boundary
  tidb-cert
  tidb-logical-roundtrip
  tidb-tiproxy
  observability
)

ALL_PHASES=("${DEFAULT_PHASES[@]}" "${HEAVY_PHASES[@]}")

usage() {
  cat <<'EOF'
Run dbtool's local database verification suite.

Environment:
  DBTOOL_IT_DB_SUITE_PHASES       Space or comma separated phase list.
                                  Aliases: default, heavy, all, quick.
                                  Default: default phases.
  DBTOOL_IT_DB_SUITE_INCLUDE_HEAVY=1
                                  Run default + heavy phases when
                                  DBTOOL_IT_DB_SUITE_PHASES is unset.
  DBTOOL_IT_DB_SUITE_DRY_RUN=1    Print selected phases without running them.
  DBTOOL_IT_DB_SUITE_CONTINUE=1   Continue after a failed phase and report all
                                  failed phases at the end.

Default phases:
  compose-config service-free base flow-control fixture-data fixture-images
  data-roundtrip compat pg-compat tidb

Heavy phases:
  dbtool-image compat-extra sqlserver cassandra tidb-secure tidb-ha tidb-pd
  tidb-pd-leader tidb-tikv-boundary tidb-cert tidb-logical-roundtrip
  tidb-tiproxy observability
EOF
}

timestamp() {
  date -u "+%Y-%m-%dT%H:%M:%SZ"
}

phase_description() {
  case "$1" in
    compose-config) echo "Docker Compose integration profile config validation" ;;
    service-free) echo "service-free cargo verification and SQLite smoke" ;;
    dbtool-image) echo "containerized dbtool CLI image smoke" ;;
    base) echo "Postgres/MySQL/Redis/MongoDB live CLI workflows" ;;
    flow-control) echo "live timeout, rate/admission, and result-limit checks" ;;
    fixture-data) echo "file-backed base database fixture data" ;;
    fixture-images) echo "Dockerfile-backed base database fixture images" ;;
    data-roundtrip) echo "base SQL/KV/document dbtool logical roundtrip" ;;
    compat) echo "MariaDB and Valkey compatibility" ;;
    compat-extra) echo "MariaDB, Valkey, KeyDB, and Dragonfly compatibility" ;;
    pg-compat) echo "CockroachDB and TimescaleDB compatibility" ;;
    tidb) echo "single TiDB PD/TiKV/SQL compatibility" ;;
    tidb-secure) echo "TiDB secure HA auth/TLS lifecycle" ;;
    tidb-ha) echo "TiDB secure HA SQL-node failover drill" ;;
    tidb-pd) echo "TiDB secure HA PD quorum drill" ;;
    tidb-pd-leader) echo "TiDB secure HA current-PD-leader drill" ;;
    tidb-tikv-boundary) echo "TiDB secure HA TiKV outage bounded behavior drill" ;;
    tidb-cert) echo "TiDB secure HA certificate regeneration drill" ;;
    tidb-logical-roundtrip) echo "TiDB secure HA logical data roundtrip" ;;
    tidb-tiproxy) echo "TiDB secure HA TiProxy routing drill" ;;
    sqlserver) echo "SQL Server live SQL lifecycle" ;;
    cassandra) echo "Cassandra CQL live lifecycle" ;;
    observability) echo "OpenSearch/TLS search and Prometheus workflows" ;;
    *) return 1 ;;
  esac
}

is_known_phase() {
  phase_description "$1" >/dev/null 2>&1
}

append_phase() {
  local phase="$1"

  if ! is_known_phase "$phase"; then
    echo "unknown db suite phase: $phase" >&2
    echo >&2
    usage >&2
    exit 2
  fi

  SELECTED_PHASES+=("$phase")
}

append_phases() {
  local phase

  for phase in "$@"; do
    append_phase "$phase"
  done
}

expand_phase_token() {
  local token="$1"

  case "$token" in
    default) append_phases "${DEFAULT_PHASES[@]}" ;;
    heavy) append_phases "${HEAVY_PHASES[@]}" ;;
    all) append_phases "${ALL_PHASES[@]}" ;;
    quick) append_phases compose-config service-free base flow-control ;;
    "") ;;
    *) append_phase "$token" ;;
  esac
}

run_phase() {
  local phase="$1"

  case "$phase" in
    compose-config) "$ROOT/scripts/validate-compose-configs.sh" ;;
    service-free) "$ROOT/scripts/verify.sh" ;;
    dbtool-image) "$ROOT/scripts/smoke-docker-image.sh" ;;
    base) "$ROOT/scripts/integration-test.sh" ;;
    flow-control) "$ROOT/scripts/integration-flow-control-test.sh" ;;
    fixture-data) "$ROOT/scripts/integration-fixture-data-test.sh" ;;
    fixture-images) "$ROOT/scripts/integration-fixture-images-test.sh" ;;
    data-roundtrip) "$ROOT/scripts/integration-data-roundtrip-test.sh" ;;
    compat) "$ROOT/scripts/integration-compat-test.sh" ;;
    compat-extra)
      (
        export DBTOOL_IT_COMPAT_EXTRA=1
        "$ROOT/scripts/integration-compat-test.sh"
      )
      ;;
    pg-compat) "$ROOT/scripts/integration-pg-compat-test.sh" ;;
    tidb) "$ROOT/scripts/integration-tidb-test.sh" ;;
    tidb-secure) "$ROOT/scripts/integration-tidb-secure-test.sh" ;;
    tidb-ha) "$ROOT/scripts/integration-tidb-ha-drill.sh" ;;
    tidb-pd) "$ROOT/scripts/integration-tidb-pd-drill.sh" ;;
    tidb-pd-leader) "$ROOT/scripts/integration-tidb-pd-leader-drill.sh" ;;
    tidb-tikv-boundary) "$ROOT/scripts/integration-tidb-tikv-outage-boundary.sh" ;;
    tidb-cert) "$ROOT/scripts/integration-tidb-cert-regeneration-test.sh" ;;
    tidb-logical-roundtrip) "$ROOT/scripts/integration-tidb-logical-roundtrip-test.sh" ;;
    tidb-tiproxy) "$ROOT/scripts/integration-tidb-tiproxy-test.sh" ;;
    sqlserver) "$ROOT/scripts/integration-sqlserver-test.sh" ;;
    cassandra) "$ROOT/scripts/integration-cassandra-test.sh" ;;
    observability) "$ROOT/scripts/integration-observability-test.sh" ;;
    *)
      echo "unknown db suite phase: $phase" >&2
      return 2
      ;;
  esac
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

SELECTED_PHASES=()

if [[ -n "${DBTOOL_IT_DB_SUITE_PHASES:-}" ]]; then
  normalized="${DBTOOL_IT_DB_SUITE_PHASES//,/ }"
  for token in $normalized; do
    expand_phase_token "$token"
  done
elif [[ "${DBTOOL_IT_DB_SUITE_INCLUDE_HEAVY:-0}" == "1" ]]; then
  append_phases "${ALL_PHASES[@]}"
else
  append_phases "${DEFAULT_PHASES[@]}"
fi

if ((${#SELECTED_PHASES[@]} == 0)); then
  echo "db suite: no phases selected" >&2
  exit 2
fi

echo "db suite: selected phases"
for phase in "${SELECTED_PHASES[@]}"; do
  printf '  - %s: %s\n' "$phase" "$(phase_description "$phase")"
done

if [[ "${DBTOOL_IT_DB_SUITE_DRY_RUN:-0}" == "1" ]]; then
  echo "db suite: dry run only"
  exit 0
fi

FAILED_PHASES=()

for phase in "${SELECTED_PHASES[@]}"; do
  echo "db suite: $(timestamp) starting $phase - $(phase_description "$phase")"
  if run_phase "$phase"; then
    echo "db suite: $(timestamp) passed $phase"
  else
    status="$?"
    echo "db suite: $(timestamp) failed $phase with status $status" >&2
    FAILED_PHASES+=("$phase")
    if [[ "${DBTOOL_IT_DB_SUITE_CONTINUE:-0}" != "1" ]]; then
      exit "$status"
    fi
  fi
done

if ((${#FAILED_PHASES[@]} > 0)); then
  echo "db suite: failed phases: ${FAILED_PHASES[*]}" >&2
  exit 1
fi

echo "db suite: all selected phases passed"
