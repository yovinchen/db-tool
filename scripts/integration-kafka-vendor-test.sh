#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

if [[ -z "$DBTOOL_IT_AUTOMQ_DSN" && -z "$DBTOOL_IT_WARPSTREAM_DSN" && -z "$DBTOOL_IT_CONFLUENT_DSN" ]]; then
  if [[ "${DBTOOL_IT_REQUIRE_EXTERNAL:-0}" == "1" ]]; then
    echo "dbtool vendor Kafka smoke failed: at least one of DBTOOL_IT_AUTOMQ_DSN, DBTOOL_IT_WARPSTREAM_DSN, or DBTOOL_IT_CONFLUENT_DSN is required when DBTOOL_IT_REQUIRE_EXTERNAL=1." >&2
    exit 2
  fi

  cat <<'EOF'
dbtool vendor Kafka smoke SKIP: no vendor DSN is set.

Set one or more external broker DSNs to run it:
  DBTOOL_IT_AUTOMQ_DSN='automq://host:9092'
  DBTOOL_IT_WARPSTREAM_DSN='warpstream://host:9092'
  DBTOOL_IT_CONFLUENT_DSN='confluent://user:pass@host:9092?security-protocol=SASL_SSL&sasl-mechanism=PLAIN'

Native Kafka mode maps DSN username/password and selected query params into
librdkafka config; no credentials are committed by this script.
EOF
  exit 0
fi

export DBTOOL_RUN_VENDOR_KAFKA_INTEGRATION=1

cargo test -p dbtool-cli --no-default-features --features messaging-native --test live_messaging vendor_kafka_compatible_smoke_profiles -- --nocapture
