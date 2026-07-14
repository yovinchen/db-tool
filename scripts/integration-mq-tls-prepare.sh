#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

case "$DBTOOL_IT_MQ_TLS_DIR" in
  /*) ;;
  *) DBTOOL_IT_MQ_TLS_DIR="$ROOT/$DBTOOL_IT_MQ_TLS_DIR" ;;
esac
export DBTOOL_IT_MQ_TLS_DIR

CERT_DIR="$DBTOOL_IT_MQ_TLS_DIR/certs"
mkdir -p "$CERT_DIR"

CERT_RENEWAL_SECONDS="${DBTOOL_IT_MQ_TLS_CERT_RENEWAL_SECONDS:-3600}"

cert_is_current() {
  local cert="$1"
  [[ -f "$cert" ]] && openssl x509 \
    -checkend "$CERT_RENEWAL_SECONDS" \
    -noout \
    -in "$cert" >/dev/null 2>&1
}

if [[ "${DBTOOL_IT_MQ_TLS_REGENERATE_CERTS:-0}" == "1" ]] \
  || [[ ! -f "$CERT_DIR/ca-key.pem" ]] \
  || [[ ! -f "$CERT_DIR/rabbitmq-key.pem" ]] \
  || [[ ! -f "$CERT_DIR/nats-key.pem" ]] \
  || ! cert_is_current "$CERT_DIR/ca.pem" \
  || ! cert_is_current "$CERT_DIR/rabbitmq.pem" \
  || ! cert_is_current "$CERT_DIR/nats.pem"; then
  rm -f "$CERT_DIR"/*
fi

if [[ ! -f "$CERT_DIR/ca.pem" ]]; then
  openssl req \
    -x509 \
    -newkey rsa:2048 \
    -nodes \
    -days "${DBTOOL_IT_MQ_TLS_CERT_DAYS:-7}" \
    -subj "/CN=dbtool-it-mq-ca" \
    -keyout "$CERT_DIR/ca-key.pem" \
    -out "$CERT_DIR/ca.pem" >/dev/null 2>&1
fi

generate_server_cert() {
  local name="$1"
  local cn="$2"
  local dns_2="$3"
  local conf="$CERT_DIR/$name-openssl.cnf"

  if [[ -f "$CERT_DIR/$name.pem" ]]; then
    return
  fi

  cat >"$conf" <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = $cn

[v3_req]
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = $dns_2
IP.1 = 127.0.0.1
EOF

  openssl req \
    -newkey rsa:2048 \
    -nodes \
    -keyout "$CERT_DIR/$name-key.pem" \
    -out "$CERT_DIR/$name.csr" \
    -config "$conf" >/dev/null 2>&1
  openssl x509 \
    -req \
    -in "$CERT_DIR/$name.csr" \
    -CA "$CERT_DIR/ca.pem" \
    -CAkey "$CERT_DIR/ca-key.pem" \
    -CAcreateserial \
    -days "${DBTOOL_IT_MQ_TLS_CERT_DAYS:-7}" \
    -extensions v3_req \
    -extfile "$conf" \
    -out "$CERT_DIR/$name.pem" >/dev/null 2>&1
}

generate_server_cert "rabbitmq" "dbtool-it-rabbitmq-tls" "rabbitmq-tls"
generate_server_cert "nats" "dbtool-it-nats-tls" "nats-tls"

chmod 0644 "$CERT_DIR"/*.pem "$CERT_DIR"/*-key.pem

cat >"$DBTOOL_IT_MQ_TLS_DIR/rabbitmq.conf" <<'EOF'
listeners.tcp = none
listeners.ssl.default = 5671
ssl_options.cacertfile = /etc/rabbitmq/certs/ca.pem
ssl_options.certfile = /etc/rabbitmq/certs/rabbitmq.pem
ssl_options.keyfile = /etc/rabbitmq/certs/rabbitmq-key.pem
ssl_options.verify = verify_none
ssl_options.fail_if_no_peer_cert = false
management.tcp.port = 15672
EOF

cat >"$DBTOOL_IT_MQ_TLS_DIR/nats.conf" <<'EOF'
port: 4222
http_port: 8222
jetstream {
  store_dir: "/tmp/nats/jetstream"
}
tls {
  cert_file: "/etc/nats/certs/nats.pem"
  key_file: "/etc/nats/certs/nats-key.pem"
  ca_file: "/etc/nats/certs/ca.pem"
  verify: false
  timeout: 2
}
EOF

export DBTOOL_IT_MQ_TLS_CA="$CERT_DIR/ca.pem"
export DBTOOL_IT_AMQPS_DSN="${DBTOOL_IT_AMQPS_DSN:-amqps://${DBTOOL_IT_AMQP_USER}:${DBTOOL_IT_AMQP_PASSWORD}@127.0.0.1:${DBTOOL_IT_AMQPS_PORT}/${DBTOOL_IT_AMQP_VHOST}?tls-ca=$DBTOOL_IT_MQ_TLS_CA}"
export DBTOOL_IT_NATS_TLS_DSN="${DBTOOL_IT_NATS_TLS_DSN:-nats+tls://127.0.0.1:${DBTOOL_IT_NATS_TLS_PORT}?tls-ca=$DBTOOL_IT_MQ_TLS_CA}"
