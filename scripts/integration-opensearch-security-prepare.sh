#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

case "$DBTOOL_IT_OPENSEARCH_SECURITY_DIR" in
  /*) ;;
  *) DBTOOL_IT_OPENSEARCH_SECURITY_DIR="$ROOT/$DBTOOL_IT_OPENSEARCH_SECURITY_DIR" ;;
esac
export DBTOOL_IT_OPENSEARCH_SECURITY_DIR

CERT_DIR="$DBTOOL_IT_OPENSEARCH_SECURITY_DIR/certs"
mkdir -p "$CERT_DIR"

if [[ "${DBTOOL_IT_OPENSEARCH_SECURITY_REGENERATE_CERTS:-0}" == "1" ]]; then
  rm -f "$CERT_DIR"/*
fi

if [[ ! -f "$CERT_DIR/ca.pem" ]]; then
  openssl req \
    -x509 \
    -newkey rsa:2048 \
    -nodes \
    -days "${DBTOOL_IT_OPENSEARCH_SECURITY_CERT_DAYS:-7}" \
    -subj "/CN=dbtool-it-opensearch-security-ca" \
    -keyout "$CERT_DIR/ca-key.pem" \
    -out "$CERT_DIR/ca.pem" >/dev/null 2>&1
fi

if [[ ! -f "$CERT_DIR/node.pem" ]]; then
  cat >"$CERT_DIR/node-openssl.cnf" <<'EOF'
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = dbtool-it-opensearch-security

[v3_req]
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = opensearch-security
IP.1 = 127.0.0.1
EOF

  openssl req \
    -newkey rsa:2048 \
    -nodes \
    -keyout "$CERT_DIR/node-key.pem" \
    -out "$CERT_DIR/node.csr" \
    -config "$CERT_DIR/node-openssl.cnf" >/dev/null 2>&1
  openssl x509 \
    -req \
    -in "$CERT_DIR/node.csr" \
    -CA "$CERT_DIR/ca.pem" \
    -CAkey "$CERT_DIR/ca-key.pem" \
    -CAcreateserial \
    -days "${DBTOOL_IT_OPENSEARCH_SECURITY_CERT_DAYS:-7}" \
    -extensions v3_req \
    -extfile "$CERT_DIR/node-openssl.cnf" \
    -out "$CERT_DIR/node.pem" >/dev/null 2>&1
fi

chmod 0644 "$CERT_DIR"/*.pem "$CERT_DIR"/*-key.pem

export DBTOOL_IT_OPENSEARCH_SECURITY_CA="$CERT_DIR/ca.pem"
export DBTOOL_IT_OPENSEARCH_SECURITY_DSN="${DBTOOL_IT_OPENSEARCH_SECURITY_DSN:-opensearch+https://admin:${DBTOOL_IT_OPENSEARCH_SECURITY_ADMIN_PASSWORD}@127.0.0.1:${DBTOOL_IT_OPENSEARCH_SECURITY_PORT}?tls-ca=$DBTOOL_IT_OPENSEARCH_SECURITY_CA}"
