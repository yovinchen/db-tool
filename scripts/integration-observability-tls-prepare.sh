#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

case "$DBTOOL_IT_SEARCH_TLS_DIR" in
  /*) ;;
  *) DBTOOL_IT_SEARCH_TLS_DIR="$ROOT/$DBTOOL_IT_SEARCH_TLS_DIR" ;;
esac
export DBTOOL_IT_SEARCH_TLS_DIR

CERT_DIR="$DBTOOL_IT_SEARCH_TLS_DIR/certs"
mkdir -p "$CERT_DIR"

CERT_RENEWAL_SECONDS="${DBTOOL_IT_SEARCH_TLS_CERT_RENEWAL_SECONDS:-3600}"

cert_is_current() {
  local cert="$1"
  [[ -f "$cert" ]] && openssl x509 \
    -checkend "$CERT_RENEWAL_SECONDS" \
    -noout \
    -in "$cert" >/dev/null 2>&1
}

if [[ "${DBTOOL_IT_SEARCH_TLS_REGENERATE_CERTS:-0}" == "1" ]] \
  || [[ ! -f "$CERT_DIR/ca-key.pem" ]] \
  || [[ ! -f "$CERT_DIR/server-key.pem" ]] \
  || ! cert_is_current "$CERT_DIR/ca.pem" \
  || ! cert_is_current "$CERT_DIR/server.pem"; then
  rm -f "$CERT_DIR"/*
fi

if [[ ! -f "$CERT_DIR/ca.pem" ]]; then
  openssl req \
    -x509 \
    -newkey rsa:2048 \
    -nodes \
    -days "${DBTOOL_IT_SEARCH_TLS_CERT_DAYS:-7}" \
    -subj "/CN=dbtool-it-search-ca" \
    -keyout "$CERT_DIR/ca-key.pem" \
    -out "$CERT_DIR/ca.pem" >/dev/null 2>&1
fi

if [[ ! -f "$CERT_DIR/server.pem" ]]; then
  cat >"$CERT_DIR/server-openssl.cnf" <<'EOF'
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = dbtool-it-search-tls

[v3_req]
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = opensearch-tls
IP.1 = 127.0.0.1
EOF

  openssl req \
    -newkey rsa:2048 \
    -nodes \
    -keyout "$CERT_DIR/server-key.pem" \
    -out "$CERT_DIR/server.csr" \
    -config "$CERT_DIR/server-openssl.cnf" >/dev/null 2>&1
  openssl x509 \
    -req \
    -in "$CERT_DIR/server.csr" \
    -CA "$CERT_DIR/ca.pem" \
    -CAkey "$CERT_DIR/ca-key.pem" \
    -CAcreateserial \
    -days "${DBTOOL_IT_SEARCH_TLS_CERT_DAYS:-7}" \
    -extensions v3_req \
    -extfile "$CERT_DIR/server-openssl.cnf" \
    -out "$CERT_DIR/server.pem" >/dev/null 2>&1
fi

chmod 0644 "$CERT_DIR"/*.pem "$CERT_DIR"/*-key.pem

export DBTOOL_IT_OPENSEARCH_TLS_CA="$CERT_DIR/ca.pem"
export DBTOOL_IT_OPENSEARCH_TLS_DSN="${DBTOOL_IT_OPENSEARCH_TLS_DSN:-opensearch+https://127.0.0.1:${DBTOOL_IT_OPENSEARCH_TLS_PORT}?tls-ca=$DBTOOL_IT_OPENSEARCH_TLS_CA}"
