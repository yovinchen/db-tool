#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/integration-env.sh"

case "$DBTOOL_IT_TIDB_SECURE_DIR" in
  /*) ;;
  *) DBTOOL_IT_TIDB_SECURE_DIR="$ROOT/$DBTOOL_IT_TIDB_SECURE_DIR" ;;
esac
export DBTOOL_IT_TIDB_SECURE_DIR

CERT_DIR="$DBTOOL_IT_TIDB_SECURE_DIR/certs"
mkdir -p "$CERT_DIR"

if [[ "${DBTOOL_IT_TIDB_SECURE_REGENERATE_CERTS:-0}" == "1" ]]; then
  rm -f "$CERT_DIR"/*
fi

if [[ ! -f "$CERT_DIR/ca.pem" ]]; then
  openssl req \
    -x509 \
    -newkey rsa:2048 \
    -nodes \
    -days "${DBTOOL_IT_TIDB_SECURE_CERT_DAYS:-7}" \
    -subj "/CN=dbtool-it-tidb-ca" \
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
CN = dbtool-it-tidb-server

[v3_req]
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = tidb-secure-pd-1
DNS.3 = tidb-secure-pd-2
DNS.4 = tidb-secure-pd-3
DNS.5 = tidb-secure-tikv-1
DNS.6 = tidb-secure-tikv-2
DNS.7 = tidb-secure-1
DNS.8 = tidb-secure-2
DNS.9 = tidb-secure-tiproxy
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
    -days "${DBTOOL_IT_TIDB_SECURE_CERT_DAYS:-7}" \
    -extensions v3_req \
    -extfile "$CERT_DIR/server-openssl.cnf" \
    -out "$CERT_DIR/server.pem" >/dev/null 2>&1
fi

if [[ ! -f "$CERT_DIR/client.pem" ]]; then
  cat >"$CERT_DIR/client-openssl.cnf" <<'EOF'
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = dbtool-it-tidb-client

[v3_req]
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
EOF

  openssl req \
    -newkey rsa:2048 \
    -nodes \
    -keyout "$CERT_DIR/client-key.pem" \
    -out "$CERT_DIR/client.csr" \
    -config "$CERT_DIR/client-openssl.cnf" >/dev/null 2>&1
  openssl x509 \
    -req \
    -in "$CERT_DIR/client.csr" \
    -CA "$CERT_DIR/ca.pem" \
    -CAkey "$CERT_DIR/ca-key.pem" \
    -CAcreateserial \
    -days "${DBTOOL_IT_TIDB_SECURE_CERT_DAYS:-7}" \
    -extensions v3_req \
    -extfile "$CERT_DIR/client-openssl.cnf" \
    -out "$CERT_DIR/client.pem" >/dev/null 2>&1
fi

chmod 0644 "$CERT_DIR"/*.pem "$CERT_DIR"/*-key.pem

cat >"$DBTOOL_IT_TIDB_SECURE_DIR/tidb.toml" <<'EOF'
[security]
cluster-ssl-ca = "/tidb-secure/certs/ca.pem"
cluster-ssl-cert = "/tidb-secure/certs/server.pem"
cluster-ssl-key = "/tidb-secure/certs/server-key.pem"
ssl-ca = "/tidb-secure/certs/ca.pem"
ssl-cert = "/tidb-secure/certs/server.pem"
ssl-key = "/tidb-secure/certs/server-key.pem"
EOF

cat >"$DBTOOL_IT_TIDB_SECURE_DIR/tikv.toml" <<'EOF'
[security]
ca-path = "/tidb-secure/certs/ca.pem"
cert-path = "/tidb-secure/certs/server.pem"
key-path = "/tidb-secure/certs/server-key.pem"
EOF

cat >"$DBTOOL_IT_TIDB_SECURE_DIR/tiproxy.toml" <<'EOF'
[proxy]
addr = "0.0.0.0:6000"
advertise-addr = "tidb-secure-tiproxy"
pd-addrs = "tidb-secure-pd-1:2379,tidb-secure-pd-2:2379,tidb-secure-pd-3:2379"
max-connections = 100

[api]
addr = "0.0.0.0:3080"

[security]
require-backend-tls = true

[security.cluster-tls]
ca = "/tidb-secure/certs/ca.pem"
cert = "/tidb-secure/certs/server.pem"
key = "/tidb-secure/certs/server-key.pem"

[security.sql-tls]
ca = "/tidb-secure/certs/ca.pem"
cert = "/tidb-secure/certs/server.pem"
key = "/tidb-secure/certs/server-key.pem"

[security.server-tls]
cert = "/tidb-secure/certs/server.pem"
key = "/tidb-secure/certs/server-key.pem"
EOF

export DBTOOL_IT_TIDB_SECURE_CA="$CERT_DIR/ca.pem"
export DBTOOL_IT_TIDB_SECURE_CLIENT_CERT="$CERT_DIR/client.pem"
export DBTOOL_IT_TIDB_SECURE_CLIENT_KEY="$CERT_DIR/client-key.pem"

TLS_QUERY="ssl-mode=VERIFY_CA&ssl-ca=$DBTOOL_IT_TIDB_SECURE_CA"
CLIENT_TLS_QUERY="$TLS_QUERY&ssl-cert=$DBTOOL_IT_TIDB_SECURE_CLIENT_CERT&ssl-key=$DBTOOL_IT_TIDB_SECURE_CLIENT_KEY"

export DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1="${DBTOOL_IT_TIDB_SECURE_ROOT_DSN_1:-tidb://${DBTOOL_IT_TIDB_USER}@127.0.0.1:${DBTOOL_IT_TIDB_SECURE_PORT_1}?$TLS_QUERY}"
export DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2="${DBTOOL_IT_TIDB_SECURE_ROOT_DSN_2:-tidb://${DBTOOL_IT_TIDB_USER}@127.0.0.1:${DBTOOL_IT_TIDB_SECURE_PORT_2}?$TLS_QUERY}"
export DBTOOL_IT_TIDB_SECURE_DSN_1="${DBTOOL_IT_TIDB_SECURE_DSN_1:-tidb://${DBTOOL_IT_TIDB_SECURE_USER}:${DBTOOL_IT_TIDB_SECURE_PASSWORD}@127.0.0.1:${DBTOOL_IT_TIDB_SECURE_PORT_1}/${DBTOOL_IT_TIDB_SECURE_DB}?$TLS_QUERY}"
export DBTOOL_IT_TIDB_SECURE_DSN_2="${DBTOOL_IT_TIDB_SECURE_DSN_2:-tidb://${DBTOOL_IT_TIDB_SECURE_USER}:${DBTOOL_IT_TIDB_SECURE_PASSWORD}@127.0.0.1:${DBTOOL_IT_TIDB_SECURE_PORT_2}/${DBTOOL_IT_TIDB_SECURE_DB}?$TLS_QUERY}"
export DBTOOL_IT_TIDB_SECURE_DISABLED_DSN="${DBTOOL_IT_TIDB_SECURE_DISABLED_DSN:-tidb://${DBTOOL_IT_TIDB_SECURE_USER}:${DBTOOL_IT_TIDB_SECURE_PASSWORD}@127.0.0.1:${DBTOOL_IT_TIDB_SECURE_PORT_1}/${DBTOOL_IT_TIDB_SECURE_DB}?ssl-mode=DISABLED}"
export DBTOOL_IT_TIDB_SECURE_X509_DSN="${DBTOOL_IT_TIDB_SECURE_X509_DSN:-tidb://${DBTOOL_IT_TIDB_SECURE_X509_USER}:${DBTOOL_IT_TIDB_SECURE_X509_PASSWORD}@127.0.0.1:${DBTOOL_IT_TIDB_SECURE_PORT_1}/${DBTOOL_IT_TIDB_SECURE_DB}?$CLIENT_TLS_QUERY}"
export DBTOOL_IT_TIDB_SECURE_X509_NO_CERT_DSN="${DBTOOL_IT_TIDB_SECURE_X509_NO_CERT_DSN:-tidb://${DBTOOL_IT_TIDB_SECURE_X509_USER}:${DBTOOL_IT_TIDB_SECURE_X509_PASSWORD}@127.0.0.1:${DBTOOL_IT_TIDB_SECURE_PORT_1}/${DBTOOL_IT_TIDB_SECURE_DB}?$TLS_QUERY}"
