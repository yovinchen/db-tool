ARG PYTHON_VERSION=3.12-alpine@sha256:dbb1970cc04ce7d381c65efe8309c0c03d463e5b35c88f14d721796ad24cfbfd
FROM python:${PYTHON_VERSION}

LABEL org.opencontainers.image.title="dbtool OpenSearch TLS test harness"
LABEL org.opencontainers.image.description="Tiny OpenSearch-compatible HTTPS harness for dbtool integration tests"

WORKDIR /search-tls

COPY scripts/search-tls-harness.py /usr/local/bin/search-tls-harness
COPY testdata/search-tls-seed.ndjson /search-tls/seed.ndjson

RUN chmod 0755 /usr/local/bin/search-tls-harness

EXPOSE 9200

ENTRYPOINT ["search-tls-harness"]
