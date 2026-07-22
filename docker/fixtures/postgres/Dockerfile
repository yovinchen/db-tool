FROM postgres:16.14-alpine@sha256:16bc17c64a573ef34162af9298258d1aec548232985b33ed7b1eac33ba35c229

COPY testdata/base-postgres-seed.sql /docker-entrypoint-initdb.d/001-dbtool-fixture.sql
