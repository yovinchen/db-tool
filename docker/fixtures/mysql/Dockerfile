FROM mysql:8.4.9@sha256:c36050afdca850f23cef85703f84c7531a5ae155a11b5ee1c60acb09937c4084

COPY testdata/base-mysql-seed.sql /docker-entrypoint-initdb.d/001-dbtool-fixture.sql
