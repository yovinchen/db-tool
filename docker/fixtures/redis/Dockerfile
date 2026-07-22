FROM redis:7.4.9-alpine@sha256:6ab0b6e7381779332f97b8ca76193e45b0756f38d4c0dcda72dbb3c32061ab99

COPY testdata/base-redis-seed.commands /dbtool-fixtures/base-redis-seed.commands
COPY docker/fixtures/redis/docker-entrypoint-fixtures.sh /usr/local/bin/dbtool-redis-fixture-entrypoint

RUN chmod +x /usr/local/bin/dbtool-redis-fixture-entrypoint

ENTRYPOINT ["dbtool-redis-fixture-entrypoint"]
CMD ["redis-server"]
