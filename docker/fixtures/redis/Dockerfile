FROM redis:7-alpine

COPY testdata/base-redis-seed.commands /dbtool-fixtures/base-redis-seed.commands
COPY docker/fixtures/redis/docker-entrypoint-fixtures.sh /usr/local/bin/dbtool-redis-fixture-entrypoint

RUN chmod +x /usr/local/bin/dbtool-redis-fixture-entrypoint

ENTRYPOINT ["dbtool-redis-fixture-entrypoint"]
CMD ["redis-server"]
