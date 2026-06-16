# Database Fixture Images

These Dockerfiles build disposable database images with the shared dbtool
fixture data baked into startup initialization paths.

| Image | Dockerfile | Seed source |
| --- | --- | --- |
| PostgreSQL | `postgres/Dockerfile` | `testdata/base-postgres-seed.sql` |
| MySQL | `mysql/Dockerfile` | `testdata/base-mysql-seed.sql` |
| Redis | `redis/Dockerfile` | `testdata/base-redis-seed.commands` |
| MongoDB | `mongo/Dockerfile` | `docker/fixtures/mongo/dbtool-fixture.js` |

Run `./scripts/integration-fixture-images-test.sh` to build the images, start
the `fixture-images` Compose profile, and prove dbtool can read the preloaded
rows, keys, and documents without first injecting data from the host.
