# __KEYSPACE__ is replaced by scripts/integration-cassandra-fixture-data-test.sh.
create keyspace if not exists __KEYSPACE__ with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
drop table if exists __KEYSPACE__.dbtool_it_cassandra_fixture_people;
create table __KEYSPACE__.dbtool_it_cassandra_fixture_people (id int primary key, name text, role text, active boolean, tags list<text>);
insert into __KEYSPACE__.dbtool_it_cassandra_fixture_people (id, name, role, active, tags) values (1, 'alice', 'reader', true, ['cql', 'fixture']);
insert into __KEYSPACE__.dbtool_it_cassandra_fixture_people (id, name, role, active, tags) values (2, 'bob', 'writer', false, ['cql', 'seed']);
insert into __KEYSPACE__.dbtool_it_cassandra_fixture_people (id, name, role, active, tags) values (3, 'carol', 'reviewer', true, ['cql', 'verify']);
