drop table if exists dbtool_it_fixture_people;
create table dbtool_it_fixture_people (id integer primary key, name varchar(32) not null, role varchar(32) not null, active boolean not null);
insert into dbtool_it_fixture_people (id, name, role, active) values (1, 'alice', 'reader', true), (2, 'bob', 'writer', false), (3, 'carol', 'reviewer', true);
