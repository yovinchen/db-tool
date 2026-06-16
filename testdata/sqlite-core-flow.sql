# Statements are applied one per line by scripts/smoke-core-flow.sh.
CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT NOT NULL, role TEXT NOT NULL);
INSERT INTO people (id, name, role) VALUES (1, 'alice', 'admin');
INSERT INTO people (id, name, role) VALUES (2, 'bob', 'reader');
