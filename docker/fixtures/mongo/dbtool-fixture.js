const dbName = process.env.MONGO_INITDB_DATABASE || "dbtool_it_mongo_fixture";
const fixtureDb = db.getSiblingDB(dbName);

fixtureDb.dbtool_fixture_people.drop();
fixtureDb.dbtool_fixture_people.insertMany([
  { kind: "dbtool-fixture", id: 1, name: "alice", role: "reader", active: true },
  { kind: "dbtool-fixture", id: 2, name: "bob", role: "writer", active: false },
  { kind: "dbtool-fixture", id: 3, name: "carol", role: "reviewer", active: true },
]);
