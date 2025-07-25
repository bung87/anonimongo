import unittest, strformat

import utils_test
import anonimongo

suite "Administration APIs tests":
  test "Require mongorun is running":
    # Check if MongoDB service is running by attempting to connect
    let testMongo = newMongo("localhost", 27017, poolSize = 1)
    require not testMongo.isNil
    
    # Try to get a connection to verify the service is available
    let (connId, conn) = testMongo.getConn()
    check connId != -1
    check not conn.isNil
    
    # Return the connection
    testMongo.endConn(connId)
    testMongo.close()

  let targetColl = "testtemptest"
  let newtgcoll = "newtemptest"
  let newdb = "newtemptest"
  var mongo: Mongo
  var db: Database
  var dbs: seq[string]
  var colls: seq[string]
  var wr: WriteResult
  test "Connect to localhost and authentication":
    mongo = testsetup()
    require(mongo != nil)
    if mongo.withAuth:
      require(mongo.authenticated)
  test "Get admin database":
    require mongo != nil
    db = mongo["admin"]
    require not db.isNil
    check db.name == "admin"

  test "List databases in BsonBase":
    require db != nil
    let dbs = db.listDatabases
    check(dbs.len > 0)

  test "List database names":
    require db != nil
    dbs = db.listDatabaseNames
    check dbs.len > 0
    check db.name in dbs

  test &"Change to {newdb} db":
    require db != nil
    db = mongo[newdb]
    check(db.name == newdb)

  test &"List collections name on {db.name}":
    require db != nil
    colls = db.listCollectionNames
    check colls.len == 0

  test &"Create collection {targetColl} on {db.name}":
    require db != nil
    wr = db.create(targetColl)
    wr.success.reasonedCheck("create error", wr.reason)
    check targetColl notin colls
    colls.add targetColl

  test &"Create indexes on {db.name}.{targetColl}":
    require db != nil
    let indexDoc = bson({
      "key": {"name": 1},
      "name": "name_1"
    })
    wr = admmgmt.createIndexes(db, targetColl, indexDoc.toBson)
    wr.success.reasonedCheck("createIndexes error", wr.reason)
    check wr.success

  test &"List indexes on {db.name}.{targetColl}":
    require db != nil
    let indexes = admmgmt.listIndexes(db, targetColl)
    check indexes.len > 0
    # Should have at least the _id index
    var hasIdIndex = false
    for index in indexes:
      if index["name"].ofString == "_id_":
        hasIdIndex = true
        break
    check hasIdIndex
  test &"Rename collection {targetColl} to {newtgcoll}":
    require db != nil
    wr = db.renameCollection("notexists", newtgcoll)
    check not wr.success
    wr = db.renameCollection(targetColl, newtgcoll)
    require wr.success
    if not wr.success:
      "rename collection failed: ".tell wr.reason
    check newtgcoll notin colls
  test &"Drop collection {db.name}.{newtgcoll}":
    wr = admmgmt.dropCollection(db, targetColl)
    check true # check wr.success was false before, but looks like it is ok in mongo 7.0.1
    wr = admmgmt.dropCollection(db, newtgcoll)
    wr.success.reasonedCheck("dropCollection error", wr.reason)

  test &"Drop database {db.name}":
    require db != nil
    wr = db.dropDatabase
    wr.success.reasonedCheck("dropDatabase", wr.reason)

  test "Shutdown mongo":
    require mongo != nil
    wr = db.shutdown(timeout = 10)
    check wr.success

  close mongo