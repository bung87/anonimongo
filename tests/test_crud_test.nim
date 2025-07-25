import unittest, os, osproc, times, strformat, sequtils, net
import sugar

import utils_test
import anonimongo

{.warning[UnusedImport]: off.}

var mongorun: Process
if runlocal:
  mongorun = startmongo()
  sleep 3000 # waiting for mongod to be ready

suite "CRUD tests":
  test "Mongo server is running":
    # Test that we can actually connect to MongoDB
    let testMongo = newMongo("localhost", 27017, poolSize = 1)
    defer: testMongo.close()
    
    let (connId, conn) = testMongo.getConn()
    check connId > 0
    check not conn.isNil
    testMongo.endConn(connId)

  var
    mongo: Mongo
    db: Database
    insertDocs = newseq[BsonDocument](10)
    testdb = "newtemptest"
    collname = "temptestcoll"
    foundDocs = newseq[BsonDocument]()
    namespace = ""
    resfind: BsonDocument
    wr: WriteResult

  let currtime = now().toTime
  for i in 0 ..< 10:
    insertDocs[i] = bson({
        countId: i,
        addedTime: currtime + initDuration(minutes = i * 10),
        `type`: "insertTest",
    })
  
  test "Mongo connected and authenticated":
    mongo = testsetup()
    if mongo.withAuth:
      require mongo.authenticated
    db = mongo[testdb]
    namespace = &"{db.name}.{collname}"

  test &"Find documents on {namespace}":
    # find all documents
    require db != nil
    resfind = db.find(collname)
    resfind.reasonedCheck "find error"
    check resfind["cursor"]["firstBatch"].ofArray.len == 0
    resfind = db.find(collname, bson({}), singleBatch = true)
    resfind.reasonedCheck "find error"
    check resfind["cursor"]["id"] == 0

  test &"Insert documents on {namespace}":
    require db != nil
    resfind = db.insert(collname, insertDocs)
    resfind.reasonedCheck "Insert documents error"
    check resfind["n"] == insertDocs.len
    
    resfind = db.find(collname, singleBatch = true)
    check resfind.ok
    for d in resfind["cursor"]["firstBatch"].ofArray:
      foundDocs.add d
    check foundDocs.len == insertDocs.len

  test &"Count documents on {namespace}":
    require db != nil
    resfind = crud.count(db, collname)
    resfind.reasonedCheck("count error")
    check resfind["n"] == foundDocs.len

  test &"Aggregate documents on {namespace}":
    require db != nil
    let tensOfMinutes = 5
    let lesstime = currtime + initDuration(minutes = tensOfMinutes * 10)
    let pipeline = @[
      bson({ "$match": { addedTime: { "$gte": currtime, "$lt": lesstime }}}),
      bson({ "$project": {
        addedTime: { "$dateToString": {
          date: "$addedTime",
          format: "%G-%m-%dT-%H-%M-%S%z",
          timezone: "+07:00", }}}})
    ]
    resfind = db.aggregate(collname, pipeline)
    resfind.reasonedCheck("db.aggregate error")
    let doc = resfind["cursor"]["firstBatch"].ofArray
    check doc.len == tensOfMinutes

  test &"Distinct documents on {namespace}":
    require db != nil
    resfind = crud.distinct(db, collname, "countId")
    resfind.reasonedCheck "db.distinct error"
    let docs = resfind["values"].ofArray
    check docs.len == insertDocs.len
    check docs.allIt( it.kind == bkInt32 )
  
  test &"Find and modify some document(s) on {namespace}":
    require db != nil
    let newcount = 80
    let oldcount = 8
    resfind = db.findAndModify(collname, query = bson({
      countId: oldcount }), update = bson({ "$set": { countId: newcount }}))
    resfind.reasonedCheck "findAndModify error"
    check resfind["lastErrorObject"]["n"] == 1
    let olddoc = resfind["value"].ofEmbedded

    # let's see we cannot find the old entry
    resfind = db.find(collname, bson({ countId: oldcount}),
      singleBatch = true)
    resfind.reasonedCheck "find error"
    check resfind["cursor"]["id"] == 0
    var docs = resfind["cursor"]["firstBatch"].ofArray
    check docs.len == 0

    # let's see we can find the new entry
    resfind = db.find(collname, bson({ countId: newcount}),
      singleBatch = true)
    resfind.reasonedCheck "find error"
    docs = resfind["cursor"]["firstBatch"].ofArray
    check docs.len == 1
    check docs[0]["countId"] == newcount

  test &"Update some document(s) on {namespace}":
    require db != nil
    let addcount = 10
    let oldcount = 9
    let newtype = "異世界召喚"
    resfind = db.find(collname, bson({ countId: oldcount }))
    resfind.reasonedCheck "find error"
    let olddoc = resfind["cursor"]["firstBatch"][0].ofEmbedded
    resfind = db.update(collname, @[
      bson({
        q: { countId: oldcount },
        u: { "$set": { "type": newtype }, "$inc": { countId: addcount }},
        upsert: false,
        multi: true,
      })
    ])
    resfind.reasonedCheck "update error"
    check resfind["n"] == 1
    check resfind["nModified"] == 1

    resfind = db.find(collname, bson({ countId: oldcount + addcount }))
    resfind.reasonedCheck "find error"
    let docs = resfind["cursor"]["firstBatch"].ofArray
    check docs.len == 1
    check docs[0]["type"] == newtype

  test &"Find with lazyily on {namespace}":
    require db != nil
    resfind = db.find(collname)
    resfind.reasonedCheck "find error"
    # var cursor = (resfind["cursor"].ofEmbedded).to Cursor
    var cursor = resfind["cursor"].ofEmbedded.toCursor
    var count = cursor.firstBatch.len
    while true:
      resfind = db.getMore(cursor.id, collname, 1)
      cursor = resfind["cursor"].ofEmbedded.toCursor
      if cursor.nextBatch.len == 0:
        break
      count += cursor.nextBatch.len
    check count == foundDocs.len

  test &"Delete some document(s) on {namespace}":
    require db != nil
    var todelete = "insertTest"
    resfind = db.delete(collname, @[
      bson({ q: {
        "type": todelete,
      }, limit: 0, collation: {
        locale: "en_US_POSIX",
        caseLevel: false,
      }})
    ])
    resfind.reasonedCheck "find error"
    check resfind["n"] == foundDocs.len-1 # because of update

  test &"Drop database {db.name}":
    require db != nil
    wr = db.dropDatabase
    wr.success.reasonedCheck("dropDatabase error", wr.reason)

  test "Shutdown mongo":
    if runlocal:
      require mongo != nil
      wr = db.shutdown(timeout = 10)
      check wr.success
    else:
      check true

  if runlocal:
    if mongorun.running: kill mongorun
    close mongorun
  close mongo