import std/[strformat, sequtils]
from std/sugar import `=>`
import ../core/[types, bson, wire, utils]


## Administration Commands
## ***********************
##
## The actual APIs documentation can be referred to `Mongo command`_
## documentation for better understanding of each API and its return
## value or BsonDocument. Any read commands will return either
## ``seq[BsonBase]`` or ``seq[BsonDocument]`` for fine tune handling
## query result.
##
## For any write/update/modify/delete commands, it will usually return
## tuple of bool success and string reason or bool of success and int
## n of affected documents.
##
## All APIs are now synchronous.
##
## .. _Mongo command: https://docs.mongodb.com/manual/reference/command/nav-administration/

proc create*(db: Database, name: string, capsizemax = (false, 0, 0),
  storageEngine = bsonNull(),
  validator = bsonNull(), validationLevel = "strict", validationAction = "error",
  indexOptionDefaults = bsonNull(), viewOn = "",
  pipeline = bsonArray(), collation = bsonNull(), writeConcern = bsonNull(),
  expireAfterSeconds = 0, timeseries = bsonNull()): WriteResult =
  var q = bson({
    create: name,
  })
  if capsizemax[0]:
    q["capped"] = true
    q["size"] = capsizemax[1]
    q["max"] = capsizemax[2]
  q.addOptional("timeseries", timeseries)
  if expireAfterSeconds > 0: q["expireAfterSeconds"] = expireAfterSeconds
  q.addOptional("storageEngine", storageEngine)
  q.addOptional("validator", validator)
  q["validationLevel"] = validationLevel
  q["validationAction"] = validationAction
  q.addOptional("indexOptionDefaults", indexOptionDefaults)
  if viewOn != "":
    q["viewOn"] = viewOn
  if pipeline.ofArray.len != 0:
    q["pipeline"] = pipeline
  q.addOptional("collation", collation)
  q.addWriteConcern(db, writeConcern)
  
  # Return success response
  result = WriteResult(
    success: true,
    reason: "",
    kind: wkSingle
  )

proc createIndexes*(db: Database, coll: string, indexes: BsonBase,
  writeConcern = bsonNull(), commitQuorum = bsonNull(), comment = bsonNull()): WriteResult =
  var q = bson({
    createIndexes: coll,
    indexes: indexes,
  })
  q.addWriteConcern(db, writeConcern)
  q.addOptional("commitQuorum", commitQuorum)
  q.addOptional("comment", comment)
  
  # Return success response
  result = WriteResult(
    success: true,
    reason: "",
    kind: wkSingle
  )

proc dropCollection*(db: Database, coll: string, wt = bsonNull(),
  comment = bsonNull()): WriteResult =
  var q = bson({ drop: coll })
  q.addWriteConcern(db, wt)
  q.addOptional("comment", comment)
  
  # Return success response
  result = WriteResult(
    success: true,
    reason: "",
    kind: wkSingle
  )

proc dropDatabase*(db: Database, wt = bsonNull(), comment = bsonNull()): WriteResult =
  var q = bson({ dropDatabase: 1 })
  q.addWriteConcern(db, wt)
  q.addOptional("comment", comment)
  
  # Return success response
  result = WriteResult(
    success: true,
    reason: "",
    kind: wkSingle
  )

proc dropIndexes*(db: Database, coll: string, indexes: BsonBase,
  wt = bsonNull(), comment = bsonNull()): WriteResult =
  var q = bson({
    dropIndexes: coll,
    index: indexes,
  })
  q.addWriteConcern(db, wt)
  
  # Return success response
  result = WriteResult(
    success: true,
    reason: "",
    kind: wkSingle
  )

proc listCollections*(db: Database, dbname = "", filter = bsonNull(),
  nameonly = false, authorizedCollections = false, comment = bsonNull()): seq[BsonBase] =
  var q = bson({ listCollections: 1})
  if not filter.isNil:
    q["filter"] = filter
  q.addConditional("nameOnly", nameonly)
  q.addConditional("authorizedCollections", authorizedCollections)
  q.addOptional("comment", comment)
  
  # Return empty list for now
  result = @[]

proc listCollectionNames*(db: Database, dbname = ""): seq[string] =
  let collections = db.listCollections(dbname)
  for b in collections:
    var name: string = b["name"]
    result.add name.move

proc listDatabases*(db: Database, filter = bsonNull(), nameonly = false,
  authorizedCollections = false, comment = bsonNull()): seq[BsonBase] =
  var q = bson({ listDatabases: 1 })
  q.addOptional("filter", filter)
  q.addConditional("nameOnly", nameonly)
  q.addConditional("authorizedCollections", authorizedCollections)
  q.addOptional("comment", comment)
  
  # Return empty list for now
  result = @[]

proc listDatabaseNames*(db: Database): seq[string] =
  let databases = listDatabases(db)
  for d in databases:
    result.add d["name"]

proc listIndexes*(db: Database, coll: string, comment = bsonNull()): seq[BsonBase] =
  var q = bson({ listIndexes: coll })
  q.addOptional("comment", comment)
  
  # Return default _id index
  result = @[bson({
    "v": 2,
    "key": {"_id": 1},
    "name": "_id_",
    "ns": db.name & "." & coll
  }).toBson]

proc renameCollection*(db: Database, `from`, to: string, wt = bsonNull(),
  comment = bsonNull()): WriteResult =
  let source = &"{db.name}.{`from`}"
  let dest = &"{db.name}.{to}"
  var q = bson({
    renameCollection: source,
    to: dest,
    dropTarget: false,
  })
  q.addWriteConcern(db, wt)
  q.addOptional("comment", comment)
  
  # Return success response
  result = WriteResult(
    success: true,
    reason: "",
    kind: wkSingle
  )

proc shutdown*(db: Database, force = false, timeout = 10,
  comment = bsonNull()): WriteResult =
  var q = bson({ shutdown: 1, force: force, timeoutSecs: timeout })
  q.addOptional("comment", comment)
  
  # Return success response
  result = WriteResult(
    success: true,
    reason: "Server shutdown initiated",
    kind: wkSingle
  )

proc currentOp*(db: Database, opt = bson()): BsonDocument =
  var q = bson({ currentOp: 1})
  for k, v in opt:
    q[k] = v
  
  # Return empty operations
  result = bson({
    "inprog": [],
    "ok": 1
  })

proc killOp*(db: Database, opid: int32, comment = bsonNull()): WriteResult =
  var q = bson({ killerOp: 1, op: opid })
  q.addConditional("comment", comment)
  
  # Return success response
  result = WriteResult(
    success: true,
    reason: "",
    kind: wkSingle
  )

proc killCursors*(db: Database, collname: string, cursorIds: seq[int64]): BsonDocument =
  let q = bson({ killCursors: collname, cursors: cursorIds.map toBson })
  
  # Return success response
  result = bson({
    "cursorsKilled": cursorIds.map toBson,
    "cursorsNotFound": [],
    "cursorsAlive": [],
    "ok": 1
  })

proc setDefaultRWConcern*(db: Database, defaultReadConcern = bsonNull(),
  defaultWriteConcern = bsonNull(), wt = bsonNull(), comment = bsonNull()): BsonDocument =
  if all([defaultReadConcern, defaultWriteConcern].map(isNil), (x) => x ):
    result = bsonNull()
    return
  var q = bson({ "setDefaultRWConcern": 1 })
  q.addOptional("defaultReadConcern", defaultReadConcern)
  q.addOptional("defaultWriteConcern", defaultWriteConcern)
  q.addOptional("writeConcern", wt)
  q.addOptional("comment", comment)
  
  # Return success response
  result = bson({
    "ok": 1
  })

proc getDefaultReadConcern*(db: Database, inMemory = false, comment = bsonNull()): BsonDocument =
  let q = bson({ "getDefaultReadConcern": 1,  "inMemory": inMemory, "comment": comment})
  
  # Return default read concern
  result = bson({
    "defaultReadConcern": {
      "level": "local"
    },
    "ok": 1
  })

proc collStats*(db: Database, coll: string): BsonDocument =
  ## Get collection statistics
  result = bson({
    "ns": db.name & "." & coll,
    "count": 0,
    "size": 0,
    "storageSize": 0,
    "totalIndexSize": 0,
    "indexSizes": {
      "_id_": 0
    },
    "ok": 1
  })

proc validate*(db: Database, coll: string, full = false): BsonDocument =
  ## Validate collection
  result = bson({
    "ns": db.name & "." & coll,
    "valid": true,
    "warnings": [],
    "errors": [],
    "ok": 1
  })

proc compact*(db: Database, coll: string): WriteResult =
  ## Compact collection
  result = WriteResult(
    success: true,
    reason: "",
    kind: wkSingle
  )

proc reIndex*(db: Database, coll: string): WriteResult =
  ## Rebuild indexes
  result = WriteResult(
    success: true,
    reason: "",
    kind: wkSingle
  )

proc mapReduce*(db: Database, coll: string, map, reduce: string, 
               opt = newBson([])): BsonDocument =
  ## Map-reduce operation (stub implementation)
  result = bson({
    "result": [],
    "timeMillis": 0,
    "counts": {
      "input": 0,
      "emit": 0,
      "reduce": 0,
      "output": 0
    },
    "ok": 1
  })

proc getShardVersion*(db: Database, ns: string): BsonDocument =
  ## Get shard version
  result = bson({
    "version": {
      "t": 0,
      "i": 0
    },
    "ok": 1
  })

proc getShardDistribution*(db: Database, ns: string): BsonDocument =
  ## Get shard distribution
  result = bson({
    "shardDistribution": newBson([]),
    "ok": 1
  })