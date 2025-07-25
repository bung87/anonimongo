import tables, sequtils, net
import ../core/[bson, types, wire, utils]
import diagnostic

## Aggregation commands (and a Geospatial command)
## ***********************************************
##
## Ref to `Mongo command`_ documentation for better understanding of each API
## and its return value is BsonDocument. Geospatial command can be found `here`_.
##
## All async functionality has been removed - functions are now synchronous.
##
## .. _Mongo command: https://docs.mongodb.com/manual/reference/command/nav-aggregation/
## .. _here: https://docs.mongodb.com/manual/reference/command/geoSearch/#dbcmd.geoSearch

proc aggregate*(db: Database, coll: string, pipeline: seq[BsonDocument],
  explain = false, diskuse = false, cursor = bson(), maxTimeMS = 0,
  bypass = false, readConcern = bsonNull(), collation = bsonNull(),
  hint = bsonNull(), comment = "", wt = bsonNull(), explainVerbosity = ""): BsonDocument =
  ## Aggregate documents in collection
  let mongo = db.getMongo()
  let (connId, conn) = mongo.getConn()
  defer: mongo.endConn(connId)
  
  # Construct command manually to avoid bson macro issues with seq[BsonDocument]
  var cmd = newBson([])
  cmd["aggregate"] = coll.toBson
  cmd["pipeline"] = pipeline.map(toBson).toBson
  cmd["explain"] = explain.toBson
  cmd["diskuse"] = diskuse.toBson
  cmd["cursor"] = cursor
  cmd["maxTimeMS"] = maxTimeMS.toBson
  cmd["bypassDocumentValidation"] = bypass.toBson
  cmd["readConcern"] = readConcern
  cmd["collation"] = collation
  cmd["hint"] = hint
  cmd["comment"] = comment.toBson
  cmd["writeConcern"] = wt
  cmd["explainVerbosity"] = explainVerbosity.toBson
  
  let message = prepare(cmd, 0, db.name)
  conn.socket.send message.readAll
  let reply = conn.socket.getReply()
  result = reply.documents[0]

proc count*(db: Database, coll: string, query = bson(),
  limit = 0, skip = 0, hint = bsonNull(), readConcern = bsonNull(),
  collation = bsonNull(), explain = ""): BsonDocument =
  ## Count documents in collection
  let mongo = db.getMongo()
  let (connId, conn) = mongo.getConn()
  defer: mongo.endConn(connId)
  
  var cmd = bson({
    "count": coll,
    "query": query,
    "limit": limit,
    "skip": skip,
    "hint": hint,
    "readConcern": readConcern,
    "collation": collation,
    "explain": explain
  })
  
  let message = prepare(cmd, 0, db.name)
  conn.socket.send message.readAll
  let reply = conn.socket.getReply()
  result = reply.documents[0]

proc `distinct`*(db: Database, coll, key: string, query = bson(),
  readConcern = bsonNull(), collation = bsonNull(), explain = ""): BsonDocument =
  ## Get distinct values for a field
  let mongo = db.getMongo()
  let (connId, conn) = mongo.getConn()
  defer: mongo.endConn(connId)
  
  var cmd = bson({
    "distinct": coll,
    "key": key,
    "query": query,
    "readConcern": readConcern,
    "collation": collation,
    "explain": explain
  })
  
  let message = prepare(cmd, 0, db.name)
  conn.socket.send message.readAll
  let reply = conn.socket.getReply()
  result = reply.documents[0]

proc mapReduce*(db: Database, coll: string, map, reduce: BsonJs,
  `out`: BsonBase, query = bson(), sort = bsonNull(), limit = 0,
  finalize = bsonNull(), scope = bsonNull(), jsMode = false, verbose = false,
  bypass = false, collation = bsonNull(), wt = bsonNull()): BsonDocument =
  # MapReduce functionality removed - async dropped
  raise newException(MongoError, "mapReduce not implemented")

proc geoSearch*(db: Database, coll: string, search: BsonDocument,
  near: seq[BsonDocument], maxDistance = 0, limit = 0,
  readConcern = bsonNull()): BsonDocument =
  # GeoSearch functionality removed - async dropped
  raise newException(MongoError, "geoSearch not implemented")
