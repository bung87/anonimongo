import tables, sequtils
import ../core/[bson, types, wire, utils]
import diagnostic


## Aggregation commands (and a Geospatial command)
## ***********************************************
##
## Ref to `Mongo command`_ documentation for better understanding of each API
## and its return value is BsonDocument. Geospatial command can be found `here`_.
##
## All async functionality has been removed - functions are stubbed out.
##
## .. _Mongo command: https://docs.mongodb.com/manual/reference/command/nav-aggregation/
## .. _here: https://docs.mongodb.com/manual/reference/command/geoSearch/#dbcmd.geoSearch

proc aggregate*(db: Database, coll: string, pipeline: seq[BsonDocument],
  explain = false, diskuse = false, cursor = bson(), maxTimeMS = 0,
  bypass = false, readConcern = bsonNull(), collation = bsonNull(),
  hint = bsonNull(), comment = "", wt = bsonNull(), explainVerbosity = ""): BsonDocument =
  # Aggregation functionality removed - async dropped
  raise newException(MongoError, "aggregate not implemented")

proc count*(db: Database, coll: string, query = bson(),
  limit = 0, skip = 0, hint = bsonNull(), readConcern = bsonNull(),
  collation = bsonNull(), explain = ""): BsonDocument =
  # Count functionality removed - async dropped
  raise newException(MongoError, "count not implemented")

proc `distinct`*(db: Database, coll, key: string, query = bson(),
  readConcern = bsonNull(), collation = bsonNull(), explain = ""): BsonDocument =
  # Distinct functionality removed - async dropped
  raise newException(MongoError, "distinct not implemented")

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
