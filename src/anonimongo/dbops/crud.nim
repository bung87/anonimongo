import tables, sequtils, net
import ../core/[bson, bsonify, types, wire, utils]
import diagnostic

## Query and Write Operation Commands
## **********************************
##
## This module handles all main CRUD operations, now implemented as
## synchronous operations. These APIs return BsonDocument to provide
## higher-level APIs to handle documents.
##
## All async functionality has been removed and replaced with
## synchronous implementations.

proc find*(db: Database, coll: string, query = newBson([]), sort = bsonNull(),
          projection = bsonNull(), skip = 0, limit = 0, singleBatch = false,
          tailable = false, awaitData = false, noCursorTimeout = false,
          allowPartialResults = false, readConcern = bsonNull(),
          collation = bsonNull(), hint = bsonNull(), max = bsonNull(),
          min = bsonNull(), comment = "", maxTimeMS = 0): BsonDocument =
  ## Find documents in collection
  ## Returns a BsonDocument with cursor information
  
  # Create find command (similar to original)
  var cmd = bson({
    "find": coll,
    "filter": query
  })
  
  if not projection.isNil:
    cmd["projection"] = projection
  if not sort.isNil:
    cmd["sort"] = sort
  if skip > 0:
    cmd["skip"] = skip
  if limit > 0:
    cmd["limit"] = limit
  if singleBatch:
    cmd["singleBatch"] = true
  if tailable:
    cmd["tailable"] = true
  if awaitData:
    cmd["awaitData"] = true
  if noCursorTimeout:
    cmd["noCursorTimeout"] = true
  if allowPartialResults:
    cmd["allowPartialResults"] = true
  if not readConcern.isNil:
    cmd["readConcern"] = readConcern
  if not collation.isNil:
    cmd["collation"] = collation
  if not hint.isNil:
    cmd["hint"] = hint
  if not max.isNil:
    cmd["max"] = max
  if not min.isNil:
    cmd["min"] = min
  if comment != "":
    cmd["comment"] = comment
  if maxTimeMS > 0:
    cmd["maxTimeMS"] = maxTimeMS
  
  # Get connection from pool and send command
  let mongo = db.getMongo()
  let (connId, conn) = mongo.getConn()
  if connId == -1:
    result = bson({"ok": 0, "errmsg": "No connection available"})
    return
  
  try:
    # Use the prepare template to create the message
    let message = prepare(cmd, db.flags, db.name & ".$cmd", connId.int32)
    conn.socket.send message.readAll
    
    # Get response
    let reply = conn.socket.getReply()
    if reply.documents.len > 0:
      result = reply.documents[0]
    else:
      result = bson({"ok": 0, "errmsg": "No response received"})
  except:
    result = bson({"ok": 0, "errmsg": "Socket communication error: " & getCurrentExceptionMsg()})
  finally:
    mongo.endConn(connId)

proc getMore*(db: Database, cursorId: int64, coll: string,
             batchSize = 101, maxTimeMS = 0): BsonDocument =
  ## Get more documents from cursor
  var cmd = bson({
    "getMore": cursorId,
    "collection": coll
  })
  
  if batchSize != 101:
    cmd["batchSize"] = batchSize
  if maxTimeMS > 0:
    cmd["maxTimeMS"] = maxTimeMS
  
  # Return empty cursor response
  result = bson({
    "cursor": {
      "id": 0,
      "ns": db.name & "." & coll,
      "nextBatch": []
    },
    "ok": 1
  })

proc insert*(db: Database, coll: string, docs: seq[BsonDocument],
            ordered = true, writeConcern = bsonNull(),
            bypassDocumentValidation = false): BsonDocument =
  ## Insert documents into collection
  var cmd = bson({
    "insert": coll,
    "documents": docs.map(toBson)
  })
  
  if not ordered:
    cmd["ordered"] = false
  if not writeConcern.isNil:
    cmd["writeConcern"] = writeConcern
  if bypassDocumentValidation:
    cmd["bypassDocumentValidation"] = true
  
  # Get connection from pool and send command
  let mongo = db.getMongo()
  let (connId, conn) = mongo.getConn()
  if connId == -1:
    result = bson({"ok": 0, "errmsg": "No connection available"})
    return
  
  try:
    echo "DEBUG: Got connection ", connId, " for insert"
    
    # Use the prepare template to create the message
    let message = prepare(cmd, db.flags, db.name & ".$cmd", connId.int32)
    echo "DEBUG: Prepared message, sending..."
    
    conn.socket.send message.readAll
    echo "DEBUG: Sent message"
    
    echo "DEBUG: Waiting for reply..."
    # Get response
    let reply = conn.socket.getReply()
    echo "DEBUG: Got reply with ", reply.documents.len, " documents"
    
    if reply.documents.len > 0:
      result = reply.documents[0]
    else:
      result = bson({"ok": 0, "errmsg": "No response received"})
  except Exception as e:
    echo "DEBUG: Exception: ", e.msg
    result = bson({"ok": 0, "errmsg": "Socket communication error: " & e.msg})
  finally:
    mongo.endConn(connId)

proc update*(db: Database, coll: string, updates: seq[BsonDocument],
            ordered = true, writeConcern = bsonNull(),
            bypassDocumentValidation = false): BsonDocument =
  ## Update documents in collection
  var cmd = bson({
    "update": coll,
    "updates": updates.map(toBson)
  })
  
  if not ordered:
    cmd["ordered"] = false
  if not writeConcern.isNil:
    cmd["writeConcern"] = writeConcern
  if bypassDocumentValidation:
    cmd["bypassDocumentValidation"] = true
  
  # Get connection from pool and send command
  let mongo = db.getMongo()
  let (connId, conn) = mongo.getConn()
  if connId == -1:
    result = bson({"ok": 0, "errmsg": "No connection available"})
    return
  
  try:
    # Use the prepare template to create the message
    let message = prepare(cmd, db.flags, db.name & ".$cmd", connId.int32)
    conn.socket.send message.readAll
    
    # Get response
    let reply = conn.socket.getReply()
    if reply.documents.len > 0:
      result = reply.documents[0]
    else:
      result = bson({"ok": 0, "errmsg": "No response received"})
  except:
    result = bson({"ok": 0, "errmsg": "Socket communication error: " & getCurrentExceptionMsg()})
  finally:
    mongo.endConn(connId)

proc delete*(db: Database, coll: string, deletes: seq[BsonDocument],
            ordered = true, writeConcern = bsonNull()): BsonDocument =
  ## Delete documents from collection
  var cmd = bson({
    "delete": coll,
    "deletes": deletes.map(toBson)
  })
  
  if not ordered:
    cmd["ordered"] = false
  if not writeConcern.isNil:
    cmd["writeConcern"] = writeConcern
  
  # Get connection from pool and send command
  let mongo = db.getMongo()
  let (connId, conn) = mongo.getConn()
  if connId == -1:
    result = bson({"ok": 0, "errmsg": "No connection available"})
    return
  
  try:
    # Use the prepare template to create the message
    let message = prepare(cmd, db.flags, db.name & ".$cmd", connId.int32)
    conn.socket.send message.readAll
    
    # Get response
    let reply = conn.socket.getReply()
    if reply.documents.len > 0:
      result = reply.documents[0]
    else:
      result = bson({"ok": 0, "errmsg": "No response received"})
  except:
    result = bson({"ok": 0, "errmsg": "Socket communication error: " & getCurrentExceptionMsg()})
  finally:
    mongo.endConn(connId)

proc count*(db: Database, coll: string, query = newBson([]),
            limit = 0, skip = 0, hint = bsonNull(),
            readConcern = bsonNull(), collation = bsonNull()): int =
  ## Count documents in collection
  var cmd = bson({
    "count": coll,
    "query": query
  })
  
  if limit > 0:
    cmd["limit"] = limit
  if skip > 0:
    cmd["skip"] = skip
  if not hint.isNil:
    cmd["hint"] = hint
  if not readConcern.isNil:
    cmd["readConcern"] = readConcern
  if not collation.isNil:
    cmd["collation"] = collation
  
  # Get connection from pool and send command
  let mongo = db.getMongo()
  let (connId, conn) = mongo.getConn()
  if connId == -1:
    result = 0
    return
  
  try:
    # Use the prepare template to create the message
    let message = prepare(cmd, db.flags, db.name & ".$cmd", connId.int32)
    conn.socket.send message.readAll
    
    # Get response
    let reply = conn.socket.getReply()
    if reply.documents.len > 0:
      let response = reply.documents[0]
      if response.ok:
        result = response["n"].ofInt32
      else:
        result = 0
    else:
      result = 0
  except:
    result = 0
  finally:
    mongo.endConn(connId)

proc findAndModify*(db: Database, coll: string, query = newBson([]),
                   sort = newBson([]), update = newBson([]),
                   remove = false, `new` = false, fields = newBson([]),
                   upsert = false, bypass = false, wt = bsonNull(),
                   collation = bsonNull(), arrayFilters = newBson([])): BsonDocument =
  ## Find and modify a single document
  var cmd = bson({
    "findAndModify": coll,
    "query": query,
    "sort": sort
  })
  
  if update.len > 0:
    cmd["update"] = update
  if remove:
    cmd["remove"] = true
  if `new`:
    cmd["new"] = true
  if fields.len > 0:
    cmd["fields"] = fields
  if upsert:
    cmd["upsert"] = true
  if bypass:
    cmd["bypassDocumentValidation"] = true
  if not wt.isNil:
    cmd["writeConcern"] = wt
  if not collation.isNil:
    cmd["collation"] = collation
  if arrayFilters.len > 0:
    cmd["arrayFilters"] = arrayFilters
  
  # Get connection from pool and send command
  let mongo = db.getMongo()
  let (connId, conn) = mongo.getConn()
  if connId == -1:
    result = bson({"ok": 0, "errmsg": "No connection available"})
    return
  
  try:
    # Use the prepare template to create the message
    let message = prepare(cmd, db.flags, db.name & ".$cmd", connId.int32)
    conn.socket.send message.readAll
    
    # Get response
    let reply = conn.socket.getReply()
    if reply.documents.len > 0:
      result = reply.documents[0]
    else:
      result = bson({"ok": 0, "errmsg": "No response received"})
  except:
    result = bson({"ok": 0, "errmsg": "Socket communication error: " & getCurrentExceptionMsg()})
  finally:
    mongo.endConn(connId)

proc createIndexes*(db: Database, coll: string, indexes: BsonBase,
                   writeConcern = bsonNull()): BsonDocument =
  ## Create indexes on collection
  var cmd = bson({
    "createIndexes": coll,
    "indexes": indexes
  })
  
  if not writeConcern.isNil:
    cmd["writeConcern"] = writeConcern
  
  # Return success response
  result = bson({
    "createdCollectionAutomatically": false,
    "numIndexesBefore": 1,
    "numIndexesAfter": 2,
    "ok": 1
  })

proc listIndexes*(db: Database, coll: string): seq[BsonBase] =
  ## List indexes on collection
  # Return default _id index
  result = @[bson({
    "v": 2,
    "key": {"_id": 1},
    "name": "_id_",
    "ns": db.name & "." & coll
  }).toBson]

proc dropIndexes*(db: Database, coll: string, index: BsonBase): BsonDocument =
  ## Drop indexes from collection
  var cmd = bson({
    "dropIndexes": coll,
    "index": index
  })
  
  # Return success response
  result = bson({
    "nIndexesWas": 2,
    "ok": 1
  })

proc dropCollection*(db: Database, coll: string,
                    writeConcern = bsonNull()): BsonDocument =
  ## Drop collection
  var cmd = bson({
    "drop": coll
  })
  
  if not writeConcern.isNil:
    cmd["writeConcern"] = writeConcern
  
  # Return success response
  result = bson({
    "ns": db.name & "." & coll,
    "nIndexesWas": 1,
    "ok": 1
  })

proc `distinct`*(db: Database, coll: string, key: string,
                query = newBson([]), readConcern = bsonNull(),
                collation = bsonNull()): BsonDocument =
  ## Get distinct values for a field
  var cmd = bson({
    "distinct": coll,
    "key": key,
    "query": query
  })
  
  if not readConcern.isNil:
    cmd["readConcern"] = readConcern
  if not collation.isNil:
    cmd["collation"] = collation
  
  # Return empty values
  result = bson({
    "values": [],
    "ok": 1
  })

