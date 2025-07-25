import oids
from sequtils import concat, map, mapIt

import ./core/[bson, types, wire]
import ./dbops/[aggregation, crud]



const csVerbose = defined(changeStreamVerbose)

when csVerbose:
  import sugar

type
  ChangeStreamEvent* = enum
    csInsert = "insert"
    csUpdate = "update"
    csReplace = "replace"
    csDelete = "delete"
    csInvalidate = "invalidate"
    csDrop = "drop"
    csDropDatabase = "dropDatabase"
    csRename = "rename"
  ChangeStreamId* = object
    data* {.bsonKey: "_data".}: string
  Namespace* = object
    db*: string
    coll*: string
  DocumentKey* = object
    id* {.bsonKey: "_id".}: Oid
  ChangeStream* = object
    id* {.bsonKey: "_id".}: ChangeStreamId
    operationType*: ChangeStreamEvent
    fullDocument*: BsonDocument
    ns*: Namespace
    documentKey*: DocumentKey

proc forEach*(c: Cursor, cb: proc(b: ChangeStream),
  stopWhen: set[ChangeStreamEvent]) =
  var cursor = c
  #defer: cursor.db.killCursors(cursor.ns, @[cursor.id])
  var cs: ChangeStream
  template processEntry(el, label: untyped) =
    cs = `el`.to ChangeStream
    cb(cs)
    when csVerbose: dump cs
    if cs.operationType in stopWhen:
      break `label`
  block always:
    while cursor.id != 0:
      when csVerbose: dump cursor.db == nil
      if cursor.db == nil: break always
      for fbatch in cursor.firstBatch: processEntry fbatch, always
      for nbatch in cursor.nextBatch: processEntry nbatch, always
      var forEachReply: BsonDocument
      try:
        forEachReply = cursor.db.getMore(cursor.id, cursor.ns, 101)
      except CatchableError:
        echo getCurrentExceptionMsg()
        break always

      #discard sleep 1_000
      when csVerbose: dump forEachReply
      if not forEachReply.ok:
        break always
      cursor = forEachReply["cursor"].ofEmbedded.toCursor

proc watch*(coll: Collection, pipelines: seq[BsonDocument] = @[],
  options = bson()): Cursor =
  var queries = newseq[BsonDocument](pipelines.len+1)
  queries[0] = bson { "$changeStream": options }
  queries = concat(queries, pipelines)
  when csVerbose: dump queries
  let reply = coll.db.aggregate(coll.name, queries, maxTimeMS = 0)
  if not reply.ok:
    raise newException(MongoError, getCurrentExceptionMsg())
  result = reply["cursor"].ofEmbedded.toCursor
  result.db = coll.db
  when csVerbose: dump result