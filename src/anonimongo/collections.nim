import sequtils, strformat
import sugar

import dbops/[admmgmt, aggregation, crud]
import core/[bson, types, utils]

{.warning[UnusedImport]: off.}

## Collection Methods
## ******************
##
## Collection module implements selected APIs documented `here`_ in Mongo page.
## Not all APIs will be implemented as there are several APIs that just
## helper based on more basic APIs. This methods offload the actual operations
## to `dbops crud`_ module hence any resemblance on the parameter with a bit
## differents in returned values. The APIs methods implemented here can be
## viewed as higher-level than `dbops crud`_ module APIs.
##
## Collection should return `Query`_ / `Cursor`_ type or anything that will run
## for the actual query. Users can set the values for queries setting
## before actual query request.
##
## .. _here: https://docs.mongodb.com/manual/reference/method/js-collection/
## .. _dbops crud: dbops/crud.html
## .. _Query: core/types.html#Query
## .. _Cursor: core/types.html#Cursor

## The strategies here will consist of:
##
## 1. Collection invoke methods
## 2. Got the query necessary
## 3. Users chose how to run the query e.g.
##
##      a. Find a document about it
##      b. Find several documents about it
##      c. Iterate the documents for it
##

## Query will return Cursor that implement `items`_ iterators to iterate whether
## firstBatch field or nextBatch field and will immediately `getMore`_ if it's empty.
##
## .. _items: #items.i,Cursor
## .. _getMore: dbops/crud.html#getMore,Database,int64,string,int

proc one*(q: Query): BsonDocument {.gcsafe.} =
  ## Execute query and return first document
  let doc = q.collection.db.find(q.collection.name, q.query, q.sort,
    q.projection, skip = q.skip, limit = 1, singleBatch = true)
  let batch = doc["cursor"]["firstBatch"].ofArray
  if batch.len > 0:
    result = batch[0].ofEmbedded
  else:
    result = newBson([])

proc all*(q: Query): seq[BsonDocument] =
  ## Execute query and return all matching documents
  var doc = q.collection.db.find(q.collection.name, q.query, q.sort,
    q.projection, skip = q.skip, limit = q.limit)
  var cursor = doc["cursor"].ofEmbedded.toCursor()
  result = cursor.firstBatch
  if result.len >= q.limit:
    return
  while cursor.id > 0:
    doc = q.collection.db.getMore(cursor.id, q.collection.name,
      batchSize = q.batchSize)
    cursor = doc["cursor"].ofEmbedded.toCursor()
    if cursor.nextBatch.len == 0:
      break
    result = concat(result, cursor.nextBatch)

iterator items*(cur: Cursor): BsonDocument =
  ## Iterate over cursor documents
  for b in cur.firstBatch:
    yield b
  let batchSize = if cur.firstBatch.len != 0: cur.firstBatch.len
                  else: 101
  var doc: BsonDocument
  var newcur = cur
  let collname = newcur.ns.split(".")[^1]  # Get collection name from namespace
  var db = cur.db
  while newcur.id != 0:
    doc = db.getMore(newcur.id, collname, batchSize)
    newcur = doc["cursor"].ofEmbedded.toCursor()
    if newcur.nextBatch.len <= 0:
      break
    for b in newcur.nextBatch:
      yield b

iterator pairs*(cur: Cursor): (int, BsonDocument) =
  ## Iterate over cursor with index
  var count = 0
  for doc in cur:
    yield (count, doc)
    inc count

proc iter*(q: Query): Cursor =
  ## Execute query and return cursor
  var doc = q.collection.db.find(q.collection.name, q.query, q.sort,
    q.projection, skip = q.skip, limit = q.limit)
  result = doc["cursor"].ofEmbedded.toCursor()
  result.db = q.collection.db

proc find*(c: Collection, query = newBson([]), projection = bsonNull()): Query =
  ## Create query for finding documents
  result = cast[Query](allocShared0(sizeof(types.QueryObj)))
  result.query = query
  result.collection = c
  result.projection = projection
  result.batchSize = 101

proc findOne*(c: Collection, query = newBson([]), projection = bsonNull(),
  sort = bsonNull()): BsonDocument {.gcsafe.} =
  ## Find a single document
  var q = c.find(query, projection)
  q.sort = sort
  result = q.one

proc findAll*(c: Collection, query = newBson([]), projection = bsonNull(),
  sort = bsonNull(), limit = 0): seq[BsonDocument] =
  ## Find all matching documents
  var q = c.find(query, projection)
  q.sort = sort
  q.limit = int32 limit
  result = q.all

proc findIter*(c: Collection, query = newBson([]), projection = bsonNull(),
  sort = bsonNull()): Cursor =
  ## Find documents and return cursor
  var q = c.find(query, projection)
  q.sort = sort
  result = q.iter

proc findAndModify*(c: Collection, query = newBson([]), sort = bsonNull(),
  remove = false, update = bsonNull(), `new` = false, fields = bsonNull(),
  upsert = false, bypass = false, wt = bsonNull(), collation = bsonNull(),
  arrayFilters: seq[BsonDocument] = @[]): BsonDocument =
  ## Find and modify a document
  let doc = c.db.findAndModify(c.name, query, 
    if sort.isNil: newBson([]) else: sort.ofEmbedded,
    if update.isNil: newBson([]) else: update.ofEmbedded,
    remove, `new`,
    if fields.isNil: newBson([]) else: fields.ofEmbedded,
    upsert, bypass, wt, collation, newBson([]))
  result = doc["value"].ofEmbedded

template operationFor(doIt: bool, label: string, op: untyped): untyped =
  ## Template for retryable writes
  if doIt:
    try:
      op
      if not result.success:
        raise newException(Exception, result.reason)
    except CatchableError:
      echo "first attempt retryable ", label, " failed: ", getCurrentExceptionMsg()
      op
  else:
    op

proc update*(c: Collection, query = newBson([]), updates = bsonNull(),
  opt = newBson([])): WriteResult =
  ## Update documents
  var q = bson({
    q: query,
    u: updates,
   })
  var ordered = true
  var retryable = true
  var isMulti = false
  for k, v in opt:
    if k == "ordered": ordered = v.ofBool
    elif k == "multi":
      retryable = false
      isMulti = v.ofBool
      q[k] = v
    elif k == "writeConcern" and v.kind == bkInt32 and v == 0:
      retryable = false
      q[k] = v
    else:
      var kk = k
      q[move kk] = v
  retryable = retryable and c.db.retryableWrites
  retryable.operationFor("update"):
    let doc = c.db.update(c.name, @[q], ordered = ordered)
    if isMulti:
      result = doc.getWResult
    else:
      result = doc.getWSingleResult
      
proc remove*(c: Collection, query: BsonDocument, justone = false): WriteResult =
  ## Remove documents
  let limit = if justone: 1 else: 0
  var delq = bson({
    q: query,
    limit: limit
  })
  var retryable = justone and c.db.retryableWrites
  retryable.operationFor("remove"):
    let doc = c.db.delete(c.name, @[delq])
    result = doc.getWResult

proc remove*(c: Collection, query, opt: BsonDocument): WriteResult =
  ## Remove documents with options
  var delq = bson({ q: query })
  var wt: BsonBase
  var retryable = true
  for k, v in opt:
    if k == "writeConcern":
      wt = v
      if wt.kind == bkInt32 and wt == 0: retryable = false
    elif k == "justOne":
      retryable = retryable and v.ofBool
      delq["limit"] = if v.ofBool: 1 else: 0
    else:
      var kk = k
      delq[move kk] = v
  retryable = retryable and c.db.retryableWrites
  retryable.operationFor("remove"):
    let doc = c.db.delete(c.name, @[delq], writeConcern = wt)
    result = doc.getWResult

proc remove*(c: Collection, query: seq[BsonDocument]): WriteResult =
  ## Remove multiple document sets
  var q = newseq[BsonDocument](query.len)
  for i, que in query:
    var ii = i
    q[move ii] = bson({
      q: que,
      limit: 0,
    })
  result = c.db.delete(c.name, q).getWResult

proc insert*(c: Collection, docs: seq[BsonDocument], opt = newBson([])): WriteResult =
  ## Insert documents
  var retryable = false
  let wt =
    if "writeConcern" in opt:
      let w = opt["writeConcern"]
      if not w.isNil and w.kind == bkInt32 and w.ofInt32 == 0: retryable = false
      else: retryable = true
      w
    else: bsonNull()
  let ordered = if "ordered" in opt: opt["ordered"].ofBool else: true
  retryable = retryable and c.db.retryableWrites

  retryable.operationFor("insert"):
    let doc = c.db.insert(c.name, docs, ordered, wt)
    result = doc.getWResult

proc insert*(c: Collection, doc: BsonDocument): WriteResult =
  ## Insert single document
  var retryable = c.db.retryableWrites
  retryable.operationFor("insert"):
    let doc = c.db.insert(c.name, @[doc], true)
    result = doc.getWSingleResult

proc drop*(c: Collection, wt = bsonNull()): WriteResult =
  ## Drop collection
  result = admmgmt.dropCollection(c.db, c.name, wt)

proc count*(c: Collection, query = newBson([]), opt = newBson([])): int =
  ## Count documents
  var
    hint, readConcern, collation: BsonBase
    limit = 0
    skip = 0
  for k, v in opt.mpairs:
    case k
    of "hint": hint = move v
    of "readConcern": readConcern = move v
    of "collation": collation = move v
    of "limit": limit = move v
    of "skip": skip = move v
  let doc = crud.count(c.db, c.name, query, limit, skip, hint,
    readConcern, collation)
  result = doc["n"]

proc createIndex*(c: Collection, key: BsonDocument, opt = newBson([])): WriteResult =
  ## Create index
  let wt = if "writeConcern" in opt: opt["writeConcern"]
           else: bsonNull()
  var q = bson({ key: key })
  if "name" notin opt:
    var name = ""
    for k, v in key:
      name &= &"{k}_{v}_"
    q["name"] = move name
  for k, v in opt:
    var kk = k
    q[move kk] = v
  let qarr = bsonArray q.toBson
  let doc = crud.createIndexes(c.db, c.name, qarr, wt)
  result = doc.getWResult

proc listIndexes*(c: Collection): seq[BsonDocument] =
  ## List collection indexes
  let indexes = crud.listIndexes(c.db, c.name)
  result = indexes.map ofEmbedded

proc `distinct`*(c: Collection, field: string, query = newBson([]),
  opt = newBson([])): seq[BsonBase] =
  ## Get distinct values
  var readConcern, collation: BsonBase
  if "readConcern" in opt: readConcern = opt["readConcern"]
  if "collation" in opt: collation = opt["collation"]
  let doc = crud.distinct(c.db, c.name, field, query, readConcern, collation)
  result = doc["values"].ofArray

proc dropIndex*(c: Collection, indexes: BsonBase): WriteResult =
  ## Drop index
  let doc = crud.dropIndexes(c.db, c.name, indexes)
  result = doc.getWResult

proc dropIndexes*(c: Collection, indexes: seq[string]): WriteResult =
  ## Drop multiple indexes
  let doc = crud.dropIndexes(c.db, c.name, indexes.map toBson)
  result = doc.getWResult

proc aggregate*(c: Collection, pipeline: seq[BsonDocument], opt = newBson([])): seq[BsonDocument] =
  ## Aggregate documents
  type tempopt = object
    explain {.bsonExport.}: bool
    diskuse {.bsonExport, bsonKey: "allowDiskUse".}: bool
    cursor {.bsonExport.}: BsonDocument
    maxTimeMS {.bsonExport.}: int
    bypass {.bsonExport, bsonKey: "bypassDocumentValidation".}: bool
    readConcern {.bsonExport.}: BsonBase
    collation {.bsonExport.}: BsonBase
    hint {.bsonExport.}: BsonBase
    comment {.bsonExport.}: string
    wt {.bsonExport, bsonKey: "writeConcern".}: BsonBase
  var optobj = opt.to tempopt
  if not optobj.explain and optobj.cursor.isNil:
    optobj.cursor = newBson([])
  let reply = c.db.aggregate(c.name, pipeline,
    optobj.explain, optobj.diskuse, optobj.cursor, optobj.maxTimeMS,
    optobj.bypass, optobj.readConcern, optobj.collation, optobj.hint,
    optobj.comment, optobj.wt)
  result = reply["cursor"]["firstBatch"].ofArray.map ofEmbedded

proc preparebulkUpdate(op: BsonDocument, wt: BsonBase, ordered: bool,
  db: Database): (BsonDocument, BsonBase, BsonDocument) =
  ## Prepare bulk update operation
  result[1] = bsonNull()
  result[2] = newBson([])
  for k, v in op:
    if k == "filter": result[0] = v.ofEmbedded
    elif k == "update": result[1] = v
    else: result[2][k] = v
  if not ordered:
    result[2]["ordered"] = false
  result[2].addWriteConcern(db, wt)

proc bulkWrite*(c: Collection, operations: seq[BsonDocument],
  wt = bsonNull(), ordered = true): BulkResult =
  ## Bulk write operations
  var wr: WriteResult
  let opt = bson({ writeConcern: wt, ordered: ordered })
  
  template checkOrdered(wr: WriteResult, target, fieldName: untyped): untyped =
    if not wr.success or wr.errmsgs.len != 0:
      if wr.reason != "":
        var s = wr.reason
        target.writeErrors.add move(s)
      for s in wr.errmsgs:
        var ss = s
        target.writeErrors.add move(ss)
      if ordered: return
    target.fieldName += wr.n
    
  template updateOp(optype: string, op: BsonDocument): untyped =
    let (query, update, updopt) = op[optype].ofEmbedded.
      preparebulkUpdate(wt, ordered, c.db)
    wr = c.update(query, update, updopt)
    checkOrdered(wr, result, nModified)
    
  template removeOp(optype: string, op: BsonDocument, one = false): untyped =
    wr = c.remove(op[optype]["filter"].ofEmbedded, justone = one)
    checkOrdered(wr, result, nRemoved)
    
  for i, op in operations:
    if "insertOne" in op:
      wr = c.insert(@[op["insertOne"]["document"].ofEmbedded], opt)
      checkOrdered(wr, result, nInserted)
    elif "deleteOne" in op:
      removeOp("deleteOne", op, true)
    elif "updateOne" in op:
      updateOp("updateOne", op)
    elif "deleteMany" in op:
      removeOp("deleteMany", op, false)
    elif "updateMany" in op:
      updateOp("updateMany", op)
    elif "replaceOne" in op:
      var (query, _, updopt) = op["replaceOne"].ofEmbedded.
        preparebulkUpdate(wt, ordered, c.db)
      updopt.del "replacement"
      let update = op["replaceOne"]["replacement"]
      wr = c.update(query, update, updopt)
      checkOrdered(wr, result, nModified)
    else:
      var key: string
      for k, _ in op:
        key = k
        break
      var msg = &"Invalid command given: {key}"
      raise newException(MongoError, move msg)

# Convenience methods for single operations
proc insertOne*(c: Collection, doc: BsonDocument): WriteResult =
  ## Insert one document
  var retryable = c.db.retryableWrites
  retryable.operationFor("insertOne"):
    let doc = c.db.insert(c.name, @[doc], true)
    result = doc.getWSingleResult

proc insertMany*(c: Collection, docs: seq[BsonDocument]): WriteResult =
  ## Insert many documents
  result = c.insert(docs)

proc updateOne*(c: Collection, filter: BsonDocument, update: BsonDocument,
               upsert = false): WriteResult =
  ## Update one document
  var opt = newBson([])
  if upsert:
    opt["upsert"] = upsert
  var q = bson({
    q: filter,
    u: update,
  })
  for k, v in opt:
    var kk = k
    q[move kk] = v
  var retryable = c.db.retryableWrites
  retryable.operationFor("updateOne"):
    let doc = c.db.update(c.name, @[q], ordered = true)
    result = doc.getWSingleResult

proc updateMany*(c: Collection, filter: BsonDocument, update: BsonDocument): WriteResult =
  ## Update many documents
  let opt = bson({"multi": true})
  result = c.update(filter, update, opt)

proc deleteOne*(c: Collection, filter: BsonDocument): WriteResult =
  ## Delete one document
  let limit = 1
  var delq = bson({
    q: filter,
    limit: limit
  })
  var retryable = c.db.retryableWrites
  retryable.operationFor("deleteOne"):
    let doc = c.db.delete(c.name, @[delq])
    result = doc.getWSingleResult

proc deleteMany*(c: Collection, filter: BsonDocument): WriteResult =
  ## Delete many documents
  result = c.remove(filter, justone = false)

proc replaceOne*(c: Collection, filter: BsonDocument, replacement: BsonDocument,
                upsert = false): WriteResult =
  ## Replace one document
  var opt = newBson([])
  if upsert:
    opt["upsert"] = upsert
  result = c.update(filter, replacement, opt)

proc estimatedDocumentCount*(c: Collection): int =
  ## Get estimated document count
  let doc = c.count()
  result = doc

proc countDocuments*(c: Collection, filter = newBson([]), 
                    opt = newBson([])): int =
  ## Count documents matching filter
  result = c.count(filter, opt)

proc watch*(c: Collection, pipeline: seq[BsonDocument] = @[],
           opt = newBson([])): Cursor =
  ## Watch for changes on collection
  # This would require change streams implementation
  # For now, return empty cursor
  let cursorDoc = bson({
    "id": 0,
    "ns": c.dbname & "." & c.name,
    "firstBatch": [],
    "nextBatch": []
  })
  result = cursorDoc.toCursor()
  result.db = c.db

proc ensureIndex*(c: Collection, key: BsonDocument, 
                 opt = newBson([])): WriteResult =
  ## Ensure index exists (alias for createIndex)
  result = c.createIndex(key, opt)

proc getIndexes*(c: Collection): seq[BsonDocument] =
  ## Get indexes (alias for listIndexes)
  result = c.listIndexes()

proc getIndexSpecs*(c: Collection): seq[BsonDocument] =
  ## Get index specifications
  result = c.listIndexes()

proc getIndexKeys*(c: Collection): seq[BsonDocument] =
  ## Get index keys
  let indexes = c.listIndexes()
  result = indexes.map(proc(idx: BsonDocument): BsonDocument = idx["key"].ofEmbedded)

proc dropIndex*(c: Collection, indexName: string): WriteResult =
  ## Drop index by name
  let doc = crud.dropIndexes(c.db, c.name, indexName.toBson)
  result = doc.getWResult

proc dropIndexes*(c: Collection): WriteResult =
  ## Drop all indexes except _id
  let doc = crud.dropIndexes(c.db, c.name, "*".toBson)
  result = doc.getWResult

proc getFullName*(c: Collection): string =
  ## Get full collection name (database.collection)
  result = c.dbname & "." & c.name

proc getName*(c: Collection): string =
  ## Get collection name
  result = c.name

proc getDB*(c: Collection): Database =
  ## Get database
  result = c.db

proc help*(c: Collection): string =
  ## Get help text
  result = """
Collection methods:
  aggregate(pipeline, opt) - run aggregation pipeline
  bulkWrite(operations, opt) - bulk write operations
  count(query, opt) - count documents
  countDocuments(filter, opt) - count documents matching filter
  createIndex(key, opt) - create index
  deleteMany(filter) - delete multiple documents
  deleteOne(filter) - delete one document
  distinct(field, query, opt) - get distinct values
  drop() - drop collection
  dropIndex(index) - drop index
  dropIndexes() - drop all indexes
  estimatedDocumentCount() - get estimated count
  find(query, projection) - find documents
  findAndModify(...) - find and modify
  findOne(query, projection, sort) - find one document
  findAll(query, projection, sort, limit) - find all documents
  findIter(query, projection, sort) - find with cursor
  getIndexes() - list indexes
  insert(docs, opt) - insert documents
  insertMany(docs) - insert multiple documents
  insertOne(doc) - insert one document
  listIndexes() - list indexes
  remove(query, opt) - remove documents
  replaceOne(filter, replacement, opt) - replace one document
  update(query, update, opt) - update documents
  updateMany(filter, update) - update multiple documents
  updateOne(filter, update, opt) - update one document
"""