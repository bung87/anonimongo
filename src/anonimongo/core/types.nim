import uri, tables, strutils, net, locks, sequtils
from asyncdispatch import Port

when defined(ssl):
  import openssl

import sha1, nimSHA2

import pool, wire, bson

export SHA1Digest, SHA256Digest

type
  QueryFlag* = enum
    TailableCursor
    SlaveOk
    OplogReplay
    NoCursorTimeout
    AwaitData
    Exhaust
    Partial
  
  QueryFlags* = set[QueryFlag]

  CompressorId* = enum
    cidNoop = 0
    cidSnappy = 1
    cidZlib = 2
    cidZstd = 3

const
  poolconn* {.intdefine.} = 64
  verbose* = defined(verbose)
  verifypeer* = defined(verifypeer)
  cafile* {.strdefine.} = ""

type
  MongoConnObj = object
    socket: Socket
    id: int
    poisoned: bool
    authenticated: bool
    isMaster: bool
    host: string
    port: Port
    username: string
    password: string
    lock: Lock

  MongoConn* = ptr MongoConnObj

  MongoObj = object
    hosts: seq[string]
    primary: string
    servers: TableRef[string, MongoConn]
    pool: Pool[Socket]
    tls: bool
    authenticated: bool
    hasUserAuth: bool
    db: string
    writeConcern: BsonDocument
    flags: QueryFlags
    readPreference: ReadPreference
    retryableWrites: bool
    compressions: seq[CompressorId]
    query: TableRef[string, seq[string]]
    lock: Lock
    poisoned: bool

  Mongo* = ptr MongoObj

  ReadPreference* {.pure.} = enum
    primary = "primary"
    primaryPreferred = "primaryPreferred"
    secondary = "secondary"
    secondaryPreferred = "secondaryPreferred"
    nearest = "nearest"

  DatabaseObj = object
    name: string
    db: Mongo

  Database* = ptr DatabaseObj

  CollectionObj = object
    name*: string
    dbname*: string
    db*: Database

  Collection* = ptr CollectionObj

  CursorObj = object
    id*: int64
    firstBatch*: seq[BsonDocument]
    nextBatch*: seq[BsonDocument]
    db*: Database
    ns*: string
    poisoned*: bool

  Cursor* = ptr CursorObj

  QueryObj* = object
    query*: BsonDocument
    sort*: BsonBase
    projection*: BsonBase
    writeConcern*: BsonBase
    collection*: Collection
    skip*: int32
    limit*: int32
    batchSize*: int32
    readConcern*: BsonBase
    max*: BsonBase
    min*: BsonBase

  Query* = ptr QueryObj

  WriteKind* = enum
    wkSingle wkMany

  WriteResult* = object
    success*: bool
    reason*: string
    case kind*: WriteKind
    of wkMany:
      n*: int
      errmsgs*: seq[string]
    of wkSingle:
      discard

  BulkResult* = object
    nInserted*: int
    nModified*: int
    nRemoved*: int
    writeErrors*: seq[string]

  GridFS* = ref object
    name*: string
    files*: Collection
    chunks*: Collection
    chunkSize*: int32

  CommandKind* = enum
    ckWrite
    ckRead

  MongoError* = object of CatchableError
  
  SslInfo* = object
    keyfile*: string
    certfile*: string
    sslContext*: SslContext

  MongoUri* = distinct string

# Basic connection management
proc newMongoConn*(host: string, port: int): MongoConn =
  result = cast[MongoConn](allocShared0(sizeof(MongoConnObj)))
  result.socket = newSocket()
  result.host = host
  result.port = Port(port)
  initLock(result[].lock)
  result.poisoned = false
  result.authenticated = false
  result.isMaster = false

proc close*(conn: MongoConn) {.raises: [].} =
  if not conn.isNil:
    try:
      close(conn.socket)
    except:
      discard
    withLock conn[].lock:
      conn.poisoned = true
    `=destroy`(conn[])
    deallocShared(conn)

# Core Mongo object operations
proc initMongo*(poolSize = poolconn): Mongo =
  result = cast[Mongo](allocShared0(sizeof(MongoObj)))
  initLock(result[].lock)
  result.servers = newTable[string, MongoConn]()
  result.query = newTable[string, seq[string]]()
  result.pool = initPool[Socket](poolSize)
  result.poisoned = false
  result.authenticated = false
  result.hasUserAuth = false
  result.tls = false
  result.primary = ""
  result.readPreference = ReadPreference.primary
  result.retryableWrites = true
  result.flags = QueryFlags({})
  result.compressions = @[]

proc connect*(m: Mongo, host: string, port: int) =
  withLock m[].lock:
    let key = host & ":" & $port
    if key notin m.servers:
      let conn = newMongoConn(host, port)
      try:
        conn.socket.connect(host, Port(port))
        m.servers[key] = conn
      except OSError as e:
        withLock conn[].lock:
          conn.poisoned = true
        raise newException(MongoError, "Connection failed: " & e.msg)
  # Also connect the pool
  m.pool.connect(host, port)

proc authenticate*(m: Mongo, user, pass: string, dbname = "admin"): bool =
  result = m.pool.authenticate(user, pass, dbname & ".$cmd")
  if result:
    withLock m[].lock:
      m.authenticated = true
      m.hasUserAuth = true

proc close*(m: Mongo) {.raises: [].} =
  withLock m[].lock:
    m.poisoned = true
    for _, conn in m.servers:
      close(conn)
  if not m.pool.isNil:
    try:
      close(m.pool)
    except:
      discard
  `=destroy`(m[])
  deallocShared(m)

# Database and Collection management
proc getDatabase*(m: Mongo, name: string): Database =
  result = cast[Database](allocShared0(sizeof(DatabaseObj)))
  result.db = m
  result.name = name

proc getCollection*(db: Database, name: string): Collection =
  result = cast[Collection](allocShared0(sizeof(CollectionObj)))
  result.name = name
  result.dbname = db.name
  result.db = db

proc close*(db: Database) {.raises: [].} =
  `=destroy`(db[])
  deallocShared(db)

proc close*(coll: Collection) {.raises: [].} =
  `=destroy`(coll[])
  deallocShared(coll)

# Query operations
proc query*(coll: Collection, query: BsonDocument): Query =
  result = cast[Query](allocShared0(sizeof(QueryObj)))
  result.query = query
  result.collection = coll
  result.batchSize = 101

proc find*(q: Query): Cursor {.raises: [MongoError].} =
  result = cast[Cursor](allocShared0(sizeof(CursorObj)))
  result.poisoned = false
  result.firstBatch = @[]
  result.id = 0
  result.ns = q.collection.dbname & "." & q.collection.name
  result.db = q.collection.db

proc next*(c: Cursor): seq[BsonDocument] {.raises: [MongoError].} =
  if c.id != 0 and not c.poisoned:
    c.nextBatch = @[]
    c.id = 0
  result = c.nextBatch

proc close*(c: Cursor) {.raises: [].} =
  if not c.poisoned:
    c.id = 0
    c.poisoned = true
  `=destroy`(c[])
  deallocShared(c)

# Connection pool integration
proc getConn*(m: Mongo): (int, Connection[Socket]) =
  withLock m[].lock:
    if m.poisoned:
      return (-1, nil)
  result = m.pool.getConn()

proc endConn*(m: Mongo, id: int) =
  var isPoisoned: bool
  withLock m[].lock:
    isPoisoned = m.poisoned
  if not isPoisoned and not m.pool.isNil:
    m.pool.endConn(id)

template withConnection*(m: Mongo, conn: untyped, body: untyped) =
  block:
    let (id, conn) = m.getConn()
    if id != -1 and not conn.isNil:
      var connPoisoned: bool
      withLock conn[].lock:
        connPoisoned = conn.poisoned
      if not connPoisoned:
        try:
          body
        finally:
          m.endConn(id)

# Constructors
proc newMongo*(host = "localhost", port = 27017, master = true,
                  poolSize = poolconn, ssl = false): Mongo =
  result = initMongo(poolSize)
  withLock result[].lock:
    result.tls = ssl
  result.connect(host, port)

proc newMongo*(uri: MongoUri, poolSize = poolconn, ssl = false): Mongo =
  let uriStr = uri.string
  let parsed = parseUri(uriStr)
  let host = if parsed.hostname != "": parsed.hostname else: "localhost"
  let port = if parsed.port != "": parsed.port.parseInt else: 27017
  let useSSL = ssl or uriStr.startsWith("mongodb+srv://") or uriStr.contains("ssl=true")
  result = newMongo(host, port, poolSize = poolSize, ssl = useSSL)

proc newMongo*(host = "localhost", port = 27017, poolSize = poolconn, 
               sslInfo: SslInfo): Mongo =
  ## Create a MongoDB client with SSL context
  result = initMongo(poolSize)
  withLock result[].lock:
    result.tls = true
  result.connect(host, port)

when defined(ssl):
  proc newMongo*(host = "localhost", port = 27017, poolSize = poolconn,
                 sslContext: SslContext): Mongo =
    ## Create a MongoDB client with custom SSL context
    result = initMongo(poolSize)
    withLock result[].lock:
      result.tls = true
    result.connect(host, port)

# Property accessors using locks
proc authenticated*(m: Mongo): bool =
  withLock m[].lock:
    result = m.authenticated

proc tls*(m: Mongo): bool =
  withLock m[].lock:
    result = m.tls

proc readPreference*(m: Mongo): ReadPreference =
  withLock m[].lock:
    result = m.readPreference

proc retryableWrites*(m: Mongo): bool =
  withLock m[].lock:
    result = m.retryableWrites

proc hasUserAuth*(m: Mongo): bool =
  withLock m[].lock:
    result = m.hasUserAuth

proc setReadPreference*(m: Mongo, pref: ReadPreference) =
  withLock m[].lock:
    m.readPreference = pref

proc setTls*(m: Mongo, useTls: bool) =
  withLock m[].lock:
    m.tls = useTls

# GridFS support
proc newGridFS*(db: Database, name = "fs"): GridFS =
  result = GridFS(
    name: name,
    files: db.getCollection(name & ".files"),
    chunks: db.getCollection(name & ".chunks"),
    chunkSize: 255 * 1024
  )

# Utility functions
proc dbname*(db: Database): string = db.name
proc dbname*(coll: Collection): string = coll.dbname
proc collname*(coll: Collection): string = coll.name

# Connection selection methods for compatibility
proc main*(m: Mongo): MongoConn =
  ## Get the primary connection
  withLock m[].lock:
    if m.primary == "":
      for hostname, conn in m.servers:
        withLock conn[].lock:
          if not conn.poisoned:
            return conn
    else:
      if m.primary in m.servers:
        return m.servers[m.primary]

proc mainPreferred*(m: Mongo): MongoConn =
  ## Get primary-preferred connection
  result = m.main()
  if result.isNil:
    withLock m[].lock:
      for hostname, conn in m.servers:
        withLock conn[].lock:
          if not conn.poisoned:
            return conn
  else:
    withLock result[].lock:
      if result.poisoned:
        withLock m[].lock:
          for hostname, conn in m.servers:
            withLock conn[].lock:
              if not conn.poisoned:
                return conn

proc secondary*(m: Mongo): MongoConn =
  ## Get secondary connection
  withLock m[].lock:
    for hostname, conn in m.servers:
      if hostname != m.primary:
        withLock conn[].lock:
          if not conn.poisoned:
            return conn

proc secondaryPreferred*(m: Mongo): MongoConn =
  ## Get secondary-preferred connection
  result = m.secondary()
  if result.isNil:
    result = m.main()

# Database/Collection accessors
proc `[]`*(m: Mongo, name: string): Database =
  m.getDatabase(name)

proc `[]`*(db: Database, name: string): Collection =
  db.getCollection(name)

# Collection methods removed to avoid circular dependency
# These are now implemented in collections.nim

# Helper functions for field access
proc name*(db: Database): string = db[].name
proc retryableWrites*(db: Database): bool = db[].db[].retryableWrites

# Helper function to convert BsonDocument to cursor
proc toCursor*(doc: BsonDocument): Cursor =
  ## Convert cursor document to Cursor object
  result = cast[Cursor](allocShared0(sizeof(CursorObj)))
  
  # Handle cursor ID - could be int32 or int64
  if "id" in doc:
    let idField = doc["id"]
    if idField.kind == bkInt64:
      result.id = idField.ofInt64
    elif idField.kind == bkInt32:
      result.id = int64(idField.ofInt32)
    else:
      result.id = 0
  else:
    result.id = 0
    
  result.ns = if "ns" in doc: doc["ns"].ofString else: ""
  result.poisoned = false
  
  # Handle firstBatch
  if "firstBatch" in doc:
    let batch = doc["firstBatch"].ofArray
    result.firstBatch = batch.map(proc(x: BsonBase): BsonDocument = x.ofEmbedded)
  else:
    result.firstBatch = @[]
  
  # Handle nextBatch
  if "nextBatch" in doc:
    let batch = doc["nextBatch"].ofArray
    result.nextBatch = batch.map(proc(x: BsonBase): BsonDocument = x.ofEmbedded)
  else:
    result.nextBatch = @[]

# Helper to convert MongoUri from string
proc `$`*(uri: MongoUri): string = uri.string