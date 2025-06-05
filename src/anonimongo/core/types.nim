import uri, tables, strutils, net, strformat, unicode, locks
from asyncdispatch import Port

when defined(ssl):
  import openssl

import sha1, nimSHA2
import dnsclient

import pool, wire, bson, multisock

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

  MongoObj[S] = object
    hosts: seq[string]
    primary: string
    servers: TableRef[string, MongoConn]
    pool: Pool[S]
    tls: bool
    authenticated: bool
    db: string
    writeConcern: BsonDocument
    flags: QueryFlags
    readPreference: ReadPreference
    retryableWrites: bool
    compressions: seq[CompressorId]
    query: TableRef[string, seq[string]]
    lock: Lock
    poisoned: bool

  Mongo*[S] = ptr MongoObj[S]

  ReadPreference* {.pure.} = enum
    primary = "primary"
    primaryPreferred = "primaryPreferred"
    secondary = "secondary"
    secondaryPreferred = "secondaryPreferred"
    nearest = "nearest"

  DatabaseObj[S] = object
    name: string
    db: Mongo[S]

  Database*[S] = ptr DatabaseObj[S]

  CollectionObj[S] = object
    name: string
    dbname: string
    db: Database[S]

  Collection*[S] = ptr CollectionObj[S]

  CursorObj[S] = object
    id: int64
    firstBatch: seq[BsonDocument]
    nextBatch: seq[BsonDocument]
    db: Database[S]
    ns: string
    poisoned: bool

  Cursor*[S] = ptr CursorObj[S]

  QueryObj[S] = object
    query: BsonDocument
    sort: BsonBase
    projection: BsonBase
    writeConcern: BsonBase
    collection: Collection[S]
    skip: int32
    limit: int32
    batchSize: int32
    readConcern: BsonBase
    max: BsonBase
    min: BsonBase

  Query*[S] = ptr QueryObj[S]

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

  GridFS*[S] = ref object
    name*: string
    files*: Collection[S]
    chunks*: Collection[S]
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
proc initMongo*[S](poolSize = poolconn): Mongo[S] =
  result = cast[Mongo[S]](allocShared0(sizeof(MongoObj[S])))
  initLock(result[].lock)
  result.servers = newTable[string, MongoConn]()
  result.query = newTable[string, seq[string]]()
  result.pool = initPool[S](poolSize)
  result.poisoned = false
  result.authenticated = false
  result.tls = false
  result.primary = ""
  result.readPreference = ReadPreference.primary
  result.retryableWrites = true
  result.flags = QueryFlags({})
  result.compressions = @[]

proc connect*[S](m: Mongo[S], host: string, port: int) =
  withLock m[].lock:
    let key = host & ":" & $port
    if key notin m.servers:
      let conn = newMongoConn(host, port)
      try:
        when S is Socket:
          conn.socket.connect(host, Port(port))
        m.servers[key] = conn
      except OSError as e:
        withLock conn[].lock:
          conn.poisoned = true
        raise newException(MongoError, "Connection failed: " & e.msg)
  # Also connect the pool
  when S is Socket:
    m.pool.connect(host, port)

proc authenticate*[S](m: Mongo[S], user, pass: string, dbname = "admin"): bool =
  result = m.pool.authenticate(user, pass, dbname & ".$cmd")
  if result:
    withLock m[].lock:
      m.authenticated = true

proc close*[S](m: Mongo[S]) {.raises: [].} =
  withLock m[].lock:
    m.poisoned = true
    for _, conn in m.servers:
      close(conn)
  if not m.pool.isNil:
    close(m.pool)
  `=destroy`(m[])
  deallocShared(m)

# Database and Collection management
proc getDatabase*[S](m: Mongo[S], name: string): Database[S] =
  result = cast[Database[S]](allocShared0(sizeof(DatabaseObj[S])))
  result.db = m
  result.name = name

proc getCollection*[S](db: Database[S], name: string): Collection[S] =
  result = cast[Collection[S]](allocShared0(sizeof(CollectionObj[S])))
  result.name = name
  result.dbname = db.name
  result.db = db

proc close*[S](db: Database[S]) {.raises: [].} =
  `=destroy`(db[])
  deallocShared(db)

proc close*[S](coll: Collection[S]) {.raises: [].} =
  `=destroy`(coll[])
  deallocShared(coll)

# Query operations
proc query*[S](coll: Collection[S], query: BsonDocument): Query[S] =
  result = cast[Query[S]](allocShared0(sizeof(QueryObj[S])))
  result.query = query
  result.collection = coll
  result.batchSize = 101

proc find*[S](q: Query[S]): Cursor[S] {.raises: [MongoError].} =
  result = cast[Cursor[S]](allocShared0(sizeof(CursorObj[S])))
  result.poisoned = false
  result.firstBatch = @[]
  result.id = 0
  result.ns = q.collection.dbname & "." & q.collection.name
  result.db = q.collection.db

proc next*[S](c: Cursor[S]): seq[BsonDocument] {.raises: [MongoError].} =
  if c.id != 0 and not c.poisoned:
    c.nextBatch = @[]
    c.id = 0
  result = c.nextBatch

proc close*[S](c: Cursor[S]) {.raises: [].} =
  if not c.poisoned:
    c.id = 0
    c.poisoned = true
  `=destroy`(c[])
  deallocShared(c)

# Connection pool integration
proc getConn*[S](m: Mongo[S]): (int, Connection[S]) =
  withLock m[].lock:
    if m.poisoned:
      return (-1, nil)
  result = m.pool.getConn()

proc endConn*[S](m: Mongo[S], id: int) =
  var isPoisoned: bool
  withLock m[].lock:
    isPoisoned = m.poisoned
  if not isPoisoned and not m.pool.isNil:
    m.pool.endConn(id)

template withConnection*[S](m: Mongo[S], conn: untyped, body: untyped) =
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
proc newMongo*[S](host = "localhost", port = 27017, master = true,
                  poolSize = poolconn): Mongo[S] =
  result = initMongo[S](poolSize)
  result.connect(host, port)

proc newMongo*[S](uri: MongoUri, poolSize = poolconn): Mongo[S] =
  let uriStr = uri.string
  let parsed = parseUri(uriStr)
  let host = if parsed.hostname != "": parsed.hostname else: "localhost"
  let port = if parsed.port != "": parsed.port.parseInt else: 27017
  result = newMongo[S](host, port, poolSize = poolSize)

# Property accessors using locks
proc authenticated*[S](m: Mongo[S]): bool =
  withLock m[].lock:
    result = m.authenticated

proc tls*[S](m: Mongo[S]): bool =
  withLock m[].lock:
    result = m.tls

proc readPreference*[S](m: Mongo[S]): ReadPreference =
  withLock m[].lock:
    result = m.readPreference

proc retryableWrites*[S](m: Mongo[S]): bool =
  withLock m[].lock:
    result = m.retryableWrites

proc setReadPreference*[S](m: Mongo[S], pref: ReadPreference) =
  withLock m[].lock:
    m.readPreference = pref

# GridFS support
proc newGridFS*[S](db: Database[S], name = "fs"): GridFS[S] =
  result = GridFS[S](
    name: name,
    files: db.getCollection(name & ".files"),
    chunks: db.getCollection(name & ".chunks"),
    chunkSize: 255 * 1024
  )

# Utility functions
proc dbname*[S](db: Database[S]): string = db.name
proc dbname*[S](coll: Collection[S]): string = coll.dbname
proc collname*[S](coll: Collection[S]): string = coll.name

# Connection selection methods for compatibility
proc main*[S](m: Mongo[S]): MongoConn =
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

proc mainPreferred*[S](m: Mongo[S]): MongoConn =
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

proc secondary*[S](m: Mongo[S]): MongoConn =
  ## Get secondary connection
  withLock m[].lock:
    for hostname, conn in m.servers:
      if hostname != m.primary:
        withLock conn[].lock:
          if not conn.poisoned:
            return conn

proc secondaryPreferred*[S](m: Mongo[S]): MongoConn =
  ## Get secondary-preferred connection
  result = m.secondary()
  if result.isNil:
    result = m.main()

# Database/Collection accessors
proc `[]`*[S](m: Mongo[S], name: string): Database[S] =
  m.getDatabase(name)

proc `[]`*[S](db: Database[S], name: string): Collection[S] =
  db.getCollection(name)

# Helper to convert MongoUri from string
proc `$`*(uri: MongoUri): string = uri.string