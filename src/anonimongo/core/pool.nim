import tables, locks, net, bson, auth, scram/client

export tables.pairs

type
  ConnectionObj*[S] = object
    socket*: S
    id*: int
    poisoned*: bool
    lock*: Lock
  Connection*[S] = ptr ConnectionObj[S]
  PoolObj*[S] = object
    connections*: TableRef[int, Connection[S]]
    available*: seq[int]
    lock*: Lock
    poisoned*: bool

  Pool*[S] = ptr PoolObj[S]

proc initConnection*[S](id: int): Connection[S] =
  result = cast[Connection[S]](allocShared0(sizeof(ConnectionObj[S])))
  when S is Socket:
    result.socket = newSocket()
  result.id = id
  initLock(result[].lock)
  result.poisoned = false

proc initPool*[S](size = 16): Pool[S] =
  result = cast[Pool[S]](allocShared0(sizeof(PoolObj[S])))
  initLock(result[].lock)
  result.connections = newTable[int, Connection[S]]()
  result.poisoned = false
  for i in 1..size:
    let conn = initConnection[S](i)
    result.connections[i] = conn
    result.available.add(i)

proc getConn*[S](p: Pool[S]): (int, Connection[S]) =
  acquire(p[].lock)
  try:
    while p.available.len > 0:
      let id = p.available.pop()
      let conn = p.connections[id]
      withLock conn[].lock:
        if not conn.poisoned:
          result = (id, conn)
          return
    # No available connections
    result = (-1, nil)
  finally:
    release(p[].lock)

proc endConn*[S](p: Pool[S], id: int) =
  withLock(p[].lock):
    p.available.add(id)

proc connect*[S](p: Pool[S], address: string, port: int) =
  for id, conn in p.connections:
    try:
      when S is Socket:
        conn.socket.connect(address, Port(port))
    except:
      withLock conn[].lock:
        conn.poisoned = true

proc close*[S](p: Pool[S]) =
  withLock(p[].lock):
    p.poisoned = true
    for _, conn in p.connections:
      when S is Socket:
        close(conn.socket)
      deallocShared(conn)
  deallocShared(p)

proc authenticate*[S](p: Pool[S], user, pass: string, dbname = "admin.$cmd"): bool =
  withLock(p[].lock):
    for _, conn in p.connections:
      when S is Socket:
        let res = conn.socket.authenticate(user, pass, Sha256Digest, dbname)
        if not res:
          return false
  true

when isMainModule:
  proc worker(pool: Pool[Socket], id: int) =
    let (cid, conn) = pool.getConn()
    if cid != -1:
      defer: pool.endConn(cid)
      if not conn.poisoned.load():
        discard conn.socket.send("PING")
  
  proc main() =
    let pool = initPool[Socket](16)
    pool.connect("localhost", 27017)
    var threads: array[16, Thread[tuple[p: Pool[Socket], id: int]]]
    for i in 0..<16:
      createThread(threads[i], worker, (pool, i))
    joinThreads(threads)
    close(pool)
  
  main()