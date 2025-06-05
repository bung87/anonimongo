# Multi-Threading and GC-Safe Implementation in Anonimongo

## Overview

This document describes the comprehensive multi-threading and GC-safe implementation added to the Anonimongo MongoDB driver. The implementation provides thread-safe connection pooling, atomic operations, and proper memory management for concurrent access.

## Key Features

### 1. Generic Type System

All core types have been made generic to support both synchronous and asynchronous socket operations:

```nim
type
  Mongo*[S] = ptr MongoObj[S]
  Database*[S] = ptr DatabaseObj[S]
  Collection*[S] = ptr CollectionObj[S]
  Cursor*[S] = ptr CursorObj[S]
  Query*[S] = ptr QueryObj[S]
  Pool*[S] = ptr PoolObj[S]
```

### 2. Type Aliases for Backward Compatibility

To maintain compatibility with existing code:

```nim
type
  # Original API names now alias to Socket-based generics
  Mongo* = Mongo[Socket]
  Database* = Database[Socket]
  Collection* = Collection[Socket]
  Cursor* = Cursor[Socket]
  Query* = Query[Socket]
  Pool* = Pool[Socket]
```

### 3. Thread-Safe Connection Pool

#### Pool Structure
```nim
type
  ConnectionObj*[S] = object
    socket*: S
    id*: int
    poisoned*: Atomic[bool]
  
  PoolObj*[S] = object
    connections*: TableRef[int, Connection[S]]
    available*: seq[int]
    lock*: Lock
    poisoned*: Atomic[bool]
```

#### Key Pool Operations
- `getConn*[S](p: Pool[S]): (int, Connection[S])` - Thread-safe connection acquisition
- `endConn*[S](p: Pool[S], id: int)` - Thread-safe connection return
- `connect*[S](p: Pool[S], address: string, port: int)` - Bulk connection setup
- `close*[S](p: Pool[S])` - Clean shutdown with proper resource cleanup

### 4. Atomic State Management

All shared state uses atomic operations to prevent race conditions:

```nim
type
  MongoObj[S] = object
    # Atomic fields for thread safety
    primary: Atomic[string]
    tls: Atomic[bool]
    authenticated: Atomic[bool]
    flags: Atomic[QueryFlags]
    readPreference: Atomic[ReadPreference]
    retryableWrites: Atomic[bool]
    poisoned: Atomic[bool]
    
    # Protected by locks
    servers: TableRef[string, MongoConn]
    query: TableRef[string, seq[string]]
    lock: Lock
    pool: Pool[S]
```

### 5. Thread-Safe Connection Lifecycle

#### withConnection Template
```nim
template withConnection*[S](m: Mongo[S], conn: untyped, body: untyped) =
  block:
    let (id, conn) = m.getConn()
    if id != -1 and not conn.isNil and not conn.poisoned.load(moRelaxed):
      try:
        body
      finally:
        m.endConn(id)
```

This template ensures:
- Automatic connection acquisition from pool
- Proper error handling
- Guaranteed connection return to pool
- Thread-safe access patterns

### 6. Memory Management

#### GC-Safe Allocation
All objects use `allocShared0` and `deallocShared` for cross-thread safety:

```nim
proc initMongo*[S](poolSize = poolconn): Mongo[S] =
  result = cast[Mongo[S]](allocShared0(sizeof(MongoObj[S])))
  initLock(result.lock)
  # ... initialization
```

#### Proper Cleanup
```nim
proc close*[S](m: Mongo[S]) {.raises: [].} =
  m.poisoned.store(true, moRelaxed)
  withLock m.lock:
    for _, conn in m.servers:
      close(conn)
  if not m.pool.isNil:
    close(m.pool)
  `=destroy`(m[])
  deallocShared(m)
```

## Usage Examples

### Basic Thread-Safe Operations

```nim
import anonimongo

# Create a thread-safe MongoDB client using generic API
let mongo = newMongo[Socket]("localhost", 27017, poolSize = 16)
defer: mongo.close()

# Or use backward-compatible type aliases
let mongo2: Mongo = newMongo[Socket]("localhost", 27017, poolSize = 16)
defer: mongo2.close()

# Thread-safe database and collection access
let db = mongo.getDatabase("mydb")
let coll = db.getCollection("mycoll")

# Use withConnection for thread-safe operations
mongo.withConnection(conn):
  # Perform operations with the connection
  let result = conn.socket.command("mydb.$cmd", bson({"ping": 1}))
```

### Multi-Threaded Access

```nim
import std/threadpool

proc workerTask(mongo: Mongo[Socket], threadId: int) =
  mongo.withConnection(conn):
    if not conn.poisoned.load():
      # Perform thread-safe operations
      echo "Thread ", threadId, " using connection ", conn.id

# Create with explicit generic type
let mongo = newMongo[Socket]("localhost", 27017, poolSize = 8)
defer: mongo.close()

# Spawn multiple threads
parallel:
  for i in 0..7:
    spawn workerTask(mongo, i)
```

### Atomic Property Access

```nim
# Thread-safe property access using either generic or alias types
let mongo = newMongo[Socket]("localhost", 27017)
# OR: let mongo: Mongo = newMongo[Socket]("localhost", 27017)

echo "Authenticated: ", mongo.authenticated
echo "TLS enabled: ", mongo.tls
echo "Read preference: ", mongo.readPreference

# Thread-safe property updates
mongo.setReadPreference(ReadPreference.secondary)
```

## Implementation Details

### Connection Selection

The implementation provides multiple connection selection strategies:

1. **Primary**: `main*[S](m: Mongo[S]): MongoConn`
2. **Primary Preferred**: `mainPreferred*[S](m: Mongo[S]): MongoConn`
3. **Secondary**: `secondary*[S](m: Mongo[S]): MongoConn`
4. **Secondary Preferred**: `secondaryPreferred*[S](m: Mongo[S]): MongoConn`

### Pool Configuration

```nim
# Configure pool size during creation
let mongo = newMongo[Socket]("localhost", 27017, poolSize = 32)

# Pool automatically manages:
# - Connection distribution
# - Connection reuse
# - Connection lifecycle
# - Error handling and recovery
```

### Error Handling

The implementation includes comprehensive error handling:

- **Poisoned Connections**: Automatically detected and excluded
- **Pool Exhaustion**: Graceful handling when all connections are in use
- **Network Failures**: Automatic connection marking and recovery
- **Resource Cleanup**: Guaranteed cleanup even in error conditions

### Memory Safety

Key memory safety features:

1. **Shared Memory**: All cross-thread objects use shared memory allocation
2. **Atomic Operations**: All shared state uses atomic primitives
3. **Lock Protection**: Critical sections protected by locks
4. **RAII**: Automatic resource cleanup through destructors
5. **Exception Safety**: Guaranteed cleanup in finally blocks

## Performance Characteristics

### Scalability
- Pool-based connection management reduces connection overhead
- Lock-free atomic operations for hot paths
- Minimal contention through fine-grained locking

### Memory Usage
- Shared memory allocation reduces per-thread overhead
- Connection reuse minimizes socket creation/destruction
- Proper cleanup prevents memory leaks

### Latency
- Connection pooling eliminates connection setup time
- Atomic operations avoid lock overhead for read operations
- Efficient connection selection algorithms

## Testing

The implementation includes comprehensive tests in `tests/test_threading.nim`:

1. **Basic Compilation**: Verifies all types compile correctly
2. **Pool Functionality**: Tests connection acquisition and return
3. **Atomic Operations**: Validates atomic property access
4. **Thread Simulation**: Simulates concurrent access patterns
5. **GC Safety**: Tests memory management under load
6. **Connection Limits**: Validates pool size constraints
7. **Type Compatibility**: Ensures type aliases work correctly
8. **State Management**: Tests connection state handling

## Usage Examples

### Basic API Usage

```nim
# Generic API usage
let mongo = newMongo[Socket]("localhost", 27017, poolSize = 16)
defer: mongo.close()

# Using type aliases for convenience
let mongo2: Mongo = newMongo[Socket]("localhost", 27017)  # Mongo = Mongo[Socket]
defer: mongo2.close()

# Thread-safe operations
mongo.withConnection(conn):
  let result = conn.socket.command("test.$cmd", query)
```

## Best Practices

1. **Use Generic Constructors**: Always use `newMongo[Socket]()` for sync or `newMongo[AsyncSocket]()` for async
2. **Choose Type Style**: Use `Mongo` (alias) for simplicity or `Mongo[Socket]` for clarity
3. **Configure Pool Size**: Set pool size based on expected concurrent load (typically 2-4x CPU cores)
4. **Use withConnection**: Always use the template for thread-safe access - never access connections directly
5. **Proper Cleanup**: Always call `close()` on Mongo objects, preferably with `defer:`
6. **Error Handling**: Check connection validity before operations (`conn.poisoned.load()`)
7. **Atomic Access**: Use provided atomic accessors for properties (`mongo.readPreference`, not direct field access)

## Future Enhancements

1. **Full Async Support**: Complete `newMongo[AsyncSocket]()` implementation with async/await integration
2. **Connection Health Monitoring**: Automatic connection health checks and recovery
3. **Load Balancing**: Advanced connection selection algorithms for replica sets
4. **Pool Metrics**: Connection pool statistics and monitoring capabilities
5. **Dynamic Configuration**: Runtime pool size and configuration updates
6. **SSL/TLS Integration**: Thread-safe SSL/TLS connection handling

## Conclusion

This implementation provides a robust, thread-safe foundation for MongoDB operations in multi-threaded Nim applications. The key achievements include:

- **Generic Type System**: `Mongo[S]`, `Database[S]`, etc. supporting both sync and async operations
- **Thread-Safe Connection Pooling**: Automatic connection lifecycle management with atomic operations
- **Memory Safety**: GC-safe allocation and proper cleanup for cross-thread usage
- **Backward Compatibility**: Original API preserved through type aliases (`Mongo = Mongo[Socket]`)

### Usage Summary:
```nim
# Create thread-safe client
let mongo = newMongo[Socket]("localhost", 27017, poolSize = 16)
defer: mongo.close()

# Thread-safe operations
mongo.withConnection(conn):
  # Your operations here
  
# Backward compatible
let mongo2: Mongo = newMongo[Socket]("localhost", 27017)  # Mongo = Mongo[Socket]
```

The design successfully bridges traditional single-threaded MongoDB access with modern concurrent programming needs, ensuring both safety and performance in multi-threaded Nim applications.