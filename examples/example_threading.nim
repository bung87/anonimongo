## Multi-Threading Example for Anonimongo
## 
## This example demonstrates how to use the new thread-safe, generic API
## for concurrent MongoDB operations.

import std/threadpool, std/atomics, std/times, net
import src/anonimongo

# Example 1: Basic thread-safe usage
proc basicExample() =
  echo "=== Basic Thread-Safe Example ==="
  
  # Create a thread-safe MongoDB client with connection pool
  let mongo = newMongo[Socket]("localhost", 27017, poolSize = 8)
  defer: mongo.close()
  
  # These type aliases work for backward compatibility
  let db: Database = mongo.getDatabase("example_db")
  let coll: Collection = db.getCollection("test_collection")
  
  # Thread-safe connection access
  mongo.withConnection(conn):
    echo "Connected with connection ID: ", conn.id
    echo "Connection is healthy: ", not conn.poisoned.load()
  
  echo "✓ Basic example completed\n"

# Example 2: Concurrent operations simulation
proc concurrentExample() =
  echo "=== Concurrent Operations Example ==="
  
  let mongo = newMongo[Socket]("localhost", 27017, poolSize = 4)
  defer: mongo.close()
  
  var operationCount = Atomic[int]
  var successCount = Atomic[int]
  
  proc workerTask(threadId: int, operations: int) =
    for i in 0..<operations:
      operationCount.atomicInc()
      
      # Each operation gets a connection from the pool
      mongo.withConnection(conn):
        if not conn.isNil and not conn.poisoned.load():
          # Simulate some work
          successCount.atomicInc()
          echo "Thread ", threadId, " operation ", i, " using connection ", conn.id
  
  # Spawn multiple concurrent tasks
  echo "Spawning concurrent tasks..."
  parallel:
    for i in 0..3:
      spawn workerTask(i, 5)
  
  let totalOps = operationCount.load()
  let successful = successCount.load()
  
  echo "Total operations: ", totalOps
  echo "Successful operations: ", successful
  echo "✓ Concurrent example completed\n"

# Example 3: Generic type usage
proc genericExample() =
  echo "=== Generic Types Example ==="
  
  # Explicit generic usage
  let syncMongo: Mongo[Socket] = newMongo[Socket]("localhost", 27017, poolSize = 2)
  defer: syncMongo.close()
  
  # Using type aliases
  let aliasMongo: Mongo = newMongo[Socket]("localhost", 27017, poolSize = 2)
  defer: aliasMongo.close()
  
  # Test atomic properties
  echo "Read preference: ", syncMongo.readPreference
  echo "Retry writes enabled: ", syncMongo.retryableWrites
  echo "Authenticated: ", syncMongo.authenticated
  echo "TLS enabled: ", syncMongo.tls
  
  # Modify read preference atomically
  syncMongo.setReadPreference(ReadPreference.secondary)
  echo "New read preference: ", syncMongo.readPreference
  
  echo "✓ Generic types example completed\n"

# Example 4: Connection pool behavior
proc poolExample() =
  echo "=== Connection Pool Example ==="
  
  # Small pool to demonstrate pooling behavior
  let mongo = newMongo[Socket]("localhost", 27017, poolSize = 2)
  defer: mongo.close()
  
  echo "Testing connection acquisition..."
  
  # Get connections manually (normally you'd use withConnection)
  let (id1, conn1) = mongo.getConn()
  if id1 != -1:
    echo "Acquired connection ", id1
    
    let (id2, conn2) = mongo.getConn()
    if id2 != -1:
      echo "Acquired connection ", id2
      
      # Try to get a third connection (should fail with pool size 2)
      let (id3, conn3) = mongo.getConn()
      if id3 == -1:
        echo "Third connection request failed (pool exhausted) ✓"
      
      # Return connections
      mongo.endConn(id2)
      echo "Returned connection ", id2
    
    mongo.endConn(id1)
    echo "Returned connection ", id1
  
  echo "✓ Pool example completed\n"

# Example 5: Error handling and cleanup
proc errorHandlingExample() =
  echo "=== Error Handling Example ==="
  
  # This will work even if MongoDB is not running (for demonstration)
  block:
    let mongo = newMongo[Socket]("localhost", 27017, poolSize = 1)
    
    # Safe cleanup is guaranteed
    defer: 
      echo "Cleaning up MongoDB connection..."
      mongo.close()
    
    # withConnection handles errors automatically
    mongo.withConnection(conn):
      if not conn.isNil:
        echo "Connection successful"
      else:
        echo "Connection failed, but handled safely"
  
  echo "✓ Error handling example completed\n"

# Main execution
when isMainModule:
  echo "Anonimongo Multi-Threading Examples"
  echo "===================================\n"
  
  try:
    basicExample()
    concurrentExample()
    genericExample()
    poolExample()
    errorHandlingExample()
    
    echo "All examples completed successfully! 🎉"
    echo "\nKey takeaways:"
    echo "1. Use newMongo[Socket](...) for sync operations"
    echo "2. Use newMongo[AsyncSocket](...) for async operations" 
    echo "3. Type aliases (Mongo, Database, etc.) work for backward compatibility"
    echo "4. Always use withConnection{} for thread-safe operations"
    echo "5. Configure poolSize based on your concurrency needs"
    echo "6. The implementation handles cleanup and error cases automatically"
    
  except Exception as e:
    echo "Error running examples: ", e.msg
    echo "Make sure MongoDB is running on localhost:27017"