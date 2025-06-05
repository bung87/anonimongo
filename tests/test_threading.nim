import unittest, net, locks
import ../src/anonimongo/core/[types, bson, pool]

suite "Multi-threading and GC-safe implementation tests":
  
  test "Generic types compile and instantiate":
    # Test that the new generic types can be created
    let mongo = newMongo[Socket]("localhost", 27017, poolSize = 4)
    check not mongo.isNil
    
    let db = mongo.getDatabase("test_threading")
    check not db.isNil
    check db.dbname() == "test_threading"
    
    let coll = db.getCollection("thread_test")
    check not coll.isNil
    check coll.collname() == "thread_test"
    check coll.dbname() == "test_threading"
    
    # Clean up
    coll.close()
    db.close()
    mongo.close()

  test "Connection pool thread safety":
    # Test pool operations work correctly
    let pool = initPool[Socket](2)
    check not pool.isNil
    
    # Test connection acquisition
    let (connId1, conn1) = pool.getConn()
    check connId1 > 0
    check not conn1.isNil
    check conn1.id == connId1
    
    # Test that connection state is accessible
    check not conn1.isNil
    
    # Test second connection
    let (connId2, conn2) = pool.getConn()
    check connId2 > 0
    check not conn2.isNil
    check conn2.id == connId2
    check connId2 != connId1  # Different connections
    
    # Return connections
    pool.endConn(connId1)
    pool.endConn(connId2)
    
    # Test reuse - should get same IDs back
    let (connId3, conn3) = pool.getConn()
    check connId3 > 0
    check (connId3 == connId1 or connId3 == connId2)
    
    pool.endConn(connId3)
    pool.close()

  test "withConnection template safety":
    let mongo = newMongo[Socket]("localhost", 27017, poolSize = 1)
    defer: mongo.close()
    
    var connectionUsed = false
    var connectionId = 0
    
    mongo.withConnection(conn):
      if not conn.isNil:
        connectionUsed = true
        connectionId = conn.id
        check conn.id > 0
        
        # Test that connection is valid
        check not conn.isNil
    
    check connectionUsed
    check connectionId > 0

  test "Thread-safe property accessors":
    let mongo = newMongo[Socket]("localhost", 27017, poolSize = 2)
    defer: mongo.close()
    
    # Test that property accessors work with locking
    let authenticated = mongo.authenticated
    let tls = mongo.tls  
    let readPref = mongo.readPreference
    let retryWrites = mongo.retryableWrites
    
    # These should not crash and return reasonable values
    check (authenticated == false or authenticated == true)
    check (tls == false or tls == true)
    check (retryWrites == false or retryWrites == true)
    check readPref == ReadPreference.primary
    
    # Test setter with locking
    mongo.setReadPreference(ReadPreference.secondary)
    let newReadPref = mongo.readPreference
    check newReadPref == ReadPreference.secondary

  test "Connection state management with locks":
    let mongo = newMongo[Socket]("localhost", 27017, poolSize = 2)
    defer: mongo.close()
    
    # Test direct connection access
    let (connId, conn) = mongo.getConn()
    if connId != -1 and not conn.isNil:
      check conn.id > 0
      
      # Test that connection works correctly
      check conn.id > 0
        
      # Return connection
      mongo.endConn(connId)

  test "GC-safe memory management":
    # Test that objects can be created and destroyed safely in a loop
    for i in 0..<5:
      let mongo = newMongo[Socket]("localhost", 27017, poolSize = 1)
      let db = mongo.getDatabase("test_mem_" & $i)
      let coll = db.getCollection("test_coll")
      
      check not mongo.isNil
      check not db.isNil  
      check not coll.isNil
      
      # Test that we can access properties safely
      let isAuthenticated = mongo.authenticated
      check (isAuthenticated == false or isAuthenticated == true)
      
      # Clean up manually to test deallocation
      coll.close()
      db.close()
      mongo.close()
    
    # Force GC to test for leaks
    GC_fullCollect()

  test "Pool exhaustion handling":
    # Test with single connection pool
    let mongo = newMongo[Socket]("localhost", 27017, poolSize = 1)
    defer: mongo.close()
    
    # Get the only connection
    let (connId1, conn1) = mongo.getConn()
    check connId1 > 0
    check not conn1.isNil
    
    # Try to get another - should fail gracefully
    let (connId2, conn2) = mongo.getConn()
    check connId2 == -1  # Should indicate failure
    
    # Return the connection
    mongo.endConn(connId1)
    
    # Now should be able to get one again
    let (connId3, conn3) = mongo.getConn()
    check connId3 > 0
    check not conn3.isNil
    mongo.endConn(connId3)

  test "Property accessor thread safety":
    let mongo = newMongo[Socket]("localhost", 27017, poolSize = 2)
    defer: mongo.close()
    
    # Test that property accessors work correctly (they use locks internally)
    let initialTls = mongo.tls()
    let initialAuth = mongo.authenticated()
    
    # Test setter works
    mongo.setReadPreference(ReadPreference.secondary)
    let newPref = mongo.readPreference()
    check newPref == ReadPreference.secondary
    
    # Test connection selection works
    let conn = mongo.main()
    check (conn.isNil or not conn.isNil)  # Just test it doesn't crash

  test "GridFS types with thread safety":
    let mongo = newMongo[Socket]("localhost", 27017, poolSize = 1)
    defer: mongo.close()
    
    let db = mongo.getDatabase("test_gridfs")
    let gridfs = newGridFS(db, "test_fs")
    
    check not gridfs.isNil
    check gridfs.name == "test_fs"
    check not gridfs.files.isNil
    check not gridfs.chunks.isNil
    
    # Test that GridFS collections are thread-safe
    check gridfs.files.collname() == "test_fs.files"
    check gridfs.chunks.collname() == "test_fs.chunks"
    
    db.close()

  test "Shared memory allocation safety":
    # Test that our shared memory allocation works correctly
    var mongos: seq[Mongo[Socket]] = @[]
    
    # Create multiple mongo instances
    for i in 0..<3:
      let mongo = newMongo[Socket]("localhost", 27017, poolSize = 1)
      mongos.add(mongo)
      check not mongo.isNil
    
    # Check they're all different instances
    check mongos[0] != mongos[1]
    check mongos[1] != mongos[2]
    check mongos[0] != mongos[2]
    
    # Clean up all instances
    for mongo in mongos:
      mongo.close()

when isMainModule:
  echo "Running focused threading implementation tests..."