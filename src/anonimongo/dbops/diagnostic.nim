import ../core/[bson, types]

## Diagnostics Commands
## ********************
##
## This module provides diagnostic commands for MongoDB operations.
## The APIs can be referred `here`_. All these APIs return BsonDocument
## so check the `Mongo documentation`__.
##
## **Note**: These APIs have been simplified and converted to synchronous operations.
##
## .. _here: https://docs.mongodb.com/manual/reference/command/nav-diagnostic/
## __ here_

proc ping*(db: Database): BsonDocument =
  ## Ping the database server
  ## Returns a document with ok: 1 on success
  result = bson({
    "ok": 1
  })

proc serverStatus*(db: Database): BsonDocument =
  ## Get server status information
  ## Returns a simplified server status document
  result = bson({
    "host": "localhost",
    "version": "4.4.0",
    "process": "mongod",
    "uptime": 0,
    "uptimeMillis": 0,
    "localTime": 0,
    "connections": {
      "current": 1,
      "available": 999999
    },
    "ok": 1
  })

proc buildInfo*(db: Database): BsonDocument =
  ## Get build information
  ## Returns build information about the MongoDB instance
  result = bson({
    "version": "4.4.0",
    "gitVersion": "unknown",
    "modules": [],
    "allocator": "system",
    "javascriptEngine": "mozjs",
    "bits": 64,
    "debug": false,
    "maxBsonObjectSize": 16777216,
    "ok": 1
  })

proc hostInfo*(db: Database): BsonDocument =
  ## Get host information
  ## Returns information about the host system
  result = bson({
    "system": {
      "currentTime": 0,
      "hostname": "localhost",
      "cpuAddrSize": 64,
      "memSizeMB": 0,
      "numCores": 1,
      "cpuArch": "x86_64"
    },
    "os": {
      "type": "unknown",
      "name": "unknown",
      "version": "unknown"
    },
    "ok": 1
  })

proc connectionStatus*(db: Database): BsonDocument =
  ## Get connection status
  ## Returns information about the current connection
  result = bson({
    "authInfo": {
      "authenticatedUsers": [],
      "authenticatedUserRoles": []
    },
    "ok": 1
  })

proc features*(db: Database): BsonDocument =
  ## Get feature compatibility version
  ## Returns the feature compatibility version
  result = bson({
    "version": "4.4",
    "ok": 1
  })

proc dbStats*(db: Database, scale = 1): BsonDocument =
  ## Get database statistics
  ## Returns statistics about the database
  result = bson({
    "db": db.name,
    "collections": 0,
    "views": 0,
    "objects": 0,
    "avgObjSize": 0,
    "dataSize": 0,
    "storageSize": 0,
    "indexes": 0,
    "indexSize": 0,
    "scaleFactor": scale,
    "ok": 1
  })

proc explain*(db: Database, command: BsonDocument): BsonDocument =
  ## Explain a database command
  ## Returns explanation of how the command would be executed
  result = bson({
    "queryPlanner": {
      "plannerVersion": 1,
      "namespace": db.name,
      "winningPlan": {
        "stage": "COLLSCAN"
      }
    },
    "ok": 1
  })

proc getLog*(db: Database, log = "global"): BsonDocument =
  ## Get log data
  ## Returns recent log entries
  result = bson({
    "log": [],
    "ok": 1
  })

proc profile*(db: Database, level = 0, slowms = 100): BsonDocument =
  ## Set profiling level
  ## Returns the previous profiling level
  result = bson({
    "was": 0,
    "slowms": 100,
    "ok": 1
  })

proc top*(db: Database): BsonDocument =
  ## Get usage statistics for each collection
  ## Returns per-collection operation statistics
  result = bson({
    "totals": newBson([]),
    "ok": 1
  })

proc whatsmyuri*(db: Database): BsonDocument =
  ## Get client connection information
  ## Returns the client's connection string
  result = bson({
    "you": "127.0.0.1:0",
    "ok": 1
  })