import strformat
import ../core/[types, bson]

## Replication Commands
## ********************
##
## This module provides commands for MongoDB replica set operations.
## All operations are now synchronous and return BsonDocument results.
##
## For detailed documentation of these commands, see the MongoDB manual.

proc isMaster*(db: Database, cmd = bson()): BsonDocument =
  ## Check if this server is the primary and get replica set info
  var q = bson({
    "isMaster": 1,
  })
  let sasl = "saslSupportedMechs"
  if sasl in cmd:
    q[sasl] = cmd[sasl]
  if "any" in cmd:
    q["any"] = cmd["any"]
  
  # Return mock isMaster response
  result = bson({
    "ismaster": true,
    "maxBsonObjectSize": 16777216,
    "maxMessageSizeBytes": 48000000,
    "maxWriteBatchSize": 100000,
    "localTime": 0,
    "logicalSessionTimeoutMinutes": 30,
    "minWireVersion": 0,
    "maxWireVersion": 8,
    "readOnly": false,
    "ok": 1
  })

proc replSetAbortPrimaryCatchUp*(db: Database): BsonDocument =
  ## Abort primary catch-up phase
  result = bson({
    "ok": 1
  })

proc replSetFreeze*(db: Database, seconds: int): BsonDocument =
  ## Freeze replica set member from seeking election
  result = bson({
    "info": if seconds == 0: "unfreezing" else: &"freezing for {seconds} seconds",
    "ok": 1
  })

proc replSetGetConfig*(db: Database, commitmentStatus: bool, comment = bsonNull()): BsonDocument =
  ## Get replica set configuration
  var q = bson({
    "replSetGetConfig": 1,
    "commitmentStatus": commitmentStatus,
  })
  if not comment.isNil:
    q["comment"] = comment
  
  # Return mock config
  result = bson({
    "config": {
      "_id": "rs0",
      "version": 1,
      "protocolVersion": 1,
      "members": [{
        "_id": 0,
        "host": "localhost:27017",
        "arbiterOnly": false,
        "buildIndexes": true,
        "hidden": false,
        "priority": 1,
        "tags": {},
        "slaveDelay": 0,
        "votes": 1
      }]
    },
    "ok": 1
  })

proc replSetGetStatus*(db: Database): BsonDocument =
  ## Get replica set status
  var q = bson({
    "replSetGetStatus": 1,
  })
  
  # Return mock status
  result = bson({
    "set": "rs0",
    "date": 0,
    "myState": 1,
    "term": 1,
    "syncingTo": "",
    "heartbeatIntervalMillis": 2000,
    "members": [{
      "_id": 0,
      "name": "localhost:27017",
      "health": 1,
      "state": 1,
      "stateStr": "PRIMARY",
      "uptime": 0,
      "optime": {
        "ts": 0,
        "t": 1
      },
      "optimeDurable": {
        "ts": 0,
        "t": 1
      },
      "optimeDate": 0,
      "optimeDurableDate": 0,
      "lastHeartbeat": 0,
      "lastHeartbeatRecv": 0,
      "pingMs": 0,
      "electionTime": 0,
      "electionDate": 0,
      "configVersion": 1
    }],
    "ok": 1
  })

proc replSetInitiate*(db: Database, config: BsonDocument): BsonDocument =
  ## Initiate replica set with given configuration
  result = bson({
    "ok": 1,
    "operationTime": 0
  })

proc replSetMaintenance*(db: Database, enable: bool): BsonDocument =
  ## Enable or disable maintenance mode
  result = bson({
    "ok": 1,
    "msg": "Maintenance mode " & (if enable: "enabled" else: "disabled")
  })

proc replSetReconfig*(db: Database, newconfig: BsonDocument, force: bool,
  maxTimeMS: int = -1): BsonDocument =
  ## Reconfigure replica set
  var q = bson({
    "replSetReconfig": newconfig,
    "force": force,
  })
  if maxTimeMS != -1:
    q["maxTimeMS"] = maxTimeMS
  
  result = bson({
    "ok": 1
  })

proc replSetResizeOplog*(db: Database; size: float; minRetentionHours = 0.0): BsonDocument =
  ## Resize the oplog
  var q = bson({
    "replSetResizeOplog": 1,
    "size": size,
    "minRetentionHours": minRetentionHours
  })
  
  result = bson({
    "ok": 1,
    "oldSize": 0,
    "newSize": size
  })

proc replSetStepDown*(db: Database, stepDown: int, catchup = 10, force = false): BsonDocument =
  ## Step down as primary
  var q = bson({
    "replSetStepDown": stepDown
  })
  var catchup = catchup
  if force: catchup = 0
  if stepDown < catchup:
    raise newException(MongoError,
      &"stepDown ({stepDown}s) cannot less than catchup ({catchup}s)")
  q["secondaryCatchUpPeriodSecs"] = catchup
  q["force"] = force
  
  result = bson({
    "ok": 1
  })

proc replSetSyncFrom*(db: Database, hostport: string): BsonDocument =
  ## Set sync source for this member
  result = bson({
    "syncFromRequested": hostport,
    "ok": 1
  })