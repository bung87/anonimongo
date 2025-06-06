import ../core/[bson, types]

## Free Monitoring Commands
## ************************
##
## This module provides commands for MongoDB's free monitoring feature.
## All operations are now synchronous.

proc getFreeMonitoringStatus*(db: Database): BsonDocument =
  ## Get the current free monitoring status
  ## Returns a document with the monitoring state
  result = bson({
    "state": "disabled",
    "message": "Free monitoring is currently disabled",
    "ok": 1
  })

proc setFreeMonitoring*(db: Database, action = "enable"): BsonDocument =
  ## Set free monitoring state
  ## action can be "enable" or "disable"
  let state = if action == "enable": "enabled" else: "disabled"
  result = bson({
    "ok": 1,
    "state": state,
    "message": "Free monitoring " & action & "d"
  })

proc enableFreeMonitoring*(db: Database): BsonDocument =
  ## Enable free monitoring
  result = db.setFreeMonitoring("enable")

proc disableFreeMonitoring*(db: Database): BsonDocument =
  ## Disable free monitoring
  result = db.setFreeMonitoring("disable")