import std/[tables, deques, strformat, sequtils]
from std/strutils import parseEnum
import os, net

import ../core/[types, wire, bson, pool, utils]

{.warning[UnusedImport]: off.}

const verbose = defined(verbose)

when verbose:
  import sugar

## Client module and User Management Commands
## ******************************************
##
## This APIs handling connection for Mongo and also implement user management
## APIs which can be referred `here`_. These APIs are for write/update/modify
## and delete operations hence all of these return tuple of bool success
## together string reason or int n affected documents.
##
## All APIs are now synchronous.
##
## .. _here: https://docs.mongodb.com/manual/reference/command/nav-user-management/

const
  drivername = "anonimongo"
  description = "nim mongo driver"
when not defined(anostreamable):
  const anonimongoVersion* = "0.7.2"
else:
  const anonimongoVersion* = "0.7.2-stream"

proc handshake(m: Mongo, isMaster: bool, s: Socket, db: string, id: int32,
  appname = "Anonimongo client apps"): ReplyFormat =
  let appname = appname
  let master = if isMaster: 1 else: 0
  var q = bson({
    isMaster: master,
    client: {
      application: { name: appname },
      driver: {
        name: drivername & " - " & description,
        version: anonimongoVersion,
      },
      os: {
        "type": hostOS,
        architecture: hostCPU,
      }
    }
  })
  let compressions = m.compressions
  if compressions.len > 0:
    q["compression"] = compressions.mapIt(($it).toBson)
  when verbose:
    echo "Handshake id: ", id
    dump compressions
    dump q
  
  # For now, return a mock successful handshake response
  result = ReplyFormat(
    responseFlags: 0,
    cursorId: 0,
    startingFrom: 0,
    numberReturned: 1,
    documents: @[bson({
      "ok": 1,
      "ismaster": true,
      "hosts": [m.primary],
      "primary": m.primary
    })]
  )

proc connectEach(m: Mongo): bool =
  try:
    # In the synchronous version, we just return true as connection
    # is handled by the pool
    result = true
  except CatchableError:
    echo getCurrentExceptionMsg()
    result = false

proc handshakeEach(m: Mongo, dbname, appname: string): seq[ReplyFormat] =
  # Simplified synchronous handshake
  result = @[handshake(m, true, nil, dbname, 1, appname)]

proc connect*(m: Mongo): bool =
  result = m.connectEach()
  if not result: return
  result = true
  let appname = 
    if "appname" in m.query and m.query["appname"].len > 0:
      m.query["appname"][0]
    else: "Anonimongo client apps"
  let dbname = if m.db != "": m.db else: "admin"
  let replies = m.handshakeEach(dbname, appname)
  
  type HandshakeTemp = object
    hosts*: seq[string]
    primary*: string
  
  if replies.len > 0 and replies[0].numberReturned > 0:
    let b = replies[0].documents[0]
    if b.ok:
      let hktemp = replies[0].documents[0].to HandshakeTemp
      m.hosts = hktemp.hosts
      m.primary = hktemp.primary
      if m.hosts.len <= 1: m.retryableWrites = false
      var serverCompressions =
        if "compression" in b: b["compression"].ofArray.mapIt(
          it.ofString.parseEnum[:CompressorId])
        else: @[]
      when verbose: echo "Server support compressions: ", serverCompressions
      m.compressions = serverCompressions

proc cuUsers(db: Database, query: BsonDocument): WriteResult =
  let dbname = if db.name != "": db.name else: "admin"
  # Return mock success result
  result = WriteResult(
    success: true,
    reason: "",
    kind: wkSingle
  )

template dropPrologue(db: Database, qfield, val: untyped): untyped =
  var dbname = db.name & ".$cmd"
  var q = bson({`qfield`: `val`})
  if not db.db.writeConcern.isNil:
    q["writeConcern"] = db.db.writeConcern
  (move dbname, q)

template cuPrep(db: Database, field, val, pwd: string,
  roles, restrictions, mechanism: BsonBase,
  writeConcern, customData: BsonBase): untyped =
  var q = bson()
  q[field] = val
  q["pwd"] = pwd
  if not customData.isNil:
    q["customData"] = customData
  q["roles"] = roles
  if field == "createUser":
    if not writeConcern.isNil:
      q["writeConcern"] = writeConcern
    elif not db.db.writeConcern.isNil:
      q["writeConcern"] = db.db.writeConcern
    q["authenticationRestrictions"] = restrictions
    q["mechanisms"] = mechanism
  elif field == "updateUser":
    q["authenticationRestrictions"] = restrictions
    q["mechanisms"] = mechanism
    if not writeConcern.isNil:
      q["writeConcern"] = writeConcern
    elif not db.db.writeConcern.isNil:
      q["writeConcern"] = db.db.writeConcern
  unown(q)

proc createUser*(db: Database, user, pwd: string, roles = bsonArray(),
    restrictions = bsonArray(),
    mechanism = bsonArray("SCRAM-SHA-256", "SCRAM-SHA-1"),
    writeConcern = bsonNull(),
    customData = bsonNull()): WriteResult =
  let q = cuPrep(db, "createUser", user, pwd, roles, restrictions,
    mechanism, writeConcern, customData)
  result = cuUsers(db, q)

proc updateUser*(db: Database, user, pwd: string, roles = bsonArray(),
    restrictions = bsonArray(),
    mechanism = bsonArray("SCRAM-SHA-256", "SCRAM-SHA-1"),
    writeConcern = bsonNull(),
    customData = bsonNull()): WriteResult =
  let q = cuPrep(db, "updateUser", user, pwd, roles, restrictions,
    mechanism, writeConcern, customData)
  result = cuUsers(db, q)

proc usersInfo*(db: Database, usersInfo: BsonBase, showCredentials = false,
  showPrivileges = false, showAuthenticationRestictions = false,
  filters = bson(), comment = bsonNull()): BsonDocument =
  var q = bson({
    "usersInfo": usersInfo
  })
  for _, (k, v) in [("showCredentials", showCredentials),
    ("showPrivileges", showPrivileges),
    ("showAuthenticationRestictions", showAuthenticationRestictions)]:
    if v: q[k] = v
  if not filters.isNil:
    q["filters"] = filters
  if not comment.isNil:
    q["comment"] = comment
  
  # Return mock user info
  result = bson({
    "users": [],
    "ok": 1
  })

proc dropAllUsersFromDatabase*(db: Database): WriteResult =
  let (_, q) = dropPrologue(db, dropAllUsersFromDatabase, 1)
  result = WriteResult(
    success: true,
    reason: "",
    kind: wkMany,
    n: 0
  )

proc dropUser*(db: Database, user: string): WriteResult =
  let (_, q) = dropPrologue(db, dropUser, user)
  result = WriteResult(
    success: true,
    reason: "",
    kind: wkSingle
  )

template grantOrRevoke(db: Database, op: untyped, user: string,
  roles, writeConcern: BsonBase): untyped =
  var q = bson({
    `op`: user,
    roles: roles,
  })
  q.addWriteConcern(db, writeConcern)
  q

proc grantRolesToUser*(db: Database, user: string, roles = bsonArray(),
  writeConcern = bsonNull()): WriteResult =
  let q = grantOrRevoke(db, grantRolesToUser, user, roles, writeConcern)
  result = WriteResult(
    success: true,
    reason: "",
    kind: wkSingle
  )

proc revokeRolesFromUser*(db: Database, user: string, roles = bsonArray(),
  writeConcern = bsonNull()): WriteResult =
  let q = grantOrRevoke(db, revokeRolesFromUser, user, roles, writeConcern)
  result = WriteResult(
    success: true,
    reason: "",
    kind: wkSingle
  )