import unittest, osproc, os, strformat, net

import utils_test
import anonimongo

var mongorun: Process
var mongoAlreadyRunning = false
if runlocal:
  # Check if MongoDB is already running on the port
  let sock = newSocket()
  defer: sock.close()
  try:
    sock.connect("localhost", Port(port))
    echo "MongoDB is already running on port ", port
    mongoAlreadyRunning = true
  except:
    echo "Starting MongoDB process..."
    mongorun = startmongo()
    sleep 3000 # waiting for mongod to be ready
    # Additional check to ensure mongod is actually running
    if not mongorun.running:
      echo "MongoDB process failed to start"
      echo "Process exit code: "
    echo "MongoDB process started successfully"

suite "Client connection and user management tests":
  test "Required mongo is running":
    if runlocal:
      if mongoAlreadyRunning:
        check true
      else:
        require mongorun.running
    else:
      check true

  var mongo: Mongo
  var db: Database
  var wr: WriteResult

  let existingDb = "temptest"
  let existingUser = bson {
    user: user, db: "admin"
  }
  let newuser = "temptest-user"

  test "Connected mongo and authenticated":
    mongo = testsetup()
    if mongo.withAuth:
      require mongo.authenticated

  test "Look users info":
    require mongo != nil
    db = mongo[existingDb]
    # test looking for not existing user
    var reply = db.usersInfo("not-exists0user")
    check reply.ok
    check reply["users"].len == 0

    reply = db.usersInfo(existingUser)
    let users = reply["users"]
    when defined(existingMongoSetup):
      check users.len == 1
      let theuser = users[0]
      check theuser["user"] == user
    else:
      check users.len == 0

  test &"Create new user: {newuser}":
    wr = db.createUser(newuser, newuser,
      roles = bsonArray("read"), customData = bson({ role: "testing"}))
    wr.success.reasonedCheck("createUser error", wr.reason)
    
    var reply = db.usersInfo(newuser)
    check reply.ok
    let users = reply["users"]
    check users.len == 1
    let newdoc = users[0].ofEmbedded
    check newdoc["user"] == newuser
    check newdoc["customData"]["role"] == "testing"

  test &"Look for all users in {existingDb}":
    let reply = db.usersInfo(1)
    check reply.ok
    check reply["users"].len == 1

  test &"No added {newuser} to admin database":
    let reply = db.usersInfo(bson {
      user: newuser, db: "admin",
    })
    check reply.ok
    check reply["users"].len == 0

  test &"Check info of the connected user admin":
    let reply = db.usersInfo(bson {
      user: "admin", db: "admin"
    })
    check reply.ok
    let users = reply["users"]

    # This is needed to avoid the tripping of number users in admin when
    # running github action.
    # Since the os used, Ubuntu, has pristine mongo installation so
    # there's no user created for it hence it's always returning zero
    # when checking the users in admin databaes.
    when defined(existingMongoSetup):
      check users.len == 1
    else:
      check users.len == 0

  test &"Grant roles to {newuser}":
    wr = db.grantRolesToUser(newuser,
      roles = bsonArray("readWrite"))
    wr.success.reasonedCheck("grantRolesToUser error", wr.reason)

  proc checkUserRoles(checkRoles: seq[string]) {.used.} =
    var reply = db.usersInfo(newuser)
    check reply.ok
    let users = reply["users"]
    check users.len == 1
    for udoc in users.ofArray:
      let roles = udoc["roles"].ofArray
      check roles.len == checkRoles.len
      for role in roles.ofArray:
        check role["role"] in checkRoles

  test &"Check newly granted roles to {newuser}":
    checkUserRoles @["read", "readWrite"]

  test &"Revoke roles to {newuser}":
    wr = db.revokeRolesFromUser(newuser,
      roles = bsonArray("read", "readWrite"))
    wr.success.reasonedCheck("revokeRolesFromUser error", wr.reason)

  test &"Check newly revoked roles to {newuser}":
    checkUserRoles @[]

  test &"Update {newuser}":
    wr = db.updateUser(newuser, newuser,
      roles = bsonArray("read"))
    wr.success.reasonedCheck("updateUser error", wr.reason)

  test &"Check newly updated roles to {newuser}":
    checkUserRoles @["read"]

  test &"Delete/drop the {newuser}":
    wr = db.dropUser(newuser)
    wr.success.reasonedCheck("dropUser error", wr.reason)

  test "Shutdown mongo":
    if runlocal:
      require mongo != nil
      wr = db.shutdown(timeout = 10)
      check wr.success
    else:
      skip()

  if runlocal and not mongoAlreadyRunning:
    if mongorun.running: 
      kill mongorun
    close mongorun
  close mongo