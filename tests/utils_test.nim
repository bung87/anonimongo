import asyncdispatch, net, tables, uri
import osproc, sugar, unittest
import strformat

import ../src/anonimongo

{.warning[UnusedImport]: off.}

const
  pem* {.strdefine.} = "d:/dev/self-signed-cert/srv.key.pem"
  exe* {.strdefine.} =
    when defined windows: "d:/installer/mongodb/bin/mongod"
    else: "mongod"
  key* {.strdefine.} = "d:/dev/self-signed-cert/key.pem"
  cert* {.strdefine.} = "d:/dev/self-signed-cert/cert.pem"
  dbpath* {.strdefine.} = "d:/dev/mongodata"
  filename* {.strdefine.} = ""
  saveas* {.strdefine.} = ""
  user* {.strdefine.} = "rdruffy"
  pass* {.strdefine.} = "rdruffy"
  host* {.strdefine.} = "localhost"
  port* {.intdefine.} = 27017
  poolconn* {.intdefine.} = 2
  localhost* = host == "localhost"
  nomongod* = not defined(nomongod)
  runlocal* = localhost and nomongod
  anoSocketSync* = defined(anoSocketSync)

  mongourl {.strdefine, used.} = &"mongo://{user}:{pass}@{host}:{port}/" &
    "?tlscertificateKeyfile=" &
    &"certificate:{encodeUrl(cert)},key:{encodeUrl(key)}&authSource=admin" &
    "&compressors=snappy,zlib"
  verbose* = defined(verbose)

when verbose:
  import times, strformat

when not anoSocketSync:
  type TheSock* = AsyncSocket
else:
  type TheSock* = Socket

proc startmongo*: Process =
  var args = @[
    "--port", $port,
    "--dbpath", dbpath,
    "--bind_ip_all",
    "--networkMessageCompressors", "snappy,zlib",
  ]
  when defined(ssl):
    args.add "--sslMode"
    args.add "requireSSL"
    args.add "--sslPEMKeyFile"
    args.add pem
    if cert != "":
      args.add "--sslCAFile"
      args.add cert
  if user != "" and pass != "":
    args.add "--auth"
  when defined(windows):
    # the process cannot continue unless the stdout flushed
    let opt = {poUsePath, poStdErrToStdOut, poInteractive, poParentStreams}
  else:
    let opt = {poUsePath, poStdErrToStdOut}
  result = unown startProcess(exe, args = args, options = opt)

proc withAuth*(m: Mongo): bool =
  (user != "" and pass != "") or m.hasUserAuth()

proc testsetup*: Mongo =
  when defined(ssl):
    let sslinfo {.used.} = SslInfo(keyfile: key, certfile: cert)
  else:
    let sslinfo {.used.} = SslInfo(keyfile: "dummykey", certfile: "dummycert")
  when not defined(uri):
    let mongo = newMongo(host = host, port = port, poolSize = poolconn, sslInfo = sslinfo)
  else:
    let mongo = newMongo(MongoUri mongourl, poolSize = poolconn)

  # Note: retryableWrites is now a property accessor, not a field
  # mongo.retryableWrites = true  # This is set by default in the constructor
  
  # Note: appname field doesn't exist in the new implementation
  # mongo.appname = "Test driver"  # Removed for now
  
  # Connection is handled in the constructor now
  # No need for explicit connect call
  
  when verbose:
    let start = cpuTime()
  
  # Authentication using the new API
  if mongo.withAuth and not mongo.authenticate(user, pass):
    echo "cannot authenticate the connection"
  
  when verbose:
    echo &"auth ended taking {cpuTime() - start} for poolconn {poolconn}"
  result = mongo

proc tell*(label, reason: string) =
  stdout.write label
  dump reason

template reasonedCheck*(b: BsonDocument | bool, label: string, reason = "") =
  when b is BsonDocument:
    check b.ok
    if not b.ok: (label & ": ").tell b.errmsg
  else:
    check b
    if not b: (label & ": ").tell reason