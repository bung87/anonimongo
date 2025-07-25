import net
import wire, bson, types, pool

const verbose {.booldefine.} = false

when verbose:
  import sugar

func cmd*(name: string): string = name & ".$cmd"
  ## Add suffix ".$cmd" to Database name to avoid any typo.

func flags*(d: Database): int32 = 0 # Simplified for now
  ## Get Mongo available ``wire.QueryFlags`` as int32 bitfield

proc addWriteConcern*(q: var BsonDocument, db: Database, wt: BsonBase) =
  ## Helper that will modify add writeConcern to BsonDocument query based
  ## on writeConcer data given. If it's nil and Mongo.writeConcern also nil,
  ## bypass without adding this field in query.
  if not wt.isNil:
    q["writeConcern"] = wt

template addOptional*(q: var BsonDocument, name: string, f: BsonBase) =
  ## Add any optional field to query if it's not nil.
  if not f.isNil:
    q[name] = f

template addConditional*(q: var BsonDocument, field: string, val: BsonBase) =
  ## Add any optional boolean field to query if it's true.
  if val:
    q[field] = val

proc epilogueCheck*(reply: ReplyFormat, target: var string): bool =
  ## Helper utility to check and modify string reason if the operation
  ## wasn't success.
  let (success, reason) = check reply
  if not success:
    target = reason
    return false
  let stat = reply.documents[0]
  if not stat.ok:
    target = stat.errMsg
    return false
  true

# proceed removed - async functionality dropped

# crudops removed - async functionality dropped

proc getWResult*(b: BsonDocument): WriteResult =
  ## Helper to fetch a WriteResult of kind wkMany.
  result = WriteResult(
    success: b.ok,
    kind: wkMany
  )
  if "nModified" in b:
    result.n = b["nModified"]
  elif "n" in b:
    result.n = b["n"]
  if "writeErrors" in b:
    result.success = false
    let errdocs = b["writeErrors"].ofArray
    result.errmsgs = newseq[string](errdocs.len)
    for i, errb in errdocs:
      result.errmsgs[i] = errb.ofEmbedded.errmsg
      when verbose:
        dump result.errmsgs[i]
  when verbose:
    dump result

proc getWSingleResult*(b: BsonDocument): WriteResult =
  ## Helper to fetch a WriteResult of kind wkSingle.
  result = WriteResult(
    success: b.ok,
    kind: wkSingle
  )
  if "writeErrors" in b:
    result.success = false
    let errdocs = b["writeErrors"].ofArray
    if errdocs.len > 0:
      result.reason = errdocs[0].ofEmbedded.errmsg
      when verbose:
        dump result.reason
  when verbose:
    dump result