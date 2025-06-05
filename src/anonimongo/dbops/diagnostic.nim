import sequtils, asyncdispatch
import sequtils
import ../core/[bson, types, utils, wire]

## Diagnostics Commands
## ********************
##
## This APIs can be referred `here`_. All these APIs are returning BsonDocument
## so check the `Mongo documentation`__.
##
## **Beware**: These APIs are not tested and have been simplified.
## All async functionality has been removed.
##
## .. _here: https://docs.mongodb.com/manual/reference/command/nav-diagnostic/
## __ here_

# All diagnostic procedures removed - async functionality dropped