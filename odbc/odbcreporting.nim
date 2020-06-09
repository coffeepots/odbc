type
  ODBCReportLevel* = enum rlErrorsAndInfo, rlErrors, rlNone
  ODBCReportDestination* = enum rdEcho, rdStore, rdFile, rdCallBack, rdIgnore
  ODBCReportState* = ref object
    level*: ODBCReportLevel
    destinations*: set[ODBCReportDestination]
    filename*: string
    messages*: seq[string]
    callBack*: proc(msg: string) {.gcsafe.}
    displayHandles*: bool

proc newODBCReportState*: ODBCReportState =
  new(result)
  result.destinations = {rdEcho}
  result.messages = @[]

proc odbcLog*(rptState: var ODBCReportState, logStr: string) =
  for dest in rptState.destinations:
    case dest
    of rdStore:
      rptState.messages.add(logStr)
    of rdEcho:
      echo logStr
    of rdFile:
      var f: File
      if open(f, rptState.filename, fmAppend):
        try:
          write(f, logStr)
        except:
          rptState.messages.add("Exception saving log to file \"" & rptState.filename & "\": " & getCurrentExceptionMsg())
          if not (rdStore in rptState.destinations):
            # only add this message if it is not already being stored
            rptState.messages.add(logStr)
        finally:
          f.close
      else:
        # default to storing message
        rptState.messages.add("Could not open file \"" & rptState.filename & "\" to save log")
        if not (rdStore in rptState.destinations):
          # only add this message if it is not already being stored
          rptState.messages.add(logStr)
    of rdCallBack:
      if rptState.callBack != nil: rptState.callBack(logStr)
    of rdIgnore:
      discard
