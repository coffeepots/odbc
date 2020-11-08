import odbcsql, odbcreporting

type
  ODBCException* = ref object of ValueError
  ODBCUnknownParameterException* = ref object of ODBCException
  ODBCUnsupportedTypeException* = ref object of ODBCException
  ODBCOverflowException* = ref object of ODBCException
  ODBCFieldException* = ref object of ODBCException

const
  msgUnsupportedType = "Unsupported type when setting value: "

proc newODBCException*(msg: string): ODBCException =
  new(result)
  result.msg = msg

proc newODBCUnknownParameterException*(msg: string): ODBCUnknownParameterException =
  new(result)
  result.msg = msg

proc newODBCUnknownFieldException*(msg: string): ODBCFieldException =
  new(result)
  result.msg = msg

proc newODBCUnsupportedTypeException*(msg: string): ODBCUnsupportedTypeException =
  new(result)
  result.msg = msgUnsupportedType & msg

proc newODBCOverflowException*(msg: string): ODBCOverflowException =
  new(result)
  result.msg = msgUnsupportedType & msg

proc sqlRespToStr(sqlErr: TSqlSmallInt): string =
  case sqlErr:
    of SQL_SUCCESS:
      result = "success (" & $sqlErr & ")"
    of SQL_SUCCESS_WITH_INFO:
      result = "success with info (" & $sqlErr & ")"
    of SQL_NO_DATA:
      result = "no data (" & $sqlErr & ")"
    of SQL_ERROR:
      result = "error (" & $sqlErr & ")"
    of SQL_INVALID_HANDLE:
      result = "invalid handle (" & $sqlErr & ")"
    else:
      result = "unknown (" & $sqlErr & ")"

proc reportOn(reportLevel: ODBCReportLevel, resp: TSqlSmallInt): bool =
  case reportLevel
  of rlNone: return
  of rlErrors:
    if resp == SQL_ERROR: return true
  of rlErrorsAndInfo:
    if resp != SQL_SUCCESS: return true

proc getDiagMsg*(handle: SqlHandle, handleType: TSqlSmallInt): string =
  var
    nativeError: TSqlInteger
    retval: TSqlSmallInt
    recNo = 1.TSqlSmallInt
    sqlState = newString(6)
    msg = newString(SQL_MAX_MESSAGE_LENGTH)
    bufferLength: TSqlSmallInt = SQL_MAX_MESSAGE_LENGTH
    textLength: TSqlSmallInt

  result = ""
  retval = SQLGetDiagRec(handleType.TSqlSmallInt, handle, recNo, sqlState, nativeError, msg, bufferLength, textLength)
  while retval != SQL_NO_DATA:
    recNo += 1

    msg.setLen(textLength)
    sqlState.setLen(5)
    if sqlState[0]=='\0':
      sqlState = ""
    result &= "\nstate = '" & $sqlState & "' (err=" & $nativeError & ") message = '" & msg & "' "
    retval = SQLGetDiagRec(handleType.TSqlSmallInt, handle, recNo, sqlState, nativeError, msg, bufferLength, textLength)
    if retval<0:
      break

from strutils import toHex

proc toStr*(h: SqlHandle, handleType: TSqlSmallInt): string =
  result = toHex(cast[int](h), 8)
  if handleType == SQL_HANDLE_ENV: result &= " (env)"
  elif handleType == SQL_HANDLE_DBC: result &= " (dbc)"
  elif handleType == SQL_HANDLE_STMT: result &= " (stmt)"
  elif handleType == SQL_HANDLE_DESC: result &= " (desc)"  
  else: result &= " (" & $handleType.int & ")"

import os

proc debugPrefix: string =
  ## Report the first line number and proc in the call stack outside of the library source.
  let entries = getStackTraceEntries()
  const parentDirectory = currentSourcePath().parentDir
  when not defined(odbcDebug):
    const sourceDirs = [parentDirectory, parentDirectory.parentDir]

  # Find first stack line outside of the current source directory.
  var stackIdx: int
  when not defined(odbcDebug):
    for idx in countDown(entries.high, 0):
      let
        fn = $entries[idx].filename
        dir = fn.parentDir
      if dir notin sourceDirs:
        stackIdx = idx
        break
  else:
    # When debugging is enabled, the line number within the library is reported.
    # We still don't want to include this proc or rptOnErr so we try to move 2
    # levels up if possible.
    stackIdx = max(0, entries.high - 2)
  if stackIdx < entries.len:
    let
      st = entries[stackIdx]
      fnStr = $(st.filename)
      debugPrefix = "ODBC [" & fnStr & "(" & $st.line & ") " & " " & $st.procname & "] "
    debugPrefix
  else:
    ""

proc rptOnErr*(rptState: var ODBCReportState, resp: TSqlSmallInt, callname: string, handle: SqlHandle = nil, handleType = SQL_HANDLE_ENV.TSqlSmallInt) =
  var doRpt: bool
  if rptState != nil:
    doRpt = reportOn(rptState.level, resp)
  else:
    doRpt = resp == SQL_ERROR

  if doRpt:
    var msgDetails: string = ""
    if handle != nil:
      msgDetails = sqlRespToStr(resp)
      if rptState.displayHandles: msgDetails &= " Handle: " & toStr(handle, handleType)

      if resp == SQL_SUCCESS_WITH_INFO or resp == SQL_ERROR:
        msgDetails &= getDiagMsg(handle, handleType)
    else:
      msgDetails = "<No handle/nil supplied>"

    let rptStr = debugPrefix() & "ODBC call " & callname & " returned non-success: " & msgDetails
    when defined(odbcexceptonfail):
      raise newODBCException(rptStr)
    else:
      if rptState != nil:
        rptState.odbcLog(rptStr)
      else:
        # defaults to echoing errors only.
        echo rptStr
