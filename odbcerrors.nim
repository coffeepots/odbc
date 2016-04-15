import odbcsql, odbcreporting

type
  ODBCException* = ref object of Exception
  ODBCUnknownParameterException* = ref object of ODBCException
  ODBCUnsupportedTypeException* = ref object of ODBCException
  ODBCOverflowException* = ref object of ODBCException

const
  msgUnsupportedType = "Unsupported type when setting value: "

proc newODBCException*(msg: string): ODBCException =
  new(result)
  result.msg = msg

proc newODBCUnknownParameterException*(msg: string): ODBCUnknownParameterException =
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
    sqlState = newStringOfCap(6)
    msg = newStringOfCap(SQL_MAX_MESSAGE_LENGTH)
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
  elif handleType == SQL_HANDLE_ENV: result &= " (env)"
  elif handleType == SQL_HANDLE_ENV: result &= " (env)"
  else: result &= " (" & $handleType.int & ")"

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
      msgDetails = "<no handle/nil supplied>"

    let rptStr = "ODBC call " & callname & " returned non-success: " & msgDetails
    when defined(odbcexceptonfail):
      raise newODBCException(rptStr)
    else:
      if rptState != nil:
        rptState.odbcLog(rptStr)
      else:
        # defaults to echoing errors only.
        echo rptStr
