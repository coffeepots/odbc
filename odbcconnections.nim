import odbcsql, odbcerrors, strutils, odbchandles, odbcreporting, tables

type
  ODBCServerType* = enum SQLSever,ApacheDrill
  ODBCTransactionMode = enum tmAuto, tmManual

  ODBCConnection* = ref object
    envHandle: SqlHEnv
    conHandle: SqlHDBC
    conTimeout: int
    #
    driver*: string
    connectionString*: string
    host*: string
    database*: string
    userName*: string
    password*: string
    provider*: string
    integratedSecurity*: bool
    connected: bool
    serverType:ODBCServerType
    transMode: ODBCTransactionMode
    multipleActiveResultSets*: bool
    autoTranslate*: bool
    encrypted*: bool
    authenticationType*:string
    connectionType*:string
    zkClusterID*:string
    port*:int
    reporting*: ODBCReportState

var
  defaultTimeout* = 30
  defaultReportingLevel* = rlErrorsAndInfo
  quitProcRegistered = false
  activeConnections = initTable[SqlHDBC, ODBCConnection]()

proc disconnect*(con: ODBCConnection) =
  if con.conHandle != nil:
    if con.connected:
      disconnect(con.conHandle, con.reporting)
      con.connected = false
    freeConHandle(con.conHandle, con.reporting)
    activeConnections.withValue(con.conHandle, value):
      activeConnections.del(value.conHandle)
    when defined(odbcdebug): echo "Disconnected handle ", repr(con.conHandle)
    con.conHandle = nil

proc close*(con: ODBCConnection) = con.disconnect

proc freeConnection*(con: ODBCConnection) =
  con.disconnect
  if con.envHandle != nil:
    freeEnvHandle(con.envHandle, con.reporting)

proc finaliseConnections {.noconv.} =
  # free up any connections
  when defined(odbcdebug): echo "Finalising connections..."
  for pair in activeConnections.pairs:
    when defined(odbcdebug): echo "freeing connection to: ", pair[1].host
    pair[1].freeConnection
  when defined(odbcdebug): echo "Finalising connections done"

proc newODBCConnection*(driver: string = "", host: string = "", database: string = "",server:ODBCServerType = SQLSever): ODBCConnection =
  new(result, freeConnection)
  result.connectionString = ""
  result.driver = driver
  result.host = host
  result.database = database
  result.userName = ""
  result.password = ""
  result.transMode = tmAuto
  result.conTimeout = defaultTimeout
  result.multipleActiveResultSets = true
  result.autoTranslate = true
  # Use SQL Server Native Client by default
  result.driver = "SQL Server Native Client 11.0"
  result.provider = ""
  result.serverType = server
  result.reporting = newODBCReportState()
  result.reporting.level = defaultReportingLevel
  result.reporting.destinations = {rdStore}
  if not quitProcRegistered:
    addQuitProc(finaliseConnections)
    quitProcRegistered = true

proc getConnectionString*(con: var ODBCConnection): string =
  var params: seq[string] = @[]

  if con.host != "":
    params.add("Server=" & con.host)

  if con.database != "":
    params.add("Database=" & con.database)

  if con.driver != "":
    params.add("Driver=" & con.driver)

  if con.provider != "":
    params.add("Provider=" & con.provider)

  if not con.integratedSecurity and con.username != "":
    params.add("Uid=" & con.username)
    # only add pw if un present
    if con.password != "":
      params.add("Pwd=" & con.password)
  else:
    # use integrated security if no username present
    params.add("Trusted_Connection=Yes")
  if con.multipleActiveResultSets:
    params.add("Mars_Connection=Yes")
  if con.encrypted:
    params.add("Encrypt=yes")
  if not con.authenticationType.isNil():
    params.add("AuthenticationType=" & con.authenticationType)
  if not con.connectionType.isNil():
    params.add("connectionType=" & con.connectionType)
  if con.port > 0:
    params.add("Port=" & $con.port)
  if not con.zkClusterID.isNil():
    params.add("ZKClusterID=" & con.zkClusterID)
  if con.autoTranslate:
    params.add("AutoTranslate=Yes")
  else:
    params.add("AutoTranslate=No")


  result = join(params, ";")

proc setConnectionAttr*(con: var ODBCConnection, connectParam, value: int): bool =
  ## Set an integer ODBC connection attribute.
  var
    valBuf = value
    ret = SQLSetConnectAttr(con.conHandle, connectParam.TSqlInteger, cast[pointer](valBuf), 0)
  if not ret.sqlSucceeded:
    result = false
    rptOnErr(con.reporting, ret, "SQLSetConnectAttr " & $connectParam & " = " & $value, con.conHandle, SQL_HANDLE_DBC.TSqlSmallInt)
  else: result = true

proc setConnectionAttr*(con: var ODBCConnection, connectParam: int, value: string): bool =
  var
    ret = SQLSetConnectAttr(con.conHandle, connectParam.TSqlInteger, value.cstring, value.len)
  if not ret.sqlSucceeded:
    result = false
    rptOnErr(con.reporting, ret, "SQLSetConnectAttr " & $connectParam & " = " & $value, con.conHandle, SQL_HANDLE_DBC.TSqlSmallInt)
  else: result = true

# read only access properties
proc connectionHandle*(con: ODBCConnection): SqlHandle = con.conHandle
proc environmentHandle*(con: ODBCConnection): SqlHEnv = con.envHandle
proc transactionMode*(con: ODBCConnection): ODBCTransactionMode = con.transactionMode
proc timeout*(con: var ODBCConnection): int = con.conTimeout
proc connected*(con: var ODBCConnection): bool = con.connected

proc `transactionMode=`*(con: var ODBCConnection, tmMode: ODBCTransactionMode) =
  # if connected, this will set the connection attribute, otherwise just updates object's variable
  if con.transMode == tmMode: return
  if con.connected:
    case tmMode:
    of tmAuto:
      if con.setConnectionAttr(SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_ON):
        # only set object variable on success
        con.transMode = tmMode
    of tmManual:
      if con.setConnectionAttr(SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF):
        # only set object variable on success
        con.transMode = tmMode
  else:
    con.transMode = tmMode

proc `timeout=`*(con: var ODBCConnection, conTimeout: int) =
  # if connected, this will set the connection attribute, otherwise just updates object's variable
  if con.connected:
    if con.setConnectionAttr(SQL_LOGIN_TIMEOUT, conTimeout):
      # only set object variable on success
      con.conTimeout = conTimeout
  else: con.conTimeout = conTimeout

proc connect*(con: var ODBCConnection): bool =
  var
    outstr: string = newStringOfCap(256)
    outstr_len: TSqlSmallInt
    conStr: string

  if con.connectionString != "":
    conStr = con.connectionString
  else:
    conStr = con.getConnectionString

  con.envHandle = newEnvHandle(con.reporting)
  setODBCType(con.envHandle, con.reporting)
  con.conHandle = newConnectionHandle(con.envHandle, con.reporting)

  # note:
  # M008: 'Dialog failed' may occur if you do not use SQL_DRIVER_NOPROMPT
  # and don't supply a login/pw as we are not passing a windows handle to display the dialog.
  var
    ret = SQLDriverConnect(con.conHandle, nil, conStr, conStr.len.TSqlSmallInt,
      outstr, 255.TSqlSmallInt, outstr_len, SQL_DRIVER_NOPROMPT)

  rptOnErr(con.reporting, ret, "SQLDriverConnect", con.conHandle, SQL_HANDLE_DBC.TSqlSmallInt)
  outstr.setLen(outstr_len.int)
  con.connected = ret.sqlSucceeded
  result = con.connected

  if con.connected:
    activeConnections.add(con.conHandle, con)
    when defined(odbcdebug): echo "Registered connection handle ", repr(con.conHandle)
  # Set timeout and handle transaction settings
  con.timeout = con.conTimeout
  con.transactionMode = con.transMode

proc beginTrans*(con: var ODBCConnection) =
  con.transactionMode = tmManual

proc commitTrans*(con: var ODBCConnection) =
  # NOTE: This switches the connection back to auto commit
  var ret = SQLEndTran(SQL_HANDLE_DBC, con.conHandle, SQL_COMMIT)
  rptOnErr(con.reporting, ret, "SQLEndTran", con.conHandle, SQL_HANDLE_DBC.TSqlSmallInt)
  con.transactionMode = tmAuto

proc rollbackTrans*(con: var ODBCConnection) =
  # NOTE: This switches the connection back to auto commit
  var ret = SQLEndTran(SQL_HANDLE_DBC, con.conHandle, SQL_ROLLBACK)
  rptOnErr(con.reporting, ret, "SQLEndTran", con.conHandle, SQL_HANDLE_DBC.TSqlSmallInt)
  con.transactionMode = tmAuto
