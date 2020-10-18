# Handle stuff
# NOTE that at the moment, this unit must be included in odbc to be compiled

import odbcsql, odbcerrors, odbcreporting

type
  ODBCHandleType* = enum
    htEnvironment, htStatement

  ODBCHandle* = object
    handleType*: ODBCHandleType
    handle*: SqlHandle


proc newEnvHandle*(rptState: var ODBCReportState): SqlHEnv {.inline.} =
  rptOnErr(rptState, SQLAllocHandle(SQL_HANDLE_ENV.TSqlSmallInt, SQL_NULL_HANDLE, result), "Alloc Handle ENV", result)

proc newConnectionHandle*(envHandle: SqlHEnv, rptState: var ODBCReportState): SqlHDBC {.inline.} =
  rptOnErr(rptState, SQLAllocHandle(SQL_HANDLE_DBC.TSqlSmallInt, envHandle, result), "Alloc Handle DBC", envHandle)

proc newStatementHandle*(conHandle: SqlHDBC, rptState: var ODBCReportState): SQLHSTMT {.inline.} =
  rptOnErr(rptState, SQLAllocHandle(SQL_HANDLE_STMT.TSqlSmallInt, conHandle, result), "Alloc Handle STMT", conHandle, SQL_HANDLE_DBC)

proc freeEnvHandle*(handle: SqlHEnv, rptState: var ODBCReportState) {.inline.} =
  rptOnErr(rptState, SQLFreeHandle(SQL_HANDLE_ENV.TSqlSmallInt, handle), "FreeHandle", handle)

proc freeConHandle*(handle: SqlHDBC, rptState: var ODBCReportState) {.inline.} =
  rptOnErr(rptState, SQLFreeHandle(SQL_HANDLE_DBC.TSqlSmallInt, handle), "Free Connection Handle", handle, SQL_HANDLE_DBC)

proc freeStatementHandle*(handle: SqlHStmt, rptState: var ODBCReportState) {.inline.} =
  rptOnErr(rptState, SQLFreeHandle(SQL_HANDLE_STMT.TSqlSmallInt, handle), "Free Statement Handle", handle, SQL_HANDLE_STMT)

proc disconnect*(conHandle: SqlHDBC, rptState: var ODBCReportState) {.inline.} =
  rptOnErr(rptState, SQLDisconnect(conHandle), "Disconnect", conHandle, SQL_HANDLE_DBC)

proc getEnvVal*(envHandle: SqlHEnv, rptState: var ODBCReportState): TSqlInteger {.inline.} =
  var
    strLen: TSqlInteger
  rptOnErr(rptState, SQLGetEnvAttr(envHandle, SQL_ATTR_ODBC_VERSION.TSqlInteger, addr result, 0, addr strLen), "GetEnv", envHandle)

proc setODBCType*(envHandle: SqlHEnv, rptState: var ODBCReportState) {.inline.} =
  var odbcVersion: TSqlInteger = SQL_OV_ODBC3
  rptOnErr(rptState, SQLSetEnvAttr(envHandle, SQL_ATTR_ODBC_VERSION.TSqlInteger, cast[SQLPointer](odbcVersion), 0), "SetEnv ODBC type", envHandle)

proc sqlSucceeded*(resint: TSqlSmallInt): bool {.inline.} =
  result = resint == SQL_SUCCESS or resint == SQL_SUCCESS_WITH_INFO



