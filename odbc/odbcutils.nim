import odbcsql, odbcerrors, odbcconnections, odbcreporting, odbchandles

type
  SQLDriverAttribute* = tuple[key, value: string]
  SQLDriverInfo* = object
    driver*: string
    attributes*: seq[SQLDriverAttribute]

proc `$`*(driverInfo: SQLDriverInfo): string =
  result = driverInfo.driver & ": \n"
  for attr in driverInfo.attributes:
    result &= " " & attr[0] & "=" & attr[1] & "\n"

proc `$`*(driverInfos: seq[SQLDriverInfo]): string =
  result = ""
  for di in driverInfos:
    result &= $di & "\n"

proc listDrivers*(con: ODBCConnection = nil): seq[SQLDriverInfo] =
  const
    driverBufLen = 256
    attrBufLen = 2048
  var
    env: SqlHEnv
    driver = newString(driverBufLen)
    attr = newString(attrBufLen)
    driver_ret: TSqlSmallInt
    attr_ret: TSqlSmallInt
    direction: SqlUSmallInt
    ret: TSqlSmallInt
    newStr: string = ""
    i: int
    rpt: ODBCReportState

  result = @[]

  if con != nil:
    rpt = con.reporting
  else:
    rpt = newODBCReportState()

  env = newEnvHandle(rpt)

  setODBCType(env, con.reporting)

  direction = SQL_FETCH_FIRST

  ret = SQLDrivers(env, direction, driver.cstring, driverBufLen.TSqlSmallInt,
    addr driver_ret, attr.cstring, attrBufLen.TSqlSmallInt, addr attr_ret)
  rptOnErr(rpt, ret, "sqlDrivers", env)

  while ret == SQL_SUCCESS or ret == SQL_SUCCESS_WITH_INFO:

    driver.setLen(driver_ret)
    attr.setLen(attrBufLen)

    var newDriverInfo: SQLDriverInfo
    newDriverInfo.attributes = @[]
    newDriverInfo.driver = driver

    # get attributes
    #if ret == SQL_SUCCESS_WITH_INFO:
    # The data has been truncated, and attr_ret now contains how many chars we missed
    i = 0
    while i < attr_ret:
      if attr[i].int == 0:
        var
          newAttr: SQLDriverAttribute
          eqls = newStr.find('=')
        if eqls > -1:
          # we have a key/value pair
          newAttr[0] = newStr[0..eqls-1]
          newAttr[1] = newStr.substr(eqls+1)
        else:
          newAttr[0] = newStr

        newDriverInfo.attributes.add(newAttr)
        newStr = ""
      else: newStr.add(attr[i])
      i += 1

    direction = SQL_FETCH_NEXT
    result.add(newDriverInfo)

    ret = SQLDrivers(env, direction, driver, driverBufLen.TSqlSmallInt,
      addr driver_ret, attr, attrBufLen.TSqlSmallInt, addr attr_ret)

  freeEnvHandle(env, rpt)

