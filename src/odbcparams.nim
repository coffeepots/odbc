import odbcsql, odbctypes, tables, strutils, odbcerrors, times, unicode, odbcreporting
include odbcfields
from math import round

var
  # Default buffer size for parameters and also
  # for reading data into for each field when fetching results.
  # This is settable by the user.
  sqlDefaultBufferSize* = 255

type
  # This is used to cast pointers for easier access
  UncheckedArray {.unchecked.} [t] = array[0,t]
  SQLByteArrayUC = ptr UncheckedArray[byte]
  Utf16ArrayUC = ptr UncheckedArray[Utf16Char]

  SQLParam* = SQLValue
  #    paramDir: SQLParamDirection

  ParamBuffer* = pointer
  ParamIndBuf* = TSqlInteger

  SQLParams* = object
    items*: seq[SQLParam]         # actual parameter field & data
    references: seq[int]          # list of indexes to items seq; this allows duplicate parameters
    names: Table[string, int]     # for looking up parameter names in O(1)
    paramBuf: seq[ParamBuffer]    # buffer for data
    paramIndBuf: seq[ParamIndBuf] # buffer for ind

proc initParams*: SQLParams =
  result.items = @[]
  result.references = @[]
  result.names = initTable[string, int]()
  result.paramBuf = @[]
  result.paramIndBuf = @[]

proc initParam*: SQLParam =
  result.field = newSQLField()

proc len*(params: SQLParams): int =
  result = params.items.len

proc high*(params: SQLParams): int =
  result = params.items.high

proc freeParamBufs(params: var SQLParams) =
  # called by finalizer
  if params.paramBuf != nil:
    for item in params.paramBuf:
      if item != nil:
        dealloc(item)
    params.paramBuf = @[]

proc `[]`*(params: SQLParams, index: int): SQLParam =
  result = params.items[index]

proc `[]=`*(params: var SQLParams, index: int, value: SQLParam) =
  params.items[index] = value

proc `[]`*(params: SQLParams, name: string): SQLParam =
  let nameLower = toLowerAscii(name)
  if params.names.hasKey(nameLower): result = params.items[params.names[nameLower]]
  else:
    raise newODBCUnknownParameterException("parameter \"" & $name & "\" not found in statement")

proc `[]=`*(params: var SQLParams, name: string, value: SQLParam) =
  # for some reason this needs to be * exported to be used below.
  # changing to a non `` name solved the problem like so:
  # proc setByName(params: var SQLParams, name: string, value: SQLParam) =
  var nameLower = toLowerAscii(name)
  when defined(odbcdebug): echo "Setting param: ", value
  if params.names.hasKey(nameLower):
    when defined(odbcdebug): echo " Has key, setting value for index ", params.names[nameLower]
    params.items[params.names[nameLower]] = value
  else:
    # create new param name
    raise newODBCUnknownParameterException("parameter \"" & $name & "\" not found in statement")

proc `[]=`*(params: var SQLParams, index: string, data: int|int64|string|bool|float|TimeInterval|Time|SQLBinaryData) =
  # have to determine the column details for this type
  let
    paramName = toLowerAscii(index)

  if not params.names.hasKey(paramName):
    raise newODBCUnknownParameterException("parameter \"" & $index & "\" not found in statement")

  var
    curParam: SQLParam = initParam()

  # param is just replaced
  curParam.data = initSQLData[data.type](data)
  curParam.field.setType(data)

  params[paramName] = curParam

proc clear*(params: var SQLParams) =
  params.freeParamBufs  # frees existing memory buffers or initialises seq to @[]
  params = initParams()

proc clear*(params: var SQLParams, index: string) =
  # look up param
  let
    paramName = toLowerAscii(index)

  if not params.names.hasKey(paramName):
    raise newODBCUnknownParameterException("parameter \"" & $index & "\" not found in statement")

  # Param is copied from existing, but kind is changed to null
  var
    curParam: SQLParam = params[paramName]

  curParam.data.kind = dtNull
  params[paramName] = curParam

proc bufWideStrToStr(buffer: pointer, count: int): string {.inline.} =
  # SQL Server uses widestring for unicode
  var
    sBuf = cast[Utf16ArrayUC](buffer)
    ws = newWideCString("", count)  # allocate ws ref of size count
    i = 0

  while int16(sBuf[i]) != 0'i16:
    ws[i] = sBuf[i]
    i+=1
  ws[i] = Utf16Char(0'i16)  # terminator

  result = $ws

proc strToWideStrBuf(strVal: string, buffer: pointer) {.inline.} =
  var
    ws = newWideCString(strVal)
    bufStr = cast[Utf16ArrayUC](buffer)
    i = 0
  while i < ws.len:
    bufStr[i] = ws[i]
    i += 1
  bufStr[i] = Utf16Char(0'i16)  # terminator

proc bufToSeq(buffer: pointer, count: int): SQLBinaryData {.inline.} =
  result = @[]
  var sbuf = cast[SQLByteArrayUC](buffer)
  for idx in 0..<count: result.add(sbuf[idx])

proc seqToBuf(seqVal: SQLBinaryData, buffer: pointer, count: int) {.inline.} =
  var sbuf = cast[SQLByteArrayUC](buffer)
  for idx in 0..<count:
    sbuf[idx] = seqVal[idx]

proc readFromBuf(dataItem: var SQLData, buffer: ParamBuffer, indicator: int) =
  # this function assumes the data item kind is already set up
  case dataItem.kind
  of dtNull: dataItem.nullVal = nullValue
  of dtString: dataItem.strVal = bufWideStrToStr(buffer, indicator)
  of dtInt: dataItem.intVal = cast[ptr int](buffer)[]
  of dtInt64: dataItem.int64Val = cast[ptr int64](buffer)[]
  of dtBool: dataItem.boolVal = cast[ptr bool](buffer)[]
  of dtFloat: dataItem.floatVal = cast[ptr float](buffer)[]
  of dtBinary: dataItem.binVal = bufToSeq(buffer, indicator)
  of dtTime:
    var
      timestamp = cast[ptr SQL_TIMESTAMP_STRUCT](buffer)
    dataItem.timeVal = initInterval(timestamp.Second.int, timestamp.Minute.int, timestamp.Hour.int,
        timestamp.Day.int - 1, timestamp.Month.int, timestamp.Year.int) # day is 1 indexed in SQL
  when defined(odbcdebug):
    echo "Read buffer (first 255 bytes): ", repr(cast[ptr array[0..255, byte]](buffer))

proc writeToBuf(dataItem: SQLData, buffer: ParamBuffer) =
  # this function assumes the data item kind is already set up
  # todo: string appropriate structure for this?
  case dataItem.kind
  of dtNull: cast[ptr int](buffer)[] = 0
  of dtString: dataItem.strVal.strToWideStrBuf(buffer)
  of dtInt: cast[ptr int](buffer)[] = dataItem.intVal
  of dtInt64: cast[ptr int64](buffer)[] = dataItem.int64Val
  of dtBool: cast[ptr bool](buffer)[] = dataItem.boolVal
  of dtFloat: cast[ptr float](buffer)[] = dataItem.floatVal
  of dtBinary: dataItem.binVal.seqToBuf(buffer, dataItem.binVal.len)
  of dtTime:
    var
      timestamp = cast[ptr SQL_TIMESTAMP_STRUCT](buffer)
    timestamp.Fraction = (dataItem.timeVal.milliseconds * 1000).SqlUSmallInt
    timestamp.Second = dataItem.timeVal.seconds.SqlUSmallInt
    timestamp.Minute = dataItem.timeVal.minutes.SqlUSmallInt
    timestamp.Hour = dataItem.timeVal.hours.SqlUSmallInt
    timestamp.Day = dataItem.timeVal.days.SqlUSmallInt + 1  # day is 1 indexed in SQL
    timestamp.Month = dataItem.timeVal.months.SqlUSmallInt
    timestamp.Year = dataItem.timeVal.years.SqlUSmallInt
  when defined(odbcdebug):
    echo "Write buffer (first 255 bytes): ", repr(cast[ptr array[0..255, byte]](buffer))

proc allocateBuffers(params: var SQLParams) =
  # assumes parameter quantity have been read from statement
  when defined(odbcdebug):
    echo "Allocate buffers: requesting $# items, cur len $#" % [$params.items.len, $params.paramBuf.len]
  if params.items.len == params.paramBuf.len: return  # no work

  if params.paramBuf == nil:
    params.paramBuf = @[]
  if params.paramIndBuf == nil:
    params.paramIndBuf = @[]

  if params.items.len < params.paramBuf.len:
    # we need to reduce the number of buffers
    when defined(odbcdebug): echo "Reducing buffer size:"
    for idx in params.items.len .. params.paramBuf.len - 1:
      if params.paramBuf[idx] != nil:
        when defined(odbcdebug): echo " Freeing paramBuf ", idx
        params.paramBuf[idx].dealloc

    params.paramBuf.setLen(params.items.len)
    params.paramIndBuf.setLen(params.items.len) # no need to free anything here
  else:
    # l = 20, ol = 10
    # 10 .. 19
    when defined(odbcdebug): echo "Increasing buffer size:"
    for idx in params.paramBuf.len .. params.items.len - 1:
      var indBuf: ParamIndBuf = 0 # Just an int no need to free
      when defined(odbcdebug): echo " Adding paramBuf $# size (default): $# bytes" % [$idx, $sqlDefaultBufferSize]

      # allocate parameter buffer to default size
      params.paramBuf.add(alloc0(sqlDefaultBufferSize))
      params.paramIndBuf.add(indBuf)

proc bindParams(handle: SqlHStmt, params: var SQLParams, rptState: var ODBCReportState) =
  # Parameters: ODBC can't use named parameters, so we need to do a string lookup
  # and populate according to order
  # Note that we don't do anything else until we need to perform the query

  var paramIdx = 1

  for idx, paramIdxRef in params.references:
    # get actual parameter
    let
      curParam = params.items[paramIdxRef]
    var
      paramDataSize: SqlUInteger = curParam.field.size
      colSize = curParam.field.size

    if curParam.data.kind == dtNull:
      params.paramIndBuf[paramIdxRef] = SQL_NULL_DATA  # null
    else:
      if curParam.data.kind == dtString:
        if curParam.data.strVal.len > 0:
          paramDataSize = (curParam.field.size + 1) * 2 # WideChar
          params.paramIndBuf[paramIdxRef] = curParam.data.strVal.len*2
        else:
          # allow empty strings, else ODBC complains of precision error.
          # NOTE: This ends up meaning that a "" is converted to " " :(
          colSize = 1
      else:
        params.paramIndBuf[paramIdxRef] = curParam.field.size

    if paramDataSize > sqlDefaultBufferSize:
      # reallocate buffer size to accommodate larger data sizes
      when defined(odbcdebug): echo "Reallocating parameter buffer to be length: ", paramDataSize + 2
      params.paramBuf[idx].dealloc
      params.paramBuf[idx] = alloc0(paramDataSize + 2)

    # write data to param buffers
    curParam.data.writeToBuf(params.paramBuf[paramIdxRef]) # main data buffer
    # indicator buffer

    when defined(odbcdebug):
      echo """Binding: $idx (real index $realidx),
        type = $type ctype = $ctype, raw odbc type = $odbctype size = $size
        (passed size: $passedSize, bufSize: $bufsize, indBuff: $indbuffsize)
        digits = $digits
        bind: data = $data
        """ %
        ["idx", $idx,
        "realidx", $paramIdxRef,
        "type", $curParam.field.dataType,
        "ctype", $curParam.field.cType,
        "odbctype", $curParam.field.rawSqlType,
        "size", $curParam.field.size,
        "passedSize", $paramDataSize,
        "data", $curParam.data,
        "bufsize", $sqlDefaultBufferSize,
        "digits", $curParam.field.digits,
        "indbuffsize", $params.paramIndBuf[paramIdxRef]]

    var res = SQLBindParameter(handle,
      paramIdx.SqlUSmallInt,
      SQL_PARAM_INPUT.TSqlSmallInt,
      curParam.field.cType,
      curParam.field.rawSQLType,
      colSize,                            # column size (for string types this is number of characters)
      curParam.field.digits.TSqlSmallInt, # digit size
      params.paramBuf[paramIdxRef],       # pointer to data to bind
      paramDataSize,                      # byte count of data
      addr(params.paramIndBuf[paramIdxRef])
      )
    when defined(odbcdebug): echo "param ind after bind is ", params.paramIndBuf[paramIdxRef]
    rptOnErr(rptState, res, "SQLBindParameter", handle, SQL_HANDLE_STMT.TSqlSmallInt)

    paramIdx += 1

# saves importing parseutils for one func
proc readAlphaNumeric(text: string, position: var int): string =
  result = ""
  while (text[position] in Letters or text[position] in Digits) and position < text.len:
    result.add(text[position])
    position += 1

proc setupParams(params: var SQLParams, sqlStatement: string, paramPrefix: string = "?") =
  # find all "?name" variables (where ? is paramPrefix) and construct parameters for them
  var
    start = 0
    p = sqlStatement.find(paramPrefix, start)
    paramName: string

  # This routine is called every time a new SQL statement is supplied.
  # Buffers are allocated below after the number of parameters has been determined.
  params = initParams()
  while p >= 0:
    # found prefix, try to add to params
    p += paramPrefix.len  # don't include prefix in param name

    paramName = toLowerAscii(sqlStatement.readAlphaNumeric(p))

    # check for existing parameter with this name
    if not params.names.hasKey(paramName):
      # add parameter
      var newParam: SQLValue
      # set up field, type specified by user
      newParam.field = newSQLField()
      params.items.add(newParam)
      params.names[paramName] = params.high
      when defined(odbcdebug): echo "Found new param: \"" & paramName & "\""

    # set reference to point to the parameter with this name,
    # this allows queries such as "SELECT ?a, ?a, ?a" with only one parameter
    params.references.add(params.names[paramName])

    # prepare for next param
    start = p
    p = sqlStatement.find(paramPrefix, start)

  # allocate space for above buffers, to be filled when params are bound (on execute/open)
  params.allocateBuffers

proc odbcParamStatement(sqlStatement: string, paramPrefix: string = "?"): string =
  # returns a string where parameters are replaced with "?"
  var
    start = 0
    p = sqlStatement.find(paramPrefix, start)

  if p == -1:
    result = sqlStatement # no parameters
  else:
    result = ""
    while p >= 0:
      # found prefix, try to add to params
      result &= sqlStatement[start .. p - 1] & "?" # ? is ODBC's param marker

      # prepare for next param
      start = p + paramPrefix.len # don't include prefix in param name
      # read to end of word to remove param name
      var pname = ""
      while start < sqlStatement.len and
        (sqlStatement[start] in Letters or sqlStatement[start] in Digits):
        pname.add(sqlStatement[start])
        start += 1

      p = sqlStatement.find(paramPrefix, start)

    if start < sqlStatement.len:
      result &= sqlStatement[start .. sqlStatement.len]

  when defined(odbcdebug): echo "ODBC Statement: ", result
