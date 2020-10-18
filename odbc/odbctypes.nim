import times, odbcsql, typetraits, tables, strutils, unicode, odbcerrors

const
  nullValue* = ""
  ms = 1_000_000

# Note:
# If an application working with a Unicode driver binds to SQL_CHAR,
# the Driver Manager will not map the SQL_CHAR data to SQL_WCHAR.
# The Unicode driver must accept the SQL_CHAR data.

type
  FieldIdxs* = Table[string, int]  # this allows linking fieldname with index into field columns
  SQLDataType* = enum dtNull, dtString, dtInt, dtInt64, dtBool, dtFloat, dtTime, dtBinary, dtGuid

  SQLColType* = enum ctUnknown, ctFixedStr, ctString, ctUnicodeFixedStr, ctUnicodeStr, ctDecimal,
    ctBit, ctTinyInt, ctSmallInt, ctInt, ctBigInt, ctFloat, ctDouble, ctFixedBinary, ctBinary, ctDateTime,
    ctDate, ctTime, ctTimeStamp, ctYearInterval, ctMonthInterval, ctDayInterval, ctHourInterval,
    ctMinuteInterval, ctSecondInterval, ctGUID

  SQLBinaryData* = seq[byte]

  GuidData* {.final, pure.} = object
    D1*: int32
    D2*: int16
    D3*: int16
    D4*: array[0..7, int8]

  SQLData* = object
    ## Variant type that represents a field's value.
    case kind: SQLDataType
    of dtNull: nullVal*: string
    of dtString: strVal*: string
    of dtInt: intVal*: int
    of dtInt64: int64Val*: int64
    of dtBool: boolVal*: bool
    of dtFloat: floatVal*: float  # NOTE: C's float is float32, Nim's is equiv to C's double
    of dtTime: timeVal*: TimeInterval
    of dtBinary: binVal*: SQLBinaryData
    of dtGuid: guidVal*: GuidData

  SQLRow* = seq[SQLData]

proc initFieldIdxs*: FieldIdxs = initTable[string, int]()

proc toSQLDataType*(t: typeDesc): SQLDataType =
  # Convert type to datatype
  if t is string: result = dtString # NOTE: This assumes string is unicode!
  elif t is int: result = dtInt
  elif t is int64: result = dtInt64
  elif t is bool: result = dtBool
  elif t is float: result = dtFloat
  elif t is DateTime: result = dtTime
  elif t is SQLBinaryData: result = dtBinary
  else:
    result = dtNull
    raise newODBCUnsupportedTypeException($t.name)

proc toSQLColumnType*(t: typeDesc): SQLColType =
  # Convert type to columntype
  if t is string: result = ctString # NOTE: This assumes string is unicode!
  elif t is int: result = ctInt
  elif t is int64: result = ctBigInt
  elif t is bool: result = ctBit
  elif t is float: result = ctFloat
  elif t is DateTime or t is TimeInterval: result = ctTime
  elif t is SQLBinaryData: result = ctBinary
  else:
    result = ctUnknown
    raise newODBCUnsupportedTypeException($t.name)

proc toSQLColumnType*(datatype: TSqlSmallInt): SQLColType =
  case datatype
  of SQL_LONGVARCHAR: result = ctString
  of SQL_BINARY: result = ctFixedBinary
  of SQL_VARBINARY, SQL_LONGVARBINARY: result = ctBinary
  of SQL_BIGINT: result = ctBigInt
  of SQL_TINYINT: result = ctTinyInt
  of SQL_BIT: result = ctBit
  of SQL_WCHAR: result = ctUnicodeFixedStr
  of SQL_WVARCHAR, SQL_WLONGVARCHAR: result = ctUnicodeStr
  of SQL_CHAR: result = ctFixedStr
  #of SQL_NUMERIC, SQL_DECIMAL: result = ctString  # Yep: https://msdn.microsoft.com/en-us/library/eshhha8h.aspx
  of SQL_NUMERIC, SQL_DECIMAL: result = ctFloat
  of SQL_INTEGER: result = ctInt
  of SQL_SMALLINT: result = ctSmallInt
  of SQL_FLOAT, SQL_REAL: result = ctFloat
  of SQL_DOUBLE: result = ctDouble
  of SQL_DATETIME: result = ctDateTime
  of SQL_VARCHAR: result = ctString
  of SQL_TYPE_DATE: result = ctDate
  of SQL_TYPE_TIME, SQL_TIME: result = ctTime
  of SQL_TYPE_TIMESTAMP, SQL_TIMESTAMP: result = ctTimeStamp
  of SQL_GUID: result = ctGUID
  of SQL_INTERVAL_YEAR: result = ctYearInterval
  of SQL_INTERVAL_MONTH: result = ctMonthInterval
  of SQL_INTERVAL_DAY: result = ctDayInterval
  of SQL_INTERVAL_HOUR: result = ctHourInterval
  of SQL_INTERVAL_MINUTE: result = ctMinuteInterval
  of SQL_INTERVAL_SECOND: result = ctSecondInterval
  else: result = ctUnknown

proc toDataType*(dataType: SQLColType): SQLDataType =
  case dataType
  of ctFixedStr, ctString, ctUnicodeFixedStr, ctUnicodeStr: result = dtString
  of ctDecimal, ctTinyInt, ctSmallInt, ctInt: result = dtInt
  of ctBigInt: result = dtInt64
  of ctBit: result = dtBool
  of ctFloat, ctDouble: result = dtFloat
  of ctDate, ctTime, ctTimeStamp: result = dtTime
  of ctBinary, ctFixedBinary: result = dtBinary
  of ctGUID: result = dtGuid
  else:
    result = dtNull
    # TODO: dtFixedBinary, dtBigInt, dtYearInterval, dtMonthInterval, dtDayInterval, dtSecondInterval
    raise newODBCUnsupportedTypeException($dataType)

proc toCType*(sqlType: SQLDataType): TSqlSmallInt =
  case sqlType
  of dtNull: SQL_TYPE_NULL
  of dtString: SQL_WCHAR
  of dtInt: SQL_C_SLONG
  of dtInt64: SQL_C_SBIGINT
  of dtBool: SQL_C_BIT
  of dtFloat: SQL_C_DOUBLE
  of dtTime: SQL_C_TYPE_TIMESTAMP
  of dtBinary: SQL_C_BINARY
  of dtGUID: SQL_C_GUID 

proc toCType*(data: SQLData): TSqlSmallInt =
  result = data.kind.toCType

proc toCType*(colType: SQLColType): TSqlSmallInt =
  result = colType.toDataType.toCType

proc isString*(colType: SQLColType): bool =
  colType == ctFixedStr or colType == ctString or colType == ctUnicodeFixedStr or colType == ctUnicodeStr

proc toCType*(t: typeDesc): TSqlSmallInt =
  if t is string: result = SQL_WCHAR
  elif t is int: result = SQL_INTEGER
  elif t is int64: result = SQL_C_SBIGINT
  elif t is bool: result = SQL_C_BIT
  elif t is float: result = SQL_C_DOUBLE
  elif t is DateTime or t is TimeInterval: result = SQL_C_TYPE_TIMESTAMP
  elif t is SQLBinaryData: result = SQL_C_BINARY
  elif t is GuidData: result = SQL_C_GUID
  else:
    result = SQL_TYPE_NULL
    raise newODBCUnsupportedTypeException($t.name)

proc toSqlType*(t: typeDesc): TSqlSmallInt =
  if t is string: result = SQL_WCHAR
  elif t is int: result = SQL_INTEGER
  elif t is int64: result = SQL_BIGINT
  elif t is bool: result = SQL_BIT
  elif t is float: result = SQL_FLOAT
  elif t is DateTime or t is TimeInterval: result = SQL_TYPE_TIMESTAMP
  elif t is SQLBinaryData: result = SQL_BINARY
  else:
    result = SQL_UNKNOWN_TYPE
    raise newODBCUnsupportedTypeException($t.name)

proc toSqlType*(dataType: SQLDataType): TSqlSmallInt =
  case dataType
  of dtNull: SQL_UNKNOWN_TYPE
  of dtString: SQL_WCHAR
  of dtInt: SQL_INTEGER
  of dtInt64: SQL_BIGINT
  of dtBool: SQL_BIT
  of dtFloat: SQL_FLOAT
  of dtTime: SQL_C_TIMESTAMP
  of dtBinary: SQL_BINARY
  of dtGuid: SQL_GUID

proc toSqlType*(data: SQLData): TSqlSmallInt =
  result = data.kind.toSqlType

proc initSQLData*(kind: SQLDataType = dtNull): SQLData = SQLData(kind: kind)

proc distributeNanoseconds*(interval: var TimeInterval) =
  ## Populates fractional components milliseconds and microseconds from nanoseconds,
  ## and trims nanoseconds.
  let
    ns = interval.nanoseconds
    msRemaining = ns mod ms
  interval.milliseconds = ns div ms
  interval.microseconds = msRemaining div 1_000
  interval.nanoseconds = msRemaining mod 1_000

proc distributeNanoseconds*(interval: TimeInterval): TimeInterval =
  result = interval
  result.distributeNanoseconds

template stuffNanoseconds*(interval: TimeInterval): int =
  interval.nanoseconds + interval.microseconds * 1_000 + interval.milliseconds * ms

proc initSQLData*(inputData: int): SQLData = SQLData(kind: dtInt, intVal: inputData)
proc initSQLData*(inputData: int64): SQLData = SQLData(kind: dtInt64, int64Val: inputData)
proc initSQLData*(inputData: string): SQLData = SQLData(kind: dtString, strVal: inputData)
proc initSQLData*(inputData: bool): SQLData = SQLData(kind: dtBool, boolVal: inputData)
proc initSQLData*(inputData: float): SQLData = SQLData(kind: dtFloat, floatVal: inputData)
proc initSQLData*(inputData: DateTime): SQLData =
  var ti: TimeInterval
  ti.nanoseconds = inputData.nanosecond
  ti.distributeNanoseconds
  ti.seconds = inputData.second
  ti.minutes = inputData.minute
  ti.hours = inputData.hour
  ti.days = inputData.monthDay
  #ti.weeks = inputData.we
  ti.months = int(inputData.month)
  ti.years = inputData.year
  SQLData(kind: dtTime, timeVal: ti)
proc initSQLData*(inputData: TimeInterval): SQLData = SQLData(kind: dtTime, timeVal: inputData)
proc initSQLData*(inputData: SQLBinaryData): SQLData = SQLData(kind: dtBinary, binVal: inputData)
proc initSQLData*(inputData: GuidData): SQLData = SQLData(kind: dtGuid, guidVal: inputData)

proc `$`*(guid: GuidData): string =
  template clock(value: untyped): string = value.toHex
  proc arrayHex(value: openarray[int8]): string {.inline.} =
    for b in value:
      result &= b.toHex
  result = 
    guid.D1.clock & "-" &
    guid.D2.clock & "-" &
    guid.D3.clock & "-" &
    guid.D4[0..1].arrayHex & "-" &
    guid.D4[2..7].arrayHex

proc `$`*(sqlData: SQLData): string =
  ## Return a string representation of `sqlData`.
  case sqlData.kind
  of dtNull: result = "<NULL>"
  of dtString: result = "" & $sqlData.strVal & ""
  of dtInt: result = $sqlData.intVal
  of dtInt64: result = $sqlData.int64Val
  of dtBool: result = (if sqlData.boolVal: "true" else: "false")
  of dtFloat: result = $sqlData.floatVal
  of dtTime: result = "" & $sqlData.timeVal & ""
  of dtBinary:
    result = ""
    for byt in sqlData.binVal:
      result &= $byt & '.'
  of dtGuid:
    result = $sqlData.guidVal
  result &= "\n(" & $sqlData.kind & ")"

proc setTo[T: int8|uint8, U: SomeInteger](list: var openarray[T], startPos: int, value: U) =
  # Set a consecutive portion of a list to the bytes in value.
  let maxIndex = min(startPos + U.sizeOf, list.len) - 1
  for i in startPos..maxIndex:
    list[i] = cast[T]((value shr ((maxIndex - i) * 8) and 0xFf))

proc guid*(guidStr: string): GuidData =
  ## Create a GUID from a string of the format `xxxxxxxx-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx`.
  doAssert guidStr.len == 36, "Expecting format: xxxxxxxx-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx"

  result.D1 = fromHex[int32](guidStr[0..7])
  result.D2 = fromHex[int16](guidStr[9..12])
  result.D3 = fromHex[int16](guidStr[14..17])
  result.D4.setTo 0, fromHex[int16](guidStr[19..22])
  result.D4.setTo 2, fromHex[int](guidStr[24..35])

proc asInt*(sqlData: SQLData): int =
  ## Return `sqlData` as an int where possible.
  case sqlData.kind
  of dtNull: result = 0
  of dtString:
    try:
      result = sqlData.strVal.parseInt
    except:
      raise newODBCException("Error converting to int from string value \"" & sqlData.strVal & "\"")
  of dtInt: result = sqlData.intVal
  of dtInt64:
    if sqlData.int64Val > int.high:
      raise newODBCOverflowException("64 bit value " & $sqlData.int64Val & " is too large to fit into int")
    else:
      result = sqlData.int64Val.int
  of dtBool: result = if sqlData.boolVal: 1 else: 0
  of dtFloat: result = sqlData.floatVal.int
  of dtTime: raise newODBCException("Cannot transform " & sqlData.timeVal.type.name & " to int")
  of dtBinary: raise newODBCException("Cannot transform binary to int")
  of dtGuid: raise newODBCException("Cannot transform GUID to int")

proc asInt64*(sqlData: SQLData): int64 =
  ## Return `sqlData` as an int64 where possible.
  case sqlData.kind
  of dtNull: result = 0
  of dtString:
    try:
      # TODO: This will only work on 64 bit systems AFAICT :/
      result = sqlData.strVal.parseBiggestInt
    except:
      raise newODBCException("Error converting to int64 from string value \"" & sqlData.strVal & "\"")
  of dtInt:
    result = sqlData.intVal.int64
  of dtInt64:
    result = sqlData.int64Val
  of dtBool: result = if sqlData.boolVal: 1 else: 0
  of dtFloat: result = sqlData.floatVal.int64
  of dtTime: raise newODBCException("Cannot transform " & sqlData.timeVal.type.name & " to int64")
  of dtBinary: raise newODBCException("Cannot transform binary to int64")
  of dtGuid: raise newODBCException("Cannot transform GUID to int64")

proc asFloat*(sqlData: SQLData): float =
  ## Return `sqlData` as a float where possible.
  case sqlData.kind
  of dtNull: result = 0.0
  of dtString:
    try:
      result = sqlData.strVal.parseFloat
    except:
      raise newODBCException("Error converting to float from string value \"" & sqlData.strVal & "\"")
  of dtInt: result = sqlData.intVal.float
  of dtInt64: result = sqlData.int64Val.float
  of dtBool: result = if sqlData.boolVal: 1.0 else: 0.0
  of dtFloat: result = sqlData.floatVal
  of dtTime: raise newODBCException("Cannot transform " & sqlData.timeVal.type.name & " to float")
  of dtBinary: raise newODBCException("Cannot transform binary to float")
  of dtGuid: raise newODBCException("Cannot transform GUID to float")

proc asBool*(sqlData: SQLData): bool =
  ## Return `sqlData` as a boolean where possible.
  case sqlData.kind
  of dtNull: result = false
  of dtString:
    try:
      result = sqlData.strVal.parseBool
    except:
      raise newODBCException("Error converting to bool from string value \"" & sqlData.strVal & "\"")
  of dtInt: result = sqlData.intVal != 0
  of dtInt64: result = sqlData.int64Val != 0
  of dtBool: result = sqlData.boolVal
  of dtFloat: result = sqlData.floatVal != 0.0
  of dtTime: raise newODBCException("Cannot transform " & sqlData.timeVal.type.name & " to bool")
  of dtBinary: raise newODBCException("Cannot transform binary to bool")
  of dtGuid: raise newODBCException("Cannot transform GUID to bool")

proc asString*(sqlData: SQLData): string =
  ## Return `sqlData` as a string.
  case sqlData.kind
  of dtNull: nullValue
  of dtString: sqlData.strVal
  of dtInt: $sqlData.intVal
  of dtInt64: $sqlData.int64Val
  of dtBool: $sqlData.boolVal
  of dtFloat: $sqlData.floatVal
  of dtTime: $sqlData.timeVal
  of dtBinary: $sqlData.binVal
  of dtGuid: $sqlData.guidVal

proc asBinary*(sqlData: SQLData): SQLBinaryData =
  ## Return `sqlData` as a seq[byte] where possible.
  case sqlData.kind
  of dtNull: discard
  of dtString:
    var cs = sqlData.strVal.cstring
    result.setLen sqlData.strVal.len
    for idx, c in cs:
      result[idx] = c.byte
  of dtInt:
    const intSize = int.sizeOf
    var buf = cast[array[intSize, byte]](sqlData.intVal)
    result.setLen intSize
    for idx, b in buf:
      result[idx] = b
  of dtInt64:
    const intSize = int64.sizeOf
    var buf = cast[array[intSize, byte]](sqlData.int64Val)
    result.setLen intSize
    for idx, b in buf:
      result[idx] = b
  of dtBool:
    result.setLen 1
    result[0] = sqlData.boolVal.byte
  of dtFloat:
    const floatSize = float.sizeOf
    var buf = cast[array[floatSize, byte]](sqlData.floatVal)
    result.setLen floatSize
    for idx, b in buf:
      result[idx] = b
  of dtTime:
    const timeSize = sqlData.timeVal.sizeOf
    var buf = cast[array[timeSize, byte]](sqlData.timeVal)
    result.setLen timeSize
    for idx, b in buf:
      result[idx] = b
  of dtBinary:
    result.setLen sqlData.binVal.len
    for idx, b in sqlData.binVal: result[idx] = b
  of dtGuid:
    template guid: untyped = sqlData.guidVal
    result.setLen 16
    result.setTo 0, guid.D1
    result.setTo 4, guid.D2
    result.setTo 6, guid.D3
    for i, b in guid.D4:
      result[i] = uint8(b)
    
proc asTimeInterval*(sqlData: SQLData): TimeInterval =
  ## Return `sqlData` as a time interval where possible.
  if sqlData.kind == dtTime: result = sqlData.timeVal
  else: raise newODBCException("Cannot transform " & $sqlData.kind & " to time")

## Check if a value is `null`.
proc isNull*(sqlData: SQLData): bool = sqlData.kind == dtNull

proc kind*(data: SQLData): SQLDataType {.inline.} = data.kind

proc `kind=`*(data: var SQLData, kind: SQLDataType) =
  data = SQLData(kind: kind)

converter toBinary*(sqlData: SQLData): SQLBinaryData = sqlData.asBinary
converter toString*(sqlData: SQLData): string = sqlData.asString
converter toFloat*(sqlData: SQLData): float = sqlData.asFloat
converter toBool*(sqlData: SQLData): bool = sqlData.asBool
converter toInt*(sqlData: SQLData): int = sqlData.asInt
converter toInt64*(sqlData: SQLData): int64 = sqlData.asInt64
converter toTimeInterval*(sqlData: SQLData): TimeInterval = sqlData.asTimeInterval

proc `$`*(handle: SqlHStmt): string = handle.repr

