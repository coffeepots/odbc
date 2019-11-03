import times, odbcsql, typetraits, tables, strutils, unicode, odbcerrors

const
  nullValue* = ""

# Note:
# If an application working with a Unicode driver binds to SQL_CHAR,
# the Driver Manager will not map the SQL_CHAR data to SQL_WCHAR.
# The Unicode driver must accept the SQL_CHAR data.

type
  FieldIdxs* = Table[string, int]  # this allows linking fieldname with index into field columns
  SQLDataType* = enum dtNull, dtString, dtInt, dtInt64, dtBool, dtFloat, dtTime, dtBinary

  SQLColType* = enum ctUnknown, ctFixedStr, ctString, ctUnicodeFixedStr, ctUnicodeStr, ctDecimal,
    ctBit, ctTinyInt, ctSmallInt, ctInt, ctBigInt, ctFloat, ctDouble, ctFixedBinary, ctBinary, ctDateTime,
    ctDate, ctTime, ctTimeStamp, ctYearInterval, ctMonthInterval, ctDayInterval, ctHourInterval,
    ctMinuteInterval, ctSecondInterval, ctGUID

  SQLBinaryData* = seq[byte]

  SQLData* = object
    case kind: SQLDataType
    of dtNull: nullVal*: string
    of dtString: strVal*: string
    of dtInt: intVal*: int
    of dtInt64: int64Val*: int64
    of dtBool: boolVal*: bool
    of dtFloat: floatVal*: float  # NOTE: C's float is float32, Nim's is equiv to C's double
    of dtTime: timeVal*: TimeInterval
    of dtBinary: binVal*: SQLBinaryData

  SQLRow* = seq[SQLData]

proc initFieldIdxs*: FieldIdxs = initTable[string, int]()

proc toSQLDataType*(t: typeDesc): SQLDataType =
  # convert type to datatype
  # NOTE: This assumes string is unicode!
  if t is string: result = dtString
  elif t is int: result = dtInt
  elif t is int64: result = dtInt64
  elif t is bool: result = dtBool
  elif t is float: result = dtFloat
  elif t is Time: result = dtTime
  elif t is SQLBinaryData: result = dtBinary
  else:
    result = dtNull
    raise newODBCUnsupportedTypeException($t.name)

proc toSQLColumnType*(t: typeDesc): SQLColType =
  # convert type to columntype
  # NOTE: This assumes string is unicode!
  if t is string: result = ctString
  elif t is int: result = ctInt
  elif t is int64: result = ctBigInt
  elif t is bool: result = ctBit
  elif t is float: result = ctFloat
  elif t is Time or t is TimeInterval: result = ctTime
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
  else:
    result = dtNull
    # todo:
    # dtFixedBinary, dtBinary, dtBigInt, dtYearInterval, dtMonthInterval, dtDayInterval, dtSecondInterval, dtGUID
    raise newODBCUnsupportedTypeException($dataType)

proc toCType*(sqlType: SQLDataType): TSqlSmallInt =
  case sqlType
  of dtNull: result = SQL_TYPE_NULL
  of dtString: result = SQL_WCHAR
  #of dtString: result = SQL_UNICODE #SQL_C_CHAR
  of dtInt: result = SQL_C_SLONG
  of dtInt64: result = SQL_C_SBIGINT
  of dtBool: result = SQL_C_BIT
  of dtFloat: result = SQL_C_DOUBLE
  of dtTime: result = SQL_C_TYPE_TIMESTAMP
  of dtBinary: result = SQL_C_BINARY

# TODO: This is broken
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
  elif t is Time or t is TimeInterval: result = SQL_C_TYPE_TIMESTAMP
  elif t is SQLBinaryData: result = SQL_C_BINARY
  else:
    result = SQL_TYPE_NULL
    raise newODBCUnsupportedTypeException($t.name)

proc toSqlType*(t: typeDesc): TSqlSmallInt =
  if t is string: result = SQL_WCHAR
  elif t is int: result = SQL_INTEGER
  elif t is int64: result = SQL_BIGINT
  elif t is bool: result = SQL_BIT
  elif t is float: result = SQL_FLOAT
  elif t is Time or t is TimeInterval: result = SQL_TYPE_TIMESTAMP
  elif t is SQLBinaryData: result = SQL_BINARY
  else:
    result = SQL_UNKNOWN_TYPE
    raise newODBCUnsupportedTypeException($t.name)

proc toSqlType*(dataType: SQLDataType): TSqlSmallInt =
  case dataType
  of dtNull: result = SQL_UNKNOWN_TYPE
  of dtString: result = SQL_WCHAR
  of dtInt: result = SQL_INTEGER
  of dtInt64: result = SQL_BIGINT
  of dtBool: result = SQL_BIT
  of dtFloat: result = SQL_FLOAT
  of dtTime: result = SQL_C_TIMESTAMP
  of dtBinary: result = SQL_BINARY

proc toSqlType*(data: SQLData): TSqlSmallInt =
  result = data.kind.toSqlType

proc initSQLData*(kind: SQLDataType = dtNull): SQLData = SQLData(kind: kind)

proc initSQLData*[T](inputData: T): SQLData =
  when T is int:
    SQLData(kind: dtInt, intVal: inputData)
  elif T is int64:
    SQLData(kind: dtInt64, int64Val: inputData)
  elif T is string:
    SQLData(kind: dtString, strVal: inputData)
  elif T is bool:
    SQLData(kind: dtBool, boolVal: inputData)
  elif T is float:
    SQLData(kind: dtFloat, floatVal: inputData)
  elif T is TimeInterval:
    SQLData(kind: dtTime, timeVal: inputData)
  elif T is Time:
    SQLData(kind: dtTime, timeVal: inputData.toTimeInterval)
  elif T is SQLBinaryData:
    SQLData(kind: dtBinary, binVal: inputData)
  else:
    raise newODBCUnsupportedTypeException($T.name)

proc kind*(data: SQLData): SQLDataType {.inline.} = data.kind

proc `kind=`*(data: var SQLData, kind: SQLDataType) =
  data.reset
  data.kind = kind

proc `$`*(sqlData: SQLData): string =
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
  result &= " (" & $sqlData.kind & ")"

proc asInt*(sqlData: SQLData): int =
  case sqlData.kind
  of dtNull: result = 0
  of dtString:
    try:
      result = sqlData.strVal.parseInt
    except:
      raise newODBCException("error converting to int from string value \"" & sqlData.strVal & "\"")
  of dtInt: result = sqlData.intVal
  of dtInt64:
    if sqlData.int64Val > int.high:
      raise newODBCOverflowException("64 bit value " & $sqlData.int64Val & " is too large to fit into int")
    else:
      result = sqlData.int64Val.int
  of dtBool: result = if sqlData.boolVal: 1 else: 0
  of dtFloat: result = sqlData.floatVal.int
  of dtTime: raise newODBCException("cannot transform " & sqlData.timeVal.type.name & " to int")
  of dtBinary: raise newODBCException("cannot transform binary to int")

proc asInt64*(sqlData: SQLData): int64 =
  case sqlData.kind
  of dtNull: result = 0
  of dtString:
    try:
      # TODO: This will only work on 64 bit systems AFAICT :/
      result = sqlData.strVal.parseBiggestInt
    except:
      raise newODBCException("error converting to int64 from string value \"" & sqlData.strVal & "\"")
  of dtInt:
    result = sqlData.intVal.int64
  of dtInt64:
    result = sqlData.int64Val
  of dtBool: result = if sqlData.boolVal: 1 else: 0
  of dtFloat: result = sqlData.floatVal.int64
  of dtTime: raise newODBCException("cannot transform " & sqlData.timeVal.type.name & " to int64")
  of dtBinary: raise newODBCException("cannot transform binary to int64")

proc asFloat*(sqlData: SQLData): float =
  case sqlData.kind
  of dtNull: result = 0.0
  of dtString:
    try:
      result = sqlData.strVal.parseFloat
    except:
      raise newODBCException("error converting to float from string value \"" & sqlData.strVal & "\"")
  of dtInt: result = sqlData.intVal.float
  of dtInt64: result = sqlData.int64Val.float
  of dtBool: result = if sqlData.boolVal: 1.0 else: 0.0
  of dtFloat: result = sqlData.floatVal
  of dtTime: raise newODBCException("cannot transform " & sqlData.timeVal.type.name & " to int")
  of dtBinary: raise newODBCException("cannot transform binary to int")

proc asBool*(sqlData: SQLData): bool =
  case sqlData.kind
  of dtNull: result = false
  of dtString:
    try:
      result = sqlData.strVal.parseBool
    except:
      raise newODBCException("error converting to bool from string value \"" & sqlData.strVal & "\"")
  of dtInt: result = sqlData.intVal != 0
  of dtInt64: result = sqlData.int64Val != 0
  of dtBool: result = sqlData.boolVal
  of dtFloat: result = sqlData.floatVal != 0.0
  of dtTime: raise newODBCException("cannot transform " & sqlData.timeVal.type.name & " to bool")
  of dtBinary: raise newODBCException("cannot transform binary to int")

proc asString*(sqlData: SQLData): string =
  case sqlData.kind
  of dtNull: result = nullValue
  of dtString: result = sqlData.strVal
  of dtInt: result = $sqlData.intVal
  of dtInt64: result = $sqlData.int64Val
  of dtBool: result = $sqlData.boolVal
  of dtFloat: result = $sqlData.floatVal
  of dtTime: result = $sqlData.timeVal
  of dtBinary: result = $sqlData.binVal

proc asBinary*(sqlData: SQLData): SQLBinaryData =
  result = @[]
  case sqlData.kind
  of dtNull: discard
  of dtString:
    var cs = sqlData.strVal.cstring
    for c in cs:
      result.add(c.byte)
  of dtInt:
    var buf = cast[array[int.sizeOf, byte]](sqlData.intVal)
    for b in buf:
      result.add(b)
  of dtInt64:
    var buf = cast[array[int64.sizeOf, byte]](sqlData.int64Val)
    for b in buf:
      result.add(b)
  of dtBool: result.add(sqlData.boolVal.byte)
  of dtFloat:
    var buf = cast[array[float.sizeOf, byte]](sqlData.floatVal)
    for b in buf:
      result.add(b)
  of dtTime: raise newODBCException("cannot transform " & sqlData.timeVal.type.name & " to binary")
  of dtBinary:
    for b in sqlData.binVal: result.add(b)

proc asTimeInterval*(sqlData: SQLData): TimeInterval =
  if sqlData.kind == dtTime: result = sqlData.timeVal
  else: raise newODBCException("cannot transform " & $sqlData.kind & " to time")

proc isNull*(sqlData: SQLData): bool = sqlData.kind == dtNull

converter toBinary*(sqlData: SQLData): SQLBinaryData = sqlData.asBinary
converter toString*(sqlData: SQLData): string = sqlData.asString
converter toFloat*(sqlData: SQLData): float = sqlData.asFloat
converter toBool*(sqlData: SQLData): bool = sqlData.asBool
converter toInt*(sqlData: SQLData): int = sqlData.asInt
converter toInt64*(sqlData: SQLData): int64 = sqlData.asInt64
converter toTimeInterval*(sqlData: SQLData): TimeInterval = sqlData.asTimeInterval

proc `$`*(handle: SqlHStmt): string = handle.repr


