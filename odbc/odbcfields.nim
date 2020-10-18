import odbcsql, odbctypes, tables, odbcerrors, times
from strutils import toLowerAscii

type
  SQLFieldObj = object
    fieldname*: string
    tablename*: string
    # add some metadata where appropriate
    dataType*: SQLDataType
    colType*: SQLColType
    sqlType*: TSqlSmallInt
    cType*: TSqlSmallInt
    size*: int
    digits*: int
    nullable*: bool
    colIndex: int # Only used for results, allows quick data lookup directly from field

  SQLField* = ref SQLFieldObj

  SQLValue* = object
    field*: SQLField
    data*: SQLData

  SQLResults* = object
    fieldnameIndex*: FieldIdxs
    colFields*: seq[SQLField]
    rows*: seq[SQLRow]
    curRow*: int

proc `[]`*(results: SQLResults, index: int): SQLRow =
  result = results.rows[index]

proc add*(results: var SQLResults, row: SQLRow) =
  results.rows.add(row)

proc fieldCount*(results: SQLResults): int = results.colFields.len

proc fields*(results: SQLResults, index: int): SQLField = results.colFields[index]

iterator fields*(results: SQLResults): SQLField =
  var idx: int
  while idx < results.colFields.len:
    yield results.colFields[idx]
    idx.inc

iterator items*(results: SQLResults): SQLRow =
  for idx in 0 ..< results.rows.len:
    yield results.rows[idx]

proc fields*(results: SQLResults, fieldName: string): SQLField =
  let idx = results.fieldnameIndex.getOrDefault(fieldName.toLowerAscii, -1)
  if idx >= 0:
    results.colFields[idx]
  else:
    raise newODBCUnknownFieldException("Fieldname not found \"" & fieldName & "\"")

## Fetch the column index for `fieldName`. Raises an exception if the field can't be found.
proc fieldIndex*(results: SQLResults, fieldName: string): int =
  result = results.fieldnameIndex.getOrDefault(fieldName.toLowerAscii, -1)
  if result < 0:
    raise newODBCUnknownFieldException("field \"" & fieldName & "\" not found in results")

proc len*(results: SQLResults): int = results.rows.len

proc index*(results: SQLResults, name: string): int =
  result = results.fieldnameIndex[name]

proc tryData*(results: SQLResults, fieldName: string, rowIdx: int, value: var SQLData): bool {.inline.} =
  ## Populates `value` if the field exists and returns true. If the field can't be found, returns false.
  ## If `rowIdx` < 0, `results`.curRow is used.
  let idx = results.fieldnameIndex.getOrDefault(fieldName.toLowerAscii, -1)
  if idx < 0: return
  if rowIdx < 0:
    value = results[results.curRow][idx]
  else:
    value = results[rowIdx][idx]
  true

## Populates `value` from the current row if the field exists and returns true, otherwise returns false.
template tryData*(results: SQLResults, fieldName: string, value: var SQLData): bool = results.tryData(fieldName, results.curRow, value)

## Check by name to see if a result set contains a field.
proc hasField*(results: SQLResults, fieldName: string): bool = results.fieldnameIndex.getOrDefault(fieldName.toLowerAscii, -1) != -1

proc data*(results: SQLResults, fieldName: string, rowIdx: int = -1): SQLData =
  ## Return the SQLData associated with a field and row in `results`.
  ## If the field can't be found, an exception is raised.
  if not results.tryData(fieldName, rowIdx, result):
    raise newODBCUnknownFieldException("Fieldname not found \"" & fieldName & "\"")

proc data*(results: SQLResults, field: SQLField): SQLData =
  result = results[results.curRow][field.colIndex] 

proc data*(results: SQLResults, field: SQLField, rowIndex: int): SQLData =
  result = results[rowIndex][field.colIndex] 

template data*(results: SQLResults, columnIndex: Natural): SQLData = results[results.curRow][columnIndex]
template data*(results: SQLResults, columnIndex: Natural, rowIndex: Natural): SQLData = results[rowIndex][columnIndex]

template fromField*(results: SQLResults, name: string, rowIdx = -1): SQLData {.deprecated: "use `data` instead".} = results.data(name, rowIdx)

proc initSQLRow*: SQLRow =
  result = @[]

proc initSQLResults*: SQLResults =
  result.rows = @[]
  result.colFields = @[]
  result.fieldnameIndex = initFieldIdxs()

proc next*(results: var SQLResults) =
  if results.curRow < results.rows.len: results.curRow += 1

proc prev*(results: var SQLResults) =
  if results.curRow > 0: results.curRow -= 1

proc newSQLField*(tablename: string = "", fieldname: string = "", colIndex = 0): SQLField =
  new(result)
  result.tablename = tablename
  result.fieldname = fieldname
  # set field type to default
  result.cType = SQL_C_DEFAULT
  result.colIndex = colIndex

proc setType*[T](field: var SQLField, data: T) =
  field.colType = toSQLColumnType(T)
  field.dataType = field.colType.toDataType
  field.sqlType= toSqlType(T)
  field.cType = toCType(T)
  when T is string or T is SQLBinaryData:
    field.size = data.len
  elif T is DateTime or T is TimeInterval:
    # See: https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/column-size-decimal-digits-transfer-octet-length-and-display-size
    # 20 + s (the number of characters in the yyyy-mm-dd hh:mm:ss[.fff...] format, where s is the seconds precision).
    field.size = 27
    # number of digits to the right of the decimal point in the seconds part 
    field.digits = 7
  else:
    field.size = T.sizeOf

proc initSQLValue*(dataType: SQLDataType): SQLValue =
  result.field = newSQLField()
  result.field.dataType = dataType
  result.data = initSQLData(dataType)

proc `$`*(field: SQLField): string =
  result = ""
  if field != nil:
    result &= "tablename : \""
    if field.tablename != "":
      result &= field.tablename
    else:
      result &= "<unknown>"
    result &= "\""

    result &= " fieldname: \""
    if field.fieldname != "":
      result &= field.fieldname
    else:
      result &= "<unknown>"
    result &= "\", "

    result &= "column type: " & $field.colType & ", "
    result &= "data type: " & $field.dataType & ", "
    result &= "size: " & $field.size & ", "
    result &= "digits: " & $field.digits & ", "
    result &= "nullable: " & $field.nullable
  else:
    result = "<nil>"

proc `$`*(sqlValue: SQLValue): string =
  result = "field: [" & $sqlValue.field & "]"
  result &= " data: " & $sqlValue.data

proc `$`*(sqlRow: SQLRow): string =
  result = "["
  for idx, item in sqlRow:
    result &= $item
    if idx < sqlRow.high: result &= ",\n"
  result &= "]"

proc `$`*(sqlResults: SQLResults): string =
  result = "["
  for idx, row in sqlResults.rows:
    result &= $row
    if idx < sqlResults.rows.high: result &= ",\n"
  result &= "]"

proc isNull*(sqlVal: SQLValue): bool = sqlVal.data.kind == dtNull
