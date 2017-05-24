import odbcsql, odbctypes, tables, strutils, odbcerrors

type
  SQLFieldObj = object
    fieldname*: string
    tablename*: string
    # add some metadata where appropriate
    dataType*: SQLDataType
    colType*: SQLColType
    rawSqlType*: TSqlSmallInt
    cType*: TSqlSmallInt
    size*: int
    digits*: int
    nullable*: bool

  SQLField* = ref SQLFieldObj

  SQLValue* = object
    field*: SQLField
    data*: SQLData

  SQLRow* = seq[SQLData]
  SQLResults* = object
    fieldTable: Table[string, int]
    rows*: seq[SQLRow]
    curRow*: int

proc `[]`*(results: SQLResults, index: int): SQLRow =
  result = results.rows[index]

proc add*(results: var SQLResults, row: SQLRow) =
  results.rows.add(row)

proc field*(results: var SQLResults, fieldName: string, rowIdx: int = -1): SQLData =
  # table.withValue(key, value) do:
  results.fieldTable.withValue(fieldName.toLowerAscii, fldIdx) do:
    # value found
    if rowIdx < 0:
      result = results[results.curRow][fldIdx[]] 
    else:
      result = results[rowIdx][fldIdx[]]
  do:
    # fieldname not found
    raise newODBCUnknownFieldException("Fieldname not found \"" & fieldName & "\"")

proc initSQLRow*: SQLRow =
  result = @[]

proc initSQLResults*: SQLResults =
  result.rows = @[]
  result.fieldTable = initTable[string, int]()

proc next*(results: var SQLResults) =
  if results.curRow < results.rows.len: results.curRow += 1

proc prev*(results: var SQLResults) =
  if results.curRow > 0: results.curRow -= 1

proc newSQLField*(tablename: string = "", fieldname: string = ""): SQLField =
  new(result)
  result.tablename = tablename
  result.fieldname = fieldname
  # set field type to default
  result.cType = SQL_C_DEFAULT

proc setType*[T](field: var SQLField, data: T) =
  field.colType = toSQLColumnType(T)
  field.dataType = field.colType.toDataType
  field.rawSqlType = toRawSQLType(T)
  field.cType = toCType(T)
  when T is string or T is SQLBinaryData:
    field.size = data.len
  else:
    field.size = T.sizeOf

proc initSQLValue*(dataType: SQLDataType): SQLValue =
  result.field = newSQLField()
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
    if idx < sqlRow.len - 1: result &= "\n"
  result &= "]"

proc `$`*(sqlResults: SQLResults): string =
  result = "["
  for row in sqlResults.rows:
    result &= $row & "\n"
  result &= "]"

#from strutils import cmpIgnoreCase

proc isNull*(sqlVal: SQLValue): bool = sqlVal.data.kind == dtNull
