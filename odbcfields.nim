import odbcsql, odbctypes

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

  SQLRow* = seq[SQLValue]
  SQLResults* = seq[SQLRow]

proc initSQLRow*: SQLRow =
  result = @[]

proc initSQLResults*: SQLResults =
  result = @[]

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
  for row in sqlResults:
    result &= $row & "\n"
  result &= "]"

from strutils import cmpIgnoreCase

proc byFieldname*(row: SQLRow, fieldname: string): SQLData =
  for item in row:
    if item.field.fieldname.cmpIgnoreCase(fieldname) == 0:
      result = item.data
      break

proc isNull*(sqlVal: SQLValue): bool = sqlVal.data.kind == dtNull
