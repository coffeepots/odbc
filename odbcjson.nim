import odbcfields, odbctypes, json, unicode, tables

proc toJson*(data: SQLData): JsonNode =
  case data.kind:
    of dtNull:
      result.kind = JNull
    of dtString:
      result = % $(data.strVal)
    of dtInt:
      result = %data.intVal
    of dtInt64:
      result = %data.int64Val # not sure this will work on 32 bit! :-|
    of dtBool:
      result = %data.boolVal
    of dtFloat:
      result = %data.floatVal
    of dtTime:
      let s = $data.timeVal
      result = %s
    of dtBinary:
      let s = $data.binVal # :(
      result = %s

proc toJson*(value: SQLValue): JsonNode =
  ## Convert an SQLValue to JSON using the json unit.
  result = newJObject()
  # Initialize empty sequence with expected field tuples.
  var s = initOrderedTable[string, JsonNode]()
  # Add the fieldname tuple to the sequence of values.
  s.add("field", newJString(value.field.fieldname))
  # Add the string field tuple to the sequence of values.
  try:
    #echo "* ", value.data.toJson
    # note: issue which crashes json: odbc converting to weird unicode?
    s.add("data", value.data.toJson)
  except:
    let e = getCurrentException()
    if e != nil:
      echo "JSON conversion raised exception: " & e.msg & " for row: ", repr(result)
    else:
      echo "JSON conversion raised an exception but the details could not be retrieved for row: ", repr(result)
  result.fields = s

proc toJson*(row: SQLRow): JsonNode =
  result = newJArray()
  for dataItem in row:
    result.elems.add(dataItem.toJson)

proc toJson*(results: SQLResults): JsonNode =
  var fields = initTable[int,string]()
  result = newJArray()
  for k,v in results.fieldTable:
    fields.add(v,k)
  for row in results.rows:
    var js = %*{}
    for idx,fld in row:
      js.add(fields[idx],fld.toJson())
    result.add(js)



