import ../odbc.nim, wininifiles, times, os

var
  con = newODBCConnection()
  iniSettings: IniFile
if not iniSettings.loadIni(getCurrentDir().joinPath("DBConnect.ini")):
  echo "Could not find ini file ", iniSettings.filename
  quit()

# Here we load database properties from an inifile.
# You can set these properties manually, or if you want to use
# the wininifiles module, here is an example layout:
#
# [Database]
# HostName=MyDBHost
# Database=MyDatabaseName
# UserName=MyUsername
# Password=MyPassword
# WinAuth=0

con.host = iniSettings.find("database", "hostname")
con.driver = "SQL Server Native Client 11.0"
con.database = iniSettings.find("database", "database")
con.userName = iniSettings.find("database", "username")
con.password = iniSettings.find("database", "password")
con.integratedSecurity = iniSettings.find("database", "winauth") == "1"
con.reporting.level = rlErrorsAndInfo
con.reporting.destinations = {rdStore, rdEcho}
echo "DSN: ", con.getConnectionString

if not connect(con):
  echo "Could not connect to database."
  quit()
echo "Connected to host \"" & con.host & "\", database \"" & con.database & "\""

type
  TestMode = enum testSimpleParams, testNulls, testListDrivers, testInsert, testWithExecute, testDuplicateParams,
    testVolumeParams, testEmptyStrings, testBinary, testDateTime, testInt64, testUnicodeStrings,
    testLongStrings, testTransactions, testJson, testFieldByName, testConversions, testDestruction
  TestModes = set[TestMode]

const tests: TestModes = {testSimpleParams, testNulls, testListDrivers, testInsert, testWithExecute, testDuplicateParams,
    testEmptyStrings, testBinary, testDateTime, testInt64, testUnicodeStrings,
    testLongStrings, testTransactions, testJson, testFieldByName, testConversions}

var
  qry = newQuery(con)
  res: SQLResults

when testSimpleParams in tests:
  echo "Simple parameter tests:"
  qry.statement = "SELECT ?b+?c, ?a"
  qry.params["a"] = 1
  qry.params["b"] = 2
  qry.params["c"] = 3
  echo qry.statement, " a=", $qry.params["a"].data, " b=", $qry.params["b"].data, " c=", $qry.params["c"].data
  res = qry.executeFetch
  echo " result: ", res
  assert(res[0][0].intVal == 5)
  assert(res[0][1].intVal == 1)

when testNulls in tests:
  qry.statement = "SELECT ?a"
  qry.params["a"] = "test"
  qry.params.clear("a")
  echo qry.executeFetch

when testListDrivers in tests:
  echo listDrivers()

when testInt64 in tests:
  # Note that when selecting, ODBC returns SQL_DECIMAL as the type of a large number.
  # This means that the value will be returned as a FLOAT.

  qry.statement = "SELECT 4611686018427387904"
  res = qry.executeFetch
  assert(res[0][0].floatVal == 4611686018427387904.0)
  echo " large int result native (float): ", qry.executeFetch
  qry.statement = "SELECT CAST(4611686018427387904 AS BigInt)"
  res = qry.executeFetch
  assert(res[0][0].int64Val == 4611686018427387904)
  echo " large int as bigint: ", qry.executeFetch
  # Here, we specify a bigint directly (via type check in the param to int64).
  # This means we get an int64 back.
  import math
  qry.statement = "SELECT ?a"
  qry.params["a"] = pow(2.float64, 62.float64)
  res = qry.executeFetch
  echo " large float result as param: ", res
  assert(res[0][0].asFloat == 4611686018427387904.0)

when testDuplicateParams in tests:
  echo "Duplicate parameters test:"
  qry.statement = "SELECT ?a, ?a+?a, ?a*?a"
  qry.params["a"] = 10
  echo qry.statement
  echo " a=", $qry.params["a"].data
  res = qry.executeFetch
  echo " result: \n", res
  assert(res[0][0].intVal == 10)
  assert(res[0][1].intVal == 20)
  assert(res[0][2].intVal == 100)

when testInsert in tests:
  qry.statement = """
  SET NOCOUNT ON
  CREATE TABLE #Temp (Name Varchar(255), textval varchar(255), intval int, timeval DateTime, boolval bit)
  INSERT INTO #Temp VALUES (?name, ?textval, ?intval, ?timeval, ?boolval)
  SELECT * FROM #Temp
  DROP TABLE #Temp
  """
  qry.params["name"] = "testy"
  qry.params["textval"] = " testing "
  qry.params["intval"] = 99
  qry.params["boolval"] = true
  qry.params["timeval"] = getTime()

  echo qry.executeFetch

when testWithExecute in tests:
  qry.statement = """
  SET NOCOUNT ON
  CREATE TABLE #Temp (Name Varchar(255), textval nvarchar(255), intval int)
  INSERT INTO #Temp VALUES ('Test', 'Some text', 1)
  INSERT INTO #Temp VALUES ('ABCD', 'More text', 1)
  INSERT INTO #Temp VALUES ('CJK Compatibility', N'More unicode ㌀ ㌁ ㌂ ㌃ ㌄ ㌅ ㌆ ㌇ ㌈ end of more unicode', 3)
  INSERT INTO #Temp VALUES ('An example of ideographs', N'Unicode 豈 更 車 賈 滑 串 句 龜 龜 契 end of unicode', 2)
  SELECT * FROM #Temp WHERE Name LIKE ?name
  DROP TABLE #Temp
  """
  qry.params["name"] = "%a%"
  var i = 0
  qry.withExecute(row):
    echo "Row: ", i
    for item in row:
      echo "item: ", item
    i += 1

when testVolumeParams in tests:
  echo "Volume param test:"
  qry.statement = "SELECT ?id"
  for i in 0..100_000:
    qry.params["id"] = i
    qry.open
    var row: SQLRow
    discard qry.fetchRow(row)
    qry.close
  echo "done"

when testEmptyStrings in tests:
  echo "Empty strings test:"
  qry.statement = "SELECT '*'+?a+'*', '*'+?b+'*'"
  qry.params["a"] = ""  # NOTE: This gets converted to " " :(
  qry.params["b"] = "test"
  echo qry.executeFetch

when testBinary in tests:
  echo "Binary parameter test:"
  qry.statement = "SELECT ?a"
  var binData: SQLBinaryData = @[]
  binData.setLen(100)
  for i in 0..<100: binData[i] = i.byte
  qry.params["a"] = binData
  echo qry.executeFetch

when testDateTime in tests:
  echo "Datetime test:"
  qry.statement = "SELECT getDate()"
  echo qry.statement & " : "
  qry.withExecute(row):
    for item in row:
      echo item
  qry.statement = "SELECT ?p"
  qry.params["p"] = getTime()
  echo "t to i: ", getTime(), " i = ", toTimeInterval(getTime())
  echo qry.statement & " : "
  qry.withExecute(row):
    for item in row:
      echo item

when testUnicodeStrings in tests:
  echo "Strings test:"
  #qry.statement = "SELECT N'ਵtest1'"
  qry.statement = "SELECT N'ਵtest1'"
  echo "Get unicode from select ", qry.executeFetch
  qry.statement = "SELECT ?a"
  qry.params["a"] = "ਵtest2"
  echo "Read unicode param ", qry.executeFetch

when testLongStrings in tests:
  echo "Long strings test:"
  qry.statement = "SELECT '123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890' AS LongStr"
  echo "Reading long string: ", qry.executeFetch
  qry.statement = "SELECT ?a AS LongStr"
  qry.params["a"] = "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
  echo "Writing long string: ", qry.executeFetch

when testTransactions in tests:
  echo "Transactions test:"
  con.beginTrans
  qry.statement = """
    CREATE TABLE ##Temp (textval varchar(255))
    INSERT INTO ##Temp VALUES (?textval)
    """
  qry.params["textval"] = "test"
  qry.execute
  qry.statement = """
    SELECT * FROM ##Temp
    DROP TABLE ##Temp
  """
  echo qry.executeFetch
  con.commitTrans

when testJson in tests:
  import json
  echo "Json test:"
  qry.statement = """
    SET NOCOUNT ON
    CREATE TABLE #Temp (intval int, textval nvarchar(255))
    INSERT INTO #Temp VALUES (1, N'test1ਵ')
    INSERT INTO #Temp VALUES (2, 'test2')
    INSERT INTO #Temp VALUES (3, 'test3')
    INSERT INTO #Temp VALUES (4, 'test4')
    SELECT * FROM #Temp
    """
  echo qry.executeFetch.toJson.pretty

when testFieldByName in tests:
  echo "Demo test:"
  echo "Setting timeout..."
  con.timeout = 10
  echo "Timeout set to ", con.timeout
  qry = newQuery(con)
  qry.statement = "SELECT ?test as StrCol, ?a, ?b, ?a+?b"
  qry.params["a"] = 1
  qry.params["b"] = 4
  qry.params["test"] = "test string"
  qry.withExecute(row):
    echo row
    var
      data1 = row[0]
      # look up fieldname
      data2 = row[qry.fields("StrCol")]
    echo "d1 ", data1, " d2 ", data2

when testConversions in tests:
  proc testStuff =
    echo "Type conversions test:"
    var inStr = "Hello"
    qry.statement = "SELECT ?a"
    qry.params["a"] = inStr
    res = qry.executeFetch
    echo " \"" & inStr & "\" as Binary: ", res[0][0].asBinary
    var inNum = 123456
    qry.params["a"] = inNum
    res = qry.executeFetch
    echo " " & $inNum & " as Binary: ", res[0][0].asBinary
    echo " " & $inNum & " as String: ", res[0][0].asString
    echo " " & $inNum & " as Int: ", res[0][0].asInt
    echo " " & $inNum & " as Int64: ", res[0][0].asInt64
    echo " " & $inNum & " as Float: ", res[0][0].asFloat
    #
    inStr = "123456"
    qry.params["a"] = inStr
    res = qry.executeFetch
    echo " \"" & inStr & "\" as int: ", res[0][0].asInt
    echo " \"" & inStr & "\" as float: ", res[0][0].asFloat
    echo " \"" & inStr & "\" as int64: ", res[0][0].asInt64
  testStuff()

proc testDestructionProc: ODBCConnection =
  result = newODBCConnection()
  result.host = iniSettings.find("database", "hostname")
  result.driver = "SQL Server Native Client 11.0"
  result.database = iniSettings.find("database", "database")
  result.userName = iniSettings.find("database", "username")
  result.password = iniSettings.find("database", "password")
  result.integratedSecurity = iniSettings.find("database", "winauth") == "1"
  result.reporting.destinations = {rdStore, rdEcho}

when testDestruction in tests:
  echo "Testing automatic freeing of handles:"
  defaultReportingLevel = rlErrors
  for i in 0..10:
    var c = testDestructionProc()
    echo i, " connected: ", c.connect
