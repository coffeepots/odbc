import ../odbc, wininifiles, times, os, unittest, json
from math import pow
from strutils import repeat

var
  iniSettings: IniFile
  iniPath = getCurrentDir().joinPath("DBConnect.ini")

#[

Below we load database properties from an inifile.
You can set these properties manually for your own database,
or if you want to use the wininifiles module, here is an example layout:

  [Database]
  HostName=MyDBHost
  Database=MyDatabaseName
  UserName=MyUsername
  Password=MyPassword
  WinAuth=0

Any database with access to run queries and create temporary tables can
be used to run these tests.

]#

if not iniPath.fileExists: iniPath = getCurrentDir().joinPath("tests").joinPath("DBConnect.ini")
if not iniSettings.loadIni(iniPath):
  echo "Could not find ini file ", iniSettings.filename
  quit()

var connections = newSeq[tuple[name: string, connection: ODBCConnection]]()

var sqlServer = newODBCConnection()
sqlServer.host = iniSettings.find("database", "hostname")
sqlServer.driver = "SQL Server Native Client 11.0"
sqlServer.database = iniSettings.find("database", "database")
sqlServer.userName = iniSettings.find("database", "username")
sqlServer.password = iniSettings.find("database", "password")
sqlServer.integratedSecurity = iniSettings.find("database", "winauth") == "1"
sqlServer.reporting.level = rlErrorsAndInfo
sqlServer.reporting.destinations = {rdStore, rdEcho}
connections.add ("SqlServer", sqlServer)

var drillCon = newODBCConnection(server=ApacheDrill)
drillCon.driver = "/opt/mapr/drill/lib/64/libdrillodbc_sb64.so"
drillCon.host = iniSettings.find("database", "hostname")
drillCon.port = 31010
drillCon.integratedSecurity = false
drillCon.database = iniSettings.find("database", "database")
drillCon.userName = iniSettings.find("database", "username")
drillCon.password = iniSettings.find("database", "password")
drillCon.authenticationType = "Plain"
drillCon.connectionType = "Direct"
drillCon.zkClusterID = "drillbits1"
drillCon.reporting.level = rlErrorsAndInfo
drillCon.reporting.destinations = {rdStore}
connections.add ("Apache Drill", drillCon)

for conDetails in connections.mitems:
  template con: untyped = conDetails.connection
  var
    qry = newQuery(con)
    res: SQLResults
  echo "Connecting to ", con.getConnectionString()
  if not con.connect:
    echo "Could not connect to ", conDetails.name
    continue
  else:
    echo "Success, connected to host \"" & con.host & "\", database \"" & con.database & "\""

  suite "Utilities":
    test "List drivers":
      # Can't be automatically checked.
      echo con.listDrivers

  suite "Parameter tests (" & conDetails.name & ")":

    test "Simple parameters":
      qry.statement = "SELECT ?b+?c, ?a"
      qry.params["a"] = 1
      qry.params["b"] = 2
      qry.params["c"] = 3
      res = qry.executeFetch
      check res[0][0].intVal == 5
      check res[0][1].intVal == 1

    test "Accessing field data":
      qry.statement = "SELECT 123 as data1, 456 as data2"
      res = qry.executeFetch
      let data = res.data("data1")
      check data == 123
      # Alternate access through `fields`.
      check res.data(res.fields(0)) == 123
      check res.data(res.fields(1)) == 456
      # Access through column and row index.
      check res.data(0, 0) == 123
      check res.data(1) == 456

    test "Conditional field access":
      qry.statement = "SELECT 123 as data"
      res = qry.executeFetch
      var data: SQLData
      check res.tryData("data", data)
      check data == 123
      data.reset
      check not res.tryData("doota", data)
      check data.kind == dtNull

    test "Times":
      let curTime = getTime()
      qry.statement = "SELECT ?a"
      qry.params["a"] = curTime
      res = qry.executeFetch
      let timeVal = res[0][0].timeVal
      let curTimeInterval =
        when defined(rawTimes):
          curTime.toTimeInterval
        else:
          # toTimeInterval doesn't set up milli/microseconds for us so for comparison we do this manually.
          curTime.toTimeInterval.distributeNanoseconds
      echo curTimeInterval
      # Here we can directly compare to the nanosecond level because the date time value
      # is not being converted and so does not lose precision.
      check timeVal == curTimeInterval

    test "Nulls":
      qry.statement = "SELECT ?a"
      qry.params["a"] = "test"
      qry.params.clear("a")
      res = qry.executeFetch
      check res[0][0].isNull

    test "Large ordinal parameter types":
      # Note that when selecting, ODBC returns SQL_DECIMAL as the type of a large number.
      # This means that the value will be returned as a FLOAT.
      qry.statement = "SELECT 4611686018427387904"
      res = qry.executeFetch
      check res[0][0] == 4611686018427387904.0
      qry.statement = "SELECT CAST(4611686018427387904 AS BigInt)"
      res = qry.executeFetch
      check res[0][0] == 4611686018427387904
      qry.statement = "SELECT ?a"
      const testData = pow(2.float64, 68.float64)
      qry.params["a"] = testData
      res = qry.executeFetch
      check res[0][0] == testData

    test "Duplicate Parameters":
      qry.statement = "SELECT ?a, ?a+?a, ?a*?a"
      qry.params["a"] = 10
      res = qry.executeFetch
      check res[0][0].intVal == 10
      check res[0][1].intVal == 20
      check res[0][2].intVal == 100

    test "Insert and Read":
      qry.statement = """
      SET NOCOUNT ON
      CREATE TABLE #Temp (Name Varchar(255), textval varchar(255), intval int, timeval DateTime2, boolval bit)
      INSERT INTO #Temp VALUES (?name, ?textval, ?intval, ?timeval, ?boolval)
      SELECT * FROM #Temp
      DROP TABLE #Temp
      """
      let curTime = 
        when defined(rawTimes):
          getTime().toTimeInterval
        else:
          # toTimeInterval doesn't set up milli/microseconds for us so for comparison we do this manually.
          getTime().toTimeInterval.distributeNanoseconds
      qry.params["name"] = "testy"
      qry.params["textval"] = " testing "
      qry.params["intval"] = 99
      qry.params["boolval"] = true
      qry.params["timeval"] = curTime

      var res = qry.executeFetch

      check res[0].len == 5
      check res[0][0] == "testy"
      check res[0][1] == " testing "
      check res[0][2] == 99
      # Depending on the database's precision, milliseconds and below may be zero or
      # a slightly different value to the one passed to the query. In this case a direct
      # comparison will fail. Set the following to `false` to perform checks up to the 
      # nearest second.
      when true:
        # Comparison up to nanosecond precision.
        check res[0][3].timeVal == curTime
      else:
        # Checking up to one second difference from input.
        let timeDiff = curTime - res[0][3].timeVal
        check:
          timeDiff.years == 0 and timeDiff.months == 0 and timeDiff.weeks == 0
          timeDiff.days == 0 and timeDiff.minutes == 0 and timeDiff.seconds < 1

      check res[0][4] == true

    test "Volume Parameters":
      qry.statement = "SELECT ?id"
      var row: SQLRow
      for i in 0..100:
        qry.params["id"] = i
        qry.open
        check qry.fetchRow(row)
        check row[0] == i
        qry.close

    test "Empty string parameters":
      qry.statement = "SELECT '*'+?a+'*', '*'+?b+'*'"
      qry.params["a"] = ""  # NOTE: This gets converted to " " :(
      qry.params["b"] = "test"
      var res = qry.executeFetch
      # note: to allow the empty (zero length) C string to be bound,
      # we need to simply bind it to a parameter of at lease size 1.
      # this is an issue in many database implementations
      check res[0][0] == "* *"
      check res[0][1] == "*test*"  # converted due to weirdness

    test "Binary parameters":
      qry.statement = "SELECT ?a"
      var binData: SQLBinaryData = @[]
      binData.setLen(100)
      for i in 0..<100: binData[i] = i.byte
      qry.params["a"] = binData
      let
        res = qry.executeFetch[0][0]
        bin = res.binVal
      for i in 0..<100:
        check bin[i] == i.byte
      check bin == res.asBinary
      
  suite "Unicode":
    test "Read/write unicode":
      qry.statement = """
      SET NOCOUNT ON
      CREATE TABLE #Temp (Name Varchar(255), textval nvarchar(255), intval int)
      INSERT INTO #Temp VALUES ('Test', 'Some text', 1)
      INSERT INTO #Temp VALUES ('ABCD', 'More text', 1)
      INSERT INTO #Temp VALUES ('CJK Compatibility', N'More unicode ㌀㌁㌂㌃㌄㌅㌆㌇ ㌈ end of more unicode', 3)
      INSERT INTO #Temp VALUES ('An example of ideographs', N'Unicode 豈更車賈滑串句龜龜 契 end of unicode', 2)
      SELECT * FROM #Temp WHERE Name LIKE ?name
      DROP TABLE #Temp
      """
      qry.params["name"] = "%a%"
      var res = qry.executeFetch
      # the first row is omitted due to the like constraint.
      check res[0][0] == "ABCD"
      check res[0][1] == "More text"
      check res[0][2] == 1
      check res[1][0] == "CJK Compatibility"
      check res[1][1] == "More unicode ㌀㌁㌂㌃㌄㌅㌆㌇ ㌈ end of more unicode"
      check res[1][2] == 3
      check res[2][0] == "An example of ideographs"
      check res[2][1] == "Unicode 豈更車賈滑串句龜龜 契 end of unicode"
      check res[2][2] == 2

    test "Long strings":
      let longStr = "1234567890".repeat(100)
      qry.statement = "SELECT '" & longStr & "' AS LongStr"
      check qry.executeFetch[0][0] == longStr
      qry.statement = "SELECT ?a AS LongStr"
      qry.params["a"] = longStr
      check qry.executeFetch[0][0] == longStr

  suite "Misc":
    test "Transactions":
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
      check qry.executeFetch[0][0] == "test"
      con.commitTrans

    test "Json":
      qry.statement = """
        SET NOCOUNT ON
        CREATE TABLE #Temp (intval int, textval nvarchar(255))
        INSERT INTO #Temp VALUES (1, N'test1ਵ')
        SELECT * FROM #Temp
        """
      let jsonNode = qry.executeFetch.toJson
      check jsonNode[0]["intval"].kind == JInt
      check jsonNode[0]["intval"].getInt == 1
      check jsonNode[0]["textval"].kind == JString
      check jsonNode[0]["textval"].getStr == "test1ਵ"
    
    test "withExecuteByField":
      qry.statement = "SELECT 'A' AS A, 5 AS B, 0.8 AS C"
      qry.withExecuteByField:
        if fieldIdx == 0:
          check field.fieldname == "A"
          check field.dataType == dtString
          check data == "A"
        if fieldIdx == 1:
          check field.fieldname == "B"
          check field.dataType == dtint
          check data == 5
        if fieldIdx == 2:
          check field.fieldname == "C"
          check field.dataType == dtFloat
          check data == 0.8

    test "withExecute":
      qry.statement = "SELECT 'A' AS A, 5 AS B, 0.8 AS C"

      qry.withExecute(row):
        check row.len == 3

        check qry.fields(0).dataType == dtString
        check qry.fields(0).fieldname == "A"
        check row[0] == "A"

        check qry.fields(1).dataType == dtInt
        check qry.fields(1).fieldname == "B"
        check row[1] == 5

        check qry.fields(2).dataType == dtFloat
        check qry.fields(2).fieldname == "C"
        check row[2] == 0.8

    test "FieldByName":
      con.timeout = 10
      qry = newQuery(con)
      qry.statement = "SELECT ?test as StrCol, ?a, ?b, ?a+?b"
      qry.params["a"] = 1
      qry.params["b"] = 4
      qry.params["test"] = "test string"
      qry.withExecute(row):
        check row[0] == "test string"
        check row[qry.fieldIndex("StrCol")] == "test string"

  suite "Conversions":
    test "From text":
      var inStr = "Hello"
      qry.statement = "SELECT ?a"
      qry.params["a"] = inStr
      res = qry.executeFetch
      var bres = res[0][0].asBinary
      check bres[0] == 72
      check bres[1] == 101
      check bres[2] == 108
      check bres[3] == 108
      check bres[4] == 111
    test "From int":
      var inNum = 123456
      qry.params["a"] = inNum
      res = qry.executeFetch
      var bres2 = res[0][0].asBinary
      check bres2[0] == 64
      check bres2[1] == 226
      check bres2[2] == 1
      check bres2[3] == 0
      check res[0][0].asString == "123456"
      check res[0][0].asInt == 123456
      check res[0][0].asInt64 == 123456
      check res[0][0].asFloat == 123456.0
      #
    test "From int64":
      var inNum: int64 = 1234567890123456789
      qry.params["a"] = inNum
      res = qry.executeFetch
      var bres2 = res[0][0].asBinary
      check bres2[0] == 21
      check bres2[1] == 129
      check bres2[2] == 233
      check bres2[3] == 125
      check bres2[4] == 244
      check bres2[5] == 16
      check bres2[6] == 34
      check bres2[7] == 17
      check res[0][0].asString == "1234567890123456789"
      check res[0][0].asInt64 == 1234567890123456789
      check res[0][0].asFloat == 1234567890123456789.0
      #
    test "From text number":
      var inStr = "123456"
      qry.params["a"] = inStr
      res = qry.executeFetch
      check res[0][0].asInt == 123456
      check res[0][0].asInt64 == 123456
      check res[0][0].asFloat == 123456.0

