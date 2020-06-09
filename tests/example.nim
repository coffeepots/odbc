import odbc

#[
  A small example to ask SQL to generate the next days of the week from today,
  and demonstrate parameter passing.
  No tables are accessed.
]# 

var con = newODBCConnection()

con.host = r"localhost\SQLEXPRESS"
con.driver = "SQL Server Native Client 11.0"
con.database = ""

con.integratedSecurity = true

con.reporting.level = rlErrorsAndInfo
con.reporting.destinations = {rdEcho}

doAssert con.connect(), "Cannot connect"

for i in 0 ..< 10:
  echo con.dbq("SELECT DATENAME(WEEKDAY, DATEADD(DAY, ?i, GETDATE()))", ("i", i))
  echo con.dbq("SELECT ?i + ?v", ("i", i), ("v", 24.4))
