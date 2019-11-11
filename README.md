# ODBC for Nim
This module extends the odbcsql wrapper to allow easy access to databases through ODBC.

The primary goal is to give Nim simpler access to Microsoft SQL Server in particular. This module has been tested on SQL Server and Apache Drill, though it should be possible to use any ODBC driver. However, you may need to manually set the connection string as other drivers may use different nomenclature and/or have different capabilities.

Database access consists of two main objects: ODBCConnection and Query.

## Connections

Connections can be initialised as follows:

    var
      con = newODBCConnection()
    con.driver = "SQL Server Native Client 11.0"
    con.host = "ServerAddress"
    con.database = "DatabaseName"

    if not con.connect:
      echo "Could not connect to database."

Connection authentication can be set to "integrated security", which will use your current security context (in Windows), or you can specify a username and password manaully.

    con.integratedSecurity = true # Use your current logged on Windows user to access the database.

or

    con.integratedSecurity = false
    con.userName = "Username"
    con.password = "Password"

For Apache Drill this would be

    var
      con = newODBCConnection(server=ApacheDrill)
    con.driver = "/opt/mapr/drill/lib/64/libdrillodbc_sb64.so"
    con.host = "ServerAddress"
    con.port = 31010
    con.integratedSecurity = false
    con.userName = "Username"
    con.password = "Password"
    con.authenticationType = "Plain"
    con.connectionType = "Direct"

    if not con.connect:
      echo "Could not connect to database.

The connection object also offers some convenience settings such as multipleActiveResultSets, which tells ODBC that you want to be able to run multiple queries at the same time - for instance, if you need to use a lookup query whilst another query is active. This is set to true by default.

To manually set the connection string, you can use the connection's "connectionString" field. When connectionString is not empty, all other connection related settings are ignored and connectionString is used as the connection string.

Connections are cleaned up on program exit, so any open handles should be removed automatically.

#### Unicode and codepages
All ODBC data is requested in widestring and stored internally as UTF-8.

#### Connection messages and error reporting

Connections allow control of errors and information messages via the reporting settings.
This allows you to decide if you only want errors reported, or if want information such as when the active database is changed or the current database language to be reported too. You can also turn off all reporting, though this would mean errors are ignored.
Information that is reported can be set to multiple destinations.

Destination options are:
* rdStore: Stores any messages in the connection under reporting.messages, which is a seq[string].
* rdEcho: Echos messages to the display.
* rdFile: Stores messages in a file. The filename is defined under the connection in reporting.filename.
* rdCallBack: Passes any messages to a custom procedure you may assign to the connection under reporting.callBack

By default, reporting is set to rdEcho.

    con.reporting.level = rlErrorsAndInfo           # Will report any errors and also relevant database information
    con.reporting.destinations = {rdStore, rdEcho}  # Stores in con.reporting.messages and echos them to the display

#### Transactions

Transactions can be controlled using the beginTrans and commitTrans procs, and are performed at the connection level as follows:

    con.beginTrans
    # do work using any query that's set to use this connection
    con.commitTrans

## Queries

Queries are the fundamental mechanism for running SQL.
To create a query, you must pass an existing connection to it:

    var
      qry = newQuery(con)

You can then assign SQL by setting the query's statement:

    qry.statement = "SELECT 1 + 1"

There are a few ways to actually execute the query, which are detailed below.
A query needs to be 'closed' by called the close proc. This frees resources both locally and on the database server.

#### executeFetch

This executes the query and returns the entire result set.

    var results = qry.executeFetch

#### open

This executes the query but does not return the results.
Rows can then be retrieved either one by one with the fetch proc:

    var row: SQLRow
    qry.open
    while qry.fetchRow(row):
      # use row

or in one go with the fetch proc:

    qry.open
    var results = qry.fetch

#### withExecute

This is a template that allows you to process each row, and is equivilent to opening a query and performing fetchRow, then calling close when the query's data has been depleted.

    qry.withExecute(row):
      # the variable, in this case 'row', within the brackets above is declared as SQLRow within the template
      # so there's no need to define it separately. It is then setup with data for you to use for each row.
      for item in row:
        echo item.data

#### execute

This is useful for command type queries where you don't expect a result set. Calling this procedure opens the query and then closes it, ignoring any results.

### Result sets

A result set is defined as the type SQLResults, which contains a sequence of SQLRow and this in turn is a sequence of field data defined as SQLData.
SQLResults also contains a table of fields, which stores the fieldname, size and datatype of the field.

SQLData elements are a variant object, and can be of several different types.
* nullVal: Null. This is actually a string. Future versions may allow a default string representation of this to be configured; currently, it's just an empty string.
* strVal: String
* intVal: Int
* int64Val: Int64
* boolVal: Bool
* floatVal: Float
* timeVal: Time. Stored as `TimeInterval`.
* binVal: Binary. Stored as a `seq[byte]`.

Data from a result set can be accessed directly using indexes.

    let data = results[rowIdx][columnIdx]

Alternatively, data can be accessed via field name with the `data` procs:

    let
      # Via numeric indexes
      data1 = results.data(columnIndex)               # Use current row (results.curRow)
      data2 = results.data(columnIndex, rowIndex)     # This is the same as results[rowIdx][columnIdx]

      # Via field names
      data3 = results.data(fieldnameString)
      data4 = results.data(fieldnameString, rowIdx)
    
SQLResults also allows working with SQLFields via the `fields` procs.

    let
      # Fields can be retreived by column index or name.
      fieldCol5 = results.fields(5)
      myField = results.fields(fieldnameString)

      # Access the data via SQLField for the current row.
      data5 = results.data(fieldCol5)
      myData = results.data(myField)
      
      # Access by SQlQField and row index.
      data5AtRow = results.data(fieldCol5, rowIdx)
      myDataAtRow = results.data(myField, rowIdx)

#### Working with fields ####

For large result sets, performing many calls to `data` with a string fieldname parameter is wasteful
due to having to perform a hash table lookup with the field string each time.

Fetching fields or field indexes allows faster processing:

    let
      results = myQuery.executeFetch
      myField = results.fields("MyField")
      
    for i in 0 ..< results.len:
      let data = results.data(myField, i)
      # ... 

Alternatively you can fetch the column index directly using `fieldIndex`:

    let
      results = myQuery.executeFetch
      myFieldColIdx = results.fieldIndex("MyField")
      
    for i in 0 ..< results.len:
      let data1 = results.data(myFieldColIdx, i)
      # or
      let data2 = results[i][myFieldColIdx]
      # ... 

#### Data conversions

To extract data you can either use the raw value - for example 'data.boolVal' - or you can use one of the conversions functions listed below. Please note that not all conversions are possible, for instance converting binary to bool or time. Like for like values simply return their existing value (eg; asInt for an integer will return intVal).
* asString: Converts the value to it's string representation
* asInt:
  * For string, returns parseInt
  * For int64 returns the int value in an int64
  * For bool, returns 1 if true, else 0
  * For float, returns the integer portion of the float value
* asInt64:
  * For string, returns parseInt
  * For int, tries to fit into int and otherwise raises an error
  * For bool, returns 1 if true, else 0
  * For float, returns the integer portion of the float value
* asBool:
  * For string, returns parseBool
  * For int, int64 and float, returns true when non-zero
* asFloat:
  * For string, returns parseFloat
  * For int and int64, returns the value cast to float
  * For bool, returns 1.0 when true, else 0.0
* asBinary:
  * For string, casts each string element to byte and adds it to the seq
  * For int, int64 and float, returns the value split into a seq of bytes
  * For bool, returns a seq containing the bool value cast to byte

#### Example results use

Here we use two different methods of working with results from a simple query.
Note that the statement is set only once, and in each example the opening/closing of the query is performed behind the scenes.

    qry.statement = "SELECT 'A' AS A, 5 AS B, 0.8 AS C"
    
    qry.withExecuteByField:
      # Execute query and perform this block for each field returned.
      echo "Field ", field.fieldname, " (", field.dataType, "): ", data

    qry.withExecute(row):
      for idx, data in row:
        if qry.fields(idx).fieldname == "A": echo "A is found!"
        else: echo qry.fields(idx).fieldname, " is found, and of type ", qry.fields(idx).dataType
        echo "Item data: ", data
        
    var results = qry.executeFetch
    echo results.len, " total row(s)"                   # outputs "1 total row(s)
    echo "First field's data is ", results[0][0].data   # outputs "First field's data is A"
    for row in results:
      echo row.len, " fields in this row"               # outputs "3 fields in this row"


### Parameters

Parameters can be any of the datatypes that SQLValue can hold, and are referenced in the SQL (by default) with the '?' prefix. You can use any string as a prefix, however, by setting the paramPrefix variable - note that this is a global variable!
Parameters are set by value by using their name as follows:

    qry.statement = "SELECT ?b+?c, ?a"
    qry.params["a"] = 1
    qry.params["b"] = 2
    qry.params["c"] = 3
    echo qry.executeFetch
    # outputs 5, 1

Parameters are constructed internally when you set the statement for the query, and will raise an error if you try to set a value for one that isn't referenced in the statement string.
Parameters can also be used multiple times in the same query without having to redefine them. This is different from normal odbc, where you must apply the same value to different parameters if you want to use it more than once.
For example:

    qry.statement = "SELECT ?a, ?a+?a, ?a*?a"
    qry.params["a"] = 10
    echo qry.executeFetch
    # outputs 10, 20, 100

## JSON

You can convert results, rows or individual SQLValue objects to JSON format using the toJson proc:

    qry.statement = "SELECT 'A', 'B', 'C'
    var results = qry.executeFetch

    from json import pretty # This allows us to convert json to human readable form for the echo
    echo results.toJson.pretty

Please note: because the fields are stored in the SQLResults type, not the data, passing a row or data item to toJson will produce a 'data only' Json node.
        
## Utilities

You can list the available drivers on your system with:

    echo listDrivers()
