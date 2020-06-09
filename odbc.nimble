# Package
version       = "0.2.2"
author        = "coffeepots"
description   = "ODBC library around the odbcsql wrapper"
license       = "MIT"

# Deps
requires: "nim >= 1.0"

# Tests
task test, "Runs odbc test suite":
  exec "nim c -r tests/all"