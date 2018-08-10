# Package
version       = "0.1.1"
author        = "coffeepots"
description   = "ODBC library around the odbcsql wrapper"
license       = "MIT"

# Deps
requires: "nim >= 0.18.1"

# Tests
task test, "Runs odbc test suite":
  exec "nim c -r tests/all"