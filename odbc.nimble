# Package
version       = "0.1.3"
author        = "coffeepots"
description   = "ODBC library around the odbcsql wrapper"
license       = "MIT"

# Deps
requires: "nim >= 0.19"

# Tests
task test, "Runs odbc test suite":
  exec "nim c -r tests/all"