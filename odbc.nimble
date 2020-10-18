# Package
version       = "0.3.0"
author        = "coffeepots"
description   = "Efficient ODBC queries using native Nim types"
license       = "MIT"

# Deps
requires: "nim >= 1.4"

# Tests
task test, "Runs odbc test suite":
  exec "nim c -r tests/all"