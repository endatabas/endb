(defsystem "endb-test"
  :author "Håkan Råberg <hakan.raberg@gmail.com>, Steven Deobald <steven@deobald.ca>"
  :license "AGPL-3.0-only"
  :class :package-inferred-system
  :pathname "test"
  :depends-on ("endb" "endb-test/core" "fiveam")
  :description "Test system for Endatabas"
  :perform (test-op (op c) (symbol-call :fiveam :run-all-tests)))
