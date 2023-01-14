(defsystem "endb-test"
  :version "0.1.0"
  :author "Håkan Råberg <hakan.raberg@gmail.com>, Steven Deobald <steven@deobald.ca>"
  :license "AGPLv3"
  :class :package-inferred-system
  :pathname "test"
  :depends-on ("endb-test/main" "fiveam")
  :description "Test system for Endatabas"
  :perform (test-op (op c) (symbol-call :fiveam :run-all-tests)))
