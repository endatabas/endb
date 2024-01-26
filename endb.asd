(defsystem "endb"
  :author "Håkan Råberg <hakan.raberg@gmail.com>, Steven Deobald <steven@deobald.ca>"
  :license "AGPL-3.0-only"
  :description "Endatabas is a SQL document database with full history."
  :homepage "https://www.endatabas.com/"
  :source-control "https://github.com/endatabas/endb"
  :class :package-inferred-system
  :depends-on ("endb/core"
               "alexandria"
               "archive"
               "bordeaux-threads"
               "cffi"
               "cl-ppcre"
               "com.inuoe.jzon"
               "flexi-streams"
               "float-features"
               "fset"
               "local-time"
               "periods"
               "trivial-backtrace"
               "trivial-utf-8"
               "trivia")
  :pathname "src"
  :build-operation program-op
  :build-pathname "../target/endb"
  :entry-point "endb/core:main"
  :in-order-to ((test-op (test-op "endb-test"))))
