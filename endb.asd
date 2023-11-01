(register-system-packages "clack-handler-hunchentoot" '(:clack.handler.hunchentoot))
(register-system-packages "lack-request" '(:lack.request))
(push :hunchentoot-no-ssl *features*)

(defsystem "endb"
  :version "0.1.0"
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
               "cl-bloom"
               "cl-murmurhash"
               "cl-ppcre"
               "clack"
               "clack-handler-hunchentoot"
               "clingon"
               "com.inuoe.jzon"
               "fast-io"
               "fset"
               "ironclad"
               "lack"
               "local-time"
               "log4cl"
               "mmap"
               "periods"
               "qbase64"
               "trivial-gray-streams"
               "trivial-utf-8"
               "trivia")
  :pathname "src"
  :build-operation program-op
  :build-pathname "../target/endb"
  :entry-point "endb/core:main"
  :in-order-to ((test-op (test-op "endb-test"))))
