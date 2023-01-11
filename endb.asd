(defsystem "endb"
  :version "0.1.0"
  :author "Håkan Råberg <hakan.raberg@gmail.com>, Steven Deobald <steven@deobald.ca>"
  :license "AGPLv3"
  :homepage "https://www.endatabas.com/"
  :depends-on ()
  :components ((:module "src"
                :serial t
                :components ((:file "endb"))))
  :description "Endatabas"
  :build-operation "program-op"
  :build-pathname "endb"
  :entry-point "endb:main"
  :in-order-to ((test-op (test-op "endb/tests"))))

(defsystem "endb/slt"
  :version "0.1.0"
  :author "Håkan Råberg <hakan.raberg@gmail.com>, Steven Deobald <steven@deobald.ca>"
  :license "AGPLv3"
  :depends-on ("endb")
  :components ((:module "slt"
                :serial t
                :components ((:file "endb-slt"))))
  :build-operation "program-op"
  :build-pathname "slt-runner"
  :entry-point "endb-slt:slt-main"
  :description "SLT engine for Endatabas")

(defsystem "endb/tests"
  :version "0.1.0"
  :author "Håkan Råberg <hakan.raberg@gmail.com>, Steven Deobald <steven@deobald.ca>"
  :license "AGPLv3"
  :depends-on ("endb" "fiveam")
  :components ((:module "tests"
                :serial t
                :components ((:file "endb-tests"))))
  :description "Test system for Endatabas"
  :perform (test-op (op c) (symbol-call :fiveam :run-all-tests)))
