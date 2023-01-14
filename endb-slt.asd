(defsystem "endb-slt"
  :version "0.1.0"
  :author "Håkan Råberg <hakan.raberg@gmail.com>, Steven Deobald <steven@deobald.ca>"
  :license "AGPLv3"
  :class :package-inferred-system
  :pathname "slt"
  :depends-on ("endb-slt/core" "cffi" "sqlite")
  :build-operation "program-op"
  :build-pathname "../slt-runner"
  :entry-point "endb-slt/core:main"
  :description "SLT engine for Endatabas")
