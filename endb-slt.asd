(defsystem "endb-slt"
  :version "0.1.0"
  :author "Håkan Råberg <hakan.raberg@gmail.com>, Steven Deobald <steven@deobald.ca>"
  :license "AGPL-3.0-only"
  :class :package-inferred-system
  :pathname "slt"
  :depends-on ("endb-slt/core" "cffi" "sqlite" #+sbcl "sb-sprof")
  :build-operation "program-op"
  :build-pathname "../target/slt"
  :entry-point "endb-slt/core:main"
  :description "SLT engine for Endatabas")
