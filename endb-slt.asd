(defsystem "endb-slt"
  :license "AGPL-3.0-only"
  :class :package-inferred-system
  :pathname "slt"
  :depends-on ("endb-slt/core" "cffi" "cl-ppcre" #+sbcl "sb-sprof")
  :build-operation program-op
  :build-pathname "../target/slt"
  :entry-point "endb-slt/core:main"
  :description "SLT engine for Endatabas")
