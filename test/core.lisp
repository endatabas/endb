(defpackage :endb-test/core
  (:use :cl :fiveam)
  (:import-from :endb-test/sql/expr)
  (:import-from :endb-test/sql/parser))
(in-package :endb-test/core)

(setf fiveam:*run-test-when-defined* t)
