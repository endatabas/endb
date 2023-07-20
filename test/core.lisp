(defpackage :endb-test/core
  (:use :cl :fiveam)
  (:import-from :endb-test/sql/parser)
  (:import-from :endb-test/sql/expr)
  (:import-from :endb-test/sql)
  (:import-from :endb-test/arrow)
  (:import-from :endb-test/storage)
  (:import-from :endb-test/lib/arrow)
  (:import-from :endb-test/lib/parser)
  (:import-from :endb/core))
(in-package :endb-test/core)

(setf fiveam:*run-test-when-defined* t)
