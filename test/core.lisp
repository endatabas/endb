(defpackage :endb-test/core
  (:use :cl :fiveam)
  (:import-from :endb/core)
  (:import-from :endb-test/arrow)
  (:import-from :endb-test/http)
  (:import-from :endb-test/json)
  (:import-from :endb-test/lib/arrow)
  (:import-from :endb-test/lib/cst)
  (:import-from :endb-test/sql/expr)
  (:import-from :endb-test/sql)
  (:import-from :endb-test/storage))
(in-package :endb-test/core)

(setf fiveam:*run-test-when-defined* t)

(when (intersection '(:slynk :swank) *features*)
  (setf fiveam:*on-error* :debug
        fiveam:*on-failure* :debug))
