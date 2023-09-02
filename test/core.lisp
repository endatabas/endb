(defpackage :endb-test/core
  (:use :cl :fiveam)
  (:import-from :endb-test/sql/expr)
  (:import-from :endb-test/sql)
  (:import-from :endb-test/arrow)
  (:import-from :endb-test/json)
  (:import-from :endb-test/storage)
  (:import-from :endb-test/lib/arrow)
  (:import-from :endb-test/lib/parser)
  (:import-from :endb/core))
(in-package :endb-test/core)

(setf fiveam:*run-test-when-defined* t
      fiveam:*on-error* (when (find :slynk *features*)
                            :debug)
      fiveam:*on-failure* (when (find :slynk *features*)
                            :debug))
