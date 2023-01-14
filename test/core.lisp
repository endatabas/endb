(defpackage :endb-test/core
  (:use :cl :fiveam :endb/core))
(in-package :endb-test/core)

(def-suite endb-test/core)
(in-suite endb-test/core)

(test test-sanity
  (is (= (+ 1 1) 2)))
