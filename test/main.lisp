(defpackage :endb-test/main
  (:use :cl :fiveam :endb/main))
(in-package :endb-test/main)

(def-suite endb-test/main)
(in-suite endb-test/main)

(test test-sanity
      (is (= (+ 1 1) 2)))
