(defpackage :endb-test/lib
  (:use :cl :fiveam :endb/lib)
  (:import-from :endb/sql/parser))
(in-package :endb-test/lib)

(in-suite* :all-tests)

(test lib-parser
  (let ((sql "SELECT a, b, 123, myfunc(b) FROM table_1 WHERE a > b AND b < 100 ORDER BY a DESC, b"))
    (is (equal
         (princ-to-string (endb/sql/parser:parse-sql sql))
         (princ-to-string (parse-sql sql))))))
