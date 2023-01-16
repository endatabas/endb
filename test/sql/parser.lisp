(defpackage :endb-test/sql/parser
  (:use :cl :fiveam :endb/sql/parser))
(in-package :endb-test/sql/parser)

(def-suite endb-test/sql/parser)
(in-suite endb-test/sql/parser)

(test test-parse-sql
  (is (parse-sql "CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)"))
  (is (parse-sql "INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104)"))
  (is (parse-sql "SELECT a+b*2+c*3+d*4+e*5,
       (a+b+c+d+e)/5
  FROM t1
 ORDER BY 1,2")))
