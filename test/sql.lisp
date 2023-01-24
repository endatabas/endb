(defpackage :endb-test/sql
  (:use :cl :fiveam :endb/sql))
(in-package :endb-test/sql)

(in-suite* :all-tests)

(test create-db-and-insert
  (let ((db (create-db)))
    (multiple-value-bind (result result-code)
        (execute-sql db "CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)")
      (is (null result))
      (is (eq t result-code))
      (is (equal '("a" "b" "c" "d" "e")
                 (gethash :columns (gethash "t1" db)))))

    (multiple-value-bind (result result-code)
        (execute-sql db "INSERT INTO t1 VALUES(103,102,100,101,104)")
      (is (null result))
      (is (= 1 result-code))
      (is (equal '((103 102 100 101 104))
                 (gethash :rows (gethash "t1" db)))))

    (multiple-value-bind (result result-code)
        (execute-sql db "INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104), (NULL,102,NULL,101,104)")
      (is (null result))
      (is (= 2 result-code))
      (is (equal '((104 100 102 101 103)
                   (104 :null 102 101 :null)
                   (103 102 100 101 104))
                 (gethash :rows (gethash "t1" db)))))

    (multiple-value-bind (result result-code)
        (execute-sql db "CREATE INDEX t1i0 ON t1(a1,b1,c1,d1,e1,x1)")
      (is (null result))
      (is (eq t result-code)))))

(test simple-select
  (let ((db (create-db)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT 1 + 1")
      (is (equal '("column1") columns))
      (is (equal '((2)) result)))

    (execute-sql db "CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)")
    (execute-sql db "INSERT INTO t1 VALUES(103,102,100,101,104)")

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT a FROM t1")
      (is (equal '((103)) result))
      (is (equal '("a") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT a FROM t1 LIMIT 0")
      (is (equal '() result))
      (is (equal '("a") columns)))


    (multiple-value-bind (result columns)
        (execute-sql db "SELECT t1.b + t1.c AS x FROM t1")
      (is (equal '((202)) result))
      (is (equal '("x") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT * FROM t1")
      (is (equal '((103 102 100 101 104)) result))
      (is (equal '("a" "b" "c" "d" "e") columns)))))
