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
                 (endb/sql/expr:base-table-columns (gethash "t1" db)))))

    (multiple-value-bind (result result-code)
        (execute-sql db "INSERT INTO t1 VALUES(103,102,100,101,104)")
      (is (null result))
      (is (= 1 result-code))
      (is (equal '((103 102 100 101 104))
                 (endb/sql/expr:base-table-rows (gethash "t1" db)))))

    (multiple-value-bind (result result-code)
        (execute-sql db "INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104), (NULL,102,NULL,101,104)")
      (is (null result))
      (is (= 2 result-code))
      (is (equal '((104 100 102 101 103)
                   (104 :null 102 101 :null)
                   (103 102 100 101 104))
                 (endb/sql/expr:base-table-rows (gethash "t1" db)))))

    (multiple-value-bind (result result-code)
        (execute-sql db "CREATE INDEX t1i0 ON t1(a1,b1,c1,d1,e1,x1)")
      (is (null result))
      (is (eq t result-code)))))

(test simple-select
  (let ((db (create-db)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT 1 + 1")
      (is (equal '((2)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT CASE 1 + 1 WHEN 3 THEN 1 WHEN 2 THEN 2 END")
      (is (equal '((2)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT CASE WHEN TRUE THEN 2 WHEN FALSE THEN 1 END")
      (is (equal '((2)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT CASE WHEN FALSE THEN 2 ELSE 1 END")
      (is (equal '((1)) result))
      (is (equal '("column1") columns)))

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
      (is (equal '("a" "b" "c" "d" "e") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT * FROM t1 WHERE a IN (102, 103)")
      (is (equal '((103 102 100 101 104)) result))
      (is (equal '("a" "b" "c" "d" "e") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT * FROM t1 WHERE b = 102")
      (is (equal '((103 102 100 101 104)) result))
      (is (equal '("a" "b" "c" "d" "e") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT b, COUNT(t1.a) FROM t1 GROUP BY b")
      (is (equal '((102 1)) result))
      (is (equal '("b" "column2") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT SUM(a) FROM t1")
      (is (equal '((103)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT t1.a, x.a FROM t1, t1 AS x WHERE t1.a = x.a")
      (is (equal '((103 103)) result))
      (is (equal '("a" "a") columns)))

    (execute-sql db "INSERT INTO t1(e,c,b,d,a) VALUES(103,102,102,101,104), (NULL,102,NULL,101,104)")

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT COUNT(*), COUNT(e), SUM(e), AVG(a), MIN(b), MAX(c), b FROM t1 GROUP BY b")
      (is (equalp '((1 0 :null 104.0 :null 102 :null) (2 2 207 103.5 102 102 102)) result))
      (is (equal '("column1" "column2" "column3" "column4" "column5" "column6" "b") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT COUNT(*) FROM t1 WHERE FALSE")
      (is (equal '((0)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT ALL 74 * - COALESCE ( + CASE - CASE WHEN NOT ( NOT - 79 >= NULL ) THEN 48 END WHEN + + COUNT( * ) THEN 6 END, MIN( ALL + - 30 ) * 45 * 77 ) * - 14")
      (is (equal '((-107692200)) result))
      (is (equal '("column1") columns)))))
