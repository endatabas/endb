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
