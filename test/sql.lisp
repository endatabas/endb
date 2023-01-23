(defpackage :endb-test/sql
  (:use :cl :fiveam :endb/sql))
(in-package :endb-test/sql)

(in-suite* :all-tests)

(test create-db-and-insert
  (let ((db (create-db)))
    (execute-sql db "CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)")
    (is (equal '("a" "b" "c" "d" "e")
               (gethash :columns (gethash "t1" db))))

    (execute-sql db "INSERT INTO t1 VALUES(103,102,100,101,104)")
    (is (equal '((103 102 100 101 104))
               (gethash :rows (gethash "t1" db))))

    (execute-sql db "INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104), (103,102,100,101,104)")
    (is (equal '((104 100 102 101 103)
                 (104 100 102 101 103)
                 (103 102 100 101 104))
               (gethash :rows (gethash "t1" db))))))
