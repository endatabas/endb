(defpackage :endb-test/sql/parser
  (:use :cl :fiveam :endb/sql/parser))
(in-package :endb-test/sql/parser)

(in-suite* :all-tests)

(test test-parse-sql
  (is (parse-sql "CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)"))
  (is (parse-sql "INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104)"))
  (is (parse-sql "SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 ORDER BY 1"))
  (is (parse-sql "SELECT a+b*2+c*3+d*4+e*5,
       (a+b+c+d+e)/5
  FROM t1
 ORDER BY 1,2"))
  (is (parse-sql "SELECT a+b*2+c*3+d*4+e*5,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       abs(b-c),
       (a+b+c+d+e)/5,
       a+b*2+c*3
  FROM t1
 WHERE (e>c OR e<d)
   AND d>e
   AND EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
 ORDER BY 4,2,1,3,5"))
  (is (parse-sql "SELECT c,
       d-e,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2+c*3+d*4,
       e
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
    OR c BETWEEN b-2 AND d+2
    OR (e>c OR e<d)
 ORDER BY 1,5,3,2,4"))
  (is (parse-sql "SELECT a,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a+b*2+c*3+d*4+e*5,
       d
  FROM t1
 WHERE a IS NULL"))
  (is (parse-sql "CREATE TABLE t1(
  a1 INTEGER PRIMARY KEY,
  b1 INTEGER,
  x1 VARCHAR(40)
)"))
  (is (parse-sql "INSERT INTO t1 VALUES(382,414,67,992,483,'table tn1 row 1')")))
