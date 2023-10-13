(defpackage :endb-test/lib/cst
  (:use :cl :fiveam :endb/lib/cst)
  (:import-from :endb/json)
  (:import-from :fset)
  (:import-from :alexandria)
  (:import-from :trivial-utf-8))
(in-package :endb-test/lib/cst)

(in-suite* :lib)

(test parse-cst
  (is (equal '(:|sql_stmt_list|
               (:|sql_stmt|
                (:|select_stmt|
                 (:|select_core| ("SELECT" 0 6)
                  (:|result_expr_list|
                   (:|result_column|
                    (:|expr|
                      (:|or_expr|
                        (:|and_expr|
                          (:|not_expr|
                            (:|equal_expr|
                              (:|rel_expr|
                                (:|bit_expr|
                                  (:|add_expr|
                                    (:|mul_expr|
                                      (:|concat_expr|
                                        (:|unary_expr|
                                          (:|access_expr|
                                            (:|atom|
                                              (:|literal|
                                                (:|numeric_literal| ("1" 7 8))))))))))))))))))))))
                 (parse-sql-cst "SELECT 1"))))

(defvar report-json
  "{\"kind\":\"Error\",\"msg\":\"parse error: unexpected SEL\",\"note\":\"/ sql_stmt_list / sql_stmt / select_stmt / with_clause\",\"location\":[\"/sql\",0],\"labels\":[{\"span\":[\"/sql\",{\"start\":0,\"end\":3}],\"msg\":\"expected WITH\",\"color\":\"Red\",\"order\":0,\"priority\":0},{\"span\":[\"/sql\",{\"start\":0,\"end\":0}],\"msg\":\"while parsing with_clause\",\"color\":\"Blue\",\"order\":1,\"priority\":0}],\"source\":\"SEL\"}")

(test json-error-report
  (let ((report (fset:map ("kind" "Error")
                          ("msg" "parse error: unexpected SEL")
                          ("note" "/ sql_stmt_list / sql_stmt / select_stmt / with_clause")
                          ("location" (fset:seq "/sql" 0))
                          ("source" "SEL")
                          ("labels" (fset:seq (fset:map ("span" (fset:seq "/sql" (fset:map ("start" 0) ("end" 3))))
                                                        ("msg" "expected WITH")
                                                        ("color" "Red")
                                                        ("order" 0)
                                                        ("priority" 0))
                                              (fset:map ("span" (fset:seq "/sql" (fset:map ("start" 0) ("end" 0))))
                                                        ("msg" "while parsing with_clause")
                                                        ("color" "Blue")
                                                        ("order" 1)
                                                        ("priority" 0)))))))
    (is (equalp report (endb/json:json-parse report-json)))
    (is (equal
         (render-error-report report)
         (render-error-report
          (endb/json:json-parse report-json))))
    (is (alexandria:starts-with-subseq "Error: parse error: unexpected SEL"
                                       (render-error-report report)))))

(test parse-escapes
  (let* ((sql "SELECT \"\\/f\\bo\\fo\\nb\\\\a\\\"\\tr\\r\\u03BB\"")
         (result (caaadr (cst->ast sql (parse-sql-cst sql)))))
    (is (equalp (format nil "/f~Ao~Ao~Ab\\a\"~Ar~Aλ" #\Backspace #\Page #\NewLine #\Tab #\Return) result)))

  (let* ((sql "SELECT '\\/f\\0o\\fo\\nb\\\\a\\'\\v\\
r\\r\\u03BB'")
         (result (caaadr (cst->ast sql (parse-sql-cst sql)))))
    (is (equalp (format nil "/f~Ao~Ao~Ab\\a'~Ar~Aλ" #\Nul #\Page #\NewLine #\Vt #\Return) result))))

(defun is-valid (sql)
  (is (equal
       (prin1-to-string (endb/lib/parser:parse-sql sql))
       (prin1-to-string (cst->ast sql (parse-sql-cst sql))))))

(test cst-to-ast
  (is-valid "SELECT 1")
  (is-valid "SELECT a, b, 123, myfunc(b) FROM table_1 WHERE a > b AND b < 100 ORDER BY a DESC, b")

  (is-valid "CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)")
  (is-valid "INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104)")
  (is-valid "SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 ORDER BY 1")
  (is-valid "SELECT a+b*2+c*3+d*4+e*5,
       (a+b+c+d+e)/5
  FROM t1
 ORDER BY 1,2")
  (is-valid "SELECT a+b*2+c*3+d*4+e*5,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       abs(b-c),
       (a+b+c+d+e)/5,
       a+b*2+c*3
  FROM t1
 WHERE (e>c OR e<d)
   AND d>e
   AND EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
 ORDER BY 4,2,1,3,5")
  (is-valid "SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2+c*3+d*4,
       a+b*2+c*3,
       c,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       abs(b-c)
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
    OR b>c
    OR d NOT BETWEEN 110 AND 150
 ORDER BY 4,1,5,2,6,3,7")
  (is-valid "SELECT c,
       d-e,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2+c*3+d*4,
       e
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
    OR c BETWEEN b-2 AND d+2
    OR (e>c OR e<d)
 ORDER BY 1,5,3,2,4")
  (is-valid "SELECT a,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a+b*2+c*3+d*4+e*5,
       d
  FROM t1
 WHERE a IS NULL")
  (is-valid "CREATE TABLE t1(
  a1 INTEGER PRIMARY KEY,
  b1 INTEGER,
  x1 VARCHAR(40)
)")
  (is-valid "INSERT INTO t1 VALUES(382,414,67,992,483,'table tn1 row 1')")
  (is-valid "CREATE INDEX t1i0 ON t1(a1,b1,c1,d1,e1,x1)")
  (is-valid "CREATE INDEX t8all ON t8(e8 DESC, d8 ASC, c8 DESC, b8 ASC, a8 DESC)")
  (is-valid "SELECT e1 FROM t1
   WHERE a1 in (767,433,637,363,776,109,451)
      OR c1 in (683,531,654,246,3,876,309,284)
      OR (b1=738)
EXCEPT
  SELECT b8 FROM t8
   WHERE NOT ((761=d8 AND b8=259 AND e8=44 AND 762=c8 AND 563=a8)
           OR e8 in (866,579,106,933))
EXCEPT
  SELECT e6 FROM t6
   WHERE NOT ((825=b6 OR d6=500)
           OR (230=b6 AND e6=731 AND d6=355 AND 116=a6))
UNION
  SELECT b2 FROM t2
   WHERE (d2=416)
UNION
  SELECT a4 FROM t4
   WHERE c4 in (806,119,489,658,366,424,2,471)
      OR (215=c4 OR c4=424 OR e4=405)
UNION ALL
  SELECT a9 FROM t9
   WHERE (e9=195)
      OR (c9=98 OR d9=145)
UNION ALL
  SELECT e5 FROM t5
   WHERE (44=c5 AND a5=362 AND 193=b5)
      OR (858=b5)
UNION
  SELECT d3 FROM t3
   WHERE (b3=152)
      OR (726=d3)
UNION
  SELECT e7 FROM t7
   WHERE d7 in (687,507,603,52,118)
      OR (d7=399 AND e7=408 AND 396=b7 AND a7=97 AND c7=813)
      OR (e7=605 OR 837=b7 OR e7=918)")
  (is-valid "SELECT * FROM t1")
  (is-valid "INSERT INTO t1(e,c,b,d,a) VALUES(NULL,102,NULL,101,104)")
  (is-valid "SELECT cor0.col2 AS col2 FROM tab2 AS cor0 GROUP BY col2 HAVING NOT NULL < NULL")
  (is-valid "SELECT 20 / - - 96 + CAST ( 90 AS INTEGER ) AS col2")
  (is-valid "SELECT ALL * FROM tab0 cor0 CROSS JOIN tab2 AS cor1")
  (is-valid "SELECT + + MIN ( ALL - + 32 ) AS col0, + COUNT ( * ) * + COUNT ( * ) FROM ( tab0 AS cor0 CROSS JOIN tab0 cor1 )")
  (is-valid "SELECT ALL NULLIF ( - COUNT ( * ), + 67 * - - ( + 25 ) + 89 + - 39 * 63 ) + + 54 AS col2, 11 * 31 * - - ( 70 )")
  (is-valid "SELECT ALL 74 * - COALESCE ( + CASE - CASE WHEN NOT ( NOT - 79 >= NULL ) THEN 48 END WHEN + + COUNT( * ) THEN 6 END, MIN( ALL + - 30 ) * 45 * 77 ) * - 14")
  (is-valid "SELECT * FROM tab1 WHERE NULL NOT IN ( col0 * col0 )")
  (is-valid "SELECT SUM ( + 73 ) * - CASE WHEN NOT ( NOT 27 BETWEEN 15 AND - NULLIF ( - 63, - 28 + + 76 ) ) THEN NULL ELSE + 77 * + 69 END / - CAST ( - 69 AS INTEGER ) AS col0")
  (is-valid "CREATE UNIQUE INDEX idx_tab2_2 ON tab2 (col1 DESC)")
  (is-valid "INSERT INTO tab0 VALUES(0,6,5.6,'jtqxx',9,5.19,'qvgba')")
  (is-valid "DELETE FROM tab0 WHERE col4 > 2.27")
  (is-valid "DROP TABLE tab0")
  (is-valid "SELECT ALL col2 FROM tab0 WHERE + col0 IS NOT NULL")
  (is-valid "DROP VIEW IF EXISTS view_3_tab0_153")
  (is-valid "CREATE VIEW view_1_tab0_153 AS SELECT pk, col0 FROM tab0 WHERE col0 = 49")
  (is-valid "DROP VIEW view_1_tab1_153")
  (is-valid "SELECT pk FROM ( SELECT pk, col0 FROM tab0 WHERE col0 = 49 ) AS tab0_153")
  (is-valid "SELECT * FROM tab0 cor0 JOIN tab0 AS cor1 ON NULL IS NULL")
  (is-valid "DROP INDEX t1i1")
  (is-valid "DROP TABLE IF EXISTS t1")
  (is-valid "SELECT 1 FROM t1 WHERE 1 IN ()")
  (is-valid "SELECT 1 FROM t1 WHERE 1 IN (2)")
  (is-valid "CREATE TABLE t3(z INTEGER UNIQUE)")
  (is-valid "SELECT x'303132' IN (SELECT * FROM t1)")
  (is-valid "UPDATE t1 SET x=1 WHERE x>0")
  (is-valid "UPDATE t1 SET x=3, x=4, x=5")
  (is-valid "SELECT 1 IN t1")
  (is-valid "CREATE TEMP VIEW view2 AS SELECT x FROM t1 WHERE x>0")
  (is-valid "INSERT OR REPLACE INTO view1 VALUES(2,'unknown')")
  (is-valid "SELECT x FROM t1 WHERE x NOT NULL ORDER BY x")
  (is-valid "INSERT INTO t1 VALUES(1<<63,'true')")
  (is-valid "CREATE TABLE partsupp (
  ps_partkey    INTEGER,
  ps_suppkey    INTEGER,
  ps_availqty   INTEGER,
  ps_supplycost INTEGER,
  ps_comment    TEXT,
  PRIMARY KEY (ps_partkey, ps_suppkey),
  FOREIGN KEY (ps_suppkey) REFERENCES supplier(s_suppkey),
  FOREIGN KEY (ps_partkey) REFERENCES part(p_partkey)
)")
  (is-valid "SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.a = t2.a")
  (is-valid "SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.a")
  (is-valid "SELECT * FROM t1 JOIN t2 ON t1.a = t2.a")
  (is-valid "SELECT DISTINCT * FROM t1")
  (is-valid "SELECT ALL * FROM t1")
  (is-valid "SELECT 1; SELECT 1;")
  (is-valid "SELECT 1;")
  (is-valid "WITH foo(c) AS (SELECT 1), bar(a, b) AS (SELECT 1, 2) SELECT * FROM foo, bar"))
