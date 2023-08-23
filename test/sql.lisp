(defpackage :endb-test/sql
  (:use :cl :fiveam :endb/sql)
  (:import-from :endb/arrow)
  (:import-from :endb/sql/expr)
  (:import-from :endb/storage/object-store)
  (:import-from :cl-ppcre)
  (:import-from :fset)
  (:import-from :sqlite)
  (:import-from :asdf)
  (:import-from :uiop))
(in-package :endb-test/sql)

(in-suite* :sql)

(test create-table-and-insert
  (let ((endb/sql/expr:*sqlite-mode* t)
        (db (begin-write-tx (make-db))))
    (multiple-value-bind (result result-code)
        (execute-sql db "CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)")
      (is (null result))
      (is (eq t result-code))
      (is (equal '("a" "b" "c" "d" "e")
                 (endb/sql/expr:table-columns db "t1")))
      (is (zerop (endb/sql/expr:base-table-size db "t1"))))

    (multiple-value-bind (result result-code)
        (execute-sql db "INSERT INTO t1 VALUES(103,102,100,101,104)")
      (is (null result))
      (is (= 1 result-code))
      (is (equal '((103 102 100 101 104))
                 (endb/sql/expr:base-table-visible-rows db "t1")))
      (is (= 1 (endb/sql/expr:base-table-size db "t1"))))

    (multiple-value-bind (result result-code)
        (execute-sql db "INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104), (NULL,102,NULL,101,104)")
      (is (null result))
      (is (= 2 result-code))
      (is (equal '((103 102 100 101 104)
                   (104 100 102 101 103)
                   (104 :null 102 101 :null))
                 (endb/sql/expr:base-table-visible-rows db "t1")))
      (is (= 3 (endb/sql/expr:base-table-size db "t1"))))

    (multiple-value-bind (result result-code)
        (execute-sql db "CREATE INDEX t1i0 ON t1(a1,b1,c1,d1,e1,x1)")
      (is (null result))
      (is (eq t result-code)))

    (multiple-value-bind (result result-code)
        (execute-sql db "CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)")
      (is (null result))
      (is (null result-code)))

    (multiple-value-bind (result result-code)
        (execute-sql db "DROP TABLE t1")
      (is (null result))
      (is (eq t result-code)))

    (multiple-value-bind (result result-code)
        (execute-sql db "DROP TABLE t1")
      (is (null result))
      (is (null result-code)))))

(test isolation
  (let* ((endb/sql/expr:*sqlite-mode* t)
         (db (make-db))
         (write-db (begin-write-tx db)))

    (multiple-value-bind (result result-code)
        (execute-sql write-db "CREATE TABLE t1(a INTEGER)")
      (is (null result))
      (is (eq t result-code)))

    (multiple-value-bind (result result-code)
        (execute-sql write-db "INSERT INTO t1 VALUES(103)")
      (is (null result))
      (is (= 1 result-code)))

    (is (equal '((103)) (endb/sql/expr:base-table-visible-rows write-db "t1")))
    (is (= 1 (endb/sql/expr:base-table-size write-db "t1")))

    (is (null (endb/sql/expr:base-table-visible-rows db "t1")))
    (is (zerop (endb/sql/expr:base-table-size db "t1")))

    (setf db (commit-write-tx db write-db))

    (is (equal '((103)) (endb/sql/expr:base-table-visible-rows db "t1")))
    (is (= 1 (endb/sql/expr:base-table-size db "t1")))))

(test no-ddl
  (let* ((db (make-db))
         (write-db (begin-write-tx db)))

    (multiple-value-bind (result result-code)
        (execute-sql write-db "INSERT INTO t1(a, b) VALUES(103, 104)")
      (is (null result))
      (is (= 1 result-code)))

    (multiple-value-bind (result result-code)
        (execute-sql write-db "INSERT INTO t1(b, c) VALUES(105, FALSE)")
      (is (null result))
      (is (= 1 result-code)))

    (multiple-value-bind (result result-code)
        (execute-sql write-db "UPDATE t1 SET a = 101 WHERE a = 103")
      (is (null result))
      (is (= 1 result-code)))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql write-db "INSERT INTO t1 VALUES(105, FALSE)"))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql write-db "INSERT INTO t1(a) VALUES(105, FALSE)"))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql write-db "INSERT INTO t1(a, b) VALUES(105, FALSE), (106)"))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql write-db "DELETE FROM t2 WHERE a = 103"))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql write-db "UPDATE t2 SET a = 101 WHERE a = 103"))

    (setf db (commit-write-tx db write-db))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql db "SELECT * FROM t2"))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql db "SELECT d FROM t1"))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql db "SELECT t1.d FROM t1"))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql db "SELECT foo(1) FROM t1"))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT * FROM t1 ORDER BY b")
      (is (equal '((101 104 :null) (:null 105 nil)) result))
      (is (equal '("a" "b" "c") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT b, b FROM t1 ORDER BY b")
      (is (equal '((104 104) (105 105)) result))
      (is (equal '("b" "b") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT * FROM (VALUES (1, 2)) AS foo(a, b)")
      (is (equal '((1 2)) result))
      (is (equal '("a" "b") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT a FROM (VALUES (1, 2)) AS foo(a, b)")
      (is (equal '((1)) result))
      (is (equal '("a") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT foo.b FROM (VALUES (1, 2)) AS foo(a, b)")
      (is (equal '((2)) result))
      (is (equal '("b") columns)))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql db "SELECT * FROM (VALUES (1, 2)) AS foo(a)"))

    (multiple-value-bind (result result-code)
        (execute-sql write-db "INSERT INTO t2 {a: 2, b: 3}, {a: 3, c: 4}")
      (is (null result))
      (is (= 2 result-code)))

    (multiple-value-bind (result result-code)
        (execute-sql write-db "INSERT INTO t2 OBJECTS {a: 1, b: 2, c: 1 + 1}")
      (is (null result))
      (is (= 1 result-code)))

    (multiple-value-bind (result columns)
        (execute-sql write-db "SELECT * FROM t2 ORDER BY a")
      (is (equal '((1 2 2) (2 3 :null) (3 :null 4)) result))
      (is (equal '("a" "b" "c") columns)))

    (multiple-value-bind (result result-code)
        (execute-sql write-db "UPDATE t2 SET d = 4 WHERE c = 2")
      (is (null result))
      (is (= 1 result-code)))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql write-db "UPDATE t2"))

    (multiple-value-bind (result columns)
        (execute-sql write-db "SELECT * FROM t2 ORDER BY a")
      (is (equal '((1 2 2 4) (2 3 :null :null) (3 :null 4 :null)) result))
      (is (equal '("a" "b" "c" "d") columns)))

    (multiple-value-bind (result result-code)
        (execute-sql write-db "INSERT INTO t2(a, b, c, d) VALUES(1, 2, 5, 6), (4, 5, 7, 8) ON CONFLICT (a, b) DO NOTHING")
      (is (null result))
      (is (= 1 result-code)))

    (multiple-value-bind (result result-code)
        (execute-sql write-db "INSERT INTO t2(a, b, c, d) VALUES(1, 2, 5, 6), (4, 5, 7, 8) ON CONFLICT (a, b) DO NOTHING")
      (is (null result))
      (is (zerop result-code)))

    (multiple-value-bind (result columns)
        (execute-sql write-db "SELECT * FROM t2 ORDER BY a")
      (is (equal '((1 2 2 4) (2 3 :null :null) (3 :null 4 :null) (4 5 7 8)) result))
      (is (equal '("a" "b" "c" "d") columns)))

    (multiple-value-bind (result result-code)
        (execute-sql write-db "INSERT INTO t2 {a: 1, b: 2, c: 4, d: 6} ON CONFLICT (a, b) DO UPDATE SET c = excluded.c, d = t2.d")
      (is (null result))
      (is (= 1 result-code)))

    (multiple-value-bind (result result-code)
        (execute-sql write-db "INSERT INTO t2 {a: 1, b: 2, c: 4, d: 6} ON CONFLICT (a, b) DO UPDATE SET c = excluded.c WHERE t2.d = 6")
      (is (null result))
      (is (zerop result-code)))

    (multiple-value-bind (result columns)
        (execute-sql write-db "SELECT * FROM t2 ORDER BY a")
      (is (equal '((1 2 4 4) (2 3 :null :null) (3 :null 4 :null) (4 5 7 8)) result))
      (is (equal '("a" "b" "c" "d") columns)))

    (multiple-value-bind (result result-code)
        (execute-sql write-db "INSERT INTO t2 {c: 4, d: 6} ON CONFLICT (c) DO UPDATE SET d = excluded.d WHERE b IS NULL")
      (is (null result))
      (is (= 1 result-code)))

    (multiple-value-bind (result columns)
        (execute-sql write-db "SELECT * FROM t2 ORDER BY a")
      (is (equal '((1 2 4 4) (2 3 :null :null) (3 :null 4 6) (4 5 7 8)) result))
      (is (equal '("a" "b" "c" "d") columns)))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql write-db "INSERT INTO t2 {c: 4, e: 5}, {c: 4, d: 6} ON CONFLICT (c, e) DO NOTHING"))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql write-db "INSERT INTO t2 {} ON CONFLICT (a) DO NOTHING"))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql write-db "INSERT INTO t2(a) VALUES (1) ON CONFLICT (b) DO NOTHING"))

    (multiple-value-bind (result result-code)
        (execute-sql write-db "UPDATE t2 SET c = 5 UNSET a WHERE b = 3")
      (is (null result))
      (is (= 1 result-code)))

    (multiple-value-bind (result columns)
        (execute-sql write-db "SELECT * FROM t2 ORDER BY c")
      (is (equal '((3 :null 4 6) (1 2 4 4) (:null 3 5 :null) (4 5 7 8)) result))
      (is (equal '("a" "b" "c" "d") columns)))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql write-db "INSERT INTO t2 {c: 4, e: 5}, {c: 4, e: 5} ON CONFLICT (c, e) DO UPDATE SET f = 1"))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql write-db "INSERT INTO t2 {c: 4, e: 5} ON CONFLICT (c, e) DO UPDATE SET c = 1"))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql write-db "INSERT INTO t2 {c: 4, e: 5} ON CONFLICT (c, e) DO UPDATE UNSET c"))

    (multiple-value-bind (result result-code)
        (execute-sql write-db "INSERT INTO t2 {c: 4, e: 5} ON CONFLICT (c, e) DO UPDATE SET f = 1")
      (is (null result))
      (is (= 1 result-code)))

    (multiple-value-bind (result columns)
        (execute-sql write-db "SELECT * FROM t2 ORDER BY c")
      (is (equal '((:null :null 4 :null 5) (3 :null 4 6 :null) (1 2 4 4 :null)
                   (:null 3 5 :null :null) (4 5 7 8 :null))
                 result))
      (is (equal '("a" "b" "c" "d" "e") columns)))

    (multiple-value-bind (result result-code)
        (execute-sql write-db "INSERT INTO t2 {a: 4, e: 5} ON CONFLICT (e) DO UPDATE SET a = excluded.a")
      (is (null result))
      (is (= 1 result-code)))

    (multiple-value-bind (result columns)
        (execute-sql write-db "SELECT * FROM t2 ORDER BY c")
      (is (equal '((4 :null 4 :null 5) (3 :null 4 6 :null) (1 2 4 4 :null)
                   (:null 3 5 :null :null) (4 5 7 8 :null))
                 result))
      (is (equal '("a" "b" "c" "d" "e") columns)))

    (multiple-value-bind (result result-code)
        (execute-sql write-db "INSERT INTO t3 SELECT a, b, c FROM t2 WHERE c > 4 ON CONFLICT (b) DO NOTHING")
      (is (null result))
      (is (= 2 result-code)))

    (multiple-value-bind (result columns)
        (execute-sql write-db "SELECT * FROM t3 ORDER BY c")
      (is (equal '((:null 3 5) (4 5 7)) result))
      (is (equal '("a" "b" "c") columns)))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql write-db "INSERT INTO users {}"))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql write-db "INSERT INTO foo { ...:a } ON CONFLICT (name) DO NOTHING" (fset:map ("a" (fset:map ("b" 1))))))
    (signals endb/sql/expr:sql-runtime-error
      (execute-sql write-db "INSERT INTO foo { :a } ON CONFLICT (a) DO NOTHING" (fset:map ("a" 1))))
    (signals endb/sql/expr:sql-runtime-error
      (execute-sql write-db "INSERT INTO foo { [1 + 1]: 2 } ON CONFLICT (name) DO NOTHING"))))

(test multiple-statments
  (let* ((db (make-db)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT 1 AS a; SELECT 2 AS b;")
      (is (equal '((2)) result))
      (is (equal '("b") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT 2 AS b;")
      (is (equal '((2)) result))
      (is (equal '("b") columns)))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql db "SELECT 1 AS a; SELECT ? AS b;" (fset:seq 2)))

    (let* ((write-db (begin-write-tx db)))
      (multiple-value-bind (result columns)
          (execute-sql write-db "INSERT INTO t1(a, b) VALUES(103, 104); INSERT INTO t1(b, c) VALUES(105, FALSE); SELECT * FROM t1 ORDER BY b;")
        (is (equal '((103 104 :null) (:null 105 nil)) result))
        (is (equal '("a" "b" "c") columns))))))

(test with
  (let* ((db (make-db)))
    (multiple-value-bind (result columns)
        (execute-sql db "WITH foo(c) AS (SELECT 1), bar(a, b) AS (SELECT 2, 3) SELECT * FROM foo, bar")
      (is (equal '((1 2 3)) result))
      (is (equal '("c" "a" "b") columns)))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql db "WITH bar(a, b) AS (SELECT 2) SELECT * FROM bar"))

    (multiple-value-bind (result columns)
        (execute-sql db "WITH foo AS (SELECT 1) SELECT * FROM foo")
      (is (equal '((1)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n+1 FROM t WHERE n < 100) SELECT sum(n) FROM t")
      (is (equal '((5050)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "WITH RECURSIVE cnt(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x<5) SELECT x FROM cnt ORDER BY x")
      (is (equal '((1) (2) (3) (4) (5)) result))
      (is (equal '("x") columns)))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql db "WITH RECURSIVE cnt(x) AS (SELECT x+1 FROM cnt WHERE x<5) SELECT x FROM cnt ORDER BY x"))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql db "WITH RECURSIVE cnt(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM cnt AS c1, cnt AS c2 WHERE c1.x<5) SELECT x FROM cnt"))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql db "WITH RECURSIVE cnt(x) AS (VALUES(1) UNION ALL SELECT COUNT(x) FROM cnt WHERE x<5) SELECT x FROM cnt"))

    (let* ((write-db (begin-write-tx db)))
      (multiple-value-bind (result result-code)
          (execute-sql write-db "INSERT INTO org(name, boss) VALUES('Alice',NULL), ('Bob','Alice'), ('Cindy','Alice'), ('Dave','Bob'), ('Emma','Bob'), ('Fred','Cindy'), ('Gail','Cindy')")
        (is (null result))
        (is (= 7 result-code)))

      (multiple-value-bind (result columns)
          (execute-sql write-db "WITH RECURSIVE
  under_alice(name,level) AS (
    VALUES('Alice',0)
    UNION ALL
    SELECT org.name, under_alice.level+1
      FROM org JOIN under_alice ON org.boss = under_alice.name
  )
SELECT substr('..........',1,level*3) || name FROM under_alice ORDER BY under_alice.level")
        (is (equal '(("Alice")
                     ("...Bob")
                     ("...Cindy")
                     ("......Dave")
                     ("......Emma")
                     ("......Fred")
                     ("......Gail"))
                   result))
        (is (equal '("column1") columns))))

    (multiple-value-bind (result columns)
        (execute-sql db
                     "WITH RECURSIVE
      xaxis(x) AS (VALUES(-2.0) UNION ALL SELECT x+0.05 FROM xaxis WHERE x<1.2),
      yaxis(y) AS (VALUES(-1.0) UNION ALL SELECT y+0.1 FROM yaxis WHERE y<1.0),
      m(iter, cx, cy, x, y) AS (
        SELECT 0, x, y, 0.0, 0.0 FROM xaxis, yaxis
        UNION ALL
        SELECT iter+1, cx, cy, x*x-y*y + cx, 2.0*x*y + cy FROM m
         WHERE (x*x + y*y) < 4.0 AND iter<28
      ),
      m2(iter, cx, cy) AS (
        SELECT max(iter), cx, cy FROM m GROUP BY cx, cy
      ),
      a(t) AS (
        SELECT group_concat( substr(' .+*#', 1+min(iter/7,4), 1), '')
        FROM m2 GROUP BY cy
      )
    SELECT group_concat(rtrim(t),x'0a') FROM a")
      (is (equal '((
"                                    ....#
                                   ..#*..
                                 ..+####+.
                            .......+####....   +
                           ..##+*##########+.++++
                          .+.##################+.
              .............+###################+.+
              ..++..#.....*#####################+.
             ...+#######++#######################.
          ....+*################################.
 #############################################...
          ....+*################################.
             ...+#######++#######################.
              ..++..#.....*#####################+.
              .............+###################+.+
                          .+.##################+.
                           ..##+*##########+.++++
                            .......+####....   +
                                 ..+####+.
                                   ..#*..
                                    ....#
                                    +."))
                 result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "WITH RECURSIVE
  input(sud) AS (
    VALUES('53..7....6..195....98....6.8...6...34..8.3..17...2...6.6....28....419..5....8..79')
  ),
  digits(z, lp) AS (
    VALUES('1', 1)
    UNION ALL SELECT
    CAST(lp+1 AS TEXT), lp+1 FROM digits WHERE lp<9
  ),
  x(s, ind) AS (
    SELECT sud, instr(sud, '.') FROM input
    UNION ALL
    SELECT
      substr(s, 1, ind-1) || z || substr(s, ind+1),
      instr( substr(s, 1, ind-1) || z || substr(s, ind+1), '.' )
     FROM x, digits AS z
    WHERE ind>0
      AND NOT EXISTS (
            SELECT 1
              FROM digits AS lp
             WHERE z.z = substr(s, ((ind-1)/9)*9 + lp, 1)
                OR z.z = substr(s, ((ind-1)%9) + (lp-1)*9 + 1, 1)
                OR z.z = substr(s, (((ind-1)/3) % 3) * 3
                        + ((ind-1)/27) * 27 + lp
                        + ((lp-1) / 3) * 6, 1)
         )
  )
SELECT s FROM x WHERE ind=0")
      (is (equal '(("534678912672195348198342567859761423426853791713924856961537284287419635345286179")) result))
      (is (equal '("s") columns)))))

(test parameters
  (let* ((db (make-db)))
    (multiple-value-bind (result columns)
        (execute-sql db "SELECT ? + ?" (fset:seq 1 3))
      (is (equal '((4)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT [?, ?]" (fset:seq 1 3))
      (is (equalp `((,(fset:seq 1 3))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT :x + :y" (fset:map ("x" 1) ("y" 3)))
      (is (equal '((4)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT :x" (fset:map ("x" 1)))
      (is (equal '((1)) result))
      (is (equal '("x") columns)))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql db "SELECT ?"))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql db "SELECT ?" 1))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql db "SELECT ?" nil))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT ? + ?" (fset:seq (fset:seq 1 3) (fset:seq 2 4)) t)
      (is (equal '((6)) result))
      (is (equal '("column1") columns)))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql db "SELECT ?" (fset:map ("x" 1) ("y" 3)) t))

    (let ((write-db (begin-write-tx db)))
      (multiple-value-bind (result result-code)
          (execute-sql write-db "INSERT INTO foo(x, y) VALUES (?, ?)" (fset:seq (fset:seq 1 3) (fset:seq 2 4)) t)
        (is (null result))
        (is (= 2 result-code)))

      (multiple-value-bind (result columns)
          (execute-sql write-db "SELECT * FROM foo ORDER BY x")
        (is (equal '((1 3) (2 4)) result))
        (is (equal '("x" "y") columns))))

    (let ((write-db (begin-write-tx db)))
      (multiple-value-bind (result result-code)
          (execute-sql write-db "INSERT INTO foo(x, y) VALUES (:x, :y)" (fset:seq (fset:map ("x" 1) ("y" 3)) (fset:map ("x" 2) ("y" 4))) t)
        (is (null result))
        (is (= 2 result-code)))

      (multiple-value-bind (result columns)
          (execute-sql write-db "SELECT * FROM foo ORDER BY x")
        (is (equal '((1 3) (2 4)) result))
        (is (equal '("x" "y") columns)))

      (multiple-value-bind (result result-code)
          (execute-sql write-db "INSERT INTO foo(x, y) VALUES (:x, :y)" (fset:seq) t)
        (is (null result))
        (is (null result-code))))

    (let ((write-db (begin-write-tx db)))
      (multiple-value-bind (result result-code)
          (execute-sql write-db "INSERT INTO foo {x: ?, y: ?};"  (fset:seq (fset:seq 1 3) (fset:seq 2 4)) t)
        (is (null result))
        (is (= 2 result-code)))

      (multiple-value-bind (result columns)
          (execute-sql write-db "SELECT * FROM foo ORDER BY x")
        (is (equal '((1 3) (2 4)) result))
        (is (equal '("x" "y") columns))))

    (let ((write-db (begin-write-tx db)))
      (multiple-value-bind (result result-code)
          (execute-sql write-db "INSERT INTO foo { ...? }"  (fset:seq (fset:seq (fset:map ("x" 1) ("y" 3)))
                                                                      (fset:seq (fset:map ("x" 2) ("y" 4))))
                       t)
        (is (null result))
        (is (= 2 result-code)))

      (multiple-value-bind (result columns)
          (execute-sql write-db "SELECT * FROM foo ORDER BY x")
        (is (equal '((1 3) (2 4)) result))
        (is (equal '("x" "y") columns))))

    (let ((write-db (begin-write-tx db)))
      (multiple-value-bind (result result-code)
          (execute-sql write-db "INSERT INTO foo { :x, :y }"  (fset:seq (fset:map ("x" 1) ("y" 3))
                                                                        (fset:map ("x" 2) ("y" 4)))
                       t)
        (is (null result))
        (is (= 2 result-code)))

      (multiple-value-bind (result columns)
          (execute-sql write-db "SELECT * FROM foo ORDER BY x")
        (is (equal '((1 3) (2 4)) result))
        (is (equal '("x" "y") columns))))))

(test information-schema
  (let* ((db (make-db))
         (write-db (begin-write-tx db)))

    (multiple-value-bind (result columns)
        (execute-sql write-db "SELECT * FROM information_schema.tables")
      (is (null result))
      (is (equal '("table_catalog" "table_schema" "table_name" "table_type")
                 columns)))

    (multiple-value-bind (result columns)
        (execute-sql write-db "SELECT * FROM information_schema.columns")
      (is (null result))
      (is (equal '("table_catalog" "table_schema" "table_name" "column_name" "ordinal_position")
                 columns)))

    (multiple-value-bind (result columns)
        (execute-sql write-db "SELECT * FROM information_schema.views")
      (is (null result))
      (is (equal '("table_catalog" "table_schema" "table_name" "view_definition")
                 columns)))

    (multiple-value-bind (result result-code)
        (execute-sql write-db "INSERT INTO t1(a, b) VALUES(103, 104); INSERT INTO t1(b, c) VALUES(105, FALSE);")
      (is (null result))
      (is (equal 1 result-code)))

    (multiple-value-bind (result columns)
        (execute-sql write-db "SELECT * FROM information_schema.tables")
      (is (equal '((:null "main" "t1" "BASE TABLE")) result))
      (is (equal '("table_catalog" "table_schema" "table_name" "table_type")
                 columns)))

    (multiple-value-bind (result columns)
        (execute-sql write-db "SELECT * FROM information_schema.columns ORDER BY column_name")
      (is (equal '((:null "main" "t1" "a" 0)
                   (:null "main" "t1" "b" 0)
                   (:null "main" "t1" "c" 0))
                 result))
      (is (equal '("table_catalog" "table_schema" "table_name" "column_name" "ordinal_position")
                 columns)))

    (multiple-value-bind (result columns)
        (execute-sql write-db "SELECT tables.table_name, table_type FROM information_schema.tables")
      (is (equal '(("t1" "BASE TABLE")) result))
      (is (equal '("table_name" "table_type") columns)))


    (multiple-value-bind (result result-code)
        (execute-sql write-db "CREATE VIEW foo(a, b) AS SELECT 1, 2")
      (is (null result))
      (is (eq t result-code)))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql write-db "CREATE VIEW bar(a, b) AS SELECT 1"))

    (multiple-value-bind (result columns)
        (execute-sql write-db "SELECT * FROM information_schema.tables WHERE table_type = 'VIEW'")
      (is (equal '((:null "main" "foo" "VIEW")) result))
      (is (equal '("table_catalog" "table_schema" "table_name" "table_type")
                 columns)))

    (multiple-value-bind (result columns)
        (execute-sql write-db "SELECT * FROM information_schema.views")
      (is (equal '((:null "main" "foo" "(:SELECT ((1) (2)))")) result))
      (is (equal '("table_catalog" "table_schema" "table_name" "view_definition")
                 columns)))

    (multiple-value-bind (result columns)
        (execute-sql write-db "SELECT * FROM information_schema.columns WHERE table_name = 'foo' ORDER BY ordinal_position")
      (is (equal '((:null "main" "foo" "a" 1)
                   (:null "main" "foo" "b" 2))
                 result))
      (is (equal '("table_catalog" "table_schema" "table_name" "column_name" "ordinal_position")
                 columns)))

    (multiple-value-bind (result columns)
        (execute-sql write-db "SELECT * FROM foo")
      (is (equal '((1 2)) result))
      (is (equal '("a" "b") columns)))

    (multiple-value-bind (result result-code)
        (execute-sql write-db "DROP VIEW foo")
      (is (null result))
      (is (eq t result-code)))

    (is (null (execute-sql write-db "SELECT * FROM information_schema.tables WHERE table_type = 'VIEW'")))
    (is (null (execute-sql write-db "SELECT * FROM information_schema.views")))
    (is (null (execute-sql write-db "SELECT * FROM information_schema.columns WHERE table_name = 'foo' ORDER BY ordinal_position")))))

(test temporal-scalars
  (let* ((db (make-db)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT 2001-01-01")
      (is (equalp (list (list (endb/arrow:parse-arrow-date-millis "2001-01-01"))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT DATE '2001-01-01'")
      (is (equalp (list (list (endb/arrow:parse-arrow-date-millis "2001-01-01"))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT 12:01:20")
      (is (equalp (list (list (endb/arrow:parse-arrow-time-micros "12:01:20"))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT TIME '12:01:20'")
      (is (equalp (list (list (endb/arrow:parse-arrow-time-micros "12:01:20"))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT 2023-05-16T14:43:39.970062Z")
      (is (equalp (list (list (endb/arrow:parse-arrow-timestamp-micros "2023-05-16T14:43:39.970062Z"))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT TIMESTAMP '2023-05-16 14:43:39.970062'")
      (is (equalp (list (list (endb/arrow:parse-arrow-timestamp-micros "2023-05-16T14:43:39.970062Z"))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT 2001-01-01 = 2001-01-01T00:00:00")
      (is (equalp (list (list t)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT 2002-01-01 > 2001-01-01T00:00:00")
      (is (equalp (list (list t)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT CAST(2023-05-16T14:43:39.970062 AS TIME)")
      (is (equalp (list (list (endb/arrow:parse-arrow-time-micros "14:43:39.970062Z"))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT CAST(2023-05-16T14:43:39.970062 AS DATE)")
      (is (equalp (list (list (endb/arrow:parse-arrow-date-millis "2023-05-16"))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT P3Y6M")
      (is (equalp (list (list (endb/arrow:parse-arrow-interval-month-day-nanos "P3Y6M"))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT PT12H30M5S")
      (is (equalp (list (list (endb/arrow:parse-arrow-interval-month-day-nanos "PT12H30M5S"))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT INTERVAL '14:43:39.970062' HOUR TO SECOND")
      (is (equalp (list (list (endb/arrow:parse-arrow-interval-month-day-nanos "PT14H43M39.970062S"))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT 2001-01-01T00:00:00 + P1Y1DT1H")
      (is (equalp (list (list (endb/arrow:parse-arrow-timestamp-micros "2002-01-02T01:00:00"))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT 2001-01-01 - P1D")
      (is (equalp (list (list (endb/arrow:parse-arrow-date-millis "2000-12-31"))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT 2001-01-01 + PT1H")
      (is (equalp (list (list (endb/arrow:parse-arrow-timestamp-micros "2001-01-01T01:00:00"))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT PT1H + 10:00:00")
      (is (equalp (list (list (endb/arrow:parse-arrow-time-micros "11:00:00"))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT P1D + P1D")
      (is (equalp (list (list (endb/arrow:parse-arrow-interval-month-day-nanos "P2D"))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT P2M1D - P1M")
      (is (equalp (list (list (endb/arrow:parse-arrow-interval-month-day-nanos "P1M1D"))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT 2001-01-02 - 2001-01-01")
      (is (equalp (list (list (endb/arrow:parse-arrow-interval-month-day-nanos "PT24H"))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT 09:00:00 - 10:30:00")
      (is (equalp (list (list (endb/arrow:parse-arrow-interval-month-day-nanos "PT1H30M"))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT {start: 2001-01-01, end: 2001-03-01} OVERLAPS {start: 2001-02-01, end: 2001-04-01}")
      (is (equalp (list (list t)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT {start: 2001-01-01, end: 2001-03-01} CONTAINS {start: 2001-02-01, end: 2001-04-01}")
      (is (equalp (list (list nil)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT {start: 2001-01-01, end: 2001-04-01} CONTAINS {start: 2001-02-01, end: 2001-04-01}")
      (is (equalp (list (list t)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT 2001-04-01 IMMEDIATELY PRECEDES [2001-04-01T00:00:00Z, 2001-05-01]")
      (is (equalp (list (list t)) result))
      (is (equal '("column1") columns)))))

(test temporal-current-literals
  (let* ((db (make-db))
         (now (endb/arrow:parse-arrow-timestamp-micros "2023-05-16T14:43:39.970062Z")))

    (setf (endb/sql/expr:db-current-timestamp db) now)

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT CURRENT_TIMESTAMP")
      (is (equalp (list (list now)) result))
      (is (equal '("CURRENT_TIMESTAMP") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT CURRENT_TIME")
      (is (equalp (list (list (endb/arrow:parse-arrow-time-micros "14:43:39.970062"))) result))
      (is (equal '("CURRENT_TIME") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT CURRENT_DATE")
      (is (equalp (list (list (endb/arrow:parse-arrow-date-millis "2023-05-16"))) result))
      (is (equal '("CURRENT_DATE") columns)))))

(test system-time
  (let* ((db (make-db))
         (system-time-as-of-empty (endb/sql/expr:sql-current-timestamp db)))

    (sleep 0.01)

    (let* ((write-db (begin-write-tx db))
           (system-time-as-of-insert (endb/sql/expr:sql-current-timestamp write-db)))

      (is (not (equalp system-time-as-of-empty system-time-as-of-insert)))

      (multiple-value-bind (result result-code)
          (execute-sql write-db "INSERT INTO t1(a, b) VALUES(103, 104)")
        (is (null result))
        (is (= 1 result-code)))

      (setf db (commit-write-tx db write-db))

      (is (equal '((103 104)) (execute-sql db "SELECT * FROM t1")))

      (is (null (execute-sql db "SELECT * FROM t1 FOR SYSTEM_TIME AS OF ?" (fset:seq system-time-as-of-empty))))

      (sleep 0.01)

      (let* ((write-db (begin-write-tx db))
             (system-time-as-of-update (endb/sql/expr:sql-current-timestamp write-db)))
        (is (not (equalp system-time-as-of-insert system-time-as-of-update)))

        (multiple-value-bind (result result-code)
            (execute-sql write-db "UPDATE t1 SET a = 101 WHERE a = 103")
          (is (null result))
          (is (= 1 result-code)))

        (is (equal '((101 104)) (execute-sql write-db "SELECT * FROM t1")))

        (setf db (commit-write-tx db write-db))

        (is (equal '((101 104)) (execute-sql db "SELECT * FROM t1")))
        (is (equal '((101 104)) (execute-sql db "SELECT * FROM t1 FOR SYSTEM_TIME AS OF ?" (fset:seq system-time-as-of-update))))
        (is (equal '((103 104)) (execute-sql db "SELECT * FROM t1 FOR SYSTEM_TIME AS OF ?" (fset:seq system-time-as-of-insert))))
        (is (null (execute-sql db "SELECT * FROM t1 FOR SYSTEM_TIME AS OF ?" (fset:seq system-time-as-of-empty))))

        (is (equal '((101 104) (103 104)) (execute-sql db "SELECT * FROM t1 FOR SYSTEM_TIME BETWEEN ? AND ?" (fset:seq system-time-as-of-insert system-time-as-of-update))))
        (is (equal '((103 104)) (execute-sql db "SELECT * FROM t1 FOR SYSTEM_TIME BETWEEN ? AND ?" (fset:seq system-time-as-of-empty system-time-as-of-insert))))
        (is (null (execute-sql db "SELECT * FROM t1 FOR SYSTEM_TIME BETWEEN ? AND ?" (fset:seq system-time-as-of-empty system-time-as-of-empty))))

        (is (equal '((103 104)) (execute-sql db "SELECT * FROM t1 FOR SYSTEM_TIME FROM ? TO ?" (fset:seq system-time-as-of-insert system-time-as-of-update))))
        (is (null (execute-sql db "SELECT * FROM t1 FOR SYSTEM_TIME FROM ? TO ?" (fset:seq system-time-as-of-empty system-time-as-of-insert))))

        (multiple-value-bind (result columns)
            (execute-sql db "SELECT t1.*, t1.system_time FROM t1 FOR SYSTEM_TIME BETWEEN ? AND ?" (fset:seq system-time-as-of-insert system-time-as-of-update))
          (is (equalp (list (list 101 104 (fset:map ("start" system-time-as-of-update) ("end" endb/sql/expr:+end-of-time+)))
                            (list 103 104 (fset:map ("start" system-time-as-of-insert) ("end" system-time-as-of-update))))
                      result))
          (is (equal '("a" "b" "system_time") columns)))

        (signals endb/sql/expr:sql-runtime-error
          (execute-sql write-db "INSERT INTO t1(a, b, system_time) VALUES(103, 104, {start: 2001-01-01, end: 2002-01-01})"))))))

(test semi-structured
  (let* ((db (make-db)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT []")
      (is (equalp `((,(fset:seq))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT ARRAY []")
      (is (equalp `((,(fset:seq))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT [1, 2]")
      (is (equalp `((,(fset:seq 1 2))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT ARRAY [1, 2]")
      (is (equalp `((,(fset:seq 1 2))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT ARRAY (VALUES (1), (2))")
      (is (equalp `((,(fset:seq 1 2))) result))
      (is (equal '("column1") columns)))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql db "SELECT ARRAY (VALUES (1, 2), (2, 3))"))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT ARRAY_AGG(x.column1) FROM (VALUES (1), (2)) AS x")
      (is (equalp `((,(fset:seq 1 2))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT ARRAY_AGG(x.column1 ORDER BY x.column1 DESC) FROM (VALUES (1), (2)) AS x")
      (is (equalp `((,(fset:seq 2 1))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT ARRAY_AGG(x.column1 ORDER BY x.column2 ASC) FROM (VALUES (3, 2), (4, 1)) AS x")
      (is (equalp `((,(fset:seq 4 3))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT ARRAY_AGG(x.column1) FROM (VALUES (1)) AS x WHERE x.column1 = 2")
      (is (equalp `((,(fset:seq))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT OBJECT_AGG(x.column1, x.column2) FROM (VALUES ('foo', 1), ('bar', 2)) AS x")
      (is (equalp `((,(fset:map ("bar" 2) ("foo" 1)))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT OBJECT_AGG(x.column1, x.column2) FROM (VALUES ('foo', 1), ('baz', 1), ('bar', 2)) AS x GROUP BY x.column2")
      (is (equalp `((,(fset:map ("bar" 2)))
                    (,(fset:map ("baz" 1) ("foo" 1))))
                  result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT OBJECT_AGG(x.column1, x.column2) FROM (VALUES ('foo', 1), ('foo', 2)) AS x")
      (is (equalp `((,(fset:map ("foo" 2)))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT OBJECT_AGG(x.column1, x.column2) FROM (VALUES ('foo', 1)) AS x WHERE x.column1 = 'bar'")
      (is (equalp `((,(fset:empty-map))) result))
      (is (equal '("column1") columns)))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql db "SELECT OBJECT_AGG(x.column1) FROM (VALUES ('foo', 1), ('bar', 2)) AS x"))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql db "SELECT OBJECT_AGG(x.column1, x.column2) FROM (VALUES (1, 1), ('bar', 2)) AS x"))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT * FROM UNNEST([1, 2, 3]) AS foo(bar)")
      (is (equal '((1) (2) (3)) result))
      (is (equal '("bar") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT x.column1 AS foo, y.column1 AS bar FROM (VALUES ('foo', [1, 2, 3]), ('bar', [5, 6]), ('baz', 2), ('boz', [])) AS x, UNNEST(x.column2) AS y ORDER BY foo")
      (is (equal '(("bar" 5) ("bar" 6) ("foo" 1) ("foo" 2) ("foo" 3))
                 result))
      (is (equal '("foo" "bar") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT y.* FROM (VALUES (['a', 'b', 'c'])) AS x(foo), UNNEST(x.foo) WITH ORDINALITY AS y(foo, bar)")
      (is (equal '(("a" 0) ("b" 1) ("c" 2))
                 result))
      (is (equal '("foo" "bar") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT y.* FROM (VALUES (['a', 'b', 'c'], ['d', 'e'])) AS x(foo, baz), UNNEST(x.foo, x.baz) WITH ORDINALITY AS y(foo, baz, pos)")
      (is (equal '(("a" "d" 0) ("b" "e" 1) ("c" :null 2))
                 result))
      (is (equal '("foo" "baz" "pos") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT {}")
      (is (equalp `((,(fset:empty-map))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT OBJECT()")
      (is (equalp `((,(fset:empty-map))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT {foo: 2, bar: 'baz'}")
      (is (equalp `((,(fset:map ("foo" 2) ("bar" "baz")))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT {\"foo\": 2, 'bar': 'baz'}")
      (is (equalp `((,(fset:map ("foo" 2) ("bar" "baz")))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT OBJECT(foo: 2, bar: 'baz')")
      (is (equalp `((,(fset:map ("foo" 2) ("bar" "baz")))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT {address: {street: 'Street', number: 42}, friends: [1, 2]}")
      (is (equalp `((,(fset:map ("address" (fset:map ("street" "Street") ("number" 42)))
                                ("friends" (fset:seq 1 2)))))
                  result))
      (is (equal '("column1") columns)))

    (let ((write-db (begin-write-tx (make-db))))
      (multiple-value-bind (result result-code)
          (execute-sql write-db "INSERT INTO users(user) VALUES ({address: {street: 'Street', number: 42}, friends: [1, 2]}), ({address: {street: 'Street', number: 43}, friends: [3, 1]})")
        (is (null result))
        (is (= 2 result-code)))

      (multiple-value-bind (result columns)
          (execute-sql write-db "SELECT * FROM users")
        (is (equalp `((,(fset:map ("address" (fset:map ("street" "Street") ("number" 43)))
                                  ("friends" (fset:seq 3 1))))
                      (,(fset:map ("address" (fset:map ("street" "Street") ("number" 42)))
                                  ("friends" (fset:seq 1 2)))))
                    result))
        (is (equal '("user") columns))))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT { foo: 2, bar, [2 + 2]: 5, ...baz } FROM (VALUES (1, [6, 7]), (2, {boz: 7, foo: 1})) AS foo(bar, baz)")
      (is (equalp `((,(fset:map ("bar" 2) ("4" 5) ("boz" 7) ("foo" 1)))
                    (,(fset:map ("foo" 2) ("bar" 1) ("4" 5) ("0" 6) ("1" 7))))
                  result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT [1, 2, ...[3, 4], 5]")
      (is (equalp `((,(fset:seq 1 2 3 4 5))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT [1, 2, ...4, 5]")
      (is (equalp `((,(fset:seq 1 2 5))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT [1, 2, ...\"foo\", 5]")
      (is (equalp `((,(fset:seq 1 2 "f" "o" "o" 5))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT { a: 1, ...4 }")
      (is (equalp `((,(fset:map ("a" 1)))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT { a: 1, ...{} }")
      (is (equalp `((,(fset:map ("a" 1)))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT { a: 1, ...[2, 3], ...\"f\" }")
      (is (equalp `((,(fset:map ("a" 1) ("1" 3) ("0" "f")))) result))
      (is (equal '("column1") columns)))))

(test semi-structured-access
  (let* ((db (make-db)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT [][0]")
      (is (equal '((:null)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT [1, 2][0]")
      (is (equal '((1)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT [1, 2][2 - 1]")
      (is (equal '((2)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT [1, 2][-1]")
      (is (equal '((2)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT [NULL, 2][0]")
      (is (equal '((:null)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT [1, 2][-3]")
      (is (equal '((:null)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT 5[0]")
      (is (equal '((5)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT NULL[1]")
      (is (equal '((:null)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT 5[1]")
      (is (equal '((:null)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT 5.foo")
      (is (equal '((:null)) result))
      (is (equal '("foo") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT NULL.foo")
      (is (equal '((:null)) result))
      (is (equal '("foo") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT {foo: 5}[1]")
      (is (equal '((:null)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT {}.bar")
      (is (equal '((:null)) result))
      (is (equal '("bar") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT {bar: NULL}.bar")
      (is (equal '((:null)) result))
      (is (equal '("bar") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT {foo: 2, bar: ['baz']}.bar[0]")
      (is (equal '(("baz")) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT {foo: 2, bar: ['baz']}['bar']")
      (is (equalp `((,(fset:seq "baz"))) result))
      (is (equal '("bar") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT x.foo[0] FROM (VALUES ([{foo: [2]}, {foo: [3]}]), ([{foo: [4]}, {foo: [5]}])) AS x(x)")
      (is (equalp `((,(fset:seq 4 5)) (,(fset:seq 2 3))) result))
      (is (equal '("column1") columns)))

    (let ((write-db (begin-write-tx (make-db))))
      (multiple-value-bind (result result-code)
          (execute-sql write-db "INSERT INTO users(user) VALUES ({address: {street: 'Street', number: 42}, friends: [1, 2]}), ({address: {street: 'Street', number: 43}, friends: [3, 1]})")
        (is (null result))
        (is (= 2 result-code)))

      (multiple-value-bind (result columns)
          (execute-sql write-db "SELECT users.user.address FROM users")
        (is (equalp `((,(fset:map ("street" "Street") ("number" 43)))
                      (,(fset:map ("street" "Street") ("number" 42))))
                    result))
        (is (equal '("address") columns)))

      (multiple-value-bind (result columns)
          (execute-sql write-db "SELECT user.address FROM users")
        (is (equalp `((,(fset:map ("street" "Street") ("number" 43)))
                      (,(fset:map ("street" "Street") ("number" 42))))
                    result))
        (is (equal '("address") columns)))

      (multiple-value-bind (result columns)
          (execute-sql write-db "SELECT users.user.address.street FROM users")
        (is (equal '(("Street") ("Street")) result))
        (is (equal '("street") columns)))

      (multiple-value-bind (result columns)
          (execute-sql write-db "SELECT user.address.street FROM users")
        (is (equal '(("Street") ("Street")) result))
        (is (equal '("street") columns))))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT {a: 2, b: 3}[*]")
      (is (equalp `((,(fset:seq 2 3))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT {a: 2, b: 3}..c")
      (is (equalp `((,(fset:seq))) result))
      (is (equal '("c") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT {a: 2, b: {a: NULL}, c: [{a: 3}, 2]}..a")
      (is (equalp `((,(fset:seq 2 :null 3))) result))
      (is (equal '("a") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT {a: {b: 2}, b: {a: 3}, c: [{a: {b: 1}}, {b: 2}]}..a.b")
      (is (equalp `((,(fset:seq 2 1))) result))
      (is (equal '("b") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT [{a: NULL}, {a: 3}, {b: 4}].a")
      (is (equalp `((,(fset:seq :null 3))) result))
      (is (equal '("a") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT [{a: 2}, {a: 3}, {b: 4}, 5][*]")
      (is (equalp `((,(fset:seq (fset:map ("a" 2)) (fset:map ("a" 3)) (fset:map ("b" 4)) 5))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT [{a: 2, b: {c: [1, 2]}}, {b: 4}, 5]..[*]")
      (is (equalp `((,(fset:seq (fset:map ("a" 2) ("b" (fset:map ("c" (fset:seq 1 2)))))
                                (fset:map ("b" 4)) 5 2 (fset:map ("c" (fset:seq 1 2))) (fset:seq 1 2) 1 2 4)))
                  result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT [{a: 2, b: {c: [1, 2]}}, {b: 4}, 5]..[-1]")
      (is (equalp `((,(fset:seq 5 2))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT [{a: 2, b: {c: [1, 2]}}, {b: 4}, 5]..b")
      (is (equalp `((,(fset:seq (fset:map ("c" (fset:seq 1 2))) 4))) result))
      (is (equal '("b") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT [1, 2, 3, 4][*]")
      (is (equalp `((,(fset:seq 1 2 3 4))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT 1[*]")
      (is (equalp `((,(fset:seq))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT {}[*]")
      (is (equalp `((,(fset:seq))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT 1..a")
      (is (equalp `((,(fset:seq))) result))
      (is (equal '("a") columns)))))

(test directory-db
  (let* ((endb/sql/expr:*sqlite-mode* t)
         (target-dir (asdf:system-relative-pathname :endb-test "target/"))
         (test-dir (merge-pathnames "endb_data_directory/" target-dir)))
    (unwind-protect
         (let ((db (make-directory-db :directory test-dir)))
           (unwind-protect
                (let ((write-db (begin-write-tx db)))

                  (multiple-value-bind (result result-code)
                      (execute-sql write-db "CREATE TABLE t1(a INTEGER)")
                    (is (null result))
                    (is (eq t result-code)))

                  (multiple-value-bind (result result-code)
                      (execute-sql write-db "INSERT INTO t1 VALUES(103)")
                    (is (null result))
                    (is (= 1 result-code)))

                  (is (equal '((103)) (execute-sql write-db "SELECT * FROM t1")))

                  (setf db (commit-write-tx db write-db)))
             (close-db db))

           (let ((db (make-directory-db :directory test-dir)))
             (unwind-protect
                  (progn
                    (is (equal '((103)) (execute-sql db "SELECT * FROM t1")))

                    (let ((write-db (begin-write-tx db)))

                      (multiple-value-bind (result result-code)
                          (execute-sql write-db "INSERT INTO t1 VALUES(104)")
                        (is (null result))
                        (is (= 1 result-code)))

                      (is (equal '((103) (104)) (execute-sql write-db "SELECT * FROM t1 ORDER BY a")))

                      (multiple-value-bind (result result-code)
                          (execute-sql write-db "DELETE FROM t1 WHERE a = 103")
                        (is (null result))
                        (is (= 1 result-code)))

                      (is (equal '((104)) (execute-sql write-db "SELECT * FROM t1 ORDER BY a")))

                      (setf db (commit-write-tx db write-db))))
               (close-db db)))

           (let ((db (make-directory-db :directory test-dir)))
             (unwind-protect
                  (is (equal '((104)) (execute-sql db "SELECT * FROM t1 ORDER BY a")))
               (close-db db))))
      (when (probe-file test-dir)
        (uiop:delete-directory-tree test-dir :validate t)))))

(test wal-only-directory-db
  (let* ((endb/sql/expr:*sqlite-mode* t)
         (target-dir (asdf:system-relative-pathname :endb-test "target/"))
         (test-dir (merge-pathnames "endb_data_wal_only/" target-dir)))
    (unwind-protect
         (let ((db (make-directory-db :directory test-dir :object-store-path nil)))
           (unwind-protect
                (let ((write-db (begin-write-tx db)))

                  (multiple-value-bind (result result-code)
                      (execute-sql write-db "CREATE TABLE t1(a INTEGER)")
                    (is (null result))
                    (is (eq t result-code)))

                  (multiple-value-bind (result result-code)
                      (execute-sql write-db "INSERT INTO t1 VALUES(103)")
                    (is (null result))
                    (is (= 1 result-code)))

                  (is (equal '((103)) (execute-sql write-db "SELECT * FROM t1")))

                  (setf db (commit-write-tx db write-db)))
             (close-db db))

           (let ((db (make-directory-db :directory test-dir :object-store-path nil)))
             (unwind-protect
                  (progn
                    (is (equal '((103)) (execute-sql db "SELECT * FROM t1")))

                    (let ((write-db (begin-write-tx db)))

                      (multiple-value-bind (result result-code)
                          (execute-sql write-db "INSERT INTO t1 VALUES(104)")
                        (is (null result))
                        (is (= 1 result-code)))

                      (is (equal '((103) (104)) (execute-sql write-db "SELECT * FROM t1 ORDER BY a")))

                      (multiple-value-bind (result result-code)
                          (execute-sql write-db "DELETE FROM t1 WHERE a = 103")
                        (is (null result))
                        (is (= 1 result-code)))

                      (is (equal '((104)) (execute-sql write-db "SELECT * FROM t1 ORDER BY a")))

                      (setf db (commit-write-tx db write-db))))
               (close-db db)))

           (let ((db (make-directory-db :directory test-dir :object-store-path nil)))
             (unwind-protect
                  (is (equal '((104)) (execute-sql db "SELECT * FROM t1 ORDER BY a")))
               (close-db db))))
      (when (probe-file test-dir)
        (uiop:delete-directory-tree test-dir :validate t)))))

(test wal-only-directory-db-corrupt-archive-when-reading-appended-wal-bug
  (let* ((target-dir (asdf:system-relative-pathname :endb-test "target/"))
         (test-dir (merge-pathnames "endb_data_corrupt_archive_bug/" target-dir)))
    (unwind-protect
         (let ((db (make-directory-db :directory test-dir :object-store-path nil)))
           (unwind-protect
                (progn
                  (let ((write-db (begin-write-tx db)))

                    (multiple-value-bind (result result-code)
                        (execute-sql write-db "INSERT INTO t1(a) VALUES(103)")
                      (is (null result))
                      (is (= 1 result-code)))

                    (setf db (commit-write-tx db write-db)))

                  ;; At this point, the files from the first
                  ;; transaction have been appended, but the WAL
                  ;; hasn't been properly terminated as an
                  ;; archive. This results in an "Corrupt archive"
                  ;; error when looking for a file which doesn't
                  ;; exist, reaching the end of the file. This happens
                  ;; when the next write transaction tries to find an
                  ;; active Arrow batch for a table.
                  (let ((write-db (begin-write-tx db)))
                    (multiple-value-bind (result result-code)
                        (execute-sql write-db "INSERT INTO t1(a) VALUES(104)")
                      (is (null result))
                      (is (= 1 result-code)))

                    (is (equal '((103) (104)) (execute-sql write-db "SELECT * FROM t1 ORDER BY a")))

                    (setf db (commit-write-tx db write-db)))

                  (is (equal '((103) (104)) (execute-sql db "SELECT * FROM t1 ORDER BY a")))

                  ;; Listing the WAL will also read to the end,
                  ;; resulting in the same issue.
                  (is (equal '("_log/0000000000000001.json"
                               "_log/0000000000000002.json"
                               "information_schema.columns/0000000000000001.arrow"
                               "information_schema.tables/0000000000000001.arrow"
                               "t1/0000000000000001.arrow"
                               "t1/0000000000000002.arrow")
                             (endb/storage/object-store:object-store-list (endb/sql/expr:db-object-store db)))))
             (close-db db)))
      (when (probe-file test-dir)
        (uiop:delete-directory-tree test-dir :validate t)))))

(test dml
  (let ((endb/sql/expr:*sqlite-mode* t)
        (db (begin-write-tx (make-db))))
    (multiple-value-bind (result result-code)
        (execute-sql db "CREATE TABLE t1(a INTEGER, b INTEGER)")
      (is (null result))
      (is (eq t result-code)))

    (multiple-value-bind (result result-code)
        (execute-sql db "INSERT INTO t1 VALUES(1,2), (1,3)")
      (is (null result))
      (is (= 2 result-code))
      (is (equal '((1 2) (1 3))
                 (endb/sql/expr:base-table-visible-rows db "t1")))
      (is (equal '((1 2) (1 3)) (execute-sql db "SELECT * FROM t1 ORDER BY 2"))))

    (multiple-value-bind (result result-code)
        (execute-sql db "UPDATE t1 SET a = 2 WHERE b = 2")
      (is (null result))
      (is (= 1 result-code))
      (is (equal '((1 3) (2 2))
                 (endb/sql/expr:base-table-visible-rows db "t1")))
      (is (equal '((2 2) (1 3)) (execute-sql db "SELECT * FROM t1 ORDER BY 2"))))

    (multiple-value-bind (result result-code)
        (execute-sql db "DELETE FROM t1 WHERE b = 3")
      (is (null result))
      (is (= 1 result-code))
      (is (equal '((2 2))
                 (endb/sql/expr:base-table-visible-rows db "t1")))
      (is (equal '((2 2)) (execute-sql db "SELECT * FROM t1 ORDER BY 2"))))))

(test dql
  (let ((endb/sql/expr:*sqlite-mode* t)
        (db (begin-write-tx (make-db))))
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
        (execute-sql db "SELECT * FROM t1 WHERE a IN (VALUES (102), (103))")
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
        (execute-sql db "SELECT 1 FROM t1 HAVING SUM(a) = 103")
      (is (equal '((1)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT t1.a, x.a FROM t1, t1 AS x WHERE t1.a = x.a")
      (is (equal '((103 103)) result))
      (is (equal '("a" "a") columns)))

    (execute-sql db "INSERT INTO t1(e,c,b,d,a) VALUES(103,102,102,101,104), (NULL,102,NULL,101,104)")

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT COUNT(*), COUNT(e), SUM(e), AVG(a), MIN(b), MAX(c), b FROM t1 GROUP BY b")
      (is (equal '((1 0 :null 104.0d0 :null 102 :null) (2 2 207 103.5d0 102 102 102)) result))
      (is (equal '("column1" "column2" "column3" "column4" "column5" "column6" "b") columns)))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql db "SELECT COUNT(SUM(e)) FROM t1 WHERE FALSE"))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT COUNT(*) FROM t1 WHERE FALSE")
      (is (equal '((0)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT COUNT(*) FILTER (WHERE a = 104) FROM t1")
      (is (equal '((2)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT ALL 74 * - COALESCE ( + CASE - CASE WHEN NOT ( NOT - 79 >= NULL ) THEN 48 END WHEN + + COUNT( * ) THEN 6 END, MIN( ALL + - 30 ) * 45 * 77 ) * - 14")
      (is (equal '((-107692200)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "VALUES(0,6,5.6,'jtqxx',9,5.19,'qvgba')")
      (is (equal '((0 6 5.6d0 "jtqxx" 9 5.19d0 "qvgba")) result))
      (is (equal '("column1" "column2" "column3" "column4" "column5" "column6" "column7") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT 1 IN (2)")
      (is (equal '((nil)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT SUM(t1.b) FROM t1 HAVING SUM(t1.b) = 204")
      (is (equal '((204)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT * FROM (SELECT 1 AS a) x JOIN (SELECT 1 AS b) y ON x.a = y.b")
      (is (equal '((1 1)) result))
      (is (equal '("a" "b") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT * FROM (SELECT 1 AS a) x JOIN (SELECT 1 AS b) y ON x.a = y.b AND x.a > y.b")
      (is (null result))
      (is (equal '("a" "b") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT * FROM (SELECT 1 AS a) x JOIN (SELECT 2 AS b) y ON x.a < y.b")
      (is (equal '((1 2)) result))
      (is (equal '("a" "b") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT * FROM (SELECT 1 AS a) x JOIN (SELECT 2 AS b) y ON x.a = y.b")
      (is (null result))
      (is (equal '("a" "b") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT * FROM (SELECT 1 AS a) x LEFT JOIN (SELECT 1 AS b) y ON x.a = y.b")
      (is (equal '((1 1)) result))
      (is (equal '("a" "b") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT * FROM (SELECT 1 AS a) x LEFT JOIN (SELECT 1 AS b) y ON x.a = y.b AND x.a > y.b")
      (is (equal '((1 :null)) result))
      (is (equal '("a" "b") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT * FROM (SELECT 1 AS a) x LEFT JOIN (SELECT 2 AS b) y ON x.a < y.b")
      (is (equal '((1 2)) result))
      (is (equal '("a" "b") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT * FROM (SELECT 1 AS a) x LEFT JOIN (SELECT 2 AS b) y ON x.a = y.b")
      (is (equal '((1 :null)) result))
      (is (equal '("a" "b") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT CARDINALITY([1, 2])")
      (is (equal '((2)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT LENGTH([1, 2])")
      (is (equal '((2)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT LENGTH({a: 1})")
      (is (equal '((1)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT LENGTH({})")
      (is (equal '((0)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT CHARACTER_LENGTH('josé')")
      (is (equal '((4)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT OCTET_LENGTH('josé')")
      (is (equal '((5)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT \"josé\"")
      (is (equal '(("josé")) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT [1, 2] || [3, 4]")
      (is (equalp `((,(fset:seq 1 2 3 4))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT [1, 2] || 3")
      (is (equalp `((,(fset:seq 1 2 3))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT NULL || [1, 2]")
      (is (equalp `((,(fset:seq :null 1 2))) result))
      (is (equal '("column1") columns)))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql db "SELECT SQRT(-1)"))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT UUID()")
      (is (endb/sql/expr::%random-uuid-p (caar result)))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT CAST([1, 'foo', {a: 2001-01-01, b: NULL}] AS VARCHAR)")
      (is (equalp `(("[1,\"foo\",{\"a\":2001-01-01,\"b\":null}]")) result))
      (is (equalp (fset:seq 1 "foo" (fset:map ("a" (endb/arrow:parse-arrow-date-millis "2001-01-01")) ("b" :null)))
                  (interpret-sql-literal (caar result))))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT UNHEX('ABCD')")
      (is (equalp '((#(171 205))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT UNHEX('A ')")
      (is (equalp '((:null)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT UNHEX('AB CD')")
      (is (equalp '((:null)) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT UNHEX('ab CD', ' ')")
      (is (equalp '((#(171 205))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT RANDOMBLOB(8)")
      (is (typep (caar result) '(vector (unsigned-byte 8))))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT PATCH({a: 'b', c: {d: 'e', f: 'g'}}, {a: 'z', c: {f: NULL}})")
      (is (equalp `((,(fset:map ("a" "z") ("c" (fset:map ("d" "e")))))) result))
      (is (equal '("column1") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT x AS y FROM (VALUES (1), (2)) AS foo(x) ORDER BY -foo.x")
      (is (equalp '((2) (1)) result))
      (is (equal '("y") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT x FROM (VALUES ('foo'), ('bar'), ('food')) AS foo(x) ORDER BY LENGTH(x), x DESC")
      (is (equalp '(("foo") ("bar") ("food")) result))
      (is (equal '("x") columns)))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql db "SELECT x AS y FROM (VALUES (1), (2)) AS foo(x) ORDER BY -y"))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql db "VALUES (1), (2) ORDER BY -y"))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql db "VALUES (1), (2) ORDER BY column2"))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql db "VALUES (1), (2) ORDER BY 2"))))

(test interpret-sql-literal
  (is (equal "foo" (interpret-sql-literal "'foo'")))
  (is (equal "foo" (interpret-sql-literal "\"foo\"")))
  (is (= 2.0 (interpret-sql-literal "2.0")))
  (is (= 9223372036854775807 (interpret-sql-literal "9223372036854775807")))
  (is (= -9223372036854775808 (interpret-sql-literal "-9223372036854775808")))

  (is (= 170141183460469231731687303715884105727 (interpret-sql-literal "170141183460469231731687303715884105727")))
  (is (= -170141183460469231731687303715884105727 (interpret-sql-literal "-170141183460469231731687303715884105727")))

  (is (eq t (interpret-sql-literal "TRUE")))
  (is (null (interpret-sql-literal "FALSE")))
  (is (eq :null (interpret-sql-literal "NULL")))

  (is (equalp #(171 205) (interpret-sql-literal "X'ABCD'")))

  (is (equalp (endb/arrow:parse-arrow-date-millis "2001-01-01") (interpret-sql-literal "2001-01-01")))
  (is (equalp (endb/arrow:parse-arrow-time-micros "12:01:20") (interpret-sql-literal "12:01:20")))
  (is (equalp (endb/arrow:parse-arrow-timestamp-micros "2023-05-16T14:43:39.970062Z") (interpret-sql-literal "2023-05-16T14:43:39.970062Z")))
  (is (equalp (endb/arrow:parse-arrow-interval-month-day-nanos "P3Y2MT12H30M5S") (interpret-sql-literal "P3Y2MT12H30M5S")))

  (is (equalp (endb/arrow:parse-arrow-interval-month-day-nanos "P1Y2M") (interpret-sql-literal "INTERVAL '1-2' YEAR TO MONTH")))

  (is (equalp (fset:map ("address" (fset:map ("street" "Street") ("number" 42)))
                        ("friends" (fset:seq 1 2)))
              (interpret-sql-literal "{address: {street: 'Street', number: 42}, friends: [1, 2]}")))
  (is (equalp (fset:empty-map) (interpret-sql-literal "{}")))

  (signals endb/sql/expr:sql-runtime-error
    (interpret-sql-literal "2001-01-01ASFOO"))

  (signals endb/sql/expr:sql-runtime-error
    (interpret-sql-literal "2001-01"))

  (signals endb/sql/expr:sql-runtime-error
    (interpret-sql-literal "1 + 2"))

  (signals endb/sql/expr:sql-runtime-error
    (interpret-sql-literal "INTERVAL '1-2' MONTH TO YEAR"))

  (signals endb/sql/expr:sql-runtime-error
    (interpret-sql-literal "INTERVAL '1-2' YEAR"))

  (signals endb/sql/expr:sql-runtime-error
    (interpret-sql-literal "[1 + 2]"))

  (signals endb/sql/expr:sql-runtime-error
    (interpret-sql-literal "{foo: 1 + 2}"))

  (is (equalp (endb/arrow:parse-arrow-date-millis "2001-01-01")
              (endb/json:resolve-json-ld-xsd-scalars (interpret-sql-literal "{\"@value\": \"2001-01-01\", \"@type\": \"xsd:date\"}")))))

(defun endb->sqlite (x)
  (cond
    ((null x) 0)
    ((eq t x) 1)
    ((eq :null x) nil)
    ((typep x 'endb/arrow:arrow-date-millis)
     (format nil "~A" x))
    ((typep x 'endb/arrow:arrow-time-micros)
     (let ((s (format nil "~A" x)))
       (subseq s 0 (- (length s) 7))))
    ((typep x 'endb/arrow:arrow-timestamp-micros)
     (let ((s (format nil "~A" x)))
       (ppcre:regex-replace "T" (subseq s 0 (- (length s) 8)) " ")))
    (t x)))

(defun expr (expr)
  (sqlite:with-open-database (sqlite ":memory:")
    (let* ((endb (make-db))
           (query (format nil "SELECT ~A" expr))
           (sqlite-result (sqlite:execute-single sqlite query))
           (endb-result (first (first (execute-sql endb query)))))
      (list endb-result sqlite-result expr))))

(defun sqlite-compileoption-used-p (option)
  (sqlite:with-open-database (sqlite ":memory:")
    (= 1 (sqlite:execute-single sqlite (format nil "SELECT sqlite_compileoption_used('~A')" option)))))

(defun is-valid (result)
  (destructuring-bind (endb-result sqlite-result expr)
      result
    (is (equalp (endb->sqlite endb-result) sqlite-result)
        "~2&~S~2% evaluated to ~2&~S~2% which is not ~2&~S~2% to ~2&~S~2%"
        expr endb-result 'equal sqlite-result)))

(test sqlite-expr
  (is-valid (expr "FALSE"))
  (is-valid (expr "TRUE"))
  (is-valid (expr "NULL"))

  (is-valid (expr "1 + 2"))
  (is-valid (expr "1 - 2"))
  (is-valid (expr "1 * 2"))
  (is-valid (expr "1 / 2"))
  (is-valid (expr "1 % 2"))
  (is-valid (expr "1 < 2"))
  (is-valid (expr "1 <= 2"))
  (is-valid (expr "1 > 2"))
  (is-valid (expr "1 >= 2"))

  (is-valid (expr "1 + 2.0"))
  (is-valid (expr "1 - 2.0"))
  (is-valid (expr "1 * 2.0"))
  (is-valid (expr "1 / 2.0"))
  (is-valid (expr "1 % 2.0"))
  (is-valid (expr "1 < 2.0"))
  (is-valid (expr "1 <= 2.0"))
  (is-valid (expr "1 > 2.0"))
  (is-valid (expr "1 >= 2.0"))

  (is-valid (expr "1 + NULL"))
  (is-valid (expr "1 - NULL"))
  (is-valid (expr "1 * NULL"))
  (is-valid (expr "1 / NULL"))
  (is-valid (expr "1 % NULL"))
  (is-valid (expr "1 < NULL"))
  (is-valid (expr "1 <= NULL"))
  (is-valid (expr "1 > NULL"))
  (is-valid (expr "1 >= NULL"))

  (is-valid (expr "NULL + 2.0"))
  (is-valid (expr "NULL - 2.0"))
  (is-valid (expr "NULL * 2.0"))
  (is-valid (expr "NULL / 2.0"))
  (is-valid (expr "NULL % 2.0"))
  (is-valid (expr "NULL < 2.0"))
  (is-valid (expr "NULL <= 2.0"))
  (is-valid (expr "NULL > 2.0"))
  (is-valid (expr "NULL >= 2.0"))

  (is-valid (expr "1 + 'foo'"))
  (is-valid (expr "1 - 'foo'"))
  (is-valid (expr "1 * 'foo'"))
  (is-valid (expr "1 / 'foo'"))
  (is-valid (expr "1 % 'foo'"))
  (is-valid (expr "1 < 'foo'"))
  (is-valid (expr "1 <= 'foo'"))
  (is-valid (expr "1 > 'foo'"))
  (is-valid (expr "1 >= 'foo'"))

  (is-valid (expr "'foo' + 2.0"))
  (is-valid (expr "'foo' - 2.0"))
  (is-valid (expr "'foo' * 2.0"))
  (is-valid (expr "'foo' / 2.0"))
  (is-valid (expr "'foo' % 2.0"))
  (is-valid (expr "'foo' < 2.0"))
  (is-valid (expr "'foo' <= 2.0"))
  (is-valid (expr "'foo' > 2.0"))
  (is-valid (expr "'foo' >= 2.0"))

  (is-valid (expr "NULL + NULL"))
  (is-valid (expr "NULL + 'foo'"))
  (is-valid (expr "'foo' + NULL"))

  (is-valid (expr "'foo' + 'foo'"))
  (is-valid (expr "'foo' - 'foo'"))
  (is-valid (expr "'foo' * 'foo'"))
  (is-valid (expr "'foo' / 'foo'"))
  (is-valid (expr "'foo' % 'foo'"))

  (is-valid (expr "+1"))
  (is-valid (expr "-1"))

  (is-valid (expr "+NULL"))
  (is-valid (expr "-NULL"))

  (is-valid (expr "+'foo'"))
  (is-valid (expr "-'foo'"))

  (is-valid (expr "1 / 0"))
  (is-valid (expr "1 % 0"))

  (is-valid (expr "1 / 0.0"))
  (is-valid (expr "1 % 0.0"))

  (is-valid (expr "2 IS 2"))
  (is-valid (expr "2 IS 3"))
  (is-valid (expr "2 IS NULL"))
  (is-valid (expr "2 IS NOT NULL"))
  (is-valid (expr "NULL IS NULL"))
  (is-valid (expr "NULL IS NOT NULL"))

  (is-valid (expr "abs(2.0)"))
  (is-valid (expr "abs(-2)"))
  (is-valid (expr "abs(NULL)"))

  (is-valid (expr "nullif(1, 1)"))
  (is-valid (expr "nullif(1, 'foo')"))
  (is-valid (expr "nullif('foo', 1)"))

  (is-valid (expr "coalesce(NULL, 'foo', 1)"))
  (is-valid (expr "coalesce(NULL, NULL, 1)"))

  (is-valid (expr "date('2001-01-01')"))
  (is-valid (expr "date(NULL)"))
  (is-valid (expr "strftime('%Y', date('2001-01-01'))"))
  (is-valid (expr "strftime('%Y', NULL)"))
  (is-valid (expr "strftime(NULL, date('2001-01-01'))"))
  (is-valid (expr "strftime(NULL, NULL)"))

  (is-valid (expr "time('14:43:39')"))
  (is-valid (expr "time(NULL)"))
  (is-valid (expr "datetime('2023-05-16T14:43:39')"))
  (is-valid (expr "datetime(NULL)"))

  (is-valid (expr "date('2001-01-01') = date('2001-01-01')"))
  (is-valid (expr "date('2001-01-01') < date('2002-01-01')"))
  (is-valid (expr "date('2001-01-01') <= date('2002-01-01')"))
  (is-valid (expr "date('2001-01-01') > date('2002-01-01')"))
  (is-valid (expr "date('2001-01-01') >= date('2002-01-01')"))

  (is-valid (expr "time('14:43:39') = time('14:43:39')"))
  (is-valid (expr "time('14:43:39') < time('15:43:39')"))
  (is-valid (expr "time('14:43:39') <= time('15:43:39')"))
  (is-valid (expr "time('14:43:39') > time('15:43:39')"))
  (is-valid (expr "time('14:43:39') >= time('15:43:39')"))

  (is-valid (expr "datetime('2023-05-16T14:43:39') = datetime('2023-05-16T14:43:39')"))
  (is-valid (expr "datetime('2023-05-16T14:43:39') < datetime('2024-05-16T14:43:39')"))
  (is-valid (expr "datetime('2023-05-16T14:43:39') <= datetime('2024-05-16T14:43:39')"))
  (is-valid (expr "datetime('2023-05-16T14:43:39') > datetime('2024-05-16T14:43:39')"))
  (is-valid (expr "datetime('2023-05-16T14:43:39') >= datetime('2024-05-16T14:43:39')"))

  (is-valid (expr "cast(date('2001-01-01') AS VARCHAR)"))
  (is-valid (expr "cast(10 AS DECIMAL)"))
  (is-valid (expr "cast(10 AS REAL)"))
  (is-valid (expr "cast(10.5 AS INTEGER)"))
  (is-valid (expr "cast(10 AS VARCHAR)"))
  (is-valid (expr "cast(10.5 AS VARCHAR)"))

  (is-valid (expr "cast(10.5 AS varchar)"))

  (is-valid (expr "cast(TRUE AS VARCHAR)"))
  (is-valid (expr "cast(FALSE AS VARCHAR)"))
  (is-valid (expr "cast(NULL AS VARCHAR)"))
  (is-valid (expr "cast('foo' AS VARCHAR)"))

  (is-valid (expr "cast(TRUE AS INTEGER)"))
  (is-valid (expr "cast(FALSE AS INTEGER)"))
  (is-valid (expr "cast(NULL AS INTEGER)"))
  (is-valid (expr "cast('foo' AS INTEGER)"))
  (is-valid (expr "cast('10' AS INTEGER)"))
  (is-valid (expr "cast('-10' AS INTEGER)"))
  (is-valid (expr "cast(10.5 AS INTEGER)"))
  (is-valid (expr "cast(10 AS INTEGER)"))

  (is-valid (expr "cast(TRUE AS DECIMAL)"))
  (is-valid (expr "cast(FALSE AS DECIMAL)"))
  (is-valid (expr "cast(NULL AS DECIMAL)"))
  (is-valid (expr "cast('foo' AS DECIMAL)"))
  (is-valid (expr "cast('10' AS DECIMAL)"))
  (is-valid (expr "cast('-10.5' AS DECIMAL)"))
  (is-valid (expr "cast(10.5 AS DECIMAL)"))
  (is-valid (expr "cast(10 AS DECIMAL)"))

  (is-valid (expr "cast(TRUE AS REAL)"))
  (is-valid (expr "cast(FALSE AS REAL)"))
  (is-valid (expr "cast(NULL AS REAL)"))
  (is-valid (expr "cast('foo' AS REAL)"))
  (is-valid (expr "cast('10' AS REAL)"))
  (is-valid (expr "cast('-10' AS REAL)"))
  (is-valid (expr "cast(10.5 AS REAL)"))
  (is-valid (expr "cast(10 AS REAL)"))

  (is-valid (expr "cast(TRUE AS SIGNED)"))
  (is-valid (expr "cast(FALSE AS SIGNED)"))
  (is-valid (expr "cast(NULL AS SIGNED)"))
  (is-valid (expr "cast('foo' AS SIGNED)"))
  (is-valid (expr "cast('10' AS SIGNED)"))
  (is-valid (expr "cast('-10' AS SIGNED)"))
  (is-valid (expr "cast('-10.5' AS SIGNED)"))
  (is-valid (expr "cast(10.5 AS SIGNED)"))
  (is-valid (expr "cast(10 AS SIGNED)"))

  (is-valid (expr "substring('foo', 1, 2)"))
  (is-valid (expr "substring('foo', 1, 5)"))
  (is-valid (expr "substring('foo', 1)"))
  (is-valid (expr "substring('foo', -1)"))
  (is-valid (expr "substring('foo', 2, 1)"))
  (is-valid (expr "substring(NULL, 1)"))
  (is-valid (expr "substring('foo', NULL)"))
  (is-valid (expr "substring('foo', NULL, NULL)"))
  (is-valid (expr "substring(NULL, NULL, NULL)"))

  (is-valid (expr "'foo' LIKE '%fo'"))
  (is-valid (expr "'foo' LIKE 'fo%'"))
  (is-valid (expr "'foo' LIKE 'bar'"))
  (is-valid (expr "NULL LIKE 'bar'"))
  (is-valid (expr "'foo' LIKE NULL"))

  (is-valid (expr "'foo' || 'bar'"))
  (is-valid (expr "1 || 2"))
  (is-valid (expr "1 || NULL"))
  (is-valid (expr "NULL || 'bar'"))

  (is-valid (expr "LENGTH('bar')"))
  (is-valid (expr "TRIM(' bar  ')"))
  (is-valid (expr "LTRIM(' bar  ')"))
  (is-valid (expr "RTRIM(' bar  ')"))

  (is-valid (expr "LOWER('FooBar')"))
  (is-valid (expr "UPPER('FooBar')"))

  (when (sqlite-compileoption-used-p "SQLITE_ENABLE_MATH_FUNCTIONS")
    (is-valid (expr "ROUND(2.4)"))
    (is-valid (expr "FLOOR(2.4)"))
    (is-valid (expr "CEIL(-2.4)"))
    (is-valid (expr "CEILING(4)"))

    (is-valid (expr "SIN(2.4)"))
    (is-valid (expr "ATAN(2)"))
    (is-valid (expr "COS(NULL)"))
    (is-valid (expr "SQRT(2)"))
    (is-valid (expr "POWER(2, 2)"))
    (is-valid (expr "POWER(2, NULL)"))
    (is-valid (expr "LN(4.2)"))
    (is-valid (expr "LOG10(2)"))
    (is-valid (expr "LOG(2, 64)"))
    (is-valid (expr "SIGN(-2.4)")))

  (is-valid (expr "TYPEOF('foo')"))
  (is-valid (expr "TYPEOF(2)"))
  (is-valid (expr "TYPEOF(2.0)"))
  (is-valid (expr "TYPEOF(x'CAFEBABE')"))
  (is-valid (expr "TYPEOF(NULL)"))

  (is-valid (expr "HEX(12345678)"))
  (is-valid (expr "HEX(x'ABCD')"))

  (is-valid (expr "REPLACE('foobar', 'oo', 'aa')"))

  (is-valid (expr "UNICODE('foo')"))
  (is-valid (expr "UNICODE(NULL)"))
  (is-valid (expr "UNICODE('')"))

  (is-valid (expr "INSTR('foo', 'o')"))
  (is-valid (expr "INSTR('foo', NULL)"))

  (is-valid (expr "UNICODE('foo')"))
  (is-valid (expr "UNICODE(NULL)"))

  (is-valid (expr "CHAR(102, 111, 111)"))
  (is-valid (expr "CHAR()"))
  (is-valid (expr "CHAR(NULL, 102)"))

  (is-valid (expr "GLOB('*foo', 'barfoo')"))
  (is-valid (expr "GLOB('*fo', 'barfoo')"))
  (is-valid (expr "GLOB('?arfoo', 'barfoo')"))
  (is-valid (expr "GLOB('?rfoo', 'barfoo')"))

  (is-valid (expr "ZEROBLOB(8)"))

  (is-valid (expr "IIF(FALSE, 1, 2)"))
  (is-valid (expr "IIF(TRUE, 1, 2)"))
  (is-valid (expr "IIF(NULL, 1, 2)"))
  (is-valid (expr "IIF('foo', 1, 2)")))
