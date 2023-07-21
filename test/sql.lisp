(defpackage :endb-test/sql
  (:use :cl :fiveam :endb/sql)
  (:import-from :endb/arrow)
  (:import-from :endb/sql/expr)
  (:import-from :endb/storage/object-store)
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
                 (endb/sql/expr:base-table-columns db "t1")))
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

    (setf db (commit-write-tx db write-db))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql write-db "SELECT * FROM t2"))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql write-db "SELECT d FROM t1"))

    (signals endb/sql/expr:sql-runtime-error
      (execute-sql write-db "SELECT t1.d FROM t1"))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT * FROM t1 ORDER BY b")
      (is (equal '((101 104 :null) (:null 105 nil)) result))
      (is (equal '("a" "b" "c") columns)))

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT b, b FROM t1 ORDER BY b")
      (is (equal '((104 104) (105 105)) result))
      (is (equal '("b" "b") columns)))))

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

    (multiple-value-bind (result columns)
        (execute-sql db "SELECT COUNT(*) FROM t1 WHERE FALSE")
      (is (equal '((0)) result))
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
      (is (equal '("a" "b") columns)))))

(defun endb->sqlite (x)
  (cond
    ((null x) 0)
    ((eq t x) 1)
    ((eq :null x) nil)
    ((typep x 'endb/arrow:arrow-date-days)
     (format nil "~A" x))
    (t x)))

(defun expr (expr)
  (sqlite:with-open-database (sqlite ":memory:")
    (let* ((endb (make-db))
           (query (format nil "SELECT ~A" expr))
           (sqlite-result (sqlite:execute-single sqlite query))
           (endb-result (first (first (execute-sql endb query)))))
      (list endb-result sqlite-result expr))))

(defun is-valid (result)
  (destructuring-bind (endb-result sqlite-result expr)
      result
    (is (equal (endb->sqlite endb-result) sqlite-result)
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

  (is-valid (expr "cast(date('2001-01-01') AS VARCHAR)"))
  (is-valid (expr "cast(10 AS DECIMAL)"))
  (is-valid (expr "cast(10 AS REAL)"))
  (is-valid (expr "cast(10.5 AS INTEGER)"))
  (is-valid (expr "cast(10 AS VARCHAR)"))
  (is-valid (expr "cast(10.5 AS VARCHAR)"))

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
  (is-valid (expr "'foo' LIKE NULL")))
