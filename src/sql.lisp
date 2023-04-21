(defpackage :endb/sql
  (:use :cl)
  (:export #:*query-timing* #:*lib-parser* #:create-db #:execute-sql)
  (:import-from :endb/sql/parser)
  (:import-from :endb/sql/compiler)
  (:import-from :endb/lib/parser))
(in-package :endb/sql)

(defvar *query-timing* nil)
(defvar *lib-parser* nil)

(defun create-db ()
  (make-hash-table :test 'equal))

(defun %execute-sql (db sql)
  (let* ((ast (if *lib-parser*
                  (endb/lib/parser:parse-sql sql)
                  (endb/sql/parser:parse-sql sql)))
         (ctx (list (cons :db db)))
         (sql-fn (endb/sql/compiler:compile-sql ctx ast))
         (*print-length* 16))
    (funcall sql-fn db)))

(defun execute-sql (db sql)
  (if *query-timing*
      (time (%execute-sql db sql))
      (%execute-sql db sql)))
