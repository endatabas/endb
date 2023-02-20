(defpackage :endb/sql
  (:use :cl)
  (:export #:*query-timing* #:create-db #:execute-sql)
  (:import-from :endb/sql/parser)
  (:import-from :endb/sql/compiler))
(in-package :endb/sql)

(defvar *query-timing* nil)

(defun create-db ()
  (make-hash-table :test 'equal))

(defun %execute-sql (db sql)
  (let* ((ast (endb/sql/parser:parse-sql sql))
         (ctx (list (cons :db db)))
         (sql-fn (endb/sql/compiler:compile-sql ctx ast))
         (*print-length* 16))
    (funcall sql-fn db)))

(defun execute-sql (db sql)
  (if *query-timing*
      (time (%execute-sql db sql))
      (%execute-sql db sql)))
