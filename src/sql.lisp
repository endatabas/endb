(defpackage :endb/sql
  (:use :cl)
  (:export #:create-db #:execute-sql)
  (:import-from :endb/sql/parser)
  (:import-from :endb/sql/compiler))
(in-package :endb/sql)

(defun create-db ()
  (make-hash-table :test 'equal))

(defun execute-sql (db sql)
  (let* ((ast (endb/sql/parser:parse-sql sql))
         (ctx (list (cons :db db)))
         (sql-fn (endb/sql/compiler:compile-sql ctx ast)))
    (funcall sql-fn db)))
