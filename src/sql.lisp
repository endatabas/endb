(defpackage :endb/sql
  (:use :cl)
  (:export #:create-db #:execute-sql)
  (:import-from :endb/sql/parser)
  (:import-from :endb/sql/compiler))
(in-package :endb/sql)

(defun create-db ()
  (make-hash-table :test 'equal))

(defun execute-sql (db sql)
  (funcall
   (endb/sql/compiler:compile-sql (list (cons :db db))
                                  (endb/sql/parser:parse-sql sql))
   db))
