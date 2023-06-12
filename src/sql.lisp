(defpackage :endb/sql
  (:use :cl)
  (:export #:*query-timing* #:*lib-parser* #:make-db #:execute-sql)
  (:import-from :endb/sql/parser)
  (:import-from :endb/sql/compiler)
  (:import-from :endb/lib/parser)
  (:import-from :endb/storage/buffer-pool)
  (:import-from :endb/storage/meta-data)
  (:import-from :endb/storage/object-store)
  (:import-from :endb/storage/wal)
  (:import-from :fset))
(in-package :endb/sql)

(defvar *query-timing* nil)
(defvar *lib-parser* nil)

;; (defstruct db
;;   (wal (endb/storage/wal:make-memory-wal))
;;   (object-store (endb/storage/object-store:make-memory-object-store))
;;   (buffer-pool (endb/storage/buffer-pool:make-buffer-pool))
;;   (meta-data (fset:empty-map)))

(defun make-db ()
  (make-hash-table :test 'equal))

(defun %execute-sql (db sql)
  (let* ((ast (if *lib-parser*
                  (endb/lib/parser:parse-sql sql)
                  (endb/sql/parser:parse-sql sql)))
         (ctx (fset:map (:db db)))
         (sql-fn (endb/sql/compiler:compile-sql ctx ast))
         (*print-length* 16))
    (funcall sql-fn db)))

(defun execute-sql (db sql)
  (if *query-timing*
      (time (%execute-sql db sql))
      (%execute-sql db sql)))
