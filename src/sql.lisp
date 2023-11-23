(defpackage :endb/sql
  (:use :cl)
  (:export #:*query-timing* #:*use-cst-parser* #:*use-cst-parser-only*
           #:make-db #:make-directory-db #:close-db #:begin-write-tx #:commit-write-tx #:execute-sql #:interpret-sql-literal)
  (:import-from :alexandria)
  (:import-from :endb/arrow)
  (:import-from :endb/json)
  (:import-from :endb/sql/expr)
  (:import-from :endb/sql/compiler)
  (:import-from :endb/lib/arrow)
  (:import-from :endb/lib/cst)
  (:import-from :endb/lib/parser)
  (:import-from :endb/lib)
  (:import-from :endb/storage)
  (:import-from :endb/storage/buffer-pool)
  (:import-from :endb/storage/object-store)
  (:import-from :endb/storage/wal)
  (:import-from :trivial-utf-8)
  (:import-from :fset))
(in-package :endb/sql)

(defvar *query-timing* nil)
(defvar *use-cst-parser* nil)
(defvar *use-cst-parser-only* nil)

(defun make-db (&key (store (make-instance 'endb/storage:in-memory-store)))
  (let* ((object-store (endb/storage:store-get-object-store store))
         (buffer-pool (endb/storage/buffer-pool:make-buffer-pool :object-store object-store))
         (meta-data (endb/storage:store-replay store)))
    (endb/sql/expr:make-db :store store :buffer-pool buffer-pool :meta-data meta-data)))

(defun make-directory-db (&key (directory "endb_data") wal-only-p)
  (let* ((store (make-instance 'endb/storage:disk-store :directory directory :wal-only-p wal-only-p)))
    (make-db :store store)))

(defun close-db (db)
  (endb/storage:store-close (endb/sql/expr:db-store db))
  (endb/storage/buffer-pool:buffer-pool-close (endb/sql/expr:db-buffer-pool db)))

(defun begin-write-tx (db)
  (let* ((bp (endb/storage/buffer-pool:make-writeable-buffer-pool :parent-pool (endb/sql/expr:db-buffer-pool db)))
         (write-db (endb/sql/expr:copy-db db)))
    (setf (endb/sql/expr:db-buffer-pool write-db) bp)
    (setf (endb/sql/expr:db-current-timestamp write-db) (endb/sql/expr:syn-current_timestamp db))
    write-db))

(defun %execute-constraints (db)
  (let ((ctx (fset:map (:db db))))
    (fset:do-map (k v (endb/sql/expr:constraint-definitions db))
      (when (equal '((nil)) (handler-case
                                (funcall (endb/sql/compiler:compile-sql ctx v) db (fset:empty-seq))
                              (endb/sql/expr:sql-runtime-error (e)
                                (endb/lib:log-warn "Constraint ~A raised an error, ignoring: ~A" k e))))
        (error 'endb/sql/expr:sql-runtime-error :message (format nil "Constraint failed: ~A" k))))))

(defun commit-write-tx (current-db write-db &key (fsyncp t))
  (let ((current-md (endb/sql/expr:db-meta-data current-db))
        (tx-md (endb/sql/expr:db-meta-data write-db)))
    (if (eq current-md tx-md)
        current-db
        (progn
          (%execute-constraints write-db)
          (let* ((tx-id (1+ (or (fset:lookup tx-md "_last_tx") 0)))
                 (tx-md (fset:with tx-md "_last_tx" tx-id))
                 (md-diff (endb/json:json-diff current-md tx-md))
                 (md-diff (fset:with md-diff "_tx_log_version" endb/storage:*tx-log-version*))
                 (store (endb/sql/expr:db-store write-db))
                 (bp (endb/sql/expr:db-buffer-pool write-db))
                 (arrow-arrays-map (endb/storage/buffer-pool:writeable-buffer-pool-pool bp)))
            (endb/storage:store-write-tx store tx-id md-diff arrow-arrays-map :fsyncp fsyncp)
            (let ((new-db (endb/sql/expr:copy-db current-db))
                  (new-md (endb/json:json-merge-patch current-md md-diff)))
              (setf (endb/sql/expr:db-meta-data new-db) new-md)
              new-db))))))

(defun %resolve-parameters (ast)
  (let ((idx 0)
        (parameters))
    (labels ((walk (x)
               (cond
                 ((and (listp x)
                       (eq :parameter (first x)))
                  (if (second x)
                      (progn
                        (pushnew (symbol-name (second x)) parameters)
                        x)
                      (let ((src `(:parameter ,idx)))
                        (push idx parameters)
                        (incf idx)
                        src)))
                 ((listp x)
                  (mapcar #'walk x))
                 (t x))))
      (values (walk ast) parameters))))

(defun %parse-sql (sql)
  (if *use-cst-parser-only*
      (endb/lib/cst:cst->ast sql (endb/lib/cst:parse-sql-cst sql))
      (let ((ast (endb/lib/parser:parse-sql sql)))
        (if *use-cst-parser*
            (let ((ast-via-cst (endb/lib/cst:cst->ast sql (endb/lib/cst:parse-sql-cst sql))))
              (assert (equal
                       (prin1-to-string ast)
                       (prin1-to-string ast-via-cst)))
              ast-via-cst)
            ast))))

(defun %execute-sql (db sql parameters manyp)
  (when (and manyp (not (fset:seq? parameters)))
    (error 'endb/sql/expr:sql-runtime-error :message "Many parameters must be an array"))
  (let* ((ast (%parse-sql sql))
         (ctx (fset:map (:db db) (:sql sql)))
         (*print-length* 16)
         (sql-fn (multiple-value-bind (ast expected-parameters)
                     (%resolve-parameters ast)
                   (if (eq :multiple-statments (first ast))
                       (let ((asts (second ast)))
                         (if (= 1 (length asts))
                             (endb/sql/compiler:compile-sql ctx (first asts) expected-parameters)
                             (lambda (db parameters)
                               (loop with end-idx = (length asts)
                                     for ast in asts
                                     for idx from 1
                                     for sql-fn = (endb/sql/compiler:compile-sql ctx ast expected-parameters)
                                     if (= end-idx idx)
                                       do (return (funcall sql-fn db parameters))
                                     else
                                       do (funcall sql-fn db parameters)))))
                       (endb/sql/compiler:compile-sql ctx ast expected-parameters))))
         (all-parameters (if manyp
                             (fset:convert 'list parameters)
                             (list parameters)))
         (all-parameters (loop for parameters in all-parameters
                               collect (etypecase parameters
                                         (fset:map parameters)
                                         (fset:seq (fset:convert 'fset:map (loop for x in (fset:convert 'list parameters)
                                                                                 for idx from 0
                                                                                 collect (cons idx x))))
                                         (t (error 'endb/sql/expr:sql-runtime-error :message "Parameters must be an array or an object"))))))
    (loop with final-result = nil
          with final-result-code = nil
          for parameters in all-parameters
          do (multiple-value-bind (result result-code)
                 (funcall sql-fn db parameters)
               (setf final-result result)
               (if (numberp result-code)
                   (setf final-result-code (+ result-code (or final-result-code 0)))
                   (setf final-result-code result-code)))
          finally (return (values final-result final-result-code)))))

(defun execute-sql (db sql &optional (parameters (fset:empty-seq)) manyp)
  (handler-case
      (if *query-timing*
          (time (%execute-sql db sql parameters manyp))
          (%execute-sql db sql parameters manyp))
    #+sbcl (sb-pcl::effective-method-condition (e)
             (let ((fn (sb-pcl::generic-function-name
                        (sb-pcl::effective-method-condition-generic-function e))))
               (if (equal (find-package 'endb/sql/expr)
                          (symbol-package fn))
                   (error 'endb/sql/expr:sql-runtime-error
                          :message (format nil "Invalid argument types: ~A(~{~A~^, ~})"
                                           (ppcre:regex-replace "^SQL-(UNARY)?"
                                                                (symbol-name fn)
                                                                "")
                                           (loop for arg in (sb-pcl::effective-method-condition-args e)
                                                 collect (if (stringp arg)
                                                             (prin1-to-string arg)
                                                             (endb/sql/expr:syn-cast arg :varchar)))))
                   (error e))))))

(defun %interpret-sql-literal (ast)
  (cond
    ((or (stringp ast)
         (numberp ast)
         (vectorp ast))
     ast)
    ((eq :true ast) t)
    ((eq :false ast) nil)
    ((eq :null ast) :null)
    ((and (listp ast)
          (= 2 (length ast)))
     (case (first ast)
       (:- (if (numberp (second ast))
               (- (second ast))
               (error 'endb/sql/expr:sql-runtime-error :message "Invalid literal")))
       (:date (endb/sql/expr:sql-date (second ast)))
       (:time (endb/sql/expr:sql-time (second ast)))
       (:timestamp (endb/sql/expr:sql-datetime (second ast)))
       (:duration (endb/sql/expr:sql-duration (second ast)))
       (:blob (endb/sql/expr:sql-unhex (second ast)))
       (:array (fset:convert 'fset:seq (mapcar #'%interpret-sql-literal (second ast))))
       (:object (reduce
                 (lambda (acc kv)
                   (let ((k (first kv)))
                     (fset:with acc (if (stringp k)
                                        k
                                        (symbol-name k))
                                (%interpret-sql-literal (second kv)))))
                 (second ast)
                 :initial-value (fset:empty-map)))
       (t (error 'endb/sql/expr:sql-runtime-error :message "Invalid literal"))))
    ((and (listp ast)
          (eq :interval (first ast))
          (<= 2 (length (rest ast)) 3))
     (apply #'endb/sql/expr:syn-interval (rest ast)))
    (t (error 'endb/sql/expr:sql-runtime-error :message "Invalid literal"))))

(defun interpret-sql-literal (src)
  (let* ((select-list (handler-case
                          (cadr (%parse-sql (format nil "SELECT ~A" src)))
                        (endb/lib/parser:sql-parse-error (e)
                          (declare (ignore e)))))
         (ast (car select-list))
         (literal (if (or (not (= 1 (length select-list)))
                          (not (= 1 (length ast))))
                      :error
                      (handler-case
                          (%interpret-sql-literal (car ast))
                        (endb/sql/expr:sql-runtime-error (e)
                          (declare (ignore e))
                          :error)))))
    (if (eq :error literal)
        (error 'endb/sql/expr:sql-runtime-error
               :message (format nil "Invalid literal: ~A" src))
        literal)))
