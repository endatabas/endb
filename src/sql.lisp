(defpackage :endb/sql
  (:use :cl)
  (:export #:*interrupt-query-memory-usage-threshold* #:install-interrupt-query-handler
           #:make-db #:make-directory-db #:db-close #:make-dbms #:dbms-close #:begin-write-tx #:commit-write-tx #:execute-sql #:interpret-sql-literal)
  (:import-from :bordeaux-threads)
  (:import-from :cl-ppcre)
  (:import-from :endb/arrow)
  (:import-from :endb/json)
  (:import-from :endb/sql/db)
  (:import-from :endb/sql/expr)
  (:import-from :endb/sql/compiler)
  (:import-from :endb/lib/arrow)
  (:import-from :endb/lib/cst)
  (:import-from :endb/lib)
  (:import-from :endb/storage)
  (:import-from :endb/storage/buffer-pool)
  (:import-from :fset)
  (:import-from :trivia)
  (:import-from :uiop))
(in-package :endb/sql)

(defun make-db (&key (store (make-instance 'endb/storage:in-memory-store)))
  (endb/lib:init-lib)
  (let* ((buffer-pool (endb/storage/buffer-pool:make-buffer-pool :get-object-fn (lambda (path)
                                                                                  (endb/storage:store-get-object store path))))
         (meta-data (endb/storage:store-replay store)))
    (endb/sql/db:make-db :store store :buffer-pool buffer-pool :meta-data meta-data)))

(defun make-directory-db (&key (directory "endb_data"))
  (endb/lib:init-lib)
  (let* ((store (make-instance 'endb/storage:disk-store :directory directory)))
    (handler-case
        (make-db :store store)
      (error (e)
        (endb/storage:store-close store)
        (error e)))))

(defun db-close (db)
  (endb/queue:queue-close (endb/sql/db:db-indexer-queue db))
  (when (endb/sql/db:db-indexer-thread db)
    (bt:join-thread (endb/sql/db:db-indexer-thread db)))
  (endb/storage:store-close (endb/sql/db:db-store db))
  (endb/storage/buffer-pool:buffer-pool-close (endb/sql/db:db-buffer-pool db)))

(defun make-dbms (&key directory)
  (let ((dbms (endb/sql/db:make-dbms :db (if (and directory (not (equal ":memory:" directory)))
                                             (make-directory-db :directory directory)
                                             (make-db)))))
    (when directory
      (endb/sql/db:start-background-compaction
       dbms
       (lambda (tx-fn)
         (bt:with-lock-held ((endb/sql/db:dbms-write-lock dbms))
           (let ((write-db (begin-write-tx (endb/sql/db:dbms-db dbms))))
             (funcall tx-fn write-db)
             (setf (endb/sql/db:dbms-db dbms) (commit-write-tx (endb/sql/db:dbms-db dbms) write-db)))))
       (lambda (path buffer)
         (endb/storage:store-put-object (endb/sql/db:db-store (endb/sql/db:dbms-db dbms)) path buffer))))
    (endb/sql/db:start-background-indexer (endb/sql/db:dbms-db dbms))
    dbms))

(defun dbms-close (dbms)
  (endb/queue:queue-close (endb/sql/db:dbms-compaction-queue dbms))
  (when (endb/sql/db:dbms-compaction-thread dbms)
    (bt:join-thread (endb/sql/db:dbms-compaction-thread dbms)))
  (db-close (endb/sql/db:dbms-db dbms)))

(defun begin-write-tx (db)
  (let* ((bp (endb/storage/buffer-pool:make-writeable-buffer-pool :parent-pool (endb/sql/db:db-buffer-pool db)))
         (write-db (endb/sql/db:copy-db db)))
    (setf (endb/sql/db:db-buffer-pool write-db) bp)
    (setf (endb/sql/db:db-current-timestamp write-db) (endb/sql/db:syn-current_timestamp db))
    write-db))

(defun %execute-constraints (db)
  (fset:do-map (k v (endb/sql/db:constraint-definitions db))
    (when (equalp '(#(nil)) (handler-case
                                (execute-sql db v)
                              (endb/sql/expr:sql-runtime-error (e)
                                (endb/lib:log-warn "constraint ~A raised an error, ignoring: ~A" k e))))
      (error 'endb/sql/expr:sql-runtime-error :message (format nil "Constraint failed: ~A" k)))))

(defun commit-write-tx (current-db write-db &key (fsyncp t))
  (let* ((current-md (endb/sql/db:db-meta-data current-db))
         (tx-md (endb/sql/db:db-meta-data write-db))
         (tx-id (1+ (endb/sql/db:db-base-tx-id write-db))))
    (if (eq current-md tx-md)
        current-db
        (endb/lib:with-trace-kvs-span "commit" (fset:map ("tx_id" (format nil "~(~,'0x~)" tx-id)))
          (%execute-constraints write-db)
          (let* ((tx-md (fset:with tx-md "_last_tx" tx-id))
                 (md-diff (endb/json:json-diff current-md tx-md))
                 (md-diff (fset:with md-diff "_tx_log_version" endb/storage:*tx-log-version*))
                 (store (endb/sql/db:db-store write-db))
                 (bp (endb/sql/db:db-buffer-pool write-db))
                 (arrow-buffers-map (make-hash-table :test 'equal)))
            (maphash
             (lambda (k v)
               (let ((buffer (endb/lib/arrow:write-arrow-arrays-to-ipc-buffer v)))
                 (destructuring-bind (table batch-file)
                     (uiop:split-string k :max 2 :separator "/")
                   (let* ((table-md (fset:lookup md-diff table))
                          (batch-md (fset:map-union (fset:lookup table-md batch-file)
                                                    (fset:map ("sha1" (string-downcase (endb/lib:sha1 buffer)))
                                                              ("buffers_byte_size" (loop for b in (endb/arrow:arrow-all-buffers (first v))
                                                                                         sum (endb/lib:vector-byte-size b)))))))
                     (setf md-diff (fset:with md-diff table (fset:with table-md batch-file batch-md)))))
                 (setf (gethash k arrow-buffers-map) buffer)))
             (endb/storage/buffer-pool:writeable-buffer-pool-pool bp))
            (let ((new-md (endb/json:json-merge-patch current-md md-diff))
                  (current-local-time (endb/arrow:arrow-timestamp-micros-to-local-time (endb/sql/db:db-current-timestamp write-db))))
              (endb/storage:store-write-tx store tx-id new-md md-diff arrow-buffers-map :fsyncp fsyncp :mtime current-local-time)
              (let ((new-db (endb/sql/db:copy-db current-db)))
                (setf (endb/sql/db:db-meta-data new-db) new-md)
                new-db)))))))

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

(defun %compile-sql-fn (db sql)
  (let ((k (endb/sql/db:query-cache-key db sql)))
    (or (gethash k (endb/sql/db:db-query-cache db))
        (let* ((ast (endb/lib/cst:parse-sql-ast sql))
               (ctx (fset:map (:db db) (:sql sql))))
          (multiple-value-bind (sql-fn cachep)
              (multiple-value-bind (ast expected-parameters)
                  (%resolve-parameters ast)
                (if (eq :multiple-statments (first ast))
                    (let ((asts (second ast)))
                      (if (= 1 (length asts))
                          (endb/sql/compiler:compile-sql ctx (first asts) expected-parameters)
                          (values
                           (lambda (db parameters)
                             (handler-case
                                 (loop with end-idx = (length asts)
                                       for ast in asts
                                       for idx from 1
                                       for sql-fn = (endb/sql/compiler:compile-sql ctx ast expected-parameters)
                                       if (= end-idx idx)
                                         do (return (funcall sql-fn db parameters))
                                       else
                                         do (funcall sql-fn db parameters))
                               (endb/sql/db:sql-tx-error ()
                                 (error 'endb/sql/expr:sql-runtime-error :message "Explicit transactions not supported in multiple statements"))))
                           nil)))
                    (endb/sql/compiler:compile-sql ctx ast expected-parameters)))
            (when cachep
              (setf (gethash k (endb/sql/db:db-query-cache db)) sql-fn))
            (values sql-fn ast))))))

(defun %fset-values (m)
  (fset:reduce (lambda (acc k v)
                 (declare (ignore k))
                 (vector-push-extend v acc)
                 acc)
               m
               :initial-value (make-array 0 :fill-pointer 0)))

(defun %execute-sql (db sql parameters manyp)
  (when (and manyp (not (or (fset:seq? parameters)
                            (typep parameters 'endb/arrow:arrow-binary))))
    (error 'endb/sql/expr:sql-runtime-error :message "Many parameters must be an array"))
  (multiple-value-bind (sql-fn ast)
      (%compile-sql-fn db sql)
    (let* ((all-parameters (cond
                             ((typep parameters 'endb/arrow:arrow-binary)
                              (loop for batch in (endb/lib/arrow:read-arrow-arrays-from-ipc-buffer parameters)
                                    append (coerce batch 'list)))
                             (manyp (fset:convert 'list parameters) )
                             (t (list parameters)))))
      (trivia:match ast
        ((trivia:guard (list :insert table-name (list* :values (list (list* ps)) _) :column-names column-names)
                       (and manyp
                            (every (lambda (x)
                                     (equal '(:parameter) x))
                                   ps)
                            (every (lambda (x)
                                     (and (fset:seq? x) (= (length ps) (fset:size x))))
                                   all-parameters)))
         (endb/sql/db:dml-insert db
                                 (symbol-name table-name)
                                 (loop for ps in all-parameters
                                       collect (fset:convert 'vector ps))
                                 :column-names (mapcar #'symbol-name column-names)))
        ((trivia:guard (list :insert table-name (list :objects (list (list* :object (list* ps) _))))
                       (and manyp
                            (= 1 (fset:size (fset:convert 'fset:set (mapcar #'fset:domain all-parameters))))
                            (fset:equal? (fset:domain (first all-parameters))
                                         (fset:convert 'fset:set
                                                       (mapcar (lambda (x)
                                                                 (trivia:match x
                                                                   ((list :shorthand-property (list :parameter p))
                                                                    (symbol-name p))))
                                                               ps)))))
         (endb/sql/db:dml-insert-objects db
                                         (symbol-name table-name)
                                         all-parameters))
        ((trivia:guard (list :insert table-name (list* :objects (list (list :parameter)) _))
                       (and manyp
                            (every (lambda (x)
                                     (and (fset:seq? x) (= 1 (fset:size x))))
                                   all-parameters)))
         (endb/sql/db:dml-insert-objects db
                                         (symbol-name table-name)
                                         (loop for ps in all-parameters
                                               collect (fset:first ps))))
        (_ (loop with all-parameters = (loop for parameters in all-parameters
                                             collect (etypecase parameters
                                                       (fset:map parameters)
                                                       (fset:seq (fset:convert 'fset:map (loop for x in (fset:convert 'list parameters)
                                                                                               for idx from 0
                                                                                               collect (cons idx x))))
                                                       (t (error 'endb/sql/expr:sql-runtime-error :message "Parameters must be an array or an object"))))
                 with final-result = nil
                 with final-result-code = nil
                 for parameters in all-parameters
                 do (multiple-value-bind (result result-code)
                        (funcall sql-fn db parameters)
                      (setf final-result result)
                      (if (numberp result-code)
                          (setf final-result-code (+ result-code (or final-result-code 0)))
                          (setf final-result-code result-code)))
                 finally (return (values final-result final-result-code))))))))

(defparameter +active-queries+ (make-hash-table :synchronized t))

(defstruct active-query id sql start-time thread)

(defun execute-sql (db sql &optional (parameters (fset:empty-seq)) manyp)
  (let* ((query-id (endb/lib:uuid-v4))
         (kvs (fset:map ("query_id" query-id)
                        ("query_base_tx_id" (endb/sql/db:db-base-tx-id db))))
         (kvs (if (endb/sql/db:db-interactive-tx-id db)
                  (fset:with kvs "query_interactive_tx_id" (endb/sql/db:db-interactive-tx-id db))
                  kvs)))
    (endb/lib:with-trace-kvs-span "query" kvs
      (unwind-protect
           (let ((active-query (make-active-query :id query-id
                                                  :sql sql
                                                  :start-time (get-internal-real-time)
                                                  :thread (bt:current-thread))))
             (setf (gethash (bt:current-thread) +active-queries+) active-query)
             (endb/lib:log-debug "query start:~%~A" sql)
             (handler-case
                 #+sbcl (if (endb/lib:log-level-active-p :debug)
                            (sb-ext:call-with-timing
                             (lambda (&rest args)
                               (endb/lib:log-debug "query end:~%~A"
                                                   (with-output-to-string (out)
                                                     (let ((*trace-output* out))
                                                       (apply #'sb-impl::print-time args)))))
                             (lambda ()
                               (%execute-sql db sql parameters manyp)))
                            (%execute-sql db sql parameters manyp))
                 #-sbcl (%execute-sql db sql parameters manyp)
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
        (remhash (bt:current-thread) +active-queries+)))))

(defvar *interrupt-query-memory-usage-threshold* 0.6)

(defun install-interrupt-query-handler ()
  #+sbcl (push (lambda ()
                 (endb/lib:with-trace-span "gc"
                  (let* ((usage (sb-kernel:dynamic-usage))
                         (usage-ratio (coerce (/ usage (sb-ext:dynamic-space-size)) 'double-float)))
                    (endb/lib:log-debug "dynamic space usage: ~,2f%" (* 100 usage-ratio))
                    (endb/lib:log-debug "active queries: ~A" (hash-table-count +active-queries+))
                    (when (> usage-ratio *interrupt-query-memory-usage-threshold*)
                      (let ((oldest-active-query (cdr (first (sort (alexandria:hash-table-alist +active-queries+)
                                                                   #'<
                                                                   :key (lambda (x)
                                                                          (active-query-start-time (cdr x))))))))
                        (when oldest-active-query
                          (endb/lib:log-warn "interrupting query ~A on ~A to save memory, usage: ~A (~,2f%):~%~A"
                                             (active-query-id oldest-active-query)
                                             (bt:thread-name (active-query-thread oldest-active-query))
                                             usage usage-ratio
                                             (active-query-sql oldest-active-query))
                          (endb/lib:log-warn "room:~%~A" (with-output-to-string (out)
                                                           (let ((*standard-output* out))
                                                             (room t))))
                          (bt:interrupt-thread (active-query-thread oldest-active-query)
                                               (lambda ()
                                                 (signal 'endb/sql/db:sql-abort-query-error)))))))))
               sb-ext:*after-gc-hooks*))

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
          (eq :object (first ast))
          (>= (length ast) 2))
     (reduce
      (lambda (acc kv)
        (let ((k (first kv)))
          (fset:with acc (if (stringp k)
                             k
                             (symbol-name k))
                     (%interpret-sql-literal (second kv)))))
      (second ast)
      :initial-value (fset:empty-map)))
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
       (t (error 'endb/sql/expr:sql-runtime-error :message "Invalid literal"))))
    ((and (listp ast)
          (eq :interval (first ast))
          (<= 2 (length (rest ast)) 3))
     (apply #'endb/sql/expr:syn-interval (rest ast)))
    (t (error 'endb/sql/expr:sql-runtime-error :message "Invalid literal"))))

(defun interpret-sql-literal (src)
  (let* ((select-list (handler-case
                          (cadr (endb/lib/cst:parse-sql-ast (format nil "SELECT ~A" src)))
                        (endb/lib/cst:sql-parse-error (e)
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
