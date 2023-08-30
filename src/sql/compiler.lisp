(defpackage :endb/sql/compiler
  (:use :cl)
  (:import-from :endb/lib/parser)
  (:import-from :endb/sql/expr)
  (:import-from :endb/arrow)
  (:import-from :alexandria)
  (:import-from :fset)
  (:import-from :log4cl)
  (:export #:compile-sql))
(in-package :endb/sql/compiler)

(defgeneric sql->cl (ctx type &rest args))

(defun %anonymous-column-name (idx)
  (concatenate 'string "column" (princ-to-string idx)))

(defun %unqualified-column-name (column)
  (let* ((idx (position #\. column)))
    (if idx
        (subseq column (1+ idx))
        column)))

(defun %qualified-column-name (table-alias column)
  (concatenate 'string table-alias "." column))

(defun %qualified-asterisk-p (x)
  (and (listp x)
       (= 2 (length x))
       (eq :* (first x))
       (symbolp (second x))))

(defun %select-projection (select-list select-star-projection table-projections)
  (loop for idx from 1
        for (expr alias) in select-list
        append (cond
                 ((eq :* expr) select-star-projection)
                 (alias (list (symbol-name alias)))
                 ((symbolp expr) (list (%unqualified-column-name (symbol-name expr))))
                 ((%qualified-asterisk-p expr)
                  (let ((projection (fset:lookup table-projections (symbol-name (second expr)))))
                    (if projection
                        (mapcar #'%unqualified-column-name projection)
                        (%annotated-error (second expr) "Unknown table"))))
                 ((and (listp expr)
                       (eq :parameter (first expr))
                       (symbolp (second expr)))
                  (list (%unqualified-column-name (symbol-name (second expr)))))
                 ((and (listp expr)
                       (eq :object (first expr))
                       (= 1 (length (second expr)))
                       (eq :* (first (first (second expr)))))
                  (list (%unqualified-column-name (symbol-name (second (first (second expr)))))))
                 ((and (listp expr)
                       (eq :access (first expr))
                       (not (member (nth 2 expr) '(:* :#)))
                       (or (stringp (nth 2 expr))
                           (symbolp (nth 2 expr)))) (list (format nil "~A" (nth 2 expr))))
                 (t (list (%anonymous-column-name idx))))))

(defun %annotated-error (s message)
  (error 'endb/sql/expr:sql-runtime-error
         :message (endb/lib/parser:annotate-input-with-error (get s :input) message (get s :start) (get s :end))))

(defun %base-table-or-view->cl (ctx table-name &key temporal (errorp t))
  (let* ((db (fset:lookup ctx :db))
         (ctes (or (fset:lookup ctx :ctes) (fset:empty-map)))
         (cte (fset:lookup ctes (symbol-name table-name)))
         (table-type (endb/sql/expr:table-type db (symbol-name table-name))))
    (cond
      (cte
       (progn
         (dolist (cb (fset:lookup ctx :on-cte-access))
           (funcall cb (symbol-name table-name)))
         (values `(,(intern (symbol-name table-name))) (cte-projection cte) (cte-free-vars cte))))
      ((equal "BASE TABLE" table-type)
       (values (make-base-table :name (symbol-name table-name)
                                :temporal temporal
                                :size (endb/sql/expr:base-table-size db (symbol-name table-name)))
               (endb/sql/expr:table-columns db (symbol-name table-name))))
      ((equal "VIEW" table-type)
       (multiple-value-bind (src projection free-vars)
           (%ast->cl-with-free-vars ctx (endb/sql/expr:view-definition db (symbol-name table-name)))
         (declare (ignore projection))
         (values src (endb/sql/expr:table-columns db (symbol-name table-name)) free-vars)))
      (errorp (%annotated-error table-name "Unknown table")))))

(defun %wrap-with-order-by-and-limit (src order-by limit offset)
  (let* ((src (if order-by
                  `(endb/sql/expr:ra-order-by ,src ',order-by)
                  src))
         (src (if (and order-by limit)
                  `(endb/sql/expr:ra-limit ,src ',limit ',offset)
                  src)))
    src))

(defun %resolve-order-by (order-by projection &key allow-expr-p)
  (loop with expr-idx = (length projection)
        for (expr direction) in order-by
        for projected-idx = (when (symbolp expr)
                              (position (symbol-name expr) projection :test 'equal))
        collect (list (cond
                        ((numberp expr)
                         (if (<= 1 expr (length projection))
                             expr
                             (error 'endb/sql/expr:sql-runtime-error :message (format nil "ORDER BY index not in range: ~A" expr))))
                        (projected-idx (1+ projected-idx))
                        (allow-expr-p (incf expr-idx))
                        (t (if (symbolp expr)
                               (error 'endb/sql/expr:sql-runtime-error :message (format nil "Cannot resolve ORDER BY column: ~A" expr))
                               (error 'endb/sql/expr:sql-runtime-error :message (format nil "Invalid ORDER BY expression")))))
                      (or direction :asc))))

(defun %and-clauses (expr)
  (if (listp expr)
      (case (first expr)
        (:and (append (%and-clauses (second expr))
                      (%and-clauses (third expr))))
        (:or (append (list expr)
                     (intersection (%and-clauses (second expr))
                                   (%and-clauses (third expr))
                                   :key #'prin1-to-string
                                   :test 'equal)))
        (t (list expr)))
      (list expr)))

(defstruct base-table name temporal size)

(defstruct from-table src vars free-vars size projection alias)

(defstruct where-clause src free-vars ast)

(defstruct aggregate src init-src var where-src)

(defstruct cte src free-vars projection ast)

(defun %binary-predicate-p (x)
  (and (listp x)
       (= 3 (length x))))

(defun %equi-join-predicate-p (vars clause)
  (when (%binary-predicate-p clause)
    (destructuring-bind (op lhs rhs)
        clause
      (and (equal 'endb/sql/expr:sql-= op)
           (member lhs vars)
           (member rhs vars)))))

(defun %unique-vars (vars)
  (let ((seen ()))
    (loop for x in (reverse vars)
          if (not (member x seen))
            do (push x seen)
          else
            do (push nil seen)
          finally (return seen))))

(defun %flatten-from (from)
  (let ((from-element (first from)))
    (when from-element
      (append (if (eq :join (first from-element))
                  (destructuring-bind (table-1 table-2 &key on type)
                      (rest from-element)
                    (if (eq :left type)
                        (let ((table-alias (if (= 1 (length table-2))
                                               (first table-2)
                                               (second table-2))))
                          (append (%flatten-from (list table-1)) (list (list (list :left-join table-2 on) table-alias))))
                        (%flatten-from (list table-1 table-2))))
                  (list from-element))
              (%flatten-from (rest from))))))

(defun %from-where-clauses (from)
  (let ((from-element (first from)))
    (when from-element
      (append
       (when (eq :join (first from-element))
         (destructuring-bind (table-1 table-2 &key on type)
             (rest from-element)
           (if (eq :left type)
               (%from-where-clauses (list table-1))
               (append (%and-clauses on)
                       (%from-where-clauses (list table-1 table-2))))))
       (%from-where-clauses (rest from))))))

(defun %replace-all (smap x)
  (cond
    ((symbolp x) (or (cdr (assoc x smap)) x))
    ((listp x) (cons (%replace-all smap (car x))
                     (%replace-all smap (cdr x))))
    (t x)))

(defun %table-scan->cl (ctx vars projection from-src where-clauses nested-src)
  (let ((where-src (loop for clause in where-clauses
                         append `(when (eq t ,(where-clause-src clause))))))
    (if (base-table-p from-src)
        (alexandria:with-gensyms (table-md-sym
                                  arrow-file-md-sym
                                  deletes-md-sym batch-row-sym
                                  system-time-start-sym
                                  system-time-end-sym
                                  temporal-sym
                                  raw-deleted-row-ids-sym
                                  deleted-row-ids-sym
                                  lambda-sym
                                  batch-sym
                                  scan-row-id-sym
                                  scan-batch-idx-sym
                                  scan-arrow-file-sym)
          (let* ((table-name (base-table-name from-src))
                 (batch-sym (or (fset:lookup ctx :batch-sym) batch-sym))
                 (scan-row-id-sym (or (fset:lookup ctx :scan-row-id-sym) scan-row-id-sym))
                 (scan-batch-idx-sym (or (fset:lookup ctx :scan-batch-idx-sym) scan-batch-idx-sym))
                 (scan-arrow-file-sym (or (fset:lookup ctx :scan-arrow-file-sym) scan-arrow-file-sym)))
            (destructuring-bind (&optional (temporal-type :as-of temporal-type-p) (temporal-start :current_timestamp) (temporal-end temporal-start))
                (base-table-temporal from-src)
              `(let ((,table-md-sym (endb/sql/expr:base-table-meta ,(fset:lookup ctx :db-sym) ,table-name))
                     ,@(when temporal-type-p
                         `((,system-time-start-sym ,(ast->cl ctx temporal-start))
                           (,system-time-end-sym ,(ast->cl ctx temporal-end)))))
                 (fset:do-map (,scan-arrow-file-sym ,arrow-file-md-sym ,table-md-sym)
                   (loop with ,deletes-md-sym = (or (fset:lookup ,arrow-file-md-sym "deletes") (fset:empty-map))
                         for ,batch-row-sym in (endb/sql/expr:base-table-arrow-batches ,(fset:lookup ctx :db-sym) ,table-name ,scan-arrow-file-sym)
                         for ,batch-sym = (cdr (assoc ,table-name ,batch-row-sym :test 'equal))
                         for ,temporal-sym = (cdr (assoc "system_time_start" ,batch-row-sym :test 'equal))
                         for ,scan-batch-idx-sym from 0
                         for ,raw-deleted-row-ids-sym = (or (fset:lookup ,deletes-md-sym (prin1-to-string ,scan-batch-idx-sym))
                                                            (fset:empty-seq))
                         for ,deleted-row-ids-sym = ,(if temporal-type-p
                                                         `(fset:filter
                                                           (lambda (,lambda-sym)
                                                             (eq t (endb/sql/expr:sql-<= (fset:lookup ,lambda-sym "system_time_end") ,system-time-start-sym)))
                                                           ,raw-deleted-row-ids-sym)
                                                         raw-deleted-row-ids-sym)
                         do (loop for ,scan-row-id-sym of-type fixnum below (endb/arrow:arrow-length ,batch-sym)
                                  for ,vars = ,(if  endb/sql/expr:*sqlite-mode*
                                                    `(endb/arrow:arrow-struct-projection ,batch-sym ,scan-row-id-sym ',projection)
                                                    `(append (endb/arrow:arrow-struct-projection ,batch-sym ,scan-row-id-sym ',(remove "system_time" projection :test 'equal))
                                                             (list (endb/arrow:arrow-get ,batch-sym ,scan-row-id-sym)
                                                                   (fset:map ("start" (endb/arrow:arrow-get ,temporal-sym ,scan-row-id-sym))
                                                                             ("end" (endb/sql/expr:batch-row-system-time-end ,raw-deleted-row-ids-sym ,scan-row-id-sym))))))
                                  when (and ,(if temporal-type-p
                                                 `(eq t (,(case temporal-type
                                                            (:as-of 'endb/sql/expr:sql-<=)
                                                            (:from 'endb/sql/expr:sql-<)
                                                            (:between 'endb/sql/expr:sql-<=))
                                                         (endb/arrow:arrow-get ,temporal-sym ,scan-row-id-sym)
                                                         ,system-time-end-sym))
                                                 t)
                                            (not (fset:find ,scan-row-id-sym
                                                            ,deleted-row-ids-sym
                                                            :key (lambda (,lambda-sym)
                                                                   (fset:lookup ,lambda-sym "row_id")))))
                                    ,@where-src
                                  ,@nested-src)))))))
        `(loop for ,(%unique-vars vars)
                 in ,from-src
               ,@where-src
               ,@nested-src))))

(defun %join->cl (ctx from-table scan-where-clauses equi-join-clauses)
  (with-slots (src vars free-vars)
      from-table
    (multiple-value-bind (in-vars out-vars)
        (loop for (nil lhs rhs) in (mapcar #'where-clause-src equi-join-clauses)
              if (member lhs vars)
                collect lhs into out-vars
              else
                collect lhs into in-vars
              if (member rhs vars)
                collect rhs into out-vars
              else
                collect rhs into in-vars
              finally
                 (return (values in-vars out-vars)))
      (alexandria:with-gensyms (index-table-sym index-key-sym index-key-form-sym)
        (let* ((new-free-vars (set-difference free-vars in-vars))
               (index-sym (fset:lookup ctx :index-sym))
               (index-key-form `(list ',index-key-form-sym ,@new-free-vars)))
          `(gethash (list ,@in-vars)
                    (let ((,index-key-sym ,index-key-form))
                      (or (gethash ,index-key-sym ,index-sym)
                          (let ((,index-table-sym (setf (gethash ,index-key-sym ,index-sym)
                                                        (make-hash-table :test 'equal))))
                            ,(%table-scan->cl ctx
                                              vars
                                              (mapcar #'%unqualified-column-name (from-table-projection from-table))
                                              src
                                              scan-where-clauses
                                              `(do (push (list ,@vars)
                                                         (gethash (list ,@out-vars) ,index-table-sym))))
                            ,index-table-sym)))))))))

(defun %selection-with-limit-offset->cl (ctx selected-src &optional limit offset)
  (let ((acc-sym (fset:lookup ctx :acc-sym)))
    (if (or limit offset)
        (let* ((rows-sym (fset:lookup ctx :rows-sym))
               (block-sym (fset:lookup ctx :block-sym))
               (limit (if offset
                          (+ offset limit)
                          limit))
               (offset (or offset 0)))
          `(when (and (>= ,rows-sym ,offset)
                      (not (eql ,rows-sym ,limit)))
             do (progn
                  (incf ,rows-sym)
                  (push (list ,@selected-src) ,acc-sym))
             when (eql ,rows-sym ,limit)
             do (return-from ,block-sym ,acc-sym)))
        `(do (push (list ,@selected-src) ,acc-sym)))))

(defun %scan-where-clause-p (ast)
  (if (listp ast)
      (if (eq :not (first ast))
          (%scan-where-clause-p (second ast))
          (not (eq :exists (first ast))))
      t))

(defun %from->cl (ctx from-tables where-clauses selected-src &optional vars)
  (let* ((candidate-tables (remove-if-not (lambda (x)
                                            (subsetp (from-table-free-vars x) vars))
                                          from-tables))
         (from-table (or (find-if (lambda (x)
                                    (loop with vars = (append (from-table-vars x) vars)
                                          for c in where-clauses
                                          thereis (%equi-join-predicate-p vars (where-clause-src c))))
                                  candidate-tables)
                         (first candidate-tables)))
         (from-tables (remove from-table from-tables))
         (new-vars (from-table-vars from-table))
         (vars (append new-vars vars)))
    (multiple-value-bind (scan-clauses equi-join-clauses pushdown-clauses)
        (loop for c in where-clauses
              if (and (subsetp (where-clause-free-vars c) new-vars)
                      (%scan-where-clause-p (where-clause-ast c)))
                collect c into scan-clauses
              else if (%equi-join-predicate-p vars (where-clause-src c))
                     collect c into equi-join-clauses
              else if (subsetp (where-clause-free-vars c) vars)
                     collect c into pushdown-clauses
              finally
                 (return (values scan-clauses equi-join-clauses pushdown-clauses)))
      (let* ((new-where-clauses (append scan-clauses equi-join-clauses pushdown-clauses))
             (where-clauses (set-difference where-clauses new-where-clauses))
             (nested-src (if from-tables
                             `(do ,(%from->cl ctx from-tables where-clauses selected-src vars))
                             (append `(,@(loop for clause in where-clauses append `(when (eq t ,(where-clause-src clause)))))
                                     selected-src)))
             (projection (mapcar #'%unqualified-column-name (from-table-projection from-table))))
        (if equi-join-clauses
            (%table-scan->cl ctx
                             new-vars
                             projection
                             (%join->cl ctx from-table scan-clauses equi-join-clauses)
                             pushdown-clauses
                             nested-src)
            (%table-scan->cl ctx
                             new-vars
                             projection
                             (from-table-src from-table)
                             (append scan-clauses pushdown-clauses)
                             nested-src))))))

(defun %group-by->cl (ctx from-tables where-clauses selected-src limit offset group-by having-src correlated-vars)
  (alexandria:with-gensyms (group-acc-sym group-key-sym group-sym)
    (let* ((aggregate-table (fset:lookup ctx :aggregate-table))
           (group-by-projection (loop for g in group-by
                                      collect (ast->cl ctx g)))
           (group-by-exprs-projection (loop for k being the hash-key of aggregate-table
                                            collect k))
           (group-key-form `(list ,@group-by-projection))
           (init-srcs (loop for v being the hash-value of aggregate-table
                            collect (aggregate-init-src v)))
           (group-by-selected-src `(do (let* ((,group-key-sym ,group-key-form)
                                              (,group-sym  (gethash ,group-key-sym ,group-acc-sym)))
                                         (unless ,group-sym
                                           (setf ,group-sym (list ,@init-srcs))
                                           (setf (gethash ,group-key-sym ,group-acc-sym) ,group-sym))
                                         (destructuring-bind ,group-by-exprs-projection
                                             ,group-sym
                                           ,@(loop for v being the hash-value of aggregate-table
                                                   collect `(when (eq t ,(aggregate-where-src v))
                                                              (endb/sql/expr:agg-accumulate ,(aggregate-var v) ,@(aggregate-src v))))))))
           (empty-group-key-form `(list ,@(loop repeat (length group-by-projection) collect :null)))
           (group-by-src `(let ((,group-acc-sym (make-hash-table :test 'equal)))
                            ,(%from->cl ctx from-tables where-clauses group-by-selected-src correlated-vars)
                            (when (zerop (hash-table-count ,group-acc-sym))
                              (setf (gethash ,empty-group-key-form ,group-acc-sym)
                                    (list ,@init-srcs)))
                            ,group-acc-sym)))
      (append `(loop for ,(%unique-vars group-by-projection) being the hash-key
                       using (hash-value ,group-by-exprs-projection)
                         of ,group-by-src
                     when (eq t ,having-src))
              (%selection-with-limit-offset->cl ctx selected-src limit offset)))))

(defun %where-clause-selectivity-factor (clause)
  (let ((ast (where-clause-ast clause)))
    (case (when (listp ast)
            (first ast))
      (:= 10)
      (:in 5)
      (:exists 5)
      ((:between :like) 4)
      ((:< :<= :> :>=) 3)
      (:in-query 2)
      (:not (case (when (listp (second ast))
                    (first (second ast)))
              (:in 5)
              (:exists 5)
              (:in-query 2)
              (t 1)))
      (t 1))))

(defun %env-extension (table-alias projection)
  (reduce
   (lambda (acc column)
     (let* ((qualified-column (%qualified-column-name table-alias column))
            (column-sym (gensym (concatenate 'string qualified-column "__")))
            (acc (fset:with acc column column-sym)))
       (fset:with acc qualified-column column-sym)))
   projection
   :initial-value (fset:empty-map)))

(defun %recursive-select-p (ctx table-or-subquery)
  (and (symbolp table-or-subquery)
       (equal (fset:lookup ctx :current-cte)
              (symbol-name table-or-subquery))))

(defmethod sql->cl (ctx (type (eql :select)) &rest args)
  (destructuring-bind (select-list &key distinct (from '(((:values ((:null))) #:dual))) (where :true)
                                     (group-by () group-by-p) (having :true havingp)
                                     order-by limit offset)
      args
    (labels ((select->cl (ctx from-ast from-tables-acc)
               (destructuring-bind (table-or-subquery &optional table-alias column-names temporal)
                   (first from-ast)
                 (multiple-value-bind (table-src projection free-vars)
                     (cond
                       ((symbolp table-or-subquery)
                        (%base-table-or-view->cl ctx table-or-subquery :temporal temporal))
                       ((and (listp table-or-subquery)
                             (eq :objects (first table-or-subquery)))
                        (%ast->cl-with-free-vars (fset:with ctx :inside-from-p t) table-or-subquery))
                       (t (%ast->cl-with-free-vars ctx table-or-subquery)))
                   (when (and column-names (not (= (length projection) (length column-names))))
                     (if (symbolp table-or-subquery)
                         (%annotated-error table-or-subquery "Number of column names does not match projection")
                         (error 'endb/sql/expr:sql-runtime-error :message (format nil "Number of column names: ~A does not match projection: ~A"
                                                                                  (length column-names)
                                                                                  (length projection)))))
                   (let* ((table-alias (or table-alias table-or-subquery))
                          (projection (or (mapcar #'symbol-name column-names) projection))
                          (table-alias (%unqualified-column-name (symbol-name table-alias)))
                          (qualified-projection (loop for column in projection
                                                      collect (%qualified-column-name table-alias column)))
                          (projection (append projection (when (and (base-table-p table-src)
                                                                    (not endb/sql/expr:*sqlite-mode*))
                                                           (list "!doc" "system_time"))))
                          (projection (append projection (when (and (listp table-or-subquery)
                                                                    (eq :objects (first table-or-subquery))
                                                                    (not endb/sql/expr:*sqlite-mode*))
                                                           (list "!doc"))))
                          (env-extension (%env-extension table-alias projection))
                          (ctx (fset:map-union ctx env-extension))
                          (ctx (if (%recursive-select-p ctx table-or-subquery)
                                   (fset:with ctx :recursive-select t)
                                   ctx))
                          (ctx (fset:with ctx
                                          :table-projections
                                          (fset:with (or (fset:lookup ctx :table-projections) (fset:empty-map))
                                                     table-alias
                                                     qualified-projection)))
                          (from-table (make-from-table :src table-src
                                                       :vars (loop for p in projection
                                                                   collect (fset:lookup env-extension p))
                                                       :free-vars free-vars
                                                       :size (if (base-table-p table-src)
                                                                 (base-table-size table-src)
                                                                 most-positive-fixnum)
                                                       :projection qualified-projection
                                                       :alias table-alias))
                          (from-tables-acc (append from-tables-acc (list from-table))))
                     (if (rest from-ast)
                         (select->cl ctx (rest from-ast) from-tables-acc)
                         (let* ((aggregate-table (make-hash-table))
                                (ctx (fset:with ctx :aggregate-table aggregate-table))
                                (ctx (fset:less ctx :on-cte-access))
                                (full-projection (loop for from-table in from-tables-acc
                                                       append (from-table-projection from-table)))
                                (table-projections (fset:lookup ctx :table-projections))
                                (selected-src (loop for (expr) in select-list
                                                    append (cond
                                                             ((eq :* expr)
                                                              (loop for p in full-projection
                                                                    collect (ast->cl ctx (make-symbol p))))
                                                             ((%qualified-asterisk-p expr)
                                                              (loop for p in (fset:lookup table-projections (symbol-name (second expr)))
                                                                    collect (ast->cl ctx (make-symbol p))))
                                                             (t (list (ast->cl ctx expr))))))
                                (select-star-projection (mapcar #'%unqualified-column-name full-projection))
                                (select-projection (%select-projection select-list select-star-projection table-projections))
                                (order-by-selected-src (loop for (expr) in order-by
                                                             for projected-idx = (when (symbolp expr)
                                                                                   (position (symbol-name expr) select-projection :test 'equal))
                                                             unless (or projected-idx (numberp expr))
                                                               collect (ast->cl ctx expr)))
                                (selected-src (append selected-src order-by-selected-src))
                                (where-clauses (loop for clause in (append (%from-where-clauses from)
                                                                           (%and-clauses where))
                                                     collect (multiple-value-bind (src projection free-vars)
                                                                 (%ast->cl-with-free-vars ctx clause)
                                                               (declare (ignore projection))
                                                               (make-where-clause :src src
                                                                                  :free-vars free-vars
                                                                                  :ast clause))))
                                (from-tables (sort from-tables-acc #'< :key
                                                   (lambda (x)
                                                     (/ (from-table-size x)
                                                        (1+ (loop for clause in where-clauses
                                                                  when (subsetp (where-clause-free-vars clause) (from-table-vars x))
                                                                    sum (%where-clause-selectivity-factor clause)))))))
                                (having-src (ast->cl ctx having))
                                (limit (unless order-by
                                         limit))
                                (offset (unless order-by
                                          offset))
                                (group-by-needed-p (or group-by-p havingp (plusp (hash-table-count aggregate-table))))
                                (correlated-vars (append (set-difference (loop for clause in where-clauses
                                                                               append (where-clause-free-vars clause))
                                                                         (loop for from-table in from-tables
                                                                               append (from-table-vars from-table)))
                                                         (loop for (nil . cte) in (fset:convert 'list (fset:lookup ctx :ctes))
                                                               append (cte-free-vars cte)))))
                           (values
                            (if group-by-needed-p
                                (%group-by->cl ctx from-tables where-clauses selected-src limit offset group-by having-src correlated-vars)
                                (%from->cl ctx from-tables where-clauses (%selection-with-limit-offset->cl ctx selected-src limit offset) correlated-vars))
                            select-projection))))))))
      (alexandria:with-gensyms (block-sym acc-sym rows-sym row-sym)
        (let* ((ctx (fset:map-union ctx (fset:map (:rows-sym rows-sym)
                                                  (:acc-sym acc-sym)
                                                  (:block-sym block-sym))))
               (from (%flatten-from from)))
          (multiple-value-bind (src select-projection)
              (select->cl ctx from ())
            (let* ((src `(block ,block-sym
                           (let (,@(when (and limit (not order-by))
                                     (list `(,rows-sym 0)))
                                 (,acc-sym))
                             ,src
                             ,acc-sym)))
                   (src (if (eq :distinct distinct)
                            `(endb/sql/expr:ra-distinct ,src)
                            src))
                   (resolved-order-by (%resolve-order-by order-by select-projection :allow-expr-p t))
                   (src (%wrap-with-order-by-and-limit src resolved-order-by limit offset)))
              (values (if (loop for (idx) in resolved-order-by
                                thereis (> idx (length select-projection)))
                          `(loop for ,row-sym in ,src
                                 collect (subseq ,row-sym 0 ,(length select-projection)))
                          src)
                      select-projection))))))))

(defun %values-projection (arity)
  (loop for idx from 1 upto arity
        collect (%anonymous-column-name idx)))

(defmethod sql->cl (ctx (type (eql :values)) &rest args)
  (destructuring-bind (values-list &key order-by limit offset)
      args
    (let* ((arity (length (first values-list)))
           (projection (%values-projection arity)))
      (unless (apply #'= (mapcar #'length values-list))
        (error 'endb/sql/expr:sql-runtime-error :message (format nil "All VALUES must have the same number of columns: ~A" arity)))
      (values (%wrap-with-order-by-and-limit (ast->cl ctx values-list)
                                             (%resolve-order-by order-by projection) limit offset)
              projection))))

(defun %object-ast-keys (object &key (require-literal-p t))
  (loop for (k v) in (second object)
        for x = (cond
                  ((and (symbolp k)
                        (not (keywordp k)))
                   (symbol-name k))
                  ((eq :shorthand-property k)
                   (symbol-name (second v)))
                  ((stringp k) k)
                  (require-literal-p
                   (error 'endb/sql/expr:sql-runtime-error :message "All OBJECTS must have literal keys")))
        when x
          collect x))

(defmethod sql->cl (ctx (type (eql :objects)) &rest args)
  (destructuring-bind (objects-list &key order-by limit offset)
      args
    (let* ((projection (sort (delete-duplicates
                              (mapcan #'%object-ast-keys objects-list)
                              :test 'equal)
                             #'string<)))
      (alexandria:with-gensyms (object-sym key-sym)
        (values (%wrap-with-order-by-and-limit
                 `(loop for ,object-sym in ,(ast->cl (fset:less ctx :inside-from-p) objects-list)
                        collect (append (loop for ,key-sym in ',projection
                                              collect (endb/sql/expr:syn-access-finish ,object-sym ,key-sym nil))
                                        ,(when (fset:lookup ctx :inside-from-p)
                                           `(list ,object-sym))))
                 (%resolve-order-by order-by projection) limit offset)
                projection)))))

(defmethod sql->cl (ctx (type (eql :exists)) &rest args)
  (destructuring-bind (query)
      args
    `(endb/sql/expr:ra-exists ,(ast->cl ctx (append query '(:limit 1))))))

(defmethod sql->cl (ctx (type (eql :in)) &rest args)
  (destructuring-bind (expr query)
      args
    `(endb/sql/expr:ra-in ,(ast->cl ctx expr) ,(ast->cl ctx query))))

(defmethod sql->cl (ctx (type (eql :left-join)) &rest args)
  (destructuring-bind (table on)
      args
    (multiple-value-bind (src projection free-vars)
        (%ast->cl-with-free-vars ctx (list :select (list (list :*)) :from (list table) :where on))
      (values `(or ,src (list (list ,@(loop for idx below (length projection)
                                            collect :null))))
              projection
              free-vars))))

(defmethod sql->cl (ctx (type (eql :scalar-subquery)) &rest args)
  (destructuring-bind (query)
      args
    (multiple-value-bind (src projection free-vars)
        (%ast->cl-with-free-vars ctx query)
      (declare (ignore projection))
      (alexandria:with-gensyms (index-key-sym index-key-form-sym)
        (let* ((index-sym (fset:lookup ctx :index-sym))
               (index-key-form `(list ',index-key-form-sym ,@free-vars)))
          `(let* ((,index-key-sym ,index-key-form))
             (endb/sql/expr:ra-scalar-subquery
              (multiple-value-bind (result resultp)
                  (gethash ,index-key-sym ,index-sym)
                (if resultp
                    result
                    (setf (gethash ,index-key-sym ,index-sym)
                          ,src))))))))))

(defmethod sql->cl (ctx (type (eql :unnest)) &rest args)
  (destructuring-bind (exprs &key with-ordinality)
      args
    (values `(endb/sql/expr:ra-unnest ,(ast->cl ctx exprs) :with-ordinality ,with-ordinality)
            (%values-projection (if with-ordinality
                                    (1+ (length exprs))
                                    (length exprs))))))

(defun %compound-select->cl (ctx fn-name fn args)
  (destructuring-bind (lhs rhs &key order-by limit offset)
      args
    (multiple-value-bind (lhs-src lhs-projection)
        (ast->cl ctx lhs)
      (multiple-value-bind (rhs-src rhs-projection)
          (ast->cl ctx rhs)
        (unless (= (length lhs-projection)
                   (length rhs-projection))
          (error 'endb/sql/expr:sql-runtime-error
                 :message (format nil "Number of ~A left columns: ~A does not match right columns: ~A"
                                  fn-name
                                  (length lhs-projection)
                                  (length rhs-projection))))
        (values (%wrap-with-order-by-and-limit `(,fn ,lhs-src ,rhs-src)
                                               (%resolve-order-by order-by lhs-projection) limit offset)
                lhs-projection)))))

(defmethod sql->cl (ctx (type (eql :union)) &rest args)
  (%compound-select->cl ctx "UNION" 'endb/sql/expr:ra-union args))

(defmethod sql->cl (ctx (type (eql :union-all)) &rest args)
  (%compound-select->cl ctx "UNION ALL" 'endb/sql/expr:ra-union-all args))

(defmethod sql->cl (ctx (type (eql :except)) &rest args)
  (%compound-select->cl ctx "EXCEPT" 'endb/sql/expr:ra-except args))

(defmethod sql->cl (ctx (type (eql :intersect)) &rest args)
  (%compound-select->cl ctx "INTERSECT" 'endb/sql/expr:ra-intersect args))

(defmethod sql->cl (ctx (type (eql :+)) &rest args)
  (destructuring-bind (lhs &optional (rhs nil rhsp))
      args
    (if rhsp
        `(endb/sql/expr:sql-+ ,(ast->cl ctx lhs) ,(ast->cl ctx rhs))
        (ast->cl ctx lhs))))

(defmethod sql->cl (ctx (type (eql :-)) &rest args)
  (destructuring-bind (lhs &optional (rhs nil rhsp))
      args
    (if rhsp
        `(endb/sql/expr:sql-- ,(ast->cl ctx lhs) ,(ast->cl ctx rhs))
        (if (numberp lhs)
            (- lhs)
            `(endb/sql/expr:sql-- 0 ,(ast->cl ctx lhs))))))

(defmethod sql->cl (ctx (type (eql :create-index)) &rest args)
  (declare (ignore args))
  `(endb/sql/expr:ddl-create-index ,(fset:lookup ctx :db-sym)))

(defmethod sql->cl (ctx (type (eql :drop-index)) &rest args)
  (declare (ignore args))
  `(endb/sql/expr:ddl-drop-index ,(fset:lookup ctx :db-sym)))

(defmethod sql->cl (ctx (type (eql :create-table)) &rest args)
  (destructuring-bind (table-name column-names)
      args
    `(endb/sql/expr:ddl-create-table ,(fset:lookup ctx :db-sym) ,(symbol-name table-name) ',(mapcar #'symbol-name column-names))))

(defmethod sql->cl (ctx (type (eql :drop-table)) &rest args)
  (destructuring-bind (table-name &key if-exists)
      args
    `(endb/sql/expr:ddl-drop-table ,(fset:lookup ctx :db-sym) ,(symbol-name table-name) :if-exists ,(when if-exists
                                                                                                      t))))

(defparameter +create-assertion-scanner+ (ppcre:create-scanner "(?is)^\\s*CREATE.+?ASSERTION.+?\\s+CHECK\\s+\\((.+)\\)\\s*$"))

(defmethod sql->cl (ctx (type (eql :create-assertion)) &rest args)
  (destructuring-bind (constraint-name check-clause)
      args
    (ast->cl (fset:with ctx :no-parameters "Assertions do not support parameters")
             `(:select ((,check-clause))))
    (let ((assertion-matches (nth-value 1 (ppcre:scan-to-strings +create-assertion-scanner+ (fset:lookup ctx :sql)))))
      (unless (and (vectorp assertion-matches) (= 1 (length assertion-matches)))
        (%annotated-error constraint-name "Invalid assertion definition"))
      `(endb/sql/expr:ddl-create-assertion ,(fset:lookup ctx :db-sym) ,(symbol-name constraint-name) ,(aref assertion-matches 0)))))

(defmethod sql->cl (ctx (type (eql :drop-assertion)) &rest args)
  (destructuring-bind (constraint-name &key if-exists)
      args
    `(endb/sql/expr:ddl-drop-assertion ,(fset:lookup ctx :db-sym) ,(symbol-name constraint-name) :if-exists ,(when if-exists
                                                                                                                t))))

(defparameter +create-view-scanner+ (ppcre:create-scanner "(?is)^\\s*CREATE.+?VIEW.+?\\s+AS\\s+(.+)\\s*$"))

(defmethod sql->cl (ctx (type (eql :create-view)) &rest args)
  (destructuring-bind (table-name query &key column-names)
      args
    (multiple-value-bind (view-src projection)
        (ast->cl ctx query)
      (declare (ignore view-src))
      (when (and column-names (not (= (length projection) (length column-names))))
        (%annotated-error table-name "Number of column names does not match projection"))
      (let ((query-matches (nth-value 1 (ppcre:scan-to-strings +create-view-scanner+ (fset:lookup ctx :sql)))))
        (unless (and (vectorp query-matches) (= 1 (length query-matches)))
          (%annotated-error table-name "Invalid view definition"))
        `(endb/sql/expr:ddl-create-view ,(fset:lookup ctx :db-sym) ,(symbol-name table-name) ,(aref query-matches 0) ',(or (mapcar #'symbol-name column-names) projection))))))

(defmethod sql->cl (ctx (type (eql :drop-view)) &rest args)
  (destructuring-bind (view-name &key if-exists)
      args
    `(endb/sql/expr:ddl-drop-view ,(fset:lookup ctx :db-sym) ,(symbol-name view-name)  :if-exists ,(when if-exists
                                                                                                     t))))

(defun %insert-on-conflict (ctx table-name on-conflict update &key (values nil upsertp) column-names)
  (when upsertp
    (assert (or (and (eq :objects (first values)) (null column-names))
                column-names)
            nil))
  (destructuring-bind (update-cols &key (where :true) unset patch)
      (or update '(nil))
    (multiple-value-bind (from-src projection)
        (%base-table-or-view->cl ctx table-name :errorp (not upsertp))
      (when (or (base-table-p from-src) (null from-src))
        (alexandria:with-gensyms (scan-row-id-sym
                                  scan-batch-idx-sym
                                  scan-arrow-file-sym
                                  batch-sym
                                  object-sym
                                  updated-rows-sym
                                  deleted-row-ids-sym
                                  value-sym
                                  key-sym
                                  insertp-sym)
          (let* ((objectsp (eq :objects (first values)))
                 (on-conflict (delete-duplicates (mapcar #'symbol-name on-conflict) :test 'equal))
                 (excluded-projection (when upsertp
                                        (if objectsp
                                            (sort (delete-duplicates (loop for object in (second values)
                                                                           for keys = (%object-ast-keys object :require-literal-p nil)
                                                                           unless (subsetp on-conflict keys :test 'equal)
                                                                             do (%annotated-error table-name "All inserted values needs to provide the on conflict columns")
                                                                           append keys)
                                                                     :test 'equal)
                                                  #'string<)
                                            (if (subsetp on-conflict column-names :test 'equal)
                                                column-names
                                                (%annotated-error table-name "Column names needs to contain the on conflict columns")))))
                 (projection (append (delete-duplicates (append projection on-conflict) :test 'equal)
                                     (unless endb/sql/expr:*sqlite-mode*
                                       (list "!doc" "system_time"))))
                 (env-extension (%env-extension (symbol-name table-name) projection))
                 (vars (loop for p in projection
                             collect (fset:lookup env-extension p)))
                 (excluded-env-extension (%env-extension "excluded" excluded-projection))
                 (excluded-env-extension (fset:with excluded-env-extension "excluded.!doc" object-sym))
                 (ctx (fset:map-union (fset:map-union ctx (fset:map-union excluded-env-extension env-extension))
                                      (fset:map (:scan-row-id-sym scan-row-id-sym)
                                                (:scan-arrow-file-sym scan-arrow-file-sym)
                                                (:scan-batch-idx-sym scan-batch-idx-sym)
                                                (:batch-sym batch-sym))))
                 (conflict-clauses (when upsertp
                                     (loop for v in on-conflict
                                           collect (list :is
                                                         (make-symbol (format nil "~A.~A" table-name v))
                                                         (make-symbol (format nil "excluded.~A" v))))))
                 (where-clauses (loop for clause in (if upsertp
                                                        conflict-clauses
                                                        (%and-clauses where))
                                      collect (make-where-clause :src (ast->cl ctx clause)
                                                                 :ast clause)))
                 (update-paths (loop for (update-col expr) in update-cols
                                     append (list (if (symbolp update-col)
                                                      `(fset:seq ,(symbol-name update-col))
                                                      (ast->cl ctx update-col))
                                                  (ast->cl ctx expr))))
                 (unset-paths (loop for unset-col in unset
                                    collect (if (symbolp unset-col)
                                                `(fset:seq ,(symbol-name unset-col))
                                                (ast->cl ctx unset-col))))
                 (updated-columns (delete-duplicates
                                   (append (loop for col in (append (mapcar #'car update-cols) unset)
                                                 collect (if (symbolp col)
                                                             (symbol-name col)
                                                             (second col))))
                                   :test 'equal))
                 (update-src (when update
                               `((push (endb/sql/expr:sql-patch
                                        (endb/sql/expr:sql-path_remove
                                         (endb/sql/expr:sql-path_set
                                          (endb/arrow:arrow-get ,batch-sym ,scan-row-id-sym)
                                          ,@update-paths)
                                         ,@unset-paths)
                                        ,(if patch
                                             (ast->cl ctx patch)
                                             (fset:empty-map)))
                                       ,updated-rows-sym)
                                 (push (list ,scan-arrow-file-sym ,scan-batch-idx-sym ,scan-row-id-sym) ,deleted-row-ids-sym)))))
            (when (and update (null updated-columns) (null patch))
              (%annotated-error table-name "Update requires at least one set or unset column or patch"))
            (when (and upsertp (intersection on-conflict updated-columns :test 'equal))
              (%annotated-error table-name "Cannot update the on conflict columns"))
            `(let ((,updated-rows-sym)
                   (,deleted-row-ids-sym))
               ,(if upsertp
                    `(loop for ,object-sym in (let ((,object-sym ,(if objectsp
                                                                      (ast->cl ctx (second values))
                                                                      `(loop for ,value-sym in ,(ast->cl ctx values)
                                                                             collect (fset:convert 'fset:map (pairlis ',excluded-projection ,value-sym))))))
                                                (unless (= (length ,object-sym)
                                                           (length (delete-duplicates
                                                                    ,object-sym
                                                                    :test 'equal
                                                                    :key (lambda (,object-sym)
                                                                           (loop for ,key-sym in ',on-conflict
                                                                                 collect (endb/sql/expr:syn-access-finish ,object-sym ,key-sym nil))))))
                                                  (%annotated-error ',table-name "Inserted values cannot contain duplicated on conflict columns"))
                                                ,object-sym)
                           for ,(loop for v in excluded-projection
                                      collect (fset:lookup excluded-env-extension v))
                             = (loop for ,key-sym in ',excluded-projection
                                     collect (endb/sql/expr:syn-access-finish ,object-sym ,key-sym nil))
                           for ,insertp-sym = t
                           do
                           ,@(when from-src
                               (list (%table-scan->cl ctx vars projection from-src where-clauses
                                                      `(if (and ,@(loop for clause in (%and-clauses where)
                                                                        collect `(eq t ,(ast->cl ctx clause))))
                                                           do (setf ,insertp-sym nil)
                                                           ,@update-src
                                                           else
                                                           do (setf ,insertp-sym nil)))))
                             (when ,insertp-sym
                               (push ,object-sym ,updated-rows-sym)))
                    (%table-scan->cl ctx vars projection from-src where-clauses `(do ,@update-src)))
               (endb/sql/expr:dml-insert-objects ,(fset:lookup ctx :db-sym) ,(symbol-name table-name) ,updated-rows-sym)
               (endb/sql/expr:dml-delete ,(fset:lookup ctx :db-sym) ,(symbol-name table-name) ,deleted-row-ids-sym)
               (values nil (length ,updated-rows-sym)))))))))

(defmethod sql->cl (ctx (type (eql :insert)) &rest args)
  (destructuring-bind (table-name values &key column-names on-conflict update)
      args
    (when (and endb/sql/expr:*sqlite-mode* on-conflict)
      (%annotated-error table-name "Insert on conflict not supported in SQLite mode"))
    (when (and (not endb/sql/expr:*sqlite-mode*) (null column-names) (eq :values (first values)))
      (%annotated-error table-name "Column names are required for values"))
    (if (eq :objects (first values))
        (if on-conflict
            (%insert-on-conflict ctx table-name on-conflict update :values values)
            `(endb/sql/expr:dml-insert-objects ,(fset:lookup ctx :db-sym) ,(symbol-name table-name) ,(ast->cl ctx (second values))))
        (multiple-value-bind (src projection)
            (ast->cl ctx values)
          (let ((column-names (if (or column-names endb/sql/expr:*sqlite-mode*)
                                  (mapcar #'symbol-name column-names)
                                  projection)))
            (if on-conflict
                (%insert-on-conflict ctx table-name on-conflict update :values values :column-names column-names)
                `(endb/sql/expr:dml-insert ,(fset:lookup ctx :db-sym) ,(symbol-name table-name) ,src
                                           :column-names ',column-names)))))))

(defmethod sql->cl (ctx (type (eql :delete)) &rest args)
  (destructuring-bind (table-name &key (where :true))
      args
    (multiple-value-bind (from-src projection)
        (%base-table-or-view->cl ctx table-name)
      (when (base-table-p from-src)
        (alexandria:with-gensyms (scan-row-id-sym scan-batch-idx-sym scan-arrow-file-sym deleted-row-ids-sym)
          (let* ((env-extension (%env-extension (symbol-name table-name) projection))
                 (ctx (fset:map-union (fset:map-union ctx env-extension)
                                      (fset:map (:scan-row-id-sym scan-row-id-sym)
                                                (:scan-arrow-file-sym scan-arrow-file-sym)
                                                (:scan-batch-idx-sym scan-batch-idx-sym))))
                 (vars (loop for p in projection
                             collect (fset:lookup env-extension p)))
                 (where-clauses (loop for clause in (%and-clauses where)
                                      collect (make-where-clause :src (ast->cl ctx clause)
                                                                 :ast clause))))
            `(let ((,deleted-row-ids-sym))
               ,(%table-scan->cl ctx vars projection from-src where-clauses
                                 `(do (push (list ,scan-arrow-file-sym ,scan-batch-idx-sym ,scan-row-id-sym) ,deleted-row-ids-sym)))
               (endb/sql/expr:dml-delete ,(fset:lookup ctx :db-sym) ,(symbol-name table-name) ,deleted-row-ids-sym))))))))

(defmethod sql->cl (ctx (type (eql :update)) &rest args)
  (destructuring-bind (table-name update-cols &key (where :true) unset patch)
      args
    (%insert-on-conflict ctx table-name nil (list update-cols :where where :unset unset :patch patch))))

(defmethod sql->cl (ctx (type (eql :in-query)) &rest args)
  (destructuring-bind (expr query)
      args
    (multiple-value-bind (src projection free-vars)
        (if (symbolp query)
            (ast->cl ctx (list :select (list (list :*)) :from (list (list query))))
            (%ast->cl-with-free-vars ctx query))
      (unless (= 1 (length projection))
        (error 'endb/sql/expr:sql-runtime-error :message "IN query must return single column"))
      (alexandria:with-gensyms (in-var-sym expr-sym index-table-sym index-key-sym index-key-form-sym)
        (let* ((index-key-form `(list ',index-key-form-sym ,@free-vars)))
          `(let* ((,index-key-sym ,index-key-form)
                  (,index-table-sym (or (gethash ,index-key-sym ,(fset:lookup ctx :index-sym))
                                        (loop with ,index-table-sym = (setf (gethash ,index-key-sym ,(fset:lookup ctx :index-sym))
                                                                            (make-hash-table :test 'equal))
                                              for (,in-var-sym) in ,src
                                              do (setf (gethash ,in-var-sym ,index-table-sym)
                                                       (if (eq :null ,in-var-sym)
                                                           :null
                                                           t))
                                              finally (return ,index-table-sym))))
                  (,expr-sym ,(ast->cl ctx expr)))
             (or (gethash ,expr-sym ,index-table-sym)
                 (gethash :null ,index-table-sym)
                 (when (and (eq :null ,expr-sym)
                            (plusp (hash-table-count ,index-table-sym)))
                   :null))))))))

(defmethod sql->cl (ctx (type (eql :subquery)) &rest args)
  (destructuring-bind (query)
      args
    (ast->cl ctx query)))

(defmethod sql->cl (ctx (type (eql :cast)) &rest args)
  (destructuring-bind (x sql-type)
      args
    `(endb/sql/expr:sql-cast ,(ast->cl ctx x) ,(intern (string-upcase (symbol-name sql-type)) :keyword))))

(defmethod sql->cl (ctx (type (eql :interval)) &rest args)
  (destructuring-bind (x from &optional to)
      args
    `(endb/sql/expr:sql-interval ,(ast->cl ctx x) ,from ,to)))

(defmethod sql->cl (ctx (type (eql :date)) &rest args)
  (destructuring-bind (x)
      args
    (endb/sql/expr:sql-date x)))

(defmethod sql->cl (ctx (type (eql :time)) &rest args)
  (destructuring-bind (x)
      args
    (endb/sql/expr:sql-time x)))

(defmethod sql->cl (ctx (type (eql :timestamp)) &rest args)
  (destructuring-bind (x)
      args
    (endb/sql/expr:sql-timestamp x)))

(defmethod sql->cl (ctx (type (eql :blob)) &rest args)
  (destructuring-bind (x)
      args
    (endb/sql/expr:sql-unhex x)))

(defmethod sql->cl (ctx (type (eql :glob)) &rest args)
  (destructuring-bind (x y)
      args
    `(endb/sql/expr:sql-glob ,(ast->cl ctx y) ,(ast->cl ctx x))))

(defmethod sql->cl (ctx (type (eql :regexp)) &rest args)
  (destructuring-bind (x y)
      args
    `(endb/sql/expr:sql-regexp ,(ast->cl ctx y) ,(ast->cl ctx x))))

(defmethod sql->cl (ctx (type (eql :match)) &rest args)
  (destructuring-bind (x y)
      args
    `(endb/sql/expr:sql-match ,(ast->cl ctx y) ,(ast->cl ctx x))))

(defmethod sql->cl (ctx (type (eql :array)) &rest args)
  (destructuring-bind (args)
      args
    (alexandria:with-gensyms (acc-sym spread-sym)
      `(let ((,acc-sym (make-array 0 :fill-pointer 0)))
         ,@(loop for ast in args
                 collect (if (and (listp ast)
                                  (eq :spread-property (first ast)))
                             `(let* ((,spread-sym ,(ast->cl ctx (second ast)))
                                     (,spread-sym (if (fset:seq? ,spread-sym)
                                                      (fset:convert 'vector ,spread-sym)
                                                      ,spread-sym)))
                                (when (vectorp ,spread-sym)
                                  (loop for ,spread-sym across ,spread-sym
                                        do (vector-push-extend (if (characterp ,spread-sym)
                                                                   (princ-to-string ,spread-sym)
                                                                   ,spread-sym)
                                                               ,acc-sym))))
                             `(vector-push-extend ,(ast->cl ctx ast) ,acc-sym)))
         (fset:convert 'fset:seq ,acc-sym)))))

(defmethod sql->cl (ctx (type (eql :array-query)) &rest args)
  (destructuring-bind (query)
      args
    (multiple-value-bind (src projection)
        (ast->cl ctx query)
      (unless (= 1 (length projection))
        (error 'endb/sql/expr:sql-runtime-error :message "ARRAY query must return single column"))
      `(fset:convert 'fset:seq (mapcar #'car ,src)))))

(defmethod sql->cl (ctx (type (eql :object)) &rest args)
  (destructuring-bind (args)
      args
    `(fset:convert
      'fset:map
      (append ,@(loop for kv in args
                      collect (case (first kv)
                                (:shorthand-property
                                 (let* ((k (second kv))
                                        (k (if (and (listp k)
                                                    (eq :parameter (first k)))
                                               (second k)
                                               k)))
                                   `(list (cons ,(%unqualified-column-name (symbol-name k))
                                                ,(ast->cl ctx (second kv))))))
                                (:computed-property
                                 `(list (cons (endb/sql/expr:sql-cast ,(ast->cl ctx (second kv)) :varchar)
                                              ,(ast->cl ctx (nth 2 kv)))))
                                (:*
                                 (let* ((k (second kv))
                                        (doc (fset:lookup ctx (%qualified-column-name (symbol-name k) "!doc")))
                                        (projection (fset:lookup (fset:lookup ctx :table-projections) (symbol-name k))))
                                   (cond
                                     (doc `(fset:convert 'list ,doc))
                                     (projection `(list ,@(loop for p in projection
                                                                collect `(cons ,(%unqualified-column-name p)
                                                                               ,(ast->cl ctx (make-symbol p))))))
                                     (t (%annotated-error k "Unknown table")))))
                                (:spread-property
                                 (alexandria:with-gensyms (spread-sym idx-sym)
                                   `(let* ((,spread-sym ,(ast->cl ctx (second kv)))
                                           (,spread-sym (if (fset:seq? ,spread-sym)
                                                            (fset:convert 'vector ,spread-sym)
                                                            ,spread-sym)))
                                      (cond
                                        ((typep ,spread-sym 'endb/arrow:arrow-struct)
                                         (fset:convert 'list ,spread-sym))
                                        ((vectorp ,spread-sym)
                                         (loop for ,spread-sym across ,spread-sym
                                               for ,idx-sym from 0
                                               collect (cons (format nil "~A" ,idx-sym) (if (characterp ,spread-sym)
                                                                                            (princ-to-string ,spread-sym)
                                                                                            ,spread-sym))))))))
                                (t `(list (cons ,(if (symbolp (first kv))
                                                     (symbol-name (first kv))
                                                     (first kv))
                                                ,(ast->cl ctx (second kv)))))))))))

(defmethod sql->cl (ctx (type (eql :access)) &rest args)
  (destructuring-bind (base path &key recursive)
      args
    `(,(if (eq :seq (fset:lookup ctx :access))
           'endb/sql/expr:syn-access
           'endb/sql/expr:syn-access-finish)
      ,(ast->cl (fset:with ctx :access :seq) base)
      ,(cond
         ((member path '(:* :#)) path)
         ((symbolp path) (symbol-name path))
         (t (ast->cl (fset:less ctx :access) path)))
      ,(eq :recursive recursive))))

(defun %find-recursive-cte (ast)
  (let ((init-operator)
        (init-ast))
    (labels ((walk (ast)
               (cond
                 ((and (listp ast)
                       (member (first ast) '(:union :union-all)))
                  (if (and (listp (second ast))
                           (member (first (second ast))
                                   '(:union :union-all :intersect :except)))
                      (append (list (first ast))
                              (list (walk (second ast)))
                              (nthcdr 2 ast))
                      (progn
                        (setf init-operator (first ast))
                        (setf init-ast (second ast))
                        (append (nth 2 ast)
                                (nthcdr 3 ast)))))
                 ((and (listp ast)
                       (member (first ast) '(:intersect :except)))
                  (append (list (first ast))
                          (list (walk (second ast)))
                          (nthcdr 2 ast)))
                 (t ast))))
      (values (walk ast) init-operator init-ast))))

(defun %with-recursive->cl (ctx ctes query)
  (let* ((new-ctes (reduce
                    (lambda (acc cte)
                      (destructuring-bind (cte-name cte-ast &optional cte-columns)
                          cte
                        (unless cte-columns
                          (%annotated-error cte-name "WITH RECURSIVE requires named columns"))
                        (alexandria:with-gensyms (cte-varying-free-var)
                          (let ((cte (make-cte :src nil
                                               :free-vars (list cte-varying-free-var)
                                               :projection (mapcar #'symbol-name cte-columns)
                                               :ast cte-ast)))
                            (fset:with acc (symbol-name cte-name) cte)))))
                    ctes
                    :initial-value (fset:empty-map)))
         (ctx (fset:with ctx :ctes (fset:map-union (or (fset:lookup ctx :ctes) (fset:empty-map)) new-ctes)))
         (cte-free-vars (loop for (cte-name) in ctes
                              for cte = (fset:lookup new-ctes (symbol-name cte-name))
                              collect (first (cte-free-vars cte)))))
    (multiple-value-bind (src projection)
        (ast->cl ctx query)
      (values `(let ,(loop for var in cte-free-vars
                           collect (list var 0))
                 (declare (ignorable ,@cte-free-vars))
                 (labels ,(loop for (cte-name) in ctes
                                for cte = (fset:lookup new-ctes (symbol-name cte-name))
                                collect
                                (let* ((ctx (fset:with ctx :current-cte (symbol-name cte-name)))
                                       (src (multiple-value-bind (cte-ast init-operator init-ast)
                                                (%find-recursive-cte (cte-ast cte))
                                              (if init-operator
                                                  (multiple-value-bind (src projection)
                                                      (let* ((cte-accessed)
                                                             (ctx (fset:with
                                                                   ctx
                                                                   :on-cte-access
                                                                   (cons (lambda (k)
                                                                           (when (equal k (symbol-name cte-name))
                                                                             (if cte-accessed
                                                                                 (%annotated-error cte-name "Non-linear recursion not supported")
                                                                                 (setf cte-accessed t))))
                                                                         (fset:lookup ctx :on-cte-access)))))
                                                        (ast->cl ctx cte-ast))
                                                    (multiple-value-bind (init-src init-projection)
                                                        (let* ((ctx (fset:with
                                                                     ctx
                                                                     :on-cte-access
                                                                     (cons (lambda (k)
                                                                             (when (equal k (symbol-name cte-name))
                                                                               (%annotated-error cte-name "Left recursion not supported")))
                                                                           (fset:lookup ctx :on-cte-access)))))
                                                          (ast->cl ctx init-ast))
                                                      (unless (= (length projection) (length init-projection) (length (cte-projection cte)))
                                                        (%annotated-error cte-name "Number of column names does not match projection"))
                                                      (alexandria:with-gensyms (acc-sym last-acc-sym cte-sym)
                                                        (let ((distinct (when (eq :union init-operator)
                                                                          :distinct)))
                                                          `(block ,cte-sym
                                                             (let* ((,last-acc-sym (endb/sql/expr:ra-distinct ,init-src ,distinct))
                                                                    (,acc-sym ,last-acc-sym))
                                                               (flet ((,(intern (symbol-name cte-name)) ()
                                                                        (incf ,(first (cte-free-vars cte)))
                                                                        ,last-acc-sym))
                                                                 (loop
                                                                   (if ,last-acc-sym
                                                                       (progn
                                                                         (setf ,last-acc-sym (endb/sql/expr:ra-distinct ,src ,distinct))
                                                                         (setf ,acc-sym (append ,acc-sym ,last-acc-sym)))
                                                                       (return-from ,cte-sym (endb/sql/expr:ra-distinct ,acc-sym ,distinct)))))))))))
                                                  (multiple-value-bind (src projection)
                                                      (let* ((ctx (fset:with
                                                                   ctx
                                                                   :on-cte-access
                                                                   (cons (lambda (k)
                                                                           (when (equal k (symbol-name cte-name))
                                                                             (%annotated-error cte-name "Recursion not supported without UNION / UNION ALL")))
                                                                         (fset:lookup ctx :on-cte-access)))))
                                                        (ast->cl ctx cte-ast))
                                                    (unless (= (length projection) (length (cte-projection cte)))
                                                      (%annotated-error cte-name "Number of column names does not match projection"))
                                                    src)))))
                                  `(,(intern (symbol-name cte-name)) () ,src)))
                   ,src))
              projection))))

(defmethod sql->cl (ctx (type (eql :with)) &rest args)
  (destructuring-bind (ctes query &key recursive)
      args
    (if recursive
        (%with-recursive->cl ctx ctes query)
        (let* ((new-ctes (reduce
                          (lambda (acc cte)
                            (destructuring-bind (cte-name cte-ast &optional cte-columns)
                                cte
                              (multiple-value-bind (src projection)
                                  (ast->cl (fset:with ctx :ctes (fset:map-union (or (fset:lookup ctx :ctes) (fset:empty-map)) acc)) cte-ast)
                                (when (and cte-columns (not (= (length projection) (length cte-columns))))
                                  (%annotated-error cte-name "Number of column names does not match projection"))
                                (let ((cte (make-cte :src src
                                                     :projection (or (mapcar #'symbol-name cte-columns) projection))))
                                  (fset:with acc (symbol-name cte-name) cte)))))
                          ctes
                          :initial-value (fset:empty-map)))
               (ctx (fset:with ctx :ctes (fset:map-union (or (fset:lookup ctx :ctes) (fset:empty-map)) new-ctes))))
          (multiple-value-bind (src projection)
              (ast->cl ctx query)
            (values `(flet ,(loop for (cte-name) in ctes
                                  for cte = (fset:lookup new-ctes (symbol-name cte-name))
                                  collect `(,(intern (symbol-name cte-name)) () ,(cte-src cte)))
                       ,src)
                    projection))))))

(defun %find-expr-symbol (fn prefix)
  (find-symbol (string-upcase (concatenate 'string prefix (symbol-name fn))) :endb/sql/expr))

(defun %valid-sql-fn-call-p (fn-sym fn args)
  (handler-case
      (let ((sql-fn (symbol-function fn-sym)))
        #+sbcl (let* ((arg-list (if (typep sql-fn 'standard-generic-function)
                                    (sb-pcl::generic-function-pretty-arglist sql-fn)
                                    (sb-impl::%fun-lambda-list sql-fn)))
                      (optional-idx (position '&optional arg-list))
                      (rest-idx (position '&rest arg-list))
                      (optional-args (if optional-idx
                                         (- (or rest-idx (length arg-list)) optional-idx)
                                         0))
                      (rest-idx (if (and optional-idx rest-idx)
                                    (1- rest-idx)
                                    rest-idx))
                      (min-args (or optional-idx rest-idx (length arg-list)))
                      (max-args (if rest-idx
                                    (max (+ min-args optional-args) (length args))
                                    (+ min-args optional-args))))
                 (unless (<= min-args (length args) max-args)
                   (error 'endb/sql/expr:sql-runtime-error
                          :message (format nil "Invalid number of arguments: ~A to: ~A min: ~A max: ~A"
                                           (length args)
                                           (string-upcase (symbol-name fn))
                                           min-args
                                           max-args))))
        t)
    (undefined-function (e)
      (declare (ignore e)))))

(defmethod sql->cl (ctx (type (eql :path)) &rest args)
  (destructuring-bind (args)
      args
    `(fset:seq ,@(loop for ast in args
                       collect (cond
                                 ((and (symbolp ast)
                                       (not (keywordp ast)))
                                  (symbol-name ast))
                                 ((eq :# ast)
                                  (symbol-name ast))
                                 (t (ast->cl ctx ast)))))))

(defmethod sql->cl (ctx (type (eql :function)) &rest args)
  (destructuring-bind (fn args)
      args
    (let ((fn-sym (%find-expr-symbol fn "sql-")))
      (unless (and fn-sym (%valid-sql-fn-call-p fn-sym fn args))
        (error 'endb/sql/expr:sql-runtime-error :message (format nil "Unknown built-in function: ~A" fn)))
      `(,fn-sym ,@(loop for ast in args
                        collect (ast->cl ctx ast))))))

(defmethod sql->cl (ctx (type (eql :aggregate-function)) &rest args)
  (destructuring-bind (fn args &key distinct (where :true) order-by)
      args
    (if (and (member fn '(:min :max))
             (> (length args) 1))
        (ast->cl ctx (cons fn args))
        (progn
          (when (fset:lookup ctx :aggregate)
            (error 'endb/sql/expr:sql-runtime-error :message (format nil "Cannot nest aggregate functions: ~A" fn)))
          (when (fset:lookup ctx :recursive-select)
            (error 'endb/sql/expr:sql-runtime-error :message (format nil "Cannot use aggregate functions in recursion: ~A" fn)))
          (let ((min-args (if (eq :object_agg fn)
                              2
                              1))
                (max-args (if (member fn '(:group_concat :object_agg))
                              2
                              1))
                (args (if (eq :count-star fn)
                          (list :null)
                          args)))
            (unless (<= min-args (length args) max-args)
              (error 'endb/sql/expr:sql-runtime-error
                     :message (format nil "Invalid number of arguments: ~A to: ~A min: ~A max: ~A"
                                      (length args)
                                      (string-upcase (symbol-name fn))
                                      min-args
                                      max-args)))
            (alexandria:with-gensyms (aggregate-sym)
              (let* ((aggregate-table (fset:lookup ctx :aggregate-table))
                     (ctx (fset:with ctx :aggregate t))
                     (fn-sym (%find-expr-symbol fn "agg-"))
                     (init-src (if order-by
                                   `(endb/sql/expr:make-agg ,fn :order-by ',(loop for (nil dir) in order-by
                                                                                  for idx from (1+ (length args))
                                                                                  collect (list idx dir)))
                                   `(endb/sql/expr:make-agg ,fn :distinct ,distinct)))
                     (src (loop for ast in (append args (mapcar #'first order-by))
                                collect (ast->cl ctx ast)))
                     (where-src (ast->cl ctx where))
                     (agg (make-aggregate :src src :init-src init-src :var aggregate-sym :where-src where-src)))
                (assert fn-sym nil (format nil "Unknown aggregate function: ~A" fn))
                (setf (gethash aggregate-sym aggregate-table) agg)
                `(endb/sql/expr:agg-finish ,aggregate-sym))))))))

(defmethod sql->cl (ctx (type (eql :case)) &rest args)
  (destructuring-bind (cases-or-expr &optional cases)
      args
    (alexandria:with-gensyms (expr-sym)
      `(let ((,expr-sym ,(if cases
                             (ast->cl ctx cases-or-expr)
                             t)))
         (cond
           ,@(append (loop for (test then) in (or cases cases-or-expr)
                           collect (list (if (eq :else test)
                                             t
                                             `(eq t (endb/sql/expr:sql-= ,expr-sym ,(ast->cl ctx test))))
                                         (ast->cl ctx then)))
                     (list (list t :null))))))))

(defmethod sql->cl (ctx (type (eql :parameter)) &rest args)
  (when (fset:lookup ctx :no-parameters)
    (error 'endb/sql/expr:sql-runtime-error :message (fset:lookup ctx :no-parameters)))
  (destructuring-bind (parameter)
      args
    `(fset:lookup ,(fset:lookup ctx :param-sym)
                  ,(if (symbolp parameter)
                       (symbol-name parameter)
                       parameter))))

(defmethod sql->cl (ctx fn &rest args)
  (sql->cl ctx :function fn args))

(defun %ast-function-call-p (ast)
  (and (listp ast)
       (keywordp (first ast))
       (not (member (first ast) '(:null :true :false)))))

(defun ast->cl (ctx ast)
  (cond
    ((eq :true ast) t)
    ((eq :false ast) nil)
    ((eq :current_date ast)
     `(endb/sql/expr:syn-current_date ,(fset:lookup ctx :db-sym)))
    ((eq :current_time ast)
     `(endb/sql/expr:syn-current_time ,(fset:lookup ctx :db-sym)))
    ((eq :current_timestamp ast)
     `(endb/sql/expr:syn-current_timestamp ,(fset:lookup ctx :db-sym)))
    ((%ast-function-call-p ast)
     (apply #'sql->cl ctx ast))
    ((listp ast)
     (cond
       ((some (lambda (x)
                (or (%ast-function-call-p x)
                    (and (symbolp x)
                         (not (keywordp x)))
                    (listp x)))
              ast)
        (cons 'list (loop for ast in ast
                          collect (ast->cl ctx ast))))
       ((fset:lookup ctx :quote)
        (loop for ast in ast
              collect (ast->cl ctx ast)))
       (t (let ((ctx (fset:with ctx :quote t)))
            (list 'quote (loop for ast in ast
                               collect (ast->cl ctx ast)))))))
    ((and (symbolp ast)
          (not (keywordp ast)))
     (let* ((k (symbol-name ast))
            (v (fset:lookup ctx k)))
       (if v
           (progn
             (dolist (cb (fset:lookup ctx :on-var-access))
               (funcall cb k v))
             v)
           (let* ((idx (position #\. k)))
             (if idx
                 (let ((column (subseq k 0 idx))
                       (path (subseq k (1+ idx))))
                   (if (fset:lookup ctx column)
                       (ast->cl ctx (list :access (make-symbol column) path))
                       (%annotated-error ast "Unknown column")))
                 (%annotated-error ast "Unknown column"))))))
    (t ast)))

(defun %ast->cl-with-free-vars (ctx ast)
  (let* ((vars ())
         (ctx (fset:with
               ctx
               :on-var-access
               (cons (lambda (k v)
                       (when (eq v (fset:lookup ctx k))
                         (pushnew v vars)))
                     (fset:lookup ctx :on-var-access)))))
    (multiple-value-bind (src projection)
        (ast->cl ctx ast)
      (values src projection vars))))

(defvar *interpreter-from-limit* 8)

(defun %interpretp (ast)
  (when (listp ast)
    (case (first ast)
      ((:create-table :create-index :drop-table :drop-view :create-assertion :drop-assertion) t)
      (:insert (member (first (third ast)) '(:values :objects)))
      (:select (let ((from (cdr (getf ast :from))))
                 (or (> (length from)
                        *interpreter-from-limit*)
                     (null from)))))))

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

(defun compile-sql (ctx ast)
  (let ((*print-length* 16)
        (*print-level* 8))
    (log:debug ast)
    (alexandria:with-gensyms (db-sym index-sym param-sym)
      (let* ((ctx (fset:map-union ctx (fset:map (:db-sym db-sym)
                                                (:index-sym index-sym)
                                                (:param-sym param-sym)))))
        (multiple-value-bind (ast parameters)
            (%resolve-parameters ast)
          (multiple-value-bind (src projection)
              (ast->cl ctx ast)
            (log:debug src)
            (let* ((src (if projection
                            `(values ,src ',projection)
                            src))
                   (src `(lambda (,db-sym &optional ,param-sym)
                           (declare (optimize (speed 3) (safety 0) (debug 0) (compilation-speed 3)))
                           (declare (ignorable ,db-sym ,param-sym))
                           (unless (fset:equal? (fset:convert 'fset:set ',parameters)
                                                (fset:domain ,param-sym))
                             (error 'endb/sql/expr:sql-runtime-error :message (format nil "Required parameters: ~A does not match given: ~A"
                                                                                      (fset:convert 'list ',parameters)
                                                                                      (fset:convert 'list (fset:domain ,param-sym)))))
                           (let ((,index-sym (make-hash-table :test 'equal)))
                             (declare (ignorable ,index-sym))
                             ,src))))
              #+sbcl (let ((sb-ext:*evaluator-mode* (if (%interpretp ast)
                                                        :interpret
                                                        sb-ext:*evaluator-mode*)))
                       (eval src))
              #-sbcl (eval src))))))))
