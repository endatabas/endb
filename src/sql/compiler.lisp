(defpackage :endb/sql/compiler
  (:use :cl)
  (:import-from :endb/lib/parser)
  (:import-from :endb/sql/expr)
  (:import-from :endb/arrow)
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

(defun %select-projection (select-list select-star-projection table-by-alias)
  (loop for idx from 1
        for (expr alias) in select-list
        append (cond
                 ((eq :* expr) select-star-projection)
                 (alias (list (symbol-name alias)))
                 ((symbolp expr) (list (%unqualified-column-name (symbol-name expr))))
                 ((%qualified-asterisk-p expr)
                  (mapcar #'%unqualified-column-name (gethash (symbol-name (second expr)) table-by-alias)))
                 ((and (listp expr)
                       (eq :access (first expr))
                       (not (eq :* (nth 2 expr)))
                       (or (stringp (nth 2 expr))
                           (symbolp (nth 2 expr)))) (list (format nil "~A" (nth 2 expr))))
                 (t (list (%anonymous-column-name idx))))))

(defun %base-table-p (ctx table-name)
  (and (symbolp table-name)
       (equal "BASE TABLE" (endb/sql/expr:table-type (fset:lookup ctx :db) (symbol-name table-name)))))

(defun %annotated-error (s message)
  (error 'endb/sql/expr:sql-runtime-error
         :message (endb/lib/parser:annotate-input-with-error (get s :input) message (get s :start) (get s :end))))

(defun %base-table-or-view->cl (ctx table-name)
  (let* ((db (fset:lookup ctx :db))
         (ctes (or (fset:lookup ctx :ctes) (fset:empty-map)))
         (cte (fset:lookup ctes (symbol-name table-name)))
         (table-type (endb/sql/expr:table-type db (symbol-name table-name))))
    (cond
      (cte
       (multiple-value-bind (ast projection free-vars)
           (%ast->cl-with-free-vars ctx (cte-src cte))
         (declare (ignore projection))
         (values ast (cte-projection cte) free-vars)))
      ((equal "BASE TABLE" table-type)
       (values (symbol-name table-name)
               (endb/sql/expr:table-columns db (symbol-name table-name))
               ()
               (endb/sql/expr:base-table-size db (symbol-name table-name))))
      ((equal "VIEW" table-type)
       (multiple-value-bind (ast projection free-vars)
           (%ast->cl-with-free-vars ctx (endb/sql/expr:view-definition db (symbol-name table-name)))
         (declare (ignore projection))
         (values ast (endb/sql/expr:table-columns db (symbol-name table-name)) free-vars)))
      (t (%annotated-error table-name "Unknown table")))))

(defun %wrap-with-order-by-and-limit (src order-by limit offset)
  (let* ((src (if order-by
                  `(endb/sql/expr::%sql-order-by ,src ',order-by)
                  src))
         (src (if (and order-by limit)
                  `(endb/sql/expr::%sql-limit ,src ',limit ',offset)
                  src)))
    src))

(defun %resolve-order-by (order-by projection)
  (loop for (col direction) in order-by
        collect (list (if (symbolp col)
                          (1+ (position (symbol-name col) projection :test 'equal))
                          col)
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

(defstruct from-table src vars free-vars size projection alias base-table-p)

(defstruct where-clause src free-vars ast)

(defstruct aggregate src init-src var where-src)

(defstruct cte src projection)

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

(defun %table-scan->cl (ctx vars projection from-src where-clauses nested-src &optional base-table-p)
  (let ((where-src (loop for clause in where-clauses
                         append `(when (eq t ,(where-clause-src clause))))))
    (if base-table-p
        (let* ((table-name from-src)
               (table-md-sym (gensym))
               (arrow-file-md-sym (gensym))
               (deletes-md-sym (gensym))
               (batch-sym (gensym))
               (scan-row-id-sym (or (fset:lookup ctx :scan-row-id-sym) (gensym)))
               (scan-batch-idx-sym (or (fset:lookup ctx :scan-batch-idx-sym) (gensym)))
               (scan-arrow-file-sym (or (fset:lookup ctx :scan-arrow-file-sym) (gensym)))
               (deleted-row-ids-sym (gensym)))
          `(let ((,table-md-sym (endb/sql/expr:base-table-meta ,(fset:lookup ctx :db-sym) ,table-name)))
             (fset:do-map (,scan-arrow-file-sym ,arrow-file-md-sym ,table-md-sym)
               (loop with ,deletes-md-sym = (or (fset:lookup ,arrow-file-md-sym "deletes") (fset:empty-map))
                     for ,batch-sym in (endb/sql/expr:base-table-arrow-batches ,(fset:lookup ctx :db-sym) ,table-name ,scan-arrow-file-sym)
                     for ,scan-batch-idx-sym from 0
                     for ,deleted-row-ids-sym = (or (fset:lookup ,deletes-md-sym (prin1-to-string ,scan-batch-idx-sym)) (fset:empty-seq))
                     do (loop for ,scan-row-id-sym of-type fixnum below (endb/arrow:arrow-length ,batch-sym)
                              for ,vars = (endb/arrow:arrow-struct-projection ,batch-sym ,scan-row-id-sym ',projection)
                              unless (fset:find ,scan-row-id-sym ,deleted-row-ids-sym)
                                ,@where-src
                              ,@nested-src)))))
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
      (let* ((new-free-vars (set-difference free-vars in-vars))
             (index-sym (fset:lookup ctx :index-sym))
             (index-table-sym (gensym))
             (index-key-sym (gensym))
             (index-key-form `(list ',(gensym) ,@new-free-vars)))
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
                                                       (gethash (list ,@out-vars) ,index-table-sym)))
                                            (from-table-base-table-p from-table))
                          ,index-table-sym))))))))

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
                             nested-src
                             (from-table-base-table-p from-table)))))))

(defun %group-by->cl (ctx from-tables where-clauses selected-src limit offset group-by having-src correlated-vars)
  (let* ((aggregate-table (fset:lookup ctx :aggregate-table))
         (group-by-projection (loop for g in group-by
                                    collect (ast->cl ctx g)))
         (group-by-exprs-projection (loop for k being the hash-key of aggregate-table
                                          collect k))
         (group-acc-sym (gensym))
         (group-key-sym (gensym))
         (group-sym (gensym))
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
                                                            (endb/sql/expr:sql-agg-accumulate ,(aggregate-var v) ,@(aggregate-src v))))))))
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
            (%selection-with-limit-offset->cl ctx selected-src limit offset))))

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

(defmethod sql->cl (ctx (type (eql :select)) &rest args)
  (destructuring-bind (select-list &key distinct (from '(((:values ((:null))) #:dual))) (where :true)
                                     (group-by () group-by-p) (having :true havingp)
                                     order-by limit offset)
      args
    (labels ((select->cl (ctx from-ast from-tables-acc)
               (destructuring-bind (table-or-subquery &optional table-alias column-names)
                   (first from-ast)
                 (multiple-value-bind (table-src projection free-vars table-size)
                     (if (symbolp table-or-subquery)
                         (%base-table-or-view->cl ctx table-or-subquery)
                         (%ast->cl-with-free-vars ctx table-or-subquery))
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
                          (env-extension (%env-extension table-alias projection))
                          (ctx (fset:map-union ctx env-extension))
                          (from-table (make-from-table :src table-src
                                                       :vars (loop for p in projection
                                                                   collect (fset:lookup env-extension p))
                                                       :free-vars free-vars
                                                       :size (or table-size most-positive-fixnum)
                                                       :projection qualified-projection
                                                       :alias table-alias
                                                       :base-table-p (%base-table-p ctx table-or-subquery)))
                          (from-tables-acc (append from-tables-acc (list from-table))))
                     (if (rest from-ast)
                         (select->cl ctx (rest from-ast) from-tables-acc)
                         (let* ((aggregate-table (make-hash-table))
                                (ctx (fset:with ctx :aggregate-table aggregate-table))
                                (full-projection (loop for from-table in from-tables-acc
                                                       append (from-table-projection from-table)))
                                (table-by-alias (reduce
                                                 (lambda (acc from-table)
                                                   (setf (gethash (from-table-alias from-table) acc)
                                                         (from-table-projection from-table))
                                                   acc)
                                                 from-tables-acc
                                                 :initial-value (make-hash-table :test 'equal)))
                                (selected-src (loop for (expr) in select-list
                                                    append (cond
                                                             ((eq :* expr)
                                                              (loop for p in full-projection
                                                                    collect (ast->cl ctx (make-symbol p))))
                                                             ((%qualified-asterisk-p expr)
                                                              (loop for p in (gethash (symbol-name (second expr)) table-by-alias)
                                                                    collect (ast->cl ctx (make-symbol p))))
                                                             (t (list (ast->cl ctx expr))))))
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
                                (correlated-vars (set-difference (loop for clause in where-clauses
                                                                       append (where-clause-free-vars clause))
                                                                 (loop for from-table in from-tables
                                                                       append (from-table-vars from-table)))))
                           (values
                            (if group-by-needed-p
                                (%group-by->cl ctx from-tables where-clauses selected-src limit offset group-by having-src correlated-vars)
                                (%from->cl ctx from-tables where-clauses (%selection-with-limit-offset->cl ctx selected-src limit offset) correlated-vars))
                            full-projection
                            table-by-alias))))))))
      (let* ((block-sym (gensym))
             (acc-sym (gensym))
             (rows-sym (gensym))
             (ctx (fset:map-union ctx (fset:map (:rows-sym rows-sym)
                                                (:acc-sym acc-sym)
                                                (:block-sym block-sym))))
             (from (%flatten-from from)))
        (multiple-value-bind (src full-projection table-by-alias)
            (select->cl ctx from ())
          (let* ((src `(block ,block-sym
                         (let (,@(when (and limit (not order-by))
                                   (list `(,rows-sym 0)))
                               (,acc-sym))
                           ,src
                           ,acc-sym)))
                 (src (if (eq :distinct distinct)
                          `(endb/sql/expr::%sql-distinct ,src)
                          src))
                 (select-star-projection (mapcar #'%unqualified-column-name full-projection))
                 (select-projection (%select-projection select-list select-star-projection table-by-alias))
                 (src (%wrap-with-order-by-and-limit src (%resolve-order-by order-by select-projection) limit offset)))
            (values src select-projection)))))))

(defun %values-projection (arity)
  (loop for idx from 1 upto arity
        collect (%anonymous-column-name idx)))

(defmethod sql->cl (ctx (type (eql :values)) &rest args)
  (destructuring-bind (values-list &key order-by limit offset)
      args
    (let ((projection (%values-projection (length (first values-list)))))
      (values (%wrap-with-order-by-and-limit (ast->cl ctx values-list)
                                             (%resolve-order-by order-by projection) limit offset)
              projection))))

(defmethod sql->cl (ctx (type (eql :exists)) &rest args)
  (destructuring-bind (query)
      args
    `(endb/sql/expr:sql-exists ,(ast->cl ctx (append query '(:limit 1))))))

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
      (let* ((index-sym (fset:lookup ctx :index-sym))
             (index-key-sym (gensym))
             (index-key-form `(list ',(gensym) ,@free-vars)))
        `(let* ((,index-key-sym ,index-key-form))
           (endb/sql/expr:sql-scalar-subquery
            (multiple-value-bind (result resultp)
                (gethash ,index-key-sym ,index-sym)
              (if resultp
                  result
                  (setf (gethash ,index-key-sym ,index-sym)
                        ,src)))))))))

(defmethod sql->cl (ctx (type (eql :unnest)) &rest args)
  (destructuring-bind (expr &key with-ordinality)
      args
    (values `(endb/sql/expr:sql-unnest ,(ast->cl ctx expr) :with-ordinality ,with-ordinality)
            (%values-projection (if with-ordinality
                                    2
                                    1)))))

(defmethod sql->cl (ctx (type (eql :union)) &rest args)
  (destructuring-bind (lhs rhs &key order-by limit offset)
      args
    (multiple-value-bind (lhs-src projection)
        (ast->cl ctx lhs)
      (values (%wrap-with-order-by-and-limit `(endb/sql/expr:sql-union ,lhs-src ,(ast->cl ctx rhs))
                                             (%resolve-order-by order-by projection) limit offset)
              projection))))

(defmethod sql->cl (ctx (type (eql :union-all)) &rest args)
  (destructuring-bind (lhs rhs &key order-by limit offset)
      args
    (multiple-value-bind (lhs-src projection)
        (ast->cl ctx lhs)
      (values (%wrap-with-order-by-and-limit `(endb/sql/expr:sql-union-all ,lhs-src ,(ast->cl ctx rhs))
                                             (%resolve-order-by order-by projection) limit offset)
              projection))))

(defmethod sql->cl (ctx (type (eql :except)) &rest args)
  (destructuring-bind (lhs rhs &key order-by limit offset)
      args
    (multiple-value-bind (lhs-src projection)
        (ast->cl ctx lhs)
      (values (%wrap-with-order-by-and-limit `(endb/sql/expr:sql-except ,lhs-src ,(ast->cl ctx rhs))
                                             (%resolve-order-by order-by projection) limit offset)
              projection))))

(defmethod sql->cl (ctx (type (eql :intersect)) &rest args)
  (destructuring-bind (lhs rhs &key order-by limit offset)
      args
    (multiple-value-bind (lhs-src projection)
        (ast->cl ctx lhs)
      (values (%wrap-with-order-by-and-limit `(endb/sql/expr:sql-intersect ,lhs-src ,(ast->cl ctx rhs))
                                             (%resolve-order-by order-by projection) limit offset)
              projection))))

(defmethod sql->cl (ctx (type (eql :+)) &rest args)
  (destructuring-bind (lhs &optional (rhs nil rhsp))
      args
    (if rhsp
        `(endb/sql/expr:sql-+ ,(ast->cl ctx lhs) ,(ast->cl ctx rhs))
        `(endb/sql/expr:sql-unary+ ,(ast->cl ctx lhs)))))

(defmethod sql->cl (ctx (type (eql :-)) &rest args)
  (destructuring-bind (lhs &optional (rhs nil rhsp))
      args
    (if rhsp
        `(endb/sql/expr:sql-- ,(ast->cl ctx lhs) ,(ast->cl ctx rhs))
        `(endb/sql/expr:sql-unary- ,(ast->cl ctx lhs)))))

(defmethod sql->cl (ctx (type (eql :create-table)) &rest args)
  (destructuring-bind (table-name column-names)
      args
    `(endb/sql/expr:sql-create-table ,(fset:lookup ctx :db-sym) ,(symbol-name table-name) ',(mapcar #'symbol-name column-names))))

(defmethod sql->cl (ctx (type (eql :create-index)) &rest args)
  (declare (ignore args))
  `(endb/sql/expr:sql-create-index ,(fset:lookup ctx :db-sym)))

(defmethod sql->cl (ctx (type (eql :drop-index)) &rest args)
  (declare (ignore args))
  `(endb/sql/expr:sql-drop-index ,(fset:lookup ctx :db-sym)))

(defmethod sql->cl (ctx (type (eql :drop-table)) &rest args)
  (destructuring-bind (table-name &key if-exists)
      args
    `(endb/sql/expr:sql-drop-table ,(fset:lookup ctx :db-sym) ,(symbol-name table-name) :if-exists ,(when if-exists
                                                                                                      t))))

(defmethod sql->cl (ctx (type (eql :create-view)) &rest args)
  (destructuring-bind (table-name query &key column-names)
      args
    (multiple-value-bind (ast projection)
        (ast->cl ctx query)
      (declare (ignore ast))
      (when (and column-names (not (= (length projection) (length column-names))))
        (%annotated-error table-name "Number of column names does not match projection"))
      `(endb/sql/expr:sql-create-view ,(fset:lookup ctx :db-sym) ,(symbol-name table-name) ',query ',(or (mapcar #'symbol-name column-names) projection)))))

(defmethod sql->cl (ctx (type (eql :drop-view)) &rest args)
  (destructuring-bind (view-name &key if-exists)
      args
    `(endb/sql/expr:sql-drop-view ,(fset:lookup ctx :db-sym) ,(symbol-name view-name)  :if-exists ,(when if-exists
                                                                                                     t))))

(defmethod sql->cl (ctx (type (eql :insert)) &rest args)
  (destructuring-bind (table-name values &key column-names)
      args
    (when (and (not endb/sql/expr:*sqlite-mode*) (null column-names))
      (%annotated-error table-name "Column names are required"))
    `(endb/sql/expr:sql-insert ,(fset:lookup ctx :db-sym) ,(symbol-name table-name) ,(ast->cl ctx values)
                               :column-names ',(mapcar #'symbol-name column-names))))

(defmethod sql->cl (ctx (type (eql :insert-objects)) &rest args)
  (destructuring-bind (table-name values)
      args
    (let ((object-sym (gensym)))
      `(progn
         (loop for ,object-sym in ,(ast->cl ctx values)
               do (endb/sql/expr:sql-insert ,(fset:lookup ctx :db-sym) ,(symbol-name table-name)
                                            (list (mapcar #'cdr ,object-sym))
                                            :column-names (mapcar #'car ,object-sym)))
         (values nil ,(length values))))))

(defmethod sql->cl (ctx (type (eql :delete)) &rest args)
  (destructuring-bind (table-name where)
      args
    (multiple-value-bind (from-src projection)
        (%base-table-or-view->cl ctx table-name)
      (when (%base-table-p ctx table-name)
        (let* ((scan-row-id-sym (gensym))
               (scan-batch-idx-sym (gensym))
               (scan-arrow-file-sym (gensym))
               (env-extension (%env-extension (symbol-name table-name) projection))
               (ctx (fset:map-union (fset:map-union ctx env-extension)
                                    (fset:map (:scan-row-id-sym scan-row-id-sym)
                                              (:scan-arrow-file-sym scan-arrow-file-sym)
                                              (:scan-batch-idx-sym scan-batch-idx-sym))))
               (vars (loop for p in projection
                           collect (fset:lookup env-extension p)))
               (where-clauses (loop for clause in (%and-clauses where)
                                    collect (make-where-clause :src (ast->cl ctx clause)
                                                               :ast clause)))
               (deleted-row-ids-sym (gensym)))
          `(let ((,deleted-row-ids-sym))
             ,(%table-scan->cl ctx vars projection from-src where-clauses
                               `(do (push (list ,scan-arrow-file-sym ,scan-batch-idx-sym ,scan-row-id-sym) ,deleted-row-ids-sym))
                               t)
             (endb/sql/expr:sql-delete ,(fset:lookup ctx :db-sym) ,(symbol-name table-name) ,deleted-row-ids-sym)))))))

(defmethod sql->cl (ctx (type (eql :update)) &rest args)
  (destructuring-bind (table-name update-cols &key (where :true))
      args
    (multiple-value-bind (from-src projection)
        (%base-table-or-view->cl ctx table-name)
      (when (%base-table-p ctx table-name)
        (let* ((scan-row-id-sym (gensym))
               (scan-batch-idx-sym (gensym))
               (scan-arrow-file-sym (gensym))
               (env-extension (%env-extension (symbol-name table-name) projection))
               (ctx (fset:map-union (fset:map-union ctx env-extension)
                                    (fset:map (:scan-row-id-sym scan-row-id-sym)
                                              (:scan-arrow-file-sym scan-arrow-file-sym)
                                              (:scan-batch-idx-sym scan-batch-idx-sym))))
               (vars (loop for p in projection
                           collect (fset:lookup env-extension p)))
               (where-clauses (loop for clause in (%and-clauses where)
                                    collect (make-where-clause :src (ast->cl ctx clause)
                                                               :ast clause)))
               (update-cols (reverse update-cols))
               (update-col-names (mapcar (lambda (x)
                                           (symbol-name (first x)))
                                         update-cols))
               (update-select-list (loop for column in projection
                                         for (update-col expr) = (find column update-cols
                                                                       :key (lambda (x)
                                                                              (symbol-name (first x)))
                                                                       :test 'equal)
                                         if update-col
                                           collect (ast->cl ctx expr)
                                         else
                                           collect (ast->cl ctx (make-symbol column))))
               (updated-rows-sym (gensym))
               (deleted-row-ids-sym (gensym)))
          (when (subsetp update-col-names projection :test 'equal)
            `(let ((,updated-rows-sym)
                   (,deleted-row-ids-sym))
               ,(%table-scan->cl ctx vars projection from-src where-clauses
                                 `(do (push (list ,@update-select-list) ,updated-rows-sym)
                                      (push (list ,scan-arrow-file-sym ,scan-batch-idx-sym ,scan-row-id-sym) ,deleted-row-ids-sym))
                                 t)
               (endb/sql/expr:sql-delete ,(fset:lookup ctx :db-sym) ,(symbol-name table-name) ,deleted-row-ids-sym)
               (endb/sql/expr:sql-insert ,(fset:lookup ctx :db-sym) ,(symbol-name table-name) ,updated-rows-sym :column-names ',projection))))))))

(defmethod sql->cl (ctx (type (eql :in-query)) &rest args)
  (destructuring-bind (expr query)
      args
    (multiple-value-bind (src projection free-vars)
        (if (symbolp query)
            (ast->cl ctx (list :select (list (list :*)) :from (list (list query))))
            (%ast->cl-with-free-vars ctx query))
      (unless (= 1 (length projection))
        (error 'endb/sql/expr:sql-runtime-error :message "IN query must return single column"))
      (let* ((in-var-sym (gensym))
             (expr-sym (gensym))
             (index-table-sym (gensym))
             (index-key-sym (gensym))
             (index-key-form `(list ',(gensym) ,@free-vars)))
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
                 :null)))))))

(defmethod sql->cl (ctx (type (eql :subquery)) &rest args)
  (destructuring-bind (query)
      args
    (ast->cl ctx query)))

(defmethod sql->cl (ctx (type (eql :cast)) &rest args)
  (destructuring-bind (x sql-type)
      args
    `(endb/sql/expr:sql-cast ,(ast->cl ctx x) ,(intern (symbol-name sql-type) :keyword))))

(defmethod sql->cl (ctx (type (eql :date)) &rest args)
  (destructuring-bind (s)
      args
    `(endb/sql/expr:sql-date ,s)))

(defmethod sql->cl (ctx (type (eql :time)) &rest args)
  (destructuring-bind (s)
      args
    `(endb/sql/expr:sql-time ,s)))

(defmethod sql->cl (ctx (type (eql :timestamp)) &rest args)
  (destructuring-bind (s)
      args
    `(endb/sql/expr:sql-datetime ,s)))

(defmethod sql->cl (ctx (type (eql :array)) &rest args)
  (destructuring-bind (args)
      args
    (let ((acc-sym (gensym)))
      `(let ((,acc-sym (make-array 0 :fill-pointer 0)))
         ,@(loop for ast in args
                 collect (if (and (listp ast)
                                  (eq :spread-property (first ast)))
                             (let ((spread-sym (gensym)))
                               `(let ((,spread-sym ,(ast->cl ctx (second ast))))
                                  (when (vectorp ,spread-sym)
                                    (loop for ,spread-sym across ,spread-sym
                                          do (vector-push-extend ,spread-sym ,acc-sym)))))
                             `(vector-push-extend ,(ast->cl ctx ast) ,acc-sym)))
         ,acc-sym))))

(defmethod sql->cl (ctx (type (eql :array-query)) &rest args)
  (destructuring-bind (query)
      args
    (multiple-value-bind (src projection)
        (ast->cl ctx query)
      (unless (= 1 (length projection))
        (error 'endb/sql/expr:sql-runtime-error :message "ARRAY query must return single column"))
      `(map 'vector #'car ,src))))

(defmethod sql->cl (ctx (type (eql :object)) &rest args)
  (destructuring-bind (args)
      args
    (if args
        `(delete-duplicates
          (append ,@(loop for kv in args
                          collect (case (first kv)
                                    (:shorthand-property
                                     `(list (cons ,(%unqualified-column-name (symbol-name (second kv)))
                                                  ,(ast->cl ctx (second kv)))))
                                    (:computed-property
                                     `(list (cons (endb/sql/expr:sql-cast ,(ast->cl ctx (second kv)) :varchar)
                                                  ,(ast->cl ctx (nth 2 kv)))))
                                    (:spread-property
                                     (let ((spread-sym (gensym))
                                           (idx-sym (gensym)))
                                       `(let ((,spread-sym ,(ast->cl ctx (second kv))))
                                          (cond
                                            ((endb/arrow::%alistp ,spread-sym)
                                             ,spread-sym)
                                            ((vectorp ,spread-sym)
                                             (loop for ,spread-sym across ,spread-sym
                                                   for ,idx-sym from 0
                                                   collect (cons (format nil "~A" ,idx-sym) ,spread-sym)))))))
                                    (t `(list (cons ,(symbol-name (first kv)) ,(ast->cl ctx (second kv))))))))
          :test 'equal :key #'car)
        :empty-struct)))

(defmethod sql->cl (ctx (type (eql :access)) &rest args)
  (destructuring-bind (base path)
      args
    `(endb/sql/expr:sql-access ,(ast->cl ctx base) ,(cond
                                                      ((eq :* path) path)
                                                      ((symbolp path) (symbol-name path))
                                                      (t (ast->cl ctx path))))))

(defmethod sql->cl (ctx (type (eql :with)) &rest args)
  (destructuring-bind (ctes query)
      args
    (let ((ctx (fset:with ctx :ctes (reduce
                                     (lambda (acc cte)
                                       (destructuring-bind (cte-name cte-query &optional cte-columns)
                                           cte
                                         (multiple-value-bind (ast projection)
                                             (ast->cl (fset:with ctx :ctes acc) cte-query)
                                           (declare (ignore ast))
                                           (when (and cte-columns (not (= (length projection) (length cte-columns))))
                                             (%annotated-error cte-name "Number of column names does not match projection"))
                                           (fset:with acc
                                                      (symbol-name cte-name)
                                                      (make-cte :src cte-query :projection (or (mapcar #'symbol-name cte-columns) projection))))))
                                     ctes
                                     :initial-value (or (fset:lookup ctx :ctes) (fset:empty-map))))))
      (ast->cl ctx query))))

(defun %find-sql-expr-symbol (fn)
  (find-symbol (string-upcase (concatenate 'string "sql-" (symbol-name fn))) :endb/sql/expr))

(defmethod sql->cl (ctx (type (eql :function)) &rest args)
  (destructuring-bind (fn args)
      args
    (let ((fn-sym (%find-sql-expr-symbol fn)))
      (unless fn-sym
        (error 'endb/sql/expr:sql-runtime-error :message (format nil "Unknown build-in function: ~A" fn)))
      `(,fn-sym ,@(loop for ast in args
                        collect (ast->cl ctx ast))))))

(defmethod sql->cl (ctx (type (eql :aggregate-function)) &rest args)
  (destructuring-bind (fn args &key distinct (where ':true))
      args
    (let* ((aggregate-table (fset:lookup ctx :aggregate-table))
           (fn-sym (%find-sql-expr-symbol fn))
           (aggregate-sym (gensym))
           (init-src `(endb/sql/expr:make-sql-agg ,fn :distinct ,distinct))
           (src (loop for ast in (if (null args)
                                     (list :null)
                                     args)
                      collect (ast->cl ctx ast)))
           (where-src (ast->cl ctx where))
           (agg (make-aggregate :src src :init-src init-src :var aggregate-sym :where-src where-src)))
      (assert fn-sym nil (format nil "Unknown aggregate function: ~A" fn))
      (setf (gethash aggregate-sym aggregate-table) agg)
      `(endb/sql/expr:sql-agg-finish ,aggregate-sym))))

(defmethod sql->cl (ctx (type (eql :case)) &rest args)
  (destructuring-bind (cases-or-expr &optional cases)
      args
    (let ((expr-sym (gensym)))
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
  (destructuring-bind (idx)
      args
    `(nth ,idx ,(fset:lookup ctx :param-sym))))

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
      ((:create-table :create-index :drop-table :drop-view) t)
      (:insert (eq :values (first (third ast))))
      (:select (> (length (cdr (getf ast :from)))
                  *interpreter-from-limit*)))))

(defun %resolve-parameters (ast)
  (let ((idx 0))
    (labels ((walk (x)
               (cond
                 ((eq :parameter x) (let ((src `(:parameter ,idx)))
                                      (incf idx)
                                      src))
                 ((listp x) (mapcar #'walk x))
                 (t x))))
      (values (walk ast) idx))))

(defun compile-sql (ctx ast)
  (let ((*print-length* 16)
        (*print-level* 8))
    (log:debug ast)
    (let* ((db-sym (gensym))
           (index-sym (gensym))
           (param-sym (gensym))
           (ctx (fset:map-union ctx (fset:map (:db-sym db-sym)
                                              (:index-sym index-sym)
                                              (:param-sym param-sym)))))
      (multiple-value-bind (ast number-of-parameters)
          (%resolve-parameters ast)
        (multiple-value-bind (src projection)
            (ast->cl ctx ast)
          (log:debug src)
          (let* ((src (if projection
                          `(values ,src ,(list 'quote projection))
                          src))
                 (src `(lambda (,db-sym &rest ,param-sym)
                         (declare (optimize (speed 3) (safety 0) (debug 0) (compilation-speed 3)))
                         (declare (ignorable ,db-sym ,param-sym))
                         (unless (= ,number-of-parameters (length ,param-sym))
                           (error 'endb/sql/expr:sql-runtime-error :message (format nil "Number of required parameters: ~A does not match given: ~A"
                                                                                    ,number-of-parameters
                                                                                    (length ,param-sym))))
                         (let ((,index-sym (make-hash-table :test 'equal)))
                           (declare (ignorable ,index-sym))
                           ,src))))
            #+sbcl (let ((sb-ext:*evaluator-mode* (if (%interpretp ast)
                                                      :interpret
                                                      sb-ext:*evaluator-mode*)))
                     (eval src))
            #-sbcl (eval src)))))))
