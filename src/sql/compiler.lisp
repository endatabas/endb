(defpackage :endb/sql/compiler
  (:use :cl)
  (:import-from :endb/sql/expr)
  (:export #:compile-sql #:*verbose*))
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

(defun %select-projection (select-list select-star-projection)
  (loop for idx from 1
        for (expr alias) in select-list
        append (cond
                 ((eq :* expr) select-star-projection)
                 (alias (list (symbol-name alias)))
                 ((symbolp expr) (list (%unqualified-column-name (symbol-name expr))))
                 (t (list (%anonymous-column-name idx))))))

(defun %base-table->cl (ctx table)
  (let ((db-table (gethash (symbol-name table) (cdr (assoc :db ctx)))))
    (if (listp db-table)
        (%ast->cl-with-free-vars ctx db-table)
        (values `(endb/sql/expr:base-table-rows (gethash ,(symbol-name table) ,(cdr (assoc :db-sym ctx))))
                (endb/sql/expr:base-table-columns db-table)
                ()
                (length (endb/sql/expr:base-table-rows db-table))))))

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

(defstruct from-table
  src
  vars
  free-vars
  size
  projection)

(defstruct where-clause
  src
  free-vars
  ast)

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

(defun %join->cl (ctx from-table scan-clauses equi-join-clauses)
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
             (index-sym (cdr (assoc :index-sym ctx)))
             (index-table-sym (gensym))
             (index-key-sym (gensym))
             (index-key-form `(list ',(gensym) ,@new-free-vars)))
        `(gethash (list ,@in-vars)
                  (let ((,index-key-sym ,index-key-form))
                    (or (gethash ,index-key-sym ,index-sym)
                        (loop with ,index-table-sym = (setf (gethash ,index-key-sym ,index-sym)
                                                            (make-hash-table :test 'equal))
                              for ,(%unique-vars vars)
                                in ,src
                              ,@(loop for clause in scan-clauses append `(when (eq t ,(where-clause-src clause))))
                              do (push (list ,@vars) (gethash (list ,@out-vars) ,index-table-sym))
                              finally (return ,index-table-sym)))))))))

(defun %selection-with-limit-offset->cl (ctx selected-src limit offset)
  (let ((acc-sym (cdr (assoc :acc-sym ctx))))
    (if (or limit offset)
        (let* ((rows-sym (cdr (assoc :rows-sym ctx)))
               (block-sym (cdr (assoc :block-sym ctx)))
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

(defun %from->cl (ctx from-tables where-clauses selected-src &optional vars limit offset)
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
             (where-clauses (set-difference where-clauses new-where-clauses)))
        `(loop for ,(%unique-vars new-vars)
                 in ,(if equi-join-clauses
                         (%join->cl ctx from-table scan-clauses equi-join-clauses)
                         (from-table-src from-table))
               ,@(loop for clause in (if equi-join-clauses
                                         pushdown-clauses
                                         (append scan-clauses pushdown-clauses))
                       append `(when (eq t ,(where-clause-src clause))))
               ,@(if from-tables
                     `(do ,(%from->cl ctx from-tables where-clauses selected-src vars limit offset))
                     (append `(,@(loop for clause in where-clauses append `(when (eq t ,(where-clause-src clause)))))
                             (%selection-with-limit-offset->cl ctx selected-src limit offset))))))))

(defun %group-by->cl (ctx from-tables where-clauses selected-src limit offset group-by having-src correlated-vars)
  (let* ((aggregate-table (cdr (assoc :aggregate-table ctx)))
         (group-by-projection (loop for g in group-by
                                    collect (ast->cl ctx g)))
         (group-by-exprs-projection (loop for k being the hash-key of aggregate-table
                                          collect k))
         (group-by-exprs (loop for v being the hash-value of aggregate-table
                               collect v))
         (group-by-selected-src (append group-by-projection group-by-exprs))
         (group-acc-sym (gensym))
         (group-ctx (cons (cons :acc-sym group-acc-sym) ctx))
         (from-src `(let ((,group-acc-sym))
                      ,(%from->cl group-ctx from-tables where-clauses group-by-selected-src correlated-vars)
                      ,group-acc-sym))
         (group-by-src `(endb/sql/expr::%sql-group-by ,from-src ,(length group-by-projection) ,(length group-by-exprs))))
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

(defmethod sql->cl (ctx (type (eql :select)) &rest args)
  (destructuring-bind (select-list &key distinct (from '(((:values ((:null))) #:dual))) (where :true)
                                     (group-by () group-by-p) (having :true havingp)
                                     order-by limit offset)
      args
    (labels ((select->cl (ctx from-ast from-tables-acc)
               (destructuring-bind (table-or-subquery &optional table-alias)
                   (first from-ast)
                 (multiple-value-bind (table-src projection free-vars table-size)
                     (if (symbolp table-or-subquery)
                         (%base-table->cl ctx table-or-subquery)
                         (%ast->cl-with-free-vars ctx table-or-subquery))
                   (let* ((table-alias (or table-alias table-or-subquery))
                          (table-alias (symbol-name table-alias))
                          (qualified-projection (loop for column in projection
                                                      collect (%qualified-column-name table-alias column)))
                          (env-extension (loop for column in projection
                                               for qualified-column = (%qualified-column-name table-alias column)
                                               for column-sym = (gensym (concatenate 'string qualified-column "__"))
                                               append (list (cons column column-sym) (cons qualified-column column-sym))))
                          (ctx (append env-extension ctx))
                          (from-table (make-from-table :src table-src
                                                       :vars (remove-duplicates (mapcar #'cdr env-extension))
                                                       :free-vars free-vars
                                                       :size (or table-size most-positive-fixnum)
                                                       :projection qualified-projection))
                          (from-tables-acc (append from-tables-acc (list from-table))))
                     (if (rest from-ast)
                         (select->cl ctx (rest from-ast) from-tables-acc)
                         (let* ((aggregate-table (make-hash-table))
                                (ctx (cons (cons :aggregate-table aggregate-table) ctx))
                                (full-projection (loop for from-table in from-tables-acc
                                                       append (from-table-projection from-table)))
                                (selected-src (loop for (expr) in select-list
                                                    append (if (eq :* expr)
                                                               (loop for p in full-projection
                                                                     collect (ast->cl ctx (make-symbol p)))
                                                               (list (ast->cl ctx expr)))))
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
                                (%from->cl ctx from-tables where-clauses selected-src correlated-vars limit offset))
                            full-projection))))))))
      (let* ((block-sym (gensym))
             (acc-sym (gensym))
             (rows-sym (gensym))
             (ctx (cons (cons :rows-sym rows-sym)
                        (cons (cons :acc-sym acc-sym)
                              (cons (cons :block-sym block-sym) ctx))))
             (from (%flatten-from from)))
        (multiple-value-bind (src full-projection)
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
                 (select-projection (%select-projection select-list select-star-projection))
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
      (let* ((index-sym (cdr (assoc :index-sym ctx)))
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
    `(endb/sql/expr:sql-create-table ,(cdr (assoc :db-sym ctx)) ,(symbol-name table-name) ',(mapcar #'symbol-name column-names))))

(defmethod sql->cl (ctx (type (eql :create-index)) &rest args)
  (declare (ignore args))
  `(endb/sql/expr:sql-create-index ,(cdr (assoc :db-sym ctx))))

(defmethod sql->cl (ctx (type (eql :drop-index)) &rest args)
  (declare (ignore args))
  `(endb/sql/expr:sql-drop-index ,(cdr (assoc :db-sym ctx))))

(defmethod sql->cl (ctx (type (eql :drop-table)) &rest args)
  (destructuring-bind (table-name &key if-exists)
      args
    `(endb/sql/expr:sql-drop-table ,(cdr (assoc :db-sym ctx)) ,(symbol-name table-name) :if-exists ,(when if-exists
                                                                                                      t))))

(defmethod sql->cl (ctx (type (eql :create-view)) &rest args)
  (destructuring-bind (table-name query)
      args
    `(endb/sql/expr:sql-create-view ,(cdr (assoc :db-sym ctx)) ,(symbol-name table-name) ',query)))

(defmethod sql->cl (ctx (type (eql :drop-view)) &rest args)
  (destructuring-bind (view-name &key if-exists)
      args
    `(endb/sql/expr:sql-drop-view ,(cdr (assoc :db-sym ctx)) ,(symbol-name view-name)  :if-exists ,(when if-exists
                                                                                                     t))))

(defmethod sql->cl (ctx (type (eql :insert)) &rest args)
  (destructuring-bind (table-name values &key column-names)
      args
    `(endb/sql/expr:sql-insert ,(cdr (assoc :db-sym ctx)) ,(symbol-name table-name) ,(ast->cl ctx values)
                               :column-names ',(mapcar #'symbol-name column-names))))

(defmethod sql->cl (ctx (type (eql :delete)) &rest args)
  (destructuring-bind (table-name where)
      args
    `(endb/sql/expr:sql-delete ,(cdr (assoc :db-sym ctx)) ,(symbol-name table-name)
                               ,(ast->cl ctx (list :select (list (list :*)) :from (list (list table-name)) :where where)))))

(defmethod sql->cl (ctx (type (eql :update)) &rest args)
  (destructuring-bind (table-name update-cols &key (where :true))
      args
    (let ((db-table (gethash (symbol-name table-name) (cdr (assoc :db ctx)))))
      (unless (listp db-table)
        (let* ((columns (endb/sql/expr:base-table-columns db-table))
               (update-cols (reverse update-cols))
               (update-col-names (mapcar (lambda (x)
                                           (symbol-name (first x)))
                                         update-cols))
               (update-select-list (loop for column in columns
                                         for (update-col expr) = (find column update-cols
                                                                       :key (lambda (x)
                                                                              (symbol-name (first x)))
                                                                       :test 'equal)
                                         if update-col
                                           collect (list expr)
                                         else
                                           collect (list (make-symbol column))))
               (update-sym (gensym)))
          (when (subsetp update-col-names columns :test 'equal)
            `(let ((,update-sym ,(ast->cl ctx (list :select update-select-list :from (list (list table-name)) :where where))))
               (endb/sql/expr:sql-delete ,(cdr (assoc :db-sym ctx)) ,(symbol-name table-name)
                                         ,(ast->cl ctx (list :select (list (list :*)) :from (list (list table-name)) :where where)))
               (endb/sql/expr:sql-insert ,(cdr (assoc :db-sym ctx)) ,(symbol-name table-name) ,update-sym))))))))

(defmethod sql->cl (ctx (type (eql :in-query)) &rest args)
  (destructuring-bind (expr query)
      args
    (multiple-value-bind (src projection free-vars)
        (if (symbolp query)
            (ast->cl ctx (list :select (list (list :*)) :from (list (list query))))
            (%ast->cl-with-free-vars ctx query))
      (unless (= 1 (length projection))
        (error 'endb/sql/expr:sql-runtime-error :message "IN query must return single column."))
      (let* ((in-var-sym (gensym))
             (expr-sym (gensym))
             (index-table-sym (gensym))
             (index-key-sym (gensym))
             (index-key-form `(list ',(gensym) ,@free-vars)))
        `(let* ((,index-key-sym ,index-key-form)
                (,index-table-sym (or (gethash ,index-key-sym ,(cdr (assoc :index-sym ctx)))
                                      (loop with ,index-table-sym = (setf (gethash ,index-key-sym ,(cdr (assoc :index-sym ctx)))
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

(defun %find-sql-expr-symbol (fn)
  (find-symbol (string-upcase (concatenate 'string "sql-" (symbol-name fn))) :endb/sql/expr))

(defmethod sql->cl (ctx (type (eql :function)) &rest args)
  (destructuring-bind (fn args)
      args
    (let ((fn-sym (%find-sql-expr-symbol fn)))
      (assert fn-sym nil (format nil "Unknown built-in function: ~A" fn))
      `(,fn-sym ,@(loop for ast in args
                        collect (ast->cl ctx ast))))))

(defmethod sql->cl (ctx (type (eql :aggregate-function)) &rest args)
  (destructuring-bind (fn args &key distinct)
      args
    (let ((aggregate-table (cdr (assoc :aggregate-table ctx)))
          (fn-sym (%find-sql-expr-symbol fn))
          (aggregate-sym (gensym)))
      (assert fn-sym nil (format nil "Unknown aggregate function: ~A" fn))
      (setf (gethash aggregate-sym aggregate-table) (ast->cl ctx (first args)))
      `(,fn-sym ,aggregate-sym ,@(loop for ast in (rest args)
                                       collect (ast->cl ctx ast))
                :distinct ,distinct))))

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
       ((assoc :quote ctx)
        (loop for ast in ast
              collect (ast->cl ctx ast)))
       (t (let ((ctx (cons (cons :quote t) ctx)))
            (list 'quote (loop for ast in ast
                               collect (ast->cl ctx ast)))))))
    ((and (symbolp ast)
          (not (keywordp ast)))
     (let ((symbol-var-pair (assoc (symbol-name ast) ctx :test 'equal)))
       (dolist (cb (cdr (assoc :on-var-access ctx)))
         (funcall cb symbol-var-pair))
       (cdr symbol-var-pair)))
    (t ast)))

(defun %ast->cl-with-free-vars (ctx ast)
  (let* ((vars ())
         (ctx (cons (cons :on-var-access
                          (cons (lambda (x)
                                  (when (member x ctx)
                                    (pushnew (cdr x) vars)))
                                (cdr (assoc :on-var-access ctx))))
                    ctx)))
    (multiple-value-bind (src projection)
        (ast->cl ctx ast)
      (values src projection vars))))

(defvar *verbose* nil)
(defvar *interpreter-from-limit* 8)

(defun %interpretp (ast)
  (when (listp ast)
    (case (first ast)
      ((:create-table :create-index :drop-table :drop-view) t)
      (:insert (eq :values (first (third ast))))
      (:select (> (length (cdr (getf ast :from)))
                  *interpreter-from-limit*)))))

(defun compile-sql (ctx ast)
  (let ((*print-length* 16)
        (*print-level* 8))
    (when *verbose*
      (pprint ast)
      (terpri))
    (let* ((db-sym (gensym))
           (index-sym (gensym))
           (ctx (cons (cons :db-sym db-sym) ctx))
           (ctx (cons (cons :index-sym index-sym) ctx)))
      (multiple-value-bind (src projection)
          (ast->cl ctx ast)
        (when *verbose*
          (pprint src)
          (terpri))
        (let* ((src (if projection
                        `(values ,src ,(list 'quote projection))
                        src))
               (src `(lambda (,db-sym)
                       (declare (optimize (speed 3) (safety 0) (debug 0)))
                       (declare (ignorable ,db-sym))
                       (let ((,index-sym (make-hash-table :test 'equal)))
                         (declare (ignorable ,index-sym))
                         ,src))))
          #+sbcl (let ((sb-ext:*evaluator-mode* (if (%interpretp ast)
                                                    :interpret
                                                    sb-ext:*evaluator-mode*)))
                   (eval src))
          #-sbcl (eval src))))))
