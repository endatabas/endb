(defpackage :endb/sql/compiler
  (:use :cl)
  (:import-from :endb/sql/expr)
  (:export #:compile-sql #:*verbose*))
(in-package :endb/sql/compiler)

(defgeneric sql->cl (ctx type &rest args))

(defun %compiler-symbol (x)
  (intern (if (symbolp x)
              (symbol-name x)
              x)
          :endb/sql/compiler))

(defun %anonymous-column-name (idx)
  (%compiler-symbol (concatenate 'string "column" (princ-to-string idx))))

(defun %unqualified-column-name (column)
  (let* ((column-str (symbol-name column))
         (idx (position #\. column-str)))
    (if idx
        (%compiler-symbol (subseq column-str (1+ idx)))
        column)))

(defun %qualified-column-name (table-alias column)
  (%compiler-symbol (concatenate 'string (symbol-name table-alias) "." (symbol-name column))))

(defun %select-projection (select-list select-star-projection)
  (loop for idx from 1
        for (expr . alias) in select-list
        append (cond
                 ((eq :star expr) select-star-projection)
                 (alias (list alias))
                 ((symbolp expr) (list (%unqualified-column-name expr)))
                 (t (list (%anonymous-column-name idx))))))

(defun %base-table->cl (ctx table)
  (let ((db-table (gethash (symbol-name table) (cdr (assoc :db ctx)))))
    (values `(endb/sql/expr:base-table-rows (gethash ,(symbol-name table) ,(cdr (assoc :db-sym ctx))))
            (mapcar #'%compiler-symbol (endb/sql/expr:base-table-columns db-table))
            (length (endb/sql/expr:base-table-rows db-table)))))

(defun %wrap-with-order-by-and-limit (src order-by limit)
  (let* ((src (if order-by
                  `(endb/sql/expr::%sql-order-by ,src ',order-by)
                  src))
         (src (if (and order-by limit)
                  `(endb/sql/expr::%sql-limit ,src ',limit)
                  src)))
    src))

(defun %and-clauses (expr)
  (if (and (listp expr)
           (eq :and (first expr)))
      (append (%and-clauses (second expr))
              (%and-clauses (third expr)))
      (list expr)))

(defstruct from-table
  src
  vars
  size
  projection)

(defstruct where-clause
  src
  vars)

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

(defun %join->cl (ctx new-vars equi-join-clauses table-src)
  (multiple-value-bind (in-vars out-vars)
      (loop for (nil lhs rhs) in (mapcar #'where-clause-src equi-join-clauses)
            if (member lhs new-vars)
              collect lhs into out-vars
            else
              collect lhs into in-vars
            if (member rhs new-vars)
              collect rhs into out-vars
            else
              collect rhs into in-vars
            finally
               (return (values in-vars out-vars)))
    (let ((index-table-sym (gensym))
          (index-key-sym (gensym)))
      `(gethash (list ,@in-vars)
                (or (gethash ',index-key-sym ,(cdr (assoc :index-sym ctx)))
                    (loop with ,index-table-sym = (setf (gethash ',index-key-sym ,(cdr (assoc :index-sym ctx)))
                                                        (make-hash-table :test 'equal))
                          for ,new-vars
                            in ,table-src
                          do (push (list ,@new-vars) (gethash (list ,@out-vars) ,index-table-sym))
                          finally (return ,index-table-sym)))))))

(defun %selection-with-limit-offset->cl (ctx selected-src limit-offset)
  (let ((acc-sym (cdr (assoc :acc-sym ctx))))
    (if limit-offset
        (destructuring-bind (limit . offset)
            limit-offset
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
               do (return-from ,block-sym ,acc-sym))))
        `(do (push (list ,@selected-src) ,acc-sym)))))

(defun %from->cl (ctx from-tables where-clauses selected-src &optional limit-offset vars)
  (let* ((from-table (or (find-if (lambda (x)
                                    (loop with vars = (append (from-table-vars x) vars)
                                          for c in where-clauses
                                          for src = (where-clause-src c)
                                          thereis (%equi-join-predicate-p vars src)))
                                  from-tables)
                         (first from-tables)))
         (from-tables (remove from-table from-tables))
         (new-vars (from-table-vars from-table))
         (table-src (from-table-src from-table))
         (vars (append new-vars vars)))
    (multiple-value-bind (scan-clauses equi-join-clauses pushdown-clauses)
        (loop for c in where-clauses
              if (subsetp (where-clause-vars c) new-vars)
                collect c into scan-clauses
              else if (%equi-join-predicate-p vars (where-clause-src c))
                     collect c into equi-join-clauses
              else if (subsetp (where-clause-vars c) vars)
                     collect c into pushdown-clauses
              finally
                 (return (values scan-clauses equi-join-clauses pushdown-clauses)))
      (let* ((new-where-clauses (append scan-clauses equi-join-clauses pushdown-clauses))
             (where-clauses (set-difference where-clauses new-where-clauses)))
        `(loop for ,new-vars
                 in ,(if equi-join-clauses
                         (let ((table-src (if scan-clauses
                                              `(loop for ,new-vars
                                                       in ,table-src
                                                     ,@(loop for clause in scan-clauses append `(when (eq t ,(where-clause-src clause))))
                                                     collect (list ,@new-vars))
                                              table-src)))
                           (%join->cl ctx new-vars equi-join-clauses table-src))
                         table-src)
               ,@(loop for clause in (if equi-join-clauses
                                         pushdown-clauses
                                         (append scan-clauses pushdown-clauses))
                       append `(when (eq t ,(where-clause-src clause))))
               ,@(if from-tables
                     `(do ,(%from->cl ctx from-tables where-clauses selected-src limit-offset vars))
                     (append `(,@(loop for clause in where-clauses append `(when (eq t ,(where-clause-src clause)))))
                             (%selection-with-limit-offset->cl ctx selected-src limit-offset))))))))

(defun %group-by->cl (ctx from-tables where-clauses selected-src limit-offset group-by having)
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
                      ,(%from->cl group-ctx from-tables where-clauses group-by-selected-src)
                      ,group-acc-sym))
         (group-by-src `(endb/sql/expr::%sql-group-by ,from-src ,(length group-by-projection) ,(length group-by-exprs))))
    (append `(loop for ,group-by-projection being the hash-key
                     using (hash-value ,group-by-exprs-projection)
                       of ,group-by-src
                   when (eq t ,(ast->cl ctx having)))
            (%selection-with-limit-offset->cl ctx selected-src limit-offset))))

(defmethod sql->cl (ctx (type (eql :select)) &rest args)
  (destructuring-bind (select-list &key distinct (from '(((:values ((:null))) . #:dual))) (where :true)
                                     (group-by () group-by-p) (having :true havingp)
                                     order-by limit)
      args
    (labels ((select->cl (ctx from-ast from-tables-acc)
               (destructuring-bind (table-or-subquery . table-alias)
                   (first from-ast)
                 (multiple-value-bind (table-src projection table-size)
                     (if (symbolp table-or-subquery)
                         (%base-table->cl ctx table-or-subquery)
                         (ast->cl ctx table-or-subquery))
                   (let* ((qualified-projection (loop for column in projection
                                                      collect (%qualified-column-name table-alias column)))
                          (env-extension (loop for column in projection
                                               for qualified-column = (%qualified-column-name table-alias column)
                                               for column-sym = (gensym (concatenate 'string (symbol-name qualified-column) "__"))
                                               append (list (cons column column-sym) (cons qualified-column column-sym))))
                          (ctx (append env-extension ctx))
                          (from-table (make-from-table :src table-src
                                                       :vars (remove-duplicates (mapcar #'cdr env-extension))
                                                       :size (or table-size most-positive-fixnum)
                                                       :projection qualified-projection))
                          (from-tables-acc (append from-tables-acc (list from-table))))
                     (if (rest from-ast)
                         (select->cl ctx (rest from-ast) from-tables-acc)
                         (let* ((aggregate-table (make-hash-table))
                                (ctx (cons (cons :aggregate-table aggregate-table) ctx))
                                (full-projection (mapcan #'from-table-projection from-tables-acc))
                                (selected-src (loop for (expr) in select-list
                                                    append (if (eq :star expr)
                                                               (loop for p in full-projection
                                                                     collect (ast->cl ctx p))
                                                               (list (ast->cl ctx expr)))))
                                (where-clauses (loop for clause in (%and-clauses where)
                                                     collect (let* ((vars ())
                                                                    (ctx (cons (cons :on-var-access
                                                                                     (cons (lambda (x)
                                                                                             (when (assoc (car x) ctx)
                                                                                               (push (cdr x) vars)))
                                                                                           (cdr (assoc :on-var-access ctx))))
                                                                               ctx)))
                                                               (make-where-clause :src (ast->cl ctx clause)
                                                                                  :vars vars))))
                                (from-tables (sort from-tables-acc #'< :key
                                                   (lambda (x)
                                                     (/ (from-table-size x)
                                                        (1+ (loop for clause in where-clauses
                                                                  for src = (where-clause-src clause)
                                                                  when (subsetp (where-clause-vars clause) (from-table-vars x))
                                                                    sum (case (first src)
                                                                          (endb/sql/expr:sql-= 10)
                                                                          (endb/sql/expr:sql-in 2)
                                                                          (t 1))))))))
                                (limit-offset (unless order-by
                                                limit))
                                (group-by-needed-p (or group-by-p havingp (plusp (hash-table-count aggregate-table)))))
                           (values
                            (if group-by-needed-p
                                (%group-by->cl ctx from-tables where-clauses selected-src limit-offset group-by having)
                                (%from->cl ctx from-tables where-clauses selected-src limit-offset))
                            full-projection))))))))
      (let* ((block-sym (gensym))
             (acc-sym (gensym))
             (rows-sym (gensym))
             (ctx (cons (cons :rows-sym rows-sym)
                        (cons (cons :acc-sym acc-sym)
                              (cons (cons :block-sym block-sym) ctx)))))
        (multiple-value-bind (src full-projection)
            (select->cl ctx from ())
          (let* ((src `(block ,block-sym
                         (let (,@(when limit
                                   (list `(,rows-sym 0)))
                               (,acc-sym))
                           ,src
                           ,acc-sym)))
                 (src (if distinct
                          `(endb/sql/expr::%sql-distinct ,src)
                          src))
                 (src (%wrap-with-order-by-and-limit src order-by limit))
                 (select-star-projection (mapcar #'%unqualified-column-name full-projection)))
            (values src (%select-projection select-list select-star-projection))))))))

(defun %values-projection (arity)
  (loop for idx from 1 upto arity
        collect (%anonymous-column-name idx)))

(defmethod sql->cl (ctx (type (eql :values)) &rest args)
  (destructuring-bind (values-list &key order-by limit)
      args
    (values (%wrap-with-order-by-and-limit (ast->cl ctx values-list) order-by limit)
            (%values-projection (length (first values-list))))))

(defmethod sql->cl (ctx (type (eql :exists)) &rest args)
  (destructuring-bind (query)
      args
    `(endb/sql/expr:sql-exists ,(ast->cl ctx (append query '(:limit (1 . 0)))))))

(defmethod sql->cl (ctx (type (eql :union)) &rest args)
  (destructuring-bind (lhs rhs &key order-by limit)
      args
    (multiple-value-bind (lhs-src columns)
        (ast->cl ctx lhs)
      (values (%wrap-with-order-by-and-limit `(endb/sql/expr:sql-union ,lhs-src ,(ast->cl ctx rhs)) order-by limit) columns))))

(defmethod sql->cl (ctx (type (eql :union-all)) &rest args)
  (destructuring-bind (lhs rhs &key order-by limit)
      args
    (multiple-value-bind (lhs-src projection)
        (ast->cl ctx lhs)
      (values (%wrap-with-order-by-and-limit `(endb/sql/expr:sql-union-all ,lhs-src ,(ast->cl ctx rhs)) order-by limit) projection))))

(defmethod sql->cl (ctx (type (eql :except)) &rest args)
  (destructuring-bind (lhs rhs &key order-by limit)
      args
    (multiple-value-bind (lhs-src projection)
        (ast->cl ctx lhs)
      (values (%wrap-with-order-by-and-limit `(endb/sql/expr:sql-except ,lhs-src ,(ast->cl ctx rhs)) order-by limit) projection))))

(defmethod sql->cl (ctx (type (eql :intersect)) &rest args)
  (destructuring-bind (lhs rhs &key order-by limit)
      args
    (multiple-value-bind (lhs-src projection)
        (ast->cl ctx lhs)
      (values (%wrap-with-order-by-and-limit `(endb/sql/expr:sql-intersect ,lhs-src ,(ast->cl ctx rhs)) order-by limit) projection))))

(defmethod sql->cl (ctx (type (eql :create-table)) &rest args)
  (destructuring-bind (table-name column-names)
      args
    `(endb/sql/expr:sql-create-table ,(cdr (assoc :db-sym ctx)) ,(symbol-name table-name) ',(mapcar #'symbol-name column-names))))

(defmethod sql->cl (ctx (type (eql :create-index)) &rest args)
  (declare (ignore args))
  `(endb/sql/expr:sql-create-index ,(cdr (assoc :db-sym ctx))))

(defmethod sql->cl (ctx (type (eql :insert)) &rest args)
  (destructuring-bind (table-name values &key column-names)
      args
    `(endb/sql/expr:sql-insert ,(cdr (assoc :db-sym ctx)) ,(symbol-name table-name) ,(ast->cl ctx values)
                               :column-names ',(mapcar #'symbol-name column-names))))

(defmethod sql->cl (ctx (type (eql :subquery)) &rest args)
  (destructuring-bind (query)
      args
    (ast->cl ctx query)))

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
      (assert (<= (length args) 1) nil (format nil "Aggregates require max 1 argument, got: ~D" (length args)))
      (setf (gethash aggregate-sym aggregate-table) (ast->cl ctx (first args)))
      `(,fn-sym ,aggregate-sym :distinct ,distinct))))

(defmethod sql->cl (ctx (type (eql :case)) &rest args)
  (destructuring-bind (cases-or-expr &optional cases)
      args
    (let ((expr-sym (gensym)))
      `(let ((,expr-sym ,(if cases
                             (ast->cl ctx cases-or-expr)
                             t)))
         (cond
           ,@(loop for (test then) in (or cases cases-or-expr)
                   collect (list (if (eq :else test)
                                     t
                                     `(eq t (endb/sql/expr:sql-= ,expr-sym ,(ast->cl ctx test))))
                                 (ast->cl ctx then))))))))

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
       ((some #'%ast-function-call-p ast)
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
     (let ((symbol-var-pair (assoc (%compiler-symbol (symbol-name ast)) ctx)))
       (dolist (cb (cdr (assoc :on-var-access ctx)))
         (funcall cb symbol-var-pair))
       (cdr symbol-var-pair)))
    (t ast)))

(defvar *verbose* nil)
(defvar *interpreter-from-limit* 8)

(defun %interpretp (ast)
  (and (listp ast)
       (eq :select (first ast))
       (> (length (cdr (getf ast :from)))
          *interpreter-from-limit*)))

(defun compile-sql (ctx ast)
  (let* ((db-sym (gensym))
         (index-sym (gensym))
         (ctx (cons (cons :db-sym db-sym) ctx))
         (ctx (cons (cons :index-sym index-sym) ctx)))
    (multiple-value-bind (src projection)
        (ast->cl ctx ast)
      (when *verbose*
        (pprint ast)
        (pprint src))
      (let* ((src (if projection
                      `(values ,src ,(cons 'list (mapcar #'symbol-name projection)))
                      src))
             (src `(lambda (,db-sym)
                     (declare (optimize (speed 3) (safety 0) (debug 0)))
                     (declare (ignorable ,db-sym))
                     (let ((,index-sym (make-hash-table :test 'equal)))
                       (declare (ignorable ,index-sym))
                       ,src))))
        #+sbcl (let ((sb-ext:*evaluator-mode* (if (%interpretp ast)
                                                  :interpret
                                                  :compile)))
                 (eval src))
        #-sbcl (eval src)))))
