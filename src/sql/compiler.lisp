(defpackage :endb/sql/compiler
  (:use :cl)
  (:import-from :endb/sql/expr)
  (:export #:compile-sql))
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

(defun %base-table (ctx table)
  (values `(gethash :rows (gethash ,(symbol-name table) ,(cdr (assoc :db-sym ctx))))
          (mapcar #'%compiler-symbol (gethash :columns (gethash (symbol-name table) (cdr (assoc :db ctx)))))))

(defun %wrap-with-order-by-and-limit (src order-by limit)
  (let* ((src (if order-by
                  `(endb/sql/expr::%sql-order-by ,src ',order-by)
                  src))
         (src (if limit
                  `(endb/sql/expr::%sql-limit ,src ',limit)
                  src)))
    src))

(defmethod sql->cl (ctx (type (eql :select)) &rest args)
  (destructuring-bind (select-list &key distinct (from '(((:values ((:null))) . #:dual))) (where :true)
                                     (group-by () group-by-p) (having :true havingp)
                                     order-by limit)
      args
    (labels ((from->cl (from-tables where-src selected-src)
               (destructuring-bind (table-src env-extension qualified-projection)
                   (first from-tables)
                 (declare (ignore qualified-projection))
                 `(loop for ,(remove-duplicates (mapcar #'cdr env-extension))
                          in ,table-src
                        ,@(if (rest from-tables)
                              `(nconc ,(from->cl (rest from-tables) where-src selected-src))
                              `(when (eq t ,where-src) collect (list ,@selected-src))))))
             (group-by->cl (ctx from-tables where-src selected-src)
               (let* ((aggregate-table (cdr (assoc :aggregate-table ctx)))
                      (having-src (ast->cl ctx having))
                      (group-by-projection (loop for g in group-by
                                                 collect (ast->cl ctx g)))
                      (group-by-exprs-projection (loop for k being the hash-key of aggregate-table
                                                       collect k))
                      (group-by-exprs (loop for v being the hash-value of aggregate-table
                                            collect v))
                      (group-by-selected-src (append group-by-projection group-by-exprs))
                      (from-src (from->cl from-tables where-src group-by-selected-src))
                      (group-by-src `(endb/sql/expr::%sql-group-by ,from-src ,(length group-by-projection) ,(length group-by-exprs))))
                 `(loop for ,group-by-projection being the hash-key
                          using (hash-value ,group-by-exprs-projection)
                            of ,group-by-src
                        when (eq t ,having-src)
                          collect (list ,@selected-src))))
             (select->cl (ctx from-ast from-tables)
               (destructuring-bind (table-or-subquery . table-alias)
                   (first from-ast)
                 (multiple-value-bind (table-src projection)
                     (if (symbolp table-or-subquery)
                         (%base-table ctx table-or-subquery)
                         (ast->cl ctx table-or-subquery))
                   (let* ((qualified-projection (loop for column in projection
                                                      collect (%qualified-column-name table-alias column)))
                          (env-extension (loop for column in projection
                                               for qualified-column = (%qualified-column-name table-alias column)
                                               for column-sym = (gensym (symbol-name qualified-column))
                                               append (list (cons column column-sym) (cons qualified-column column-sym))))
                          (ctx (append env-extension ctx))
                          (from-table (list table-src env-extension qualified-projection))
                          (from-tables (append from-tables (list from-table))))
                     (if (rest from-ast)
                         (select->cl ctx (rest from-ast) from-tables)
                         (let* ((aggregate-table (make-hash-table))
                                (ctx (cons (cons :aggregate-table aggregate-table) ctx))
                                (full-projection (mapcan #'third from-tables))
                                (selected-src (loop for (expr) in select-list
                                                    append (if (eq :star expr)
                                                               (loop for p in full-projection
                                                                     collect (ast->cl ctx p))
                                                               (list (ast->cl ctx expr)))))
                                (where-src (ast->cl ctx where))
                                (group-by-needed-p (or group-by-p havingp (plusp (hash-table-count aggregate-table)))))
                           (cons
                            (if group-by-needed-p
                                (group-by->cl ctx from-tables where-src selected-src)
                                (from->cl from-tables where-src selected-src))
                            full-projection))))))))
      (destructuring-bind (src . full-projection)
          (select->cl ctx from ())
        (let* ((src (if distinct
                        `(endb/sql/expr::%sql-distinct ,src)
                        src))
               (src (%wrap-with-order-by-and-limit src order-by limit))
               (select-star-projection (mapcar #'%unqualified-column-name full-projection)))
          (values src (%select-projection select-list select-star-projection)))))))

(defun %values-projection (arity)
  (loop for idx from 1 upto arity
        collect (%anonymous-column-name idx)))

(defmethod sql->cl (ctx (type (eql :values)) &rest args)
  (destructuring-bind (values-list &key order-by limit)
      args
    (values (%wrap-with-order-by-and-limit (ast->cl ctx values-list) order-by limit)
            (%values-projection (length (first values-list))))))

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
     (cons 'list (loop for ast in ast
                       collect (ast->cl ctx ast))))
    ((and (symbolp ast)
          (not (keywordp ast)))
     (cdr (assoc (%compiler-symbol (symbol-name ast)) ctx)))
    (t ast)))

(defun compile-sql (ctx ast)
  (let* ((db-sym (gensym))
         (ctx (cons (cons :db-sym db-sym) ctx)))
    (multiple-value-bind (src projection)
        (ast->cl ctx ast)
      (eval `(lambda (,db-sym)
               (declare (optimize (speed 3) (safety 0) (debug 0)))
               (declare (ignorable ,db-sym))
               ,(if projection
                    `(values ,src ,(cons 'list (mapcar #'symbol-name projection)))
                    src))))))
