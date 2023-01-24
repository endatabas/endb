(defpackage :endb/sql/compiler
  (:use :cl)
  (:import-from :endb/sql/expr)
  (:export #:compile-sql))
(in-package :endb/sql/compiler)

(defgeneric sql->cl (ctx type &rest args))

(defun %anonymous-column-name (idx)
  (make-symbol (concatenate 'string "column" (princ-to-string idx))))

(defun %unqualified-column-name (column)
  (let* ((column-str (symbol-name column))
         (idx (position #\. column-str)))
    (if idx
        (make-symbol (subseq column-str (1+ idx)))
        column)))

(defun %select-projection (select-list)
  (loop for idx from 1
        for (expr . alias) in select-list
        collect (cond
                  (alias alias)
                  ((symbolp expr) (%unqualified-column-name expr))
                  (t (%anonymous-column-name idx)))))

(defmethod sql->cl (ctx (type (eql :select)) &rest args)
  (destructuring-bind (select-list &key distinct (from '(((:values ((nil)))))) (where :true) order-by limit)
      args
    (declare (ignore distinct from where order-by limit))
    (let ((ast nil))
      (values ast (%select-projection select-list)))))

(defun %values-projection (arity)
  (loop for idx from 1 upto arity
        collect (%anonymous-column-name idx)))

(defmethod sql->cl (ctx (type (eql :values)) &rest args)
  (destructuring-bind (values-list &key order-by limit)
      args
    (declare (ignore order-by limit))
    (values (ast->cl ctx values-list) (%values-projection (length (first values-list))))))

(defmethod sql->cl (ctx (type (eql :union)) &rest args)
  (destructuring-bind (lhs rhs &key order-by limit)
      args
    (declare (ignore order-by limit))
    (multiple-value-bind (lhs-ast columns)
        (ast->cl ctx lhs)
      (values `(union ,lhs-ast ,(ast->cl ctx rhs) :test 'equal) columns))))

(defmethod sql->cl (ctx (type (eql :union-all)) &rest args)
  (destructuring-bind (lhs rhs &key order-by limit)
      args
    (declare (ignore order-by limit))
    (multiple-value-bind (lhs-ast projection)
        (ast->cl ctx lhs)
      (values `(append ,lhs-ast ,(ast->cl ctx rhs)) projection))))

(defmethod sql->cl (ctx (type (eql :except)) &rest args)
  (destructuring-bind (lhs rhs &key order-by limit)
      args
    (declare (ignore order-by limit))
    (multiple-value-bind (lhs-ast projection)
        (ast->cl ctx lhs)
      (values `(set-difference ,lhs-ast ,(ast->cl ctx rhs) :test 'equal) projection))))

(defmethod sql->cl (ctx (type (eql :intersect)) &rest args)
  (destructuring-bind (lhs rhs &key order-by limit)
      args
    (declare (ignore order-by limit))
    (multiple-value-bind (lhs-ast projection)
        (ast->cl ctx lhs)
      (values `(intersection ,lhs-ast ,(ast->cl ctx rhs) :test 'equal) projection))))

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
    `(,(%find-sql-expr-symbol fn) ,@(mapcar (lambda (ast)
                                              (ast->cl ctx ast)) args))))

(defmethod sql->cl (ctx (type (eql :aggregate-function)) &rest args)
  (if (eq :count-star (first args))
      nil
      (destructuring-bind (fn args &key distinct)
          args
        (declare (ignore fn args distinct)))))

(defmethod sql->cl (ctx (type (eql :case)) &rest args)
  (destructuring-bind (cases-or-expr &optional cases)
      args
    (let ((expr-sym (gensym)))
      `(let ((,expr-sym ,(if cases
                             (ast->cl ctx cases-or-expr)
                             t)))
         (cond
           ,@(mapcar
              (lambda (x)
                (list (if (eq :else (first x))
                          :else
                          `(endb/sql/expr:sql-= ,expr-sym ,(ast->cl ctx (first x))))
                      (ast->cl ctx (second x))))
              (or cases cases-or-expr)))))))

(defmethod sql->cl (ctx fn &rest args)
  (sql->cl ctx :function fn args))

(defun ast->cl (ctx ast)
  (cond
    ((eq :true ast) t)
    ((eq :false ast) nil)
    ((and (listp ast)
          (symbolp (first ast))
          (not (member (first ast) '(:null :true :false))))
     (apply #'sql->cl ctx ast))
    ((listp ast)
     (cons 'list (mapcar (lambda (ast)
                           (ast->cl ctx ast)) ast)))
    (t ast)))

(defun compile-sql (ctx ast)
  (let* ((db-sym (gensym))
         (ctx (cons (cons :db-sym db-sym) ctx)))
    (eval `(lambda (,db-sym)
             (declare (ignorable ,db-sym))
             ,(ast->cl ctx ast)))))
