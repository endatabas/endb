(defpackage :endb/sql/compiler
  (:use :cl)
  (:import-from :endb/sql/expr)
  (:export #:compile-sql))
(in-package :endb/sql/compiler)

(defgeneric sql->cl (ctx type &rest args))

(defmethod sql->cl (ctx (type (eql :select)) &rest args)
  (destructuring-bind (select-list &key distinct from where order-by limit)
      args
    (declare (ignore select-list distinct from where order-by limit))))

(defmethod sql->cl (ctx (type (eql :values)) &rest args)
  (destructuring-bind (values-list &key order-by limit)
      args
    (declare (ignore order-by limit))
    (ast->cl ctx values-list)))

(defmethod sql->cl (ctx (type (eql :union)) &rest args)
  (destructuring-bind (lhs rhs &key order-by limit)
      args
    (declare (ignore order-by limit))
    `(union ,(ast->cl ctx lhs) ,(ast->cl ctx rhs) :test 'equal)))

(defmethod sql->cl (ctx (type (eql :union-all)) &rest args)
  (destructuring-bind (lhs rhs &key order-by limit)
      args
    (declare (ignore order-by limit))
    `(append ,(ast->cl ctx lhs) ,(ast->cl ctx rhs))))

(defmethod sql->cl (ctx (type (eql :except)) &rest args)
  (destructuring-bind (lhs rhs &key order-by limit)
      args
    (declare (ignore order-by limit))
    `(set-difference ,(ast->cl ctx lhs) ,(ast->cl ctx rhs) :test 'equal)))

(defmethod sql->cl (ctx (type (eql :intersect)) &rest args)
  (destructuring-bind (lhs rhs &key order-by limit)
      args
    (declare (ignore order-by limit))
    `(intersection ,(ast->cl ctx lhs) ,(ast->cl ctx rhs) :test 'equal)))

(defmethod sql->cl (ctx (type (eql :create-table)) &rest args)
  (destructuring-bind (table-name columns)
      args
    `(endb/sql/expr:sql-create-table ,(cdr (assoc :db-sym ctx)) ,(symbol-name table-name) ',(mapcar #'symbol-name columns))))

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
    ((and (listp ast)
          (symbolp (first ast)))
     (apply #'sql->cl ctx ast))
    ((listp ast)
     (cons 'list (mapcar (lambda (ast)
                           (ast->cl ctx ast)) ast)))
    (t ast)))

(defun compile-sql (ctx ast)
  (let* ((db-sym (gensym))
         (ctx (cons (cons :db-sym db-sym) ctx)))
    (eval `(lambda (,db-sym)
             ,(ast->cl ctx ast)))))
