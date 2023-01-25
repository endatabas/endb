(defpackage :endb/sql/compiler
  (:use :cl)
  (:import-from :endb/sql/expr)
  (:export #:compile-sql))
(in-package :endb/sql/compiler)

(defgeneric sql->cl (ctx type &rest args))

(defun %compiler-symbol (x)
  (intern x :endb/sql/compiler))

(defun %anonymous-column-name (idx)
  (%compiler-symbol (concatenate 'string "column" (princ-to-string idx))))

(defun %unqualified-column-name (column)
  (let* ((column-str (symbol-name column))
         (idx (position #\. column-str)))
    (if idx
        (%compiler-symbol (subseq column-str (1+ idx)))
        column)))

(defun %qualified-column-name (cv column)
  (%compiler-symbol (concatenate 'string (symbol-name cv) "." (symbol-name column))))

(defun %select-projection (select-list)
  (loop for idx from 1
        for (expr . alias) in select-list
        collect (cond
                  (alias alias)
                  ((symbolp expr) (%unqualified-column-name expr))
                  (t (%anonymous-column-name idx)))))

(defun %base-table (ctx table)
  (values `(gethash :rows (gethash ,(symbol-name table) ,(cdr (assoc :db-sym ctx))))
          (mapcar #'%compiler-symbol (gethash :columns (gethash (symbol-name table) (cdr (assoc :db ctx)))))))

(defun %wrap-with-order-by-and-limit (ast order-by limit)
  (let* ((ast (if order-by
                               `(endb/sql/expr::%sql-sort ,ast ',order-by)
                               ast))
         (ast (if limit
                  `(subseq ,ast ,(or (cdr limit) 0) ,(if (cdr limit)
                                                         (+ (car limit) (cdr limit))
                                                         (car limit)))
                  ast)))
    ast))

(defmethod sql->cl (ctx (type (eql :select)) &rest args)
  (destructuring-bind (select-list &key distinct (from '(((:values ((:null))) . #:dual))) (where :true) order-by limit)
      args
    (let ((select-star))
      (labels ((build-ast (from full-projection)
                 (when from
                   (let ((table-or-subquery (first from)))
                     (multiple-value-bind (table projection)
                         (if (symbolp (car table-or-subquery))
                             (%base-table ctx (car table-or-subquery))
                             (ast->cl ctx (car table-or-subquery)))
                       (let* ((cv (cdr table-or-subquery))
                              (cv-sym (gensym))
                              (qualified-projection (loop for column in projection
                                                          collect (%qualified-column-name cv column)))
                              (full-projection (append full-projection qualified-projection))
                              (where-pred (ast->cl ctx where))
                              (ast `(loop for ,cv-sym in ,table for ,qualified-projection = ,cv-sym for ,projection = ,cv-sym)))
                         (if (null (rest from))
                             (append ast `(when ,where-pred collect ,(if (equal '((:star)) select-list)
                                                                         (progn
                                                                           (setq select-star full-projection)
                                                                           (cons 'list full-projection))
                                                                         (ast->cl ctx (mapcar #'car select-list)))))
                             (append ast (list 'nconc (build-ast (rest from) select-star))))))))))
        (let* ((ast (build-ast from ()))
               (ast (if distinct
                        `(remove-duplicates ,ast :test 'equal)
                        ast))
               (ast (%wrap-with-order-by-and-limit ast order-by limit)))
          (values ast (if select-star
                          (mapcar #'%unqualified-column-name select-star)
                          (%select-projection select-list))))))))

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
    (multiple-value-bind (lhs-ast columns)
        (ast->cl ctx lhs)
      (values (%wrap-with-order-by-and-limit `(union ,lhs-ast ,(ast->cl ctx rhs) :test 'equal) order-by limit) columns))))

(defmethod sql->cl (ctx (type (eql :union-all)) &rest args)
  (destructuring-bind (lhs rhs &key order-by limit)
      args
    (multiple-value-bind (lhs-ast projection)
        (ast->cl ctx lhs)
      (values (%wrap-with-order-by-and-limit `(append ,lhs-ast ,(ast->cl ctx rhs)) order-by limit) projection))))

(defmethod sql->cl (ctx (type (eql :except)) &rest args)
  (destructuring-bind (lhs rhs &key order-by limit)
      args
    (multiple-value-bind (lhs-ast projection)
        (ast->cl ctx lhs)
      (values (%wrap-with-order-by-and-limit `(set-difference ,lhs-ast ,(ast->cl ctx rhs) :test 'equal) order-by limit) projection))))

(defmethod sql->cl (ctx (type (eql :intersect)) &rest args)
  (destructuring-bind (lhs rhs &key order-by limit)
      args
    (multiple-value-bind (lhs-ast projection)
        (ast->cl ctx lhs)
      (values (%wrap-with-order-by-and-limit `(intersection ,lhs-ast ,(ast->cl ctx rhs) :test 'equal) order-by limit) projection))))

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
  (let ((fn-sym (find-symbol (string-upcase (concatenate 'string "sql-" (symbol-name fn))) :endb/sql/expr)))
    (assert fn-sym nil (format nil "Unknown built-in function: ~A" fn))
    fn-sym))

(defmethod sql->cl (ctx (type (eql :function)) &rest args)
  (destructuring-bind (fn args)
      args
    `(,(%find-sql-expr-symbol fn) ,@(mapcar (lambda (ast)
                                              (ast->cl ctx ast)) args))))

(defmethod sql->cl (ctx (type (eql :aggregate-function)) &rest args)
  (destructuring-bind (fn args &key distinct)
      args
    (declare (ignore args distinct))
    (let ((fn-sym (find-symbol (string-upcase (concatenate 'string "sql-aggregate-" (symbol-name fn))) :endb/sql/expr)))
      (assert fn-sym nil (format nil "Unknown aggregate function: ~A" fn))
      fn-sym)))

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
                          t
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
          (keywordp (first ast))
          (not (member (first ast) '(:null :true :false))))
     (apply #'sql->cl ctx ast))
    ((listp ast)
     (cons 'list (mapcar (lambda (ast)
                           (ast->cl ctx ast)) ast)))
    ((and (symbolp ast)
          (not (keywordp ast))) (%compiler-symbol (symbol-name ast)))
    (t ast)))

(defun compile-sql (ctx ast)
  (let* ((db-sym (gensym))
         (ctx (cons (cons :db-sym db-sym) ctx)))
    (multiple-value-bind (ast projection)
        (ast->cl ctx ast)
      (eval `(lambda (,db-sym)
               (declare (ignorable ,db-sym))
               ,(if projection
                    `(values ,ast ,(cons 'list (mapcar #'symbol-name projection)))
                    ast))))))
