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
                  `(endb/sql/expr::%sql-sort ,src ',order-by)
                  src))
         (src (if limit
                  `(subseq ,src ,(or (cdr limit) 0) ,(if (cdr limit)
                                                         (+ (car limit) (cdr limit))
                                                         (car limit)))
                  src)))
    src))

(defmethod sql->cl (ctx (type (eql :select)) &rest args)
  (destructuring-bind (select-list &key distinct (from '(((:values ((:null))) . #:dual))) (where :true) group-by (having :true) order-by limit)
      args
    (declare (ignore group-by having))
    (let ((full-projection))
      (labels ((select->cl (from)
                 (let ((table-or-subquery (first from)))
                   (multiple-value-bind (table projection)
                       (if (symbolp (car table-or-subquery))
                           (%base-table ctx (car table-or-subquery))
                           (ast->cl ctx (car table-or-subquery)))
                     (let* ((cv (cdr table-or-subquery))
                            (cv-sym (gensym))
                            (qualified-projection (loop for column in projection
                                                        collect (%qualified-column-name cv column)))
                            (src `(loop for ,cv-sym in ,table for ,qualified-projection = ,cv-sym for ,projection = ,cv-sym)))
                       (setq full-projection (append full-projection qualified-projection))
                       (if (rest from)
                           (append src (list 'nconc (select->cl (rest from))))
                           (let ((selected-src (cons 'list (loop for (expr) in select-list
                                                                 append (if (eq :star expr)
                                                                            full-projection
                                                                            (list (ast->cl ctx expr)))))))
                             (append src (list 'when (ast->cl ctx where) 'collect selected-src)))))))))
        (let* ((src (select->cl from))
               (src (if distinct
                        `(remove-duplicates ,src :test 'equal)
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
      (values (%wrap-with-order-by-and-limit `(union ,lhs-src ,(ast->cl ctx rhs) :test 'equal) order-by limit) columns))))

(defmethod sql->cl (ctx (type (eql :union-all)) &rest args)
  (destructuring-bind (lhs rhs &key order-by limit)
      args
    (multiple-value-bind (lhs-src projection)
        (ast->cl ctx lhs)
      (values (%wrap-with-order-by-and-limit `(append ,lhs-src ,(ast->cl ctx rhs)) order-by limit) projection))))

(defmethod sql->cl (ctx (type (eql :except)) &rest args)
  (destructuring-bind (lhs rhs &key order-by limit)
      args
    (multiple-value-bind (lhs-src projection)
        (ast->cl ctx lhs)
      (values (%wrap-with-order-by-and-limit `(set-difference ,lhs-src ,(ast->cl ctx rhs) :test 'equal) order-by limit) projection))))

(defmethod sql->cl (ctx (type (eql :intersect)) &rest args)
  (destructuring-bind (lhs rhs &key order-by limit)
      args
    (multiple-value-bind (lhs-src projection)
        (ast->cl ctx lhs)
      (values (%wrap-with-order-by-and-limit `(intersection ,lhs-src ,(ast->cl ctx rhs) :test 'equal) order-by limit) projection))))

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
     (cons 'list (mapcar (lambda (ast)
                           (ast->cl ctx ast)) ast)))
    ((and (symbolp ast)
          (not (keywordp ast))) (%compiler-symbol (symbol-name ast)))
    (t ast)))

(defun compile-sql (ctx ast)
  (let* ((db-sym (gensym))
         (ctx (cons (cons :db-sym db-sym) ctx)))
    (multiple-value-bind (src projection)
        (ast->cl ctx ast)
      (eval `(lambda (,db-sym)
               (declare (ignorable ,db-sym))
               ,(if projection
                    `(values ,src ,(cons 'list (mapcar #'symbol-name projection)))
                    src))))))
