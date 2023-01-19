(defpackage :endb/sql/compiler
  (:use :cl)
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
    (declare (ignore values-list order-by limit))))

(defmethod sql->cl (ctx (type (eql :union)) &rest args)
  (destructuring-bind (lhs rhs &key order-by limit)
      args
    (declare (ignore lhs rhs order-by limit))))

(defmethod sql->cl (ctx (type (eql :union-all)) &rest args)
  (destructuring-bind (lhs rhs &key order-by limit)
      args
    (declare (ignore lhs rhs order-by limit))))

(defmethod sql->cl (ctx (type (eql :except)) &rest args)
  (destructuring-bind (lhs rhs &key order-by limit)
      args
    (declare (ignore lhs rhs order-by limit))))

(defmethod sql->cl (ctx (type (eql :intersect)) &rest args)
  (destructuring-bind (lhs rhs &key order-by limit)
      args
    (declare (ignore lhs rhs order-by limit))))

(defmethod sql->cl (ctx (type (eql :create-table)) &rest args)
  (destructuring-bind (table-name columns)
      args
    (declare (ignore table-name columns))))

(defmethod sql->cl (ctx (type (eql :insert)) &rest args)
  (destructuring-bind (table-name values &key column-names)
      args
    (declare (ignore table-name values column-names))))

(defmethod sql->cl (ctx (type (eql :subquery)) &rest args)
  (destructuring-bind (query)
      args
    (ast->cl ctx query)))

(defmethod sql->cl (ctx (type (eql :function)) &rest args)
  (destructuring-bind (fn args)
      args
    `(,(find-symbol (symbol-name fn)) ,@(ast->cl ctx args))))

(defmethod sql->cl (ctx (type (eql :aggregate-function)) &rest args)
  (if (eq :count-star (first args))
      nil
      (destructuring-bind (fn args &key distinct)
          args
        (declare (ignore fn args distinct)))))

(defmethod sql->cl (ctx (type (eql :+)) &rest args)
  (destructuring-bind (lhs &optional rhs)
      args
    (if rhs
        `(+ ,(ast->cl ctx lhs) ,(ast->cl ctx rhs))
        `(+ ,(ast->cl ctx lhs)))))

(defmethod sql->cl (ctx (type (eql :-)) &rest args)
  (destructuring-bind (lhs &optional rhs)
      args
    (if rhs
        `(- ,(ast->cl ctx lhs) ,(ast->cl ctx rhs))
        `(- ,(ast->cl ctx lhs)))))

(defmethod sql->cl (ctx (type (eql :*)) &rest args)
  (destructuring-bind (lhs rhs)
      args
    `(* ,(ast->cl ctx lhs) ,(ast->cl ctx rhs))))

(defmethod sql->cl (ctx (type (eql :/)) &rest args)
  (destructuring-bind (lhs rhs)
      args
    `(/ ,(ast->cl ctx lhs) ,(ast->cl ctx rhs))))

(defmethod sql->cl (ctx (type (eql :%)) &rest args)
  (destructuring-bind (lhs rhs)
      args
    `(mod ,(ast->cl ctx lhs) ,(ast->cl ctx rhs))))

(defmethod sql->cl (ctx (type (eql :=)) &rest args)
  (destructuring-bind (lhs rhs)
      args
    `(equal ,(ast->cl ctx lhs) ,(ast->cl ctx rhs))))

(defmethod sql->cl (ctx (type (eql :<>)) &rest args)
  (destructuring-bind (lhs rhs)
      args
    `(not (equal ,(ast->cl ctx lhs) ,(ast->cl ctx rhs)))))

(defmethod sql->cl (ctx (type (eql :<)) &rest args)
  (destructuring-bind (lhs rhs)
      args
    `(< ,(ast->cl ctx lhs) ,(ast->cl ctx rhs))))

(defmethod sql->cl (ctx (type (eql :<=)) &rest args)
  (destructuring-bind (lhs rhs)
      args
    `(<= ,(ast->cl ctx lhs) ,(ast->cl ctx rhs))))

(defmethod sql->cl (ctx (type (eql :>)) &rest args)
  (destructuring-bind (lhs rhs)
      args
    `(> ,(ast->cl ctx lhs) ,(ast->cl ctx rhs))))

(defmethod sql->cl (ctx (type (eql :>=)) &rest args)
  (destructuring-bind (lhs rhs)
      args
    `(>= ,(ast->cl ctx lhs) ,(ast->cl ctx rhs))))

(defmethod sql->cl (ctx (type (eql :and)) &rest args)
  (destructuring-bind (lhs rhs)
      args
    `(and ,(ast->cl ctx lhs) ,(ast->cl ctx rhs))))

(defmethod sql->cl (ctx (type (eql :or)) &rest args)
  (destructuring-bind (lhs rhs)
      args
    `(or ,(ast->cl ctx lhs) ,(ast->cl ctx rhs))))

(defmethod sql->cl (ctx (type (eql :not)) &rest args)
  (destructuring-bind (expr)
      args
    `(not ,(ast->cl ctx expr))))

(defmethod sql->cl (ctx (type (eql :is)) &rest args)
  (destructuring-bind (lhs rhs)
      args
    `(eq ,(ast->cl ctx lhs) ,(ast->cl ctx rhs))))

(defmethod sql->cl (ctx (type (eql :between)) &rest args)
  (destructuring-bind (expr lhs rhs)
      args
    (let ((expr-sym (gensym))
          (lhs-sym (gensym))
          (rhs-sym (gensym)))
      `(let ((,expr-sym ,(ast->cl ctx expr))
             (,lhs-sym ,(ast->cl ctx lhs))
             (,rhs-sym ,(ast->cl ctx rhs)))
         (and (>= ,expr-sym ,lhs-sym)
              (<= ,expr-sym ,rhs-sym))))))

(defmethod sql->cl (ctx (type (eql :in)) &rest args)
  (destructuring-bind (expr expr-list)
      args
    `(member ,(ast->cl ctx expr) (list ,@(ast->cl ctx expr-list)))))

(defmethod sql->cl (ctx (type (eql :exists)) &rest args)
  (destructuring-bind (subquery)
      args
    `(not (null ,(ast->cl ctx subquery)))))

(defmethod sql->cl (ctx (type (eql :case)) &rest args)
  (destructuring-bind (cases-or-expr &optional cases)
      args
    (if cases
        (let ((expr-sym (gensym)))
          `(let ((,expr-sym ,(ast->cl ctx cases-or-expr)))
             (cond
               ,@(mapcar
                  (lambda (x)
                    (if (eq :else (first x))
                        x
                        (list `(equal ,expr-sym ,(ast->cl ctx (first x))) (ast->cl ctx (second x)))))
                  cases))))
        `(cond ,@(mapcar
                  (lambda (x)
                    (list (ast->cl ctx (first x)) (ast->cl ctx (second x))))
                  cases-or-expr)))))

(defun ast->cl (ctx ast)
  (cond
    ((and (listp ast)
          (symbolp (first ast)))
     (apply #'sql->cl ctx ast))
    ((listp ast)
     (mapcar (lambda (ast)
               (ast->cl ctx ast)) ast))
    (t ast)))

(defun compile->sql (ctx ast)
  (ast->cl ctx ast))
