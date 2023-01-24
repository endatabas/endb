(defpackage :endb/sql/expr
  (:use :cl)
  (:export #:sql-= #:sql-<> #:sql-is #:sql-not #:sql-and #:sql-or
           #:sql-< #:sql-<= #:sql-> #:sql->=
           #:sql-+ #:sql-- #:sql-* #:sql-/ #:sql-%
           #:sql-between #:sql-in #:sql-exists #:sql-coalesce
           #:sql-abs
           #:sql-create-table #:sql-create-index #:sql-insert))
(in-package :endb/sql/expr)

(deftype sql-null ()
  `(eql :null))

(deftype sql-boolean ()
  `(or boolean sql-null))

(deftype sql-number ()
  `(or number sql-null))

(deftype sql-string ()
  `(or string sql-null))

(deftype sql-value ()
  `(or sql-null sql-boolean sql-number sql-string))

(declaim (ftype (function (sql-value sql-value) sql-boolean) sql-=))
(defun sql-= (x y)
  (if (or (eq :null x) (eq :null y))
      :null
      (equalp x y)))

(declaim (ftype (function (sql-value sql-value) sql-boolean) sql-<>))
(defun sql-<> (x y)
  (sql-not (sql-= x y)))

(declaim (ftype (function (sql-value sql-value) sql-boolean) sql-is))
(defun sql-is (x y)
  (equalp x y))

(declaim (ftype (function (sql-number sql-number) sql-boolean) sql-<))
(defun sql-< (x y)
  (if (or (eq :null x) (eq :null y))
      :null
      (< x y)))

(declaim (ftype (function (sql-number sql-number) sql-boolean) sql-<=))
(defun sql-<= (x y)
  (if (or (eq :null x) (eq :null y))
      :null
      (<= x y)))

(declaim (ftype (function (sql-number sql-number) sql-boolean) sql->))
(defun sql-> (x y)
  (if (or (eq :null x) (eq :null y))
      :null
      (> x y)))

(declaim (ftype (function (sql-number sql-number) sql-boolean) sql->=))
(defun sql->= (x y)
  (if (or (eq :null x) (eq :null y))
      :null
      (>= x y)))

(declaim (ftype (function (sql-boolean) sql-boolean) sql-not))
(defun sql-not (x)
  (if (eq :null x)
      :null
      (not x)))

(defmacro sql-and (x y)
  (let ((x-sym (gensym)))
    `(let ((,x-sym ,x))
       (if (eq :null ,x-sym)
           (and ,y :null)
           (and ,x-sym ,y)))))

(defmacro sql-or (x y)
  (let ((x-sym (gensym)))
    `(let ((,x-sym ,x))
       (if (eq :null ,x-sym)
           (or ,y :null)
           (or ,x-sym ,y)))))

(declaim (ftype (function (sql-value sql-value &rest sql-value) sql-value) sql-coalesce))
(defun sql-coalesce (x y &rest args)
  (let ((tail (member-if-not (lambda (x)
                               (eq :null x))
                             (cons x (cons y args)))))
    (if tail
        (first tail)
        :null)))

(declaim (ftype (function (sql-number &optional sql-number) sql-number) sql-+))
(defun sql-+ (x &optional (y 0))
  (if (or (eq :null x) (eq :null y))
      :null
      (+ x y)))

(declaim (ftype (function (sql-number &optional sql-number) sql-number) sql--))
(defun sql-- (x &optional y)
  (if (or (eq :null x) (eq :null y))
      :null
      (if (null y)
          (- x)
          (- x y))))

(declaim (ftype (function (sql-number sql-number) sql-number) sql-*))
(defun sql-* (x y)
  (if (or (eq :null x) (eq :null y))
      :null
      (* x y)))

(declaim (ftype (function (sql-number sql-number) sql-number) sql-/))
(defun sql-/ (x y)
  (if (or (eq :null x) (eq :null y))
      :null
      (/ x y)))

(declaim (ftype (function (sql-number sql-number) sql-number) sql-%))
(defun sql-% (x y)
  (if (or (eq :null x) (eq :null y))
      :null
      (mod x y)))

(declaim (ftype (function (sql-value list) sql-boolean) sql-in))
(defun sql-in (item list)
  (reduce (lambda (x y)
            (sql-or x (sql-= y item)))
          list
          :initial-value nil))

(declaim (ftype (function (sql-number sql-number sql-number) sql-boolean) sql-between))
(defun sql-between (expr lhs rhs)
  (sql-and (sql->= expr lhs) (sql-<= expr rhs)))

(declaim (ftype (function (list) sql-boolean) sql-exists))
(defun sql-exists (list)
  (not (null list)))

(declaim (ftype (function (sql-number) sql-number) sql-abs))
(defun sql-abs (x)
  (if (eq :null x)
      :null
      (abs x)))

(declaim (ftype (function (list) sql-value) sql-scalar-subquery))
(defun sql-scalar-subquery (x)
  (assert (<= (length x) 1))
  (if (null x)
      :null
      (caar x)))

(defun %sql-sort (rows order-by)
  (sort rows (lambda (x y)
               (reduce
                (lambda (acc order)
                  (let ((idx (1- (car order))))
                    (or acc (funcall (if (eq :ASC (cdr order))
                                         #'<
                                         #'>)
                                     (nth idx x)
                                     (nth idx y)))))
                order-by
                :initial-value nil))))

(defun sql-create-table (db table-name columns)
  (let ((table (make-hash-table))
        (column->idx (make-hash-table :test 'equal)))
    (loop for column in columns
          for idx from 0
          do (setf (gethash column column->idx) idx))
    (setf (gethash :columns table) columns)
    (setf (gethash :column->idx table) column->idx)
    (setf (gethash :rows table) ())
    (setf (gethash table-name db) table)
    (values nil t)))

(defun sql-create-index (db)
  (declare (ignore db))
  (values nil t))

(defun sql-insert (db table-name values &key column-names)
  (let* ((table (gethash table-name db))
         (rows (gethash :rows table))
         (column->idx (gethash :column->idx table))
         (values (if column-names
                     (mapcar (lambda (row)
                               (mapcar (lambda (column)
                                         (nth (gethash column column->idx) row))
                                       column-names))
                             values)
                     values)))
    (setf (gethash :rows table) (append values rows))
    (values nil (length values))))
