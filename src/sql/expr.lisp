(defpackage :endb/sql/expr
  (:use :cl)
  (:export #:sql-= #:sql-<> #:sql-is #:sql-not #:sql-and #:sql-or
           #:sql-< #:sql-<= #:sql-> #:sql->=
           #:sql-+ #:sql-- #:sql-* #:sql-/ #:sql-%
           #:sql-between #:sql-in #:sql-exists
           #:sql-abs
           #:sql-create-table #:sql-insert))
(in-package :endb/sql/expr)

(deftype sql-null ()
  `(member :null))

(deftype sql-boolean ()
  `(or boolean sql-null))

(deftype sql-number ()
  `(or number sql-null))

(deftype sql-string ()
  `(or string sql-null))

(declaim (ftype (function (t t) sql-boolean) sql-=))
(defun sql-= (x y)
  (if (or (eq :null x) (eq :null y))
      :null
      (equalp x y)))

(declaim (ftype (function (t t) sql-boolean) sql-<>))
(defun sql-<> (x y)
  (sql-not (sql-= x y)))

(declaim (ftype (function (t t) sql-boolean) sql-is))
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

(declaim (ftype (function (sql-boolean sql-boolean) sql-boolean) sql-and))
(defun sql-and (x y)
  (if (and (eq t y) (eq :null x))
      :null
      (and x y)))

(declaim (ftype (function (sql-boolean sql-boolean) sql-boolean) sql-or))
(defun sql-or (x y)
  (cond
    ((eq t y) t)
    ((and (eq nil y) (eq :null x))
     :null)
    (t (or x y))))

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

(declaim (ftype (function (t list) sql-boolean) sql-in))
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
    db))

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
    db))
