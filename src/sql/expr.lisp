(defpackage :endb/sql/expr
  (:nicknames :expr)
  (:use :cl)
  (:export #:sql-= #:sql-<> #:sql-is #:sql-not #:sql-and #:sql-or
           #:sql-< #:sql-<= #:sql-> #:sql->=
           #:sql-+ #:sql-- #:sql-* #:sql-/ #:sql-%
           #:sql-abs))
(in-package :endb/sql/expr)

(defun sql-boolean-p (x)
  (member x '(t nil :sql/null)))

(deftype sql-boolean ()
  `(satisfies sql-boolean-p))

(deftype sql-null ()
  `(member :sql/null))

(deftype sql-number ()
  `(or sql-null number))

(declaim (ftype (function (t t) sql-boolean) sql-=))
(defun sql-= (x y)
  (if (or (eq :sql/null x) (eq :sql/null y))
      :sql/null
      (equal x y)))

(declaim (ftype (function (t t) sql-boolean) sql-<>))
(defun sql-<> (x y)
  (sql-not (sql-= x y)))

(declaim (ftype (function (t t) sql-boolean) sql-is))
(defun sql-is (x y)
  (equal x y))

(declaim (ftype (function (sql-number sql-number) sql-boolean) sql-<))
(defun sql-< (x y)
  (if (or (eq :sql/null x) (eq :sql/null y))
      :sql/null
      (< x y)))

(declaim (ftype (function (sql-number sql-number) sql-boolean) sql-<=))
(defun sql-<= (x y)
  (if (or (eq :sql/null x) (eq :sql/null y))
      :sql/null
      (<= x y)))

(declaim (ftype (function (sql-number sql-number) sql-boolean) sql->))
(defun sql-> (x y)
  (if (or (eq :sql/null x) (eq :sql/null y))
      :sql/null
      (> x y)))

(declaim (ftype (function (sql-number sql-number) sql-boolean) sql->=))
(defun sql->= (x y)
  (if (or (eq :sql/null x) (eq :sql/null y))
      :sql/null
      (>= x y)))

(declaim (ftype (function (sql-boolean) sql-boolean) sql-not))
(defun sql-not (x)
  (if (eq :sql/null x)
      :sql/null
      (not x)))

(declaim (ftype (function (sql-boolean sql-boolean) sql-boolean) sql-and))
(defun sql-and (x y)
  (if (and (eq t y) (eq :sql/null x))
      :sql/null
      (and x y)))

(declaim (ftype (function (sql-boolean sql-boolean) sql-boolean) sql-or))
(defun sql-or (x y)
  (cond
    ((eq t y) t)
    ((and (eq nil y) (eq :sql/null x))
     :sql/null)
    (t (or x y))))

(declaim (ftype (function (sql-number &optional sql-number) sql-number) sql-+))
(defun sql-+ (x &optional (y 0))
  (if (or (eq :sql/null x) (eq :sql/null y))
      :sql/null
      (+ x y)))

(declaim (ftype (function (sql-number &optional sql-number) sql-number) sql--))
(defun sql-- (x &optional y)
  (if (or (eq :sql/null x) (eq :sql/null y))
      :sql/null
      (if (null y)
          (- x)
          (- x y))))

(declaim (ftype (function (sql-number sql-number) sql-number) sql-*))
(defun sql-* (x y)
  (if (or (eq :sql/null x) (eq :sql/null y))
      :sql/null
      (* x y)))

(declaim (ftype (function (sql-number sql-number) sql-number) sql-/))
(defun sql-/ (x y)
  (if (or (eq :sql/null x) (eq :sql/null y))
      :sql/null
      (/ x y)))

(declaim (ftype (function (sql-number sql-number) sql-number) sql-%))
(defun sql-% (x y)
  (if (or (eq :sql/null x) (eq :sql/null y))
      :sql/null
      (mod x y)))

(declaim (ftype (function (sql-number) sql-number) sql-abs))
(defun sql-abs (x)
  (if (eq :sql/null x)
      :sql/null
      (abs x)))
