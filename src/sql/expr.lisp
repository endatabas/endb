(defpackage :endb/sql/expr
  (:use :cl)
  (:export #:sql-= #:sql-<> #:sql-is #:sql-not #:sql-and #:sql-or
           #:sql-< #:sql-<= #:sql-> #:sql->=
           #:sql-+ #:sql-- #:sql-* #:sql-/ #:sql-%
           #:sql-between #:sql-in #:sql-exists #:sql-coalesce
           #:sql-union-all #:sql-union #:sql-except #:sql-intersect
           #:sql-abs
           #:sql-count-star #:sql-count #:sql-sum #:sql-avg #:sql-min #:sql-max
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

(declaim (ftype (function (list list) list) sql-union))
(defun sql-union (lhs rhs)
  (remove-duplicates (union lhs rhs :test 'equal) :test 'equal))

(declaim (ftype (function (list list) list) sql-union-all))
(defun sql-union-all (lhs rhs)
  (append lhs rhs))

(declaim (ftype (function (list list) list) sql-except))
(defun sql-except (lhs rhs)
  (remove-duplicates (set-difference lhs rhs :test 'equal) :test 'equal))

(declaim (ftype (function (list list) list) sql-intersect))
(defun sql-intersect (lhs rhs)
  (remove-duplicates (intersection lhs rhs :test 'equal) :test 'equal))

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

(declaim (ftype (function (list &key (:distinct boolean)) sql-number) sql-count-star))
(defun sql-count-star (x &key distinct)
  (declare (ignore distinct))
  (length x))

(declaim (ftype (function (list &key (:distinct boolean)) sql-number) sql-count))
(defun sql-count (x &key distinct)
  (length (remove :null (if distinct
                            (remove-duplicates x)
                            x))))

(declaim (ftype (function (list &key (:distinct boolean)) sql-number) sql-avg))
(defun sql-avg (x &key distinct)
  (let ((x-no-nulls (remove :null (if distinct
                                      (remove-duplicates x)
                                      x))))
    (if x-no-nulls
        (sql-/ (reduce #'sql-+ x-no-nulls) (length x-no-nulls))
        :null)))

(declaim (ftype (function (list &key (:distinct boolean)) sql-number) sql-sum))
(defun sql-sum (x &key distinct)
  (let ((x-no-nulls (remove :null (if distinct
                                      (remove-duplicates x)
                                      x))))
    (if x-no-nulls
        (reduce #'sql-+ x-no-nulls)
        :null)))

(declaim (ftype (function (list &key (:distinct boolean)) sql-number) sql-min))
(defun sql-min (x &key distinct)
  (let ((x-no-nulls (remove :null (if distinct
                                      (remove-duplicates x)
                                      x))))
    (if x-no-nulls
        (reduce
         (lambda (x y)
           (if (sql-< x y)
               x
               y))
         x-no-nulls)
        :null)))

(declaim (ftype (function (list &key (:distinct boolean)) sql-number) sql-max))
(defun sql-max (x &key distinct)
  (let ((x-no-nulls (remove :null (if distinct
                                      (remove-duplicates x)
                                      x))))
    (if x-no-nulls
        (reduce
         (lambda (x y)
           (if (sql-> x y)
               x
               y))
         x-no-nulls)
        :null)))

(defun %sql-sort (rows order-by)
  (labels ((asc (x y)
             (cond
               ((eq :null x) t)
               ((eq :null y) nil)
               (t (< x y))))
           (desc (x y)
             (cond
               ((eq :null y) t)
               ((eq :null x) nil)
               (t (> x y)))))
    (sort rows (lambda (x y)
                 (loop for (idx . direction) in order-by
                       for cmp = (ecase direction
                                   (:asc #'asc)
                                   (:desc #'desc))
                       for xv = (nth (1- idx) x)
                       for yv = (nth (1- idx) y)
                       thereis (funcall cmp xv yv)
                       until (funcall cmp yv xv))))))

(defun %sql-group-by (rows group-count group-expr-count)
  (let ((acc (make-hash-table :test 'equal)))
    (if (and (null rows) (zerop group-count))
        (setf (gethash () acc) ())
        (loop for row in rows
              for k = (subseq row 0 group-count)
              do (setf (gethash k acc)
                       (let ((group-acc (or (gethash k acc) (make-list group-expr-count))))
                         (mapcar #'cons (subseq row group-count) group-acc)))))
    acc))

(defun sql-create-table (db table-name columns)
  (let ((table (make-hash-table)))
    (setf (gethash :columns table) columns)
    (setf (gethash :rows table) ())
    (setf (gethash table-name db) table)
    (values nil t)))

(defun sql-create-index (db)
  (declare (ignore db))
  (values nil t))

(defun sql-insert (db table-name values &key column-names)
  (let* ((table (gethash table-name db))
         (rows (gethash :rows table))
         (values (if column-names
                     (let ((column->idx (make-hash-table :test 'equal)))
                       (loop for column in column-names
                             for idx from 0
                             do (setf (gethash column column->idx) idx))
                       (mapcar (lambda (row)
                                 (mapcar (lambda (column)
                                           (nth (gethash column column->idx) row))
                                         (gethash :columns table)))
                               values))
                     values)))
    (setf (gethash :rows table) (append values rows))
    (values nil (length values))))
